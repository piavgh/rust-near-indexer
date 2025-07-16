mod cache;
mod config;
mod database;
mod event_handler;
mod retry;
mod shutdown_coordinator;
mod types;

use crate::cache::receipts_cache::ReceiptsCache;
use crate::cache::{CACHE_EXPIRATION_BLOCKS, CACHE_SIZE};
use crate::config::{AppConfig, init_tracing};
use crate::database::StorageBackend;
use crate::database::clickhouse::ClickhouseDatabase;
use crate::database::postgres::PostgresDatabase;
use crate::event_handler::EventHandler;
use crate::shutdown_coordinator::ShutdownCoordinator;
use near_lake_framework::LakeConfigBuilder;
use redis::cmd;
use std::env;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();
    info!("Starting NEAR Defuse Indexer...");

    let config = AppConfig::from_env();
    config.log_config();

    // Check storage backend type from environment variable
    let storage_backend = env::var("STORAGE_BACKEND")
        .expect("STORAGE_BACKEND must be set")
        .to_lowercase();

    let storage: Box<dyn StorageBackend> = match storage_backend.as_str() {
        "postgres" => {
            info!("Initializing PostgreSQL client...");

            // Initializes the PostgreSQL client using environment variables.
            // Environment variables required:
            // - `POSTGRES_CONNECTION_STRING` - Full PostgreSQL connection string (e.g., "postgresql://user:password@localhost:5432/database")
            let connection_string = env::var("POSTGRES_CONNECTION_STRING")
                .expect("POSTGRES_CONNECTION_STRING not set in environment");

            // Run database migrations
            info!("Running database migrations...");
            let migration_pool = sqlx::PgPool::connect(&connection_string).await?;
            sqlx::migrate!("./migrations").run(&migration_pool).await?;
            info!("Database migrations completed successfully");

            let postgres_database = PostgresDatabase::new(&connection_string)
                .await
                .ok_or("Failed to create PostgreSQL database connection")?;

            Box::new(postgres_database)
        }
        "clickhouse" | _ => {
            info!("Initializing ClickHouse client...");

            // Initializes the Clickhouse client using environment variables.
            // Environment variables required:
            // - `CLICKHOUSE_URL`
            // - `CLICKHOUSE_USER`
            // - `CLICKHOUSE_PASSWORD`
            // - `CLICKHOUSE_DATABASE`
            let url = env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL not set in environment");
            let user = env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER not set in environment");
            let password = env::var("CLICKHOUSE_PASSWORD")
                .expect("CLICKHOUSE_PASSWORD not set in environment");
            let database = env::var("CLICKHOUSE_DATABASE")
                .expect("CLICKHOUSE_DATABASE not set in environment");

            let clickhouse_db = ClickhouseDatabase::new(&url, &user, &password, &database);

            Box::new(clickhouse_db)
        }
    };

    // Check storage connection
    if let Err(err) = storage.check_connection().await {
        return Err(format!("{} health check failed: {}", storage_backend, err).into());
    }
    info!("{} connection successful!", storage_backend);

    info!("Initializing Redis client...");

    // Initializes Redis client using environment variables.
    // Environment variables required:
    // - `REDIS_URL` (optional, will return None if not set)
    let redis_connection_string = env::var("REDIS_URL").expect("REDIS_URL not set in environment");
    let redis_client = redis::Client::open(redis_connection_string);

    // Check Redis connection if client was created successfully
    let redis_client = if let Ok(client) = redis_client {
        let mut conn = client.get_multiplexed_async_connection().await?;

        let pong: String = cmd("PING").query_async(&mut conn).await?;

        if pong != "PONG" {
            warn!("Redis PING returned unexpected response: {}", pong);
            None
        } else {
            info!("Redis connection verified successfully");
            Some(client)
        }
    } else {
        warn!("Failed to create Redis client");
        None
    };

    // Initialize the graceful shutdown coordinator
    let shutdown_coordinator = ShutdownCoordinator::new(config.shutdown);

    if config.indexer.enabled {
        info!("Getting last processed block height...");
        let last_height = storage.get_last_height().await.unwrap_or_else(|e| {
            warn!(
                "Warning: Failed to get last height: {}. Starting from configured block height.",
                e
            );
            0
        });

        let start_block = config.indexer.block_height.max(last_height + 1);

        info!("Starting indexer at block height: {}", start_block);

        let lake_config = match LakeConfigBuilder::default()
            .mainnet()
            .start_block_height(start_block)
            .build()
        {
            Ok(config) => config,
            Err(e) => {
                return Err(format!("Failed to create NEAR Lake Framework config: {}", e).into());
            }
        };

        let receipts_cache = match &redis_client {
            Some(client) => {
                match ReceiptsCache::with_redis(client.clone(), CACHE_SIZE, CACHE_EXPIRATION_BLOCKS)
                {
                    Ok(cache) => {
                        let cache_arc = Arc::new(Mutex::new(cache));

                        {
                            let mut cache_lock = cache_arc.lock().await;
                            if let Err(e) = cache_lock.load_from_redis().await {
                                error!("Error loading cache from Redis: {}", e);
                            }
                        }

                        info!("Using Redis for transaction cache persistence");
                        cache_arc
                    }
                    Err(e) => {
                        error!(
                            "Failed to initialize Redis cache: {}, falling back to in-memory cache",
                            e
                        );
                        Arc::new(Mutex::new(ReceiptsCache::new(CACHE_SIZE)))
                    }
                }
            }
            None => {
                info!("Using in-memory cache only");
                Arc::new(Mutex::new(ReceiptsCache::new(CACHE_SIZE)))
            }
        };

        let event_handler = EventHandler::new(storage, receipts_cache);

        let stream_handler_token = shutdown_coordinator.token.clone();
        shutdown_coordinator.tracker.spawn(async move {
            event_handler
                .handle_stream(lake_config, stream_handler_token)
                .await;
        });
    }

    info!("✅ All services started successfully");

    // Set up signal handling for graceful shutdown
    let signal_handle = tokio::spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("🛑 Received SIGINT (Ctrl+C)");
            }
            _ = async {
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{signal, SignalKind};
                    let mut sigterm = signal(SignalKind::terminate()).unwrap();
                    sigterm.recv().await;
                }
                #[cfg(not(unix))]
                {
                    // On Windows, we can only listen for Ctrl+C
                    std::future::pending::<()>().await;
                }
            } => {
                info!("🛑 Received SIGTERM");
            }
        }

        // Initiate shutdown when signal received
        shutdown_coordinator.initiate_shutdown().await;
    });

    // Wait for the signal handler to complete
    // The signal handler will call initiate_shutdown() which waits for all tracked tasks
    signal_handle.await?;

    info!("🎉 Indexer shutdown complete!");

    Ok(())
}
