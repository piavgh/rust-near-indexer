mod api;
mod cache;
mod config;
mod data_source;
mod decimal_utils;
mod event_handler;
mod receipt_processor;
mod retry;
mod shutdown_coordinator;
mod storage;
mod types;

use crate::cache::receipts_cache::ReceiptsCache;
use crate::cache::{CACHE_EXPIRATION_BLOCKS, CACHE_SIZE};
use crate::config::{AppConfig, init_tracing};
use crate::event_handler::EventHandler;
use crate::receipt_processor::ReceiptProcessor;
use crate::shutdown_coordinator::ShutdownCoordinator;
use crate::storage::StorageBackend;
use crate::storage::clickhouse::ClickhouseDatabase;
use crate::storage::postgres::PostgresDatabase;
use clap::Parser;
use dotenvy::dotenv;
use redis::cmd;
use std::env;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[clap(author = "Hoang Trinh <hoang.trinhj@gmail.com>", version, about)]
/// NEAR Defuse Indexer and API
struct Arguments {
    #[arg(short = 'c', long)]
    config: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Run the indexer to process blockchain events
    Indexer,
    /// Run the API server
    Api,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv().ok();

    let args = Arguments::parse();

    if args.config.is_none() {
        panic!("No config file provided");
    }

    init_tracing();

    let config = AppConfig::new(&args.config.unwrap())?;
    config.log_config();

    info!("ENV: {}", config.env);

    match args.command {
        Commands::Indexer => {
            info!("Starting NEAR Intents Indexer...");
            run_indexer(config).await?;
        }
        Commands::Api => {
            info!("Starting API server...");
            run_api(config).await?;
        }
    }

    Ok(())
}

async fn run_indexer(config: AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Check storage backend type from environment variable
    let storage_backend = config.storage.backend;

    let storage: Box<dyn StorageBackend> = match storage_backend.as_str() {
        "postgres" => {
            info!("Initializing PostgreSQL client...");

            // Initializes the PostgreSQL client using environment variables.
            // Environment variables required:
            // - `POSTGRES_CONNECTION_STRING` - Full PostgreSQL connection string (e.g., "postgresql://user:password@localhost:5432/database")
            let connection_string = &config.storage.postgres.connection_string;
            if connection_string.is_empty() {
                return Err("No PostgreSQL connection string provided".into());
            }

            // Run database migrations
            info!("Running database migrations...");
            let migration_pool = sqlx::PgPool::connect(connection_string).await?;
            sqlx::migrate!("./migrations").run(&migration_pool).await?;
            info!("Database migrations completed successfully");

            let postgres_database = PostgresDatabase::new(connection_string)
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
            let url = &config.storage.clickhouse.url;
            let user = &config.storage.clickhouse.user;
            let password = &config.storage.clickhouse.password;
            let database = &config.storage.clickhouse.database;

            let clickhouse_db = ClickhouseDatabase::new(url, user, password, database);

            // Run database migrations
            info!("Running database migrations...");
            clickhouse_db.run_migrations().await?;
            info!("Database migrations completed successfully");

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

        let stream_handler_token = shutdown_coordinator.token.clone();

        // Create stream from configured data source
        let stream = match data_source::create_stream(
            &config.indexer.data_source,
            start_block,
            stream_handler_token.clone(),
            &shutdown_coordinator.tracker,
        )
        .await
        {
            Ok(stream) => stream,
            Err(e) => {
                return Err(format!("Failed to create data source stream: {}", e).into());
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

        let receipt_processor = ReceiptProcessor::new(receipts_cache);
        let event_handler = EventHandler::new(storage, receipt_processor);

        shutdown_coordinator.tracker.spawn(async move {
            event_handler
                .handle_stream(stream, stream_handler_token)
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

async fn run_api(config: AppConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize storage backend
    let storage_backend = config.storage.backend;

    let storage: Arc<dyn StorageBackend> = match storage_backend.as_str() {
        "postgres" => {
            info!("Initializing PostgreSQL client for API...");
            let connection_string = &config.storage.postgres.connection_string;
            if connection_string.is_empty() {
                return Err("No PostgreSQL connection string provided".into());
            }

            let postgres_database = PostgresDatabase::new(connection_string)
                .await
                .ok_or("Failed to create PostgreSQL database connection")?;

            Arc::new(postgres_database)
        }
        "clickhouse" | _ => {
            info!("Initializing ClickHouse client for API...");
            let url = &config.storage.clickhouse.url;
            let user = &config.storage.clickhouse.user;
            let password = &config.storage.clickhouse.password;
            let database = &config.storage.clickhouse.database;

            let clickhouse_db = ClickhouseDatabase::new(url, user, password, database);

            // Run database migrations
            info!("Running database migrations...");
            clickhouse_db.run_migrations().await?;
            info!("Database migrations completed successfully");

            Arc::new(clickhouse_db)
        }
    };

    // Check storage connection
    if let Err(err) = storage.check_connection().await {
        return Err(format!("{} health check failed: {}", storage_backend, err).into());
    }
    info!("{} connection successful!", storage_backend);

    // Create the Axum router
    let app = api::create_router(storage, &config.http);

    // Start the server
    let listener = tokio::net::TcpListener::bind(&config.http.bind_address).await?;
    info!(
        "🚀 API server started on http://{}",
        config.http.bind_address
    );

    // Set up graceful shutdown
    let shutdown_signal = async {
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
        info!("🔄 Initiating graceful shutdown...");
    };

    // Start the server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await?;

    info!("🎉 API server shutdown complete!");
    Ok(())
}
