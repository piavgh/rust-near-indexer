mod config;
mod database;
mod event_handler;
mod postgres_database;
mod retry;
mod shutdown_coordinator;
mod token_holders_checkpoint;
mod types;

use crate::config::{AppConfig, init_tracing};
use crate::database::{get_last_height, init_clickhouse_client, init_redis_client};
use crate::event_handler::handle_stream;
use crate::postgres_database::{
    check_postgres_connection, get_last_height_postgres, init_postgres_client,
};
use crate::retry::with_retry;
use crate::shutdown_coordinator::ShutdownCoordinator;
use crate::token_holders_checkpoint::{
    ClickHouseCheckpointDatabase, PostgresCheckpointDatabase, TokenHoldersCheckpoint,
};
use std::env;
use std::sync::Arc;
use tracing::{error, info};

use near_lake_framework::LakeConfigBuilder;

pub enum StorageBackend {
    ClickHouse(clickhouse::Client),
    PostgreSQL(Arc<tokio_postgres::Client>),
}

async fn check_clickhouse_connection(
    client: &clickhouse::Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Checking ClickHouse connection...");

    with_retry(
        || async {
            match client.query("SELECT 1").fetch_one::<u8>().await {
                Ok(1) => Ok(()),
                Ok(value) => Err(format!(
                    "Unexpected response from ClickHouse: {} (expected 1)",
                    value
                )),
                Err(err) => Err(err.to_string()),
            }
        },
        5,
        |_| true, // All errors are retriable for connection check
        "ClickHouse connection check",
    )
    .await?;

    println!("ClickHouse connection successful!");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();
    println!("Starting NEAR Defuse Indexer...");

    let config = AppConfig::from_env();
    config.log_config();

    // Check storage backend type from environment variable
    let storage_backend = env::var("STORAGE_BACKEND")
        .unwrap_or_else(|_| "clickhouse".to_string())
        .to_lowercase();

    let storage = match storage_backend.as_str() {
        "postgresql" | "postgres" => {
            println!("Initializing PostgreSQL client...");
            let postgres_client = init_postgres_client().await?;

            let postgres_client_arc = Arc::new(postgres_client);
            if let Err(err) = check_postgres_connection(&postgres_client_arc).await {
                return Err(format!("PostgreSQL health check failed: {}", err).into());
            }
            println!("PostgreSQL connection successful!");

            StorageBackend::PostgreSQL(postgres_client_arc)
        }
        "clickhouse" | _ => {
            println!("Initializing ClickHouse client...");
            let clickhouse_client = init_clickhouse_client();

            if let Err(err) = check_clickhouse_connection(&clickhouse_client).await {
                return Err(format!("ClickHouse health check failed: {}", err).into());
            }

            StorageBackend::ClickHouse(clickhouse_client)
        }
    };

    println!("Initializing Redis client...");
    let redis_client = init_redis_client().await;

    let shutdown_coordinator = ShutdownCoordinator::new(config.shutdown);

    if config.checkpoint.enabled {
        match &storage {
            StorageBackend::ClickHouse(clickhouse_client) => {
                let checkpoint_client = clickhouse_client.clone();
                let checkpoint_config = config.checkpoint.clone();
                let checkpoint_token = shutdown_coordinator.token.clone();
                shutdown_coordinator.tracker.spawn(async move {
                    let database = ClickHouseCheckpointDatabase::new(checkpoint_client);
                    let checkpoint = TokenHoldersCheckpoint::new(database, checkpoint_config);
                    if let Err(e) = checkpoint.start(checkpoint_token).await {
                        error!("Token holders checkpoint job failed: {}", e);
                    }
                });
            }
            StorageBackend::PostgreSQL(postgres_client) => {
                let checkpoint_client = postgres_client.clone();
                let checkpoint_config = config.checkpoint.clone();
                let checkpoint_token = shutdown_coordinator.token.clone();
                shutdown_coordinator.tracker.spawn(async move {
                    let database = PostgresCheckpointDatabase::new(checkpoint_client);
                    let checkpoint = TokenHoldersCheckpoint::new(database, checkpoint_config);
                    if let Err(e) = checkpoint.start(checkpoint_token).await {
                        error!("Token holders checkpoint job failed: {}", e);
                    }
                });
            }
        }
    }

    if config.indexer.enabled {
        println!("Getting last processed block height...");
        let last_height = match &storage {
            StorageBackend::ClickHouse(client) => get_last_height(client).await.unwrap_or_else(|e| {
                println!(
                    "Warning: Failed to get last height: {}. Starting from configured block height.",
                    e
                );
                0
            }),
            StorageBackend::PostgreSQL(client) => get_last_height_postgres(client).await.unwrap_or_else(|e| {
                println!(
                    "Warning: Failed to get last height: {}. Starting from configured block height.",
                    e
                );
                0
            }),
        };

        let start_block = config.indexer.block_height.max(last_height + 1);

        println!("Starting indexer at block height: {}", start_block);

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

        println!("Starting stream processing...");

        let stream_handler_token = shutdown_coordinator.token.clone();
        shutdown_coordinator.tracker.spawn(async move {
            handle_stream(lake_config, storage, redis_client, stream_handler_token).await;
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
