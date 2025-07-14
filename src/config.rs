use serde::Deserialize;
use std::env;
use tracing::info;

use crate::shutdown_coordinator::ShutdownConfig;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub indexer: IndexerConfig,
    pub shutdown: ShutdownConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexerConfig {
    pub enabled: bool,
    pub block_height: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            indexer: IndexerConfig {
                enabled: true,
                block_height: 0,
            },
            shutdown: ShutdownConfig {
                shutdown_timeout: std::time::Duration::from_secs(20),
            },
        }
    }
}

impl AppConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(enabled) = env::var("INDEXER_ENABLED") {
            config.indexer.enabled = enabled.to_lowercase() == "true";
        }
        if let Ok(block_height) = env::var("BLOCK_HEIGHT") {
            if let Ok(height) = block_height.parse() {
                config.indexer.block_height = height;
            }
        }

        config
    }

    pub fn log_config(&self) {
        info!("Application configuration:");
        info!("Indexer: enabled={}", self.indexer.enabled);
    }
}

pub fn init_tracing() {
    let env_filter = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
}
