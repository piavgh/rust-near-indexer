use figment::Figment;
use figment::providers::{Env, Format, Yaml};
use serde::Deserialize;
use std::env;
use tracing::info;

use crate::shutdown_coordinator::ShutdownConfig;
use crate::storage::StorageConfig;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub env: String,
    pub indexer: IndexerConfig,
    pub shutdown: ShutdownConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexerConfig {
    pub enabled: bool,
    pub block_height: u64,
}

impl AppConfig {
    pub fn new(config_path: &str) -> anyhow::Result<Self> {
        let config: AppConfig = Figment::new()
            .merge(Yaml::file(config_path))
            // Manually map environment variables to config fields:
            .merge(
                Env::raw()
                    .only(&[
                        "STORAGE_BACKEND",
                        "POSTGRES_CONNECTION_STRING",
                        "CLICKHOUSE_URL",
                        "CLICKHOUSE_USER",
                        "CLICKHOUSE_PASSWORD",
                        "CLICKHOUSE_DB",
                    ])
                    .map(|k| match k.as_str() {
                        "STORAGE_BACKEND" => "storage.backend".into(),
                        "POSTGRES_CONNECTION_STRING" => "storage.postgres.connection_string".into(),
                        "CLICKHOUSE_URL" => "storage.clickhouse.url".into(),
                        "CLICKHOUSE_USER" => "storage.clickhouse.user".into(),
                        "CLICKHOUSE_PASSWORD" => "storage.clickhouse.password".into(),
                        "CLICKHOUSE_DB" => "storage.clickhouse.database".into(),
                        _ => k.as_str().into(),
                    })
                    .split(".")
            )
            .merge(Env::prefixed("APP__").split("__"))
            .extract()?;

        Ok(config)
    }

    pub fn log_config(&self) {
        info!("config: {:?}", self);
    }
}

pub fn init_tracing() {
    let env_filter = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
}
