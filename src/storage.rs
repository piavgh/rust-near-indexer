pub mod clickhouse;
pub mod postgres;

use crate::storage::clickhouse::ClickhouseConfig;
use crate::storage::postgres::PostgresConfig;
use crate::types::EventRow;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub backend: String,
    pub postgres: PostgresConfig,
    pub clickhouse: ClickhouseConfig,
}

/// Common interface for storage backends
#[async_trait::async_trait]
pub trait StorageBackend: Send + Sync {
    /// Check if the database connection is working
    async fn check_connection(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Get the last processed block height
    async fn get_last_height(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>>;

    /// Insert rows into the database
    async fn insert_rows(
        &self,
        rows: &[EventRow],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
