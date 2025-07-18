pub mod clickhouse;
pub mod postgres;

use crate::storage::clickhouse::ClickhouseConfig;
use crate::storage::postgres::PostgresConfig;
use crate::types::{EventRow, SwapRow};
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

    /// Insert swap rows into the database
    async fn insert_swaps(
        &self,
        swaps: &[SwapRow],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Get events from the database with optional tx_hash filter
    async fn get_events(
        &self,
        tx_hash: Option<String>,
        limit: u32,
        offset: u32,
    ) -> Result<(Vec<EventRow>, u32), Box<dyn std::error::Error + Send + Sync>>;

    /// Get swap by intent hash
    async fn get_swap_by_intent_hash(
        &self,
        intent_hash: &str,
    ) -> Result<Option<SwapRow>, Box<dyn std::error::Error + Send + Sync>>;
}
