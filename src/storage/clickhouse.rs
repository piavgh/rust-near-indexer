use crate::storage::StorageBackend;
use crate::retry::{is_network_error, with_retry};
use crate::types::EventRow;
use clickhouse::{Client, Row};
use serde::Deserialize;
use tracing::info;

#[derive(Debug, Clone, Deserialize)]
pub struct ClickhouseConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
}

pub struct ClickhouseDatabase {
    pub client: Client,
}

#[derive(Row, serde::Serialize)]
struct ClickhouseEventRow {
    block_height: u64,
    block_timestamp: u64,
    block_hash: String,
    contract_id: String,
    execution_status: String,
    version: String,
    standard: String,
    index_in_log: u64,
    event: String,
    data: String,
    related_receipt_id: String,
    related_receipt_receiver_id: String,
    related_receipt_predecessor_id: String,
    tx_hash: Option<String>,
}

impl From<&EventRow> for ClickhouseEventRow {
    fn from(event_row: &EventRow) -> Self {
        ClickhouseEventRow {
            block_height: event_row.block_height,
            block_timestamp: event_row.block_timestamp,
            block_hash: event_row.block_hash.clone(),
            contract_id: event_row.contract_id.clone(),
            execution_status: event_row.execution_status.clone(),
            version: event_row.version.clone(),
            standard: event_row.standard.clone(),
            index_in_log: event_row.index_in_log,
            event: event_row.event.clone(),
            data: event_row.data.clone(),
            related_receipt_id: event_row.related_receipt_id.clone(),
            related_receipt_receiver_id: event_row.related_receipt_receiver_id.clone(),
            related_receipt_predecessor_id: event_row.related_receipt_predecessor_id.clone(),
            tx_hash: event_row.tx_hash.clone(),
        }
    }
}

impl ClickhouseDatabase {
    pub fn new(url: &str, user: &str, password: &str, database: &str) -> ClickhouseDatabase {
        let client = Client::default()
            .with_url(url)
            .with_user(user)
            .with_password(password)
            .with_database(database)
            .with_option("connect_timeout", "10")
            .with_option("receive_timeout", "30")
            .with_option("send_timeout", "30");

        ClickhouseDatabase { client }
    }

    async fn insert_rows_internal(
        &self,
        rows: &[ClickhouseEventRow],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut insert = self.client.insert("events")?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageBackend for ClickhouseDatabase {
    async fn check_connection(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Checking ClickHouse connection...");

        with_retry(
            || async {
                match self.client.query("SELECT 1").fetch_one::<u8>().await {
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

        info!("ClickHouse connection successful!");
        Ok(())
    }

    async fn get_last_height(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        self.client
            .query("SELECT max(block_height) FROM events")
            .fetch_one::<u64>()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    async fn insert_rows(
        &self,
        rows: &[EventRow],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Convert EventRow to ClickhouseEventRow
        let clickhouse_rows: Vec<ClickhouseEventRow> = rows.iter().map(|row| row.into()).collect();

        with_retry(
            || async { self.insert_rows_internal(&clickhouse_rows).await },
            5,
            |e| is_network_error(&e.to_string()),
            "clickhouse insert",
        )
        .await
    }
}
