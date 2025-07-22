use crate::decimal_utils::{decimal_to_u128, u128_to_decimal};
use crate::retry::{is_network_error, with_retry};
use crate::storage::StorageBackend;
use crate::types::{EventRow, SwapRow};
use clickhouse::{Client, Row};

use serde::Deserialize;
use std::fs;
use std::path::Path;
use tracing::{info, warn};

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

#[derive(Row, serde::Serialize, serde::Deserialize)]
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

#[derive(Row, serde::Serialize, serde::Deserialize)]
struct ClickhouseSwapRow {
    intent_hash: String,
    origin_asset: String,
    destination_asset: String,
    amount_in: u128,
    amount_out: u128,
    withdrawal_fee: u128,
    recipient: String,
    tx_hash: Option<String>,
}

impl From<&SwapRow> for ClickhouseSwapRow {
    fn from(swap_row: &SwapRow) -> Self {
        ClickhouseSwapRow {
            intent_hash: swap_row.intent_hash.clone(),
            origin_asset: swap_row.origin_asset.clone(),
            destination_asset: swap_row.destination_asset.clone(),
            amount_in: decimal_to_u128(swap_row.amount_in),
            amount_out: decimal_to_u128(swap_row.amount_out),
            withdrawal_fee: decimal_to_u128(swap_row.withdrawal_fee),
            recipient: swap_row.recipient.clone(),
            tx_hash: swap_row.tx_hash.clone(),
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

    pub async fn run_migrations(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Running ClickHouse database migrations...");

        // Create migrations tracking table
        self.create_migrations_table().await?;

        let migrations_dir = Path::new("migrations/clickhouse");
        if !migrations_dir.exists() {
            warn!(
                "ClickHouse migrations directory not found: {:?}",
                migrations_dir
            );
            return Ok(());
        }

        // Read all migration files
        let mut migration_files: Vec<_> = fs::read_dir(migrations_dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_name = entry.file_name();
                let file_name_str = file_name.to_str()?;
                if file_name_str.ends_with(".up.sql") {
                    Some((file_name_str.to_string(), entry.path()))
                } else {
                    None
                }
            })
            .collect();

        // Sort migrations by filename to ensure correct order
        migration_files.sort_by(|a, b| a.0.cmp(&b.0));

        for (filename, path) in migration_files {
            let migration_name = filename.replace(".up.sql", "");

            // Check if migration has already been applied
            if self.is_migration_applied(&migration_name).await? {
                info!("Migration {} already applied, skipping", migration_name);
                continue;
            }

            info!("Applying migration: {}", migration_name);

            // Read and execute migration
            let migration_sql = fs::read_to_string(&path)?;
            self.execute_migration_sql(&migration_sql).await?;

            // Record migration as applied
            self.record_migration(&migration_name).await?;

            info!("Migration {} applied successfully", migration_name);
        }

        info!("ClickHouse database migrations completed successfully");
        Ok(())
    }

    async fn create_migrations_table(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let create_table_sql = r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                migration_name String NOT NULL,
                applied_at DateTime64(3) DEFAULT now64(3)
            ) ENGINE = MergeTree()
            ORDER BY migration_name
        "#;

        self.client.query(create_table_sql).execute().await?;
        Ok(())
    }

    async fn is_migration_applied(
        &self,
        migration_name: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let query = format!(
            "SELECT count(*) FROM schema_migrations WHERE migration_name = '{}'",
            migration_name
        );

        let count: u64 = self.client.query(&query).fetch_one().await?;
        Ok(count > 0)
    }

    async fn execute_migration_sql(
        &self,
        sql: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.client.query(sql).execute().await?;
        Ok(())
    }

    async fn record_migration(
        &self,
        migration_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let query = format!(
            "INSERT INTO schema_migrations (migration_name) VALUES ('{}')",
            migration_name
        );

        self.client.query(&query).execute().await?;
        Ok(())
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

    async fn insert_swaps_internal(
        &self,
        swaps: &[ClickhouseSwapRow],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut insert = self.client.insert("swaps")?;
        for swap in swaps {
            insert.write(swap).await?;
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

    async fn insert_swaps(
        &self,
        swaps: &[SwapRow],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Convert SwapRow to ClickhouseSwapRow
        let clickhouse_swaps: Vec<ClickhouseSwapRow> =
            swaps.iter().map(|swap| swap.into()).collect();

        with_retry(
            || async { self.insert_swaps_internal(&clickhouse_swaps).await },
            5,
            |e| is_network_error(&e.to_string()),
            "clickhouse insert swaps",
        )
        .await
    }

    async fn get_events(
        &self,
        tx_hash: Option<String>,
        limit: u32,
        offset: u32,
    ) -> Result<(Vec<EventRow>, u32), Box<dyn std::error::Error + Send + Sync>> {
        let mut query = "SELECT 
            block_height,
            block_timestamp,
            block_hash,
            contract_id,
            execution_status,
            version,
            standard,
            index_in_log,
            event,
            data,
            related_receipt_id,
            related_receipt_receiver_id,
            related_receipt_predecessor_id,
            tx_hash
        FROM events"
            .to_string();

        let mut count_query = "SELECT count(*) FROM events".to_string();

        if let Some(tx_hash) = &tx_hash {
            let where_clause = format!(" WHERE tx_hash = '{}'", tx_hash);
            query.push_str(&where_clause);
            count_query.push_str(&where_clause);
        }

        query.push_str(" ORDER BY block_height DESC, index_in_log DESC");
        query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

        let events_future = self.client.query(&query).fetch_all::<ClickhouseEventRow>();
        let count_future = self.client.query(&count_query).fetch_one::<u64>();

        let (events_result, count_result) = tokio::try_join!(events_future, count_future)?;

        let events: Vec<EventRow> = events_result
            .into_iter()
            .map(|row| EventRow {
                block_height: row.block_height,
                block_timestamp: row.block_timestamp,
                block_hash: row.block_hash,
                contract_id: row.contract_id,
                execution_status: row.execution_status,
                version: row.version,
                standard: row.standard,
                index_in_log: row.index_in_log,
                event: row.event,
                data: row.data,
                related_receipt_id: row.related_receipt_id,
                related_receipt_receiver_id: row.related_receipt_receiver_id,
                related_receipt_predecessor_id: row.related_receipt_predecessor_id,
                tx_hash: row.tx_hash,
            })
            .collect();

        Ok((events, count_result as u32))
    }

    async fn get_swap_by_intent_hash(
        &self,
        intent_hash: &str,
    ) -> Result<Option<SwapRow>, Box<dyn std::error::Error + Send + Sync>> {
        let query = format!(
            "SELECT
                intent_hash,
                origin_asset,
                destination_asset,
                amount_in,
                amount_out,
                withdrawal_fee,
                recipient,
                tx_hash
            FROM swaps
            WHERE intent_hash = '{}'",
            intent_hash
        );

        let results = self
            .client
            .query(&query)
            .fetch_all::<ClickhouseSwapRow>()
            .await?;

        if results.is_empty() {
            return Ok(None);
        }

        let row = &results[0];
        Ok(Some(SwapRow {
            intent_hash: row.intent_hash.clone(),
            origin_asset: row.origin_asset.clone(),
            destination_asset: row.destination_asset.clone(),
            amount_in: u128_to_decimal(row.amount_in),
            amount_out: u128_to_decimal(row.amount_out),
            withdrawal_fee: u128_to_decimal(row.withdrawal_fee),
            recipient: row.recipient.clone(),
            tx_hash: row.tx_hash.clone(),
        }))
    }
}
