use crate::database::StorageBackend;
use crate::retry::{is_network_error, with_retry};
use crate::types::EventRow;
use chrono::{DateTime, Utc};
use tokio_postgres::{Client, NoTls};
use tracing::error;

pub struct PostgresDatabase {
    pub client: Client,
}

impl PostgresDatabase {
    pub async fn new(connection_string: &str) -> Option<PostgresDatabase> {
        match tokio_postgres::connect(&connection_string, NoTls).await {
            Ok((client, connection)) => {
                // Spawn the connection task in the background
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        error!("PostgreSQL connection error: {}", e);
                    }
                });

                Some(PostgresDatabase { client })
            }
            Err(e) => {
                error!("Failed to create PostgreSQL client: {}", e);
                None
            }
        }
    }

    async fn insert_rows_internal(
        &self,
        rows: &[EventRow],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if rows.is_empty() {
            return Ok(());
        }

        // Use batch insert with multiple rows
        for chunk in rows.chunks(100) {
            // Process in chunks to avoid too many parameters
            let mut query = String::from(
                "INSERT INTO events (
            block_height, block_timestamp, block_hash, contract_id, execution_status, 
            version, standard, index_in_log, event, data, related_receipt_id, 
            related_receipt_receiver_id, related_receipt_predecessor_id, tx_hash
        ) VALUES ",
            );

            let values_parts: Vec<String> = (0..chunk.len())
                .map(|i| {
                    let base = i * 14 + 1;
                    format!(
                        "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                        base, base + 1, base + 2, base + 3, base + 4, base + 5, base + 6,
                        base + 7, base + 8, base + 9, base + 10, base + 11, base + 12, base + 13
                    )
                })
                .collect();

            query.push_str(&values_parts.join(", "));
            query.push_str(
                " ON CONFLICT (block_height, related_receipt_id, index_in_log) DO UPDATE SET 
            block_timestamp = EXCLUDED.block_timestamp,
            block_hash = EXCLUDED.block_hash,
            contract_id = EXCLUDED.contract_id,
            execution_status = EXCLUDED.execution_status,
            version = EXCLUDED.version,
            standard = EXCLUDED.standard,
            event = EXCLUDED.event,
            data = EXCLUDED.data,
            related_receipt_receiver_id = EXCLUDED.related_receipt_receiver_id,
            related_receipt_predecessor_id = EXCLUDED.related_receipt_predecessor_id,
            tx_hash = EXCLUDED.tx_hash",
            );

            // Create parameters vector
            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();

            // Convert data to PostgreSQL-compatible types
            let mut block_heights: Vec<i64> = Vec::new();
            let mut timestamps: Vec<DateTime<Utc>> = Vec::new();
            let mut index_in_logs: Vec<i64> = Vec::new();

            for row in chunk {
                block_heights.push(row.block_height as i64);
                let timestamp = DateTime::from_timestamp_nanos(row.block_timestamp as i64);
                timestamps.push(timestamp);
                index_in_logs.push(row.index_in_log as i64);
            }

            for (i, row) in chunk.iter().enumerate() {
                params.push(&block_heights[i]);
                params.push(&timestamps[i]);
                params.push(&row.block_hash);
                params.push(&row.contract_id);
                params.push(&row.execution_status);
                params.push(&row.version);
                params.push(&row.standard);
                params.push(&index_in_logs[i]);
                params.push(&row.event);
                params.push(&row.data);
                params.push(&row.related_receipt_id);
                params.push(&row.related_receipt_receiver_id);
                params.push(&row.related_receipt_predecessor_id);
                params.push(&row.tx_hash);
            }

            self.client.execute(&query, &params).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageBackend for PostgresDatabase {
    /// Checks if PostgreSQL is available and functioning properly
    async fn check_connection(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.client.query_one("SELECT 1", &[]).await?;
        Ok(())
    }

    async fn get_last_height(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let row = self
            .client
            .query_one("SELECT COALESCE(MAX(block_height), 0) FROM events", &[])
            .await?;

        let height: i64 = row.get(0);
        Ok(height as u64)
    }

    async fn insert_rows(
        &self,
        rows: &[EventRow],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let rows_ref = rows;

        with_retry(
            || async { self.insert_rows_internal(rows_ref).await },
            5,
            |e| is_network_error(&e.to_string()),
            "postgres insert",
        )
        .await
    }
}
