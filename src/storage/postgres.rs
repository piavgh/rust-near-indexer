use crate::retry::{is_network_error, with_retry};
use crate::storage::StorageBackend;
use crate::types::{EventRow, SwapRow};
use chrono::{DateTime, Utc};
use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod};
use serde::Deserialize;
use tokio_postgres::NoTls;
use tracing::error;

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub connection_string: String,
}

pub struct PostgresDatabase {
    pub pool: Pool,
}

impl PostgresDatabase {
    pub async fn new(connection_string: &str) -> Option<PostgresDatabase> {
        let mut config = Config::new();

        // Parse connection string to extract components
        match connection_string.parse::<tokio_postgres::config::Config>() {
            Ok(pg_config) => {
                if let Some(host) = pg_config.get_hosts().first() {
                    config.host = Some(match host {
                        tokio_postgres::config::Host::Tcp(s) => s.clone(),
                        tokio_postgres::config::Host::Unix(path) => {
                            path.to_string_lossy().to_string()
                        }
                    });
                }
                config.port = pg_config.get_ports().first().copied();
                config.user = pg_config.get_user().map(|u| u.to_string());
                config.password = pg_config
                    .get_password()
                    .map(|p| String::from_utf8_lossy(p).to_string());
                config.dbname = pg_config.get_dbname().map(|db| db.to_string());
            }
            Err(e) => {
                error!("Failed to parse PostgreSQL connection string: {}", e);
                return None;
            }
        }

        config.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });

        match config.create_pool(None, NoTls) {
            Ok(pool) => {
                // Test the connection
                match pool.get().await {
                    Ok(client) => {
                        if let Err(e) = client.query_one("SELECT 1", &[]).await {
                            error!("Failed to test PostgreSQL connection: {}", e);
                            return None;
                        }
                    }
                    Err(e) => {
                        error!("Failed to get connection from pool: {}", e);
                        return None;
                    }
                }

                Some(PostgresDatabase { pool })
            }
            Err(e) => {
                error!("Failed to create PostgreSQL connection pool: {}", e);
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

        let client = self.pool.get().await?;

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
                        base,
                        base + 1,
                        base + 2,
                        base + 3,
                        base + 4,
                        base + 5,
                        base + 6,
                        base + 7,
                        base + 8,
                        base + 9,
                        base + 10,
                        base + 11,
                        base + 12,
                        base + 13
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

            client.execute(&query, &params).await?;
        }
        Ok(())
    }

    async fn insert_swaps_internal(
        &self,
        swaps: &[SwapRow],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if swaps.is_empty() {
            return Ok(());
        }

        let client = self.pool.get().await?;

        // Use batch insert with multiple rows
        for chunk in swaps.chunks(100) {
            // Process in chunks to avoid too many parameters
            let mut query = String::from(
                "INSERT INTO swaps (
                intent_hash, origin_asset, destination_asset, amount_in, amount_out, recipient, tx_hash
            ) VALUES ",
            );

            let values_parts: Vec<String> = (0..chunk.len())
                .map(|i| {
                    let base = i * 7 + 1;
                    format!(
                        "(${}, ${}, ${}, ${}, ${}, ${}, ${})",
                        base,
                        base + 1,
                        base + 2,
                        base + 3,
                        base + 4,
                        base + 5,
                        base + 6
                    )
                })
                .collect();

            query.push_str(&values_parts.join(", "));
            query.push_str(
                " ON CONFLICT (intent_hash) DO UPDATE SET
                origin_asset = EXCLUDED.origin_asset,
                destination_asset = EXCLUDED.destination_asset,
                amount_in = EXCLUDED.amount_in,
                amount_out = EXCLUDED.amount_out,
                recipient = EXCLUDED.recipient,
                tx_hash = EXCLUDED.tx_hash",
            );

            // Create parameters vector
            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();

            for row in chunk {
                params.push(&row.intent_hash);
                params.push(&row.origin_asset);
                params.push(&row.destination_asset);
                params.push(&row.amount_in);
                params.push(&row.amount_out);
                params.push(&row.recipient);
                params.push(&row.tx_hash);
            }

            client.execute(&query, &params).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageBackend for PostgresDatabase {
    /// Checks if PostgreSQL is available and functioning properly
    async fn check_connection(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;
        client.query_one("SELECT 1", &[]).await?;
        Ok(())
    }

    async fn get_last_height(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;
        let row = client
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

    async fn insert_swaps(
        &self,
        swaps: &[SwapRow],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let swaps_ref = swaps;

        with_retry(
            || async { self.insert_swaps_internal(swaps_ref).await },
            5,
            |e| is_network_error(&e.to_string()),
            "postgres insert swaps",
        )
        .await
    }

    async fn get_events(
        &self,
        tx_hash: Option<String>,
        limit: u32,
        offset: u32,
    ) -> Result<(Vec<EventRow>, u32), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;

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
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
        let mut count_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();

        if let Some(tx_hash) = &tx_hash {
            let where_clause = " WHERE tx_hash = $1";
            query.push_str(where_clause);
            count_query.push_str(where_clause);
            params.push(tx_hash);
            count_params.push(tx_hash);
        }

        query.push_str(" ORDER BY block_height DESC, index_in_log DESC");

        let param_offset = if tx_hash.is_some() { 2 } else { 1 };
        query.push_str(&format!(
            " LIMIT ${} OFFSET ${}",
            param_offset,
            param_offset + 1
        ));

        let limit_i64 = limit as i64;
        let offset_i64 = offset as i64;
        params.push(&limit_i64);
        params.push(&offset_i64);

        let events_future = client.query(&query, &params);
        let count_future = client.query_one(&count_query, &count_params);

        let (events_result, count_result) = tokio::try_join!(events_future, count_future)?;

        let events: Vec<EventRow> = events_result
            .into_iter()
            .map(|row| EventRow {
                block_height: row.get::<_, i64>(0) as u64,
                block_timestamp: row
                    .get::<_, DateTime<Utc>>(1)
                    .timestamp_nanos_opt()
                    .unwrap_or(0) as u64,
                block_hash: row.get(2),
                contract_id: row.get(3),
                execution_status: row.get(4),
                version: row.get(5),
                standard: row.get(6),
                index_in_log: row.get::<_, i64>(7) as u64,
                event: row.get(8),
                data: row.get(9),
                related_receipt_id: row.get(10),
                related_receipt_receiver_id: row.get(11),
                related_receipt_predecessor_id: row.get(12),
                tx_hash: row.get(13),
            })
            .collect();

        let total_count: i64 = count_result.get(0);
        Ok((events, total_count as u32))
    }

    async fn get_swap_by_intent_hash(
        &self,
        intent_hash: &str,
    ) -> Result<Option<SwapRow>, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.pool.get().await?;

        let query = "SELECT 
            intent_hash, 
            origin_asset, 
            destination_asset, 
            amount_in, 
            amount_out, 
            recipient, 
            tx_hash 
        FROM swaps 
        WHERE intent_hash = $1";

        let rows = client.query(query, &[&intent_hash]).await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];
        Ok(Some(SwapRow {
            intent_hash: row.get(0),
            origin_asset: row.get(1),
            destination_asset: row.get(2),
            amount_in: row.get(3),
            amount_out: row.get(4),
            recipient: row.get(5),
            tx_hash: row.get(6),
        }))
    }
}
