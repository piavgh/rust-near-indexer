use tokio_postgres::{Client, NoTls};
use std::env;
use std::sync::Arc;
use crate::retry::{with_retry, is_network_error};
use crate::types::EventRow;
use chrono::{DateTime, Utc};

/// Initializes the PostgreSQL client using environment variables.
/// Environment variables required:
/// - `POSTGRES_CONNECTION_STRING` - Full PostgreSQL connection string (e.g., "postgresql://user:password@localhost:5432/database")
pub async fn init_postgres_client() -> Result<Client, Box<dyn std::error::Error>> {
    let connection_string = env::var("POSTGRES_CONNECTION_STRING").expect("POSTGRES_CONNECTION_STRING not set in environment");

    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;
    
    // Spawn the connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });

    Ok(client)
}

pub async fn get_last_height_postgres(client: &Arc<Client>) -> Result<u64, Box<dyn std::error::Error>> {
    let row = client
        .query_one("SELECT COALESCE(MAX(block_height), 0) FROM events", &[])
        .await?;
    
    let height: i64 = row.get(0);
    Ok(height as u64)
}

pub async fn insert_rows_postgres(client: &Arc<Client>, rows: &[EventRow]) -> Result<(), Box<dyn std::error::Error>> {
    let rows_ref = rows;
    
    with_retry(
        || async { insert_rows_internal_postgres(client, rows_ref).await },
        5,
        |e| is_network_error(&e.to_string()),
        "postgres insert"
    ).await
}

async fn insert_rows_internal_postgres(client: &Arc<Client>, rows: &[EventRow]) -> Result<(), Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(());
    }

    // Use batch insert with multiple rows
    for chunk in rows.chunks(100) { // Process in chunks to avoid too many parameters
        let mut query = String::from("INSERT INTO events (
            block_height, block_timestamp, block_hash, contract_id, execution_status, 
            version, standard, index_in_log, event, data, related_receipt_id, 
            related_receipt_receiver_id, related_receipt_predecessor_id, tx_hash
        ) VALUES ");
        
        let mut param_idx = 1;
        let mut values_parts = Vec::new();
        
        for _ in 0..chunk.len() {
            values_parts.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                param_idx, param_idx + 1, param_idx + 2, param_idx + 3, param_idx + 4,
                param_idx + 5, param_idx + 6, param_idx + 7, param_idx + 8, param_idx + 9,
                param_idx + 10, param_idx + 11, param_idx + 12, param_idx + 13
            ));
            param_idx += 14;
        }
        
        query.push_str(&values_parts.join(", "));
        query.push_str(" ON CONFLICT (block_height, related_receipt_id, index_in_log) DO UPDATE SET 
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
            tx_hash = EXCLUDED.tx_hash");
        
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

/// Checks if PostgreSQL is available and functioning properly
pub async fn check_postgres_connection(client: &Arc<Client>) -> Result<(), Box<dyn std::error::Error>> {
    client.query_one("SELECT 1", &[]).await?;
    Ok(())
}
