use crate::cache::ReceiptOrDataId;
use crate::cache::receipts_cache::ReceiptsCacheArc;
use crate::database::StorageBackend;
use crate::retry::{is_network_error, with_retry};
use crate::types::{EventJson, EventRow};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use near_lake_framework::LakeConfig;
use near_lake_framework::near_indexer_primitives::{
    self, IndexerExecutionOutcomeWithReceipt, StreamerMessage, views::BlockHeaderView,
    views::ExecutionStatusView, views::ReceiptEnumView,
};
use serde_json::from_str;
use std::collections::HashMap;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

const TRACKING_CONTRACT: &str = "intents.near";
const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";

pub struct EventHandler {
    storage: Box<dyn StorageBackend>,
    receipts_cache: ReceiptsCacheArc,
}

impl EventHandler {
    pub fn new(storage: Box<dyn StorageBackend>, receipts_cache: ReceiptsCacheArc) -> Self {
        Self {
            storage,
            receipts_cache,
        }
    }

    pub async fn handle_stream(&self, config: LakeConfig, shutdown_token: CancellationToken) {
        info!("Starting stream processing...");

        let (_, stream) = near_lake_framework::streamer(config);
        let mut stream = ReceiverStream::new(stream);

        loop {
            tokio::select! {
                message = stream.next() => {
                    match message {
                        Some(msg) => {
                            let block_height = msg.block.header.height;

                            if let Err(e) = with_retry(
                                || async { self.handle_streamer_message(msg.clone()).await },
                                3,
                                |e| is_network_error(&e.to_string()),
                                &format!("process block {}", block_height)
                            ).await {
                                error!("Failed to process block {}: {}", block_height, e);
                            }
                        }
                        None => {
                            info!("Stream ended");
                            break;
                        }
                    }
                }
                _ = shutdown_token.cancelled() => {
                    info!("Received shutdown signal, stopping stream processing");
                    break;
                }
            }
        }
    }

    async fn handle_streamer_message(
        &self,
        message: StreamerMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let header = &message.block.header;
        info!("Processing block {}", header.height);

        // Build local receipt-to-tx mapping for this block
        let receipt_to_tx_mapping = self.build_receipt_to_tx_mapping(&message);

        // Update cache with transaction data
        self.update_cache_with_transactions(&message).await;

        // Process all receipt outcomes concurrently
        let events = self
            .process_receipt_outcomes(&message, &receipt_to_tx_mapping)
            .await;

        // Insert events into storage
        if !events.is_empty() {
            self.storage.insert_rows(&events).await?;
        }

        Ok(())
    }

    fn build_receipt_to_tx_mapping(&self, message: &StreamerMessage) -> HashMap<String, String> {
        let mut mapping = HashMap::new();

        for shard in &message.shards {
            if let Some(chunk) = &shard.chunk {
                for tx in &chunk.transactions {
                    let tx_hash = tx.transaction.hash.to_string();
                    for receipt_id in &tx.outcome.execution_outcome.outcome.receipt_ids {
                        mapping.insert(receipt_id.to_string(), tx_hash.clone());
                    }
                }
            }
        }

        mapping
    }

    async fn update_cache_with_transactions(&self, message: &StreamerMessage) {
        let mut cache_lock = self.receipts_cache.lock().await;
        cache_lock.update_block_height(message.block.header.height);

        for shard in &message.shards {
            if let Some(chunk) = &shard.chunk {
                for tx in &chunk.transactions {
                    cache_lock
                        .cache_tx_hash_trace(
                            &tx.transaction.signer_id.to_string(),
                            &tx.transaction.public_key.to_string(),
                            message.block.header.height,
                            tx.transaction.hash.to_string(),
                        )
                        .await;
                }
            }
        }
    }

    async fn process_receipt_outcomes(
        &self,
        message: &StreamerMessage,
        receipt_to_tx_mapping: &HashMap<String, String>,
    ) -> Vec<EventRow> {
        let mut all_events = Vec::new();

        for shard in &message.shards {
            let shard_events = self
                .process_shard_outcomes(shard, &message.block.header, receipt_to_tx_mapping)
                .await;
            all_events.extend(shard_events);
        }

        all_events
    }

    async fn process_shard_outcomes(
        &self,
        shard: &near_indexer_primitives::IndexerShard,
        header: &BlockHeaderView,
        receipt_to_tx_mapping: &HashMap<String, String>,
    ) -> Vec<EventRow> {
        let futures: FuturesUnordered<_> = shard
            .receipt_execution_outcomes
            .iter()
            .map(|outcome| {
                let header = header.clone();
                let outcome = outcome.clone();
                let receipt_to_tx_mapping = receipt_to_tx_mapping.clone();

                async move {
                    let tx_hash = self
                        .resolve_transaction_hash(&outcome, &receipt_to_tx_mapping, &header)
                        .await;

                    outcome
                        .execution_outcome
                        .outcome
                        .logs
                        .iter()
                        .enumerate()
                        .filter_map(|(index_in_log, log)| {
                            self.parse_event(index_in_log, log, tx_hash.clone(), &outcome, &header)
                        })
                        .collect::<Vec<_>>()
                }
            })
            .collect();

        let mut all_events = Vec::new();
        let event_rows = futures.collect::<Vec<_>>().await;
        for row_vec in event_rows {
            all_events.extend(row_vec);
        }

        all_events
    }

    async fn resolve_transaction_hash(
        &self,
        outcome: &IndexerExecutionOutcomeWithReceipt,
        local_mapping: &HashMap<String, String>,
        header: &BlockHeaderView,
    ) -> String {
        if outcome.receipt.predecessor_id == "system" {
            return String::new();
        }

        let receipt_id_str = outcome.receipt.receipt_id.to_string();

        // Try local mapping first
        if let Some(tx_hash) = local_mapping.get(&receipt_id_str) {
            return tx_hash.clone();
        }

        // Fall back to cache
        let mut cache_lock = self.receipts_cache.lock().await;
        let receipt_id = ReceiptOrDataId::ReceiptId(outcome.receipt.receipt_id);

        match cache_lock.cache_get(&receipt_id) {
            Some(hash) => hash.clone(),
            None => {
                self.trace_transaction_hash(outcome, &mut cache_lock, header)
                    .await
            }
        }
    }

    async fn trace_transaction_hash(
        &self,
        outcome: &IndexerExecutionOutcomeWithReceipt,
        cache_lock: &mut tokio::sync::MutexGuard<'_, crate::cache::receipts_cache::ReceiptsCache>,
        header: &BlockHeaderView,
    ) -> String {
        if let ReceiptEnumView::Action {
            signer_id,
            signer_public_key,
            ..
        } = &outcome.receipt.receipt
        {
            if let Some(tx_hash) = cache_lock
                .trace_tx_hash(
                    &signer_id.to_string(),
                    &signer_public_key.to_string(),
                    header.height,
                )
                .await
            {
                let receipt_id = ReceiptOrDataId::ReceiptId(outcome.receipt.receipt_id);
                cache_lock.cache_set(receipt_id, tx_hash.clone());
                tx_hash
            } else {
                String::new()
            }
        } else {
            String::new()
        }
    }

    fn parse_event(
        &self,
        index_in_log: usize,
        log: &str,
        tx_hash: String,
        outcome: &IndexerExecutionOutcomeWithReceipt,
        header: &BlockHeaderView,
    ) -> Option<EventRow> {
        let log_trimmed = log.trim();

        if log_trimmed.starts_with(EVENT_JSON_PREFIX) {
            if let Ok(event) = from_str::<EventJson>(&log_trimmed[EVENT_JSON_PREFIX.len()..]) {
                let contract_id = &outcome.execution_outcome.outcome.executor_id.to_string();
                if contract_id == TRACKING_CONTRACT {
                    if log_trimmed.contains("dip4") || log_trimmed.contains("nep245") {
                        info!("Event: {}", log_trimmed);
                        return Some(EventRow {
                            block_height: header.height,
                            block_timestamp: header.timestamp,
                            block_hash: header.hash.to_string(),
                            contract_id: contract_id.to_string(),
                            execution_status: self
                                .parse_status(outcome.execution_outcome.outcome.status.clone()),
                            version: event.version,
                            standard: event.standard,
                            index_in_log: index_in_log as u64,
                            event: event.event,
                            data: event.data.to_string(),
                            related_receipt_id: outcome.receipt.receipt_id.to_string(),
                            related_receipt_receiver_id: outcome.receipt.receiver_id.to_string(),
                            related_receipt_predecessor_id: outcome
                                .receipt
                                .predecessor_id
                                .to_string(),
                            tx_hash: Some(tx_hash),
                        });
                    }
                }
            }
        }
        None
    }

    fn parse_status(&self, status: ExecutionStatusView) -> String {
        match status {
            ExecutionStatusView::SuccessReceiptId(_) => "success_receipt_id".to_string(),
            ExecutionStatusView::SuccessValue(_) => "success_value".to_string(),
            ExecutionStatusView::Unknown => "unknown".to_string(),
            ExecutionStatusView::Failure(_) => "failure".to_string(),
        }
    }
}
