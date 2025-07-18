use crate::receipt_processor::ReceiptProcessor;
use crate::retry::{is_network_error, with_retry};
use crate::storage::StorageBackend;
use futures::StreamExt;
use near_lake_framework::LakeConfig;
use near_lake_framework::near_indexer_primitives::StreamerMessage;
use std::collections::HashMap;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub struct EventHandler {
    storage: Box<dyn StorageBackend>,
    receipt_processor: ReceiptProcessor,
}

impl EventHandler {
    pub fn new(storage: Box<dyn StorageBackend>, receipt_processor: ReceiptProcessor) -> Self {
        Self {
            storage,
            receipt_processor,
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
        let (events, swaps) = self
            .receipt_processor
            .process_receipt_outcomes(&message, &receipt_to_tx_mapping)
            .await;

        // Insert events into storage
        if !events.is_empty() {
            self.storage.insert_rows(&events).await?;
        }

        // Insert swaps into storage
        if !swaps.is_empty() {
            info!("Found {} swaps in block {}", swaps.len(), header.height);
            self.storage.insert_swaps(&swaps).await?;
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
        let mut cache_lock = self.receipt_processor.receipts_cache.lock().await;
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
}
