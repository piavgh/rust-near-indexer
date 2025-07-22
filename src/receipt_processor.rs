use std::collections::HashMap;
use std::str::FromStr;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use near_lake_framework::near_indexer_primitives::{
    IndexerExecutionOutcomeWithReceipt, StreamerMessage, views::BlockHeaderView,
    views::ExecutionStatusView, views::ReceiptEnumView,
};
use rust_decimal::Decimal;
use serde_json::from_str;
use tracing::{info, warn};

use crate::asset_manager::AssetManager;
use crate::cache::ReceiptOrDataId;
use crate::cache::receipts_cache::ReceiptsCacheArc;
use crate::types::{EventJson, EventRow, SwapRow};

const TRACKING_CONTRACT: &str = "intents.near";
const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";

#[derive(Default)]
struct SwapData {
    account_id: Option<String>,
    intent_hash: Option<String>,
    origin_asset: Option<String>,
    destination_asset: Option<String>,
    amount_in_partial: Option<String>, // Amount from diff (without affiliate fee)
    amount_out: Option<String>,
    recipient: Option<String>,
    affiliate_fee: Option<String>, // Fee from transfer event
}

impl SwapData {
    fn is_complete(&self) -> bool {
        self.intent_hash.is_some()
            && self.origin_asset.is_some()
            && self.destination_asset.is_some()
            && self.amount_in_partial.is_some()
            && self.amount_out.is_some()
            && self.recipient.is_some()
    }

    fn calculate_final_amount_in(&self) -> String {
        if let (Some(amount_in_partial), Some(affiliate_fee)) =
            (&self.amount_in_partial, &self.affiliate_fee)
        {
            // Parse both values as u128 and add them
            let partial = amount_in_partial.parse::<u128>().unwrap_or(0);
            let fee = affiliate_fee.parse::<u128>().unwrap_or(0);
            (partial + fee).to_string()
        } else {
            self.amount_in_partial.clone().unwrap_or_default()
        }
    }
}

pub struct ReceiptProcessor {
    pub receipts_cache: ReceiptsCacheArc,
    pub asset_manager: std::sync::Arc<AssetManager>,
}

impl ReceiptProcessor {
    pub fn new(
        receipts_cache: ReceiptsCacheArc,
        asset_manager: std::sync::Arc<AssetManager>,
    ) -> Self {
        Self {
            receipts_cache,
            asset_manager,
        }
    }

    pub async fn process_receipt_outcomes(
        &self,
        message: &StreamerMessage,
        receipt_to_tx_mapping: &HashMap<String, String>,
    ) -> (Vec<EventRow>, Vec<SwapRow>) {
        let mut all_events = Vec::new();
        let mut all_swaps = Vec::new();

        for shard in &message.shards {
            let (shard_events, shard_swaps) = self
                .process_shard_outcomes(shard, &message.block.header, receipt_to_tx_mapping)
                .await;
            all_events.extend(shard_events);
            all_swaps.extend(shard_swaps);
        }

        (all_events, all_swaps)
    }

    async fn process_shard_outcomes(
        &self,
        shard: &near_lake_framework::near_indexer_primitives::IndexerShard,
        header: &BlockHeaderView,
        receipt_to_tx_mapping: &HashMap<String, String>,
    ) -> (Vec<EventRow>, Vec<SwapRow>) {
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

                    // Process the entire outcome
                    self.process_outcome(&outcome, tx_hash, &header).await
                }
            })
            .collect();

        let mut all_events = Vec::new();
        let mut all_swaps = Vec::new();
        let results = futures.collect::<Vec<_>>().await;
        for (events, swaps) in results {
            all_events.extend(events);
            all_swaps.extend(swaps);
        }

        (all_events, all_swaps)
    }

    async fn process_outcome(
        &self,
        outcome: &IndexerExecutionOutcomeWithReceipt,
        tx_hash: String,
        header: &BlockHeaderView,
    ) -> (Vec<EventRow>, Vec<SwapRow>) {
        let logs = &outcome.execution_outcome.outcome.logs;
        let mut events = Vec::new();
        let mut swaps = Vec::new();

        // First, scan through all logs to find token_diff events with referral
        let mut swap_data_candidates: Vec<SwapData> = Vec::new();

        for log in logs.iter() {
            let log_trimmed = log.trim();
            if log_trimmed.starts_with(EVENT_JSON_PREFIX) {
                if let Ok(event_json) =
                    from_str::<serde_json::Value>(&log_trimmed[EVENT_JSON_PREFIX.len()..])
                {
                    if event_json.get("event").and_then(|e| e.as_str()) == Some("token_diff") {
                        if let Some(data_array) = event_json.get("data").and_then(|d| d.as_array())
                        {
                            if let Some(first_data) = data_array.get(0) {
                                // Check for referral field
                                if first_data.get("referral").and_then(|r| r.as_str())
                                    == Some("1click-kyberswap")
                                {
                                    let mut swap_data = SwapData::default();

                                    // Extract account_id
                                    swap_data.account_id = first_data
                                        .get("account_id")
                                        .and_then(|a| a.as_str())
                                        .map(|s| s.to_string());

                                    // Extract intent_hash
                                    swap_data.intent_hash = first_data
                                        .get("intent_hash")
                                        .and_then(|h| h.as_str())
                                        .map(|s| s.to_string());

                                    // Extract positive and negative values from diff
                                    if let Some(diff) =
                                        first_data.get("diff").and_then(|d| d.as_object())
                                    {
                                        for (token, value) in diff {
                                            if let Some(val_str) = value.as_str() {
                                                if val_str.starts_with('-') {
                                                    swap_data.origin_asset = Some(token.clone());
                                                    swap_data.amount_in_partial = Some(
                                                        val_str.trim_start_matches('-').to_string(),
                                                    );
                                                } else {
                                                    swap_data.destination_asset =
                                                        Some(token.clone());
                                                    swap_data.amount_out =
                                                        Some(val_str.to_string());
                                                }
                                            }
                                        }
                                    }

                                    swap_data_candidates.push(swap_data);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Now look for matching transfer events for each swap candidate
        for mut swap_data in swap_data_candidates {
            if swap_data.account_id.is_none() {
                continue;
            }

            // Scan through all logs to find matching transfer and ft_withdraw events
            for log in logs.iter() {
                let log_trimmed = log.trim();
                if log_trimmed.starts_with(EVENT_JSON_PREFIX) {
                    if let Ok(event_json) =
                        from_str::<serde_json::Value>(&log_trimmed[EVENT_JSON_PREFIX.len()..])
                    {
                        let event_type = event_json.get("event").and_then(|e| e.as_str());

                        // Look for transfer event to get affiliate fee
                        if event_type == Some("transfer") {
                            if let Some(data_array) =
                                event_json.get("data").and_then(|d| d.as_array())
                            {
                                if let Some(first_data) = data_array.get(0) {
                                    // Check if account_id matches
                                    if first_data.get("account_id").and_then(|a| a.as_str())
                                        == swap_data.account_id.as_deref()
                                    {
                                        // Extract affiliate fee from tokens
                                        if let Some(tokens) =
                                            first_data.get("tokens").and_then(|t| t.as_object())
                                        {
                                            // Get the first token value (should be the affiliate fee)
                                            if let Some((_, value)) = tokens.iter().next() {
                                                if let Some(fee_str) = value.as_str() {
                                                    swap_data.affiliate_fee =
                                                        Some(fee_str.to_string());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Look for ft_withdraw event to get recipient
                        if event_type == Some("ft_withdraw") {
                            if let Some(data_array) =
                                event_json.get("data").and_then(|d| d.as_array())
                            {
                                if let Some(first_data) = data_array.get(0) {
                                    // Check if account_id matches
                                    if first_data.get("account_id").and_then(|a| a.as_str())
                                        == swap_data.account_id.as_deref()
                                    {
                                        // Extract recipient from memo field
                                        if let Some(memo) =
                                            first_data.get("memo").and_then(|m| m.as_str())
                                        {
                                            if let Some(recipient_part) =
                                                memo.strip_prefix("WITHDRAW_TO:")
                                            {
                                                swap_data.recipient =
                                                    Some(recipient_part.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Create swap record if we have all required data
            if swap_data.is_complete() {
                // Calculate amount_in before consuming swap_data
                let amount_in_str = swap_data.calculate_final_amount_in();
                let amount_out_str = swap_data.amount_out.unwrap_or_default();

                // Parse amounts as Decimal, fallback to 0 if parsing fails
                let amount_in_decimal = Decimal::from_str(&amount_in_str).unwrap_or_else(|_| {
                    warn!("Failed to parse amount_in '{}', using 0", amount_in_str);
                    Decimal::ZERO
                });
                let amount_out_decimal = Decimal::from_str(&amount_out_str).unwrap_or_else(|_| {
                    warn!("Failed to parse amount_out '{}', using 0", amount_out_str);
                    Decimal::ZERO
                });

                // Get withdrawal fee from asset manager using destination_asset (intents_token_id)
                let destination_asset = swap_data.destination_asset.clone().unwrap_or_default();
                let withdrawal_fee_str = self
                    .asset_manager
                    .get_withdrawal_fee(&destination_asset)
                    .await
                    .unwrap_or_else(|| "0".to_string());

                let withdrawal_fee_decimal =
                    Decimal::from_str(&withdrawal_fee_str).unwrap_or_else(|_| {
                        warn!(
                            "Failed to parse withdrawal_fee '{}', using 0",
                            withdrawal_fee_str
                        );
                        Decimal::ZERO
                    });

                swaps.push(SwapRow {
                    intent_hash: swap_data.intent_hash.unwrap_or_default(),
                    origin_asset: swap_data.origin_asset.unwrap_or_default(),
                    destination_asset,
                    amount_in: amount_in_decimal,
                    amount_out: amount_out_decimal,
                    withdrawal_fee: withdrawal_fee_decimal,
                    recipient: swap_data.recipient.unwrap_or_default(),
                    tx_hash: Some(tx_hash.clone()),
                });
            }
        }

        // Process all logs and create events
        for (index_in_log, log) in logs.iter().enumerate() {
            if let Some(event_row) =
                self.parse_event(index_in_log, log, tx_hash.clone(), outcome, header)
            {
                events.push(event_row);
            }
        }

        (events, swaps)
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
