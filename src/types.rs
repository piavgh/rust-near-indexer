use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Debug)]
pub struct EventRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub block_hash: String,
    pub contract_id: String,
    pub execution_status: String,
    pub version: String,
    pub standard: String,
    pub index_in_log: u64,
    pub event: String,
    pub data: String,
    pub related_receipt_id: String,
    pub related_receipt_receiver_id: String,
    pub related_receipt_predecessor_id: String,
    pub tx_hash: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct SwapRow {
    pub intent_hash: String,
    pub origin_asset: String,
    pub destination_asset: String,
    pub amount_in: Decimal,
    pub amount_out: Decimal,
    pub withdrawal_fee: Decimal,
    pub recipient: String,
    pub tx_hash: Option<String>,
}

#[derive(Deserialize)]
pub struct EventJson {
    pub version: String,
    pub standard: String,
    pub event: String,
    pub data: Value,
}

// Asset-related types for asset worker
#[derive(Serialize, Debug, Clone)]
pub struct Asset {
    pub defuse_asset_identifier: String,
    pub near_token_id: String,
    pub intents_token_id: String,
    pub decimals: u8,
    pub asset_name: String,
    pub symbol: Option<String>,
    pub min_deposit_amount: Option<String>,
    pub min_withdrawal_amount: Option<String>,
    pub withdrawal_fee: Option<String>,
    pub standard: String,
    pub blockchain: String,
    pub price: Option<f64>,
    pub price_updated_at: Option<DateTime<Utc>>,
    pub contract_address: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SupportedToken {
    pub defuse_asset_identifier: String,
    pub near_token_id: String,
    pub intents_token_id: String,
    pub decimals: u8,
    pub asset_name: String,
    pub min_deposit_amount: Option<String>,
    pub min_withdrawal_amount: Option<String>,
    pub withdrawal_fee: Option<String>,
    pub standard: String,
}

#[derive(Deserialize, Debug)]
pub struct SupportedTokensResponse {
    pub result: SupportedTokensResult,
}

#[derive(Deserialize, Debug)]
pub struct SupportedTokensResult {
    pub tokens: Vec<SupportedToken>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TokenPrice {
    #[serde(rename = "assetId")]
    pub asset_id: String,
    pub symbol: String,
    pub blockchain: String,
    pub price: f64,
    #[serde(rename = "priceUpdatedAt")]
    pub price_updated_at: String,
    #[serde(rename = "contractAddress")]
    pub contract_address: Option<String>,
}
