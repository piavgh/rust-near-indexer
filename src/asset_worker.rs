use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::asset_manager::AssetManager;
use crate::retry::with_retry;
use crate::types::{Asset, SupportedToken, SupportedTokensResponse, TokenPrice};

#[derive(Debug, Clone, Deserialize)]
pub struct AssetWorkerConfig {
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub max_retries: usize,
    pub mm_supported_tokens_url: String,
    pub intents_supported_tokens_url: String,
}

/// AssetWorker handles the periodic fetching and processing of asset data
pub struct AssetWorker {
    asset_manager: Arc<AssetManager>,
    config: AssetWorkerConfig,
    client: reqwest::Client,
}

impl AssetWorker {
    pub fn new(asset_manager: Arc<AssetManager>, config: AssetWorkerConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            asset_manager,
            config,
            client,
        }
    }

    pub async fn start(&self, shutdown_token: CancellationToken) {
        info!(
            "Starting asset worker with interval: {:?}",
            self.config.interval
        );

        // Load existing assets from database into memory on startup
        if let Err(e) = self.asset_manager.load_assets_from_database().await {
            error!("Failed to load assets from database on startup: {}", e);
            // Continue anyway - we'll populate from API calls
        }

        let mut interval_timer = interval(Duration::from(self.config.interval));

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    if let Err(e) = self.fetch_and_store_assets().await {
                        error!("Error fetching and storing assets: {}", e);
                    }
                }
                _ = shutdown_token.cancelled() => {
                    info!("Asset worker received shutdown signal");
                    break;
                }
            }
        }

        // Flush all assets from memory to database during graceful shutdown
        info!("Flushing assets to database during graceful shutdown...");
        if let Err(e) = self.asset_manager.flush_assets_to_database().await {
            error!("Failed to flush assets to database during shutdown: {}", e);
        }

        info!("Asset worker shutdown complete");
    }

    async fn fetch_and_store_assets(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Fetching supported tokens and prices...");

        // Fetch data from both APIs concurrently
        let (supported_tokens, token_prices) = tokio::try_join!(
            self.fetch_mm_supported_tokens(),
            self.fetch_intents_supported_token()
        )?;

        // Create a map of intents_token_id -> TokenPrice for easy lookup
        let price_map: HashMap<String, &TokenPrice> = token_prices
            .iter()
            .map(|price| (price.asset_id.clone(), price))
            .collect();

        // Combine the data into Asset structs
        let assets: Vec<Asset> = supported_tokens
            .into_iter()
            .map(|token| {
                let price_info = price_map.get(&token.intents_token_id);

                Asset {
                    defuse_asset_identifier: token.defuse_asset_identifier,
                    near_token_id: token.near_token_id.clone(),
                    intents_token_id: token.intents_token_id,
                    decimals: token.decimals,
                    asset_name: token.asset_name,
                    symbol: price_info.map(|p| p.symbol.clone()),
                    min_deposit_amount: token.min_deposit_amount,
                    min_withdrawal_amount: token.min_withdrawal_amount,
                    withdrawal_fee: token.withdrawal_fee,
                    standard: token.standard,
                    blockchain: price_info
                        .map(|p| p.blockchain.clone())
                        .unwrap_or_else(|| "near".to_string()),
                    price: price_info.map(|p| p.price),
                    price_updated_at: price_info.and_then(|p| {
                        DateTime::parse_from_rfc3339(&p.price_updated_at)
                            .ok()
                            .map(|dt| dt.with_timezone(&Utc))
                    }),
                    contract_address: price_info
                        .and_then(|p| p.contract_address.clone())
                        .unwrap_or_else(|| token.near_token_id),
                }
            })
            .collect();

        // Update assets through the asset manager
        self.asset_manager.update_assets(assets).await;

        info!("Asset data updated successfully in memory");
        Ok(())
    }

    async fn fetch_mm_supported_tokens(
        &self,
    ) -> Result<Vec<SupportedToken>, Box<dyn std::error::Error + Send + Sync>> {
        let request_body = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "supported_tokens",
            "params": []
        });

        with_retry(
            || async {
                let response = self
                    .client
                    .post(&self.config.mm_supported_tokens_url)
                    .header("Content-Type", "application/json")
                    .json(&request_body)
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(format!("HTTP error: {}", response.status()).into());
                }

                let supported_tokens_response: SupportedTokensResponse = response.json().await?;
                Ok(supported_tokens_response.result.tokens)
            },
            self.config.max_retries,
            |e: &Box<dyn std::error::Error + Send + Sync>| {
                // Retry on network errors and HTTP 5xx errors
                e.to_string().contains("timeout")
                    || e.to_string().contains("connection")
                    || e.to_string().contains("HTTP error: 5")
            },
            "fetch MM supported tokens",
        )
        .await
    }

    async fn fetch_intents_supported_token(
        &self,
    ) -> Result<Vec<TokenPrice>, Box<dyn std::error::Error + Send + Sync>> {
        with_retry(
            || async {
                let response = self
                    .client
                    .get(&self.config.intents_supported_tokens_url)
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(format!("HTTP error: {}", response.status()).into());
                }

                let token_prices: Vec<TokenPrice> = response.json().await?;
                Ok(token_prices)
            },
            self.config.max_retries,
            |e: &Box<dyn std::error::Error + Send + Sync>| {
                // Retry on network errors and HTTP 5xx errors
                e.to_string().contains("timeout")
                    || e.to_string().contains("connection")
                    || e.to_string().contains("HTTP error: 5")
            },
            "fetch Intents supported tokens",
        )
        .await
    }
}
