use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::info;

use crate::storage::StorageBackend;
use crate::types::Asset;

/// AssetManager handles the in-memory storage and persistence of asset data
pub struct AssetManager {
    storage: Arc<dyn StorageBackend + Send + Sync>,
    // In-memory storage for assets, keyed by defuse_asset_identifier
    assets_cache: Arc<RwLock<HashMap<String, Asset>>>,
}

impl AssetManager {
    pub fn new(storage: Arc<dyn StorageBackend + Send + Sync>) -> Self {
        Self {
            storage,
            assets_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if the asset manager has data loaded in memory
    pub async fn has_data(&self) -> bool {
        let cache = self.assets_cache.read().await;
        !cache.is_empty()
    }

    /// Get withdrawal fee for a specific asset by intents_token_id
    pub async fn get_withdrawal_fee(&self, intents_token_id: &str) -> Option<String> {
        let cache = self.assets_cache.read().await;
        cache
            .values()
            .find(|asset| asset.intents_token_id == intents_token_id)
            .and_then(|asset| asset.withdrawal_fee.clone())
    }

    /// Update assets in memory cache
    pub async fn update_assets(&self, assets: Vec<Asset>) {
        let mut cache = self.assets_cache.write().await;
        for asset in assets {
            cache.insert(asset.defuse_asset_identifier.clone(), asset);
        }
        info!("Updated {} assets in memory cache", cache.len());
    }

    /// Load all existing assets from database into memory on startup
    pub async fn load_assets_from_database(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Loading assets from database into memory...");

        let assets = self.storage.get_all_assets().await?;
        let mut cache = self.assets_cache.write().await;

        for asset in assets {
            cache.insert(asset.defuse_asset_identifier.clone(), asset);
        }

        info!("Loaded {} assets from database into memory", cache.len());
        Ok(())
    }

    /// Flush all assets from memory to database (called during graceful shutdown)
    pub async fn flush_assets_to_database(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Flushing assets from memory to database...");

        let cache = self.assets_cache.read().await;
        let assets: Vec<Asset> = cache.values().cloned().collect();

        if assets.is_empty() {
            info!("No assets to flush to database");
            return Ok(());
        }

        info!("Flushing {} assets to database", assets.len());
        self.storage.insert_assets(&assets).await?;

        info!("Successfully flushed assets to database");
        Ok(())
    }
}
