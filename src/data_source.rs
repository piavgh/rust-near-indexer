//! Data source abstraction for NEAR blockchain data
//!
//! This module provides a unified interface for accessing NEAR blockchain data
//! from different sources:
//! - NEAR Lake Framework (existing)
//! - neardata-server API (new alternative)

pub mod neardata_api;

use std::error::Error;

use near_lake_framework::LakeConfigBuilder;
use near_lake_framework::near_indexer_primitives::StreamerMessage;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::data_source::neardata_api::{NearDataApiConfig, NearDataApiWorker};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSourceType {
    LakeFramework,
    NeardataApi,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataSourceConfig {
    pub source_type: DataSourceType,
    pub neardata_api: Option<NearDataApiConfig>,
}

/// Create a stream of NEAR blockchain data from the configured data source
///
/// This function abstracts away the differences between NEAR Lake Framework and
/// neardata-server API, returning a unified stream interface.
pub async fn create_stream(
    config: &DataSourceConfig,
    start_block_height: u64,
    shutdown_token: CancellationToken,
    tracker: &TaskTracker,
) -> Result<ReceiverStream<StreamerMessage>, Box<dyn Error + Send + Sync>> {
    match config.source_type {
        DataSourceType::LakeFramework => {
            info!(
                "Creating NEAR Lake Framework stream from block {}",
                start_block_height
            );

            // Use existing NEAR Lake Framework logic
            let lake_config = LakeConfigBuilder::default()
                .mainnet()
                .start_block_height(start_block_height)
                .build()?;

            let (_, stream) = near_lake_framework::streamer(lake_config);
            Ok(ReceiverStream::new(stream))
        }

        DataSourceType::NeardataApi => {
            info!(
                "Creating neardata-server API stream from block {}",
                start_block_height
            );

            let api_config = config.neardata_api.as_ref().cloned().unwrap_or_default();

            // Create channel for communication between worker and stream
            let (sender, receiver) = mpsc::channel::<StreamerMessage>(100);

            // Create and start the background worker
            let mut worker = NearDataApiWorker::new(sender, api_config, start_block_height)?;

            // Spawn the worker task with proper tracking
            tracker.spawn(async move {
                worker.run(shutdown_token).await;
            });

            Ok(ReceiverStream::new(receiver))
        }
    }
}
