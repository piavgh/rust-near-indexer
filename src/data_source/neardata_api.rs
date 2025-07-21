//! neardata-server API worker implementation
//!
//! This module implements a background worker that polls the neardata-server API
//! to fetch NEAR blockchain blocks sequentially and forwards them through a channel
//! to create a stream interface compatible with NEAR Lake Framework.

use near_lake_framework::near_indexer_primitives::StreamerMessage;
use reqwest::Client;
use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::data_source::NearDataApiConfig;

#[derive(Debug)]
pub enum NearDataApiError {
    Http(reqwest::Error),
    BlockNotFound(u64),
    Parse(String),
    RateLimit(String),
    Timeout,
}

impl fmt::Display for NearDataApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(e) => write!(f, "HTTP error: {}", e),
            Self::BlockNotFound(height) => write!(f, "Block {} not found", height),
            Self::Parse(msg) => write!(f, "Parse error: {}", msg),
            Self::RateLimit(msg) => write!(f, "Rate limit: {}", msg),
            Self::Timeout => write!(f, "Request timeout"),
        }
    }
}

impl Error for NearDataApiError {}

impl From<reqwest::Error> for NearDataApiError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            Self::Timeout
        } else {
            Self::Http(err)
        }
    }
}

impl NearDataApiError {
    pub fn is_block_not_found(&self) -> bool {
        matches!(self, Self::BlockNotFound(_))
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Http(_) | Self::Timeout | Self::RateLimit(_))
    }
}

/// Background worker that fetches blocks from neardata-server API
pub struct NearDataApiWorker {
    sender: mpsc::Sender<StreamerMessage>,
    client: Client,
    config: NearDataApiConfig,
    current_height: u64,
    rate_limiter: RateLimiter,
    retry_count: u32,
}

impl NearDataApiWorker {
    pub fn new(
        sender: mpsc::Sender<StreamerMessage>,
        config: NearDataApiConfig,
        start_block_height: u64,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .user_agent("near-intents-indexer/0.1.0")
            .build()?;

        let rate_limiter = RateLimiter::new(config.max_requests_per_second);

        Ok(Self {
            sender,
            client,
            config,
            current_height: start_block_height,
            rate_limiter,
            retry_count: 0,
        })
    }

    /// Run the worker loop to fetch blocks and send them through the channel
    pub async fn run(&mut self, shutdown_token: CancellationToken) {
        info!(
            "Starting neardata-server API worker from block {}",
            self.current_height
        );

        while !shutdown_token.is_cancelled() {
            // Check if receiver is still alive
            if self.sender.is_closed() {
                info!("Stream receiver closed, stopping worker");
                break;
            }

            match self.fetch_and_send_block().await {
                Ok(()) => {
                    self.current_height += 1;
                    self.retry_count = 0; // Reset retry count on success
                    debug!("Successfully processed block {}", self.current_height - 1);
                }
                Err(e) if e.is_block_not_found() => {
                    // We've caught up to the latest block, switch to polling mode
                    debug!(
                        "Block {} not found, caught up to latest. Polling for new blocks...",
                        self.current_height
                    );
                    sleep(Duration::from_millis(self.config.poll_interval_ms)).await;
                }
                Err(e) if e.is_retryable() && self.retry_count < self.config.max_retries => {
                    self.retry_count += 1;
                    let backoff_ms = 1000 * (2_u64.pow(self.retry_count - 1));
                    warn!(
                        "Retryable error for block {} (attempt {}): {}. Retrying in {}ms",
                        self.current_height, self.retry_count, e, backoff_ms
                    );
                    sleep(Duration::from_millis(backoff_ms)).await;
                }
                Err(e) => {
                    error!(
                        "Non-retryable error or max retries exceeded for block {}: {}",
                        self.current_height, e
                    );
                    self.retry_count = 0;
                    self.current_height += 1; // Skip this block and continue
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }

        info!("neardata-server API worker stopped");
    }

    async fn fetch_and_send_block(&mut self) -> Result<(), NearDataApiError> {
        // Apply rate limiting
        self.rate_limiter.wait().await;

        // Fetch the block from API
        let block = self.fetch_block(self.current_height).await?;

        // Send through channel
        if let Err(e) = self.sender.send(block).await {
            return match e {
                // Receiver dropped - this is expected during graceful shutdown
                _ if self.sender.is_closed() => {
                    debug!("Channel closed during shutdown, stopping block processing");
                    Ok(())
                }
                // Unexpected channel error - should be logged
                _ => {
                    error!("Unexpected channel send error for block {}: {:?}", self.current_height, e);
                    Err(NearDataApiError::Parse("Channel send failed".to_string()))
                }
            }
        }

        Ok(())
    }

    async fn fetch_block(&self, height: u64) -> Result<StreamerMessage, NearDataApiError> {
        let url = format!("{}/v0/block/{}", self.config.base_url, height);

        debug!("Fetching block {} from {}", height, url);

        let response = self.client.get(&url).send().await?;

        match response.status() {
            reqwest::StatusCode::OK => {
                let block: StreamerMessage = response
                    .json()
                    .await
                    .map_err(|e| NearDataApiError::Parse(e.to_string()))?;
                Ok(block)
            }
            reqwest::StatusCode::NOT_FOUND => Err(NearDataApiError::BlockNotFound(height)),
            reqwest::StatusCode::TOO_MANY_REQUESTS => Err(NearDataApiError::RateLimit(
                "API rate limit exceeded".to_string(),
            )),
            status => {
                let error_text = format!("Unexpected status code: {}", status);
                Err(NearDataApiError::Parse(error_text))
            }
        }
    }
}

/// Simple token bucket rate limiter
struct RateLimiter {
    tokens: f64,
    capacity: f64,
    refill_rate: f64, // tokens per second
    last_refill: Instant,
}

impl RateLimiter {
    fn new(requests_per_second: f64) -> Self {
        let capacity = requests_per_second; // Allow burst up to 1 second worth

        Self {
            tokens: capacity,
            capacity,
            refill_rate: requests_per_second,
            last_refill: Instant::now(),
        }
    }

    async fn wait(&mut self) {
        // Refill tokens based on elapsed time
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        let tokens_to_add = elapsed.as_secs_f64() * self.refill_rate;

        self.tokens = (self.tokens + tokens_to_add).min(self.capacity);
        self.last_refill = now;

        // If we don't have enough tokens, wait
        if self.tokens < 1.0 {
            let wait_time = Duration::from_secs_f64(1.0 / self.refill_rate);
            sleep(wait_time).await;
            self.tokens = 0.0; // We'll get one token after the wait
        } else {
            self.tokens -= 1.0;
        }
    }
}
