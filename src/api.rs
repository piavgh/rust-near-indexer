use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::config::HTTPConfig;
use crate::storage::StorageBackend;
use crate::types::{EventRow, SwapRow};

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    pub tx_hash: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct EventsResponse {
    pub events: Vec<EventRow>,
    pub total: u32,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub struct ApiState {
    pub storage: Arc<dyn StorageBackend>,
}

pub async fn get_events(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<EventsQuery>,
) -> Result<Json<EventsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let limit = query.limit.unwrap_or(100).min(1000); // Cap at 1000 for performance
    let offset = query.offset.unwrap_or(0);

    info!(
        "Fetching events with tx_hash: {:?}, limit: {}, offset: {}",
        query.tx_hash, limit, offset
    );

    match state.storage.get_events(query.tx_hash, limit, offset).await {
        Ok((events, total)) => {
            info!("Successfully fetched {} events", events.len());
            Ok(Json(EventsResponse { events, total }))
        }
        Err(err) => {
            tracing::error!("Error fetching events: {}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to fetch events".to_string(),
                }),
            ))
        }
    }
}

pub async fn get_swap_by_intent_hash(
    State(state): State<Arc<ApiState>>,
    Path(intent_hash): Path<String>,
) -> Result<Json<SwapRow>, (StatusCode, Json<ErrorResponse>)> {
    info!("Fetching swap by intent_hash: {}", intent_hash);

    match state.storage.get_swap_by_intent_hash(&intent_hash).await {
        Ok(Some(swap)) => {
            info!("Successfully fetched swap for intent_hash: {}", intent_hash);
            Ok(Json(swap))
        }
        Ok(None) => {
            info!("No swap found for intent_hash: {}", intent_hash);
            Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "Swap not found".to_string(),
                }),
            ))
        }
        Err(err) => {
            tracing::error!("Error fetching swap: {}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to fetch swap".to_string(),
                }),
            ))
        }
    }
}

pub async fn health_check() -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}

pub fn create_router(storage: Arc<dyn StorageBackend>, http_config: &HTTPConfig) -> Router {
    let state = Arc::new(ApiState { storage });

    // Create health router
    let health_router = Router::new()
        .route("/ready", get(health_check))
        .route("/live", get(health_check))
        .with_state(state.clone());

    // Create events router
    let events_router = Router::new()
        .route("/", get(get_events))
        .with_state(state.clone());

    // Create swaps router
    let swaps_router = Router::new()
        .route("/{intent_hash}", get(get_swap_by_intent_hash))
        .with_state(state.clone());

    // Main router with nested routes
    let mut router = Router::new()
        .nest("/health", health_router)
        .nest("/events", events_router)
        .nest("/swaps", swaps_router)
        .layer(CorsLayer::permissive())
        .with_state(state);

    // Apply prefix if configured
    if !http_config.prefix.is_empty() && http_config.prefix != "/" {
        router = Router::new().nest(&http_config.prefix, router);
    }

    router
}
