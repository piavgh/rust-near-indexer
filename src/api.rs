use axum::{
    Router,
    extract::{Query, State},
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
use crate::types::EventRow;

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

pub async fn health_check() -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(serde_json::json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}

pub fn create_router(storage: Arc<dyn StorageBackend>, http_config: &HTTPConfig) -> Router {
    let state = Arc::new(ApiState { storage });

    let mut router = Router::new()
        .route("/events", get(get_events))
        .route("/health", get(health_check))
        .layer(CorsLayer::permissive())
        .with_state(state);

    // Apply prefix if configured
    if !http_config.prefix.is_empty() && http_config.prefix != "/" {
        router = Router::new().nest(&http_config.prefix, router);
    }

    router
}
