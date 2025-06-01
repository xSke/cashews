use std::{sync::{atomic::AtomicBool, Arc, Mutex, RwLock}, time::{Duration, Instant}};

use axum::{
    Router,
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use chron_base::{load_config};
use chron_db::{ChronDb, derived::PercentileStats};
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer},
};
use tracing::info;

mod chron_api;
mod derived_api;

#[derive(Clone)]
pub struct AppState {
    db: ChronDb,
    percentile_cache: Arc<RwLock<(Option<(Vec<PercentileStats>, Instant)>, bool)>>
}

pub struct AppError(anyhow::Error);

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = load_config()?;
    let db = ChronDb::new(&config).await?;

    let state = AppState {
        db,
        percentile_cache: Arc::new(RwLock::new((None, false)))
    };

    let cors = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_origin(Any);

    let trace = TraceLayer::new_for_http()
        .on_request(DefaultOnRequest::new())
        .on_response(DefaultOnResponse::new());

    let app = Router::new()
        // .route("/v0/game-events", get(api::get_game_events))
        // .route("/v0/events", get(api::get_events))
        .route("/chron/v0/entities", get(chron_api::get_entities))
        .route("/chron/v0/versions", get(chron_api::get_versions))
        .route("/games", get(derived_api::get_games))
        .route("/teams", get(derived_api::get_teams))
        .route("/leagues", get(derived_api::get_leagues))
        .route("/player-stats", get(derived_api::get_player_stats))
        .route("/league-aggregate-stats", get(derived_api::league_aggregate))
        // todo: is the order here right?
        .layer(cors)
        .layer(CompressionLayer::new())
        // .layer(trace)
        .with_state(state);

    let addr = "0.0.0.0:3001";
    info!("starting api at {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
