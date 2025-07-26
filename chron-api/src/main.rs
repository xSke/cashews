use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use chron_base::{ChronConfig, cache::SwrCache2, load_config, stop_signal};
use chron_db::ChronDb;
use derived_api::{LeagueAggregateResponse, refresh_league_aggregate};
// use polars::enable_string_cache;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    services::ServeDir,
    trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer},
};
use tracing::info;

mod chron_api;
mod derived_api;
mod stats;

#[derive(Clone)]
pub struct AppState {
    config: Arc<ChronConfig>,
    db: ChronDb,
    percentile_cache: SwrCache2<(), Vec<LeagueAggregateResponse>, AppState>,
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // enable_string_cache();

    let config = load_config()?;
    let db = ChronDb::new(&config).await?;

    let state = AppState {
        db,
        percentile_cache: SwrCache2::new(Duration::from_secs(60 * 10), 10, move |_, ctx| {
            refresh_league_aggregate(ctx)
        }),
        config: Arc::new(config),
    };
    state.percentile_cache.set_context(state.clone());

    let cors = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_origin(Any);

    let trace = TraceLayer::new_for_http()
        .on_request(DefaultOnRequest::new())
        .on_response(DefaultOnResponse::new());

    let mut app = Router::new()
        .route("/chron/v0/entities", get(chron_api::get_entities))
        .route("/chron/v0/versions", get(chron_api::get_versions))
        .route("/games", get(derived_api::get_games))
        .route("/teams", get(derived_api::get_teams))
        .route("/leagues", get(derived_api::get_leagues))
        .route("/player-stats", get(derived_api::get_player_stats))
        .route("/scorigami", get(derived_api::scorigami))
        .route("/locations", get(derived_api::locations))
        .route("/stats", get(stats::stats));

    if let Some(dir) = &state.config.export_path {
        dbg!(dir);
        app = app.nest_service("/export", ServeDir::new(dir));
    }

    let app = app
        .layer(cors)
        .layer(CompressionLayer::new())
        .layer(trace)
        // .layer(TimeoutLayer::new(Duration::from_secs(10)))
        // .layer(ResponseBodyTimeoutLayer::new(Duration::from_secs(10)))
        .with_state(state);

    let addr = "0.0.0.0:3001";
    info!("starting api at {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let serve_fut = axum::serve(listener, app);
    let ctrlc_fut = stop_signal();
    tokio::select! {
        res = serve_fut => res?,
        _ = ctrlc_fut => {}
    };

    Ok(())
}
