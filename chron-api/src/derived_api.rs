use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use axum::{
    Json,
    extract::{Query, State},
};
use chron_db::{
    derived::{DbGame, DbGamePlayerStats, DbLeague, DbTeam, PercentileStats},
    models::PageToken,
    queries::{PaginatedResult, SortOrder},
};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use tracing::{error, info};

use crate::{AppError, AppState};

#[derive(Deserialize, Debug)]
pub struct GetGamesQuery {
    pub season: i32,
    pub day: Option<i32>,
    pub team: Option<String>,

    #[serde(default)]
    order: SortOrder,
    #[serde(default)]
    count: Option<u64>,
    page: Option<PageToken>,
}

pub async fn get_games(
    State(ctx): State<AppState>,
    Query(q): Query<GetGamesQuery>,
) -> Result<Json<PaginatedResult<DbGame>>, AppError> {
    let games = ctx
        .db
        .get_games(chron_db::derived::GetGamesQuery {
            season: Some(q.season),
            day: q.day,
            team: q.team,

            order: q.order,
            count: q.count.unwrap_or(1000),
            page: q.page,
        })
        .await?;

    Ok(Json(games))
}

#[derive(Deserialize, Debug)]
pub struct GetTeamsQuery {}

pub async fn get_teams(
    State(ctx): State<AppState>,
    Query(_q): Query<GetTeamsQuery>,
) -> Result<Json<PaginatedResult<DbTeam>>, AppError> {
    let teams = ctx.db.get_teams().await?;

    Ok(Json(fake_paginate(teams)))
}

#[derive(Deserialize, Debug)]
pub struct GetLeaguesQuery {}

pub async fn get_leagues(
    State(ctx): State<AppState>,
    Query(_q): Query<GetTeamsQuery>,
) -> Result<Json<PaginatedResult<DbLeague>>, AppError> {
    let teams = ctx.db.get_leagues().await?;

    Ok(Json(fake_paginate(teams)))
}

#[derive(Deserialize, Debug)]
pub struct GetPlayerStatsQuery {
    pub start: Option<i32>,
    pub end: Option<i32>,

    pub player: Option<String>,
    pub team: Option<String>,
}

pub async fn get_player_stats(
    State(ctx): State<AppState>,
    Query(q): Query<GetPlayerStatsQuery>,
) -> Result<Json<Vec<ApiPlayerStats>>, AppError> {
    if q.player.is_none() && q.team.is_none() {
        return Err(anyhow::anyhow!("must include either player or team id").into());
    }

    let stats = ctx
        .db
        .get_player_stats(chron_db::derived::GetPlayerStatsQuery {
            start: q.start.map(|x| (0, x)),
            end: q.end.map(|x| (0, x)),
            player: q.player,
            team: q.team,
        })
        .await?;

    Ok(Json(aggregate_player_stats(&stats)))
}

pub async fn league_aggregate(
    State(ctx): State<AppState>,
) -> Result<Json<Vec<PercentileStats>>, AppError> {
    // this caching code is stupid, redo with a proper lib at some point...
    let (data, should_recalc) = {
        let _lock = ctx.percentile_cache.read().unwrap();

        if let Some((ref data, ref expiry)) = (*_lock).0 {
            (data.clone(), Instant::now() > *expiry)
        } else {
            (Vec::new(), true)
        }
    };

    if should_recalc {
        let should_spawn = {
            let mut _lock = ctx.percentile_cache.write().unwrap();
            if !_lock.1 {
                _lock.1 = true;
                true
            } else {
                false
            }
        };
        if should_spawn {
            tokio::spawn(async move {
                if let Err(e) = refresh_league_aggregate(&ctx).await {
                    error!("error refreshing league aggregates: {}", e);
                }

                let mut _lock = ctx.percentile_cache.write().unwrap();
                _lock.1 = false;
            });
        }
    }

    Ok(Json(data))
}

#[derive(FromRow, Serialize)]
pub struct ScorigamiEntry {
    min: i32,
    max: i32,
    count: i32,
    first: String,
}

pub async fn scorigami(State(ctx): State<AppState>) -> Result<Json<Vec<ScorigamiEntry>>, AppError> {
    let r = fetch_scorigami(&ctx).await?;
    Ok(Json(r))
}

async fn fetch_scorigami(ctx: &AppState) -> anyhow::Result<Vec<ScorigamiEntry>> {
    // inline sql here is a bit nasty but we ball
    let r = sqlx::query_as(r"with games2 as (select least((last_update->>'home_score')::int, (last_update->>'away_score')::int) as min, greatest((last_update->>'home_score')::int, (last_update->>'away_score')::int) as max, game_id from games where state = 'Complete') select min(game_id) as first, min, max, count(*)::int as count from games2 group by (min, max);")
    .fetch_all(&ctx.db.pool)
    .await?;
    Ok(r)
}

async fn refresh_league_aggregate(ctx: &AppState) -> anyhow::Result<()> {
    info!("refreshing league aggregates");
    let percentiles = [0.05, 0.2, 0.35, 0.5, 0.65, 0.8, 0.95];
    let res = ctx.db.get_league_percentiles(&percentiles).await?;

    let expiry = Instant::now() + Duration::from_secs(5);
    let mut lock = ctx.percentile_cache.write().unwrap();
    lock.0 = Some((res, expiry));

    Ok(())
}

#[derive(Serialize, Debug)]
pub struct ApiPlayerStats {
    player_id: String,
    team_id: String,
    stats: BTreeMap<String, i32>,
}

fn aggregate_player_stats(source: &[DbGamePlayerStats]) -> Vec<ApiPlayerStats> {
    // team -> player -> stat -> i32
    let mut keys = BTreeMap::<String, BTreeMap<String, BTreeMap<String, i32>>>::new();

    for row in source {
        let team_map = if let Some(team_map) = keys.get_mut(&row.team_id) {
            team_map
        } else {
            keys.entry(row.team_id.clone()).or_default()
        };

        let player_map = if let Some(player_map) = team_map.get_mut(&row.player_id) {
            player_map
        } else {
            team_map.entry(row.player_id.clone()).or_default()
        };

        if let Some(data) = row.data.as_object() {
            for (key, value) in data {
                let entry = if let Some(entry) = player_map.get_mut(key.as_str()) {
                    entry
                } else {
                    player_map.entry(key.clone()).or_insert(0)
                };

                if let Some(val) = value.as_i64() {
                    *entry += val as i32;
                }
            }
        }
    }

    let mut output = Vec::new();
    for (team_id, team_map) in keys {
        for (player_id, player_map) in team_map {
            output.push(ApiPlayerStats {
                player_id,
                team_id: team_id.clone(),
                stats: player_map,
            })
        }
    }

    output
}

fn fake_paginate<T>(data: Vec<T>) -> PaginatedResult<T> {
    PaginatedResult {
        items: data,
        next_page: None,
    }
}
