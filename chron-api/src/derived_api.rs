use std::collections::{BTreeMap, HashMap};

use axum::{
    Json,
    extract::{Query, State},
};
use chron_db::{
    derived::{DbGame, DbGamePlayerStats, DbLeague, DbTeam},
    models::PageToken,
    queries::{PaginatedResult, SortOrder},
};
use serde::{Deserialize, Serialize};

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
pub struct GetTeamsQuery {
}

pub async fn get_teams(
    State(ctx): State<AppState>,
    Query(_q): Query<GetTeamsQuery>,
) -> Result<Json<PaginatedResult<DbTeam>>, AppError> {
    let teams = ctx
        .db
        .get_teams()
        .await?;

    Ok(Json(fake_paginate(teams)))
}

#[derive(Deserialize, Debug)]
pub struct GetLeaguesQuery {
}

pub async fn get_leagues(
    State(ctx): State<AppState>,
    Query(_q): Query<GetTeamsQuery>,
) -> Result<Json<PaginatedResult<DbLeague>>, AppError> {
    let teams = ctx
        .db
        .get_leagues()
        .await?;

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
    PaginatedResult { items: data, next_page: None }
}