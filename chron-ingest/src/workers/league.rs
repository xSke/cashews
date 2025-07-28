use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use chron_db::{
    derived::{DbLeagueSaveModel, DbTeamSaveModel, GetGamesQuery},
    models::{EntityKind, NewObject},
};
use futures::TryStreamExt;
use serde::Deserialize;
use tokio::time::interval;
use tracing::{info, warn};

use crate::{
    http::ClientResponse,
    models::{MmolbLeague, MmolbState, MmolbTeam},
    synthetic,
};

use super::{IntervalWorker, WorkerContext};

pub struct PollLeague;
pub struct PollNewPlayers;
pub struct PollAllPlayers;
pub struct PollBenches;

impl IntervalWorker for PollLeague {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(10 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        poll_league(ctx).await?;
        Ok(())
    }
}

pub async fn poll_league(ctx: &WorkerContext) -> anyhow::Result<()> {
    let state_resp = ctx
        .fetch_and_save("https://mmolb.com/api/state", EntityKind::State, "state")
        .await?;

    let _time = ctx.try_update_time().await?;

    let state: MmolbState = state_resp.parse()?;

    let league_ids = get_league_ids(&state);
    info!("got {} league ids", league_ids.len());
    ctx.process_many_with_progress(league_ids, 3, "fetch leagues", fetch_league)
        .await;

    let team_ids = get_all_known_team_ids(ctx).await?;
    info!("got {} team ids", team_ids.len());
    ctx.process_many_with_progress(team_ids, 3, "fetch teams", fetch_team)
        .await;

    ctx.fetch_and_save(
        "https://mmolb.com/api/postseason-bracket",
        EntityKind::PostseasonBracket,
        "postseason-bracket",
    )
    .await?;

    Ok(())
}

#[derive(Deserialize)]
struct GameWithBench {
    #[serde(rename = "OriginalBench")]
    original_bench: Option<HashMap<String, Bench>>,
}

#[derive(Deserialize)]
struct Bench {
    #[serde(rename = "Batters")]
    batters: Vec<String>,
    #[serde(rename = "Pitchers")]
    pitchers: Vec<String>,
}

impl IntervalWorker for PollBenches {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(30 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let mut stream = ctx.db.get_all_latest_stream(EntityKind::Game);
        let mut bench_players = HashSet::new();
        let mut i = 0;
        while let Some(game_ver) = stream.try_next().await? {
            match game_ver.parse::<GameWithBench>() {
                Ok(parsed) => {
                    if let Some(bench) = parsed.original_bench {
                        for team_bench in bench.values() {
                            // todo: mark if a bench player is a pitcher or a batter?
                            bench_players.extend(team_bench.batters.iter().cloned());
                            bench_players.extend(team_bench.pitchers.iter().cloned());
                        }
                    }
                }
                Err(e) => warn!(
                    "failed to parse game with bench {}: {:?}",
                    game_ver.entity_id, e
                ),
            }
            if i % 1000 == 0 {
                info!("finding bench players (at {} games)", i);
            }
            i += 1;
        }

        info!("found {} players on bench, polling", bench_players.len());

        let bench_players = Vec::from_iter(bench_players);
        ctx.process_many_with_progress(
            bench_players.chunks(100),
            1,
            "fetch bench players",
            fetch_players_bulk,
        )
        .await;

        Ok(())
    }
}

impl IntervalWorker for PollNewPlayers {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let player_ids = get_all_known_player_ids(ctx).await?;

        let player_object_ids = ctx.db.get_all_entity_ids(EntityKind::Player).await?;
        let player_object_ids = HashSet::from_iter(player_object_ids);

        let new_players = player_ids.difference(&player_object_ids).cloned();
        ctx.process_many_with_progress(new_players, 3, "fetch players", fetch_player)
            .await;

        Ok(())
    }
}

impl IntervalWorker for PollAllPlayers {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(60 * 10))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let player_ids = get_all_known_player_ids(ctx).await?;
        info!("got {} player ids", player_ids.len());

        let player_ids = player_ids.into_iter().collect::<Vec<_>>();

        // this one can go slowly, that's fine
        // ...although i think at this rate, we may literally *always* be polling players...
        // maybe some sort of thing to prioritize players that have shown up in *team* feed events recently?
        ctx.process_many_with_progress(
            player_ids.chunks(100),
            1,
            "fetch all players",
            fetch_players_bulk,
        )
        .await;

        Ok(())
    }
}

async fn fetch_league(ctx: &WorkerContext, id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/league/{}", id);
    let resp = ctx.fetch_and_save(url, EntityKind::League, &id).await?;

    let league_data = resp.parse::<MmolbLeague>()?;
    ctx.db
        .update_league(DbLeagueSaveModel {
            league_id: &id,
            league_type: &league_data.league_type,
            name: &league_data.name,
            color: &league_data.color,
            emoji: &league_data.emoji,
        })
        .await?;

    Ok(())
}

pub async fn fetch_team(ctx: &WorkerContext, id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/team/{}", id);
    let resp = ctx.fetch_and_save(url, EntityKind::Team, &id).await?;

    let team = resp.parse::<serde_json::Value>()?;
    synthetic::handle_incoming(ctx, EntityKind::Team, &id, &team, resp.timestamp()).await?;

    let team_data = resp.parse::<MmolbTeam>()?;
    ctx.db
        .update_team(DbTeamSaveModel {
            team_id: &id,
            league_id: team_data.league.as_deref(),
            location: &team_data.location,
            name: &team_data.name,
            full_location: &team_data.full_location,
            emoji: &team_data.emoji,
            color: &team_data.color,
            abbreviation: &team_data.abbreviation,
        })
        .await?;

    Ok(())
}

#[derive(Deserialize)]
struct BulkPlayerResponse {
    players: Vec<serde_json::Value>,
}

pub async fn fetch_players_bulk(ctx: &WorkerContext, ids: &[String]) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/players?ids={}", ids.join(","));
    let resp = ctx.client.fetch(url).await?;
    let parsed = resp.parse::<BulkPlayerResponse>()?;

    // somehow the i/o here is the slowest part
    ctx.process_many(parsed.players, 25, |ctx, player_obj| {
        save_player_inner(ctx, player_obj, &resp)
    })
    .await;

    Ok(())
}

pub async fn fetch_player(ctx: &WorkerContext, id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/player/{}", id);
    let resp = ctx.fetch_and_save(url, EntityKind::Player, &id).await?;

    let data = resp.parse::<serde_json::Value>()?;
    synthetic::handle_incoming(ctx, EntityKind::Player, &id, &data, resp.timestamp()).await?;
    Ok(())
}

async fn save_player_inner(
    ctx: &WorkerContext,
    player_obj: serde_json::Value,
    resp: &ClientResponse,
) -> anyhow::Result<()> {
    let player_id = player_obj
        .as_object()
        .and_then(|x| x.get("_id"))
        .and_then(|x| x.as_str())
        .map(|x| x.to_string())
        .ok_or_else(|| anyhow::anyhow!("couldn't find _id on player"))?;

    let db_obj = NewObject {
        data: player_obj.clone(),
        kind: EntityKind::Player,
        entity_id: player_id.clone(),
        request_time: resp.request_time(),
        timestamp: resp.timestamp(),
    };
    ctx.db.save(db_obj).await?;

    synthetic::handle_incoming(
        ctx,
        EntityKind::Player,
        &player_id,
        &player_obj,
        resp.timestamp(),
    )
    .await?;

    Ok(())
}

fn get_league_ids(state: &MmolbState) -> Vec<String> {
    let mut out = Vec::new();
    out.extend(state.greater_leagues.iter().cloned());
    out.extend(state.lesser_leagues.iter().cloned());
    out
}

async fn get_all_known_team_ids(ctx: &WorkerContext) -> anyhow::Result<HashSet<String>> {
    let mut team_ids = HashSet::new();

    // get from DB leagues
    for league_obj in ctx.db.get_all_latest(EntityKind::League).await? {
        let league = league_obj.parse::<MmolbLeague>()?;
        team_ids.extend(league.teams);
        team_ids.extend(league.superstar_team);
    }

    // get from DB teams
    team_ids.extend(ctx.db.get_all_entity_ids(EntityKind::Team).await?);

    // get from stats obj?
    team_ids.extend(ctx.db.get_all_team_ids_from_stats().await?);

    Ok(team_ids)
}

async fn get_all_known_player_ids(ctx: &WorkerContext) -> anyhow::Result<HashSet<String>> {
    let mut team_ids = HashSet::new();

    // get from DB teams
    let get_all_latest = ctx.db.get_all_latest(EntityKind::Team).await?;
    for team_obj in get_all_latest {
        let team = team_obj.parse::<MmolbTeam>()?;

        for player_slot in team.players {
            if player_slot.player_id != "#" {
                team_ids.insert(player_slot.player_id);
            }
        }
    }

    // get from DB players
    team_ids.extend(ctx.db.get_all_entity_ids(EntityKind::Player).await?);

    // get from stats obj?
    team_ids.extend(ctx.db.get_all_player_ids_from_stats().await?);

    Ok(team_ids)
}

pub async fn fetch_all_players(ctx: &WorkerContext) -> anyhow::Result<()> {
    let all_players = get_all_known_player_ids(ctx).await?;
    let all_players = all_players.into_iter().collect::<Vec<_>>();
    ctx.process_many_with_progress(
        all_players.chunks(100),
        50,
        "fetch all players",
        fetch_players_bulk,
    )
    .await;
    Ok(())
}
