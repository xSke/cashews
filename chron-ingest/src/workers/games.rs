use std::{
    collections::{BTreeSet, HashSet},
    time::Duration,
};

use chron_db::{
    derived::{DbGame, DbGameSaveModel, GetGamesQuery},
    models::{EntityKind, EntityVersion},
};
use futures::StreamExt;
use time::OffsetDateTime;
use tokio::time::interval;
use tracing::{error, info, warn};

use crate::models::{MmolbGame, MmolbGameByTeam, MmolbTime};

use super::{IntervalWorker, WorkerContext};

pub struct PollAllGames;
pub struct PollNewCurrentGames;
pub struct PollLiveGames;

impl IntervalWorker for PollAllGames {
    fn interval() -> tokio::time::Interval {
        interval(Duration::from_secs(60 * 60))
    }

    async fn tick(&mut self, ctx: &mut super::WorkerContext) -> anyhow::Result<()> {
        let game_ids = get_all_known_game_ids(ctx).await?;

        ctx.process_many(game_ids, 50, poll_game_by_id).await;

        Ok(())
    }
}

impl IntervalWorker for PollNewCurrentGames {
    fn interval() -> tokio::time::Interval {
        interval(Duration::from_secs(10))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let time = ctx.client.fetch("https://mmolb.com/api/time").await?;
        let time = time.parse::<MmolbTime>()?;

        // get all games already in db for the current season/day
        // we could probably do this entire query in sql if we wanted, ig...
        let known_games_today = ctx
            .db
            .get_games(GetGamesQuery {
                count: 99999, // ignore pagination for now?
                season: Some(time.season_number),
                day: Some(time.season_day),
                order: chron_db::queries::SortOrder::Asc,
                page: None,
                team: None,
            })
            .await?;
        info!("found {} games in db today", known_games_today.items.len());

        // filter teams that we already have known games for
        let team_ids = ctx.db.get_all_entity_ids(EntityKind::Team).await?;
        let mut team_ids = BTreeSet::from_iter(team_ids);
        for g in known_games_today.items {
            team_ids.remove(&g.away_team_id);
            team_ids.remove(&g.home_team_id);
        }

        info!("found {} teams without any games", team_ids.len());
        ctx.process_many(team_ids, 10, poll_game_for_team).await;
        Ok(())
    }
}

impl IntervalWorker for PollLiveGames {
    fn interval() -> tokio::time::Interval {
        interval(Duration::from_secs(5))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let time = ctx.client.fetch("https://mmolb.com/api/time").await?;
        let time = time.parse::<MmolbTime>()?;

        let known_games_today = ctx
            .db
            .get_games(GetGamesQuery {
                count: 99999, // ignore pagination for now?
                season: Some(time.season_number),
                day: Some(time.season_day),
                order: chron_db::queries::SortOrder::Asc,
                page: None,
                team: None,
            })
            .await?;
        let live_games = known_games_today
            .items
            .into_iter()
            .filter(|x| x.state != "Complete")
            .collect::<Vec<_>>();
        info!("found {} live games in db", live_games.len());

        ctx.process_many(live_games, 50, poll_live_game).await;

        Ok(())
    }
}

async fn poll_game_for_team(ctx: &WorkerContext, id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/game-by-team/{}", &id);
    let resp = ctx.client.try_fetch(url).await?;
    if let Some(resp) = resp {
        let game_by_team = resp.parse::<MmolbGameByTeam>()?;
        poll_game_by_id(ctx, game_by_team.game_id).await?;
    } else {
        warn!("team {} has no game in api", id);
    }
    Ok(())
}

async fn poll_live_game(ctx: &WorkerContext, game: DbGame) -> anyhow::Result<()> {
    let current_count = game.event_count;

    let url = format!(
        "https://mmolb.com/api/game/{}/live?after={}",
        game.game_id, current_count
    );
    let resp = ctx.client.fetch(&url).await?;

    let events = resp.parse::<Vec<serde_json::Value>>()?;

    let events_indexed = events
        .iter()
        .enumerate()
        .map(|(idx, value)| (idx as i32 + current_count, value))
        .collect::<Vec<_>>();
    ctx.db
        .update_game_events(&game.game_id, &resp.timestamp(), &events_indexed)
        .await?;

    fn is_game_over_event(e: &serde_json::Value) -> bool {
        // oh no
        if let Some(obj) = e.as_object() {
            if let Some(event_val) = obj.get("event") {
                if let Some(event_str) = event_val.as_str() {
                    if event_str == "GameOver" {
                        return true;
                    }
                }
            }
        }
        return false;
    }
    let new_state = if events.iter().any(is_game_over_event) {
        "Complete".to_string()
    } else {
        game.state
    };

    if let Some(last_update) = events.last() {
        ctx.db
            .update_game(DbGameSaveModel {
                game_id: &game.game_id,
                season: game.season,
                day: game.day,
                home_team_id: &game.home_team_id,
                away_team_id: &game.away_team_id,
                state: &new_state,
                event_count: current_count + events.len() as i32,
                last_update: Some(last_update),
            })
            .await?;
    }
    Ok(())
}

async fn get_all_known_game_ids(ctx: &WorkerContext) -> anyhow::Result<HashSet<String>> {
    let preset_game_ids = include_str!("./game_ids.txt");

    let mut game_ids: HashSet<String> = preset_game_ids
        .split("\n")
        .map(|x| x.to_string())
        .filter(|x| !x.is_empty())
        .collect();

    game_ids.extend(ctx.db.get_all_entity_ids(EntityKind::Game).await?);
    Ok(game_ids)
}

async fn poll_game_by_id(ctx: &WorkerContext, id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/game/{}", id);
    let resp = ctx.fetch_and_save(url, EntityKind::Game, &id).await?;

    let game: MmolbGame = resp.parse()?;
    process_game_data(ctx, &id, &game, &resp.timestamp()).await?;

    Ok(())
}

async fn process_game_data(
    ctx: &WorkerContext,
    id: &str,
    game: &MmolbGame,
    timestamp: &OffsetDateTime,
) -> anyhow::Result<()> {
    ctx.db
        .update_game(DbGameSaveModel {
            game_id: &id,
            season: game.season,
            day: game.day,
            home_team_id: &game.home_team_id,
            away_team_id: &game.away_team_id,
            state: &game.state,
            event_count: game.event_log.len() as i32,
            last_update: game.event_log.last(),
        })
        .await?;

    let events = game
        .event_log
        .iter()
        .enumerate()
        .map(|(idx, data)| (idx as i32, data))
        .collect::<Vec<_>>();
    ctx.db.update_game_events(&id, timestamp, &events).await?;

    if let Some(game_stats) = &game.stats {
        let mut stats = Vec::new();
        for (team_id, team_stats) in game_stats {
            for (player_id, player_stats) in team_stats {
                stats.push((team_id.as_str(), player_id.as_str(), player_stats));
            }
        }
        ctx.db
            .update_game_player_stats(&id, game.season, game.day, &stats)
            .await?;
    }

    Ok(())
}

pub async fn rebuild_games(ctx: &WorkerContext) -> anyhow::Result<()> {
    // get game ids separately because "all game objects" is gonna be massive
    let all_game_ids = ctx.db.get_all_entity_ids(EntityKind::Game).await?;

    ctx.process_many(all_game_ids, 100, rebuild_game).await;
    Ok(())
}

pub async fn rebuild_games_slow(ctx: &WorkerContext) -> anyhow::Result<()> {
    let count = ctx.db.get_version_count(EntityKind::Game).await?;
    let stream = ctx.db.get_all_versions_stream(EntityKind::Game).await?;

    stream
        .map(|v| rebuild_games_slow_inner(ctx, v))
        .buffer_unordered(10)
        .enumerate()
        .for_each(async |(i, res)| {
            if i % 1000 == 0 {
                info!("rebuild games: at {}/{}", i, count);
            }
            if let Err(e) = res {
                error!("error rebuilding: {:?}", e);
            }
        })
        .await;

    Ok(())
}

async fn rebuild_games_slow_inner(
    ctx: &WorkerContext,
    version: sqlx::Result<EntityVersion>,
) -> anyhow::Result<()> {
    let version = version?;
    let parsed = version.parse::<MmolbGame>()?;
    process_game_data(ctx, &version.entity_id, &parsed, &version.valid_from.0).await?;
    Ok(())
}

async fn rebuild_game(ctx: &WorkerContext, game_id: String) -> anyhow::Result<()> {
    let game_data = ctx.db.get_latest(EntityKind::Game, &game_id).await?;
    if let Some(game_data) = game_data {
        let parsed = game_data.parse()?;
        process_game_data(ctx, &game_id, &parsed, &game_data.valid_from.0).await?;
    }

    Ok(())
}

pub async fn fetch_all_games(ctx: &WorkerContext) -> anyhow::Result<()> {
    let game_ids = get_all_known_game_ids(ctx).await?;
    ctx.process_many(game_ids, 50, poll_game_by_id).await;
    Ok(())
}
