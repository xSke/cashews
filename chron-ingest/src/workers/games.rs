use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use chron_db::{derived::DbGameSaveModel, models::EntityKind};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::interval;
use tracing::info;

use super::{IntervalWorker, WorkerContext};

pub struct PollAllGames;

impl IntervalWorker for PollAllGames {
    fn interval() -> tokio::time::Interval {
        interval(Duration::from_secs(60 * 60))
    }

    async fn tick(&mut self, ctx: &mut super::WorkerContext) -> anyhow::Result<()> {
        let game_ids = get_all_known_game_ids(ctx).await?;

        ctx.process_many(game_ids, 50, process_game).await;

        Ok(())
    }
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

async fn process_game(ctx: &WorkerContext, id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/game/{}", id);
    let resp = ctx.fetch_and_save(url, EntityKind::Game, &id).await?;

    let game: MmolbGame = resp.parse()?;
    process_game_data(ctx, &id, &game).await?;

    Ok(())
}

async fn process_game_data(ctx: &WorkerContext, id: &str, game: &MmolbGame) -> anyhow::Result<()> {
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
    ctx.db.update_game_events(&id, &events).await?;

    let mut stats = Vec::new();
    for (team_id, team_stats) in &game.stats {
        for (player_id, player_stats) in team_stats {
            stats.push((team_id.as_str(), player_id.as_str(), player_stats));
        }
    }
    ctx.db
        .update_game_player_stats(&id, game.season, game.day, &stats)
        .await?;
    info!("updated game {}", id);

    Ok(())
}

#[derive(Debug, Deserialize)]
struct MmolbGame {
    #[serde(rename = "Season")]
    season: i32,
    #[serde(rename = "Day")]
    day: i32,

    #[serde(rename = "AwayTeamID")]
    away_team_id: String,
    #[serde(rename = "HomeTeamID")]
    home_team_id: String,

    #[serde(rename = "State")]
    state: String,

    #[serde(rename = "Stats")]
    stats: HashMap<String, HashMap<String, Value>>,

    #[serde(rename = "EventLog")]
    event_log: Vec<Value>,
}

pub async fn rebuild_games(ctx: &WorkerContext) -> anyhow::Result<()> {
    // get game ids separately because "all game objects" is gonna be massive
    let all_game_ids = ctx.db.get_all_entity_ids(EntityKind::Game).await?;

    ctx.process_many(all_game_ids, 100, rebuild_game).await;
    Ok(())
}

async fn rebuild_game(ctx: &WorkerContext, game_id: String) -> anyhow::Result<()> {
    let game_data = ctx.db.get_latest(EntityKind::Game, &game_id).await?;
    if let Some(game_data) = game_data {
        let parsed = game_data.parse()?;
        process_game_data(ctx, &game_id, &parsed).await?;
    }

    Ok(())
}

pub async fn fetch_all_games(ctx: &WorkerContext) -> anyhow::Result<()> {
    let game_ids = get_all_known_game_ids(ctx).await?;
    ctx.process_many(game_ids, 50, process_game).await;
    Ok(())
}
