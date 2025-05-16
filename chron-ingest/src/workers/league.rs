use std::{collections::HashSet, time::Duration};

use chron_db::models::{EntityKind, NewObject};
use tracing::info;

use crate::models::{MmolbLeague, MmolbState, MmolbTeam};

use super::{IntervalWorker, WorkerContext};

pub struct PollLeague;
pub struct PollNewPlayers;
// pub struct PollLeague;

impl IntervalWorker for PollLeague {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(5 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let state_resp = ctx
            .fetch_and_save("https://mmolb.com/api/state", EntityKind::State, "state")
            .await?;
        ctx.fetch_and_save("https://mmolb.com/api/time", EntityKind::Time, "time")
            .await?;

        let state: MmolbState = state_resp.parse()?;

        let league_ids = get_league_ids(&state);
        info!("got {} league ids", league_ids.len());
        ctx.process_many(league_ids, 3, fetch_league).await;

        let team_ids = get_all_known_team_ids(ctx).await?;
        info!("got {} team ids", team_ids.len());
        ctx.process_many(team_ids, 10, fetch_team).await;

        // let player_ids = get_all_known_player_ids(ctx).await?;
        // info!("got {} player ids", player_ids.len());
        // ctx.process_many(player_ids, 50, fetch_player).await;

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
        ctx.process_many(new_players, 20, fetch_player).await;

        Ok(())
    }
}

async fn fetch_league(ctx: &WorkerContext, id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/league/{}", id);
    ctx.fetch_and_save(url, EntityKind::League, id).await?;
    Ok(())
}

async fn fetch_team(ctx: &WorkerContext, id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/team/{}", id);
    let resp = ctx.fetch_and_save(url, EntityKind::Team, &id).await?;

    // write a "cleaned" version of the team object without the big Players[x].Stats objects
    // can we make this code nicer?
    let mut team_data = resp.parse::<serde_json::Value>()?;
    if let Some(o) = team_data.as_object_mut() {
        if let Some(players_value) = o.get_mut("Players") {
            if let Some(players) = players_value.as_array_mut() {
                for player_value in players {
                    if let Some(p) = player_value.as_object_mut() {
                        p.remove("Stats");
                    }
                }
            }
        }
    }

    ctx.db
        .save(NewObject {
            kind: EntityKind::TeamLite,
            entity_id: id.to_string(),
            data: team_data,
            timestamp: resp.timestamp(),
            request_time: resp.request_time(),
        })
        .await?;
    Ok(())
}

async fn fetch_player(ctx: &WorkerContext, id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/player/{}", id);
    let resp = ctx.fetch_and_save(url, EntityKind::Player, &id).await?;

    // write a "cleaned" version of the player object without the big stats object
    let mut player_data = resp.parse::<serde_json::Value>()?;
    if let Some(o) = player_data.as_object_mut() {
        o.remove("Stats");
    }

    ctx.db
        .save(NewObject {
            kind: EntityKind::PlayerLite,
            entity_id: id.to_string(),
            data: player_data,
            timestamp: resp.timestamp(),
            request_time: resp.request_time(),
        })
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
    for team_obj in ctx.db.get_all_latest(EntityKind::Team).await? {
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
