use crate::workers::{IntervalWorker, WorkerContext};
use chron_db::models::EntityKind;
use futures::TryStreamExt;
use serde::Deserialize;
use std::ops::Deref;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::time::{Interval, interval};

pub struct ProcessFeeds;
pub struct PollPlayerFeeds;
pub struct PollTeamFeeds;

fn handle_feed(output: &mut Vec<PlayerNameMapEntry>, feed: &[FeedEntry]) {
    for entry in feed {
        for link in &entry.links {
            if link.kind.as_deref() == Some("player") {
                let player_id = if entry.text.contains(" was Recomposed into ") {
                    let first_player = entry
                        .links
                        .iter()
                        .find(|l| l.kind.as_deref() == Some("player"));
                    first_player.and_then(|x| x.id.as_deref())
                } else {
                    link.id.as_deref()
                };
                if let (Some(player_id), Some(player_name)) = (player_id, link.string.as_deref()) {
                    output.push(PlayerNameMapEntry {
                        timestamp: entry.ts,
                        player_id: player_id.to_string(),
                        player_name: player_name.to_string(),
                    })
                }
            }
        }
    }
}

impl IntervalWorker for ProcessFeeds {
    fn interval() -> Interval {
        interval(Duration::from_secs(60 * 5))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let mut entries = Vec::new();

        let mut player_stream = ctx.db.get_all_latest_stream(EntityKind::PlayerFeed);
        while let Some(entity) = player_stream.try_next().await? {
            let holder = entity.parse::<FeedHolder>()?;
            handle_feed(&mut entries, &holder.feed);
        }

        let mut team_stream = ctx.db.get_all_latest_stream(EntityKind::TeamFeed);
        while let Some(entity) = team_stream.try_next().await? {
            let holder = entity.parse::<FeedHolder>()?;
            handle_feed(&mut entries, &holder.feed);
        }

        for chunk in entries.chunks(1000) {
            let ids = chunk
                .iter()
                .map(|x| x.player_id.deref())
                .collect::<Vec<_>>();
            let names = chunk
                .iter()
                .map(|x| x.player_name.deref())
                .collect::<Vec<_>>();
            let timestamps = chunk.iter().map(|x| x.timestamp).collect::<Vec<_>>();

            // todo: do nothing?
            sqlx::query("insert into player_name_map (timestamp, player_id, player_name) select unnest($1), unnest($2), unnest($3) on conflict (timestamp, player_id) do nothing")
                .bind(timestamps)
                .bind(ids)
                .bind(names)
                .execute(&ctx.db.pool)
                .await?;
        }

        // add in names from chron
        sqlx::query("insert into player_name_map (player_id, player_name, timestamp) select entity_id as player_id, (data->>'FirstName') || ' ' || (data->>'LastName') as player_name, valid_from as timestamp from versions inner join objects using (hash) where kind = 9 on conflict do nothing")
            .execute(&ctx.db.pool)
            .await?;

        // for every player, smash their earliest known name into -infinity (or close enough)
        sqlx::query("insert into player_name_map (player_id, player_name, timestamp) select distinct player_id, first_value(player_name) over (partition by player_id order by timestamp) as player_name, ('1970-01-01'::timestamptz) as timestamp from player_name_map on conflict (player_id, timestamp) do update set player_name = excluded.player_name")
            .execute(&ctx.db.pool)
            .await?;

        Ok(())
    }
}

struct PlayerNameMapEntry {
    timestamp: OffsetDateTime,
    player_id: String,
    player_name: String,
}

#[derive(Deserialize)]
struct FeedHolder {
    // player/team objects had a Feed key (uppercase), feed endpoint returns a feed key (lowercase)...
    #[serde(rename = "Feed", alias = "feed", default)]
    feed: Vec<FeedEntry>,
}

#[derive(Deserialize)]
struct FeedEntry {
    #[serde(with = "time::serde::rfc3339")]
    ts: OffsetDateTime,

    #[serde(default)]
    text: String,

    #[serde(default)]
    links: Vec<FeedLink>,
}

#[derive(Deserialize)]
struct FeedLink {
    #[serde(default, rename = "type")]
    kind: Option<String>,

    #[serde(default)]
    id: Option<String>,

    #[serde(default, rename = "match")]
    string: Option<String>,
}

impl IntervalWorker for PollPlayerFeeds {
    fn interval() -> Interval {
        interval(Duration::from_secs(60 * 30))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let player_ids = ctx.db.get_all_entity_ids(EntityKind::Player).await?;

        ctx.process_many_with_progress(player_ids, 10, "player feeds", fetch_player_feed)
            .await;
        Ok(())
    }
}

impl IntervalWorker for PollTeamFeeds {
    fn interval() -> Interval {
        interval(Duration::from_secs(60 * 10))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let team_ids = ctx.db.get_all_entity_ids(EntityKind::Team).await?;

        ctx.process_many_with_progress(team_ids, 10, "team feeds", fetch_team_feed)
            .await;
        Ok(())
    }
}

async fn fetch_player_feed(ctx: &WorkerContext, player_id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/feed?player={}", &player_id);
    let _ = ctx
        .fetch_and_save(url, EntityKind::PlayerFeed, player_id)
        .await?;

    // todo: do anything immediately, or wait for ProcessFeeds to come around?
    Ok(())
}

async fn fetch_team_feed(ctx: &WorkerContext, team_id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/feed?team={}", &team_id);
    let _ = ctx
        .fetch_and_save(url, EntityKind::TeamFeed, team_id)
        .await?;

    // todo: do anything immediately, or wait for ProcessFeeds to come around?
    Ok(())
}
