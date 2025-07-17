use crate::workers::{IntervalWorker, WorkerContext};
use chron_db::models::EntityKind;
use futures::TryStreamExt;
use serde::Deserialize;
use std::ops::Deref;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::time::{Interval, interval};

pub struct ProcessFeeds;

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

        let mut player_stream = ctx.db.get_all_latest_stream(EntityKind::Player);
        while let Some(entity) = player_stream.try_next().await? {
            let holder = entity.parse::<FeedHolder>()?;
            handle_feed(&mut entries, &holder.feed);
        }

        let mut team_stream = ctx.db.get_all_latest_stream(EntityKind::Team);
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
    #[serde(rename = "Feed", default)]
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
