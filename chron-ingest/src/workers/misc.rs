use std::time::Duration;

use chron_db::models::EntityKind;

use crate::workers::{IntervalWorker, WorkerContext};

pub struct PollMiscData;


impl IntervalWorker for PollMiscData {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(5 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        ctx.fetch_and_save(
            "https://mmolb.com/api/spotlight",
            EntityKind::Spotlight,
            "spotlight",
        )
        .await?;

        ctx.fetch_and_save(
            "https://mmolb.com/api/news",
            EntityKind::News,
            "news",
        )
        .await?;

        let mut nouns_resp = ctx.client.fetch("https://mmolb.com/data/nouns.txt").await?;
        let mut adjectives_resp = ctx.client.fetch("https://mmolb.com/data/adjectives.txt").await?;
        // cheat a little, massage the data into a json format so our usual methods will take them
        nouns_resp.data = lines_to_json(&nouns_resp.data)?;
        adjectives_resp.data = lines_to_json(&adjectives_resp.data)?;

        ctx.db.save(nouns_resp.to_chron(EntityKind::Nouns, "nouns")?).await?;
        ctx.db.save(adjectives_resp.to_chron(EntityKind::Adjectives, "adjectives")?).await?;
        
        Ok(())
    }
}

fn lines_to_json(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let lines = str::from_utf8(data)?;
    let lines_owned = lines.lines().map(|x| x.to_string()).collect::<Vec<_>>();
    Ok(serde_json::to_vec(&lines_owned)?)
}