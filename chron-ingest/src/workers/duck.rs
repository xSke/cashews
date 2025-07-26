use std::time::Duration;

use tokio::time::{Interval, interval};

use crate::workers::{IntervalWorker, WorkerContext};

pub struct RefreshDuckDb;

impl IntervalWorker for RefreshDuckDb {
    fn interval() -> Interval {
        interval(Duration::from_secs(60 * 5))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        refresh_duckdb(ctx.clone()).await?;
        Ok(())
    }
}

pub async fn refresh_duckdb(ctx: WorkerContext) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || {
        ctx.db.refresh_duckdb_sync(&ctx.config)?;
        Ok(())
    })
    .await?
}
