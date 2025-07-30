use std::time::Duration;

use chron_db::models::EntityKind;
use strum::VariantArray;
use tracing::info;

use crate::workers::IntervalWorker;

use super::WorkerContext;

pub async fn rebuild_all(ctx: &WorkerContext) -> anyhow::Result<()> {
    for kind in EntityKind::VARIANTS {
        info!("rebuilding {:?}", kind);
        ctx.db.rebuild_all(*kind).await?;
    }
    Ok(())
}

pub async fn recompress(ctx: &WorkerContext) -> anyhow::Result<()> {
    let mut count = 0;
    loop {
        let res = sqlx::query(
            "with rows as (select objects.ctid from objects where pg_column_compression(data) = 'pglz' for update limit 100) update objects set data = to_jsonb(data) from rows where objects.ctid = rows.ctid",
        ).execute(&ctx.db.pool).await?;
        count += res.rows_affected();
        info!("recompressed {} objects and counting", count);
        if res.rows_affected() == 0 {
            break;
        }
    }
    Ok(())
}

pub struct FixupGameStatsNames;

impl IntervalWorker for FixupGameStatsNames {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(10 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        fixup_game_stats_names(ctx).await?;
        Ok(())
    }
}

pub async fn fixup_game_stats_names(ctx: &WorkerContext) -> anyhow::Result<()> {
    // todo: find all game stats with null player names and re-process their games
    Ok(())
}
