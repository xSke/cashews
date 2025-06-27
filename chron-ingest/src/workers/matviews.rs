use std::time::{Duration, Instant};

use tracing::{info, warn};

use super::IntervalWorker;

pub struct RefreshMatviews;

impl IntervalWorker for RefreshMatviews {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(60 * 10))
    }

    async fn tick(&mut self, ctx: &mut super::WorkerContext) -> anyhow::Result<()> {
        let matviews = [
            "players",
            "team_feeds",
            "rosters",
            "roster_slot_history",
            "game_player_stats_exploded",
            "game_player_stats_league_aggregate",
            "game_player_stats_global_aggregate",
            "pitches",
        ];
        for matview in matviews {
            info!("refreshing matview {}...", matview);

            let mut tx = ctx.db.pool.begin().await?;
            let lock: bool = sqlx::query_scalar("select pg_try_advisory_xact_lock(0x13371337)")
                .fetch_one(&mut *tx)
                .await?;
            if !lock {
                warn!("failed to claim advisory xact lock for matview refresh");
                break;
            }

            let time_before = Instant::now();
            sqlx::query(&format!(
                "refresh materialized view concurrently {}",
                matview
            ))
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            let time_after = Instant::now();

            let delta = time_after.duration_since(time_before);
            info!(
                "refreshed matview {} (took {}s)",
                matview,
                delta.as_secs_f32()
            );
        }

        Ok(())
    }
}
