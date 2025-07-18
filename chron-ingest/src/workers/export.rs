use std::{path::PathBuf, sync::Arc, time::Duration};

use chron_base::ChronConfig;
use duckdb::Connection;
use tracing::{info, warn};

use crate::workers::IntervalWorker;

pub struct ExportParquet;

impl IntervalWorker for ExportParquet {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(5 * 60))
    }

    async fn tick(&mut self, ctx: &mut super::WorkerContext) -> anyhow::Result<()> {
        let cfg = Arc::clone(&ctx.config);
        tokio::task::spawn_blocking(move || export_to(&cfg)).await??;

        Ok(())
    }
}

fn export_to(cfg: &ChronConfig) -> anyhow::Result<()> {
    let conn: Connection = Connection::open_in_memory()?;

    // init
    info!("importing into duckdb...");
    conn.execute_batch(&format!(
        r#"
        INSTALL postgres;
        LOAD postgres;

        ATTACH '{}' AS pgdb (TYPE postgres, READ_ONLY);
        CREATE OR REPLACE TABLE game_player_stats_exploded AS FROM pgdb.game_player_stats_exploded;
        
        "#,
        cfg.database_uri
    ))?;

    info!("exporting...");
    if let Some(export_path) = &cfg.export_path {
        let dir = PathBuf::from(export_path);
        let file = dir.join("gps.parquet");
        let tmpfile = dir.join("gps.parquet.tmp");
        conn.execute_batch(&format!(
            "COPY (FROM game_player_stats_exploded) TO '{}' (FORMAT parquet, COMPRESSION brotli)",
            tmpfile.to_string_lossy()
        ))?;

        std::fs::rename(&tmpfile, &file)?;
        info!("exported to {}!", file.to_string_lossy());
    } else {
        warn!("no export path specified, aborting");
    }

    Ok(())
}
