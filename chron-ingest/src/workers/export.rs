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
        export_async(&ctx.config).await?;

        Ok(())
    }
}

pub async fn export_async(cfg: &Arc<ChronConfig>) -> anyhow::Result<()> {
    let cfg = Arc::clone(cfg);
    tokio::task::spawn_blocking(move || export_to(&cfg)).await??;
    Ok(())
}

pub fn export_to(cfg: &ChronConfig) -> anyhow::Result<()> {
    let conn: Connection = Connection::open_in_memory()?;

    // init
    info!("importing into duckdb...");
    conn.execute_batch(&format!(
        r#"
        INSTALL postgres;
        LOAD postgres;

        SET preserve_insertion_order = false;

        ATTACH '{}' AS pgdb (TYPE postgres, READ_ONLY);
        CREATE OR REPLACE TABLE game_player_stats_exploded AS FROM pgdb.game_player_stats_exploded;
        
        "#,
        cfg.database_uri
    ))?;

    info!("exporting...");
    if let Some(export_path) = &cfg.export_path {
        for compression in ["snappy", "gzip", "zstd", "lz4", "lz4_raw"] {
            let dir = PathBuf::from(export_path);
            let file = dir.join(format!("gps.{}.parquet", compression));
            let tmpfile = dir.join("gps.parquet.tmp");
            conn.execute_batch(&format!(
                "COPY (FROM game_player_stats_exploded) TO '{}' (FORMAT parquet, COMPRESSION {})",
                tmpfile.to_string_lossy(),
                compression
            ))?;
            std::fs::rename(&tmpfile, &file)?;
            info!("exported to {}!", file.to_string_lossy());
        }

        for csv_compression in [/*"none", */ "gzip", "zstd"] {
            let dir = PathBuf::from(export_path);
            let file = dir.join(if csv_compression != "none" {
                format!("gps.csv.{}", csv_compression)
            } else {
                "gps.csv".to_string()
            });
            let tmpfile = dir.join("gps.csv.tmp");
            conn.execute_batch(&format!(
                "COPY (FROM game_player_stats_exploded) TO '{}' (FORMAT csv, HEADER, COMPRESSION {})",
                tmpfile.to_string_lossy(),
                csv_compression
            ))?;
            std::fs::rename(&tmpfile, &file)?;
            info!("exported to {}!", file.to_string_lossy());
        }
    } else {
        warn!("no export path specified, aborting");
    }

    Ok(())
}
