use std::path::PathBuf;

use chron_base::ChronConfig;
use duckdb::DuckdbConnectionManager;
use r2d2::Pool;
use tracing::info;

use crate::ChronDb;

pub fn new_duckdb(
    config: &ChronConfig,
) -> anyhow::Result<(
    r2d2::Pool<DuckdbConnectionManager>,
    r2d2::Pool<DuckdbConnectionManager>,
)> {
    let folder = PathBuf::from(config.duckdb_path.as_deref().unwrap_or_else(|| "/tmp"));

    let duckdb_file = folder.join("db.duckdb");

    // just create it rq
    let tmp_conn = duckdb::Connection::open(&duckdb_file)?;
    drop(tmp_conn);

    let read_cfg = duckdb::Config::default()
        .access_mode(duckdb::AccessMode::ReadOnly)?
        .enable_autoload_extension(false)?
        .enable_external_access(false)?
        .enable_object_cache(false)?
        .max_memory("512MB")?
        .threads(2)?
        .with("allow_community_extensions", "false")?
        .with("lock_configuration", "true")?;
    let write_cfg = duckdb::Config::default().threads(4)?.max_memory("2GB")?;
    let duckdb_read = DuckdbConnectionManager::file_with_flags(&duckdb_file, read_cfg)?;
    let duckdb_write = DuckdbConnectionManager::file_with_flags(&duckdb_file, write_cfg)?;

    let duckdb_read_pool = Pool::builder().max_size(5).build(duckdb_read)?;
    let duckdb_write_pool = Pool::builder().max_size(1).build(duckdb_write)?;
    Ok((duckdb_read_pool, duckdb_write_pool))
}

impl ChronDb {
    pub fn refresh_duckdb_sync(&self, config: &ChronConfig) -> anyhow::Result<()> {
        let conn = self.duckdb_write.get()?;

        // pls no inject
        conn.execute(
            &format!(
                "ATTACH '{}' AS postgres_db (TYPE postgres, READ_ONLY);",
                &config.database_uri
            ),
            [],
        )?;

        info!("loading game_player_stats");
        conn.execute(
            "CREATE OR REPLACE TABLE game_player_stats AS FROM postgres_db.game_player_stats_exploded;",
            [],
        )?;
        info!("loading teams");
        conn.execute(
            "CREATE OR REPLACE TABLE teams AS FROM postgres_db.teams;",
            [],
        )?;

        conn.execute("FORCE CHECKPOINT;", [])?;

        // sanity test
        let count: i32 =
            conn.query_row("SELECT count(*) FROM game_player_stats", [], |r| r.get(0))?;
        drop(conn);

        info!("refreshed duckdb from postgres, got {} gps rows", count);

        Ok(())
    }
}
