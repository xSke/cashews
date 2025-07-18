use std::{
    fs::File,
    io::Read,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use axum::{body::Bytes, extract::State};
use duckdb::Connection;
use tracing::{error, info};

use crate::{AppError, AppState};

pub struct DuckDBState {
    conn: Arc<Mutex<Connection>>,
    data: Arc<RwLock<Bytes>>,
}

impl DuckDBState {
    pub fn new(uri: &str) -> anyhow::Result<DuckDBState> {
        info!("init duckdb");
        let conn: Connection = Connection::open_in_memory()?;

        // init
        conn.execute_batch(&format!(
            r#"
        INSTALL postgres;
        LOAD postgres;

        ATTACH '{}' AS pgdb (TYPE postgres, READ_ONLY);
        
        "#,
            uri
        ))?;

        info!("duckdb done");

        Ok(DuckDBState {
            conn: Arc::new(Mutex::new(conn)),
            data: Arc::new(RwLock::new(Bytes::new())),
        })
    }

    pub fn get(&self) -> anyhow::Result<Connection> {
        let _lock = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned mutex"))?;
        let cloned = _lock.try_clone()?;
        return Ok(cloned);
    }

    pub fn refresh(&self) -> anyhow::Result<()> {
        info!("starting refresh");
        let conn = self.get()?;
        conn.execute("CREATE OR REPLACE TABLE game_player_stats_exploded AS FROM pgdb.game_player_stats_exploded;",[],)?;
        drop(conn);

        let conn2 = self.get()?;
        let count: i32 =
            conn2.query_row("SELECT count(*) from game_player_stats_exploded", [], |r| {
                r.get(0)
            })?;
        info!("refreshing done, {} rows in GPSe", count);

        info!("saving...");
        let dir = tempfile::Builder::new().suffix(".pq").tempdir()?;
        let file_path = dir.path().join("export.pq");

        // pls no inject
        conn2.execute(
            &format!(
                "COPY (FROM game_player_stats_exploded) TO '{}' (FORMAT parquet, COMPRESSION brotli)",
                file_path.to_string_lossy()
            ),
            [],
        )?;

        info!("saved, reading into mem");

        let mut file = File::open(&file_path)?;
        let mut data_vec = Vec::new();
        file.read_to_end(&mut data_vec)?;
        info!(
            "all done, db is {} bytes (in {})",
            data_vec.len(),
            file_path.to_string_lossy(),
        );

        let mut _guard = self
            .data
            .write()
            .map_err(|_| anyhow::anyhow!("poisoned mutex"))?;
        *_guard = Bytes::from(data_vec);

        Ok(())
    }
}

pub async fn worker(ctx: AppState) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let r = Arc::clone(&ctx.duckdb);
        match tokio::task::spawn_blocking(move || r.refresh()).await {
            Ok(Err(e)) => error!("{:?}", e),
            Err(e) => error!("{:?}", e),
            _ => {}
        }
        tokio::time::sleep(Duration::from_secs(60 * 10)).await
    }
}

pub async fn export(State(ctx): State<AppState>) -> Result<Bytes, AppError> {
    let x = ctx
        .duckdb
        .data
        .read()
        .map_err(|_| anyhow::anyhow!("poisoned mutex"))?;
    return Ok(x.clone());
}
