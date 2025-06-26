use std::{
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::AppState;
use chron_base::StatKey;
use futures::TryStreamExt;
use polars::{
    frame::DataFrame,
    io::SerReader,
    prelude::{CsvReadOptions, DataType, Schema},
};
use sqlx::Acquire;
use strum::VariantArray;
use tracing::{error, info};

pub struct PolarsState {
    pub df: DataFrame,
}

impl PolarsState {
    pub fn new() -> PolarsState {
        PolarsState {
            df: DataFrame::empty_with_schema(&get_schema()),
        }
    }
}

fn get_schema() -> Schema {
    let mut schema = Schema::default();
    schema.with_column("season".into(), DataType::UInt16);
    schema.with_column("day".into(), DataType::UInt16);
    schema.with_column("game_id".into(), DataType::String);
    schema.with_column("player_id".into(), DataType::String);
    schema.with_column("team_id".into(), DataType::String);
    for k in StatKey::VARIANTS {
        let name: &'static str = k.into();
        schema.with_column(name.into(), DataType::UInt16);
    }

    schema
}

pub async fn read(ctx: &AppState) -> anyhow::Result<PolarsState> {
    let time_before = Instant::now();
    info!("reading stats...");

    let mut conn = ctx.db.pool.acquire().await?;
    conn.close_on_drop();
    let mut tx = conn.begin().await?;
    sqlx::query("create temp table tmp_game_player_stats_exploded on commit drop as select * from game_player_stats_exploded;").execute(&mut *tx.acquire().await?).await?;
    let mut stream = tx
        .copy_out_raw("COPY tmp_game_player_stats_exploded TO STDOUT (FORMAT csv, HEADER true)")
        .await?;

    let mut csv_buf = Vec::new();
    while let Some(chunk) = stream.try_next().await? {
        csv_buf.extend(chunk);
    }
    drop(stream);

    sqlx::query("drop table tmp_game_player_stats_exploded;")
        .execute(&mut *tx.acquire().await?)
        .await?;
    tx.commit().await?;
    conn.close().await?;

    info!("read {} bytes, now reading from csv", csv_buf.len());
    let reader = CsvReadOptions::default()
        .with_schema(Some(Arc::new(get_schema())))
        .into_reader_with_file_handle(Cursor::new(csv_buf));
    let mut df = reader.finish()?;
    df.rechunk_mut();

    let time_after = Instant::now();
    info!(
        "done! took {:?}\n{:?}",
        time_after.duration_since(time_before),
        &df
    );

    Ok(PolarsState { df })
}

pub async fn worker(ctx: AppState) {
    loop {
        // ctx.polars.swap()
        match read(&ctx).await {
            Ok(ps) => {
                ctx.polars.store(ps);
            }
            Err(e) => error!("error refreshing polars df: {:?}", e),
        }
        tokio::time::sleep(Duration::from_secs(60 * 10)).await
    }
}
