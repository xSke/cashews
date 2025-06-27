use std::{
    io::Cursor,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
// use polars_arrow::array::{BinaryViewArrayGeneric};
use crate::AppState;
use chron_base::StatKey;
use futures::TryStreamExt;
use polars::{
    frame::DataFrame,
    io::SerReader,
    prelude::{
        CategoricalOrdering, CsvReadOptions, DataType, RevMapping, Schema, SchemaExt,
        create_enum_dtype,
    },
};
use polars_arrow::array::Utf8ViewArray;
use sqlx::Acquire;
use strum::VariantArray;
use tracing::{error, info};

pub struct PolarsState {
    pub stats: DataFrame,
    pub players: DataFrame,
    pub teams: DataFrame,
}

impl PolarsState {
    pub fn new() -> PolarsState {
        PolarsState {
            stats: DataFrame::empty_with_schema(&get_stats_schema()),
            players: DataFrame::empty_with_schema(&get_players_schema()),
            teams: DataFrame::empty_with_schema(&get_teams_schema()),
        }
    }
}

const DTYPE_TEAM_ID: LazyLock<DataType> = LazyLock::new(|| {
    DataType::Categorical(
        Some(Arc::new(RevMapping::default())),
        CategoricalOrdering::default(),
    )
});
const DTYPE_PLAYER_ID: LazyLock<DataType> = LazyLock::new(|| {
    DataType::Categorical(
        Some(Arc::new(RevMapping::default())),
        CategoricalOrdering::default(),
    )
});
const DTYPE_GAME_ID: LazyLock<DataType> = LazyLock::new(|| {
    DataType::Categorical(
        Some(Arc::new(RevMapping::default())),
        CategoricalOrdering::default(),
    )
});
const DTYPE_SLOT_POS: LazyLock<DataType> = LazyLock::new(|| {
    let slot_strs = [
        "1B", "2B", "3B", "C", "CF", "CL", "DH", "LF", "RF", "RP", "RP1", "RP2", "RP3", "SP",
        "SP1", "SP2", "SP3", "SP4", "SP5", "SS",
    ];
    create_enum_dtype(Utf8ViewArray::from_slice_values(slot_strs))
});

const DTYPE_NAME_COMPONENT: LazyLock<DataType> = LazyLock::new(|| {
    DataType::Categorical(
        Some(Arc::new(RevMapping::default())),
        CategoricalOrdering::default(),
    )
});

const DTYPE_HAND: LazyLock<DataType> =
    LazyLock::new(|| create_enum_dtype(Utf8ViewArray::from_slice_values(["L", "R", "S"])));

fn get_stats_schema() -> Schema {
    let mut schema = Schema::default();
    schema.with_column("season".into(), DataType::UInt16);
    schema.with_column("day".into(), DataType::UInt16);
    schema.with_column("game_id".into(), DTYPE_GAME_ID.clone());
    schema.with_column("player_id".into(), DTYPE_PLAYER_ID.clone());
    schema.with_column("team_id".into(), DTYPE_TEAM_ID.clone());
    schema.with_column("slot".into(), DTYPE_SLOT_POS.clone());
    for k in StatKey::VARIANTS {
        let name: &'static str = k.into();
        schema.with_column(name.into(), DataType::UInt16);
    }

    schema
}

fn get_players_schema() -> Schema {
    let mut schema = Schema::default();
    schema.with_column("player_id".into(), DTYPE_PLAYER_ID.clone());
    schema.with_column("first_name".into(), DTYPE_NAME_COMPONENT.clone());
    schema.with_column("last_name".into(), DTYPE_NAME_COMPONENT.clone());
    schema.with_column("full_name".into(), DataType::String);
    schema.with_column("team_id".into(), DTYPE_TEAM_ID.clone());
    schema.with_column("position".into(), DTYPE_SLOT_POS.clone());
    schema.with_column("throws".into(), DTYPE_HAND.clone());
    schema.with_column("bats".into(), DTYPE_HAND.clone());
    schema
}

fn get_teams_schema() -> Schema {
    let mut schema = Schema::default();
    schema.with_column("team_id".into(), DTYPE_TEAM_ID.clone());
    schema
}

/*pub async fn read_matview(ctx: &AppState, table_name: &str) -> anyhow::Result<DataFrame> {
    let mut conn = ctx.db.pool.acquire().await?;
    conn.close_on_drop();
    let mut tx = conn.begin().await?;

    let qstr = format!("create temp table tmp_table on commit drop as select * from {};", table_name);
    sqlx::query(&qstr).execute(&mut *tx.acquire().await?).await?;
    let mut stream = tx
        .copy_out_raw("COPY tmp_table TO STDOUT (FORMAT csv, HEADER true)")
        .await?;

    let mut csv_buf = Vec::new();
    while let Some(chunk) = stream.try_next().await? {
        csv_buf.extend(chunk);
    }
    drop(stream);

    sqlx::query("drop table tmp_table;")
        .execute(&mut *tx.acquire().await?)
        .await?;
    tx.commit().await?;
    conn.close().await?;

    let reader = CsvReadOptions::default()
        .with_schema(Some(Arc::new(get_schema())))
        .into_reader_with_file_handle(Cursor::new(csv_buf));
    let mut df = reader.finish()?;
    df.rechunk_mut();

    Ok(df)
}*/

pub async fn read_query(ctx: &AppState, schema: Schema, query: &str) -> anyhow::Result<DataFrame> {
    dbg!("start:", query);
    let df = {
        let mut conn = ctx.db.pool.acquire().await?;
        conn.close_on_drop();
        let mut tx = conn.begin().await?;

        let chunk_size = 1 << 17;

        let mut stream = tx
            .copy_out_raw(&format!(
                "COPY ({}) TO STDOUT (FORMAT csv, HEADER true)",
                query
            ))
            .await?;

        let header_row = stream.try_next().await?.unwrap();
        let mut csv_buf = header_row.to_vec();
        let header_len = csv_buf.len();

        // todo: can we do this while maintaining the same Categorical instance?
        let field_names = schema.iter_fields().map(|x| x.name).collect::<Arc<_>>();
        let overwrite_schema = Arc::new(schema);

        let mut stream_chunked = stream.try_chunks(chunk_size);

        let mut df = DataFrame::empty();
        while let Some(row_batch) = stream_chunked.try_next().await? {
            csv_buf.truncate(header_len);
            for row in &row_batch {
                csv_buf.extend(row);
            }

            let overwrite_schema = overwrite_schema.clone();
            let field_names = field_names.clone();
            let df_part = CsvReadOptions::default()
                .with_has_header(true)
                .with_schema_overwrite(Some(overwrite_schema))
                .with_columns(Some(field_names))
                .with_chunk_size(chunk_size)
                .into_reader_with_file_handle(Cursor::new(&csv_buf))
                .finish()?;
            df.vstack_mut(&df_part)?;
        }
        df
    };

    // let df: DataFrame = tokio::task::spawn_blocking(|| {
    //     let reader = CsvReadOptions::default()
    //         .with_schema_overwrite(Some(Arc::new(schema)))
    //         .with_has_header(true);
    //         .into_reader_with_file_handle(Cursor::new(csv_buf));
    //     reader.batched_borrowed().finish().map(|mut df| {
    //         df.rechunk_mut();
    //         df
    //     })
    // }).await??;

    Ok(df)
}

pub async fn refresh(ctx: &AppState) -> anyhow::Result<PolarsState> {
    let time_before = Instant::now();
    info!("reading stats...");

    let stats = read_query(
        ctx,
        get_stats_schema(),
        "select * from game_player_stats_exploded",
    )
    .await?;
    let players = read_query(ctx, get_players_schema(), "select * from players").await?;
    let teams = read_query(ctx, get_teams_schema(), "select * from teams").await?;

    let time_after = Instant::now();
    info!(
        "done! took {:?}\n{:?}",
        time_after.duration_since(time_before),
        &stats
    );

    Ok(PolarsState {
        stats,
        players,
        teams,
    })
}

pub async fn worker(ctx: AppState) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        match refresh(&ctx).await {
            Ok(ps) => {
                ctx.polars.store(ps);
            }
            Err(e) => error!("error refreshing polars df: {:?}", e),
        }
        tokio::time::sleep(Duration::from_secs(60 * 10)).await
    }
}
