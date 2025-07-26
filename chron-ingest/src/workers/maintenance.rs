use chron_db::models::EntityKind;
use strum::VariantArray;
use tracing::info;

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
