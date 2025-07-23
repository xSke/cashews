use crate::workers::WorkerContext;

pub async fn init(ctx: &WorkerContext) -> anyhow::Result<()> {
    ctx.scylla.import_sql_chron(&ctx.db).await?;

    Ok(())
}
