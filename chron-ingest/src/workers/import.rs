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
