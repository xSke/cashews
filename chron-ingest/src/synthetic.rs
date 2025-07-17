use chron_db::models::{EntityKind, NewObject};
use futures::TryStreamExt;
use time::{Duration, OffsetDateTime};
use tracing::info;

use crate::workers::WorkerContext;

struct SyntheticObject {
    kind: EntityKind,
    entity_id: String,
    data: serde_json::Value,
}

pub async fn handle_incoming(
    ctx: &WorkerContext,
    entity_kind: EntityKind,
    entity_id: &str,
    data: &serde_json::Value,
    timestamp: OffsetDateTime,
) -> anyhow::Result<()> {
    let new_objs = derive_from(entity_kind, entity_id, data)?;
    for obj in new_objs {
        ctx.db
            .save(NewObject {
                kind: obj.kind,
                entity_id: obj.entity_id,
                data: obj.data,
                timestamp,
                request_time: Duration::ZERO,
            })
            .await?;
    }

    Ok(())
}

pub async fn rebuild_players(ctx: &WorkerContext) -> anyhow::Result<()> {
    rebuild_type_and_clear(
        ctx,
        EntityKind::Player,
        &[
            EntityKind::PlayerLite,
            EntityKind::Talk,
            EntityKind::TalkBatting,
            EntityKind::TalkPitching,
            EntityKind::TalkBaserunning,
            EntityKind::TalkDefense,
        ],
    )
    .await?;
    Ok(())
}

pub async fn rebuild_teams(ctx: &WorkerContext) -> anyhow::Result<()> {
    rebuild_type_and_clear(ctx, EntityKind::Team, &[EntityKind::TeamLite]).await?;
    Ok(())
}

async fn rebuild_type_and_clear(
    ctx: &WorkerContext,
    main_kind: EntityKind,
    derived_kinds: &[EntityKind],
) -> anyhow::Result<()> {
    for k in derived_kinds {
        info!("cleaning {:?}...", k);
    }

    rebuild_type(ctx, main_kind).await?;

    for k in derived_kinds {
        info!("rebuilding versions for {:?}...", k);
        ctx.db.rebuild_all(*k).await?;
    }

    Ok(())
}

async fn rebuild_type(ctx: &WorkerContext, kind: EntityKind) -> anyhow::Result<()> {
    let ids = ctx.db.get_all_entity_ids(kind).await?;
    ctx.process_many_with_progress(ids, 10, &format!("rebuild {:?}", kind), |ctx, id| {
        rebuild_entity(ctx, kind, id)
    })
    .await;

    info!("finished rebuilding {:?}", kind);
    Ok(())
}

async fn rebuild_entity(ctx: &WorkerContext, kind: EntityKind, id: String) -> anyhow::Result<()> {
    let mut stream = ctx.db.get_versions_stream(kind, &id);

    let mut objects = Vec::new();
    while let Some(ver) = stream.try_next().await? {
        let data: serde_json::Value = ver.parse()?;
        objects.extend(
            derive_from(ver.kind, &ver.entity_id, &data)?
                .into_iter()
                .map(|x: SyntheticObject| NewObject {
                    kind: x.kind,
                    entity_id: x.entity_id,
                    data: x.data,
                    timestamp: ver.valid_from.0,
                    request_time: Duration::ZERO,
                }),
        );
    }

    ctx.db.save_raw_bulk(objects).await?;

    Ok(())
}

fn derive_from(
    entity_kind: EntityKind,
    entity_id: &str,
    data: &serde_json::Value,
) -> anyhow::Result<Vec<SyntheticObject>> {
    match entity_kind {
        EntityKind::Player => derive_from_player(entity_id, data),
        EntityKind::Team => derive_from_team(entity_id, data),
        _ => Ok(Vec::new()),
    }
}

fn derive_from_team(id: &str, data: &serde_json::Value) -> anyhow::Result<Vec<SyntheticObject>> {
    let mut objects = Vec::new();

    let mut team_lite = data.clone();
    to_team_lite(&mut team_lite);
    objects.push(SyntheticObject {
        kind: EntityKind::TeamLite,
        entity_id: id.to_string(),
        data: team_lite,
    });

    Ok(objects)
}

fn derive_from_player(id: &str, data: &serde_json::Value) -> anyhow::Result<Vec<SyntheticObject>> {
    let mut objects = Vec::new();

    let mut player_lite = data.clone();
    to_player_lite(&mut player_lite);
    objects.push(SyntheticObject {
        kind: EntityKind::PlayerLite,
        entity_id: id.to_string(),
        data: player_lite,
    });

    if let Some(talk) = data.as_object().and_then(|x| x.get("Talk")) {
        objects.push(SyntheticObject {
            kind: EntityKind::Talk,
            entity_id: id.to_string(),
            data: talk.clone(),
        });

        for (key, kind) in [
            ("Batting", EntityKind::TalkBatting),
            ("Pitching", EntityKind::TalkPitching),
            ("Baserunning", EntityKind::TalkBaserunning),
            ("Defense", EntityKind::TalkDefense),
        ] {
            if let Some(inner) = talk.as_object().and_then(|x| x.get(key)) {
                objects.push(SyntheticObject {
                    kind,
                    entity_id: id.to_string(),
                    data: inner.clone(),
                });
            }
        }
    }

    Ok(objects)
}

fn to_team_lite(data: &mut serde_json::Value) {
    // can we make this code nicer?
    if let Some(o) = data.as_object_mut() {
        if let Some(players_value) = o.get_mut("Players") {
            if let Some(players) = players_value.as_array_mut() {
                for player_value in players {
                    if let Some(p) = player_value.as_object_mut() {
                        p.remove("Stats");
                    }
                }
            }
        }
        o.remove("Feed");
    }
}

fn to_player_lite(data: &mut serde_json::Value) {
    if let Some(o) = data.as_object_mut() {
        o.remove("Stats");
        o.remove("Feed");
    }
}
