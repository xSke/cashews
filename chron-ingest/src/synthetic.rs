use std::collections::HashMap;

use chron_db::{
    json_hash,
    models::{EntityKind, NewObject},
};
use serde::Deserialize;
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
    ctx.process_many_with_progress(ids, 3, &format!("rebuild {:?}", kind), |ctx, id| {
        rebuild_entity(ctx, kind, id)
    })
    .await;

    info!("finished rebuilding {:?}", kind);
    Ok(())
}

async fn rebuild_entity(ctx: &WorkerContext, kind: EntityKind, id: String) -> anyhow::Result<()> {
    let versions_lite = ctx.db.get_versions_lite(kind, &id).await?;

    let mut new_objects = HashMap::new();
    let mut new_obs = Vec::new();

    for ver in versions_lite {
        let data = ctx.db.get_object(ver.hash).await?;
        if let Some(data) = data {
            let value = serde_json::Value::deserialize(&*data.0)?;

            for s in derive_from(ver.kind, &ver.entity_id, &value)? {
                let (hash, data) = json_hash(s.data)?;
                if !new_objects.contains_key(&hash) && !ctx.db.saved_objects.contains(&hash) {
                    new_objects.insert(hash, data);
                }

                new_obs.push((s.kind, s.entity_id, ver.valid_from.0, 0.0, hash));
            }
        }
    }

    let mut hashes = Vec::with_capacity(new_objects.len());
    let mut datas = Vec::with_capacity(new_objects.len());
    for (hash, data) in new_objects.iter() {
        hashes.push(*hash);
        datas.push(data);
    }
    ctx.db.save_objects_raw_bulk(&hashes, &datas).await?;
    ctx.db.insert_observations_raw_bulk(&new_obs).await?;

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
