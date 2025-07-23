use std::{i64, num::NonZero, sync::Arc, time::Duration};

use chron_base::ChronConfig;
use dashmap::DashSet;
use futures::{StreamExt, TryStreamExt, stream};
use itertools::Itertools;
use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    errors::{DbError, DeserializationError, ExecutionError, RequestAttemptError},
    response::{PagingState, query_result::QueryResult},
    serialize::row::SerializeRow,
    statement::{batch::Batch, prepared::PreparedStatement},
    value::{CqlTimestamp, MaybeUnset},
};
use sqlx::types::{Json, JsonRawValue};
use time::OffsetDateTime;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    ChronDb, json_hash,
    models::{EntityKind, EntityObservationRaw, NewObject},
};

struct ChronPreparedStatements {
    insert_observation: PreparedStatement,

    query_latest_version: PreparedStatement,
    up_latest_version_lwt: PreparedStatement,
    append_new_version: Batch,
    insert_version_raw: PreparedStatement,
    insert_first_latest_version_lwt: PreparedStatement,

    query_object: PreparedStatement,
    query_object_bulk: PreparedStatement,
    insert_object: PreparedStatement,
    set_latest_version: PreparedStatement,
    query_observations: PreparedStatement,

    query_all_kind_entity_ids: PreparedStatement,
}

#[derive(Clone)]
pub struct ChronScyllaDb {
    pub session: Arc<Session>,
    prepared_statements: Arc<ChronPreparedStatements>,
    pub saved_objects: Arc<DashSet<Uuid>>,
}

const VALID_TO_SENTINEL: CqlTimestamp = CqlTimestamp(i64::MAX);

async fn init(sess: &Session) -> anyhow::Result<()> {
    // sess.query_unpaged("DROP KEYSPACE cashews;", &[]).await?;
    sess.query_unpaged("CREATE KEYSPACE IF NOT EXISTS cashews WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND TABLETS = {'enabled': false};", &[]).await?;
    sess.query_unpaged("CREATE TABLE IF NOT EXISTS cashews.objects (hash uuid PRIMARY KEY, data blob) WITH compression = {'sstable_compression': 'ZstdWithDictsCompressor'} AND compaction = { 'class' : 'IncrementalCompactionStrategy' };", &[]).await?;
    sess.query_unpaged("CREATE TABLE IF NOT EXISTS cashews.observations (kind smallint, entity_id text, timestamp timestamp, hash uuid, request_time float, PRIMARY KEY ((kind, entity_id), timestamp)) WITH compression = {'sstable_compression': 'ZstdWithDictsCompressor'} AND compaction = { 'class' : 'IncrementalCompactionStrategy' };", &[]).await?;
    sess.query_unpaged("CREATE TABLE IF NOT EXISTS cashews.versions (kind smallint, entity_id text, valid_from timestamp, valid_to timestamp, hash uuid, data blob, PRIMARY KEY ((kind, entity_id), valid_from)) WITH CLUSTERING ORDER BY (valid_from DESC) AND compression = {'sstable_compression': 'ZstdWithDictsCompressor'} AND compaction = { 'class' : 'IncrementalCompactionStrategy' };", &[]).await?;
    sess.query_unpaged("CREATE TABLE IF NOT EXISTS cashews.latest_versions (kind smallint, entity_id text, valid_from timestamp, hash uuid, PRIMARY KEY (kind, entity_id)) WITH compaction = { 'class' : 'IncrementalCompactionStrategy' };", &[]).await?;
    // sess.query_unpaged(
    //     "CREATE INDEX IF NOT EXISTS ON cashews.versions ((kind, entity_id), valid_from);",
    //     &[],
    // )
    // .await?;

    sess.query_unpaged(
        "DROP MATERIALIZED VIEW IF EXISTS cashews.versions_by_kind",
        &[],
    )
    .await?;
    sess.query_unpaged(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS cashews.versions_by_kind AS
SELECT kind, entity_id, valid_from, valid_to, hash
FROM cashews.versions
WHERE kind IS NOT NULL and entity_id IS NOT NULL and valid_from IS NOT NULL
PRIMARY KEY (kind, valid_from, entity_id)
WITH CLUSTERING ORDER BY (valid_from DESC, entity_id ASC)",
        &[],
    )
    .await?;

    Ok(())
}

async fn prepare_statements(sess: &Session) -> anyhow::Result<ChronPreparedStatements> {
    let mut up_version = Batch::new(scylla::statement::batch::BatchType::Logged);
    up_version.append_statement("UPDATE cashews.versions SET valid_to = ? WHERE kind = ? AND entity_id = ? AND valid_from = ?");
    up_version.append_statement("INSERT INTO cashews.versions (kind, entity_id, valid_from, valid_to, hash, data) VALUES (?, ?, ?, ?, ?, ?)");

    Ok(ChronPreparedStatements {
        insert_observation: sess.prepare("INSERT INTO cashews.observations (kind, entity_id, timestamp, hash, request_time) VALUES (?, ?, ?, ?, ?)").await?,
        up_latest_version_lwt: sess.prepare("UPDATE cashews.latest_versions SET valid_from = ?, hash = ? WHERE kind = ? AND entity_id = ? IF valid_from = ? AND hash = ?").await?,
        query_latest_version: sess.prepare("SELECT valid_from, hash FROM cashews.latest_versions WHERE kind = ? AND entity_id = ?").await?,
        append_new_version: sess.prepare_batch(&up_version).await?,
        insert_version_raw: sess.prepare("INSERT INTO cashews.versions (kind, entity_id, valid_from, valid_to, hash, data) VALUES (?, ?, ?, ?, ?, ?)").await?,
        insert_first_latest_version_lwt: sess.prepare("INSERT INTO cashews.latest_versions (kind, entity_id, valid_from, hash) VALUES (?, ?, ?, ?) IF NOT EXISTS").await?,
        query_object: sess.prepare("SELECT data FROM cashews.objects WHERE hash = ?").await?,
        query_object_bulk: sess.prepare("SELECT data FROM cashews.objects WHERE hash IN ?").await?,
        insert_object: sess.prepare("INSERT INTO cashews.objects (hash, data) VALUES (?, ?)").await?,
        set_latest_version: sess.prepare("INSERT INTO cashews.latest_versions (kind, entity_id, valid_from, hash) VALUES (?, ?, ?, ?)").await?,
        query_observations: sess.prepare("SELECT timestamp, hash FROM cashews.observations WHERE kind = ? AND entity_id = ?").await?,
        query_all_kind_entity_ids: sess.prepare("SELECT DISTINCT kind, entity_id FROM cashews.observations").await?
    })
}

impl ChronScyllaDb {
    pub async fn new(config: &ChronConfig) -> anyhow::Result<ChronScyllaDb> {
        let session = SessionBuilder::new()
            .known_node(&config.scylla_uri)
            // .pool_size(scylla::client::PoolSize::PerShard(
            //     NonZero::new(10).unwrap(),
            // ))
            .build()
            .await?;
        let session = Arc::new(session);

        init(&session).await?;
        let db = ChronScyllaDb {
            prepared_statements: Arc::new(prepare_statements(&session).await?),
            session: session,
            saved_objects: Arc::new(DashSet::new()),
        };
        Ok(db)
    }

    pub async fn save(&self, obj: NewObject) -> anyhow::Result<()> {
        let hash = self.save_object(obj.data).await?;

        // kind, entity_id, timestamp, hash, request_time
        let ts = CqlTimestamp((obj.timestamp.unix_timestamp_nanos() / 1_000_000) as i64);
        self.try_execute_unpaged(
            &self.prepared_statements.insert_observation,
            &(
                obj.kind as i16,
                &obj.entity_id,
                ts,
                hash,
                obj.request_time.as_seconds_f32(),
            ),
        )
        .await?;

        // self.prepared_statements.insert_object.log
        self.insert_version(obj.kind, obj.entity_id, hash, obj.timestamp)
            .await?;
        Ok(())
    }

    pub async fn try_execute_unpaged(
        &self,
        prepared: &PreparedStatement,
        values: &impl SerializeRow,
    ) -> anyhow::Result<QueryResult> {
        loop {
            let res = self.session.execute_unpaged(prepared, values).await;

            match res {
                Ok(r) => return Ok(r),
                Err(ExecutionError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::WriteTimeout { .. },
                    _,
                ))) => {
                    warn!("write timeout, retrying...");
                    let sleep_dur = rand::random_range(0.5..1.5);
                    tokio::time::sleep(Duration::from_secs_f64(sleep_dur)).await;
                }
                Err(ExecutionError::LastAttemptError(RequestAttemptError::DbError(
                    DbError::ReadTimeout { .. },
                    _,
                ))) => {
                    warn!("read timeout, retrying...");
                }
                Err(e) => {
                    error!("{:?}", e);
                    return Err(e.into());
                }
            }
        }
    }

    pub async fn save_object(&self, data: serde_json::Value) -> anyhow::Result<Uuid> {
        let (hash, data) = json_hash(data)?;

        if self.saved_objects.contains(&hash) {
            return Ok(hash);
        }

        let data_json = serde_json::to_vec(&data)?;
        self.try_execute_unpaged(&self.prepared_statements.insert_object, &(hash, &data_json))
            .await?;

        // loop {
        //     let res = self
        //         .session
        //         .execute_unpaged(&self.prepared_statements.insert_object, (hash, &data_json))
        //         .await;

        //     match res {
        //         Ok(_) => break,
        //         Err(ExecutionError::LastAttemptError(RequestAttemptError::DbError(
        //             DbError::WriteTimeout { .. },
        //             _,
        //         ))) => {
        //             info!("write timeout, retrying...");
        //         }
        //         Err(e) => {
        //             info!("{:?}", e);
        //             return Err(e.into());
        //         }
        //     }
        // }

        self.saved_objects.insert(hash);
        Ok(hash)
    }

    pub async fn try_get_object(&self, hash: Uuid) -> anyhow::Result<Option<Vec<u8>>> {
        let object_res = self
            .session
            .execute_unpaged(&self.prepared_statements.query_object, (hash,))
            .await?;
        let rr = object_res.into_rows_result()?;
        let row: Option<(Vec<u8>,)> = rr.maybe_first_row()?;
        Ok(row.map(|x| x.0))
    }

    pub async fn insert_version(
        &self,
        kind: EntityKind,
        entity_id: String,
        hash: Uuid,
        valid_from: OffsetDateTime,
    ) -> anyhow::Result<()> {
        let vf = CqlTimestamp((valid_from.unix_timestamp_nanos() / 1_000_000) as i64);

        loop {
            let lv = self
                .try_execute_unpaged(
                    &self.prepared_statements.query_latest_version,
                    &(kind as i16, &entity_id),
                )
                .await?;
            let rr = lv.into_rows_result()?;

            if let Some((old_valid_from, old_hash)) =
                rr.maybe_first_row::<(CqlTimestamp, Uuid)>()?
            {
                info!("inserting new version: {:?}/{}", kind, entity_id);
                let res = self
                    .try_execute_unpaged(
                        &self.prepared_statements.up_latest_version_lwt,
                        &(vf, hash, kind as i16, &entity_id, old_valid_from, old_hash),
                    )
                    .await?;

                let rr = res.into_rows_result()?;
                if !rr.first_row::<(bool, Uuid, CqlTimestamp)>()?.0 {
                    info!("APPLY FAILED: {:?}/{}", kind, entity_id);
                    continue;
                }
                // todo: check answers

                let obj = self.try_get_object(hash).await?;

                let res = self
                    .session
                    .batch(
                        &self.prepared_statements.append_new_version,
                        (
                            (vf, kind as i16, &entity_id, old_valid_from),
                            (
                                kind as i16,
                                &entity_id,
                                // new_seq,
                                vf,
                                VALID_TO_SENTINEL,
                                hash,
                                MaybeUnset::from_option(obj),
                            ),
                        ),
                    )
                    .await?;
                return Ok(());
            } else {
                info!("inserting first row: {:?}/{}", kind, entity_id);
                self.try_execute_unpaged(
                    &self.prepared_statements.insert_first_latest_version_lwt,
                    &(kind as i16, &entity_id, vf, hash),
                )
                .await?;

                //                 let rr = res.into_rows_result()?;
                // if !rr.first_row::<(bool, Uuid, CqlTimestamp)>()?.0 {
                //     info!("APPLY FAILED");
                //     continue;
                // }

                let obj = self.try_get_object(hash).await?;

                self.try_execute_unpaged(
                    &self.prepared_statements.insert_version_raw,
                    &(
                        kind as i16,
                        &entity_id,
                        vf,
                        VALID_TO_SENTINEL,
                        hash,
                        MaybeUnset::from_option(obj),
                    ),
                )
                .await?;

                return Ok(());
            }
        }
    }

    async fn rebuild_entity(&self, kind: EntityKind, entity_id: &str) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(
                &self.prepared_statements.set_latest_version,
                (kind as i16, entity_id, VALID_TO_SENTINEL, Uuid::nil()),
            )
            .await?;

        let mut last_ver: Option<(CqlTimestamp, Uuid)> = None;

        let mut paging_state = PagingState::start();
        // self.session.exe
        loop {
            let (page, nps) = self
                .session
                .execute_single_page(
                    &self.prepared_statements.query_observations,
                    (kind as i16, entity_id),
                    paging_state,
                )
                .await?;

            let rr = page.into_rows_result()?;
            let chunk_size = 10;
            for chunk in &rr.rows::<(CqlTimestamp, Uuid)>()?.chunks(chunk_size) {
                let mut values = Vec::with_capacity(chunk_size);
                for row in chunk {
                    let (timestamp, hash) = row?;

                    if let Some((last_timestamp, last_hash)) = last_ver {
                        if hash != last_hash {
                            let obj = self.try_get_object(last_hash).await?;
                            values.push((
                                kind as i16,
                                entity_id,
                                last_timestamp,
                                timestamp,
                                last_hash,
                                MaybeUnset::from_option(obj),
                            ));
                            // self.session
                            //     .execute_unpaged(
                            //         &self.prepared_statements.insert_version_raw,
                            //         ,
                            //     )
                            // .await?;
                            last_ver = Some((timestamp, hash));
                        }
                    } else {
                        last_ver = Some((timestamp, hash));
                    }
                }

                execute_batched(
                    &self.session,
                    &self.prepared_statements.insert_version_raw,
                    values,
                )
                .await?;
            }
            match nps.into_paging_control_flow() {
                std::ops::ControlFlow::Continue(nps) => {
                    paging_state = nps;
                }
                std::ops::ControlFlow::Break(_) => break,
            }
        }

        // last
        if let Some((last_timestamp, last_hash)) = last_ver {
            let obj = self.try_get_object(last_hash).await?;
            self.session
                .execute_unpaged(
                    &self.prepared_statements.insert_version_raw,
                    (
                        kind as i16,
                        &entity_id,
                        last_timestamp,
                        VALID_TO_SENTINEL,
                        last_hash,
                        MaybeUnset::from_option(obj),
                    ),
                )
                .await?;

            self.session
                .execute_unpaged(
                    &self.prepared_statements.set_latest_version,
                    (kind as i16, entity_id, last_timestamp, last_hash),
                )
                .await?;
        }

        Ok(())
    }

    pub async fn import_sql_chron(&self, db: &ChronDb) -> anyhow::Result<()> {
        let mut count = 0;

        let mut obj_stream =
            sqlx::query_as::<_, (Uuid, Json<Box<JsonRawValue>>)>("select hash, data from objects")
                .fetch(&db.pool);

        count = 0;
        while let Some((hash, data)) = obj_stream.try_next().await? {
            loop {
                let res = self
                    .session
                    .execute_unpaged(
                        &self.prepared_statements.insert_object,
                        (hash, data.0.get().as_bytes()),
                    )
                    .await;

                match res {
                    Ok(_) => break,
                    Err(ExecutionError::LastAttemptError(RequestAttemptError::DbError(
                        DbError::WriteTimeout { .. },
                        _,
                    ))) => {
                        info!("write timeout, retrying...");
                    }
                    Err(e) => {
                        info!("{:?}", e);
                        return Err(e.into());
                    }
                }
            }

            count += 1;
            if count % 1000 == 0 {
                info!("objects: {}", count);
            }
        }

        let mut obs_stream = sqlx::query_as::<_, EntityObservationRaw>(
            "select kind, entity_id, hash, timestamp, request_time from observations order by kind, entity_id, timestamp",
        )
        .fetch(&db.pool);
        count = 0;

        let chunk_size = 100;
        let mut values = Vec::new();
        while let Some(obs) = obs_stream.try_next().await? {
            let ts = CqlTimestamp((obs.timestamp.unix_timestamp_nanos() / 1_000_000) as i64);
            values.push((
                obs.kind as i16,
                obs.entity_id,
                ts,
                obs.hash,
                obs.request_time as f32,
            ));

            if values.len() >= chunk_size {
                execute_batched(
                    &self.session,
                    &self.prepared_statements.insert_observation,
                    &values,
                )
                .await?;
                values.clear();
            }

            count += 1;
            if count % 1000 == 0 {
                info!("observations: {}", count);
            }
        }

        if values.len() > 0 {
            execute_batched(
                &self.session,
                &self.prepared_statements.insert_observation,
                &values,
            )
            .await?;
        }

        let res = self
            .session
            .execute_unpaged(&self.prepared_statements.query_all_kind_entity_ids, &[])
            .await?;
        let rr = res.into_rows_result()?;
        let total = rr.rows_num();

        async fn inner(
            s: &ChronScyllaDb,
            (i, row): (usize, Result<(i16, String), DeserializationError>),
            total: usize,
        ) -> anyhow::Result<()> {
            let (kind, entity_id) = row?;

            if let Some(kind) = EntityKind::from_repr(kind) {
                if i % 100 == 0 {
                    info!("rebuilding {:?}/{} ({}/{})", kind, entity_id, i, total);
                }
                s.rebuild_entity(kind, &entity_id).await?;
            }
            Ok(())
        }

        stream::iter(rr.rows::<(i16, String)>()?.enumerate())
            .for_each_concurrent(10, |r| async move {
                if let Err(e) = inner(self, r, total).await {
                    error!("error: {:?}", e);
                }
            })
            .await;

        Ok(())
    }
}

async fn execute_batched(
    sess: &Session,
    statement: &PreparedStatement,
    data: impl IntoIterator<Item = impl SerializeRow>,
) -> anyhow::Result<()> {
    let items = data.into_iter().collect::<Vec<_>>();
    let mut batch = Batch::new(scylla::statement::batch::BatchType::Unlogged);

    for _ in 0..items.len() {
        batch.append_statement(statement.clone());
    }

    sess.batch(&batch, items).await?;
    Ok(())
}
