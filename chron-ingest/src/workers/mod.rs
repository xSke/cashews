use std::sync::{Arc, RwLock};

use chron_db::{ChronDb, models::EntityKind};
use futures::StreamExt;
use futures::stream;
use reqwest::{IntoUrl, StatusCode, Url};
use tokio::time::Interval;
use tracing::error;
use uuid::Uuid;

use crate::http::{ClientResponse, DataClient};

pub mod crunch;
pub mod games;
pub mod import;
pub mod league;

#[derive(Clone)]
pub struct WorkerContext {
    pub sim: Arc<RwLock<SimState>>,
    pub db: ChronDb,
    pub client: DataClient,
}

impl WorkerContext {
    // pub fn season_day(&self) -> (Uuid, i32) {
    //     let s = self.sim.read().expect("should never be poisoned");
    //     (s._season.clone(), s._day)
    // }

    // pub fn update_state(&self, new_state: SimState) {
    //     let mut s = self.sim.write().expect("should never be poisoned");
    //     *s = new_state;
    // }

    pub async fn fetch_and_save(
        &self,
        url: impl IntoUrl,
        kind: EntityKind,
        entity_id: impl Into<String>,
    ) -> anyhow::Result<ClientResponse> {
        let resp = self.client.fetch(url).await?;
        self.db
            .save(resp.to_chron(kind, &entity_id.into())?)
            .await?;
        Ok(resp)
    }

    pub async fn process_many<'a, T, F, Fut>(
        &'a self,
        values: impl IntoIterator<Item = T>,
        parallel: usize,
        function: F,
    ) where
        F: Fn(&'a WorkerContext, T) -> Fut,
        Fut: Future<Output = anyhow::Result<()>>,
    {
        stream::iter(values)
            .map(|x| function(&self, x))
            .buffer_unordered(parallel)
            .for_each(|res| async {
                if let Err(e) = res {
                    error!("error processing item: {:?}", e);
                }
            })
            .await
    }
}

pub trait IntervalWorker: Send + Sync {
    fn interval() -> Interval;

    fn tick(
        &mut self,
        ctx: &mut WorkerContext,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}

pub struct SimState {
    pub _season: Uuid,
    pub _day: i32,
}
