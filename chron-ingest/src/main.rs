use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use chron_base::load_config;
use chron_db::ChronDb;
use http::DataClient;
use tracing::{error, info};
use uuid::Uuid;
use workers::{
    IntervalWorker, SimState, WorkerContext, crunch,
    games::{self},
    import,
    league::{self, PollLeague, PollNewPlayers},
    matviews::RefreshMatviews,
};

use crate::workers::{
    games::{PollAllScheduledGames, PollLiveGames, PollSchedules},
    league::PollAllPlayers,
    map::{self, LookupMapLocations},
    message::PollMessage,
};

mod http;
mod models;
mod workers;

fn spawn<T: IntervalWorker + 'static>(mut ctx: WorkerContext, mut w: T) {
    tokio::spawn(async move {
        // let pin_w = pin_w;

        let mut interval = T::interval();
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // add some jitter
        if ctx.config.jitter {
            let jitter_amount = rand::random_range(0.1..=1.0);
            tokio::time::sleep(interval.period().div_f64(jitter_amount)).await;
            interval.reset_immediately();
        }

        let type_name = std::any::type_name::<T>().split("::").last().unwrap();
        loop {
            interval.tick().await;

            info!("running: {}", type_name);
            w.tick(&mut ctx).await.unwrap_or_else(|e| {
                let type_name = std::any::type_name::<T>().split("::").last().unwrap();
                error!("error executing worker {}: {:?}", type_name, e);
            });
            info!("done: {}", type_name);
        }
    });
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Arc::new(load_config()?);

    let client = DataClient::new()?;
    let ctx = WorkerContext {
        client,
        db: ChronDb::new(&config).await?,
        config: config,
        _sim: Arc::new(RwLock::new(SimState {
            _season: Uuid::default(),
            _day: -1,
        })),
    };

    let args = std::env::args().collect::<Vec<_>>();
    if args.len() > 1 {
        if let Err(e) = handle_fn(&ctx, &args[1], &args[2..]).await {
            error!("error running cli: {:?}", e);
        }
        Ok(())
    } else {
        spawn(ctx.clone(), PollLeague);
        spawn(ctx.clone(), PollNewPlayers);
        spawn(ctx.clone(), RefreshMatviews);
        spawn(ctx.clone(), PollMessage);
        spawn(ctx.clone(), PollSchedules);
        spawn(ctx.clone(), PollLiveGames);
        spawn(ctx.clone(), PollAllPlayers);
        spawn(ctx.clone(), PollAllScheduledGames);
        spawn(ctx.clone(), LookupMapLocations);

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn handle_fn(ctx: &WorkerContext, name: &str, args: &[String]) -> anyhow::Result<()> {
    match name {
        "rebuild-games" => games::rebuild_games(ctx).await?,
        "rebuild-games-slow" => games::rebuild_games_slow(ctx).await?,
        "fetch-all-games" => games::fetch_all_games(ctx).await?,
        "fetch-all-players" => league::fetch_all_players(ctx).await?,
        "import-db" => import::import(ctx, &args[0]).await?,
        "crunch" => crunch::crunch(ctx).await?,
        _ => panic!("unknown function: {}", name),
    }

    Ok(())
}
