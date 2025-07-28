use std::sync::{Arc, RwLock};

use chron_base::{load_config, stop_signal};
use chron_db::ChronDb;
use http::DataClient;
use tracing::{error, info};
use uuid::Uuid;
use workers::{
    IntervalWorker, SimState, WorkerContext,
    games::{self},
    league::{self},
    maintenance,
};

use crate::workers::{feeds::ProcessFeeds, games::HandleSuperstarGames, league::PollBenches};
use crate::workers::{
    games::{HandleEventGames, PollGameDays, PollLiveGames},
    league::{PollAllPlayers, PollLeague, PollNewPlayers},
    map::LookupMapLocations,
    matviews::RefreshMatviews,
    message::PollMessage,
    misc::PollMiscData,
};

mod http;
mod models;
mod synthetic;
mod workers;

fn spawn<T: IntervalWorker + 'static>(mut ctx: WorkerContext, mut w: T) {
    tokio::spawn(async move {
        let mut interval = T::interval();
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let type_name = std::any::type_name::<T>().split("::").last().unwrap();

        // add some jitter to prevent hammering the server on ingest startup
        if ctx.config.jitter {
            let jitter_amount = rand::random_range(0.1..=1.0);
            let sleep_duration = interval.period().mul_f64(jitter_amount);
            info!("{}: sleeping for {:?}", type_name, sleep_duration);
            tokio::time::sleep(sleep_duration).await;
            interval.reset_immediately();
        }

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

    let args = std::env::args().collect::<Vec<_>>();
    // todo: this is stupid
    let db = if args.len() > 1 && args[1] == "migrate" {
        ChronDb::new_from_scratch(&config).await?
    } else {
        ChronDb::new(&config).await?
    };
    let client = DataClient::new()?;
    let ctx = WorkerContext {
        client,
        db,
        config: config,
        _sim: Arc::new(RwLock::new(SimState {
            _season: Uuid::default(),
            _day: -1,
        })),
    };

    if args.len() > 1 {
        let handle_fut = handle_fn(&ctx, &args[1], &args[2..]);
        let ctrl_c_fut = stop_signal();
        tokio::select! {
            result = handle_fut => {
                if let Err(e) = result {
                    error!("error running cli: {:?}", e);
                }
            },
            _ = ctrl_c_fut => {
                info!("got ctrl-c, cancelling");
                return Ok(());
            }
        };
        Ok(())
    } else {
        spawn(ctx.clone(), PollLeague);
        spawn(ctx.clone(), PollNewPlayers);
        spawn(ctx.clone(), PollBenches);
        spawn(ctx.clone(), RefreshMatviews);
        spawn(ctx.clone(), PollMessage);
        spawn(ctx.clone(), PollGameDays);
        spawn(ctx.clone(), PollLiveGames);
        spawn(ctx.clone(), PollAllPlayers);
        spawn(ctx.clone(), PollMiscData);
        spawn(ctx.clone(), LookupMapLocations);
        spawn(ctx.clone(), HandleEventGames);
        spawn(ctx.clone(), HandleSuperstarGames);
        spawn(ctx.clone(), ProcessFeeds);

        stop_signal().await?;
        info!("got ctrl-c, exiting");
        Ok(())
    }
}

async fn handle_fn(ctx: &WorkerContext, name: &str, args: &[String]) -> anyhow::Result<()> {
    match name {
        "rebuild-games" => games::rebuild_games(ctx, false).await?,
        "rebuild-games-stats" => games::rebuild_games(ctx, true).await?,
        "rebuild-games-slow" => games::rebuild_games_slow(ctx).await?,
        "rebuild-all" => maintenance::rebuild_all(ctx).await?,
        "recompress" => maintenance::recompress(ctx).await?,
        "fetch-league" => league::poll_league(ctx).await?,
        "fetch-all-seasons" => games::fetch_all_seasons(ctx).await?,
        "fetch-all-games" => games::fetch_all_games(ctx).await?,
        "fetch-all-new-games" => games::fetch_all_new_or_incomplete_games(ctx).await?,
        "fetch-all-players" => league::fetch_all_players(ctx).await?,
        "rebuild-teams" => synthetic::rebuild_teams(ctx).await?,
        "rebuild-players" => synthetic::rebuild_players(ctx).await?,
        "migrate" => ctx.db.migrate(false).await?,
        "migrate-full" => ctx.db.migrate(true).await?,
        _ => panic!("unknown function: {}", name),
    }

    Ok(())
}
