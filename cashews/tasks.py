import threading, time, random, sys, logging
arg2 = sys.argv[1] if len(sys.argv) > 1 else ""

logging.basicConfig(level="INFO", format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s")

def run_every_thread(name, interval, real_inner, random_delta=60):
    def inner():
        logger = logging.getLogger("tasks." + name)
        from cashews import utils
        utils.set_log(logger)

        if arg2 and arg2 != name:
            logger.warning("skipping %s", name)
            return

        presleep_time = random.random() * random_delta
        if arg2:
            # if specifying a specific task, no presleep
            presleep_time = 0
        logger.info("pre-sleeping %f", presleep_time)
        time.sleep(presleep_time)

        while True:
            before = time.time()
            logger.info("running")

            try:
                real_inner()
            except Exception as e:
                logger.error("error running task", exc_info=e)

            after = time.time()
            took = after - before
            logger.info("done, took %f", took)
            remaining = interval - took
            while remaining < 0:
                logger.info("missed a round, skipping")
                remaining += interval

            remaining += random.random() * random_delta + (random_delta/2)
            logger.info("sleeping for %f", remaining)
            time.sleep(remaining)

    return inner

def main():
    from cashews import fetch_games, fetch_league, maps

    if arg2 == "backfill":
        fetch_games.backfill_game_ids()
        return
    if arg2 == "backfill_players":
        fetch_league.backfill_player_data()
        return
    if arg2 == "league":
        fetch_league.fetch_league()
        return
    if arg2 == "players":
        fetch_league.fetch_players()
        return
    if arg2 == "refresh_players":
        fetch_league.refresh_known_players()
        return
    if arg2 == "maps":
        maps.fill_locations()
        return

    funcs = [
        run_every_thread("fetch_games", 3*60, lambda: fetch_games.fetch_games(False)),
        run_every_thread("fetch_league", 1*60, fetch_league.fetch_league),
        run_every_thread("fetch_players", 90*60, fetch_league.fetch_players),
        run_every_thread("fetch_election", 5*60, fetch_league.fetch_election),
        run_every_thread("fetch_new_games", 3*60, lambda: fetch_games.fetch_games(True)),
        run_every_thread("refetch_unfinished", 3*60, fetch_games.refetch_unfinished_known_games),
        run_every_thread("refetch_players", 30*60, fetch_league.refresh_known_players),
        run_every_thread("lookup_locations", 1*60, maps.fill_locations),
    ]

    threads = []
    for f in funcs:
        thread = threading.Thread(target=f)
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()