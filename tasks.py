import threading, time, random
import fetch_games
import fetch_league
import fetch_team_stats


def run_every_thread(name, interval, real_inner, random_delta=60):
    def inner():
        print(f"{name}: pre-sleeping")
        time.sleep(random.random() * random_delta)

        while True:
            before = time.time()
            print(f"{name}: running")

            try:
                real_inner()
            except Exception as e:
                print(f"!!!error!!!:", e)

            after = time.time()
            took = after - before
            print(f"{name}: done, took {took}")
            remaining = interval - took
            while remaining < 0:
                print("missed a round, skipping")
                remaining += interval

            remaining += random.random() * random_delta + (random_delta/2)
            print(f"{name}: sleeping for {remaining}")
            time.sleep(remaining)

    return inner


def fetch_games_thread():
    fetch_games.fetch_games(False)


def fetch_new_games_thread():
    fetch_games.fetch_games(True)


def fetch_league_thread():
    fetch_league.fetch_league()


def fetch_players_thread():
    fetch_league.fetch_players()


def refetch_unfinished_known_games_thread():
    fetch_games.refetch_unfinished_known_games()


def lookup_locations_thread():
    import maps
    if not maps.get_key():
        return
    
    maps.fill_locations()


def fetch_team_stats_thread():
    fetch_team_stats.fetch_team_stats()


def main():
    import sys
    arg2 = sys.argv[1] if len(sys.argv) > 1 else ""
    if arg2 == "backfill":
        fetch_games.backfill_game_ids()
        return
    if arg2 == "league":
        fetch_league.fetch_league()
        return
    if arg2 == "players":
        fetch_league.fetch_players()
        return
    if arg2 == "maps":
        import maps
        maps.fill_locations()
        return

    funcs = [
        run_every_thread("fetch_games", 3*60, fetch_games_thread),
        run_every_thread("fetch_league", 5*60, fetch_league_thread),
        run_every_thread("fetch_players", 90*60, fetch_players_thread),
        run_every_thread("fetch_new_games", 3*60, fetch_new_games_thread),
        run_every_thread("refetch_unfinished", 3*60, refetch_unfinished_known_games_thread),
        run_every_thread("lookup_locations", 30*60, lookup_locations_thread),
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