import threading, time, random

def run_every(name, seconds, random_delta=60):
    print(f"{name}: pre-sleeping")
    time.sleep(random.random() * random_delta)
    while True:
        before = time.time()
        print(f"{name}: running")
        yield None
        after = time.time()
        took = after - before
        print(f"{name}: done, took {took}")
        remaining = seconds - took
        while remaining < 0:
            print("missed a round, skipping")
            remaining += seconds
        remaining += random.random() * random_delta + (random_delta/2)
        print(f"{name}: sleeping for {remaining}")
        time.sleep(remaining)

def fetch_games_thread():
    import fetch_games
    for _ in run_every("fetch_games", 10 * 60):
        try:
            fetch_games.fetch_games(False)
        except Exception as e:
            print(e)

def fetch_new_games_thread():
    import fetch_games
    for _ in run_every("fetch_new_games", 2 * 60):
        try:
            fetch_games.fetch_games(True)
        except Exception as e:
            print(e)

def fetch_league_thread():
    import fetch_league
    for _ in run_every("fetch_league", 5 * 60):
        try:
            fetch_league.fetch_league()
        except Exception as e:
            print(e)

def fetch_players_thread():
    import fetch_league
    for _ in run_every("fetch_players", 60 * 60):
        try:
            fetch_league.fetch_players()
        except Exception as e:
            print(e)

threads = [
    threading.Thread(target=fetch_games_thread),
    threading.Thread(target=fetch_league_thread),
    threading.Thread(target=fetch_players_thread),
    threading.Thread(target=fetch_new_games_thread),
]
for t in threads:
    t.start()
for t in threads:
    t.join()
