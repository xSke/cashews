import utils, datetime

def fetch_games(new_only=False):
    utils.init_db()

    GAME_BY_TEAM_CACHE_INTERVAL = 10 * 60 * 1000
    GAME_CACHE_INTERVAL = 10 * 60 * 1000

    time = utils.fetch_time()
    season = time["season_number"]
    day = time["season_day"]


    for team in utils.fetch_all_teams():
        team_id = team["_id"]

        existing_game = utils.get_game_for_team(team_id, season, day)
        if existing_game:
            game_id, existing_data = existing_game
            # print(existing_data.keys())
            if existing_data["State"] == "Complete":
                print("already have complete game, skipping", game_id)
                continue
            if new_only:
                print("already have game, skipping")
                continue
        else:
            game_by_team = utils.fetch_and_save("game-by-team", team_id, utils.API + "/game-by-team/" + team_id, cache_interval=GAME_BY_TEAM_CACHE_INTERVAL, allow_not_found=True)
            if not game_by_team or ("game_id" not in game_by_team):
                print("team did not have game today:", team_id, utils.team_name(team))
                continue
            game_id = game_by_team["game_id"]

        game = utils.fetch_and_save("game", game_id, utils.API + "/game/" + game_id, cache_interval=GAME_CACHE_INTERVAL)
        print(f"got {game['AwayTeamName']} @ {game['HomeTeamName']}")

        utils.update_game_data(game_id)

def refetch_unfinished_known_games():
    GAME_CACHE_INTERVAL = 10 * 60 * 1000
    for game_id, data, _ in utils.get_all("game"):
        if data["State"] != "Complete":
            new_game = utils.fetch_and_save("game", game_id, utils.API + "/game/" + game_id, cache_interval=GAME_CACHE_INTERVAL)
            utils.update_game_data(game_id)
            print("game", game_id, "new state", new_game["State"])

def backfill_game_ids():
    import re
    with open("game_ids.txt") as f:
        for m in re.finditer("\\b([0-9a-f]{24})\\b", f.read()):
            game_id = m.group(1)

            if utils.get_last_update("game", game_id):
                game = utils.get_object("game", game_id)
                print("already have game", game_id, f"day {game['Day']}")
                continue

            new_game = utils.fetch_and_save("game", game_id, utils.API + "/game/" + game_id, allow_not_found=True)
            if not new_game:
                print("couldn't find game", game_id)
            else:
                print("backfilled game", game_id, "state is", new_game["State"], f"day {new_game['Day']}")
                utils.update_game_data(game_id)

def backfill_from_beiju():
    GAME_CACHE_INTERVAL = 10 * 60 * 1000

    import requests, re
    s = requests.Session()
    for team_id, team, _ in utils.get_all("team"):
        resp = s.get("https://mmolb-game-directory.beiju.me/team/" + team_id)
        print("searching team", team_id, utils.team_name(team))

        for m in re.finditer("https://mmolb.com/watch/([0-9a-f]{24})", resp.text):
            game_id = m.group(1)

            if utils.get_last_update("game", game_id):
                print("already have game", game_id)
                continue

            new_game = utils.fetch_and_save("game", game_id, utils.API + "/game/" + game_id, cache_interval=GAME_CACHE_INTERVAL, allow_not_found=True)
            if not new_game:
                print("couldn't find game", game_id)
            else:
                print("backfilled game", game_id, "state is", new_game["State"])
                utils.update_game_data(game_id)