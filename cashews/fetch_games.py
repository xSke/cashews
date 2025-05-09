from cashews import utils
from cashews.utils import LOG

def fetch_games(new_only=False):
    utils.init_db()

    GAME_BY_TEAM_CACHE_INTERVAL = 10 * 60 * 1000

    time = utils.fetch_time()
    season = time["season_number"]
    day = time["season_day"]


    all_teams = utils.get_all_as_dict("team")
    for team_id, team in all_teams.items():
        existing_game = utils.get_game_for_team(team_id, season, day)
        if existing_game:
            game_id, existing_data = existing_game

            if existing_data["State"] == "Complete":
                LOG().info("already have complete game, skipping: %s", game_id)
                continue
            if new_only:
                # LOG().info("already have game, skipping: %s", game_id)
                continue
        else:
            game_by_team = utils.fetch_and_save("game-by-team", team_id, utils.API + "/game-by-team/" + team_id, cache_interval=GAME_BY_TEAM_CACHE_INTERVAL, allow_not_found=True)
            if not game_by_team or ("game_id" not in game_by_team):
                LOG().info("team did not have game today: %s (%s)", team_id, utils.team_name(team))
                continue
            game_id = game_by_team["game_id"]

        _do_refetch_game(game_id)
        # game = utils.fetch_and_save("game", game_id, utils.API + "/game/" + game_id, cache_interval=GAME_CACHE_INTERVAL)
        # LOG().info("got %s @ %s", game['AwayTeamName'], game['HomeTeamName'])

        # utils.update_game_data(game_id)

def refetch_unfinished_known_games():
    for game_id, data, _ in utils.get_all("game"):
        if data["State"] != "Complete":
            print(game_id)
            _do_refetch_game(game_id)
            # new_game = utils.fetch_and_save("game", game_id, utils.API + "/game/" + game_id, cache_interval=GAME_CACHE_INTERVAL)
            # utils.update_game_data(game_id)
            # LOG().info("game %s got new state: %s", game_id, new_game["State"])

def _do_refetch_game(game_id):
    GAME_CACHE_INTERVAL = 5 * 60 * 1000

    last_data = utils.get_object("game", game_id)

    new_game = utils.fetch_and_save("game", game_id, utils.API + "/game/" + game_id, cache_interval=GAME_CACHE_INTERVAL)
    utils.update_game_data(game_id)

    # if we just completed...
    try:
        if last_data and last_data["State"] != "Complete" and new_game["State"] == "Complete":
            LOG().info("game %s just completed, refetching team players", game_id)

            # *don't* cache this time, we suspect the game just ended and thus stats would've been updated
            # fetch those immediately if we can
            _refetch_team_players(new_game, player_cache_interval=0)
    except Exception as e:
        LOG().error("error refetching team players", exc_info=e)         

def _refetch_team_players(game_data, player_cache_interval=None):
    team_ids = [game_data["HomeTeamID"], game_data["AwayTeamID"]]
    for team_id in team_ids:
        team = utils.get_object("team", team_id)

        for player_id in utils.team_player_ids(team):
            utils.fetch_player_and_update(player_id, cache_interval=player_cache_interval)

def backfill_game_ids():
    import re
    with open("game_ids.txt") as f:
        for m in re.finditer("\\b([0-9a-f]{24})\\b", f.read()):
            game_id = m.group(1)

            # if utils.get_last_update("game", game_id):
            #     game = utils.get_object("game", game_id)
            #     print("already have game", game_id, f"day {game['Day']}")
            #     continue

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