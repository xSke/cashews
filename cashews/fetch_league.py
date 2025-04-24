import requests
from cashews import utils
from cashews.utils import LOG

def fetch_league():
    utils.init_db()
    utils.fetch_time()

    timestamp = utils.now()
    nouns = requests.get("https://mmolb.com/data/nouns.txt").text.split("\n")
    adjectives = requests.get("https://mmolb.com/data/adjectives.txt").text.split("\n")
    utils.save_new_object("nouns", "nouns", nouns, timestamp)
    utils.save_new_object("adjectives", "adjectives", adjectives, timestamp)

    utils.fetch_and_save("news", "news", utils.API + "/news")
    utils.fetch_and_save("spotlight", "spotlight", utils.API + "/spotlight")

    # will also fetch and cache: state, leagues, teams
    for _ in utils.fetch_all_teams():
        pass

    # also do this now
    fetch_new_players()

def fetch_new_players():
    all_teams = utils.get_all_as_dict("team")
    all_players = utils.get_all_as_dict("player")
    
    for team_id, team in all_teams.items():
        for player_id in utils.team_player_ids(team):
            if player_id not in all_players:
                utils.LOG().info("found *new* player %s (on %s)", player_id, team_id)
                utils.fetch_player_and_update(player_id)

def fetch_players():
    utils.init_db()
    all_teams = utils.get_all_as_dict("team")

    all_players = []
    for team_id, team in all_teams.items():
        player_ids = utils.team_player_ids(team)
        LOG().info("got %d players from team %s", len(player_ids), team_id)
        all_players += player_ids

    total = len(all_players)
    for i, player_id in enumerate(all_players):
        player = utils.fetch_player_and_update(player_id)
        LOG().info("fetched player: %s (%d/%d)", utils.player_name(player), i+1, total) 

def refresh_known_players():
    # only fetch them if they're *really* old, like, gone
    interval = utils.PLAYER_CACHE_INTERVAL * 4

    players = utils.get_all_as_dict("player")
    for player_id in players.keys():
        utils.fetch_player_and_update(player_id, cache_interval=interval)

def backfill_player_data():
    utils.init_db()
    players = utils.get_all_as_dict("player")
    total = len(players)
    for i, p in enumerate(list(players.keys())):
        utils.update_player_data(p)
        LOG().info("%s (%d/%d)", p, i+1, total)
