import utils

def fetch_league():
    utils.init_db()
    time = utils.fetch_time()

    utils.fetch_and_save("news", "news", utils.API + "/news")
    utils.fetch_and_save("spotlight", "spotlight", utils.API + "/spotlight")

    # will also fetch and cache: state, leagues, teams
    for _ in utils.fetch_all_teams():
        pass
        
def fetch_players():
    utils.init_db()
    all_players = []
    for team in utils.fetch_all_teams():
        player_ids = utils.team_player_ids(team)
        print(f"got {len(player_ids)} players from team {team["_id"]}", flush=True)
        all_players += player_ids

    total = len(all_players)
    for i, player_id in enumerate(all_players):
        player = utils.fetch_and_save("player", player_id, utils.API + "/player/" + player_id, cache_interval=utils.PLAYER_CACHE_INTERVAL)
        print("player:", utils.player_name(player), f"({i+1}/{total})", flush=True) 
