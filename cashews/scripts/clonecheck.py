import sys

from cashews import utils
all_players = utils.get_all_as_dict("player")

by_name = {}
for player_id, player in all_players.items():
    name = utils.player_name(player)
    if name not in by_name:
        by_name[name] = []
    by_name[name].append(player_id)

teams = utils.get_all_as_dict("team")
team = teams[sys.argv[2]]
if sys.argv[3] == "league":
    check_league = True
else:
    check_league = False

our_league = team["League"]
for player_id in utils.team_player_ids(team):
    player = all_players[player_id]
    name = utils.player_name(player)

    other_players = by_name[name]
    other_team_names = []
    for other_player_id in by_name[name]:
        if other_player_id == player_id:
            continue
        
        other_player = all_players[other_player_id]
        if other_player["Likes"] != player["Likes"]:
            continue
        if other_player["Dislikes"] != player["Dislikes"]:
            continue
        other_player_team_id = other_player["TeamID"]
        other_team = teams[other_player_team_id]
        if check_league and other_team["League"] != our_league:
            continue
        other_team_name = utils.team_name(other_team)
        other_team_names.append(other_team_name)
    if len(other_team_names) == 0:
        print(f"{name} (unique!)")
    else:
        print(f"{name} ({', '.join(other_team_names)})")