import utils

days = {}
for game_id, game, _ in utils.get_all("game"):
    day = game["Day"]
    if day not in days:
        days[day] = game_id
    else:
        existing_day = days[day]
        if game_id < existing_day:
            days[day] = game_id
            
teams = utils.get_all_as_dict("team")
def scan(lowest, delta):
    int_val = int(lowest, 16)

    nopes_in_a_row = 0
    while True:
        id_val = hex(int_val)[2:]
        res = utils.fetch_json(utils.API + "/game/" + id_val, allow_not_found=True)
        
        if res is None:
            int_val2 = int_val + delta
            id_val = hex(int_val2)[2:]
            res = utils.fetch_json(utils.API + "/game/" + id_val, allow_not_found=True) 
            if res is None:
                int_val += (1 << (4*16)) * delta
            else:
                int_val = int_val2
        else:
            int_val += delta
        if res:
            ht_name = teams[res['HomeTeamID']]["Name"]
            at_name = teams[res['AwayTeamID']]["Name"]
            print(id_val, f"day {res['Day']}", at_name, "@", ht_name, flush=True)
            nopes_in_a_row = 0
        else:
            nopes_in_a_row += 1
            if nopes_in_a_row > 5:
                break

for day, lowest in sorted(list(days.items())):
    scan(lowest, -1)
    scan(lowest, 1)
    print(f"{day}: {lowest}", flush=True)