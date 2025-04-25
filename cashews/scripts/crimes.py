from cashews import utils

days = {}
highest  = {}
for game_id, game, _ in utils.get_all("game"):
    day = game["Day"]
    if day not in days:
        days[day] = game_id
        highest[day] = game_id
    else:
        existing_day = days[day]
        if game_id < existing_day:
            days[day] = game_id

        existing_day = highest[day]
        if game_id > existing_day:
            highest[day] = game_id

def check_game_exists(game_id):
    obj = utils.get_object("game", game_id)
    if obj:
        return True
    res = utils.fetch_and_save("game", game_id, utils.API + "/game/" + game_id, allow_not_found=True)
    if res:
        print(f"found new game:", game_id, flush=True)
        utils.update_game_data(game_id)
        return True

teams = utils.get_all_as_dict("team")
def scan(lowest, delta):
    int_val = int(lowest, 16)

    nopes_in_a_row = 0
    last_good = lowest
    while True:
        id_val = hex(int_val)[2:]
        # res = utils.fetch_json(utils.API + "/game/" + id_val, allow_not_found=True)
        
        exists = check_game_exists(id_val)
        if not exists:
            print("nope:", id_val)
            int_val2 = int_val + delta
            id_val = hex(int_val2)[2:]
            exists = check_game_exists(id_val)
            if not exists:
                print("nope:", id_val)
                int_val += (1 << (4*16)) * delta
            else:
                print("yep:", id_val)
                int_val = int_val2
                last_good = id_val
        else:
            int_val += delta
            print("yep:", id_val)
            last_good = id_val
        if exists:
            # ht_name = teams[res['HomeTeamID']]["Name"]
            # at_name = teams[res['AwayTeamID']]["Name"]
            # print(id_val, f"day {res['Day']}", at_name, "@", ht_name, flush=True)
            nopes_in_a_row = 0
        else:
            print("nope:", id_val)
            nopes_in_a_row += 1
            if nopes_in_a_row > 5:
                break
    return last_good

print("starting")
for day, lowest in sorted(list(days.items())):
    if day < 50:
        continue
    print("scanning day", day, flush=True)
    scan(lowest, -1)
    print("highest was", highest[day], flush=True)
    lg = scan(lowest, 1)
    print("highest was", highest[day], flush=True)
    if lg < highest[day]:
        raise Exception("nooo")
    print(f"{day}: {lowest}", flush=True)