import sqlite3, hashlib, json, time, requests, os
import zstandard as zstd
DB_PATH = "data/db.db"

DB_INIT = """
create table if not exists meta(
    key int,
    version int,
    unique(key)
);
insert or ignore into meta(key, version) values (0, -1);
"""

MIGRATIONS = [
    """
create table objects(hash text primary key, data blob);
create table observations(type text, id text, timestamp int, hash text);

create table currents(type text, id text, hash text, last_update int, primary key (type, id));
    """,

    """
create table games(id text primary key, season int, day int, away_team_id text, home_team_id text, last_update int, state text, hash text);
    """,

    """
create table locations(loc text primary key, data text);
    """,
    """
create table batting_stats(
    player_id text primary key,
    team_id text,
    team_name text,
    player_name text,
    position text,
    pa int,
    ab int,
    r int,
    x1b int,
    x2b int,
    x3b int,
    hr int,
    rbi int,
    bb int,
    hbp int,
    so int,
    cdp int,
    gidp int,
    go int,
    fo int,
    po int,
    lo int,
    force_outs int,
    field_outs int,
    fc int,
    roe int,
    sf int,
    lob int,
    sb int,
    cs int,
    hits int,
    tb int,
    ba real,
    obp real,
    slg real,
    ops real
);
    """
]

SESSION = requests.Session()
API = "https://mmolb.com/api"


def db():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_PATH)
    con = sqlite3.connect(path)
    return con


def init_db():
    with db() as con:
        cur = con.cursor()
        cur.executescript(DB_INIT)
        con.commit()

        current_version = cur.execute("select version from meta").fetchone()[0]
        for i, migration in enumerate(MIGRATIONS):
            if current_version < i:
                print(f"running migration: {i}")
                cur.executescript(migration)
                cur.execute("update meta set version = ? where version = ?", (i, current_version))
                con.commit()
                current_version = i
    pass

    
def decode_json(data):
    if type(data) == bytes:
        data_str = zstd.decompress(data).decode()
    elif type(data) == str:
        data_str = data
    return json.loads(data_str)


def encode_json(data):
    data_str = json.dumps(data, sort_keys=True)
    data_blob = data_str.encode()
    return zstd.compress(data_blob, level=3)


def json_hash(data):
    data_str = json.dumps(data, sort_keys=True)
    return hashlib.sha256(data_str.encode()).hexdigest()


def now():
    return int(time.time() * 1000)


def save_new_object(type, id, data, timestamp):
    # data_str = json.dumps(data, sort_keys=True)
    hash = json_hash(data)
    data_blob = encode_json(data)

    with db() as con:
        cur = con.cursor()
        cur.execute("insert or ignore into objects(hash, data) values (?, ?)", (hash, data_blob))
        cur.execute("insert into observations(type, id, timestamp, hash) values (?, ?, ?, ?)", (type, id, timestamp, hash))
        con.commit()

        cur.execute("insert into currents(type, id, hash, last_update) values (?, ?, ?, ?) on conflict (type, id) do update set hash=excluded.hash, last_update=excluded.last_update where excluded.last_update > currents.last_update", (type, id, hash, timestamp))
        con.commit()


def get_object(type, id):
    with db() as con:
        cur = con.cursor()
        res = cur.execute("select objects.data from currents inner join objects on objects.hash = currents.hash where type = ? and id = ?", (type, id)).fetchone()
        if res:
            data_blob = res[0]
            return decode_json(data_blob)
        return None


def get_last_update(type, id):
    with db() as con:
        cur = con.cursor()
        res = cur.execute("select last_update from currents where type = ? and id = ?", (type, id)).fetchone()
        if res:
            return res[0]
        return None


def get_all(type):
    with db() as con:
        cur = con.cursor()
        res = cur.execute("select id, objects.data, currents.last_update from currents inner join objects on objects.hash = currents.hash where type = ?", (type,))
        all_results = res.fetchall()
    
    for id, data_blob, last_update in all_results:
        data = decode_json(data_blob)
        yield id, data, last_update


def get_all_as_dict(type, map_fn=None):
    out = {}
    for id, data, last_updated in get_all(type):
        if map_fn:
            data = map_fn(data)
        data["_last_updated"] = last_updated
        out[id] = data
    return out


def fetch_json(url, allow_not_found=False):
    for _ in range(5):
        try:
            res = SESSION.get(url, timeout=5)
            if allow_not_found and res.status_code == 404:
                return None
            res.raise_for_status()
            return res.json()
        except requests.exceptions.JSONDecodeError:
            # print(res.text)
            raise
        except requests.exceptions.ConnectionError as e:
            print(f"got error {e}, retrying...")
            time.sleep(5)
    raise Exception("failed after some retries :(")


def fetch_and_save(type, id, url, cache_interval=None, allow_not_found=False):
    timestamp = now()
    if cache_interval:
        last_timestamp = get_last_update(type, id)
        if last_timestamp:
            if last_timestamp + cache_interval > timestamp:
                # reuse
                return get_object(type, id)

    data = fetch_json(url, allow_not_found=allow_not_found)
    print(f"fetching: {url}")
    if allow_not_found and not data:
        return None
    save_new_object(type, id, data, timestamp)
    return data


def player_name(data):
    return data["FirstName"] + " " + data["LastName"]


def team_name(data):
    return data["Location"] + " " + data["Name"]


def team_player_ids(team):
    if type(team["Players"]) == dict:
        player_ids = [p["PlayerID"] for p in team["Players"].values()]
    else:
        player_ids = [p["PlayerID"] for p in team["Players"]]
    player_ids = [p for p in player_ids if "#" not in p]
    return player_ids


def id_timestamp(id):
    seconds = int(id[:8], 16)
    return seconds * 1000


PLAYER_CACHE_INTERVAL = 90 * 60 * 1000
TEAM_CACHE_INTERVAL = 5 * 60 * 1000
LEAGUE_CACHE_INTERVAL = 30 * 1000
STATE_CACHE_INTERVAL = 10 * 1000
TIME_CACHE_INTERVAL = 1000


def fetch_state():
    return fetch_and_save("state", "state", API + "/state", cache_interval=STATE_CACHE_INTERVAL)


def fetch_time():
    return fetch_and_save("time", "time", API + "/time", cache_interval=TIME_CACHE_INTERVAL)


def fetch_all_leagues():
    state = fetch_state()
    league_ids = state["GreaterLeagues"] + state["LesserLeagues"]
    for league_id in league_ids:
        league = fetch_and_save("league", league_id, API + "/league/" + league_id, cache_interval=LEAGUE_CACHE_INTERVAL)
        yield league


def get_league_team_ids(league_data):
    team_ids = league_data["Teams"]
    if league_data.get("SuperstarTeam"):
        team_ids.append(league_data["SuperstarTeam"])
    return team_ids


def fetch_all_teams():
    for league in fetch_all_leagues():        
        for team_id in get_league_team_ids(league):
            team = fetch_and_save("team", team_id, API + "/team/" + team_id, cache_interval=TEAM_CACHE_INTERVAL)
            yield team


def update_game_data(game_id):
    game_data = get_object("game", game_id)
    if not game_data:
        return
    last_update = get_last_update("game", game_id)
    with db() as con:
        cur = con.cursor()
        
        season = game_data["Season"]
        day = game_data["Day"]
        away_team_id = game_data["AwayTeamID"]
        home_team_id = game_data["HomeTeamID"]
        state = game_data["State"]
        data_hash = json_hash(game_data)

        cur.execute("""
                    insert into games(id, season, day, away_team_id, home_team_id, last_update, state, hash)
                        values (?, ?, ?, ?, ?, ?, ?, ?)
                        on conflict (id) do update set season=excluded.season, day=excluded.day, away_team_id=excluded.away_team_id, home_team_id=excluded.home_team_id, last_update=excluded.last_update, state=excluded.state, hash=excluded.hash""",
                        (game_id, season, day, away_team_id, home_team_id, last_update, state, data_hash))
        con.commit()


def get_game_for_team(team_id, season, day):
    with db() as con:
        cur = con.cursor()
        res = cur.execute("select id, objects.data from games inner join objects on objects.hash = games.hash where season = ? and day = ? and (home_team_id = ? or away_team_id = ?)", (season, day, team_id, team_id)).fetchone()
        if res:
            game_id, data_blob = res
            return game_id, decode_json(data_blob)
        

def get_all_games_for_team(team_id):
    with db() as con:
        cur = con.cursor()
        res = cur.execute("select id, objects.data from games inner join objects on objects.hash = games.hash where (home_team_id = ? or away_team_id = ?)", (team_id, team_id)).fetchall()
    for game_id, data_blob in res:
        yield game_id, decode_json(data_blob)


def get_all_game_data(season, day):
    with db() as con:
        cur = con.cursor()
        return cur.execute("select id, away_team_id, home_team_id, last_update, state from games where season = ? and day = ?", (season, day)).fetchall()
        