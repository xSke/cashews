import sqlite3, hashlib, json, time, requests, os, threading, logging
import zstandard as zstd

from cashews import DATA_DIR

LOG_TLS = threading.local()

def LOG() -> logging.Logger:
    if "logger" in LOG_TLS.__dict__:
        return LOG_TLS.logger
    return logging.getLogger("UNKNOWN")

def set_log(logger: logging.Logger):
    LOG_TLS.logger = logger

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
create table player_stats(
    player_id text, team_id text, last_update int,
    allowed_stolen_bases int not null default 0, allowed_stolen_bases_risp int not null default 0, appearances int not null default 0, assists int not null default 0, assists_risp int not null default 0, at_bats int not null default 0, at_bats_risp int not null default 0, batters_faced int not null default 0, batters_faced_risp int not null default 0, blown_saves int not null default 0, caught_double_play int not null default 0, caught_double_play_risp int not null default 0, caught_stealing int not null default 0, caught_stealing_risp int not null default 0, complete_games int not null default 0, double_plays int not null default 0, double_plays_risp int not null default 0, doubles int not null default 0, doubles_risp int not null default 0, earned_runs int not null default 0, earned_runs_risp int not null default 0, errors int not null default 0, errors_risp int not null default 0, field_out int not null default 0, field_out_risp int not null default 0, fielders_choice int not null default 0, fielders_choice_risp int not null default 0, flyouts int not null default 0, flyouts_risp int not null default 0, force_outs int not null default 0, force_outs_risp int not null default 0, games_finished int not null default 0, grounded_into_double_play int not null default 0, grounded_into_double_play_risp int not null default 0, groundout int not null default 0, groundout_risp int not null default 0, hit_batters int not null default 0, hit_batters_risp int not null default 0, hit_by_pitch int not null default 0, hit_by_pitch_risp int not null default 0, hits_allowed int not null default 0, hits_allowed_risp int not null default 0, home_runs int not null default 0, home_runs_allowed int not null default 0, home_runs_allowed_risp int not null default 0, home_runs_risp int not null default 0, inherited_runners int not null default 0, inherited_runners_risp int not null default 0, inherited_runs_allowed int not null default 0, inherited_runs_allowed_risp int not null default 0, left_on_base int not null default 0, left_on_base_risp int not null default 0, lineouts int not null default 0, lineouts_risp int not null default 0, losses int not null default 0, mound_visits int not null default 0, no_hitters int not null default 0, outs int not null default 0, pitches_thrown int not null default 0, pitches_thrown_risp int not null default 0, plate_appearances int not null default 0, plate_appearances_risp int not null default 0, popouts int not null default 0, popouts_risp int not null default 0, putouts int not null default 0, putouts_risp int not null default 0, quality_starts int not null default 0, reached_on_error int not null default 0, reached_on_error_risp int not null default 0, runners_caught_stealing int not null default 0, runners_caught_stealing_risp int not null default 0, runs int not null default 0, runs_batted_in int not null default 0, runs_batted_in_risp int not null default 0, runs_risp int not null default 0, sac_flies int not null default 0, sac_flies_risp int not null default 0, sacrifice_double_plays int not null default 0, sacrifice_double_plays_risp int not null default 0, saves int not null default 0, shutouts int not null default 0, singles int not null default 0, singles_risp int not null default 0, starts int not null default 0, stolen_bases int not null default 0, stolen_bases_risp int not null default 0, strikeouts int not null default 0, strikeouts_risp int not null default 0, struck_out int not null default 0, struck_out_risp int not null default 0, triples int not null default 0, triples_risp int not null default 0, unearned_runs int not null default 0, unearned_runs_risp int not null default 0, walked int not null default 0, walked_risp int not null default 0, walks int not null default 0, walks_risp int not null default 0, wins int not null default 0,
    primary key (player_id, team_id)
);
    """
]

SESSION = requests.Session()
API = "https://mmolb.com/api"

def abbrev_hash(hash):
    return f"{hash[:8]}.."

def db():
    path = os.path.join(DATA_DIR, "db.db")

    import sys
    if sys.version_info >= (3, 12):
        con = sqlite3.connect(path, autocommit=False)
    else:
        con = sqlite3.connect(path)
    return con

def init_db():
    with db() as con:
        try:
            cur = con.cursor()
            cur.executescript(DB_INIT)
            con.commit()
        except Exception as e:
            LOG().error("failed to init db", exc_info=e)

        try:
            con.autocommit = True
            con.execute("PRAGMA journal_mode=WAL;")
        except Exception as e:
            LOG().error("failed to enable wal", exc_info=e)

        current_version = cur.execute("select version from meta").fetchone()[0]
        for i, migration in enumerate(MIGRATIONS):
            if current_version < i:
                LOG().info("running migration: %d", i)
                cur.executescript(migration)
                cur.execute("update meta set version = ? where version = ?", (i, current_version))
                con.commit()
                current_version = i

    
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

    has_hash_already = False
    with db() as con:
        cur = con.cursor()
        if cur.execute("select 1 from objects where hash = ?", (hash,)).fetchall():
            has_hash_already = True
    
    if not has_hash_already:
        data_blob = encode_json(data)
    else:
        data_blob = None

    with db() as con:
        cur = con.cursor()
        if data_blob:
            cur.execute("insert or ignore into objects(hash, data) values (?, ?)", (hash, data_blob))
        cur.execute("insert into observations(type, id, timestamp, hash) values (?, ?, ?, ?)", (type, id, timestamp, hash))
        con.commit()

        cur.execute("insert into currents(type, id, hash, last_update) values (?, ?, ?, ?) on conflict (type, id) do update set hash=excluded.hash, last_update=excluded.last_update where excluded.last_update > currents.last_update", (type, id, hash, timestamp))
        con.commit()
    return hash

def get_object_meta(type, id):
    with db() as con:
        cur = con.cursor()
        res = cur.execute("select hash, last_update from currents where type = ? and id = ?", (type, id)).fetchone()
        if res:
            hash, last_update = res
            return hash, last_update

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

def _fetch_json_with_resp(url, allow_not_found=False):
    for _ in range(5):
        try:
            res = SESSION.get(url, timeout=5)
            if allow_not_found and res.status_code == 404:
                return None, res
            res.raise_for_status()
            return res.json(), res
        except requests.exceptions.JSONDecodeError:
            raise
        except requests.exceptions.ConnectionError as e:
            LOG().error("got error, retrying...", exc_info=e)
            time.sleep(5)
    raise Exception(f"failed fetching '{url}' after some retries :(")

def fetch_json(url, allow_not_found=False):
    data, _ = _fetch_json_with_resp(url, allow_not_found=allow_not_found)
    return data

def fetch_and_save(type, id, url, cache_interval=None, allow_not_found=False):
    timestamp = now()
    if cache_interval:
        last_timestamp = get_last_update(type, id)
        if last_timestamp:
            if last_timestamp + cache_interval > timestamp:
                # reuse
                return get_object(type, id)

    import time
    time_before = time.time()
    data, resp = _fetch_json_with_resp(url, allow_not_found=allow_not_found)
    time_after = time.time()
    LOG().info("fetched: %s (%d, took %.03fs)", url, resp.status_code, time_after - time_before)
    if allow_not_found and not data:
        return None
    
    existing_meta = get_object_meta(type, id)
    new_hash = save_new_object(type, id, data, timestamp)
    if existing_meta:
        existing_hash, existing_last_update = existing_meta
        if existing_hash != new_hash:
            LOG().info("updated %s/%s: %s (%d) -> %s", type, id, abbrev_hash(existing_hash), existing_last_update, abbrev_hash(new_hash))

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

def fetch_player_and_update(player_id, cache_interval=None):
    if cache_interval is None:
        cache_interval = PLAYER_CACHE_INTERVAL
    player = fetch_and_save("player", player_id, API + "/player/" + player_id, cache_interval=cache_interval)
    try:
        update_player_data(player_id)
    except Exception as e:
        LOG().error("failed to update player data for %s", player_id, exc_info=e)

    return player

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

def update_player_data(player_id):
    player_data = get_object("player", player_id)
    if not player_data:
        return
    last_update = get_last_update("player", player_id)

    with db() as con:
        cur = con.cursor()
        for team_id, team_stats in player_data["Stats"].items():
            for k, v in team_stats.items():
                if type(v) != int:
                    LOG().error("player stat key %s has type %s (%s)", k, type(v), v)
                    continue
                # pls no sql inject
                sql = f"insert into player_stats(player_id, team_id, last_update, {k}) values (?, ?, ?, ?) on conflict (player_id, team_id) do update set last_update=excluded.last_update, {k}=excluded.{k}"
                try:
                    cur.execute(sql, (player_id, team_id, last_update, v))
                except sqlite3.OperationalError as e:
                    if "has no column named" in str(e):
                        LOG().error("player stat had unknown key %s (value: %s)", k, v)
                        continue
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
        