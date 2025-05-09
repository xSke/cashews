from typing import Annotated
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from cashews import utils
import datetime
import json
import pandas as pd
from pandas.io.formats.style import Styler
import numpy as np

app = FastAPI(docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(autoescape=False, directory="templates")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def filter_props(props):
    def inner(obj):
        return {k: v for k, v in obj.items() if k in props}
    return inner

def to_delta(timestamp):
    dt = datetime.datetime.fromtimestamp(timestamp / 1000, tz=datetime.UTC)
    delta = datetime.datetime.now(datetime.UTC) - dt
    tsecs = delta.total_seconds()

    if tsecs <= 60:
        return "now"

    hours = int(tsecs // (60*60))
    mins = int((tsecs % (60*60)) // (60))
    # secs = int((tsecs % (60)))
    out_str = ""
    if hours > 0:
        out_str += f"{hours}h "
    if mins > 0:
        out_str += f"{mins}m "
    
    out_str += "ago"
    return out_str

# @app.get("/api/leagues")
# async def api_leagues():
#     keep_properties = set(["Color", "Emoji", "LeagueType", "Name"])
#     leagues = utils.get_all_as_dict("league", filter_props(keep_properties))
#     return leagues

@app.get("/api/leagues")
async def api_leagues():
    leagues = utils.get_all_as_dict("league")
    return leagues

@app.get("/api/aggstats")
async def api_aggstats():
    from cashews import stats
    import time
    ttl = int(time.time() // 60)
    df = stats.league_agg_stats_2(ttl)

    percentiles = [.1, .2, .25, .3, .4, .5, .6, .7, .75, .8, .9]

    def flip_turnways(df):
        idx = ['count', 'mean', 'std',
               'min', '10%', '20%', '25%', '30%', '40%', '50%', '60%', '70%', '75%', '80%', '90%', 'max']
        idx_flip = ['count', 'mean', 'std', *np.flip(idx[3:])]
        pos_list = ["ba", "obp", "slg", "ops", "k9", "sb_success"]
        neg_list = ["era", "whip", "hr9", "bb9", "h9"]
        for col in df.columns:
            col_contents = df.loc[:, col]
            if ((col in pos_list) and (col_contents['min'] > col_contents['max']) or
                    (col in neg_list) and (col_contents['min'] < col_contents['max'])):
                df.loc[idx, col] = np.asarray(col_contents[idx_flip])
        return df

    agg_all = flip_turnways(df.describe(percentiles))
    agg_grouped = df.groupby(df["league_id"])
    agg_league = {}
    for name, group in agg_grouped:
        agg_league[name] = flip_turnways(group.describe(percentiles))

    # print(agg_league.loc["6805db0cac48194de3cd3fe4", ["ba", "era"]])

    out_dict = {
        "leagues": {}
    }
    for league_id in agg_league:
        # i hate this, do we have to do this? surely there's a better way... ---glum
        out_dict["leagues"][league_id] = agg_league[league_id].unstack().unstack().T.to_dict()
    out_dict["total"] = agg_all.to_dict()
    return out_dict

@app.get("/api/allteams")
async def api_teams(league: str | None = None):
    keep_properties = set(["Color", "Emoji", "FullLocation", "Location", "League", "Name", "Record", "_id"])
    teams = utils.get_all_as_dict("team", filter_props(keep_properties))
    if league:
        teams = {k: v for k, v in teams.items() if v["League"] == league}
    return teams


import functools
@functools.lru_cache(maxsize=3)
def _all_players_cached(_lru_key):
    return utils.get_all_as_dict("player")

def lru_key(interval_secs):
    import time
    return int(time.time() // interval_secs)

# @app.get("/api/allplayers")
# async def api_teams(fields: str = "FirstName,LastName"):
#     keep_properties = tuple(set(fields.split(",")))
#     players = _all_players_cached(keep_properties, lru_key(60))
#     return players

@app.get("/api/allplayers/csv")
async def api_teams(fields: str = "FirstName,LastName"):
    fields = fields.split(",")

    players = _all_players_cached(lru_key(60))

    import io, csv
    out = io.StringIO()
    cw = csv.DictWriter(out, list(fields), extrasaction="ignore", restval="")
    cw.writeheader()
    for player_id, player in players.items():
        player["_id"] = player_id
        cw.writerow(player)
    
    return Response(content=out.getvalue(), media_type="application/csv")

@app.get("/api/games")
async def api_games(team: str | None = None, season: int | None = None, day: int | None = None, state: str | None = None):
    all_teams = utils.get_all_as_dict("team")
    with utils.db() as con:
        cur = con.cursor()

        filters = []
        params = []

        if team:
            filters.append("(home_team_id = ? or away_team_id = ?)")
            params.append(team)
            params.append(team)
        if season is not None:
            filters.append("season = ?")
            params.append(season)
        if day is not None:
            filters.append("day = ?")
            params.append(day)
        if state is not None:
            filters.append("state = ?")
            params.append(state)
        
        where_clause = f"where {' and '.join(filters)}" if filters else ''
        res = cur.execute(f"select id, season, day, home_team_id, away_team_id, home_score, away_score, state, last_update from games {where_clause}", params).fetchall()
    out = []
    for game_id, season, day, home_team_id, away_team_id, home_score, away_score, state, last_update in res:
        # data = utils.decode_json(data_blob)

        # team = utils.get_object()
        league = all_teams[home_team_id]["League"]

        out.append({
            "game_id": game_id,
            "season": season,
            "day": day,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_score": home_score,
            "away_score": away_score,
            "last_update": last_update,
            "state": state,
            # "league_id": league
        })
    return out
        
def formatted_and_iso(timestamp_secs):
    dt = datetime.datetime.fromtimestamp(timestamp_secs, tz=datetime.UTC)
    formatted = dt.strftime("%Y-%m-%d %H:%M") + " UTC"
    iso = dt.isoformat()
    return formatted, iso

@app.get("/games/{team_id}", response_class=HTMLResponse)
async def games_by_team(request: Request, team_id: str):
    team = utils.get_object("team", team_id)
    if not team:
        raise HTTPException(status_code=404, detail="team not found")
    
    games = list(utils.get_all_games_for_team(team_id))

    def sort_key(row):
        _, game = row
        return game["Season"], game["Day"]
    games.sort(key=sort_key, reverse=True)

    games_list = []
    for game_id, game in games:
        game["_id"] = game_id
        game["last"] = game["EventLog"][-1]

        game_timestamp = utils.id_timestamp(game_id)
        game["time_formatted"], game["time_iso"] = formatted_and_iso(game_timestamp / 1000)

        home_pitchers = []
        away_pitchers = []
        for evt in game["EventLog"]:
            if evt["inning_side"] == 0:
                if evt["pitcher"] and evt["pitcher"] not in home_pitchers:
                    home_pitchers.append(evt["pitcher"])
            if evt["inning_side"] == 1:
                if evt["pitcher"] and evt["pitcher"] not in away_pitchers:
                    away_pitchers.append(evt["pitcher"])

        # game["timestamp"]
        game["away_pitchers"] = ", ".join(away_pitchers)
        game["home_pitchers"] = ", ".join(home_pitchers)
        games_list.append(game)

    return templates.TemplateResponse(
        request=request, name="games.html", context={"team": team, "games": games_list}
    )

def get_all_locations():
    with utils.db() as con:
        cur = con.cursor()
        res = cur.execute("select loc, data from locations").fetchall()
        return {loc: json.loads(data) for loc, data in res}

@app.get("/api/locations")
async def locations():
    return get_all_locations()

@app.get("/api/playersbyteam/{team_id}")
async def players_by_team(team_id: str):
    team = utils.get_object("team", team_id)
    if not team:
        raise HTTPException(status_code=404, detail="team not found")

    player_ids = utils.team_player_ids(team)

    players = {}
    for player_id in player_ids:
        player = utils.get_object("player", player_id)
        if not player:
            continue
        player["_last_update"] = utils.get_last_update("player", player_id)
        players[player_id] = player
    return {
        "team": team,
        "players": players
    }

@app.get("/api/teamlocations")
async def teamlocations():
    locations = get_all_locations()

    teams = []
    for team_id, team, _ in utils.get_all("team"):
        teams.append({
            "id": team_id,
            "location": locations.get(team["FullLocation"]),
            "name": utils.team_name(team),
            "emoji": team["Emoji"],
            "color": team["Color"]
        })
    return teams

@app.get("/map", response_class=HTMLResponse)
async def map(request: Request):
    return templates.TemplateResponse(
        request=request, name="map.html"
    )

@app.get("/teams", response_class=HTMLResponse)
async def teams(request: Request):
    state = utils.get_object("state", "state")
    leagues_dict = utils.get_all_as_dict("league")
    teams_dict = utils.get_all_as_dict("team")

    def team_sort(team):
        return utils.team_name(team)
    
    time = utils.fetch_time()
    season = time["season_number"]
    day = time["season_day"]

    game_datas = utils.get_all_game_data(season, day)
    game_ids_by_team = {}
    for id, away_team_id, home_team_id, _, _ in game_datas:
        game_ids_by_team[away_team_id] = id
        game_ids_by_team[home_team_id] = id

    leagues = []
    all_teams = []
    for league_id in state["GreaterLeagues"] + state["LesserLeagues"]:
        league = leagues_dict[league_id]

        league_teams = []
        for team_id in league["Teams"]:
            if team_id in ["68070065ce5a952465d7c177", "6807014aee9f269dec724ea2"]:
                # destroy my mistakes
                continue
            if team_id not in teams_dict:
                print("!!! couldn't find team:", team_id)
                continue
            team = teams_dict[team_id]

            record = team["Record"]["Regular Season"]
            team["wins"] = record["Wins"]
            team["losses"] = record["Losses"]
            team["rd"] = record["RunDifferential"]
            team["league"] = league["Name"]
            team["league_emoji"] = league["Emoji"]
            team["player_count"] = len([1 for p in team["Players"] if p["PlayerID"] != "#"])

            team_timestamp = utils.id_timestamp(team_id)
            team["created_formatted"], team["created_iso"] = formatted_and_iso(team_timestamp / 1000)

            team["last_updated"] = to_delta(team["_last_updated"])

            team["game_id"] = game_ids_by_team.get(team_id)
            
            league_teams.append(team)
            all_teams.append(team)

        
        league_teams.sort(key=team_sort)
        leagues.append({
            "name": league["Name"],
            "emoji": league["Emoji"],
            "teams": league_teams
        })
    
    def league_sort(league):
        return league["name"]
    
    all_teams.sort(key=team_sort)
    leagues.sort(key=league_sort)
    
    return templates.TemplateResponse(
        request=request, name="teams.html", context={"leagues": leagues, "teams": all_teams}
    )

@functools.lru_cache(maxsize=3)
def _player_stats(_ttl):
    with utils.db() as con:
        from cashews import stats
        return pd.read_sql_query(stats.STATS_Q, con).set_index("player_id")

@app.get("/team/{team_id}/stats", response_class=HTMLResponse)
async def stats(request: Request, team_id: str):
    team = utils.get_object("team", team_id)
    if not team:
        raise HTTPException(status_code=404, detail="team not found")
    league = utils.get_object("league", team["League"])

    # slots stored on the *team player* object, not the player itself
    player_slots = {p["PlayerID"]: p["Slot"] for p in team["Players"]}

    # player_ids = utils.team_player_ids(team)

    batting_order = utils.get_team_batting_order(team_id)
    # all_players = utils.get_all_as_dict("player")
    # all_teams = utils.get_all_as_dict("team")


    import time
    from cashews import stats
    ttl = int(time.time() // 60)

    agg_stats = stats.league_agg_stats_2(ttl)
    league_agg = agg_stats.describe(percentiles=[.25, .50, .75]).to_dict()
    subleague_agg = agg_stats[agg_stats["league_id"] == team["League"]].describe(percentiles=[.25, .50, .75]).to_dict()
 
    stats_by_player = _player_stats(ttl)
    player_ids = stats_by_player[stats_by_player["team_id"] == team_id].index

    slot_order_pitchers = ["SP1", "SP2", "SP3", "SP4", "SP5", "RP1", "RP2", "RP3", "CL"]
    players_list = []
    for i, player_id in enumerate(player_ids):
        player = utils.get_object("player", player_id)
        if not player:
            continue
        try:
            batting_position = (batting_order.index(player_id) + 1) if player_id in batting_order else 0
            is_current_pitcher = player_slots.get(player_id, player["Position"]) in slot_order_pitchers
            if is_current_pitcher:
                pitching_order = slot_order_pitchers.index(player_slots.get(player_id, player["Position"]))
            else:
                pitching_order = 99
            player_ser = pd.concat([
                pd.Series({
                    "player_id": player_id,
                    "position": player["Position"], 
                    "slot": player_slots.get(player_id, player["Position"]),  # slot field added later
                    "position_type": player["PositionType"],
                    "batting_order": batting_position,
                    "idx": pitching_order if is_current_pitcher else i,
                    "name": utils.player_name(player),
                    "team_name": utils.team_name(team) if team else "Null Team",
                    "team_emoji": team["Emoji"] if team else "❓",
                }),
                stats_by_player.loc[player_id]])
            players_list.append(player_ser)
        except KeyError as e:
            pass
            # utils.LOG().error(f"{utils.player_name(player)} has no stats")

    players = pd.DataFrame(players_list)
    if not len(players):
        raise HTTPException(status_code=404, detail="no players")

    ttl = int(time.time() // 60)
    agg_stats_3 = stats.league_agg_stats_3(ttl)
    fip_const = float(agg_stats_3["fip_const"].iloc[0])
    era_avg = float(agg_stats_3["era_avg"].iloc[0])
    obp_avg = float(agg_stats_3["obp_avg"].iloc[0])
    slg_avg = float(agg_stats_3["slg_avg"].iloc[0])

    batters = players.query("plate_appearances > 0").copy().assign(
        ops_plus=lambda x: 100 * (x.obp/obp_avg + x.slg/slg_avg - 1)
    )
    batters["hits"] = batters["singles"] + batters["doubles"] + batters["triples"] + batters["home_runs"]
    pitchers = players.query("batters_faced > 0").copy().assign(
        fip=lambda x: (13*x.home_runs_allowed + 3*(x.walks + x.hit_batters) - 2*x.strikeouts)/x.ip + fip_const,
        era_minus=lambda x: 100 * x.era/era_avg
    )
    pitchers["total_runs"] = pitchers["earned_runs"] + pitchers["unearned_runs"]

    team_batting = batters.sum(numeric_only=True)
    team_batting["ba"] = (team_batting["singles"] + team_batting["doubles"] + team_batting["triples"]
                          + team_batting["home_runs"]) / team_batting["at_bats"]
    team_batting["obp"] = (team_batting["singles"] + team_batting["doubles"] + team_batting["triples"]
                           + team_batting["home_runs"] + team_batting["walked"] + team_batting["hit_by_pitch"]
                           ) / team_batting["plate_appearances"]
    team_batting["slg"] = (team_batting["singles"] + 2 * team_batting["doubles"] + 3 * team_batting["triples"]
                           + 4 * team_batting["home_runs"]) / team_batting["at_bats"]
    team_batting["ops"] = team_batting["obp"] + team_batting["slg"]
    team_batting["sb_success"] = team_batting["stolen_bases"] / (team_batting["stolen_bases"]
                                                                 + team_batting["caught_stealing"])
    team_batting["ops_plus"] = 100 * (team_batting.obp/obp_avg + team_batting.slg/slg_avg - 1)

    team_pitching = pitchers.sum(numeric_only=True)
    team_pitching["appearances"] = team_pitching["starts"]  # total games doesn't make sense to simply add
    team_pitching["era"] = 9 * team_pitching["earned_runs"] / team_pitching["ip"]
    team_pitching["whip"] = (team_pitching["walks"] + team_pitching["hits_allowed"]) / team_pitching["ip"]
    team_pitching["hr9"] = 9 * team_pitching["home_runs_allowed"] / team_pitching["ip"]
    team_pitching["bb9"] = 9 * team_pitching["walks"] / team_pitching["ip"]
    team_pitching["k9"] = 9 * team_pitching["strikeouts"] / team_pitching["ip"]
    team_pitching["h9"] = 9 * team_pitching["hits_allowed"] / team_pitching["ip"]
    team_pitching["fip"] = (13*team_pitching.home_runs_allowed + 3*(team_pitching.walks + team_pitching.hit_batters) - 2*team_pitching.strikeouts)/team_pitching.ip + fip_const
    team_pitching["era_minus"] = 100 * team_pitching.era/era_avg

    def format_int(number):
        return str(int(number))
    
    def format_float3(number):
        if type(number) == int or type(number) == float or type(number) == np.float64:
            return f"{number:.03f}"
        return ""
    
    def format_float2(number):
        if type(number) == int or type(number) == float or type(number) == np.float64:
            return f"{number:.02f}"
        return ""
    
    def format_ip(number):
        ip_whole = int(number)
        ip_remainder = int((number - ip_whole) / 3 * 10)
        if ip_remainder == 0:
            ip_str = f"{ip_whole}"
        else:
            ip_str = f"{ip_whole}.{ip_remainder}"
        return ip_str

    defs_b = [
        ("PA", "plate_appearances", None, format_int, "Plate Appearances"),
        ("AB", "at_bats", None, format_int, "At Bats"),
        ("R", "runs", None, format_int, "Runs Scored"),
        ("1B", "singles", None, format_int, "Singles"),
        ("2B", "doubles", None, format_int, "Doubles"),
        ("3B", "triples", None, format_int, "Triples"),
        ("HR", "home_runs", None, format_int, "Home Runs"),
        ("RBI", "runs_batted_in", None, format_int, "Runs Batted In"),
        ("BB", "walked", None, format_int, "Bases on Balls (Walks)"),
        ("SO", "struck_out", None, format_int, "Strikeouts"),
        ("HBP", "hit_by_pitch", None, format_int, "Hit By Pitch"),
        ("BA", "ba", True, format_float3, "Batting Average"),
        ("OBP", "obp", True, format_float3, "On-base Percentage"),
        ("SLG", "slg", True, format_float3, "Slugging Percentage"),
        ("OPS", "ops", True, format_float3, "On Base Plus Slugging"),
        ("OPS+", "ops_plus", None, format_int, "OPS+"),
        ("SB", "stolen_bases", None, format_int, "Stolen Bases (Attempts)"),
        ("CS", "caught_stealing", None, format_int, "Caught Stealing"),
        ("SB%", "sb_success", True, format_float3, "Stolen Bases (Successes)"),
    ]
    defs_p = [  # I now have put too much in this table for our current format, whoops
        ("IP", "ip", None, format_ip, "Innings pitched"),
        ("G", "appearances", None, format_int, "Games"),
        ("GS", "starts", None, format_int, "Games started"),
        ("W", "wins", None, format_int, "Wins"),
        ("L", "losses", None, format_int, "Losses"),
        ("CG", "complete_games", None, format_int, "Complete Games"),
        ("SHO", "shutouts", None, format_int, "Shutouts"),
        ("NH", "no_hitters", None, format_int, "No-hitters"),
        ("QS", "quality_starts", None, format_int, "Quality Starts"),
        ("SV", "saves", None, format_int, "Saves"),
        ("BS", "blown_saves", None, format_int, "Blown Saves"),
        ("R", "total_runs", None, format_int, "Runs"),
        ("ER", "earned_runs", None, format_int, "Earned Runs"),
        ("IR", "inherited_runners", None, format_int, "Inherited Runners"),
        ("IRA", "inherited_runs_allowed", None, format_int, "Inherited Runs Allowed"),
        ("HR", "home_runs_allowed", None, format_int, "Home Runs Allowed"),
        ("BB", "walks", None, format_int, "Walks"),
        ("SO", "strikeouts", None, format_int, "Strikeouts"),
        ("HB", "hit_batters", None, format_int, "Hit Batters"),
        ("ERA", "era", False, format_float2, "Earned Run Average"),
        ("FIP", "fip", None, format_float2, "Fielding-Independent Pitching"),
        ("ERA-", "era_minus", None, format_int, "ERA-"),
        ("WHIP", "whip", False, format_float2, "Walks + Hits per Inning Pitched"),
        ("HR/9", "hr9", False, format_float2, "Home Runs per 9 Innings"),
        ("BB/9", "bb9", False, format_float2, "Walks per 9 Innings"),
        ("K/9", "k9", True, format_float2, "Strikeouts per 9 Innings"),
        ("H/9", "h9", False, format_float2, "Hits per 9 Innings"),
    ]

    defs_f = [
        ("Putouts", "putouts", None, format_int, "Putouts"),
        ("Assists", "assists", None, format_int, "Assists"),
        ("Errors", "errors", None, format_int, "Errors"),
        ("DP", "double_plays", None, format_int, "Double Plays"),
        ("FPCT", "fpct", None, format_float3, "Fielding Percentage"),
        ("SB", "allowed_stolen_bases", None, format_int, "Allowed stolen bases"),
        ("CS", "runners_caught_stealing", None, format_int, "Runners caught stealing"),
    ]

    defs_b2 = []
    for name, key, up_good, formatter, hover in defs_b:
        defs_b2.append({
            "name": name,
            "key": key,
            "up_good": up_good,
            "league_avg": league_agg[key]["mean"] if key in league_agg else None,
            "league_stddev": league_agg[key]["std"] if key in league_agg else None,
            "format": formatter,
            "hover": hover,
        })
    defs_p2 = []
    for name, key, up_good, formatter, hover in defs_p:
        defs_p2.append({
            "name": name,
            "key": key,
            "up_good": up_good,
            "league_avg": league_agg[key]["mean"] if key in league_agg else None,
            "league_stddev": league_agg[key]["std"] if key in league_agg else None,
            "format": formatter,
            "hover": hover,
        })
    defs_f2 = []
    for name, key, up_good, formatter, hover in defs_f:
        defs_f2.append({
            "name": name,
            "key": key,
            "up_good": up_good,
            "league_avg": league_agg[key]["mean"] if key in league_agg else None,
            "league_stddev": league_agg[key]["std"] if key in league_agg else None,
            "format": formatter,
            "hover": hover,
        })

    def color_stat(key, value, avg, stddev, up_good):
        if value is None:
            return ""
        if up_good is None:
            return ""
        if stddev < 0.0001:
            stddev_diffs = 0
        else:
            stddev_diffs = (value - avg) / stddev
        if not up_good:
            stddev_diffs = -stddev_diffs
        
        if stddev_diffs < -2.5:
            # -2.5 or below
            return "text-red-800"
        if stddev_diffs < -1.5:
            # -2.5 to -1.5
            return "text-red-700"
        if stddev_diffs < -0.5:
            # -1.5 to -0.5
            return "text-orange-700"
        if stddev_diffs < 0.5:
            # -0.5 to 0.5
            return "text-yellow-700"
        if stddev_diffs < 1.5:
            # 0.5 to 1.5
            return "text-green-700"
        # 1.5 or above
        return "text-green-800"

    return templates.TemplateResponse(
        request=request, name="stats.html", context={"players": players, "batters": batters, "pitchers": pitchers,
                                                     "team": team, "league": league,
                                                     "league_agg": league_agg, "subleague_agg": subleague_agg,
                                                     "stat_defs_b": defs_b2, "stat_defs_p": defs_p2,
                                                     "stat_defs_f": defs_f2, "color_stat": color_stat,
                                                     "team_batting": team_batting, "team_pitching": team_pitching}
    )

@app.get("/players", response_class=HTMLResponse)
async def players(request: Request):
    return templates.TemplateResponse(
        request=request, name="players.html", context={}
    )


@app.get("/game/{game_id}", response_class=HTMLResponse)
async def stats(request: Request, game_id: str):
    from all_stat_columns import ALL_STAT_COLUMNS

    GAME_ID = "6805db4bac48194de3cd42d9"

    HITS_EXPR = "singles + doubles + triples + home_runs"
    BA_EXPR = f"CAST(({HITS_EXPR}) AS REAL) / CAST(at_bats AS REAL)"
    OBP_EXPR = f"CAST(({HITS_EXPR} + walked + hit_by_pitch) AS REAL) / CAST(plate_appearances AS REAL)"
    SLG_EXPR = "CAST((singles + 2 * doubles + 3 * triples + 4 * home_runs) AS REAL) / CAST(at_bats AS REAL)"
    IP_EXPR = "(CAST(outs AS REAL) / 3)"
    ERA_EXPR = f"(9 * earned_runs) / {IP_EXPR}"

    def format_ip(outs):
        return ".".join([str(int(outs / 3)), str(outs % 3)])

    def set_format(df, side):
        styler = Styler(df, uuid_len=0, cell_ids=False)
        styler.hide()
        if side == "batting":
            styler.format(precision=3, subset=["BA", "OPS"])
        if side == "pitching":
            styler.format(precision=2, subset=["ERA"])
            styler.hide(["order_h", "order_a"], axis=1)
        return styler

    data = utils.get_object("game", GAME_ID)
    events = data["EventLog"]
    away_lineup = events[2]["message"].split("<br>")[:-1]
    away_lineup_names = [x.split(maxsplit=2)[-1] for x in away_lineup]
    home_lineup = events[3]["message"].split("<br>")[:-1]
    home_lineup_names = [x.split(maxsplit=2)[-1] for x in home_lineup]
    away_team = data["AwayTeamID"]
    away_team_name = data["AwayTeamName"]
    away_team_abbrev = data["AwayTeamAbbreviation"]
    home_team = data["HomeTeamID"]
    home_team_name = data["HomeTeamName"]
    home_team_abbrev = data["HomeTeamAbbreviation"]
    day = data["Day"]
    stats = data["Stats"]
    weather = data["Weather"]

    box_cols_batting = ["name", "plate_appearances", "at_bats", "runs", "hits",
                        "runs_batted_in", "walked", "struck_out", "left_on_base", "ba", "ops"]
    box_cols_pitching = ["name", "ip", "hits_allowed", "runs", "earned_runs",
                         "walks", "strikeouts", "home_runs_allowed", "era", "order_h", "order_a"]
    box_cols_batting_rename = {
        "plate_appearances": "PA",
        "at_bats": "AB",
        "runs": "R",
        "hits": "H",
        "runs_batted_in": "RBI",
        "walked": "BB",
        "struck_out": "K",
        "left_on_base": "LOB",
        "ba": "BA",
        "ops": "OPS"
    }
    box_cols_pitching_rename = {
        "ip": "IP",
        "hits_allowed": "H",
        "runs": "R",
        "earned_runs": "ER",
        "walks": "BB",
        "strikeouts": "K",
        "home_runs_allowed": "HR",
        "era": "ERA"
    }

    home_pitcher_order = pd.Series(dtype="int", name="order_h")
    away_pitcher_order = pd.Series(dtype="int", name="order_a")
    i_h = 0
    i_a = 0
    for event in events:
        if event["pitcher"]:
            if event["inning_side"] == 0 and event["pitcher"] not in home_pitcher_order:
                home_pitcher_order[event["pitcher"]] = i_h
                i_h += 1
            if event["inning_side"] == 1 and event["pitcher"] not in away_pitcher_order:
                away_pitcher_order[event["pitcher"]] = i_a
                i_a += 1

    def get_player_name(id):
        return utils.player_name(utils.get_object("player", id))

    stat_df = pd.DataFrame(columns=["team", "name", *ALL_STAT_COLUMNS])
    for team, teamstats in stats.items():
        if team == away_team:
            current_team = away_team_name
        else:
            current_team = home_team_name
        current_stats = pd.DataFrame(teamstats).T
        current_stats["team"] = current_team
        stat_df = pd.concat([stat_df, current_stats])
    stat_df["name"] = [get_player_name(x) for x in stat_df.index]

    stats_q = f"""
    SELECT
        player_id,
        {BA_EXPR} AS ba,
        {OBP_EXPR} AS obp,
        {SLG_EXPR} AS slg,
        ({OBP_EXPR} + {SLG_EXPR}) AS ops,
        {ERA_EXPR} AS era
    FROM player_stats
    WHERE player_id IN {tuple(stat_df.index)}
    """
    with utils.db() as con:
        db_stats = pd.read_sql_query(stats_q, con).set_index("player_id")
    stat_df = stat_df.join(db_stats)

    stat_df = stat_df.join(home_pitcher_order, on="name").join(away_pitcher_order, on="name")

    fillcols_batting = {
        "singles": 0,
        "doubles": 0,
        "triples": 0,
        "home_runs": 0,
        "at_bats": 0,
        "runs": 0,
        "hits": 0,
        "runs_batted_in": 0,
        "walked": 0,
        "struck_out": 0,
        "left_on_base": 0,
    }
    fillcols_pitching = {
        "outs": 0,
        "hits_allowed": 0,
        "unearned_runs": 0,
        "earned_runs": 0,
        "walks": 0,
        "strikeouts": 0,
        "home_runs_allowed": 0,
    }
    stat_df = stat_df.convert_dtypes()

    away_batting = pd.DataFrame(columns=["team", "lineup_num", "name", "position", *ALL_STAT_COLUMNS])
    for i, entry in enumerate(away_lineup):
        batter_statline = stat_df[stat_df["name"].str.contains(away_lineup_names[i])].copy()
        batter_statline["position"] = entry.split(maxsplit=2)[1]
        batter_statline["lineup_num"] = i + 1
        away_batting = pd.concat([away_batting, batter_statline])

    away_batting = away_batting.fillna(value=fillcols_batting)
    away_batting["hits"] = away_batting["singles"] + away_batting["doubles"] + away_batting["triples"] + away_batting[
        "home_runs"]
    away_batting = away_batting.dropna(axis=1, how="all")

    home_batting = pd.DataFrame(columns=["team", "lineup_num", "name", "position"])
    for i, entry in enumerate(home_lineup):
        batter_statline = stat_df[stat_df["name"].str.contains(home_lineup_names[i])].copy()
        batter_statline["position"] = entry.split(maxsplit=2)[1]
        batter_statline["lineup_num"] = i + 1
        home_batting = pd.concat([home_batting, batter_statline])

    home_batting = home_batting.fillna(value=fillcols_batting)
    home_batting["hits"] = home_batting["singles"] + home_batting["doubles"] + home_batting["triples"] + home_batting[
        "home_runs"]
    home_batting = home_batting.dropna(axis=1, how="all")

    pitchers = stat_df[~pd.isna(stat_df.appearances)].fillna(value=fillcols_pitching).dropna(axis=1, how="all")
    pitchers["ip"] = pitchers["outs"].map(format_ip)
    pitchers["runs"] = pitchers["unearned_runs"] + pitchers["earned_runs"]
    away_pitchers = pitchers[pitchers.name.isin(away_pitcher_order.index)]
    home_pitchers = pitchers[pitchers.name.isin(home_pitcher_order.index)]

    renamecols = box_cols_batting_rename
    renamecols["name"] = f"Batters - {away_team_abbrev}"
    away_batting_box = away_batting[box_cols_batting].sort_values(
        by="name", key=lambda x: away_lineup_names).rename(columns=renamecols).pipe(set_format, side="batting")

    renamecols = box_cols_pitching_rename
    renamecols["name"] = f"Pitchers - {away_team_abbrev}"
    away_pitching_box = away_pitchers[box_cols_pitching].sort_values(
        by="order_a").rename(columns=box_cols_pitching_rename).pipe(set_format, side="pitching")

    renamecols = box_cols_batting_rename
    renamecols["name"] = f"Batters - {home_team_abbrev}"
    home_batting_box = home_batting[box_cols_batting].sort_values(
        by="name", key=lambda x: home_lineup_names).rename(columns=box_cols_batting_rename).pipe(set_format,
                                                                                                 side="batting")

    renamecols = box_cols_pitching_rename
    renamecols["name"] = f"Pitchers - {home_team_abbrev}"
    home_pitching_box = home_pitchers[box_cols_pitching].sort_values(
        by="order_h").rename(columns=box_cols_pitching_rename).pipe(set_format, side="pitching")

    return templates.TemplateResponse(request=request, name="box_score.html", context={
        "away_batting_box": away_batting_box.to_html(), "home_batting_box": home_batting_box.to_html(),
        "away_pitching_box": away_pitching_box.to_html(), "home_pitching_box":home_pitching_box.to_html(),
        "away_team_name": away_team_name, "home_team_name": home_team_name, "away_team_abbrev": away_team_abbrev,
        "home_team_abbrev": home_team_abbrev, "day": day, "weather": weather
    })
