from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from . import utils
import datetime
app = FastAPI(docs_url=None, redoc_url=None)

templates = Jinja2Templates(directory="templates")

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

# @app.get("/api/teams")
# async def api_teams():
#     keep_properties = set(["Color", "Emoji", "FullLocation", "Location", "League", "Name", "Record", "_id"])
#     teams = utils.get_all_as_dict("teams", filter_props(keep_properties))
#     return teams

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
        dt = datetime.datetime.fromtimestamp(game_timestamp / 1000, tz=datetime.UTC)
        game["time_formatted"] = dt.strftime("%Y-%m-%d %H:%M") + " UTC"
        game["time_iso"] = dt.isoformat()

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
    for league_id in state["LesserLeagues"]:
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