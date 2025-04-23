import hashlib
import json
from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone
import pandas as pd
import utils
import requests
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine
import os

DB_PATH = "data/db.db"

engine = create_engine("sqlite:///"+DB_PATH, echo=False)


def db():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_PATH)
    con = engine.connect(path)
    return con


MY_TEAM_ID = "6805f2f6c2312c04f0b4ee23"  # bell layers
HTTP_CACHE_DIR = Path("http_cache")

batting = ['plate_appearances', 'plate_appearances_risp',
           'at_bats', 'at_bats_risp',
           'runs', 'runs_risp',
           'singles', 'singles_risp',
           'doubles', 'doubles_risp',
           'triples', 'triples_risp',
           'home_runs', 'home_runs_risp',
           'runs_batted_in', 'runs_batted_in_risp',
           'walked', 'walked_risp',
           'hit_by_pitch', 'hit_by_pitch_risp',
           'struck_out', 'struck_out_risp',
           'caught_double_play', 'caught_double_play_risp',
           'grounded_into_double_play', 'grounded_into_double_play_risp',
           'groundout', 'groundout_risp',
           'flyouts', 'flyouts_risp',
           'popouts', 'popouts_risp',
           'lineouts', 'lineouts_risp',
           'force_outs', 'force_outs_risp',
           'field_out', 'field_out_risp',
           'fielders_choice', 'fielders_choice_risp',
           'reached_on_error', 'reached_on_error_risp',
           'sac_flies', 'sac_flies_risp',
           'left_on_base', 'left_on_base_risp',
           'stolen_bases', 'stolen_bases_risp',
           'caught_stealing', 'caught_stealing_risp',
           ]

pitching = ['appearances',
            'starts',
            'games_finished',
            'wins',
            'losses',
            'saves',
            'blown_saves',
            'quality_starts',
            'complete_games',
            'shutouts',
            'no_hitters',
            'perfect_games',
            'batters_faced', 'batters_faced_risp',
            'outs',
            'hits_allowed', 'hits_allowed_risp',
            'earned_runs', 'earned_runs_risp',
            'unearned_runs', 'unearned_runs_risp',
            'home_runs_allowed', 'home_runs_allowed_risp',
            'pitches_thrown', 'pitches_thrown_risp',
            'hit_batters', 'hit_batters_risp',
            'walks', 'walks_risp',
            'strikeouts', 'strikeouts_risp',
            'inherited_runners', 'inherited_runners_risp',
            'inherited_runs_allowed', 'inherited_runs_allowed_risp'
            ]

fielding = ['assists',
            'assists_risp',
            'putouts',
            'putouts_risp',
            'errors',
            'errors_risp',
            'double_plays',
            'double_plays_risp',
            ]

batting_norisp = ['plate_appearances',
                  'at_bats',
                  'runs',
                  'singles',
                  'doubles',
                  'triples',
                  'home_runs',
                  'runs_batted_in',
                  'walked',
                  'hit_by_pitch',
                  'struck_out',
                  'caught_double_play',
                  'grounded_into_double_play',
                  'groundout',
                  'flyouts',
                  'popouts',
                  'lineouts',
                  'force_outs',
                  'field_out',
                  'fielders_choice',
                  'reached_on_error',
                  'sac_flies',
                  'left_on_base',
                  'stolen_bases',
                  'caught_stealing',
                  ]

pitching_norisp = ['appearances',
                   'starts',
                   'games_finished',
                   'wins',
                   'losses',
                   'saves',
                   'blown_saves',
                   'quality_starts',
                   'complete_games',
                   'shutouts',
                   'no_hitters',
                   'perfect_games',
                   'batters_faced',
                   'outs',
                   'hits_allowed',
                   'earned_runs',
                   'unearned_runs',
                   'home_runs_allowed',
                   'pitches_thrown',
                   'hit_batters',
                   'walks',
                   'strikeouts',
                   'inherited_runners',
                   'inherited_runs_allowed',
                   ]

fielding_norisp = ['assists',
                   'putouts',
                   'errors',
                   'double_plays',
                   ]


def stable_str_hash(in_val: str) -> str:
    return hex(int(hashlib.md5(in_val.encode("utf-8")).hexdigest(), 16))[2:]


def get_json(url: str) -> dict:
    HTTP_CACHE_DIR.mkdir(exist_ok=True)

    cache = {}
    cache_file_path = HTTP_CACHE_DIR / f"{stable_str_hash(url)}.json"
    try:
        with open(cache_file_path) as cache_file:
            cache = json.load(cache_file)
    except FileNotFoundError:
        pass

    # Return from cache if the cache entry is less than 5 minutes old
    now = datetime.now(timezone.utc)
    if (
            url in cache and
            "__archived_at" in cache[url] and
            cache[url]["__archived_at"] > (now - timedelta(minutes=5)).isoformat()
    ):
        return cache[url]

    data = requests.get(url).json()
    cache[url] = data
    cache[url]["__archived_at"] = now.isoformat()
    with open(cache_file_path, "w") as cache_file:
        json.dump(cache, cache_file)

    return data


def dot_format(in_val: float) -> str:
    ip_whole = int(in_val)
    ip_remainder = int((in_val - ip_whole) / 3 * 10)
    if ip_remainder == 0:
        ip_str = f"{ip_whole}.0"
    else:
        ip_str = f"{ip_whole}.{ip_remainder}"
    return ip_str


def main():
    # usage: python basic_stats.py 6805f2f6c2312c04f0b4ee23
    team_id = sys.argv[1]
    team_obj = get_json(f"https://freecashe.ws/api/playersbyteam/{team_id}")
    team_stats = pd.DataFrame(columns=["player_id", "team_id", "team_name", "player_name", "position",
                                       *batting, *pitching, *fielding])

    for player_id, player_obj in team_obj["players"].items():
        if not player_obj["Stats"]:
            print("No stats for", player_obj["FirstName"], player_obj["LastName"])

        player_stats = pd.DataFrame(player_obj["Stats"].values())
        player_stats["player_id"] = player_id
        player_stats["team_id"] = player_obj["Stats"].keys()
        player_stats["player_name"] = " ".join([player_obj["FirstName"], player_obj["LastName"]])
        player_stats["position"] = player_obj["Position"]

        team_stats = pd.concat([team_stats, player_stats], ignore_index=True)

    team_stats.set_index("player_id", inplace=True)
    team_stats["team_name"] = team_obj["team"]["Name"]
    team_stats.fillna(0, inplace=True)
    team_batting_norisp = team_stats[["team_id", "team_name", "player_name", "position",
                                      *batting_norisp]].copy()
    team_batting_norisp.rename(columns={'plate_appearances': 'pa',
                                        'at_bats': 'ab',
                                        'runs': 'r',
                                        'singles': 'x1b',
                                        'doubles': 'x2b',
                                        'triples': 'x3b',
                                        'home_runs': 'hr',
                                        'runs_batted_in': 'rbi',
                                        'walked': 'bb',
                                        'hit_by_pitch': 'hbp',
                                        'struck_out': 'so',
                                        'caught_double_play': 'cdp',
                                        'grounded_into_double_play': 'gidp',
                                        'groundout': 'go',
                                        'flyouts': 'fo',
                                        'popouts': 'po',
                                        'lineouts': 'lo',
                                        'sac_flies': 'sf',
                                        'force_outs': 'force_outs',
                                        'field_out': 'field_outs',
                                        'reached_on_error': 'roe',
                                        'fielders_choice': 'fc',
                                        'left_on_base': 'lob',
                                        'stolen_bases': 'sb',
                                        'caught_stealing': 'cs',
                                        }, inplace=True)
    team_batting_norisp['hits'] = (team_batting_norisp["x1b"] + team_batting_norisp["x2b"] +
                                   team_batting_norisp["x3b"] + team_batting_norisp["hr"])
    team_batting_norisp['tb'] = (team_batting_norisp["x1b"] + 2*team_batting_norisp["x2b"] +
                                 3*team_batting_norisp["x3b"] + 4*team_batting_norisp["hr"])
    team_batting_norisp['ba'] = (team_batting_norisp['hits'] / team_batting_norisp['ab']).round(3)
    team_batting_norisp['obp'] = ((team_batting_norisp['hits'] + team_batting_norisp['bb'] + team_batting_norisp['hbp'])
                                  / team_batting_norisp['pa']).round(3)
    team_batting_norisp['slg'] = (team_batting_norisp['tb'] / team_batting_norisp['ab']).round(3)
    team_batting_norisp['ops'] = team_batting_norisp['obp'] + team_batting_norisp['slg']

    team_pitching_norisp = team_stats[["team_id", "team_name", "player_name", "position",
                                       *pitching_norisp, ]].copy()
    team_pitching_norisp.rename(columns={'appearances': 'g',
                                         'starts': 'gs',
                                         'games_finished': 'gf',
                                         'wins': 'w',
                                         'losses': 'l',
                                         'saves': 's',
                                         'blown_saves': 'bs',
                                         'quality_starts': 'qs',
                                         'complete_games': 'cg',
                                         'shutouts': 'sho',
                                         'no_hitters': 'nh',
                                         'perfect_games': 'pg',
                                         'batters_faced': 'bf',
                                         'hits_allowed': 'h',
                                         'earned_runs': 'er',
                                         'unearned_runs': 'ur',
                                         'home_runs_allowed': 'hr',
                                         'pitches_thrown': 'np',
                                         'hit_batters': 'hb',
                                         'walks': 'bb',
                                         'strikeouts': 'so',
                                         'inherited_runners': 'ir',
                                         'inherited_runs_allowed': 'ira',
                                         }, inplace=True)
    team_pitching_norisp['ip'] = (team_pitching_norisp['outs'] / 3).apply(dot_format)
    team_pitching_norisp['era'] = (team_pitching_norisp['er'] / team_pitching_norisp['outs'] * 27).round(2)
    team_pitching_norisp['ra9'] = (
            (team_pitching_norisp['er'] + team_pitching_norisp['ur']) / team_pitching_norisp['outs'] * 27).round(2)
    team_pitching_norisp['whip'] = ((team_pitching_norisp['h'] + team_pitching_norisp['bb'])
                                    / team_pitching_norisp['outs'] * 3).round(2)
    team_pitching_norisp['hr9'] = (team_pitching_norisp['hr'] / team_pitching_norisp['outs'] * 27).round(2)
    team_pitching_norisp['h9'] = (team_pitching_norisp['h'] / team_pitching_norisp['outs'] * 27).round(2)
    team_pitching_norisp["bb9"] = (team_pitching_norisp['bb'] / team_pitching_norisp['outs'] * 27).round(2)
    team_pitching_norisp["so9"] = (team_pitching_norisp['so'] / team_pitching_norisp['outs'] * 27).round(2)
    team_pitching_norisp["so/bb"] = (team_pitching_norisp['so'] / team_pitching_norisp['bb']).round(2)

    team_fielding_norisp = team_stats[["team_id", "team_name", "player_name", "position",
                                       *fielding_norisp, ]].copy()
    team_fielding_norisp['f_pct'] = ((team_fielding_norisp['assists'] + team_fielding_norisp['putouts']) /
                                     (team_fielding_norisp['assists'] + team_fielding_norisp['putouts']
                                      + team_fielding_norisp['errors'])).round(3)
    team_fielding_norisp.rename(columns={'errors': 'E', 'double_plays': 'DP'}, inplace=True)

    # print(team_batting_norisp.loc[(team_batting_norisp["pa"] > 0),
    #                               ["player_name", "position", "pa", "ba", "obp", "slg", "ops"]])
    # print(team_pitching_norisp.loc[(team_pitching_norisp["g"] > 0),
    #                                ["player_name", "position", "ip", "era", "whip", "h9", "hr9", "so9", "bb9"]])
    # print(team_fielding_norisp.loc[:, "player_name":])

    team_stats_norisp = team_batting_norisp.join(team_pitching_norisp[["ip", "era", "whip", "h9", "hr9", "so9", "bb9"]])
    # print(team_stats_norisp)
    # with db() as con:
    team_batting_norisp.to_sql("batting_stats", con=engine, if_exists="append", method=insert_on_conflict_update)


def insert_on_conflict_update(table, con, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]

    insert_stmt = insert(table.table).values(data)

    do_update_stmt = insert_stmt.on_conflict_do_update(set_=insert_stmt.excluded)

    result = con.execute(do_update_stmt)
    return result.rowcount


if __name__ == '__main__':
    main()

