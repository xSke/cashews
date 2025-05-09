from cashews import utils
import sqlite3, math
import pandas as pd

HITS_EXPR = "singles + doubles + triples + home_runs"
BA_EXPR = f"CAST(({HITS_EXPR}) AS REAL) / CAST(at_bats AS REAL)"
OBP_EXPR = f"CAST(({HITS_EXPR} + walked + hit_by_pitch) AS REAL) / CAST(plate_appearances AS REAL)"
SLG_EXPR = "CAST((singles + 2 * doubles + 3 * triples + 4 * home_runs) AS REAL) / CAST(at_bats AS REAL)"
BABIP_EXPR = "CAST((singles + doubles + triples) AS REAL) / CAST((at_bats - struck_out + sac_flies) AS REAL)"
IP_EXPR = f"(CAST(outs AS REAL) / 3)"
FPCT_EXPR = f"CAST((putouts + assists) AS REAL) / CAST((putouts + assists + errors) AS REAL)"

STATS_Q = f"""
SELECT
    player_stats.*,
    {IP_EXPR} AS ip,
    {BA_EXPR} AS ba,
    {OBP_EXPR} AS obp,
    {SLG_EXPR} AS slg,
    ({OBP_EXPR} + {SLG_EXPR}) AS ops,
    {BABIP_EXPR} AS babip,

    (9 * earned_runs) / {IP_EXPR} AS era,
    (walks + hits_allowed) / {IP_EXPR} AS whip,
    (9 * home_runs_allowed) / {IP_EXPR} AS hr9,
    (9 * walks) / {IP_EXPR} AS bb9,
    (9 * strikeouts) / {IP_EXPR} AS k9,
    (9 * hits_allowed) / {IP_EXPR} AS h9,
    
    (stolen_bases + caught_stealing) AS sb_attempts,
    (CAST(stolen_bases AS REAL) / (stolen_bases + caught_stealing)) AS sb_success,
    
    {FPCT_EXPR} AS fpct
FROM player_stats
"""

STATS_AVG_Q = f"""
SELECT
    (9 * SUM(earned_runs)) / (CAST(SUM(outs) AS REAL) / 3) as era_avg,
    CAST(SUM({HITS_EXPR} + walked + hit_by_pitch) AS REAL) / CAST(SUM(plate_appearances) AS REAL) as obp_avg,
    CAST(SUM(singles + 2 * doubles + 3 * triples + 4 * home_runs) AS REAL) / CAST(SUM(at_bats) AS REAL) as slg_avg,
    SUM(home_runs_allowed) AS hr_sum,
    SUM(walks) AS bb_sum,
    SUM(hit_batters) AS hb_sum,
    SUM(strikeouts) AS k_sum,
    (CAST(SUM(outs) AS REAL) / 3) as ip_sum
FROM player_stats
"""

def mean(values):
    values = list(values)
    return sum(values) / len(values)


def filtered_values(rows, key, filter=None):
    values = []
    for row in rows:
        value = row[key]
        if value is None:
            continue
        if filter and not filter(row): 
            continue
        values.append(value)
    return values

# def league_stats_filtered()

def all_player_stats():
    with utils.db() as con:
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        return cur.execute(STATS_Q).fetchall()

import functools
@functools.lru_cache(maxsize=3)
def league_agg_stats(_ttl):
    with utils.db() as con:
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        rows = cur.execute(STATS_Q).fetchall()
    keys = set(dict(rows[0]).keys()) - set(["player_id", "team_id", "last_update"])

    def filter_batters(row):
        return row["plate_appearances"] > 20
    def filter_pitchers(row):
        return row["ip"] > 20

    filters = {}
    for k in "ba obp slg ops sb_success".split():
        filters[k] = filter_batters
    for k in "era whip hr9 bb9 k9 h9".split():
        filters[k] = filter_pitchers

    stats = {}
    for k in keys:
        values = filtered_values(rows, k, filters.get(k))
        
        avg = mean(values)
        variance = mean((point - avg)**2 for point in values)
        stddev = math.sqrt(variance)

        stats[k] = {
            "avg": avg,
            "variance": variance,
            "stddev": stddev,
            "total": sum(values),
            "samples": len(values)
        }
    return stats

import functools
@functools.lru_cache(maxsize=3)
def league_agg_stats_2(_ttl):
    with utils.db() as con:
        stats = pd.read_sql_query(STATS_Q, con).set_index(["team_id", "player_id"])

    all_teams = utils.get_all_as_dict("team")
    stats["league_id"] = stats.index.to_series().apply(lambda idx: all_teams.get(idx[0])["League"])
    
    batting_stats = "ba obp slg ops".split()
    pitching_stats = "era whip hr9 bb9 k9 h9".split()
    steal_stats = "sb_success"

    batters = stats[stats["plate_appearances"] > 20]
    pitchers = stats[stats["ip"] > 20]
    stealers = stats[stats["sb_attempts"] > 5]

    relevant_stats = pd.concat([
        batters[batting_stats],
        pitchers[pitching_stats],
        pd.DataFrame(stealers[steal_stats]),
    ])
    relevant_stats["league_id"] = stats["league_id"]

    return relevant_stats


@functools.lru_cache(maxsize=3)
def league_agg_stats_3(_ttl):
    # these function names are getting worse and worse
    with utils.db() as con:
        agg_stats = pd.read_sql_query(STATS_AVG_Q, con)

    agg_stats["fip_const"] = agg_stats.era_avg - (13*agg_stats.hr_sum + 3*(agg_stats.bb_sum + agg_stats.hb_sum)
                                                  - 2*agg_stats.k_sum)/agg_stats.ip_sum
    return agg_stats


if __name__ == "__main__":
    import time
    ttl = int(time.time() // 60)
    stats = league_agg_stats_3(ttl)
    fip_const = stats.era_avg - (13*stats.hr_sum + 3*(stats.bb_sum + stats.hb_sum) - 2*stats.k_sum)/stats.ip_sum
    print(stats)
    print(stats.era_avg)
    print((13*stats.hr_sum + 3*(stats.bb_sum + stats.hb_sum) - 2*stats.k_sum)/stats.ip_sum)
    print(fip_const)
    print("Diane Huber:", (13*5 + 3*(21 + 2) - 2*26)/31 + fip_const)
    print("Jazzy Potpurri:", (13*12 + 3*(35 + 3) - 2*83)/(92 + 2/3) + fip_const)
    print("Lawrence Marxer:", (13*32 + 3*(77 + 8) - 2*195)/(194 + 2/3) + fip_const)
    print("Catherine Ota:", (13*43 + 3*(83 + 8) - 2*188)/(179 + 1/3) + fip_const)
    print("Joanne Almeida:", (13*33 + 3*(66 + 6) - 2*179) / (182 + 2/3) + fip_const)
    print("Christine van der Linden:", (13*14 + 3*(23 + 3) - 2*77) / (81 + 0/3) + fip_const)
