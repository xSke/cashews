from cashews import utils
import sqlite3, math


HITS_EXPR = "singles + doubles + triples + home_runs"
BA_EXPR = f"CAST(({HITS_EXPR}) AS REAL) / CAST(at_bats AS REAL)"
OBP_EXPR = f"CAST(({HITS_EXPR} + walked + hit_by_pitch) AS REAL) / CAST(plate_appearances AS REAL)"
SLG_EXPR = "CAST((singles + 2 * doubles + 3 * triples + 4 * home_runs) AS REAL) / CAST(at_bats AS REAL)"
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

    (9 * earned_runs) / {IP_EXPR} AS era,
    (walks + hits_allowed) / {IP_EXPR} AS whip,
    (9 * home_runs_allowed) / {IP_EXPR} AS hr9,
    (9 * walks) / {IP_EXPR} AS bb9,
    (9 * strikeouts) / {IP_EXPR} AS k9,
    (9 * hits_allowed) / {IP_EXPR} AS h9,

    (CAST(stolen_bases AS REAL) / (stolen_bases + caught_stealing)) AS sb_success,
    
    {FPCT_EXPR} AS fpct
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
