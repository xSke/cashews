from cashews import utils
import timeit
import sqlite3
from sqlalchemy import create_engine
import pandas as pd
import polars as pl

engine = create_engine("sqlite:///data/db.db")
uri = "sqlite://data/db.db"

HITS_EXPR = "singles + doubles + triples + home_runs"
BA_EXPR = f"CAST(({HITS_EXPR}) AS REAL) / CAST(at_bats AS REAL)"
OBP_EXPR = f"CAST(({HITS_EXPR} + walked + hit_by_pitch) AS REAL) / CAST(plate_appearances AS REAL)"
SLG_EXPR = "CAST((singles + 2 * doubles + 3 * triples + 4 * home_runs) AS REAL) / CAST(at_bats AS REAL)"
BABIP_EXPR = "CAST((singles + doubles + triples) AS REAL) / CAST((at_bats - struck_out + sac_flies) AS REAL)"
IP_EXPR = f"(CAST(outs AS REAL) / 3)"
FPCT_EXPR = f"CAST((putouts + assists) AS REAL) / CAST((putouts + assists + errors) AS REAL)"

STATS_Q_1 = f"""
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

STATS_Q_2 = f"""
SELECT
    player_stats.*
FROM player_stats
"""

p1 = """
stats = pl.read_database_uri(query=STATS_Q_2, uri=uri, engine="adbc")
stats = stats.with_columns(ip=pl.col("outs")/3,
                           ba=(pl.col("singles") + pl.col("doubles") + pl.col("triples") + pl.col("home_runs"))/pl.col("at_bats"),
                           obp=(pl.col("singles") + pl.col("doubles") + pl.col("triples") + pl.col("home_runs") + pl.col("walked") + pl.col("hit_by_pitch"))/pl.col("plate_appearances"),
                           slg=(pl.col("singles") + 2*pl.col("doubles") + 3*pl.col("triples") + 4*pl.col("home_runs"))/pl.col("at_bats"))\
                           .with_columns(
                           ops=pl.col("obp") + pl.col("slg"),
                           babip=(pl.col("singles") + pl.col("doubles") + pl.col("triples"))/(pl.col("at_bats") - pl.col("struck_out") + pl.col("sac_flies")),
                           era=(9 * pl.col("earned_runs"))/pl.col("ip"),
                           whip=(pl.col("walks") + pl.col("hits_allowed"))/pl.col("ip"),
                           hr9=(9 * pl.col("home_runs_allowed"))/pl.col("ip"),
                           bb9=(9 * pl.col("walks"))/pl.col("ip"),
                           k9=(9 * pl.col("strikeouts"))/pl.col("ip"),
                           h9=(9 * pl.col("hits_allowed"))/pl.col("ip"),
                           sb_attempts=pl.col("stolen_bases") + pl.col("caught_stealing"),
                           sb_success=pl.col("stolen_bases")/(pl.col("stolen_bases") + pl.col("caught_stealing")),
                           fpct=(pl.col("putouts") + pl.col("assists"))/(pl.col("putouts") + pl.col("assists") + pl.col("errors"))
                           )
"""

s1 = """
with utils.db() as con:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(STATS_Q_1).fetchall()
cols = rows[0].keys()
stats = pd.DataFrame(rows, columns=cols).set_index(["team_id", "player_id"])
"""

s1_2 = """
with utils.db() as con:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(STATS_Q_2).fetchall()
keys = set(dict(rows[0]).keys()) - set(["player_id", "team_id", "last_update"])
"""

s1_3 = """
with utils.db() as con:
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(STATS_Q_1).fetchall()
keys = set(dict(rows[0]).keys()) - set(["player_id", "team_id", "last_update"])
"""

s2 = """
with utils.db() as con:
    stats = pd.read_sql_query(STATS_Q_1, con).set_index(["team_id", "player_id"])
"""

s3_1 = """
with utils.db() as con:
    stats = pd.read_sql_query(STATS_Q_2, con).set_index(["team_id", "player_id"])
"""

s3_2 = """
# with utils.db() as con:
stats = pd.read_sql_table("player_stats", engine).set_index(["team_id", "player_id"])
"""

s3 = """
with utils.db() as con:
    stats = pd.read_sql_query(STATS_Q_2, con).set_index(["team_id", "player_id"])
stats = stats.assign(ip=lambda x: x.outs/3,
                     ba=lambda x: (x.singles + x.doubles + x.triples + x.home_runs)/x.at_bats,
                     obp=lambda x: (x.singles + x.doubles + x.triples + x.home_runs + x.walked + x.hit_by_pitch)/x.plate_appearances,
                     slg=lambda x: (x.singles + 2*x.doubles + 3*x.triples + 4*x.home_runs)/x.at_bats,
                     ops=lambda x: x.obp + x.slg,
                     babip=lambda x: (x.singles + x.doubles + x.triples)/(x.at_bats - x.struck_out + x.sac_flies),
                     era=lambda x: (9 * x.earned_runs)/x.ip,
                     whip=lambda x: (x.walks + x.hits_allowed)/x.ip,
                     hr9=lambda x: (9 * x.home_runs_allowed)/x.ip,
                     bb9=lambda x: (9 * x.walks)/x.ip,
                     k9=lambda x: (9 * x.strikeouts)/x.ip,
                     h9=lambda x: (9 * x.hits_allowed)/x.ip,
                     sb_attempts=lambda x: x.stolen_bases + x.caught_stealing,
                     sb_success=lambda x: x.stolen_bases/(x.stolen_bases + x.caught_stealing),
                     fpct=lambda x: (x.putouts + x.assists)/(x.putouts + x.assists + x.errors)
                     )
"""

# setup = """
# with utils.db() as con:
#     stats = pd.read_sql_query(STATS_Q_2, con).set_index(["team_id", "player_id"])
# """
#
# statement = """
# stats = stats.assign(ip=lambda x: x.outs/3,
#                      ba=lambda x: (x.singles + x.doubles + x.triples + x.home_runs)/x.at_bats,
#                      obp=lambda x: (x.singles + x.doubles + x.triples + x.home_runs + x.walked + x.hit_by_pitch)/x.plate_appearances,
#                      slg=lambda x: (x.singles + 2*x.doubles + 3*x.triples + 4*x.home_runs)/x.at_bats,
#                      ops=lambda x: x.obp + x.slg,
#                      babip=lambda x: (x.singles + x.doubles + x.triples)/(x.at_bats - x.struck_out + x.sac_flies),
#                      era=lambda x: (9 * x.earned_runs)/x.ip,
#                      whip=lambda x: (x.walks + x.hits_allowed)/x.ip,
#                      hr9=lambda x: (9 * x.home_runs_allowed)/x.ip,
#                      bb9=lambda x: (9 * x.walks)/x.ip,
#                      k9=lambda x: (9 * x.strikeouts)/x.ip,
#                      h9=lambda x: (9 * x.hits_allowed)/x.ip,
#                      sb_attempts=lambda x: x.stolen_bases + x.caught_stealing,
#                      sb_success=lambda x: x.stolen_bases/(x.stolen_bases + x.caught_stealing),
#                      fpct=lambda x: (x.putouts + x.assists)/(x.putouts + x.assists + x.errors)
#                      )
# """

# t = timeit.Timer(statement, setup=setup, globals=globals())
# (n, time) = t.autorange()
# times = t.repeat(repeat=10, number=n)
# print(round(min(times) / n, 6))
#


def time_thing(statement, repeat=10, number=1):
    t = timeit.Timer(statement, globals=globals())
    time = t.repeat(repeat=repeat, number=number)
    return round(min(time)/number, 6)


print("polars method:", time_thing(p1, number=5))
print("all_player_stats method:", time_thing(s1_3, number=5))
# print("all_player_stats method (no rate stats):", time_thing(s1_2, number=5))
print("all_player_stats method into dataframe:", time_thing(s1, number=5))
print("pd.read_sql_query method:", time_thing(s2, number=5))
print("pd.read_sql_query + assign method:", time_thing(s3, number=5))
# print("pd.read_sql_query method (no rate stats):", time_thing(s3_1, number=5))
# print("pd.read_sql_table method (no rate stats):", time_thing(s3_2, number=5))


