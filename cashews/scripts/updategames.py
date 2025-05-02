from cashews import utils
import tqdm
from cashews import fetch_games
# utils.init_db()
# for game_id in tqdm.tqdm(utils.get_all_ids("game")):
#     utils.update_game_data(game_id)
import sqlite3, hashlib, json, time, requests, os, threading, logging
import zstandard as zstd

from cashews import DATA_DIR

LOG_TLS = threading.local()
ZSTD_TLS = threading.local()
SESS_TLS = threading.local()

url = "https://freecashe.ws/api/games"
data = requests.get(url).json()
with open("game_ids.txt", "w") as f:
    for item in data:
        # print(item)
        f.write(str(item['game_id']))
        f.write("\n")


def init_db():
    with utils.db() as con:
        cur = con.cursor()
        script = """
        alter table games add column away_score int;
        alter table games add column home_score int;
        """
        # cur.executescript(script)
        cur.executescript(utils.MIGRATIONS[-1])
        con.commit()


init_db()

fetch_games.backfill_game_ids()
