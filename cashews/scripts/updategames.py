from cashews import utils
import tqdm

utils.init_db()
for game_id in tqdm.tqdm(utils.get_all_ids("game")):
    utils.update_game_data(game_id)