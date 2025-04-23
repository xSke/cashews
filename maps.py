import os, json, uuid, requests

import utils

def get_key():
    return os.getenv("MAPS_API_KEY")

def get_location(location):
    with utils.db() as con:
        cur = con.cursor()
        res = cur.execute("select data from locations where loc = ?", (location,)).fetchone()
        if res:
            return json.loads(res[0]), True
        return None, False

def search_and_cache_location(location):
    data, found = get_location(location)
    if found:
        return data
    
    result = _search_location_inner(location)
    # yes, save the result even if it's None/null
    # that means we searched and didn't find. don't search again. expensive.
    result_str = json.dumps(result, sort_keys=True)
    with utils.db() as con:
        cur = con.cursor()
        cur.execute("insert into locations(loc, data) values (?, ?) on conflict do nothing", (location, result_str))
        if result and result.get("_text"):
            cur.execute("insert into locations(loc, data) values (?, ?) on conflict do nothing", (result["_text"], result_str))
        con.commit()
    
    import time
    # time.sleep(1)
    return result

SESS = requests.Session()
def _search_location_inner(location):
    if not get_key():
        raise Exception("no maps api key")

    # expensive!!!
    session = str(uuid.uuid4())

    print("!!!!! searching:", location, flush=True)
    res = SESS.post("https://places.googleapis.com/v1/places:autocomplete", json={
        "input": location,
        "includedPrimaryTypes": "(cities)",
        "sessionToken": session
    }, headers={
        "X-Goog-Api-Key": get_key(),
        "X-Goog-FieldMask": "suggestions.placePrediction.placeId,suggestions.placePrediction.text.text"
    })
    if res.status_code != 200:
        print(res.text)
    res.raise_for_status()
    res = res.json()

    if "suggestions" not in res:
        return None
    
    for suggestion in res["suggestions"]:
        place_id = suggestion["placePrediction"]["placeId"]
        text = suggestion["placePrediction"]["text"]["text"]

        res = SESS.get("https://places.googleapis.com/v1/places/" + place_id, headers={
            "X-Goog-Api-Key": get_key(),
            "X-Goog-FieldMask": "id,location,formattedAddress,timeZone"
        }, params={"sessionToken": session}).json()
        if "error" in res:
            raise Exception("error: " + res["error"]["message"])
        res["_text"] = text
        res["_original"] = location
        return res
    
def fill_locations():
    utils.init_db()
    for _, team, _ in utils.get_all("team"):
        loc = team["FullLocation"]
        result = search_and_cache_location(loc)
        if result:
            print(f"location: {loc} found")
        else:
            print(f"location: {loc} not found")

if __name__ == "__main__":
    fill_locations()