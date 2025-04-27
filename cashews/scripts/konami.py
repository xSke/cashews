from cashews import utils

# ...so, has anyone pitched the konami code?

# 11       12
#    1-2-3
#    4-5-6
#    7-8-9
# 13       14

import re

pattern = re.compile(r"\b2,2,8,8,4,6,4,6\b")
from dataclasses import dataclass

@dataclass(frozen=True)
class PitcherId:
    team_id: str
    pitcher_name: str

@dataclass
class Pitch:
    season: int
    day: int
    game_id: str
    event_index: int
    pitcher: PitcherId
    zone: int
    info: str

    def sort_key(self):
        return (self.season, self.day, self.event_index)
    
pitches_by_pitcher = {}

print("indexing games...")
for game_id, game, _ in utils.get_all("game"):
    for idx, evt in enumerate(game["EventLog"]):
        if evt["zone"] and evt["pitcher"]:
            if evt["inning_side"] == 0:
                pitcher_id = PitcherId(game["HomeTeamID"], evt["pitcher"])
            else:
                pitcher_id = PitcherId(game["AwayTeamID"], evt["pitcher"])

            pitch = Pitch(game["Season"], game["Day"], game_id, idx, pitcher_id, evt["zone"], evt["pitch_info"])
            if pitcher_id not in pitches_by_pitcher:
                pitches_by_pitcher[pitcher_id] = []
            pitches_by_pitcher[pitcher_id].append(pitch)

print("sorting pitches...")
for pitches in pitches_by_pitcher.values():
    pitches.sort(key=lambda x: x.sort_key())

def find_sublist(haystack, needle):
    for start_idx in range(len(haystack) - len(needle) + 1):
        if haystack[start_idx:start_idx+len(needle)] == needle:
            return start_idx
    return None

print("finding...")
import re

matches = 0
for pitcher_id, pitches in pitches_by_pitcher.items():
    zones = ",".join(str(p.zone) for p in pitches)

    for match in pattern.finditer(zones):
        before_match = zones[:match.start(0)]
        match_idx = before_match.count(",")
        match_len = match.group(0).count(",") + 1

        pitch_match = pitches[match_idx:match_idx+match_len]
        print(match.group(0), pitch_match)
        
        matches += 1

print("matches:", matches)