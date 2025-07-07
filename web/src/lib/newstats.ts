import Papa from "papaparse";
import { API_BASE } from "./data";

// WIP
export type StatKey =
  | "allowed_stolen_bases"
  | "appearances"
  | "assists"
  | "at_bats"
  | "batters_faced"
  | "blown_saves"
  | "caught_double_play"
  | "caught_stealing"
  | "complete_games"
  | "double_plays"
  | "doubles"
  | "earned_runs"
  | "errors"
  | "field_out"
  | "fielders_choice"
  | "flyouts"
  | "force_outs"
  | "games_finished"
  | "grounded_into_double_play"
  | "groundouts"
  | "hit_batters"
  | "hit_by_pitch"
  | "hits_allowed"
  | "home_runs"
  | "home_runs_allowed"
  | "inherited_runners"
  | "inherited_runs_allowed"
  | "left_on_base"
  | "lineouts"
  | "losses"
  | "mound_visits"
  | "no_hitters"
  | "outs"
  | "perfect_games"
  | "pitches_thrown"
  | "plate_appearances"
  | "popouts"
  | "putouts"
  | "quality_starts"
  | "reached_on_error"
  | "runners_caught_stealing"
  | "runs"
  | "runs_batted_in"
  | "sac_flies"
  | "sacrifice_double_plays"
  | "saves"
  | "shutouts"
  | "singles"
  | "starts"
  | "stolen_bases"
  | "strikeouts"
  | "struck_out"
  | "triples"
  | "unearned_runs"
  | "walked"
  | "walks"
  | "wins";

export type GroupKey = "player" | "team" | "game" | "league" | "season" | "day";

export interface StatsQuery {
  fields: StatKey[];
  group?: GroupKey[];
}

export type StatRow = {
  player_id?: string;
  team_id?: string;
  game_id?: string;
  league_id?: string;
  season?: number;
  day?: number;
} & { [stat in StatKey]?: number };

export async function getStats(q: StatsQuery) {
  const qs = new URLSearchParams();
  qs.set("fields", q.fields.join(","));
  if (q.group) qs.set("group", q.group.join(","));
  // if (q.season !== undefined) qs.set("season", q.season.toString());
  // if (q.day !== undefined) qs.set("day", q.day.toString());
  // if (q.team !== undefined) qs.set("team", q.team);

  const url = API_BASE + `/stat?${qs.toString()}`;
  const fetchCsv: Promise<Papa.ParseResult<StatRow>> = new Promise(
    (res, rej) => {
      Papa.parse<StatRow>(url, {
        download: true,
        header: true,
        dynamicTyping: true,
        complete(results, _) {
          res(results);
        },
        error(error, _) {
          rej(error);
        },
      });
    },
  );

  const result = await fetchCsv;
  return result.data;
}
