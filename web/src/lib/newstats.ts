import * as uDSV from "udsv";
import { ColumnTable, table } from "arquero";
import { API_BASE, chronLatestEntityQuery } from "./data";
import {
  queryOptions,
  useQuery,
  useSuspenseQuery,
} from "@tanstack/react-query";
import { useMemo } from "react";

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

export type GroupKey =
  | "player"
  | "team"
  | "game"
  | "league"
  | "season"
  | "day"
  | "player_name";

export interface StatsQuery {
  fields: StatKey[];
  group?: GroupKey[];
  league?: string;
  season?: number;
  day?: number;
  team?: string;
  names?: boolean;
}

export interface RawTable {
  data: { [k: string]: any };
  colNames: string[];
}

export type StatRow = {
  player_id?: string;
  team_id?: string;
  game_id?: string;
  league_id?: string;
  season?: number;
  day?: number;
} & { [stat in StatKey]?: number };

export async function getStats(q: StatsQuery): Promise<ColumnTable> {
  const qs = new URLSearchParams();
  qs.set("fields", q.fields.join(","));
  if (q.group) qs.set("group", q.group.join(","));
  if (q.season !== undefined) qs.set("season", q.season.toString());
  if (q.day !== undefined) qs.set("day", q.day.toString());
  if (q.team !== undefined) qs.set("team", q.team);
  if (q.league !== undefined) qs.set("league", q.league);
  if (q.names !== undefined) qs.set("names", q.names?.toString());

  const url = API_BASE + `/stats?${qs.toString()}`;

  const resp = await fetch(url);
  console.log(url);
  const textStream = resp.body!.pipeThrough(new TextDecoderStream());
  let parser: uDSV.Parser | null = null;

  for await (const strChunk of textStream) {
    if (parser == null) {
      let schema = uDSV.inferSchema(
        strChunk.slice(0, strChunk.lastIndexOf("\n"))
      );
      parser = uDSV.initParser(schema);
    }
    parser.chunk(strChunk, parser.typedCols);
  }

  const colNames = parser?.schema.cols.map((c) => c.name) ?? [];

  const result = parser?.end() ?? [];
  const resultObj = Object.fromEntries(
    colNames.map((name, i) => [name, result[i]])
  );

  const t = { data: resultObj, colNames };
  return table(t.data, t.colNames);
}

export function statsQuery(q: StatsQuery) {
  return queryOptions({
    queryKey: [
      "stats",
      q.fields.join(",") ?? null,
      q.group?.join(",") ?? null,
      q.league ?? null,
      q.season ?? null,
      q.day ?? null,
    ],
    queryFn: () => getStats(q),
  });
}

export function useStatsTable(raw: RawTable) {
  return useMemo(() => {
    return table(raw.data, raw.colNames);
  }, [raw]);
}
