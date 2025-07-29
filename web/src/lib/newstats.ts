import * as aq from "arquero";
import qs from "qs";
import { API_BASE } from "./data";
import { queryOptions } from "@tanstack/react-query";
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
  | "player_name"
  | "slot";

export interface StatsQueryFilter {
  // lt?: number;
  gt?: number;
}

export interface StatsQuery {
  fields: StatKey[];
  group?: GroupKey[];
  league?: string;
  season?: number;
  day?: number;
  team?: string;
  names?: boolean;
  filter?: Partial<Record<StatKey, StatsQueryFilter>>;
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

export async function getStats(q: StatsQuery): Promise<aq.ColumnTable> {
  // const qs = new URLSearchParams();
  // qs.set("fields", q.fields.join(","));
  // if (q.group) qs.set("group", q.group.join(","));
  // if (q.season !== undefined) qs.set("season", q.season.toString());
  // if (q.day !== undefined) qs.set("day", q.day.toString());
  // if (q.team !== undefined) qs.set("team", q.team);
  // if (q.league !== undefined) qs.set("league", q.league);
  // if (q.names !== undefined) qs.set("names", q.names?.toString());

  const url =
    API_BASE +
    `/stats?${qs.stringify({
      ...q,
      fields: q.fields?.join(","),
      group: q.group?.join(","),
    })}`;

  const table = await aq.loadCSV(url, {
    header: true,
  });
  return table;
  // const resp = await fetch(url);
  // throwOnError(resp);
  // const textStream = resp.body!.pipeThrough(new TextDecoderStream());
  // let parser: uDSV.Parser | null = null;

  // for await (const strChunk of textStream) {
  //   if (parser == null) {
  //     let schema = uDSV.inferSchema(
  //       strChunk.slice(0, strChunk.lastIndexOf("\n"))
  //     );
  //     parser = uDSV.initParser(schema);
  //   }
  //   parser.chunk(strChunk, parser.typedCols);
  // }

  // const colNames = parser?.schema.cols.map((c) => c.name) ?? [];

  // const result = parser?.end() ?? [];
  // const resultObj = Object.fromEntries(
  //   colNames.map((name, i) => [name, result[i]])
  // );

  // const t = { data: resultObj, colNames };
  // return aq.table(t.data, t.colNames);
}

export function statsQuery(q: StatsQuery) {
  return queryOptions({
    queryKey: [
      "stats",
      q.fields.join(",") ?? null,
      q.group?.join(",") ?? null,
      q.league ?? null,
      q.team ?? null,
      q.season ?? null,
      q.day ?? null,
      q.names ?? null,
      q.filter ?? null,
    ],
    queryFn: () => getStats(q),
  });
}

export function useStatsTable(raw: RawTable) {
  return useMemo(() => {
    return aq.table(raw.data, raw.colNames);
  }, [raw]);
}

export const battingStatFields: StatKey[] = [
  "singles",
  "doubles",
  "triples",
  "home_runs",
  "at_bats",
  "walked",
  "hit_by_pitch",
  "plate_appearances",
  "stolen_bases",
  "caught_stealing",
  "struck_out",
  "runs",
  "runs_batted_in",
  "sac_flies",
];

export const pitchingStatFields: StatKey[] = [
  "outs",
  "earned_runs",
  "home_runs_allowed",
  "walks",
  "hit_batters",
  "strikeouts",
  "hits_allowed",
  "wins",
  "losses",
  "saves",
  "blown_saves",
  "unearned_runs",
  "appearances",
  "starts",
];

export function calculateBattingStats(
  data: aq.ColumnTable,
  ref?: BattingStatReferences
): aq.ColumnTable {
  let d = data
    .derive({
      hits: (d) => d.singles + d.doubles + d.triples + d.home_runs,
      total_bases_hit: (d) =>
        d.singles + d.doubles * 2 + d.triples * 3 + d.home_runs * 4,
    })
    .derive({
      pa: (d) => d.at_bats + d.walked + d.hit_by_pitch + d.sac_flies,
      ba: (d) => d.hits / d.at_bats,
      obp: (d) =>
        (d.hits + d.walked + d.hit_by_pitch) /
        (d.at_bats + d.walked + d.hit_by_pitch + d.sac_flies),
      slg: (d) =>
        (d.singles + 2 * d.doubles + 3 * d.triples + 4 * d.home_runs) /
        d.at_bats,
      sb_success: (d) => d.stolen_bases / (d.stolen_bases + d.caught_stealing),
    })
    .derive({
      ops: (d) => d.obp + d.slg,
      k_pct: (d) => d.struck_out / d.pa,
      bb_pct: (d) => d.walked / d.pa,
    });

  if (ref) {
    d = d.params(ref).derive({
      ops_plus: (d, $) => 100 * (d.obp / $.meanObp + d.slg / $.meanSlg - 1),
    });
  }
  return d;
}

export function calculatePitchingStats(
  data: aq.ColumnTable,
  ref?: PitchingStatReferences
): aq.ColumnTable {
  let d = data
    .derive({
      ip: (d) => d.outs / 3,
    })
    .derive({
      total_runs: (d) => d.earned_runs + d.unearned_runs,
      era: (d) => (9 * d.earned_runs) / d.ip,
      fip_base: (d) =>
        (13 * d.home_runs_allowed +
          3 * (d.walks + d.hit_batters) -
          2 * d.strikeouts) /
        d.ip,
      whip: (d) => (d.walks + d.hits_allowed) / d.ip,
      hr9: (d) => (9 * d.home_runs_allowed) / d.ip,
      h9: (d) => (9 * d.hits_allowed) / d.ip,
      bb9: (d) => (9 * d.walks) / d.ip,
      k9: (d) => (9 * d.strikeouts) / d.ip,
      k_bb: (d) => d.strikeouts / d.walks,
    });
  if (ref) {
    d = d
      .params(ref)
      .derive({
        fip: (d, $) => d.fip_base + ($.meanEra - $.meanFipBase),
      })
      .derive({
        era_minus: (d, $) => (100 * d.era) / $.meanEra,
        fip_minus: (d, $) => (100 * d.fip) / $.meanEra,
      });
  }
  return d;
}

export interface BattingStatReferences {
  meanObp: number;
  meanSlg: number;
}

export interface PitchingStatReferences {
  meanFipBase: number;
  meanEra: number;
}

export function calculateBattingStatReferences(
  batting: aq.ColumnTable
): BattingStatReferences {
  return calculateBattingStats(batting)
    .rollup({
      meanObp: aq.op.mean("obp"),
      meanSlg: aq.op.mean("slg"),
    })
    .object(0) as BattingStatReferences;
}

export function calculatePitchingStatReferences(
  pitching: aq.ColumnTable
): PitchingStatReferences {
  return calculatePitchingStats(pitching)
    .rollup({
      meanEra: aq.op.mean("era"),
      meanFipBase: aq.op.mean("fip_base"),
    })
    .object(0) as PitchingStatReferences;
}

export interface PercentileIndex {
  ids: string[];
  values: number[];
}

export function generatePercentileIndexes(
  data: aq.ColumnTable,
  cols: string[]
): Record<string, PercentileIndex> {
  const indexes: Record<string, PercentileIndex> = {};
  for (let col of cols) {
    if (!data.column(col)) continue;

    const tbl = data
      .select("player_id", col)
      .params({ col })
      .filter((d, $) => !aq.op.is_nan(d[$.col]))
      .orderby(col);
    indexes[col] = {
      ids: tbl.array("player_id") as string[],
      values: tbl.array(col) as number[],
    };
  }
  return indexes;
}

export interface PercentileResult {
  id: string | null;
  rank: number;
  total: number;
  percentile: number;
}
export function findPercentile(
  index: PercentileIndex,
  value: number,
  desc: boolean = true
): PercentileResult {
  // todo: binary search, asc (this is currently correct for desc)
  const len = index.values.length;

  if (desc) {
    for (let i = len - 1; i >= 0; i--) {
      if (index.values[i] <= value) {
        return {
          id: index.ids[i],
          rank: len - i - 1,
          total: len,
          percentile: i / index.values.length,
        };
      }
    }

    return {
      id: null,
      rank: len,
      total: len,
      percentile: 0,
    };
  } else {
    for (let i = 0; i < len; i++) {
      if (index.values[i] >= value) {
        return {
          id: index.ids[i],
          rank: i,
          total: len,
          percentile: 1 - i / index.values.length,
        };
      }
    }

    return {
      id: null,
      rank: len,
      total: len,
      percentile: 0,
    };
  }
}
