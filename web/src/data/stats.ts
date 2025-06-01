import { PlayerStatsEntry } from "./data";

export interface AdvancedStats {
  at_bats: number;
  plate_appearances: number;
  hits: number;
  ba: number;
  obp: number;
  slg: number;
  ops: number;

  ip: number;
  era: number;
  whip: number;
  h9: number;
  bb9: number;
  k9: number;
  hr9: number;
}

export function calculateAdvancedStats(data: PlayerStatsEntry): AdvancedStats {
  const singles = data.stats.singles ?? 0;
  const doubles = data.stats.doubles ?? 0;
  const triples = data.stats.triples ?? 0;
  const home_runs = data.stats.home_runs ?? 0;
  const earned_runs = data.stats.earned_runs ?? 0;
  const outs = data.stats.outs ?? 0;
  const walks = data.stats.walks ?? 0;
  const hits_allowed = data.stats.hits_allowed ?? 0;
  const strikeouts = data.stats.strikeouts ?? 0;
  const home_runs_allowed = data.stats.home_runs_allowed ?? 0;
  const walked = data.stats.walked ?? 0;
  const hit_by_pitch = data.stats.hit_by_pitch ?? 0;

  const ip = outs / 3;

  const abs = data.stats.at_bats ?? 0;
  const pas = data.stats.plate_appearances ?? 0;

  const hits = singles + doubles + triples + home_runs;

  const ba = hits / abs;
  const obp = (hits + walked + hit_by_pitch) / pas;
  const slg = (singles + doubles * 2 + triples * 3 + home_runs * 4) / abs;
  const ops = obp + slg;

  const era = (9 * earned_runs) / ip;
  const whip = (walks + hits_allowed) / ip;
  const hr9 = (9 * home_runs_allowed) / ip;
  const bb9 = (9 * walks) / ip;
  const k9 = (9 * strikeouts) / ip;
  const h9 = (9 * hits_allowed) / ip;

  return {
    at_bats: abs,
    plate_appearances: pas,
    hits,
    ba,
    obp,
    slg,
    ops,

    ip,
    era,
    whip,
    h9,
    bb9,
    k9,
    hr9,
  };
}
