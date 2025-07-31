import { allTeamsQuery } from "@/lib/data";
import {
  battingStatFields,
  calculateBattingStats,
  calculatePitchingStats,
  pitchingStatFields,
  statsQuery,
} from "@/lib/newstats";
import { QueryClient, useSuspenseQueries } from "@tanstack/react-query";
import * as aq from "arquero";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "./ui/tooltip";

export interface LeadersPageProps {
  season: number;
  league?: string;
}

interface LeadersTableProps {
  title: string;
  data: aq.ColumnTable;
  col: string;
  type: "batting" | "pitching";
  format: (number) => string;
}

function formatDecimal(places: number) {
  return (x) => x.toFixed(places);
}

function formatIp(x: number) {
  // when an inning begins, it begins with ins...
  const innings = Math.floor(x);
  const outings = Math.floor(x * 3) % 3;

  return `${innings}.${outings}`;
}

function LeadersTable(props: LeadersTableProps) {
  const data = props.data.derive({ rank: aq.op.rank() }).objects() as {
    team_id: string;
    player_id: string;
    player_name: string;
    team_emoji: string;
    team_name: string;
    plate_appearances?: number;
    outs?: number;
    rank: number;
  }[];
  return (
    <div className="rounded-md border">
      <table className="text-sm table-fixed w-full max-w-full">
        <colgroup>
          <col className="w-8" />
          <col />
          <col className="w-14" />
        </colgroup>

        <TableHeader>
          <TableRow>
            <TableHead colSpan={3}>{props.title}</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((row, i) => {
            return (
              <Tooltip key={i}>
                <TooltipTrigger asChild>
                  <TableRow>
                    <TableCell className="tabular-nums text-right p-0">
                      {row.rank}.
                    </TableCell>
                    <TableCell className="truncate">
                      <a href={`https://mmolb.com/player/${row.player_id}`}>
                        {row.player_name}
                      </a>
                      <span className="text-gray-500 dark:text-gray-400">
                        {" "}
                        - {row.team_emoji}&nbsp;&nbsp;{row.team_name}
                      </span>
                    </TableCell>

                    <TableCell className="tabular-nums text-right">
                      {props.format(row[props.col] ?? 0)}
                    </TableCell>
                  </TableRow>
                </TooltipTrigger>
                <TooltipContent>
                  {row.team_emoji} {row.team_name}{" "}
                  {props.type === "batting" ? (
                    <span>({row.plate_appearances} PAs)</span>
                  ) : (
                    <span>({formatIp((row.outs ?? 0) / 3)} IP)</span>
                  )}
                </TooltipContent>
              </Tooltip>
            );
          })}
        </TableBody>
      </table>
    </div>
  );
}

function statsQueryBatting(props: LeadersPageProps) {
  return statsQuery({
    season: props.season,
    league: props.league,
    fields: battingStatFields,
    group: ["player", "team", "player_name"],
    names: true,
  });
}

function statsQueryPitching(props: LeadersPageProps) {
  return statsQuery({
    season: props.season,
    league: props.league,
    fields: pitchingStatFields,
    group: ["player", "team", "player_name"],
    names: true,
  });
}

export async function preloadData(
  client: QueryClient,
  props: LeadersPageProps
) {
  await Promise.all([client.prefetchQuery(statsQueryBatting(props))]);
}

export default function LeadersPage(props: LeadersPageProps) {
  const [battingStats, pitchingStats, teams] = useSuspenseQueries({
    queries: [
      statsQueryBatting(props),
      statsQueryPitching(props),
      allTeamsQuery,
    ],
  });

  const battingDt = calculateBattingStats(battingStats.data, undefined);
  const pitchingDt = calculatePitchingStats(pitchingStats.data, undefined);

  const maxPas = aq.agg(battingDt, aq.op.max("plate_appearances"));
  const maxOuts = aq.agg(pitchingDt, aq.op.max("outs"));
  const paLimit = Math.round(Math.min(maxPas / 2, 100));
  const outLimit = Math.round(Math.min(maxOuts / 2, 100));

  const validBatters = battingDt
    .derive({
      team_emoji: aq.escape((d) => teams.data[d.team_id].emoji),
      team_color: aq.escape((d) => teams.data[d.team_id].color),
    })
    .params({ limit: paLimit })
    .filter((d, $) => d.plate_appearances > $.limit);

  const validPitchers = pitchingDt
    .derive({
      team_emoji: aq.escape((d) => teams.data[d.team_id].emoji),
      team_color: aq.escape((d) => teams.data[d.team_id].color),
    })
    .params({ limit: outLimit })
    .filter((d, $) => d.outs > $.limit);

  return (
    <div>
      <div className="container mx-auto py-4">
        <h1 className="text-xl font-semibold">Batting Leaders</h1>
        <h2 className="text-sm">
          (current threshold:{" "}
          <span className="font-semibold">{paLimit} PAs</span>)
        </h2>

        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4 container mx-auto py-2">
          <LeadersTable
            title="Batting Average (BA)"
            data={validBatters.orderby(aq.desc("ba")).slice(0, 10)}
            col="ba"
            format={formatDecimal(3)}
            type="batting"
          />
          <LeadersTable
            title="On-Base Percentage (OBP)"
            data={validBatters.orderby(aq.desc("obp")).slice(0, 10)}
            col="obp"
            format={formatDecimal(3)}
            type="batting"
          />
          <LeadersTable
            title="Slugging Percentage (SLG)"
            data={validBatters.orderby(aq.desc("slg")).slice(0, 10)}
            col="slg"
            format={formatDecimal(3)}
            type="batting"
          />
          <LeadersTable
            title="On-Base + Slugging (OPS)"
            data={validBatters.orderby(aq.desc("ops")).slice(0, 10)}
            col="ops"
            format={formatDecimal(3)}
            type="batting"
          />

          <LeadersTable
            title="Walks"
            data={validBatters.orderby(aq.desc("walked")).slice(0, 10)}
            col="walked"
            format={formatDecimal(0)}
            type="batting"
          />

          <LeadersTable
            title="Strikeouts"
            data={validBatters.orderby(aq.desc("struck_out")).slice(0, 10)}
            col="struck_out"
            format={formatDecimal(0)}
            type="batting"
          />

          <LeadersTable
            title="Singles"
            data={validBatters.orderby(aq.desc("singles")).slice(0, 10)}
            col="singles"
            format={formatDecimal(0)}
            type="batting"
          />

          <LeadersTable
            title="Doubles"
            data={validBatters.orderby(aq.desc("doubles")).slice(0, 10)}
            col="doubles"
            format={formatDecimal(0)}
            type="batting"
          />

          <LeadersTable
            title="Triples"
            data={validBatters.orderby(aq.desc("triples")).slice(0, 10)}
            col="triples"
            format={formatDecimal(0)}
            type="batting"
          />

          <LeadersTable
            title="Home Runs"
            data={validBatters.orderby(aq.desc("home_runs")).slice(0, 10)}
            col="home_runs"
            format={formatDecimal(0)}
            type="batting"
          />

          <LeadersTable
            title="Stolen Bases"
            data={validBatters.orderby(aq.desc("stolen_bases")).slice(0, 10)}
            col="stolen_bases"
            format={formatDecimal(0)}
            type="batting"
          />

          <LeadersTable
            title="Caught Stealing"
            data={validBatters.orderby(aq.desc("caught_stealing")).slice(0, 10)}
            col="caught_stealing"
            format={formatDecimal(0)}
            type="batting"
          />

          <LeadersTable
            title="Hit By Pitch"
            data={validBatters.orderby(aq.desc("hit_by_pitch")).slice(0, 10)}
            col="hit_by_pitch"
            format={formatDecimal(0)}
            type="batting"
          />
        </div>
      </div>
      <div>
        <h1 className="text-xl font-semibold">Pitching Leaders</h1>
        <h2 className="text-sm">
          (current threshold:{" "}
          <span className="font-semibold">{formatIp(outLimit / 3)} IP</span>)
        </h2>

        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4 container mx-auto py-2">
          <LeadersTable
            title="Earned Runs Average (ERA)"
            data={validPitchers.orderby("era").slice(0, 10)}
            col="era"
            format={formatDecimal(2)}
            type="pitching"
          />

          <LeadersTable
            title="Walks and Hits per Innings Pitched (WHIP)"
            data={validPitchers.orderby("whip").slice(0, 10)}
            col="whip"
            format={formatDecimal(2)}
            type="pitching"
          />

          <LeadersTable
            title="Strikeouts"
            data={validPitchers.orderby(aq.desc("strikeouts")).slice(0, 10)}
            col="strikeouts"
            format={formatDecimal(0)}
            type="pitching"
          />

          <LeadersTable
            title="Hit Batters"
            data={validPitchers.orderby(aq.desc("hit_batters")).slice(0, 10)}
            col="hit_batters"
            format={formatDecimal(0)}
            type="pitching"
          />
        </div>
      </div>
    </div>
  );
}
