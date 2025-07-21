import { allTeamsQuery } from "@/lib/data";
import { statsQuery } from "@/lib/newstats";
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
  format: (number) => string;
}

function formatDecimal(places: number) {
  return (x) => x.toFixed(3);
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
    plate_appearances: number;
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
            <TableHead colSpan={2}>{props.title}</TableHead>
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
                  {row.team_emoji} {row.team_name} ({row.plate_appearances} PAs)
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
    fields: [
      "walked",
      "singles",
      "doubles",
      "triples",
      "home_runs",
      "at_bats",
      "hit_by_pitch",
      "plate_appearances",
    ],
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
  const [stats, teams] = useSuspenseQueries({
    queries: [statsQueryBatting(props), allTeamsQuery],
  });

  const dt = stats.data
    .derive({
      team_emoji: aq.escape((d) => teams.data[d.team_id].emoji),
      team_color: aq.escape((d) => teams.data[d.team_id].color),
    })
    .derive({
      hits: (d) => d.singles + d.doubles + d.triples + d.home_runs,
    })
    .derive({
      ba: (d) => d.hits / d.at_bats,
      obp: (d) => (d.hits + d.walked + d.hit_by_pitch) / d.plate_appearances,
      slg: (d) =>
        (d.singles + 2 * d.doubles + 3 * d.triples + 4 * d.home_runs) /
        d.at_bats,
    })
    .derive({
      ops: (d) => d.obp + d.slg,
    });

  const maxPas = aq.agg(dt, aq.op.max("plate_appearances"));
  const limit = Math.max(maxPas / 2, 100);

  const validBatters = dt
    .params({ limit })
    .filter((d, $) => d.plate_appearances > $.limit);

  return (
    <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4 container mx-auto py-4 px-4">
      <LeadersTable
        title="Batting Average (BA)"
        data={validBatters.orderby(aq.desc("ba")).slice(0, 10)}
        col="ba"
        format={formatDecimal(3)}
      />
      <LeadersTable
        title="On-Base Percentage (OBP)"
        data={validBatters.orderby(aq.desc("obp")).slice(0, 10)}
        col="obp"
        format={formatDecimal(3)}
      />
      <LeadersTable
        title="Slugging Percentage (SLG)"
        data={validBatters.orderby(aq.desc("slg")).slice(0, 10)}
        col="slg"
        format={formatDecimal(3)}
      />
      <LeadersTable
        title="On-Base + Slugging (OPS)"
        data={validBatters.orderby(aq.desc("ops")).slice(0, 10)}
        col="ops"
        format={formatDecimal(3)}
      />
    </div>
  );
}
