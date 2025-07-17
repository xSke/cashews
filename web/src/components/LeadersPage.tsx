import { chronLatestEntitiesQuery, MmolbTeam } from "@/lib/data";
import { statsQuery } from "@/lib/newstats";
import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import { ColumnTable, desc, op, table } from "arquero";
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
  data: ColumnTable;
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
  const data = props.data.objects() as {
    team_id: string;
    player_id: string;
    player_name: string;
    team_emoji: string;
    team_name: string;
  }[];
  return (
    <div className="rounded-md border">
      <Table className="table-auto">
        <TableHeader>
          <TableRow>
            <TableHead colSpan={2}>{props.title}</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((row, i) => {
            return (
              <TableRow key={i}>
                <TableCell>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <a href={`https://mmolb.com/player/${row.player_id}`}>
                        {row.team_emoji} {row.player_name}
                      </a>
                    </TooltipTrigger>
                    <TooltipContent>
                      {row.team_emoji} {row.team_name}
                    </TooltipContent>
                  </Tooltip>
                </TableCell>

                <TableCell className="tabular-nums text-right">
                  {props.format(row[props.col] ?? 0)}
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
}

export default function LeadersPage(props: LeadersPageProps) {
  const stats = useSuspenseQuery(
    statsQuery({
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
      group: ["player", "team"],
      names: true,
    })
  );

  let dt = stats.data;
  const uniqueTeamIds = dt
    .select("team_id")
    .dedupe()
    .array("team_id") as string[];

  const teams = useSuspenseQuery(
    chronLatestEntitiesQuery<MmolbTeam>("team_lite", uniqueTeamIds)
  );
  const teamsDt = table({
    team_id: uniqueTeamIds,
    // we get name from the lookup
    // team_name: uniqueTeamIds.map((id) => teams.data[id].Name),
    // team_location: uniqueTeamIds.map((id) => teams.data[id].Location),
    team_emoji: uniqueTeamIds.map((id) => teams.data[id].Emoji),
    team_color: uniqueTeamIds.map((id) => teams.data[id].Color),
  });
  teamsDt.print();
  dt = dt
    .join(teamsDt, "team_id")
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

  const validBatters = dt.filter((d) => d.plate_appearances > 50);

  return (
    <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4 container mx-auto py-4 px-4">
      <LeadersTable
        title="Batting Average (BA)"
        data={validBatters.orderby(desc("ba")).slice(0, 10)}
        col="ba"
        format={formatDecimal(3)}
      />
      <LeadersTable
        title="On-Base Percentage (OBP)"
        data={validBatters.orderby(desc("obp")).slice(0, 10)}
        col="obp"
        format={formatDecimal(3)}
      />
      <LeadersTable
        title="Slugging Percentage (SLG)"
        data={validBatters.orderby(desc("slg")).slice(0, 10)}
        col="slg"
        format={formatDecimal(3)}
      />
      <LeadersTable
        title="On-Base + Slugging (OBP)"
        data={validBatters.orderby(desc("ops")).slice(0, 10)}
        col="ops"
        format={formatDecimal(3)}
      />
    </div>
  );
}
