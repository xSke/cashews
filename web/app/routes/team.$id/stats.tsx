import ColorPreview from "@/components/ColorPreview";
import StatsTable from "@/components/StatsTable";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { darkScale, lightScale } from "@/lib/colors";
import {
  getEntities,
  getLeagueAggregates,
  getTeamStats,
  MmolbPlayer,
  MmolbTeam,
} from "@/lib/data";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useTheme } from "next-themes";
import { z } from "zod";

const defaultSeason = 1;
const stateSchema = z.object({
  season: z.number().catch(1).optional(),
});

type StateParams = z.infer<typeof stateSchema>;

export const Route = createFileRoute("/team/$id/stats")({
  component: RouteComponent,
  validateSearch: (search) => stateSchema.parse(search),
  loaderDeps: ({ search: { season } }) => ({ season }),
  loader: async ({ params, deps }) => {
    const stats = await getTeamStats(params.id, deps.season ?? defaultSeason);

    const playerIds = [...new Set(stats.map((x) => x.player_id))];
    const players = await getEntities<MmolbPlayer>("player_lite", playerIds);

    const teamIds = [...new Set(stats.map((x) => x.team_id))];
    const teams = await getEntities<MmolbTeam>("team", teamIds);

    const thisTeam = teams[params.id]!;
    const aggs = await getLeagueAggregates();

    return { stats, players, teams, aggs };
  },
});

function RouteComponent() {
  const { stats, players, teams, aggs } = Route.useLoaderData();
  const { season } = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const theme = useTheme();

  const seasons = [1, 0];

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-row place-content-end">
        <Select
          value={(season ?? defaultSeason).toString()}
          onValueChange={(val) => {
            navigate({
              search: (prev) => ({ ...prev, season: parseInt(val) ?? 0 }),
            });
          }}
        >
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="Season..."></SelectValue>
          </SelectTrigger>
          <SelectContent>
            {seasons.map((s) => (
              <SelectItem value={s.toString()}>Season {s}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="">
        <ColorPreview scale={theme.theme === "dark" ? darkScale : lightScale} />
      </div>

      <div>
        <h2 className="mb-2 font-medium text-lg">Batting</h2>
        <StatsTable
          data={stats}
          players={players}
          teams={teams}
          aggs={aggs}
          type="batting"
        />
      </div>

      <div>
        <h2 className="mb-2 font-medium text-lg">Pitching</h2>
        <StatsTable
          data={stats}
          players={players}
          teams={teams}
          aggs={aggs}
          type="pitching"
        />
      </div>
    </div>
  );
}
