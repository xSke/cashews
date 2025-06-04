import StatsTable from "@/components/StatsTable";
import {
  getEntities,
  getLeagueAggregates,
  getTeamStats,
  MmolbPlayer,
  MmolbTeam,
} from "@/lib/data";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/team/$id/stats")({
  component: RouteComponent,
  loader: async ({ params }) => {
    const stats = await getTeamStats(params.id);

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
  return (
    <div>
      <h2 className="mb-2">Batting</h2>
      <StatsTable
        data={stats}
        players={players}
        teams={teams}
        aggs={aggs}
        type="batting"
      />

      <h2 className="mb-2 mt-4">Pitching</h2>
      <StatsTable
        data={stats}
        players={players}
        teams={teams}
        aggs={aggs}
        type="pitching"
      />
    </div>
  );
}
