import StatsTable from "@/components/StatsTable";
import {
  getEntities,
  getLeagueAggregates,
  getTeamStats,
  MmolbPlayer,
  MmolbTeam,
} from "@/lib/data/data";
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
    const filteredAggs = aggs
      .filter(
        (x) =>
          x.season === 0 &&
          x.league_id === thisTeam.League &&
          [0.05, 0.2, 0.35, 0.5, 0.65, 0.8, 0.95].includes(x.percentile)
      )
      .sort((a, b) => a.percentile - b.percentile);

    return { stats, players, teams, filteredAggs };
  },
});

function RouteComponent() {
  const { stats, players, teams, filteredAggs } = Route.useLoaderData();
  return (
    <div>
      <h2 className="mb-2">Batting</h2>
      <StatsTable
        data={stats}
        players={players}
        teams={teams}
        aggs={filteredAggs}
        type="batting"
      />

      <h2 className="mb-2 mt-4">Pitching</h2>
      <StatsTable
        data={stats}
        players={players}
        teams={teams}
        aggs={filteredAggs}
        type="pitching"
      />
    </div>
  );

  const posts = Route.useLoaderData();
  return <div>Hello "/team/$id/stats"!</div>;
}
