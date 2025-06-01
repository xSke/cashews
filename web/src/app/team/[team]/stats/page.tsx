import StatsTable from "@/components/StatsTable";
import TeamGamesTable from "@/components/TeamGamesTable";
import {
  ChronGame,
  getEntities,
  getEntity,
  getGames,
  getLeagueAggregates,
  getTeamStats,
  MmolbLeague,
  MmolbPlayer,
  MmolbTeam,
} from "@/data/data";
import { useMemo } from "react";

function generateStaticParams() {}

interface TeamPageProps {
  params: Promise<{ team: string }>;
}

function getRelevantTeamIds(games: ChronGame[]): string[] {
  const map: Record<string, number> = {};
  for (let game of games) {
    map[game.away_team_id] = 1;
    map[game.home_team_id] = 1;
  }
  return Object.keys(map);
}

export default async function TeamStatsPage(props: TeamPageProps) {
  const params = await props.params;
  const teamId = params.team;

  // const team = await getEntity<MmolbTeam>("team", params.team);
  // const league = await getEntity<MmolbLeague>("league", team.data.League);
  //   const games = await getGames({ season: 0, team: params.team });

  //   const teamIds = getRelevantTeamIds(games.items);
  //   const teams = await getEntities<MmolbTeam>("team", teamIds);
  const stats = await getTeamStats(teamId);

  const playerIds = [...new Set(stats.map((x) => x.player_id))];
  const players = await getEntities<MmolbPlayer>("player_lite", playerIds);

  const teamIds = [...new Set(stats.map((x) => x.team_id))];
  const teams = await getEntities<MmolbTeam>("team", teamIds);

  const thisTeam = teams[teamId]!;
  const aggs = await getLeagueAggregates();
  const filteredAggs = aggs
    .filter(
      (x) =>
        x.season === 0 &&
        x.league_id === thisTeam.League &&
        [0.05, 0.2, 0.35, 0.5, 0.65, 0.8, 0.95].includes(x.percentile)
    )
    .sort((a, b) => a.percentile - b.percentile);

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
}
