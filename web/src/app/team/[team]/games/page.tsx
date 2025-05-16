import TeamGamesTable from "@/components/TeamGamesTable";
import {
  ChronGame,
  getEntities,
  getEntity,
  getGames,
  MmolbLeague,
  MmolbTeam,
} from "@/data/data";

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

export default async function TeamGamesPage(props: TeamPageProps) {
  const params = await props.params;
  const teamId = params.team;

  // const team = await getEntity<MmolbTeam>("team", params.team);
  // const league = await getEntity<MmolbLeague>("league", team.data.League);
  const games = await getGames({ season: 0, team: params.team });

  const teamIds = getRelevantTeamIds(games.items);
  const teams = await getEntities<MmolbTeam>("team", teamIds);

  return <TeamGamesTable games={games.items} teams={teams} />;
}
