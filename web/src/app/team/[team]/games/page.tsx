import TeamGamesTable from "@/components/TeamGamesTable";
import {
  ChronGame,
  getEntities,
  getEntity,
  getGames,
  MmolbLeague,
  MmolbTeam,
} from "@/data/data";

interface TeamPageProps {
  params: Promise<{ team: string }>;
}

export default async function TeamGamesPage(props: TeamPageProps) {
  const params = await props.params;
  const teamId = params.team;

  // const team = await getEntity<MmolbTeam>("team", params.team);
  // const league = await getEntity<MmolbLeague>("league", team.data.League);
  const games = await getGames({ season: 0, team: params.team });

  // const teamIds = getRelevantTeamIds(games.items);
  const teams = {};

  return <TeamGamesTable games={games.items} teams={teams} />;
}
