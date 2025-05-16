import TeamChart from "@/components/TeamChart";
import { ChartConfig, ChartContainer } from "@/components/ui/chart";
import {
  ChronGame,
  getEntity,
  getGames,
  MmolbLeague,
  MmolbTeam,
} from "@/data/data";
import { LineChart } from "recharts";

function generateStaticParams() {}

interface TeamPageProps {
  params: Promise<{ team: string }>;
}

function didTeamWinGame(teamId: string, game: ChronGame): boolean {
  if (
    game.away_team_id == teamId &&
    (game.last_update?.away_score ?? 0) > (game.last_update?.home_score ?? 0)
  ) {
    return true;
  }

  if (
    game.home_team_id == teamId &&
    (game.last_update?.home_score ?? 0) > (game.last_update?.away_score ?? 0)
  ) {
    return true;
  }

  return false;
}

export default async function TeamPage(props: TeamPageProps) {
  const params = await props.params;
  const teamId = params.team;

  const team = await getEntity<MmolbTeam>("team", params.team);
  const league = await getEntity<MmolbLeague>("league", team.data.League);
  const games = await getGames({ season: 0, team: params.team });

  const gamesByDay: Record<string, ChronGame> = {};
  let maxDay = 0;
  for (let game of games.items) {
    gamesByDay[game.day.toString()] = game;
    if (game.day > maxDay) maxDay = game.day;
  }

  const recordData = [];
  let wins = 0;
  for (let i = 0; i < maxDay + 1; i++) {
    const game = gamesByDay[i.toString()];
    if (game && game.state === "Complete") {
      if (didTeamWinGame(teamId, game)) {
        wins += 1;
      } else {
        wins -= 1;
      }
    }

    recordData.push({
      day: i,
      wins,
    });
  }

  return (
    <main className="flex flex-col">
      <h1 className="mb-4">
        {team.data.Emoji} {team.data.Location} {team.data.Name}
      </h1>

      <div className="flex flex-row">
        <aside className="pr-4 flex flex-col flex-1">
          <h4 className="font-medium text-sm py-1">Team</h4>
          <a className="py-1 px-2 bg-primary-foreground rounded">Info</a>
          <a className="py-1 px-2 ">Roster</a>
          <a className="py-1 px-2 ">Records</a>
          <a className="py-1 px-2 ">History</a>

          <h4 className="font-medium text-sm py-1 mt-4">Stats</h4>
          <a className="py-1 px-2 ">Seasonal</a>
          <a className="py-1 px-2 ">Average</a>
          <a className="py-1 px-2 ">Whatever</a>
          <a className="py-1 px-2 ">Something else</a>
        </aside>
        <div className="pl-4 flex-4">
          <h2>{league.data.Name} League</h2>
          <TeamChart data={recordData} />
        </div>
      </div>
    </main>
  );
}
