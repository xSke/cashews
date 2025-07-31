import {
  chronLatestEntitiesQuery,
  chronLatestEntityQuery,
  MmolbPlayer,
  MmolbTeam,
} from "@/lib/data";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/team/$id/roster")({
  component: RouteComponent,
  loader: async ({ params, context }) => {
    const teamId = params.id;

    const team = await context.queryClient.ensureQueryData(
      chronLatestEntityQuery<MmolbTeam>("team", teamId)
    );
    const playerIds = team.data.Players.map((x) => x.PlayerID).filter(
      (x) => x != "#"
    );

    const players = await context.queryClient.ensureQueryData(
      chronLatestEntitiesQuery<MmolbPlayer>("player", playerIds)
    );

    return { team: team.data, players };
  },
});

function RouteComponent() {
  // const { team, players } =
  return <div>Hello "/team/$id/roster"!</div>;
}
