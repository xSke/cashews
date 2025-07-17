import LeadersPage from "@/components/LeadersPage";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/league/$id")({
  component: RouteComponent,
});

function RouteComponent() {
  const { id: leagueId } = Route.useParams();

  return <LeadersPage league={leagueId} season={2} />;
}
