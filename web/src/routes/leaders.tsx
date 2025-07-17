import LeadersPage from "@/components/LeadersPage";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/leaders")({
  component: RouteComponent,
  //   loader: LoaderComponent,
});

function RouteComponent() {
  return <LeadersPage season={3} />;
}
