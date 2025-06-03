import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/team/$id/")({
  component: RouteComponent,
});

function RouteComponent() {
  return <div>Hello "/team/$id/"! Index</div>;
}
