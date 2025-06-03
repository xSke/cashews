import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/map")({
  component: RouteComponent,
});

function RouteComponent() {
  return <div className="my-4 text-center">(under construction...)</div>;
}
