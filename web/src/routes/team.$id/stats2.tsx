import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/team/$id/stats2")({
  loader: () => {
    throw redirect({ to: "/team/$id/stats" });
  },
});
