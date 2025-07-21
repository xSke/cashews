import LeadersPage, { preloadData } from "@/components/LeadersPage";
import { allTeamsQuery } from "@/lib/data";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/leaders")({
  component: RouteComponent,
  loader: async ({ context }) => {
    await Promise.all([
      preloadData(context.queryClient, { season: 3 }),
      context.queryClient.prefetchQuery(allTeamsQuery),
    ]);
  },
  ssr: true,
});

function RouteComponent() {
  return <LeadersPage season={3} />;
}
