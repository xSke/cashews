import LeadersPage, { preloadData } from "@/components/LeadersPage";
import { allTeamsQuery, timeQuery } from "@/lib/data";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/leaders")({
  component: RouteComponent,
  loader: async ({ context }) => {
    const time = await context.queryClient.ensureQueryData(timeQuery);
    const currentSeason = time.data.season_number;

    await Promise.all([
      preloadData(context.queryClient, { season: currentSeason }),
      context.queryClient.prefetchQuery(allTeamsQuery),
    ]);
    return { season: currentSeason };
  },
  ssr: true,
});

function RouteComponent() {
  const { season } = Route.useLoaderData();
  return <LeadersPage season={season} />;
}
