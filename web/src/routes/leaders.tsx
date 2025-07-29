import LeadersPage, { preloadData } from "@/components/LeadersPage";
import SeasonSelector from "@/components/SeasonSelector";
import { allTeamsQuery, timeQuery } from "@/lib/data";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { z } from "zod";

const stateSchema = z.object({
  season: z.number().optional(),
});

type StateParams = z.infer<typeof stateSchema>;

export const Route = createFileRoute("/leaders")({
  component: RouteComponent,
  validateSearch: (search) => stateSchema.parse(search),
  loaderDeps: ({ search: { season } }) => ({ season }),
  loader: async ({ context, deps }) => {
    const time = await context.queryClient.ensureQueryData(timeQuery);
    const currentSeason = time.data.season_number;
    const season = deps.season ?? currentSeason;

    await Promise.all([
      preloadData(context.queryClient, { season }),
      context.queryClient.prefetchQuery(allTeamsQuery),
    ]);
    return { season };
  },
  ssr: true,
});

function RouteComponent() {
  const { season } = Route.useLoaderData();
  const navigate = useNavigate({ from: Route.fullPath });

  return (
    <div className="container mx-auto px-4 md:px-0 py-4">
      <div className="flex flex-row">
        <div className="flex items-center gap-3 flex-1"></div>

        <div className="place-self-end">
          <SeasonSelector
            season={season}
            setSeason={(val) => {
              navigate({
                search: (prev) => ({ ...prev, season: val }),
              });
            }}
          />
        </div>
      </div>

      <LeadersPage season={season} />
    </div>
  );
  return;
}
