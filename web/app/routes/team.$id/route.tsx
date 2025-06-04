import { getEntity, MmolbTeam } from "@/lib/data";

import { createFileRoute, Outlet } from "@tanstack/react-router";

export const Route = createFileRoute("/team/$id")({
  component: RouteComponent,
  loader: async ({ params }) => {
    const team = await getEntity<MmolbTeam>("team", params.id);
    return { team };
  },
});

function RouteComponent() {
  const { team } = Route.useLoaderData();

  return (
    <div className="container mx-auto flex flex-row h-full">
      <aside className="flex-1 flex flex-col mr-2 border-r p-2">
        <div className="mb-4 flex flex-row items-center gap-2">
          <div className="text-2xl">{team.data.Emoji}</div>
          <div className="flex flex-col">
            <div>{team.data.Location}</div>
            <div className="text-lg leading-4 font-medium mb-2">
              {team.data.Name}
            </div>
          </div>
        </div>

        <div className="flex flex-col">
          <h4 className="font-medium text-sm py-1">Team</h4>
          <a className="py-1 px-2 bg-muted rounded">Info</a>
          <a className="py-1 px-2">Roster</a>
          <a className="py-1 px-2">Records</a>
          <a className="py-1 px-2">History</a>

          <h4 className="font-medium text-sm py-1 mt-4">Stats</h4>
          <a className="py-1 px-2">Seasonal</a>
          <a className="py-1 px-2">Average</a>
          <a className="py-1 px-2">Whatever</a>
          <a className="py-1 px-2">Something else</a>
          <a className="py-1 px-2">Please Imagine More Links</a>
          <div className="pl-4 flex-4"></div>
        </div>
      </aside>

      <main className="flex-3 p-2">
        <Outlet />
      </main>
    </div>
  );
}
