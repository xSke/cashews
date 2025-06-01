import { SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/app-sidebar";
import { getEntity, MmolbTeam } from "@/data/data";

interface TeamPageProps {
  children: React.ReactNode;
  params: Promise<{ team: string }>;
}

export default async function Layout(props: TeamPageProps) {
  const { team: teamId } = await props.params;
  const team = await getEntity<MmolbTeam>("team", teamId);

  return (
    <div className="flex flex-row h-full">
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

      <main className="flex-3 ml-2 p-2">{props.children}</main>
    </div>
  );
}
