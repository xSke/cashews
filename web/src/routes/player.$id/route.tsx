import {
  createFileRoute,
  Link,
  Outlet,
  useMatchRoute,
} from "@tanstack/react-router";
import clsx from "clsx";

export const Route = createFileRoute("/player/$id")({
  component: RouteComponent,
});

function RouteComponent() {
  //   const { , league } = Route.useLoaderData();
  const matchRoute = useMatchRoute();

  const pages = [
    { name: "Info", url: "/player/$id" },
    { name: "Stats", url: "/player/$id/stats" },
    // { name: "Games", url: "/team/$id/games" },
  ];

  return (
    <div className="container mx-auto flex flex-col h-full py-4 gap-2">
      <div>
        <h1 className="text-xl font-semibold px-4 md:px-0">
          {/* {team.data.Emoji} {team.data.Location} {team.data.Name} */}
        </h1>
      </div>

      <nav className="text-sm font-medium text-center text-gray-700 border-b border-gray-200 dark:text-gray-400 dark:border-gray-700">
        <ul className="flex flex-wrap -mb-px">
          {pages.map((page) => {
            const isActive = !!matchRoute({ to: page.url });

            return (
              <li className="me-2" key={page.url}>
                <Link
                  to={page.url}
                  className={clsx(
                    "inline-block px-4 py-2 border-b-2 rounded-t-lg",
                    isActive
                      ? "text-blue-600 border-blue-600 dark:text-blue-400 dark:border-blue-500 active font-semibold"
                      : "border-transparent hover:text-gray-600 hover:border-gray-300 dark:hover:text-gray-300 font-medium"
                  )}
                  aria-current={isActive ? "page" : undefined}
                >
                  {page.name}
                </Link>
              </li>
            );
          })}
        </ul>
      </nav>
      <main className="flex-3 px-4 md:px-0">
        <Outlet />
      </main>
    </div>
  );
}
