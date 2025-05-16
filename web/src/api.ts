// import { useQuery, UseQueryResult } from "@tanstack/react-query";

import { cache } from "react";

// export function useTeams(): UseQueryResult<any> {
//     return useQuery({
//         queryKey: ["teams"],
//         queryFn: async () => {
//             return fetch("/api/allteams").then(r => r.json())
//         },
//         initialData: []
//     })
// }

export interface Team {}

const API_BASE = "https://freecashe.ws";

export const getTeams = cache(
  () =>
    fetch(API_BASE + "/api/allteams", {
      cache: "force-cache",
    }).then((x) => x.json()) as Promise<Record<string, Team>>,
);

async function getTeam(id: string): Promise<Team> {
  const teams = await getTeams();
  return teams[id];
}
