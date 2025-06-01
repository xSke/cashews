// import { useTeams } from "@/api.ts";
// import { useQuery, UseQueryResult } from "@tanstack/react-query";
import { getTeams } from "@/api";
// import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import Image from "next/image";

// function useTeams(): UseQueryResult<any> {
//   return useQuery({
//       queryKey: ["teams"],
//       queryFn: async () => {
//           return fetch("/api/allteams").then(r => r.json())
//       },
//       initialData: []
//   })
// }

export default async function Home() {
  // const teams = await fetch('https://freecashe.ws/api/allteams').then(x => x.json());
  // const teams = useTeams();
  const teams = await getTeams();

  return <div>please imagine front page</div>;
}
