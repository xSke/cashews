import NewStatsTable from "@/components/NewStatsTable";
import {
  battingStatFields,
  calculateBattingStats,
  calculatePitchingStats,
  pitchingStatFields,
  statsQuery,
} from "@/lib/newstats";
import { useQuery } from "@tanstack/react-query";
import { createFileRoute } from "@tanstack/react-router";
import { useMemo } from "react";

export const Route = createFileRoute("/player/$id/stats")({
  component: RouteComponent,
  ssr: false,
});

function RouteComponent() {
  const { id: playerId } = Route.useParams();
  const battingStatsQuery = useQuery(
    statsQuery({
      player: playerId,
      group: ["player", "player_name", "team", "season"],
      fields: battingStatFields,
    })
  );

  const pitchingStatsQuery = useQuery(
    statsQuery({
      player: playerId,
      group: ["player", "player_name", "team", "season"],
      fields: pitchingStatFields,
    })
  );

  const battingStats = useMemo(() => {
    if (!battingStatsQuery.data) return undefined;
    return calculateBattingStats(battingStatsQuery.data, undefined);
  }, [battingStatsQuery.data]);

  const pitchingStats = useMemo(() => {
    if (!pitchingStatsQuery.data) return undefined;
    return calculatePitchingStats(pitchingStatsQuery.data, undefined);
  }, [pitchingStatsQuery.data]);

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-2">
        <h1 className="text-lg font-semibold">Batting</h1>
        {battingStats ? (
          <NewStatsTable
            data={battingStats}
            position={"batting"}
            display="player"
            indexes={{}}
          ></NewStatsTable>
        ) : (
          <span>loading</span>
        )}
      </div>

      <div className="flex flex-col gap-2">
        <h1 className="text-lg font-semibold">Pitching</h1>
        {pitchingStats ? (
          <NewStatsTable
            data={pitchingStats}
            position={"pitching"}
            display="player"
            indexes={{}}
          ></NewStatsTable>
        ) : (
          <span>loading</span>
        )}
      </div>
    </div>
  );
}
