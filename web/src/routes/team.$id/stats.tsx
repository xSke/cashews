import NewStatsTable from "@/components/NewStatsTable";
import {
  chronLatestEntityQuery,
  gamesQuery,
  MmolbGame,
  MmolbTeam,
  MmolbTime,
  timeQuery,
} from "@/lib/data";
import {
  battingStatFields,
  calculateBattingStatReferences,
  calculateBattingStats,
  calculatePitchingStatReferences,
  calculatePitchingStats,
  generatePercentileIndexes,
  pitchingStatFields,
  StatKey,
  statsQuery,
} from "@/lib/newstats";
import { useQuery } from "@tanstack/react-query";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { z } from "zod";
import * as aq from "arquero";
import { useMemo, useState } from "react";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { createSeasonList } from "@/lib/utils";
import ColorPreview from "@/components/ColorPreview";
import { defaultScale } from "@/lib/colors";
import SeasonSelector from "@/components/SeasonSelector";

const stateSchema = z.object({
  season: z.number().optional(),
});

type StateParams = z.infer<typeof stateSchema>;

export const Route = createFileRoute("/team/$id/stats")({
  component: RouteComponent,
  validateSearch: (search) => stateSchema.parse(search),
  loaderDeps: ({ search: { season } }) => ({ season }),
  loader: async ({ context, params, deps }) => {
    const time = await context.queryClient.ensureQueryData(timeQuery);
    const currentSeason = time.data.season_number;
    const season = deps.season ?? currentSeason;
    return { currentSeason, season };
  },
  ssr: false,
});

function filterEligibleBatters(data: aq.ColumnTable): aq.ColumnTable {
  let { maxPas } = data
    .rollup({
      maxPas: aq.op.max("plate_appearances"),
    })
    .object(0) as { maxPas: number };
  return data
    .params({ maxPas })
    .filter((d, $) => d.plate_appearances >= $.maxPas / 2);
}

function filterEligiblePitchers(data: aq.ColumnTable): aq.ColumnTable {
  let { maxOuts } = data
    .rollup({
      maxOuts: aq.op.max("outs"),
    })
    .object(0) as { maxOuts: number };
  return data.params({ maxOuts }).filter((d, $) => d.outs >= $.maxOuts / 2);
}

function useLatestGame(season: number, team: string) {
  const games = useQuery(
    gamesQuery({
      season,
      team,
    })
  );

  const latestGame = useMemo(() => {
    if (!games.data) return undefined;
    const gameIds = games.data?.items.map((x) => x.game_id);
    gameIds?.sort();
    return gameIds[gameIds.length - 1];
  }, [games.data]);

  const game = useQuery({
    ...chronLatestEntityQuery<MmolbGame>("game", latestGame ?? ""),
    enabled: !!latestGame,
  });
  return game;
}

function getBattingOrder(game: MmolbGame, teamId: string): string[] {
  if (game.AwayTeamID === teamId) return game.AwayLineup ?? [];
  if (game.HomeTeamID === teamId) return game.HomeLineup ?? [];
  return [];
}

function RouteComponent() {
  const { season } = Route.useLoaderData();
  const { id: teamId } = Route.useParams();

  const latestGame = useLatestGame(season ?? 3, teamId);
  const battingOrder = useMemo(() => {
    return latestGame.data ? getBattingOrder(latestGame.data.data, teamId) : [];
  }, [latestGame.data]);

  const teamQuery = useQuery(chronLatestEntityQuery<MmolbTeam>("team", teamId));

  const currentRosterTable = useMemo(() => {
    const slots: string[] = [];
    const playerIds: string[] = [];
    const keys: string[] = [];
    const positionType: string[] = [];

    // if undefined, still return a table with the right columns
    if (teamQuery.data) {
      for (let player of teamQuery.data.data.Players) {
        if (player.PlayerID === "#") continue;
        playerIds.push(player.PlayerID);
        slots.push(player.Slot);
        keys.push(`${player.PlayerID}/${player.FirstName} ${player.LastName}`);
        positionType.push(player.PositionType);
      }
    }

    return aq.table({
      slot: slots,
      player_id: playerIds,
      key: keys,
      position_type: positionType,
    });
  }, [teamQuery.data]);

  const leagueBatting = useQuery({
    ...statsQuery({
      season,
      group: ["player", "team", "player_name"],
      league: teamQuery.data?.data?.League,
      fields: battingStatFields,
    }),
    enabled: !!teamQuery.data,
  });

  const leaguePitching = useQuery({
    ...statsQuery({
      season,
      group: ["player", "team", "player_name"],
      league: teamQuery?.data?.data.League,
      fields: pitchingStatFields,
    }),
    enabled: !!teamQuery.data,
  });

  const teamBattingQuery = useQuery(
    statsQuery({
      season,
      group: ["player", "team", "player_name"],
      team: teamId,
      fields: battingStatFields,
    })
  );

  const teamPitching = useQuery(
    statsQuery({
      season,
      group: ["player", "team", "player_name"],
      team: teamId,
      fields: pitchingStatFields,
    })
  );

  const leagueBattingFiltered = useMemo(
    () =>
      leagueBatting.data
        ? filterEligibleBatters(leagueBatting.data)
        : undefined,
    [leagueBatting.data]
  );

  const leaguePitchingFiltered = useMemo(
    () =>
      leaguePitching.data
        ? filterEligiblePitchers(leaguePitching.data)
        : undefined,
    [leaguePitching.data]
  );

  const battingAgg = useMemo(() => {
    return leagueBattingFiltered
      ? calculateBattingStatReferences(leagueBattingFiltered)
      : undefined;
  }, [leagueBattingFiltered]);

  const pitchingAgg = useMemo(() => {
    return leaguePitchingFiltered
      ? calculatePitchingStatReferences(leaguePitchingFiltered)
      : undefined;
  }, [leaguePitchingFiltered]);

  const battingIndexes = useMemo(() => {
    return leagueBattingFiltered
      ? generatePercentileIndexes(
          calculateBattingStats(leagueBattingFiltered, battingAgg),
          ["ba", "obp", "slg", "ops", "sb_success", "babip"]
        )
      : {};
  }, [leagueBattingFiltered, battingAgg]);

  const pitchingIndexes = useMemo(() => {
    return leaguePitchingFiltered
      ? generatePercentileIndexes(
          calculatePitchingStats(leaguePitchingFiltered, pitchingAgg),
          ["era", "fip", "whip", "h9", "hr9", "k9", "bb9", "k_bb"]
        )
      : {};
  }, [leaguePitchingFiltered, pitchingAgg]);

  const [hideInactive, setHideInactive] = useState(true);

  const battingStats = useMemo(() => {
    if (!teamBattingQuery.data) return undefined;

    let sumTable = teamBattingQuery.data
      .rollup(
        Object.fromEntries(
          battingStatFields.map((key) => [key, aq.op.sum(key)])
        )
      )
      .derive({
        player_id: aq.escape(() => "sum"),
      })
      .reify();

    const joined = teamBattingQuery.data.concat(sumTable);

    let data = calculateBattingStats(joined, battingAgg).derive({
      key: (d) => d.player_id + "/" + d.player_name,
    });
    if (currentRosterTable) {
      data = data.lookup(currentRosterTable, "key");
    }
    data = data.derive({
      current: (d) => d.position_type === "Batter" || d.player_id === "sum",
    });

    if (battingOrder) {
      const battingOrderTable = aq.table({
        battingOrder: new Array(battingOrder.length)
          .fill(0)
          .map((_, i) => i + 1),
        slot: battingOrder,
      });
      data = data.lookup(battingOrderTable, "slot", "battingOrder");
    }
    // data = data.derive({
    //   status: (d) => {
    //     if (!d.slot) return "💩 ";
    //     if (!d.current) return "🔀 ";
    //     return "";
    //   },
    // });
    if (hideInactive) data = data.filter((d) => d.current);
    return data.reify();
  }, [
    teamBattingQuery.data,
    battingAgg,
    battingOrder,
    hideInactive,
    currentRosterTable,
  ]);

  const pitchingStats = useMemo(() => {
    if (!teamPitching.data) return undefined;
    let sumTable = teamPitching.data
      .rollup(
        Object.fromEntries(
          pitchingStatFields.map((key) => [key, aq.op.sum(key)])
        )
      )
      .derive({
        player_id: aq.escape(() => "sum"),
      })
      .reify();

    const joined = teamPitching.data.concat(sumTable);

    let data = calculatePitchingStats(joined, pitchingAgg).derive({
      key: (d) => d.player_id + "/" + d.player_name,
    });
    if (currentRosterTable) {
      data = data.lookup(currentRosterTable, "key");
    }
    data = data.derive({
      current: (d) => d.position_type === "Pitcher" || d.player_id === "sum",
    });
    if (hideInactive) data = data.filter((d) => d.current);
    // data = data.derive({
    //   status: (d) => {
    //     if (!d.slot) return "💩 ";
    //     if (!d.current) return "🔀 ";
    //     return "";
    //   },
    // });

    return data.reify();
  }, [teamPitching.data, pitchingAgg, hideInactive, currentRosterTable]);

  const navigate = useNavigate({ from: Route.fullPath });

  return (
    <div className="flex flex-col gap-4 py-2">
      <div className="flex flex-row">
        <div className="flex items-center gap-3 flex-1">
          <Checkbox
            checked={hideInactive}
            onCheckedChange={(e) => setHideInactive(e as boolean)}
            id="terms"
          />
          <Label htmlFor="terms">Only show current positions</Label>
        </div>

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
      <div>
        <h2 className="mb-2 font-medium text-lg">Batting</h2>
        {battingStats ? (
          <NewStatsTable
            position={"batting"}
            display="team"
            data={battingStats}
            indexes={battingIndexes}
          />
        ) : (
          <span>
            status: {teamBattingQuery.status}, error:{" "}
            {teamBattingQuery.error?.message?.toString()}
          </span>
        )}
      </div>

      <div>
        <h2 className="mb-2 font-medium text-lg">Pitching</h2>
        {pitchingStats ? (
          <NewStatsTable
            position={"pitching"}
            display="team"
            data={pitchingStats}
            indexes={pitchingIndexes}
          />
        ) : (
          <span>
            status: {teamBattingQuery.status}, error:{" "}
            {teamBattingQuery.error?.message?.toString()}
          </span>
        )}
      </div>

      <div>
        <h2 className="mb-2 font-medium text-lg">Color Scale (percentiles)</h2>
        <ColorPreview scale={defaultScale} />
      </div>
    </div>
  );
}
