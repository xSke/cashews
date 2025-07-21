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
  calculateBattingStatReferences,
  calculateBattingStats,
  calculatePitchingStatReferences,
  calculatePitchingStats,
  generatePercentileIndexes,
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

const stateSchema = z.object({
  season: z.number().optional(),
});

type StateParams = z.infer<typeof stateSchema>;

const battingStatFields: StatKey[] = [
  "singles",
  "doubles",
  "triples",
  "home_runs",
  "at_bats",
  "walked",
  "hit_by_pitch",
  "plate_appearances",
  "stolen_bases",
  "caught_stealing",
  "struck_out",
];

const pitchingStatFields: StatKey[] = [
  "outs",
  "earned_runs",
  "home_runs_allowed",
  "walks",
  "hit_batters",
  "strikeouts",
  "hits_allowed",
  "wins",
  "losses",
  "saves",
  "blown_saves",
  "unearned_runs",
  "appearances",
  "starts",
];

export const Route = createFileRoute("/team/$id/stats2")({
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
  const { season, currentSeason } = Route.useLoaderData();
  const { id: teamId } = Route.useParams();

  const latestGame = useLatestGame(season ?? 3, teamId);
  const battingOrder = useMemo(() => {
    return latestGame.data ? getBattingOrder(latestGame.data.data, teamId) : [];
  }, [latestGame.data]);

  const teamQuery = useQuery(chronLatestEntityQuery<MmolbTeam>("team", teamId));

  const currentRoster = useMemo(() => {
    return teamQuery.data
      ? teamQuery.data.data.Players.map(
          (x) => `${x.PlayerID}/${x.FirstName} ${x.LastName}`
        ).filter((x) => x != "#")
      : [];
  }, [teamQuery.data]);

  const leagueBatting = useQuery({
    ...statsQuery({
      season,
      group: ["player", "team", "player_name", "slot"],
      league: teamQuery.data?.data?.League,
      fields: battingStatFields,
    }),
    enabled: !!teamQuery.data,
  });

  const leaguePitching = useQuery({
    ...statsQuery({
      season,
      group: ["player", "team", "player_name", "slot"],
      league: teamQuery?.data?.data.League,
      fields: pitchingStatFields,
    }),
    enabled: !!teamQuery.data,
  });

  const teamBatting = useQuery(
    statsQuery({
      season,
      group: ["player", "team", "player_name", "slot"],
      team: teamId,
      fields: battingStatFields,
    })
  );

  const teamPitching = useQuery(
    statsQuery({
      season,
      group: ["player", "team", "player_name", "slot"],
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
          ["ba", "obp", "slg", "ops", "ops_plus", "sb_success"]
        )
      : {};
  }, [leagueBattingFiltered, battingAgg]);

  const pitchingIndexes = useMemo(() => {
    return leaguePitchingFiltered
      ? generatePercentileIndexes(
          calculatePitchingStats(leaguePitchingFiltered, pitchingAgg),
          [
            "era",
            "era_minus",
            "fip",
            "fip_minus",
            "whip",
            "h9",
            "hr9",
            "k9",
            "bb9",
            "k_bb",
          ]
        )
      : {};
  }, [leaguePitchingFiltered, pitchingAgg]);

  const [hideInactive, setHideInactive] = useState(true);

  const battingStats = useMemo(() => {
    if (!teamBatting.data) return undefined;

    let data = calculateBattingStats(teamBatting.data, battingAgg)
      .params({ currentRoster })
      .derive({
        current: (d, $) =>
          aq.op.includes($.currentRoster, d.player_id + "/" + d.player_name),
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
    if (hideInactive) data = data.filter((d) => d.current);
    return data.reify();
  }, [teamBatting.data, battingAgg, battingOrder, hideInactive]);

  const pitchingStats = useMemo(() => {
    if (!teamPitching.data) return undefined;
    let data = calculatePitchingStats(teamPitching.data, pitchingAgg)
      .params({ currentRoster })
      .derive({
        current: (d, $) =>
          aq.op.includes($.currentRoster, d.player_id + "/" + d.player_name),
      });
    if (hideInactive) data = data.filter((d) => d.current);
    return data.reify();
  }, [teamPitching.data, pitchingAgg, hideInactive]);

  const navigate = useNavigate({ from: Route.fullPath });

  const seasons = createSeasonList(currentSeason);
  return (
    <div className="flex flex-col gap-4 py-2">
      <div className="flex flex-row">
        <div className="flex items-center gap-3 flex-1">
          <Checkbox
            checked={hideInactive}
            onCheckedChange={(e) => setHideInactive(e as boolean)}
            id="terms"
          />
          <Label htmlFor="terms">Hide inactive players</Label>
        </div>

        <div className="place-self-end">
          <Select
            value={season.toString()}
            onValueChange={(val) => {
              navigate({
                search: (prev) => ({ ...prev, season: parseInt(val) ?? 0 }),
              });
            }}
          >
            <SelectTrigger className="w-[180px]">
              <SelectValue placeholder="Season..."></SelectValue>
            </SelectTrigger>
            <SelectContent>
              {seasons.map((s) => (
                <SelectItem value={s.toString()}>Season {s}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
      <div>
        <h2 className="mb-2 font-medium text-lg">Batting</h2>
        {battingStats && (
          <NewStatsTable
            position={"batting"}
            data={battingStats}
            indexes={battingIndexes}
          />
        )}
      </div>

      <div>
        <h2 className="mb-2 font-medium text-lg">Pitching</h2>
        {pitchingStats && (
          <NewStatsTable
            position={"pitching"}
            data={pitchingStats}
            indexes={pitchingIndexes}
          />
        )}
      </div>

      <div>
        <h2 className="mb-2 font-medium text-lg">Color Scale (percentiles)</h2>
        <ColorPreview scale={defaultScale} />
      </div>
    </div>
  );
}
