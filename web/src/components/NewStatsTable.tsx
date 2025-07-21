import {
  BattingStatReferences,
  calculateBattingStats,
  findPercentile,
  PercentileIndex,
  PercentileResult,
} from "@/lib/newstats";
import * as aq from "arquero";
import { CSSProperties, useMemo, useState } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableFooter,
  TableHead,
  TableHeader,
  TableRow,
} from "./ui/table";
import {
  Column,
  ColumnDef,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { ChevronDown, ChevronUp, Square } from "lucide-react";
import clsx from "clsx";
import { defaultScale } from "@/lib/colors";
import { Tooltip, TooltipContent } from "./ui/tooltip";
import { TooltipTrigger } from "@radix-ui/react-tooltip";

export interface NewStatsTableProps {
  position: "batting" | "pitching";
  data: aq.ColumnTable;
  indexes: Record<string, PercentileIndex>;
}

export default function NewStatsTable(props: NewStatsTableProps) {
  const enriched = useMemo(() => props.data.reify(), [props.data]);

  function idColumn(): ColumnDef<number, string> {
    const getter = enriched.getter("player_id");
    return {
      id: "id",
      header: "ID",
      cell: (c) => <div className="font-mono">{c.getValue()?.toString()}</div>,
      accessorFn: (r) => getter(r),
      sortingFn: "basic",
    };
  }

  function slotColumn(): ColumnDef<number, string> {
    const getter = enriched.getter("slot");
    const currentGetter = enriched.getter("current");
    const order = [
      "C",
      "1B",
      "2B",
      "3B",
      "SS",
      "LF",
      "CF",
      "RF",
      "DH",
      "SP1",
      "SP2",
      "SP3",
      "SP4",
      "SP5",
      "RP1",
      "RP2",
      "RP3",
      "CL",
    ];

    function posSortGetter(row: number) {
      // sort non-current players at the bottom
      return order.indexOf(getter(row)) + (currentGetter(row) ? 0 : 100);
    }
    return {
      id: "position",
      header: "Pos.",
      accessorFn: (r) => getter(r) ?? "-",
      sortingFn: (a, b) => {
        return -(posSortGetter(b.original) - posSortGetter(a.original));
      },
    };
  }

  function nameColumn(): ColumnDef<number, string> {
    const getter = enriched.getter("player_name");
    const idGetter = enriched.getter("player_id");
    return {
      id: "name",
      header: "Name",
      accessorFn: (r) => getter(r),
      cell: (c) => (
        <span className="font-semibold">
          <a
            className="hover:underline"
            href={`https://mmolb.com/player/${idGetter(c.row.original)}`}
          >
            {c.getValue()}
          </a>
        </span>
      ),
    };
  }

  function statColumn(
    col: string,
    title: string,
    opts: {
      decimals?: number;
      format?: "number" | "ip";
      order?: "asc" | "desc";
    } = {}
  ): ColumnDef<number, number> {
    const getter = enriched.column(col) ? enriched.getter(col) : (_) => NaN;

    const format = opts.format ?? "number";
    const order = opts.order ?? "desc";
    const decimals = opts.decimals ?? 0;

    function formatValue(val: number) {
      if (format === "number") {
        return val.toFixed(decimals);
      } else if (format === "ip") {
        const innings = Math.floor(val);
        const outs = Math.floor(val * 3) % 3;
        return `${innings}.${outs}`;
      }
    }
    return {
      id: col,
      header: title,
      cell: (c) => {
        const val = c.getValue();
        let perc: PercentileResult | undefined;
        if (props.indexes[col]) {
          const index = props.indexes[col];
          perc = findPercentile(index, val, order === "desc");
        }

        const scale = defaultScale;
        const lightColor = scale.light(perc?.percentile ?? 0);
        const darkColor = scale.dark(perc?.percentile ?? 0);

        const inner = isNaN(val) ? "-" : formatValue(val);

        return (
          <div
            className={clsx(
              "tabular-nums text-right",
              perc && "font-semibold dark:font-medium"
            )}
            style={{
              color:
                perc !== undefined
                  ? `light-dark(${lightColor.css()}, ${darkColor.css()})`
                  : undefined,
            }}
          >
            {perc !== undefined ? (
              <Tooltip>
                <TooltipTrigger asChild>
                  <span>{inner}</span>
                </TooltipTrigger>
                <TooltipContent>
                  #{perc.rank + 1}/{perc.total}, better than{" "}
                  {(perc.percentile * 100).toFixed(1)}%
                </TooltipContent>
              </Tooltip>
            ) : (
              inner
            )}
          </div>
        );
      },
      accessorFn: (r) => getter(r),
      meta: {
        alignRight: true,
      },
    };
  }

  const columns: ColumnDef<number>[] = useMemo(() => {
    if (props.position === "batting")
      return [
        // idColumn(),
        { ...statColumn("battingOrder", "#"), maxSize: 40 },
        nameColumn(),
        slotColumn(),
        statColumn("plate_appearances", "PAs"),
        statColumn("at_bats", "ABs"),
        statColumn("hits", "H"),
        statColumn("singles", "1B"),
        statColumn("doubles", "2B"),
        statColumn("triples", "3B"),
        statColumn("home_runs", "HR"),
        statColumn("hit_by_pitch", "HBP"),
        statColumn("walked", "BB"),
        statColumn("struck_out", "K", { order: "asc" }),
        statColumn("ba", "BA", { decimals: 3 }),
        statColumn("obp", "OBP", { decimals: 3 }),
        statColumn("slg", "SLG", { decimals: 3 }),
        statColumn("ops", "OPS", { decimals: 3 }),
        statColumn("ops_plus", "OPS+"),
        statColumn("stolen_bases", "SB"),
        statColumn("caught_stealing", "CS", { order: "asc" }),
        statColumn("sb_success", "SB%", { decimals: 2 }),
      ];
    if (props.position === "pitching")
      return [
        nameColumn(),
        slotColumn(),
        statColumn("appearances", "G"),
        statColumn("starts", "GS"),
        statColumn("ip", "IP", { format: "ip" }),
        statColumn("wins", "W"),
        statColumn("losses", "L", { order: "asc" }),
        statColumn("saves", "S"),
        statColumn("blown_saves", "BS", { order: "asc" }),
        statColumn("earned_runs", "ER"),
        statColumn("unearned_runs", "UR"),
        statColumn("hits_allowed", "H", { order: "asc" }),
        statColumn("home_runs_allowed", "HR", { order: "asc" }),
        statColumn("strikeouts", "K"),
        statColumn("walks", "BB", { order: "asc" }),
        statColumn("hit_batters", "HB", { order: "asc" }),
        statColumn("era", "ERA", { decimals: 2, order: "asc" }),
        statColumn("era_minus", "ERA-", { order: "asc" }),
        statColumn("fip", "FIP", { decimals: 2, order: "asc" }),
        statColumn("fip_minus", "FIP-", { order: "asc" }),
        statColumn("whip", "WHIP", { decimals: 2, order: "asc" }),
        statColumn("h9", "H/9", { decimals: 2, order: "asc" }),
        statColumn("hr9", "HR/9", { decimals: 2, order: "asc" }),
        statColumn("k9", "K/9", { decimals: 2 }),
        statColumn("bb9", "BB/9", { decimals: 2, order: "asc" }),
        statColumn("k_bb", "K/BB", { decimals: 2 }),
      ];
    return [];
  }, [enriched, props.indexes, props.position]);

  const data = useMemo(() => {
    const arr: number[] = [];
    enriched.scan((row) => arr.push(row!));
    return arr;
  }, [enriched]);

  const [sorting, setSorting] = useState<SortingState>([
    { desc: false, id: "position" },
  ]);

  const table = useReactTable<number>({
    data,
    columns,

    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    onSortingChange: setSorting,
    enableSortingRemoval: false,
    initialState: {
      columnPinning: {
        left: ["battingOrder", "name"],
      },
    },
    state: {
      sorting,
    },
  });

  return (
    <div className="rounded-md border">
      <Table style={{ borderCollapse: "separate", borderSpacing: 0 }}>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id} className="group">
              {headerGroup.headers.map((header) => {
                const canSort = header.column.getCanSort();
                const alignRight = (header.column.columnDef.meta as any)
                  ?.alignRight;
                const column = header.column;
                return (
                  <TableHead
                    key={header.id}
                    className={clsx(
                      "font-semibold text-left border-b bg-background",
                      canSort && "cursor-pointer"
                    )}
                    onClick={
                      canSort ? () => header.column.toggleSorting() : () => {}
                    }
                    style={{ ...getCommonPinningStyles(column) }}
                  >
                    <div
                      className={clsx(
                        "flex items-center gap-1",
                        alignRight ? "flex-row-reverse" : "flex-row"
                      )}
                    >
                      <span>
                        {header.isPlaceholder
                          ? null
                          : flexRender(
                              header.column.columnDef.header,
                              header.getContext()
                            )}
                      </span>

                      {canSort ? (
                        <>
                          {header.column.getIsSorted() === "asc" && (
                            <ChevronUp className="h-4 w-4" />
                          )}
                          {header.column.getIsSorted() === "desc" && (
                            <ChevronDown className="h-4 w-4" />
                          )}
                          {header.column.getIsSorted() !== "asc" &&
                            header.column.getIsSorted() !== "desc" && (
                              <div className="h-4 w-4"></div>
                            )}
                        </>
                      ) : null}
                    </div>
                  </TableHead>
                );
              })}
            </TableRow>
          ))}
        </TableHeader>
        <TableBody>
          {table.getRowModel().rows?.length ? (
            table.getRowModel().rows.map((row) => {
              const isCurrent = enriched.get("current", row.original);
              return (
                <TableRow
                  key={row.id}
                  data-state={row.getIsSelected() && "selected"}
                  className={clsx("group", "hover:bg-inherit")}
                >
                  {row.getVisibleCells().map((cell) => {
                    const column = cell.column;
                    return (
                      <TableCell
                        key={cell.id}
                        className={clsx("p-0 border-b bg-background")}
                        style={{ ...getCommonPinningStyles(column) }}
                      >
                        <div
                          className={clsx(
                            "p-2 group-hover:bg-gray-300/50 dark:group-hover:bg-gray-800/50 transition-colors",
                            !isCurrent && "bg-orange-800/20"
                          )}
                        >
                          {flexRender(
                            cell.column.columnDef.cell,
                            cell.getContext()
                          )}
                        </div>
                      </TableCell>
                    );
                  })}
                </TableRow>
              );
            })
          ) : (
            <TableRow>
              <TableCell colSpan={columns.length} className="h-24 text-center">
                No results.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
        <TableFooter>
          {table.getFooterGroups().map((footerGroup) => (
            <TableRow key={footerGroup.id}>
              {footerGroup.headers.map((header) => {
                return (
                  <TableHead key={header.id} className="font-semibold">
                    {header.isPlaceholder
                      ? null
                      : flexRender(
                          header.column.columnDef.footer,
                          header.getContext()
                        )}
                  </TableHead>
                );
              })}
            </TableRow>
          ))}
        </TableFooter>
      </Table>
    </div>
  );
}

//These are the important styles to make sticky column pinning work!
//Apply styles like this using your CSS strategy of choice with this kind of logic to head cells, data cells, footer cells, etc.
//View the index.css file for more needed styles such as border-collapse: separate
const getCommonPinningStyles = (column: Column<any>): CSSProperties => {
  const isPinned = column.getIsPinned();
  const isLastLeftPinnedColumn =
    isPinned === "left" && column.getIsLastColumn("left");
  const isFirstLeftPinnedColumn =
    isPinned === "left" && column.getIsFirstColumn("left");
  const isFirstRightPinnedColumn =
    isPinned === "right" && column.getIsFirstColumn("right");

  return {
    // boxShadow: isLastLeftPinnedColumn
    //   ? "-4px 0 4px -4px gray inset"
    //   : isFirstRightPinnedColumn
    //     ? "4px 0 4px -4px gray inset"
    //     : undefined,
    borderRight: isLastLeftPinnedColumn ? "1px solid var(--border)" : undefined,
    left: isPinned === "left" ? `${column.getStart("left")}px` : undefined,
    right: isPinned === "right" ? `${column.getAfter("right")}px` : undefined,
    opacity: isPinned ? 1 : 1,
    position: isPinned ? "sticky" : "relative",
    width: column.getSize(),
    zIndex: isPinned ? 1 : 0,
  };
};
