"use client";

import {
  MmolbPlayer,
  MmolbTeam,
  PlayerStatsEntry,
  StatPercentile,
} from "@/data/data";
import { AdvancedStats, calculateAdvancedStats } from "@/data/stats";
import {
  CellContext,
  ColumnDef,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  HeaderContext,
  SortingState,
  useReactTable,
} from "@tanstack/react-table";
import {
  Table,
  TableBody,
  TableCell,
  TableFooter,
  TableHead,
  TableHeader,
  TableRow,
} from "./ui/table";
import { useMemo, useState } from "react";
import { Button } from "./ui/button";
import { ChevronDown, ChevronUp } from "lucide-react";

interface StatsTableProps {
  data: PlayerStatsEntry[];
  players: Record<string, MmolbPlayer>;
  teams: Record<string, MmolbTeam>;
  type: "batting" | "pitching";
  aggs: StatPercentile[];
}

type RowData = {
  id: string;
  name: string;
  position: string | null;
} & AdvancedStats;

function getColorClass(
  value: number,
  aggs: StatPercentile[],
  aggKey: string,
  inverse: boolean
): string {
  const colors = [
    "text-red-700",
    "text-orange-700",
    "text-amber-700",
    "text-lime-700",
    "text-green-700",
    "text-emerald-700",
    "text-teal-800",
    "text-cyan-800",
  ];

  for (let i = 0; i < aggs.length; i++) {
    if (inverse) {
      if (((aggs[aggs.length - i - 1] as any)[aggKey] as number) > value)
        return colors[aggs.length - i] ?? "";
    } else {
      if (((aggs[i] as any)[aggKey] as number) > value) return colors[i] ?? "";
    }
  }
  if (inverse) {
    return colors[0];
  } else {
    return colors[colors.length - 1];
  }
}

function StatCell(
  digits: number,
  aggKey: string | null = null,
  inverse: boolean = false
) {
  return (props: CellContext<RowData, unknown>) => {
    const data = props.getValue() as number;

    const aggs = (props.table.options.meta as any).aggs as StatPercentile[];
    const statKey = aggKey ? getColorClass(data, aggs, aggKey, inverse) : "";

    return (
      <div
        className={`text-right tabular-nums p-2 ${statKey && "font-medium"} ${statKey}`}
      >
        {data.toFixed(digits)}
      </div>
    );
  };
}

function NormalCell() {
  return (props: CellContext<RowData, unknown>) => {
    return <div className="p-2">{props.getValue()?.toString()}</div>;
  };
}

function StatFooter() {
  return (props: HeaderContext<RowData, unknown>) => {
    return <div className="p-2">todo</div>;
  };
}

function InningsCell() {
  return (props: CellContext<RowData, unknown>) => {
    const data = props.getValue() as number;

    const innings = Math.floor(data);
    const outs = Math.floor(data * 3) % 3;
    return (
      <div className="tabular-nums p-2 font-medium">
        {innings}.{outs}
      </div>
    );
  };
}

function SortableHeader(name: string) {
  return (props: HeaderContext<RowData, unknown>) => {
    return (
      //   TODO: make the headers right align somehow, and then put the chevron on the left?
      <div
        className="flex items-center cursor-pointer"
        onClick={() => props.column.toggleSorting()}
      >
        {name}

        {props.column.getIsSorted() === "asc" && (
          <ChevronUp className="h-4 w-4 ml-0.5" />
        )}
        {props.column.getIsSorted() === "desc" && (
          <ChevronDown className="h-4 w-4 ml-0.5" />
        )}
      </div>
    );
  };
}

function IdCell(props: CellContext<RowData, unknown>) {
  return <div className="p-2 font-mono">{props.getValue()?.toString()}</div>;
}

function NameCell(props: CellContext<RowData, unknown>) {
  const player = props.row.original;
  return (
    <a
      href={`https://mmolb.com/player/${player.id}`}
      className="p-2 hover:underline"
    >
      {props.getValue()?.toString()}
    </a>
  );
}

const columnsBase: ColumnDef<RowData>[] = [
  // {
  //   header: SortableHeader("ID"),
  //   accessorKey: "id",
  //   cell: IdCell,
  // },
  {
    header: SortableHeader("Name"),
    accessorKey: "name",
    cell: NameCell,
  },
  {
    header: "Pos.",
    accessorKey: "position",
    cell: NormalCell(),
  },
];

const columnsBatting: ColumnDef<RowData>[] = [
  {
    header: SortableHeader("PAs"),
    accessorKey: "plate_appearances",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("ABs"),
    accessorKey: "at_bats",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("H"),
    accessorKey: "hits",
    cell: StatCell(0),
    // footer: StatFooter(),
  },
  {
    header: SortableHeader("2B"),
    accessorKey: "doubles",
    cell: StatCell(0),
    // footer: StatFooter(),
  },
  {
    header: SortableHeader("3B"),
    accessorKey: "triples",
    cell: StatCell(0),
    // footer: StatFooter(),
  },
  {
    header: SortableHeader("HR"),
    accessorKey: "home_runs",
    cell: StatCell(0),
    // footer: StatFooter(),
  },
  {
    header: SortableHeader("BB"),
    accessorKey: "walked",
    cell: StatCell(0),
    // footer: StatFooter(),
  },
  {
    header: SortableHeader("K"),
    accessorKey: "struck_out",
    cell: StatCell(0),
    // footer: StatFooter(),
  },
  {
    header: SortableHeader("BA"),
    accessorKey: "ba",
    cell: StatCell(3, "ba"),
    // footer: StatFooter(),
  },
  {
    header: SortableHeader("OBP"),
    accessorKey: "obp",
    cell: StatCell(3, "obp"),
  },
  {
    header: SortableHeader("SLG"),
    accessorKey: "slg",
    cell: StatCell(3, "slg"),
  },
  {
    header: SortableHeader("OPS"),
    accessorKey: "ops",
    cell: StatCell(3, "ops"),
  },
  {
    header: SortableHeader("OPS+"),
    accessorKey: "ops_plus",
    cell: StatCell(0, "ops_plus"),
  },
];
const columnsPitching: ColumnDef<RowData>[] = [
  {
    header: SortableHeader("G"),
    accessorKey: "appearances",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("GS"),
    accessorKey: "starts",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("IP"),
    accessorKey: "ip",
    cell: InningsCell(),
  },
  {
    header: SortableHeader("W"),
    accessorKey: "wins",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("L"),
    accessorKey: "losses",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("H"),
    accessorKey: "hits_allowed",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("HR"),
    accessorKey: "home_runs_allowed",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("K"),
    accessorKey: "strikeouts",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("BB"),
    accessorKey: "walks",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("ERA"),
    accessorKey: "era",
    cell: StatCell(2, "era", true),
  },
  {
    header: SortableHeader("ERA-"),
    accessorKey: "era_minus",
    cell: StatCell(0, "era_minus"),
  },
  {
    header: SortableHeader("FIP"),
    accessorKey: "fip",
    cell: StatCell(2, "fip", true),
  },
  {
    header: SortableHeader("WHIP"),
    accessorKey: "whip",
    cell: StatCell(2, "whip", true),
  },
  {
    header: SortableHeader("H/9"),
    accessorKey: "h9",
    cell: StatCell(2, "h9", true),
  },
  {
    header: SortableHeader("HR/9"),
    accessorKey: "hr9",
    cell: StatCell(2, "hr9", true),
  },
  {
    header: SortableHeader("K/9"),
    accessorKey: "k9",
    cell: StatCell(2, "k9"),
  },
  {
    header: SortableHeader("BB/9"),
    accessorKey: "bb9",
    cell: StatCell(2, "bb9", true),
  },
];

export default function StatsTable(props: StatsTableProps) {
  const data = useMemo(() => {
    return processStats(props);
  }, [props.data, props.players, props.teams]);

  const columns = useMemo(() => {
    if (props.type === "batting") return [...columnsBase, ...columnsBatting];
    if (props.type === "pitching") return [...columnsBase, ...columnsPitching];
    return columnsBase;
  }, [props.type]);

  const [sorting, setSorting] = useState<SortingState>([]);

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
    onSortingChange: setSorting,
    getSortedRowModel: getSortedRowModel(),
    state: {
      sorting,
    },
    meta: {
      aggs: props.aggs,
    },
  });

  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id}>
              {headerGroup.headers.map((header) => {
                return (
                  <TableHead key={header.id} className="font-semibold">
                    {header.isPlaceholder
                      ? null
                      : flexRender(
                          header.column.columnDef.header,
                          header.getContext()
                        )}
                  </TableHead>
                );
              })}
            </TableRow>
          ))}
        </TableHeader>
        <TableBody>
          {table.getRowModel().rows?.length ? (
            table.getRowModel().rows.map((row) => (
              <TableRow
                key={row.id}
                data-state={row.getIsSelected() && "selected"}
              >
                {row.getVisibleCells().map((cell) => (
                  <TableCell key={cell.id} className="p-0">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
            ))
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
function processStats(props: StatsTableProps): RowData[] {
  const data = [];
  for (let row of props.data) {
    const stats = calculateAdvancedStats(row, props.aggs);
    if (props.type == "batting" && stats.plate_appearances == 0) continue;
    if (props.type == "pitching" && stats.ip == 0) continue;

    const player = props.players[row.player_id]!;
    const name = player.FirstName + " " + player.LastName;
    const position = player.Position;
    const id = row.player_id;
    data.push({ ...stats, name, position, id });
  }
  return data;
}
