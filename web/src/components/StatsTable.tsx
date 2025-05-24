"use client";

import { MmolbPlayer, MmolbTeam, PlayerStatsEntry } from "@/data/data";
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
}

type RowData = {
  id: string;
  name: string;
  position: string | null;
} & AdvancedStats;

function StatCell(digits: number, foo: string = "") {
  return (props: CellContext<RowData, unknown>) => {
    const data = props.getValue() as number;
    return <div className="tabular-nums p-2">{data.toFixed(digits)}</div>;
  };
}

function NormalCell() {
  return (props: CellContext<RowData, unknown>) => {
    return <div className="p-2">{props.getValue()?.toString()}</div>;
  };
}

function InningsCell() {
  return (props: CellContext<RowData, unknown>) => {
    const data = props.getValue() as number;

    const innings = Math.floor(data);
    const outs = Math.floor(data * 3) % 3;
    return (
      <div className="tabular-nums p-2 font-medium text-green-700 dark:text-green-400">
        {innings}.{outs}
      </div>
    );
  };
}

function SortableHeader(name: string) {
  return (props: HeaderContext<RowData, unknown>) => {
    return (
      <div
        className="flex items-center cursor-pointer"
        onClick={() => props.column.toggleSorting()}
      >
        {name}

        {props.column.getIsSorted() === "asc" && (
          <ChevronDown className="h-4 w-4 ml-0.5" />
        )}
        {props.column.getIsSorted() === "desc" && (
          <ChevronUp className="h-4 w-4 ml-0.5" />
        )}
      </div>
    );
  };
}

const columnsBase: ColumnDef<RowData>[] = [
  {
    header: SortableHeader("ID"),
    accessorKey: "id",
    cell: NormalCell(),
  },
  {
    header: SortableHeader("Name"),
    accessorKey: "name",
    cell: NormalCell(),
  },
  {
    header: "Pos.",
    accessorKey: "position",
    cell: NormalCell(),
  },
];

const columnsBatting: ColumnDef<RowData>[] = [
  {
    header: SortableHeader("ABs"),
    accessorKey: "at_bats",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("PAs"),
    accessorKey: "plate_appearances",
    cell: StatCell(0),
  },
  {
    header: SortableHeader("BA"),
    accessorKey: "ba",
    cell: StatCell(3),
  },
  {
    header: SortableHeader("OBP"),
    accessorKey: "obp",
    cell: StatCell(3),
  },
  {
    header: SortableHeader("SLG"),
    accessorKey: "slg",
    cell: StatCell(3),
  },
  {
    header: SortableHeader("OPS"),
    accessorKey: "ops",
    cell: StatCell(3),
  },
];
const columnsPitching: ColumnDef<RowData>[] = [
  {
    header: SortableHeader("IP"),
    accessorKey: "ip",
    cell: InningsCell(),
  },
  {
    header: SortableHeader("ERA"),
    accessorKey: "era",
    cell: StatCell(2),
  },
  {
    header: SortableHeader("WHIP"),
    accessorKey: "whip",
    cell: StatCell(2),
  },
  {
    header: SortableHeader("H/9"),
    accessorKey: "h9",
    cell: StatCell(2),
  },
  {
    header: SortableHeader("HR/9"),
    accessorKey: "hr9",
    cell: StatCell(2),
  },
  {
    header: SortableHeader("K/9"),
    accessorKey: "k9",
    cell: StatCell(2),
  },
  {
    header: SortableHeader("BB/9"),
    accessorKey: "bb9",
    cell: StatCell(2),
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
      </Table>
    </div>
  );
}
function processStats(props: StatsTableProps): RowData[] {
  const data = [];
  for (let row of props.data) {
    const stats = calculateAdvancedStats(row);
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
