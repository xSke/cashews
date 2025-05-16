"use client";

import { MmolbPlayer, MmolbTeam, PlayerStatsEntry } from "@/data/data";
import { AdvancedStats, calculateAdvancedStats } from "@/data/stats";
import {
  CellContext,
  ColumnDef,
  flexRender,
  getCoreRowModel,
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
import { useMemo } from "react";

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

function StatCell(digits: number) {
  return (props: CellContext<RowData, unknown>) => {
    const data = props.getValue() as number;
    return <span className="tabular-nums">{data.toFixed(digits)}</span>;
  };
}

function InningsCell() {
  return (props: CellContext<RowData, unknown>) => {
    const data = props.getValue() as number;

    const innings = Math.floor(data);
    const outs = Math.floor(data * 3) % 3;
    return (
      <span className="tabular-nums">
        {innings}.{outs}
      </span>
    );
  };
}

const columnsBase: ColumnDef<RowData>[] = [
  { header: "ID", accessorKey: "id" },
  {
    header: "Name",
    accessorKey: "name",
  },
  {
    header: "Pos.",
    accessorKey: "position",
  },
];

const columnsBatting: ColumnDef<RowData>[] = [
  {
    header: "ABs",
    accessorKey: "at_bats",
  },
  {
    header: "PAs",
    accessorKey: "plate_appearances",
  },
  {
    header: "BA",
    accessorKey: "ba",
    cell: StatCell(3),
  },
  {
    header: "OBP",
    accessorKey: "obp",
    cell: StatCell(3),
  },
  {
    header: "SLG",
    accessorKey: "slg",
    cell: StatCell(3),
  },
  {
    header: "OPS",
    accessorKey: "ops",
    cell: StatCell(3),
  },
];
const columnsPitching: ColumnDef<RowData>[] = [
  {
    header: "IP",
    accessorKey: "ip",
    cell: InningsCell(),
  },
  {
    header: "ERA",
    accessorKey: "era",
    cell: StatCell(2),
  },
  {
    header: "WHIP",
    accessorKey: "whip",
    cell: StatCell(2),
  },
  {
    header: "H/9",
    accessorKey: "h9",
    cell: StatCell(2),
  },
  {
    header: "HR/9",
    accessorKey: "hr9",
    cell: StatCell(2),
  },
  {
    header: "K/9",
    accessorKey: "k9",
    cell: StatCell(2),
  },
  {
    header: "BB/9",
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

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id}>
              {headerGroup.headers.map((header) => {
                return (
                  <TableHead key={header.id}>
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
                  <TableCell key={cell.id}>
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
