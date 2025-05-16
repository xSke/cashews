"use client";

import { ChronGame, MmolbTeam } from "@/data/data";
import { useMemo } from "react";
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
import React from "react";
import { Button } from "./ui/button";
import { ArrowUpDown } from "lucide-react";

interface TeamGamesTableProps {
  games: ChronGame[];
  teams: Record<string, MmolbTeam>;
}

interface TableData {
  id: string;
  season: number;
  day: number;
  away_team_id: string;
  home_team_id: string;
  away_score: number;
  home_score: number;
}

interface TableMeta {
  teams: Record<string, MmolbTeam>;
}

function TeamCell(ctx: CellContext<TableData, unknown>) {
  const meta = ctx.table.options.meta as TableMeta;
  const teamId = ctx.getValue() as string;

  const team = meta.teams[teamId];
  if (!team) return "Null Team";
  return (
    <span>
      {team.Emoji} {team.Location} {team.Name}
    </span>
  );
}

function sortableHeader(name: string) {
  return (props: HeaderContext<TableData, any>) => {
    return (
      <Button
        variant="ghost"
        onClick={() =>
          props.column.toggleSorting(props.column.getIsSorted() === "asc")
        }
      >
        {name}
        <ArrowUpDown className="ml-2 h-4 w-4" />
      </Button>
    );
  };
}

const columns: ColumnDef<TableData>[] = [
  {
    accessorKey: "season",
    header: "Season",
  },
  {
    accessorKey: "day",
    header: "Day",
  },
  {
    accessorKey: "away_team_id",
    header: "Away",
    cell: TeamCell,
  },
  {
    accessorKey: "home_team_id",
    header: "Home",
    cell: TeamCell,
  },
  {
    accessorKey: "away_score",
    header: "Away Score",
  },
  {
    accessorKey: "home_score",
    header: "Home Score",
  },
];

export default function TeamGamesTable(props: TeamGamesTableProps) {
  const data = useMemo(() => {
    const data = [];
    for (let game of props.games) {
      const tableGame = {
        id: game.game_id,
        season: game.season,
        day: game.day,
        away_team_id: game.away_team_id,
        home_team_id: game.home_team_id,
        away_score: game.last_update?.away_score ?? 0,
        home_score: game.last_update?.home_score ?? 0,
      };
      data.push(tableGame);
    }
    return data;
  }, [props.games]);

  const [sorting, setSorting] = React.useState<SortingState>([]);

  const table = useReactTable({
    data,
    columns,
    meta: { teams: props.teams } as TableMeta,
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
