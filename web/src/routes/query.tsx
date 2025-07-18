import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { use, useEffect, useMemo, useState } from "react";

import * as arrow from "apache-arrow";
import type * as duckdb from "@duckdb/duckdb-wasm";
import duckdb_wasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvp_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdb_wasm_eh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import eh_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import { useQuery } from "@tanstack/react-query";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  CellContext,
  ColumnDef,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  SortingState,
  useReactTable,
} from "@tanstack/react-table";
import clsx from "clsx";
import { DataTablePagination } from "@/components/DataTablePagination";
import { z } from "zod";

const stateSchema = z.object({
  q: z.string().optional(),
});

export const Route = createFileRoute("/query")({
  component: RouteComponent,
  ssr: false,
  validateSearch: (search) => stateSchema.parse(search),
});

async function initDuckDb() {
  if (typeof window === "undefined") return;

  const duckdb = await import("@duckdb/duckdb-wasm");

  const MANUAL_BUNDLES = {
    mvp: {
      mainModule: duckdb_wasm,
      mainWorker: mvp_worker,
    },
    eh: {
      mainModule: duckdb_wasm_eh,
      mainWorker: eh_worker,
    },
  };
  // Select a bundle based on browser checks
  const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);
  // Instantiate the asynchronus version of DuckDB-wasm
  const worker = new Worker(bundle.mainWorker!);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  await db.open({});
  return db;
}

const dbPromise = initDuckDb();

function Cell(type: arrow.DataType, name: string) {
  const isNumber =
    arrow.DataType.isDecimal(type) ||
    arrow.DataType.isInt(type) ||
    arrow.DataType.isFloat(type);

  const isFloat =
    arrow.DataType.isDecimal(type) || arrow.DataType.isFloat(type);

  const isId = name.endsWith("_id");

  function inner(props: CellContext<number, number>) {
    const value = props.getValue();
    return (
      <div
        className={clsx(
          isNumber && "text-right tabular-nums",
          isId && "font-mono"
        )}
      >
        {isFloat ? value?.toString() : value?.toString()}
      </div>
    );
  }
  return inner;
}

function ResultTable(props: { table: arrow.Table }) {
  const numRows = props.table.numRows;

  const [sorting, setSorting] = useState<SortingState>([]);

  const columns: ColumnDef<any, any>[] = useMemo(() => {
    return props.table.schema.fields.map((col, colIdx) => {
      const type: arrow.DataType = col.type;

      return {
        header: () => (
          <div>
            <span>{col.name}</span>
            <span className="text-gray-500 dark:text-gray-400 ml-1">
              {col.type[Symbol.toStringTag].toString()}
            </span>
          </div>
        ),
        id: col.name,
        accessorFn: (rowIdx: any) => {
          const row = props.table.get(rowIdx);
          return row ? row[col.name] : null;
        },
        cell: Cell(type, col.name),
      } as ColumnDef<any, any>;
    });
  }, [props.table.schema]);

  const [pagination, setPagination] = useState({
    pageIndex: 0, //initial page index
    pageSize: 100, //default page size
  });

  const placeholderData = useMemo(() => {
    const count = Math.min(
      pagination.pageSize,
      numRows - pagination.pageIndex * pagination.pageSize
    );
    const arr = new Array(count);
    for (let i = 0; i < count; i++) {
      arr[i] = pagination.pageIndex * pagination.pageSize + i;
    }
    return arr;
  }, [pagination]);

  const table = useReactTable({
    data: placeholderData,
    columns,
    rowCount: numRows,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    onSortingChange: setSorting,
    getSortedRowModel: getSortedRowModel(),
    manualPagination: true,
    onPaginationChange: setPagination,
    state: {
      sorting,
      pagination,
    },
  });

  return (
    <div className="flex flex-col">
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header) => {
                  return (
                    <TableHead
                      key={header.id}
                      className="font-semibold text-left"
                    >
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
                    <TableCell key={cell.id} className="p-2">
                      {flexRender(
                        cell.column.columnDef.cell,
                        cell.getContext()
                      )}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell
                  colSpan={table.getAllColumns().length}
                  className="h-24 text-center"
                >
                  No results.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
      <div className="m-2">
        <DataTablePagination table={table} />
      </div>
    </div>
  );
}

function useDuckDb() {
  const _ = use(dbPromise);
  const { data: db, status } = useQuery({
    queryKey: ["duckdb"],
    queryFn: async () => {
      console.log("fetching data");
      const db = await dbPromise;
      let conn = await db.connect();
      await conn.query(
        `CREATE OR REPLACE TABLE gps AS SELECT * FROM '${window.location.origin}/api/export/gps.parquet';`
      );
      return conn;
    },
    staleTime: Infinity,
  });
  return db;
}

function RouteComponent() {
  const db = useDuckDb();
  const search = Route.useSearch();
  const sqlQuery = search.q ?? "SELECT COUNT(*) FROM gps";
  // const [activeQuery, setActiveQuery] = useState("");

  const [tableData, setTableData] = useState<arrow.Table | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isQuerying, setIsQuerying] = useState(false);
  async function doQuery(q) {
    if (isQuerying) return;
    setIsQuerying(true);
    setError(null);
    setTableData(null);

    try {
      const resp = await db?.query(q);
      setTableData(resp ?? null);
    } catch (e) {
      setError(e.message);
    } finally {
      setIsQuerying(false);
    }
  }

  useEffect(() => {
    if (!!db) {
      doQuery(sqlQuery);
    }
  }, [sqlQuery, !!db]);

  // const qc = useQueryClient();
  // const queryExec = useQuery({
  //   enabled: !!(db && activeQuery),
  //   queryKey: ["duckdb", "query", activeQuery],
  //   queryFn: async () => {
  //     console.log("query started");
  //     const resp = await db?.query(activeQuery);
  //     console.log("query ended");
  //     return resp;
  //   },
  //   staleTime: 1000,
  //   structuralSharing: false,
  //   retry: false,
  // });

  const [inputQuery, setInputQuery] = useState(sqlQuery);
  const [isExporting, setIsExporting] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);

  const navigate = useNavigate({ from: Route.fullPath });
  return (
    <div className="mx-auto container flex-col gap-4 py-4">
      {!db ? (
        <span>loading data... (may take a while)</span>
      ) : (
        <>
          <form
            className="flex flex-row gap-4"
            onSubmit={(e) => {
              e.preventDefault();
              navigate({
                search: (prev) => ({ ...prev, q: inputQuery }),
              });
            }}
            action=""
          >
            <Input
              className="flex-1"
              value={inputQuery}
              onChange={(e) => setInputQuery(e.target.value)}
            />
            <Button
              type="submit"
              className="cursor-pointer"
              disabled={!!isQuerying}
            >
              Query
            </Button>
          </form>

          {tableData !== null ? (
            <div className="mt-4">
              <ResultTable table={tableData!} />

              <div className="flex flex-row gap-2">
                <Button
                  disabled={isExporting}
                  className="cursor-pointer"
                  variant="secondary"
                  onClick={async (e) => {
                    setIsExporting(true);
                    setExportError(null);
                    try {
                      await exportData(db!, sqlQuery, "csv", "csv", "text/csv");
                    } catch (e) {
                      setExportError(e.message);
                    }
                    setIsExporting(false);
                  }}
                >
                  Export CSV
                </Button>

                <Button
                  disabled={isExporting}
                  className="cursor-pointer"
                  variant="secondary"
                  onClick={async (e) => {
                    setIsExporting(true);
                    setExportError(null);
                    try {
                      await exportData(
                        db!,
                        sqlQuery,
                        "json",
                        "json",
                        "application/json"
                      );
                    } catch (e) {
                      setExportError(e.message);
                    }
                    setIsExporting(false);
                  }}
                >
                  Export JSONL
                </Button>

                {exportError ? <div>{exportError}</div> : null}
              </div>
            </div>
          ) : null}
          {error !== null ? <pre className="mt-4">error: {error}</pre> : null}
          {!!isQuerying ? (
            <div className="mt-4 text-center">please imagine spinny...</div>
          ) : null}
        </>
      )}
    </div>
  );
}

async function exportData(
  conn: duckdb.AsyncDuckDBConnection,
  q: string,
  format: string,
  ext: string,
  mime: string
) {
  const token = Math.random().toString(36).substring(2);
  const filename = `result.${token}.${ext}`;

  const duck = await dbPromise; // lol?

  await conn.query(`COPY (${q}) TO '${filename}';`);
  // console.log(await duck!.globFiles("*"));
  const buf = await duck!.copyFileToBuffer(filename);
  await duck!.dropFiles();
  const url = URL.createObjectURL(
    new Blob([buf], {
      type: mime,
    })
  );
  const tempLink = document.createElement("a");
  tempLink.href = url;
  tempLink.setAttribute("download", filename);
  tempLink.click();
}
