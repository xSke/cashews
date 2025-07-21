import { Button } from "@/components/ui/button";
import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";

import type * as duckdb from "@duckdb/duckdb-wasm";
import duckdb_wasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvp_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdb_wasm_eh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import eh_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import duckdb_wasm_coi from "@duckdb/duckdb-wasm/dist/duckdb-coi.wasm?url";
import coi_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-coi.worker.js?url";
import coi_worker_pt from "@duckdb/duckdb-wasm/dist/duckdb-browser-coi.pthread.worker.js?url";

export const Route = createFileRoute("/ducktest")({
  component: RouteComponent,
  ssr: false,
});

function RouteComponent() {
  const [dbLoadStatus, setDbLoadStatus] = useState("");
  const [dataLoadStatus, setDataLoadStatus] = useState("");
  const [hotReloadStatus, setHotReloadStatus] = useState("");
  const [queryStatus, setQueryStatus] = useState("");

  const [db, setDb] = useState<duckdb.AsyncDuckDB | null>(null);

  async function loadDuckDb() {
    setDbLoadStatus("loading");
    console.log("importing...");
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
      coi: {
        mainModule: duckdb_wasm_coi,
        mainWorker: coi_worker,
        pthreadWorker: coi_worker_pt,
      },
    };
    // Select a bundle based on browser checks
    console.log("selecting bundle");
    const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);
    // Instantiate the asynchronus version of DuckDB-wasm
    console.log("init worker");
    const worker = new Worker(bundle.mainWorker!);
    const logger = new duckdb.ConsoleLogger();
    console.log("new db");

    const beforeCreate = performance.now();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    console.log("instantiate db");
    const beforeInst = performance.now();
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    console.log("open db");
    const beforeOpen = performance.now();
    await db.open({
      path: ":memory:",
      accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
    });
    const conn = await db.connect();
    await conn.query("INSTALL parquet;LOAD parquet;");
    await conn.close();
    const after = performance.now();
    console.log("done");

    setDbLoadStatus(
      `new: ${beforeInst - beforeCreate}ms, instantiate: ${beforeOpen - beforeInst}ms, open: ${after - beforeOpen}ms`
    );
    setDb(db);
  }

  async function loadDataset() {
    setDataLoadStatus("loading");
    const conn = await db!.connect();

    const filename = "gps.zstd.parquet";
    const beforeDirect = performance.now();
    // await conn.query(
    //   `CREATE OR REPLACE TABLE gps AS SELECT * FROM '${window.location.origin}/api/export/${filename}';`
    // );
    const afterDirect = performance.now();

    const beforeFetch = performance.now();
    const dataResp = await fetch(`/api/export/${filename}`);
    const buffer = await dataResp.arrayBuffer();
    const beforeRegister = performance.now();

    console.log(buffer.slice(0, 100));
    await db!.registerFileBuffer("gps.zstd.parquet", new Uint8Array(buffer));
    const beforeLoad = performance.now();
    await conn!.query(
      "CREATE OR REPLACE TABLE gps AS FROM read_parquet('gps.zstd.parquet');"
    );
    const after = performance.now();
    setDataLoadStatus(
      `load direct: ${afterDirect - beforeDirect}ms, fetch: ${beforeRegister - beforeFetch}ms, reg: ${beforeLoad - beforeRegister}ms, load buffer: ${after - beforeLoad}ms`
    );
  }

  async function hotReload() {
    // await db!.terminate();

    const duckdb = await import("@duckdb/duckdb-wasm");
    await db!.open({
      path: "opfs://some_other_db.db",
      accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
    });

    const before = performance.now();
    await db!.open({
      path: "opfs://cashews.db",
      accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
    });

    const conn = await db?.connect();
    await conn!.query("select * from gps limit 1;");
    const after = performance.now();

    setHotReloadStatus(`hot reload: ${after - before}ms`);
  }

  async function testQuery() {
    setQueryStatus("loading");

    const conn = await db!.connect();

    const before = performance.now();
    const resp = await conn.query(
      "select player_id, any_value(player_name) as player_name, ((sum(singles) + sum(doubles) + sum(triples) + sum(home_runs) + sum(walked) + sum(hit_by_pitch)) / sum(plate_appearances)) + ((sum(singles) + 2*sum(doubles) + 3*sum(triples) + 4*sum(home_runs)) / sum(at_bats)) as ops from gps  where season = 3 group by player_id having sum(plate_appearances) > 100 order by ops desc limit 5"
    );
    const after = performance.now();

    const names = resp
      .toArray()
      .map((x) => x["player_name"])
      .join(", ");

    setQueryStatus(`ops leaders: ${after - before}ms (result: ${names})`);
  }

  return (
    <div className="container mx-auto py-4 flex flex-col gap-2">
      <div className="flex flex-row gap-4">
        <Button
          variant="secondary"
          disabled={dbLoadStatus === "loading"}
          onClick={(e) => {
            loadDuckDb();
          }}
        >
          Load DuckDB
        </Button>
        <Button
          variant="secondary"
          disabled={db === null || dataLoadStatus === "loading"}
          onClick={(e) => loadDataset()}
        >
          Load dataset (~50MB)
        </Button>

        <Button
          variant="secondary"
          disabled={
            db === null ||
            // dataLoadStatus === "loading" ||
            hotReloadStatus === "loading"
          }
          onClick={(e) => hotReload()}
        >
          Test hot reload
        </Button>

        <Button
          variant="secondary"
          disabled={
            db === null ||
            // dataLoadStatus === "loadi ng" ||
            queryStatus === "loading"
          }
          onClick={(e) => testQuery()}
        >
          Test query
        </Button>
      </div>
      <pre>{dbLoadStatus}</pre>
      <pre>{dataLoadStatus}</pre>
      <pre>{hotReloadStatus}</pre>
      <pre>{queryStatus}</pre>
    </div>
  );
}
