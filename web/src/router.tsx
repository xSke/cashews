import {
  createRouter as createTanStackRouter,
  ErrorComponentProps,
} from "@tanstack/react-router";
import { routeTree } from "./routeTree.gen";
import {
  defaultShouldDehydrateQuery,
  dehydrate,
  hydrate,
  QueryClient,
} from "@tanstack/react-query";
import { createAsyncStoragePersister } from "@tanstack/query-async-storage-persister";
import { PersistQueryClientProvider } from "@tanstack/react-query-persist-client";
import { ColumnTable, fromArrow } from "arquero";
import base64js from "base64-js";

function Spinner() {
  return (
    <div className="w-full text-center py-4 animate-spin text-4xl">⚾</div>
  );
}

function Error(props: ErrorComponentProps) {
  return (
    <div className="rounded-md bg-red-800/20 border-2 border-red-800/50 p-4 text-sm">
      <pre className="font-semibold mb-1">
        Error: {props.error.message?.toString()}
      </pre>
      <pre>{props.error.stack}</pre>
    </div>
  );
}

export const asyncStoragePersister = createAsyncStoragePersister({
  storage: import.meta.env.SSR ? undefined : window.localStorage,
});

export function createRouter() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 60 * 5 * 1000,
        gcTime: 1000 * 60 * 60 * 24,
      },
      dehydrate: {
        shouldDehydrateQuery: (query) => {
          return (
            defaultShouldDehydrateQuery(query) && query.queryKey[0] !== "duckdb"
          );
        },

        serializeData: (x) => {
          if (typeof x == "object" && x.__proto__ == ColumnTable.prototype) {
            let y = x as ColumnTable;
            return { __table: base64js.fromByteArray(y.toArrowIPC()) };
          }

          return x;
        },
      },
      hydrate: {
        deserializeData: (x) => {
          if (typeof x == "object" && x.__table) {
            const data = base64js.toByteArray(x.__table);
            const table = fromArrow(data);
            return table;
          }

          return x;
        },
      },
    },
  });

  const router = createTanStackRouter({
    routeTree,
    scrollRestoration: true,
    context: {
      queryClient,
    },
    dehydrate: () => {
      return {
        queryClientState: dehydrate(queryClient),
      };
    },
    hydrate: (dehydrated) => {
      hydrate(queryClient, dehydrated.queryClientState);
    },
    Wrap: ({ children }) => {
      return (
        <PersistQueryClientProvider
          client={queryClient}
          persistOptions={{
            persister: asyncStoragePersister,
            dehydrateOptions: {
              shouldDehydrateQuery: (query) => {
                return (
                  defaultShouldDehydrateQuery(query) &&
                  query.queryKey[0] !== "duckdb"
                );
              },
            },
          }}
        >
          {children}
        </PersistQueryClientProvider>
      );
    },
    defaultPendingComponent: Spinner,
    defaultPendingMinMs: 200,
    defaultPendingMs: 100,
    defaultErrorComponent: Error,
  });

  return router;
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof createRouter>;
  }
}
