import { createRouter as createTanStackRouter } from "@tanstack/react-router";
import { routeTree } from "./routeTree.gen";
import {
  defaultShouldDehydrateQuery,
  dehydrate,
  hydrate,
  HydrationBoundary,
  QueryClient,
  QueryClientProvider,
} from "@tanstack/react-query";
import { createAsyncStoragePersister } from "@tanstack/query-async-storage-persister";
import { PersistQueryClientProvider } from "@tanstack/react-query-persist-client";
import { ColumnTable, table, fromArrow, fromArrowStream } from "arquero";
import base64js from "base64-js";

function Spinner() {
  return (
    <div className="w-full text-center py-4 animate-spin text-4xl">⚾</div>
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
          console.log(query.state.status, query.queryKey);
          return (
            defaultShouldDehydrateQuery(query) ||
            query.state.status === "pending"
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
          persistOptions={{ persister: asyncStoragePersister }}
        >
          {children}
        </PersistQueryClientProvider>
      );
    },
    defaultPendingComponent: Spinner,
    defaultPendingMinMs: 200,
    defaultPendingMs: 100,
  });

  return router;
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof createRouter>;
  }
}
