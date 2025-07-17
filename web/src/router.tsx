import { createRouter as createTanStackRouter } from "@tanstack/react-router";
import { routeTree } from "./routeTree.gen";
import {
  dehydrate,
  hydrate,
  HydrationBoundary,
  QueryClient,
  QueryClientProvider,
} from "@tanstack/react-query";
import { createAsyncStoragePersister } from "@tanstack/query-async-storage-persister";
import { PersistQueryClientProvider } from "@tanstack/react-query-persist-client";

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
    },
  });

  const router = createTanStackRouter({
    routeTree,
    scrollRestoration: true,
    context: {
      queryClient,
    },
    dehydrate: () => {
      console.log("dehydrating", queryClient.getQueryCache().getAll());
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
