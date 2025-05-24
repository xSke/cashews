"use client";

import { getBasicLeagues, getBasicTeams } from "@/data/data";
import { getQueryClient } from "@/get-query-client";
import {
  dehydrate,
  HydrationBoundary,
  QueryClientProvider,
  usePrefetchQuery,
} from "@tanstack/react-query";
import { ThemeProvider } from "next-themes";

export default function Providers({ children }: { children: React.ReactNode }) {
  const queryClient = getQueryClient();

  return (
    <ThemeProvider
      attribute="class"
      defaultTheme="light"
      disableTransitionOnChange
    >
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    </ThemeProvider>
  );
}
