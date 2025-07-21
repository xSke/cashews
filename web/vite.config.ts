import { defineConfig, loadEnv } from "vite";
import tsConfigPaths from "vite-tsconfig-paths";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");

  // change this in `.env`
  const API_BASE = env.API_BASE ?? "https://freecashe.ws/api";

  return {
    server: {
      port: 3000,
      proxy: {
        "/api": {
          target: API_BASE,
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api/, ""),
        },
      },
    },
    build: {
      sourcemap: true,
    },
    plugins: [tsConfigPaths(), tanstackStart({})],
  };
});
