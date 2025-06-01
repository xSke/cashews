import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  async rewrites() {
    return [
      {
        source: "/api",
        destination: "http://localhost:3001/",
      },
    ];
  },
};

export default nextConfig;
