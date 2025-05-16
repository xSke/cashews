import type { NextConfig } from "next";

const nextConfig: NextConfig = {
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
