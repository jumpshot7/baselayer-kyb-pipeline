import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  reactCompiler: true,
  env: {
    NEXT_PUBLIC_API_URL: 'https://laughing-space-sniffle-wjx466vrj56cw5g-8080.app.github.dev',
  }
};

export default nextConfig;
