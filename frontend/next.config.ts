import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactCompiler: true,
  async rewrites() {
    // In production (Vercel), process.env.NEXT_PUBLIC_API_URL will be your Railway URL.
    // In local development, it defaults to http://api:8080 or localhost.
    const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://api:8080';
    return [
      {
        source: '/api/:path*',
        destination: `${apiUrl}/:path*`,
      },
    ]
  },
};

export default nextConfig;