import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const isDev = process.env.NODE_ENV !== "production";
const backendUrl = isDev
  ? "http://localhost:5000" // Local backend
  : "https://your-production-backend.com"; // Production backend URL

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "dist", // This is the default, but ensure it's specified.
  },
  assetsInclude: ["**/*.png", "**/*.jpg", "**/*.jpeg", "**/*.svg"],
  server: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin-allow-popups",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
    cors: {
      origin: backendUrl, // Flask backend URL
      credentials: true, // Allow cookies and JWTs
    },
  },
  define: {
    "import.meta.env.VITE_API_URL": JSON.stringify(backendUrl), // Expose API URL
  },
});
