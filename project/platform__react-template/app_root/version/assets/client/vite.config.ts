import path from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
	const env = loadEnv(mode, path.resolve(__dirname, ".."), "") as {
		ENDPOINT: string;
		MODULE: string;
		APP: string;
		ACCESS_KEY: string;
		SECRET_KEY: string;
	};

	return {
		plugins: [react(), tailwindcss()],
		resolve: {
			alias: {
				"@": path.resolve(__dirname, "./src"),
			},
		},
		define:
			mode === "development"
				? {
						"import.meta.env.MODULE": JSON.stringify(env.MODULE),
						"import.meta.env.APP": JSON.stringify(env.APP),
						"import.meta.env.ACCESS_KEY": JSON.stringify(env.ACCESS_KEY || ""),
						"import.meta.env.SECRET_KEY": JSON.stringify(env.SECRET_KEY || ""),
					}
				: {},
		server: {
			// Proxies /Monolith requests to the SEMOSS backend during local dev
			proxy: {
				[env.MODULE]: {
					target: env.ENDPOINT,
					changeOrigin: true,
					secure: false,
				},
			},
		},
		build: {
			// Build output goes to portals/, which SEMOSS serves when the app is published
			outDir: "../portals",
			emptyOutDir: true,
		},
	};
});
