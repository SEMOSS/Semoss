import { resolve } from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
	const env = loadEnv(mode, process.cwd(), "") as {
		ENDPOINT: string;
		MODULE: string;
		APP: string;
	};

	return {
		root: "src",
		base: "./",
		envDir: "../",
		resolve: {
			alias: {
				"@": resolve(__dirname, "./src"),
			},
		},
		define: {
			"import.meta.env.ENDPOINT": JSON.stringify(env.ENDPOINT),
			"import.meta.env.MODULE": JSON.stringify(env.MODULE),
			"import.meta.env.APP": JSON.stringify(env.APP),
		},
		server: {
			proxy: {
				[env.MODULE]: {
					target: env.ENDPOINT,
					changeOrigin: true,
					secure: false,
				},
			},
		},
		build: {
			outDir: "../../portals",
			emptyOutDir: true,
		},
		plugins: [react(), tailwindcss()],
	};
});
