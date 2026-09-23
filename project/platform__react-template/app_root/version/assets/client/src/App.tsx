import { Env, InsightProvider } from "@semoss/sdk/react";
import { Router } from "@/pages/router";

if (import.meta.env.MODE === "development") {
	Env.update({
		MODULE: import.meta.env.MODULE || "",
		ACCESS_KEY: import.meta.env.VITE_ACCESS_KEY || "",
		SECRET_KEY: import.meta.env.VITE_SECRET_KEY || "",
		APP: import.meta.env.APP || "",
	});
}


export const App = () => {
	return (
		// The InsightProvider starts a new Insight and sets the context to the current project. This components are imported from SEMOSS SDK
		<InsightProvider>
			<Router />
		</InsightProvider>
	);
};
