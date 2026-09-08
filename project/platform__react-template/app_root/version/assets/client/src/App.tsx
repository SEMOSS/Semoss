import { Env } from "@semoss/sdk";
import { InsightProvider } from "@semoss/sdk/react";
import { Toaster } from "sonner";
import { AppContextProvider } from "./contexts";
import { Router } from "./pages";

Env.update({
	MODULE: import.meta.env.MODULE || "",
	ACCESS_KEY: import.meta.env.VITE_ACCESS_KEY || "", // undefined in production
	SECRET_KEY: import.meta.env.VITE_SECRET_KEY || "", // undefined in production
	APP: import.meta.env.APP || "",
});

/**
 * Renders the SEMOSS React app.
 *
 * @component
 */
export const App = () => {
	return (
		// The InsightProvider starts a new Insight and sets the context to the current project. This components are imported from SEMOSS SDK
		<InsightProvider>
			{/* The AppContextProvider stores data specific to the current app, and runPixel.
			This component is custom to this project, and can be edited in AppContext.tsx */}
			<AppContextProvider>
				{/* The Router decides which page to render based on the url.
					This component is custom to this project, and can be edited in Router.tsx */}
				<Router />
			</AppContextProvider>

			{/* Toaster for displaying toast notifications */}
			<Toaster />
		</InsightProvider>
	);
};
