import { useInsight } from "@semoss/sdk/react";
import { Outlet } from "react-router";
import { Spinner } from "@/components/ui/spinner";
import { ErrorPage } from "@/pages/error-page";

/**
 * Holds every route until SEMOSS has initialized, so pages can run Pixel safely.
 */
export const RootLayout = () => {
	const { isInitialized, error } = useInsight();

	if (error) {
		// Initialization failed; there is no insight to run against
		return <ErrorPage />;
	}

	if (!isInitialized) {
		return (
			<div className="flex h-screen items-center justify-center">
				<Spinner className="size-8" />
			</div>
		);
	}

	return (
		<div className="flex h-screen flex-col overflow-auto">
			{/* Outlet renders the child route that matches the url */}
			<Outlet />
		</div>
	);
};
