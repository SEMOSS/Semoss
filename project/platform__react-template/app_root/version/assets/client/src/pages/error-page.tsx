import { useInsight } from "@semoss/sdk/react";
import { CircleAlert } from "lucide-react";
import { isRouteErrorResponse, useRouteError } from "react-router";
import { Button } from "@/components/ui/button";

const describeError = (error: unknown): string | null => {
	if (isRouteErrorResponse(error)) {
		return `${error.status} ${error.statusText}`;
	}
	if (error instanceof Error) {
		return error.message;
	}
	return null;
};

/**
 * Shown when a page throws while rendering, or when SEMOSS fails to initialize.
 */
export const ErrorPage = () => {
	const routeError = useRouteError();
	const { error: insightError } = useInsight();
	const message = describeError(routeError ?? insightError);

	return (
		<div className="flex h-screen flex-col items-center justify-center gap-4 p-6 text-center">
			<CircleAlert className="size-8 text-destructive" aria-hidden="true" />
			<div className="space-y-1">
				<h1 className="text-lg font-semibold">Something went wrong</h1>
				<p className="max-w-md text-sm text-muted-foreground">
					Try reloading the page. Contact support if the problem
					persists.
				</p>
				{message && (
					<p className="max-w-md font-mono text-xs text-muted-foreground">
						{message}
					</p>
				)}
			</div>
			<Button variant="outline" onClick={() => window.location.reload()}>
				Reload
			</Button>
		</div>
	);
};
