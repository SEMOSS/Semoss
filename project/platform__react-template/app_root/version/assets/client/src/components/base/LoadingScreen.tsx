import { Spinner } from "@/components/ui/spinner";

interface LoadingScreenProps {
	/** Whether to overlay the loading screen on top of existing content */
	overlay?: boolean;
}

/**
 * Returns a loading screen with a centered circular progress indicator
 *
 * @component
 */
export const LoadingScreen = ({ overlay = false }: LoadingScreenProps) => (
	<div
		className={`flex items-center justify-center h-full ${
			overlay ? "absolute inset-0 bg-background/80 z-50" : ""
		}`}
	>
		<Spinner className="size-8" />
	</div>
);
