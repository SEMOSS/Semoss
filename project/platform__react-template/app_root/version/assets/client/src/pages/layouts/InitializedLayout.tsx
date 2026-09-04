import { useInsight } from "@semoss/sdk/react";
import { Outlet } from "react-router-dom";
import { LoadingScreen } from "@/components";
import { ErrorPage } from "../ErrorPage";

/**
 * Renders a loading wheel if SEMOSS is not initialized.
 *
 * @component
 */
export const InitializedLayout = () => {
  /**
   * Library hooks
   */
  const { isInitialized, error } = useInsight();

  return (
    <div className="flex flex-col h-screen">
      {/* Allow users to navigate around the app */}

      {isInitialized ? (
        // If initialized, set up padding and scroll
        <div className="p-4 overflow-auto h-full">
          {/* Outlet is a react router component; it allows the router to choose the child based on the route */}
          <Outlet />
        </div>
      ) : error ? (
        // If there was an error during initialization, show it
        <ErrorPage />
      ) : (
        // Otherwise, show a centered loading wheel
        <LoadingScreen />
      )}
    </div>
  );
};
