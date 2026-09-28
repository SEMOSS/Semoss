import { useInsight } from "@semoss/sdk/react";
import { Navigate, Outlet, useLocation } from "react-router";

/**
 * Sends signed-out users to the login page, remembering where they were headed.
 */
export const AuthorizedLayout = () => {
	const { isAuthorized } = useInsight();
	const { pathname } = useLocation();

	if (!isAuthorized) {
		return <Navigate to="/login" replace state={{ target: pathname }} />;
	}

	return <Outlet />;
};
