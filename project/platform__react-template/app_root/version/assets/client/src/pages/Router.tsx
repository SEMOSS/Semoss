import { createHashRouter, Navigate, RouterProvider } from "react-router-dom";
import { ROUTE_PATH_LOGIN_PAGE } from "@/routes.constants";
import { ErrorPage } from "./ErrorPage";
import { HomePage } from "./HomePage";
import { LoginPage } from "./LoginPage";
import { AuthorizedLayout, InitializedLayout } from "./layouts";

const router = createHashRouter([
	{
		// Wrap every route in InitializedLayout to ensure SEMOSS is ready to handle requests
		Component: InitializedLayout,
		// Catch errors in any of the initialized pages, to prevent the whole app from crashing
		ErrorBoundary: ErrorPage,
		children: [
			{
				// Wrap pages that should only be available to logged in users
				Component: AuthorizedLayout,
				// Also catch errors in any of the authorized pages, allowing the navigation to continue working
				ErrorBoundary: ErrorPage,
				children: [
					{
						// If the path is empty, use the home page
						index: true,
						Component: HomePage,
					},
					// {
					//     // Example of a new page
					//     path: '/new-page',
					//     Component: NewPage,
					// }
				],
			},
			{
				// The login page should be available to non-logged in users (duh)
				path: ROUTE_PATH_LOGIN_PAGE,
				Component: LoginPage,
			},
			{
				// Any other urls should be sent to the home page
				path: "*",
				Component: () => <Navigate to="/" />,
			},
		],
	},
]);

/**
 * Renders pages based on url.
 *
 * @component
 */
export const Router = () => {
	return <RouterProvider router={router} />;
};
