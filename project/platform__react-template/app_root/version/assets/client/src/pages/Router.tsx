import { createHashRouter, Navigate } from "react-router";
import { RouterProvider } from "react-router/dom";
import { AuthorizedLayout } from "@/pages/authorized-layout";
import { ErrorPage } from "@/pages/error-page";
import { HomePage } from "@/pages/home-page";
import { LoginPage } from "@/pages/login-page";
import { RootLayout } from "@/pages/root-layout";

const router = createHashRouter([
    {
        // Wrap every route in InitializedLayout to ensure SEMOSS is ready to handle requests
        Component: RootLayout,
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
                path: "/login",
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

export const Router = () => {
    return <RouterProvider router={router} />;
};
