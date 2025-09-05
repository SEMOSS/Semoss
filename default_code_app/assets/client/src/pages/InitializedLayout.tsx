import { LoadingScreen, MainNavigation } from '@/components';
import { Stack } from '@mui/material';
import { useInsight } from '@semoss/sdk-react';
import { Outlet } from 'react-router';

/**
 * Renders a loading wheel if SEMOSS is not initialized.
 *
 * @component
 */
export const InitializedLayout = () => {
    const { isInitialized } = useInsight();

    return (
        <Stack height="100vh">
            {/* Allow users to navigate around the app */}
            <MainNavigation />

            {isInitialized ? (
                // If initialized, set up padding and scroll
                <Stack padding={2} overflow="auto" height="100%">
                    {/* Outlet is a react router component; it allows the router to choose the child based on the route */}
                    <Outlet />
                </Stack>
            ) : (
                // Otherwise, show a centered loading wheel
                <LoadingScreen />
            )}
        </Stack>
    );
};
