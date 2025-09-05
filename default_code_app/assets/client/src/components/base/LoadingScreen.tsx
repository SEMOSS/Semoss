import { CircularProgress, Stack } from '@mui/material';

/**
 * Returns a loading screen with a centered circular progress indicator
 *
 * @component
 */
export const LoadingScreen = () => (
    <Stack height="100%" alignItems="center" justifyContent="center">
        <CircularProgress />
    </Stack>
);
