import { useAppContext } from '@/contexts';
import { Alert, Snackbar, SnackbarCloseReason } from '@mui/material';

export interface MessageSnackbarProps {
    message: string;
    severity: 'success' | 'error' | 'info' | 'warning';
    open: boolean;
}

/**
 * Renders a snackbar for displaying messages, typically used for error or success notifications
 *
 * @component
 */
export const MessageSnackbar = ({
    open,
    severity,
    message,
}: MessageSnackbarProps) => {
    const { setMessageSnackbarProps } = useAppContext();

    /**
     * Functions
     */
    const handleClose = (
        _: React.SyntheticEvent | Event,
        reason?: SnackbarCloseReason,
    ) => {
        if (reason === 'clickaway') return;
        setMessageSnackbarProps((prev) => ({
            ...prev,
            open: false,
        }));
    };

    return (
        <Snackbar
            open={open}
            onClose={handleClose}
            anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
        >
            <Alert severity={severity} onClose={handleClose}>
                {message}
            </Alert>
        </Snackbar>
    );
};
