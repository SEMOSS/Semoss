import {
    Dialog,
    DialogActions,
    DialogContent,
    DialogTitle,
} from '@mui/material';
import { ReactNode } from 'react';

export interface ConfirmationDialogProps {
    open: boolean;
    title: string;
    buttons?: ReactNode;
    text?: string;
}

/**
 * Renders a dialog with standard confirmation options.
 *
 * @component
 */
export const ConfirmationDialog = ({
    open,
    buttons,
    title,
    text,
}: ConfirmationDialogProps) => {
    return (
        <Dialog open={open} fullWidth maxWidth="sm">
            <DialogTitle>{title}</DialogTitle>
            {text && <DialogContent>{text}</DialogContent>}
            <DialogActions>{buttons}</DialogActions>
        </Dialog>
    );
};
