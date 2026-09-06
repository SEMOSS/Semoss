import {
	AlertDialog,
	AlertDialogContent,
	AlertDialogDescription,
	AlertDialogFooter,
	AlertDialogHeader,
	AlertDialogTitle,
} from "@/components/ui/alert-dialog";

export interface ConfirmationDialogProps {
	open: boolean;
	title: string;
	text: string;
	buttons: React.ReactNode;
}

/**
 * Reusable confirmation dialog component
 *
 * @component
 */
export const ConfirmationDialog = ({
	open,
	title,
	text,
	buttons,
}: ConfirmationDialogProps) => {
	return (
		<AlertDialog open={open}>
			<AlertDialogContent>
				<AlertDialogHeader>
					<AlertDialogTitle>{title}</AlertDialogTitle>
					<AlertDialogDescription>{text}</AlertDialogDescription>
				</AlertDialogHeader>
				<AlertDialogFooter>{buttons}</AlertDialogFooter>
			</AlertDialogContent>
		</AlertDialog>
	);
};
