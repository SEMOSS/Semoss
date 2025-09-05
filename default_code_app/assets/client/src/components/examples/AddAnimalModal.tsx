import { useSettingPixel } from '@/hooks';
import {
    Button,
    Dialog,
    DialogActions,
    DialogContent,
    DialogTitle,
    IconButton,
    Stack,
    TextField,
    Typography,
} from '@mui/material';
import { useState } from 'react';
import { DatePicker } from '../library';
import { CloseRounded } from '@mui/icons-material';

export interface AddAnimalModalProps {
    open: boolean;
    onClose: (addedAnimal: boolean) => void;
}

/**
 * Renders a modal to add an animal.
 *
 * @component
 */
export const AddAnimalModal = ({ open, onClose }: AddAnimalModalProps) => {
    const [addAnimal, isLoading] = useSettingPixel();

    /**
     * State
     */
    const [animalName, setAnimalName] = useState<string>('');
    const [animalType, setAnimalType] = useState<string>('');
    const [dateOfBirth, setDateOfBirth] = useState<string | null>(null);

    /**
     * Functions
     */
    const handleSubmitClick = async () => {
        addAnimal(
            `AddAnimal(animal_name=${JSON.stringify(animalName)}, animal_type=${JSON.stringify(animalType)}, date_of_birth=${JSON.stringify(dateOfBirth)})`,
            () => handleClose(true),
        );
    };

    const handleClose = (madeChanges?: boolean) => {
        setAnimalName('');
        setAnimalType('');
        setDateOfBirth(null);
        onClose(madeChanges ?? false);
    };

    /**
     * Constants
     */
    const isReadyToSubmit =
        animalName.trim().length > 0 &&
        animalType.trim().length > 0 &&
        dateOfBirth !== null;

    return (
        <Dialog open={open} fullWidth maxWidth="sm">
            <DialogTitle>
                <Stack
                    direction="row"
                    alignItems="center"
                    justifyContent="space-between"
                >
                    <Typography variant="h6">Add Animal</Typography>
                    <IconButton onClick={() => handleClose(false)}>
                        <CloseRounded />
                    </IconButton>
                </Stack>
            </DialogTitle>

            <DialogContent>
                <Stack spacing={2}>
                    {/* div to prevent title clipping */}
                    <div />

                    <TextField
                        value={animalName}
                        onChange={(e) => setAnimalName(e.target.value)}
                        label="Name"
                    />

                    <TextField
                        value={animalType}
                        onChange={(e) => setAnimalType(e.target.value)}
                        label="Type"
                    />

                    <DatePicker
                        value={dateOfBirth}
                        onChange={(newValue) => setDateOfBirth(newValue)}
                        label="Date of birth"
                        maxDate={new Date()} // Prevent future dates
                    />
                </Stack>
            </DialogContent>

            <DialogActions>
                <Button
                    onClick={handleSubmitClick}
                    variant="contained"
                    loading={isLoading}
                    disabled={!isReadyToSubmit}
                >
                    Add animal
                </Button>
            </DialogActions>
        </Dialog>
    );
};
