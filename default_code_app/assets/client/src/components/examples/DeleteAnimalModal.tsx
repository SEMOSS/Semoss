import { ConfirmationDialog } from '../library';
import { Animal } from './examples.types';
import { useSettingPixel } from '@/hooks';
import { Button } from '@mui/material';

export interface DeleteAnimalModalProps {
    open: boolean;
    animalToDelete: Animal | null;
    onClose: (changedAnimals: boolean) => void;
}

/**
 * Modal that allows animal deletion
 *
 * @component
 */
export const DeleteAnimalModal = ({
    animalToDelete,
    onClose,
    open,
}: DeleteAnimalModalProps) => {
    /**
     * State
     */
    const [runPixel, isLoading] = useSettingPixel();

    /**
     * Functions
     */
    const handleSubmitClick = async () => {
        runPixel(`DeleteAnimal(${animalToDelete.animal_id})`, () =>
            onClose(true),
        );
    };

    return (
        <ConfirmationDialog
            title={`Delete ${animalToDelete?.animal_name}?`}
            text={`This action will permanently delete ${animalToDelete?.animal_name} from the system.`}
            open={open}
            buttons={
                <>
                    <Button variant="contained" onClick={() => onClose(false)}>
                        Cancel
                    </Button>
                    <Button
                        color="error"
                        variant="outlined"
                        onClick={handleSubmitClick}
                        loading={isLoading}
                    >
                        {`Delete ${animalToDelete?.animal_name}`}
                    </Button>
                </>
            }
        />
    );
};
