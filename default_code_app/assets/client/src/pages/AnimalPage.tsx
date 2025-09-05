import {
    AddAnimalModal,
    Animal,
    AnimalList,
    DeleteAnimalModal,
} from '@/components';
import { useLoadingPixel } from '@/hooks';
import { Button, Stack, Typography } from '@mui/material';
import { useState } from 'react';

/**
 * Renders a page for the animal example.
 *
 * @component
 */
export const AnimalPage = () => {
    /**
     * State
     */
    const [animalList, isAnimalListLoading, fetchAnimalList] = useLoadingPixel<
        Animal[]
    >('GetAnimals( )', []);
    const [isAddAnimalModalOpen, setIsAddAnimalModalOpen] =
        useState<boolean>(false);
    const [isDeleteAnimalModalOpen, setIsDeleteAnimalModalOpen] =
        useState<boolean>(false);
    const [animalToDelete, setAnimalToDelete] = useState<Animal | null>(null);

    /**
     * Functions
     */
    const handleModalClose = (changedAnimals: boolean) => {
        setIsAddAnimalModalOpen(false);
        setIsDeleteAnimalModalOpen(false);
        if (changedAnimals) {
            fetchAnimalList();
        }
    };

    return (
        <Stack spacing={2}>
            <Stack
                direction="row"
                alignItems="center"
                justifyContent="space-between"
            >
                <Typography variant="h4">Animals</Typography>
                <Button
                    onClick={() => setIsAddAnimalModalOpen(true)}
                    variant="contained"
                >
                    Add animal
                </Button>
            </Stack>

            <AnimalList
                animalList={animalList ?? []}
                loading={isAnimalListLoading}
                onDelete={(animalToDelete) => {
                    setIsDeleteAnimalModalOpen(true);
                    setAnimalToDelete(animalToDelete);
                }}
            />

            <AddAnimalModal
                open={isAddAnimalModalOpen}
                onClose={handleModalClose}
            />

            <DeleteAnimalModal
                open={isDeleteAnimalModalOpen}
                animalToDelete={animalToDelete}
                onClose={handleModalClose}
            />
        </Stack>
    );
};
