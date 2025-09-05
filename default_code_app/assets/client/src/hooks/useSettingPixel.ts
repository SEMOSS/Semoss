import { useCallback } from 'react';
import { useLoadingState } from './useLoadingState';
import { useAppContext } from '@/contexts';

/**
 * Custom hook to call a reactor, typically used for calling a pixel on a click
 *
 * @template T The expected type of the pixel response.
 * @returns {[(pixelString: string, onSuccess?:  <T>(response: T) => Promise<void> | void, onError?: (error: Error) => Promise<void> | void) => void, boolean]} A function to call the pixel and a boolean indicating if the pixel is loading.
 */
export const useSettingPixel = (): [
    <T>(
        pixelString: string,
        onSuccess?: (response: T) => Promise<void> | void,
        onError?: (error: Error) => Promise<void> | void,
    ) => void,
    boolean,
] => {
    const { runPixel, setMessageSnackbarProps } = useAppContext();

    /**
     * State
     */
    const [isLoading, setIsLoading] = useLoadingState();

    /**
     * Functions
     */
    const fetchPixel = useCallback(
        <T>(
            pixelString: string,
            onSuccess?: (response: T) => Promise<void> | void,
            onError?: (error: Error) => Promise<void> | void,
        ) => {
            (async () => {
                const loadingKey = setIsLoading(true);
                try {
                    const response = await runPixel<T>(pixelString);
                    setIsLoading(false, loadingKey, () => {
                        onSuccess?.(response);
                        setMessageSnackbarProps({
                            open: true,
                            message: 'Success',
                            severity: 'success',
                        });
                    });
                } catch (error) {
                    setIsLoading(false, loadingKey, () => onError?.(error));
                }
            })();
        },
        [setIsLoading, runPixel],
    );

    return [fetchPixel, isLoading];
};
