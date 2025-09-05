import { useCallback, useEffect, useState } from 'react';
import { useLoadingState } from './useLoadingState';
import { useAppContext } from '@/contexts';

/**
 * Custom hook to call a reactor, typically for fetching data on page load.
 *
 * @template T The expected type of the pixel response.
 * @param {string} [pixelString] The pixel to call.
 * @param {T} [initialValue] The initial value of the state, before being overwritten by the pixel return.
 * @returns {[T, boolean, () => void]} The pixel return, whether the call is loading, and a function to re-fetch.
 */
export const useLoadingPixel = <T>(
    pixelString: string,
    initialValue?: T,
): [T, boolean, () => void] => {
    const { runPixel } = useAppContext();

    /**
     * State
     */
    const [isLoading, setIsLoading] = useLoadingState();
    const [response, setResponse] = useState<T>(initialValue);

    /**
     * Functions
     */
    const fetchPixel = useCallback(() => {
        (async () => {
            const loadingKey = setIsLoading(true);
            try {
                const response = await runPixel<T>(pixelString);
                setIsLoading(false, loadingKey, () => setResponse(response));
            } catch {
                setIsLoading(false, loadingKey);
            }
        })();
    }, [pixelString, setIsLoading, runPixel]);

    /**
     * Effects
     */
    useEffect(() => {
        // Call the reactor on loadup
        fetchPixel();
    }, [fetchPixel]);

    return [response, isLoading, fetchPixel];
};
