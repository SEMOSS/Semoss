import { MessageSnackbar, MessageSnackbarProps } from '@/components';
import { useLoadingState } from '@/hooks';
import { getSystemConfig, runPixel as runPixelSemossSdk } from '@semoss/sdk';
import { useInsight } from '@semoss/sdk-react';
import {
    createContext,
    Dispatch,
    PropsWithChildren,
    SetStateAction,
    useCallback,
    useContext,
    useEffect,
    useState,
} from 'react';

export interface AppContextType {
    runPixel: <T = unknown>(pixelString: string) => Promise<T>;
    login: (username: string, password: string) => Promise<boolean>;
    logout: () => Promise<boolean>;
    userLoginName: string;
    isAppDataLoading: boolean;
    onePlusTwo: number;
    setMessageSnackbarProps: Dispatch<SetStateAction<MessageSnackbarProps>>;
}

const AppContext = createContext<AppContextType | undefined>(undefined);

/**
 * Custom hook to get the stored app data and runPixel.
 *
 * @returns {AppContextType} - The data
 */
export const useAppContext = (): AppContextType => {
    const context = useContext(AppContext);
    if (!context) {
        throw new Error(
            'useAppContext must be used within an AppContextProvider',
        );
    }

    return context;
};

/**
 * Stores data accessible to the entire app. Must be used within an InsightProvider.
 *
 * @param {ReactNode} props.children The children who will have access to the app data.
 * @component
 */
export const AppContextProvider = ({ children }: PropsWithChildren) => {
    // Get the current state of the current insight
    const { actions, isReady, system, insightId } = useInsight();

    /**
     * State
     */
    const [isAppDataLoading, setIsAppDataLoading] = useLoadingState(true);
    const [userLoginName, setUserLoginName] = useState<string | null>(null);
    const [messageSnackbarProps, setMessageSnackbarProps] =
        useState<MessageSnackbarProps>({
            open: false,
            message: '',
            severity: 'info',
        });
    // Example state variable to store the result of a pixel operation
    const [onePlusTwo, setOnePlusTwo] = useState<number>();

    /**
     * Functions
     */

    // Function to run a pixel and return the result. Opens the snackbar if there is an error.
    const runPixel = useCallback(
        async <T,>(pixelString: string) => {
            try {
                const response = await runPixelSemossSdk<T[]>(
                    pixelString,
                    insightId,
                );
                if (response.errors.length > 0)
                    throw new Error(
                        response.errors
                            .map(
                                (
                                    error:
                                        | string
                                        | { message: string }
                                        | undefined,
                                ) =>
                                    (typeof error === 'string'
                                        ? error
                                        : error?.message) ??
                                    'Error during operation',
                            )
                            .join(', '),
                    );
                return response.pixelReturn[0].output;
            } catch (error) {
                setMessageSnackbarProps({
                    open: true,
                    message: `${error.message ?? 'Error during operation'}`,
                    severity: 'error',
                });
                throw error;
            }
        },
        [insightId],
    );

    // Allow users to log in, and grab their name when they do
    const login = useCallback(
        async (username: string, password: string) => {
            try {
                await actions.login({
                    type: 'native',
                    username,
                    password,
                });
                // Run a new config call, to get the name of the user
                const response = await getSystemConfig();
                setUserLoginName(
                    Object.values(response?.logins ?? {})?.[0]?.toString() ||
                        null,
                );
                return true;
            } catch {
                return false;
            }
        },
        [actions],
    );

    // Allow users to log out, and clear their name when they do
    const logout = useCallback(async () => {
        try {
            await actions.logout();
            setUserLoginName(null);
            return true;
        } catch {
            return false;
        }
    }, [actions]);

    /**
     * Effects
     */
    useEffect(() => {
        // Function to load app data
        const loadAppData = async () => {
            const loadingKey = setIsAppDataLoading(true);

            // Define a type for the loader and setter pairs
            // This allows us to load multiple pieces of data simultaneously and set them in state
            interface LoadSetPair<T> {
                loader: string;
                value?: T;
                setter?: (value: T) => void;
            }

            // Create an array of loadSetPairs, each containing a loader function and a setter function
            const loadSetPairs: LoadSetPair<unknown>[] = [
                {
                    loader: '1 + 2',
                    setter: (response) => setOnePlusTwo(response),
                } satisfies LoadSetPair<number>,
            ];

            // Execute all loaders in parallel and wait for them all to complete
            await Promise.all(
                loadSetPairs.map(
                    async (loadSetPair) =>
                        (loadSetPair.value = await runPixel(
                            loadSetPair.loader,
                        )),
                ),
            );

            // Once all loaders have completed, set the loading state to false
            // and call each setter with the loaded value
            setIsAppDataLoading(false, loadingKey, () =>
                loadSetPairs.forEach((loadSetPair) =>
                    loadSetPair.setter?.(loadSetPair.value),
                ),
            );
        };

        if (isReady) {
            // If the insight is ready, then load the app data
            loadAppData();
        }
    }, [isReady, runPixel]);

    // On start up, grab the name of the user from the config call if they are already logged in
    useEffect(() => {
        setUserLoginName(
            Object.values(system?.config?.logins ?? {})?.[0]?.toString() ||
                null,
        );
    }, [system]);

    return (
        <AppContext.Provider
            value={{
                runPixel,
                onePlusTwo,
                isAppDataLoading,
                setMessageSnackbarProps,
                login,
                logout,
                userLoginName,
            }}
        >
            {children}
            {/* The MessageSnackbar component is rendered here so that it can be used to display messages throughout the app */}
            <MessageSnackbar {...messageSnackbarProps} />
        </AppContext.Provider>
    );
};
