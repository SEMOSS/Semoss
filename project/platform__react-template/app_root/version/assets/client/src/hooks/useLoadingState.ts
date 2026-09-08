import { useCallback, useRef, useState } from "react";

type toggleLoadingType = ((loading?: true) => number) &
	((loading: false, loadingKey: number, ifCurrent?: () => void) => void);

/**
 * Custom hook to disregard outdated calls.
 *
 * @param {boolean} [initialValue=false] Initial value.
 * @returns {[boolean, toggleLoadingType, () => void]} Current state and a function to toggle it.
 */
export const useLoadingState = (
	initialValue: boolean = false,
): [boolean, toggleLoadingType, () => void] => {
	/**
	 * State / Refs
	 */
	const [isLoading, setIsLoading] = useState<boolean>(initialValue);
	const loadingStateRef = useRef<number>(0); // Stores the id of the most recent call

	/**
	 * Functions
	 */
	const toggleLoading = useCallback(
		(
			loading: boolean = true,
			key?: number,
			ifCurrent: () => void = () => null,
		) => {
			if (loading) {
				// Allow the caller to set the state to loading, and increment the id
				setIsLoading(true);
				return ++loadingStateRef.current;
			} else if (loadingStateRef.current === key) {
				// If the caller gives the id of the most recent call, then resolve the loading state
				ifCurrent(); // Perform any caller-specified function (typically setting state)
				setIsLoading(false);
			}
		},
		[],
	) as toggleLoadingType;

	const clearLoading = useCallback(() => {
		setIsLoading(false);
	}, []);

	return [isLoading, toggleLoading, clearLoading] as const;
};
