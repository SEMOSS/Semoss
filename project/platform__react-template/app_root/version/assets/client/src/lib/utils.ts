import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

// Utility function to merge class names conditionally
export const cn = (...inputs: ClassValue[]) => {
	return twMerge(clsx(inputs));
};
