import type { HijriDate } from "./types.js";
type GregorianDateParts = {
    year: number;
    month: number;
    day: number;
};
/** Returns Gregorian date parts while preserving years < 100. */
export declare function getGregorianDateParts(date: Date): GregorianDateParts;
/** Internal Gregorian lower bound supported by the Hijri converter. */
export declare const GREGORIAN_MIN_DATE: Date;
/** Internal Gregorian upper bound supported by the Hijri converter. */
export declare const GREGORIAN_MAX_DATE: Date;
/** Clamp a Gregorian date to the supported Hijri conversion range. */
export declare function clampGregorianDate(date: Date): Date;
/** Returns whether the provided Gregorian date was clamped. */
export declare function wasGregorianDateClamped(date: Date): boolean;
/** Clamp a Hijri date to the supported range and normalize month overflow. */
export declare function clampHijriDate(date: HijriDate): HijriDate;
/** Returns whether the provided Hijri date was clamped. */
export declare function wasHijriDateClamped(date: HijriDate): boolean;
export {};
