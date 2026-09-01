import { DateType } from './types';
/**
 * Return number of days in month.
 */
export declare function hijriDaysInMonth(year: number, month: number): number;
/**
 * Return corresponding Julian day number (JDN) from Hijri date.
 */
export declare function hijriToJulian(date: DateType): number;
/**
 * Return corresponding Julian day number (JDN) from Gregorian date.
 */
export declare function gregorianToJulian(date: DateType): number;
/**
 * Construct a date from a proleptic Gregorian ordinal.
 *  January 1 of year 1 is day 1. Only the year, month and day are
 *  non-zero in the result.
 */
export declare function gregorianFromOrdinal(n: number): DateType;
