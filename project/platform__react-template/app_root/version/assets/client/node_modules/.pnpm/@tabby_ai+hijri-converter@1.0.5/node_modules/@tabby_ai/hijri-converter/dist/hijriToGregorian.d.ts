import { DateType } from './_lib/types';
/**
 * Return Gregorian object for the corresponding Hijri date.
 * @throws {Error} when date is out of supported Hijri range.
 */
export declare function hijriToGregorian(date: DateType): DateType;
