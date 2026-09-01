import { DateType } from './_lib/types';
/**
 * Return Hijri object for the corresponding Gregorian date.
 * @throws {Error} when date is out of supported Gregorian range.
 */
export declare function gregorianToHijri(date: DateType): DateType;
