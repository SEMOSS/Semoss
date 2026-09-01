/**
 * Umm al-Qura calendar constants.
 */
export type DateTuple = [number, number, number];
/**
 * Inclusive range of supported Gregorian dates (year, month and day).
 */
export declare const GREGORIAN_RANGE: [DateTuple, DateTuple];
/**
 * Inclusive range of supported Hijri dates (year, month and day).
 */
export declare const HIJRI_RANGE: [DateTuple, DateTuple];
/**
 * Total Hijri months elapsed before the beginning of Hijri range.
 */
export declare const HIJRI_OFFSET: number;
/**
 * Ordered list of Reduced Julian Day (RJD) numbers for the beginning of
 * supported Hijri months.
 */
export declare const MONTH_STARTS: number[];
