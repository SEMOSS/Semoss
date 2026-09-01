import type { HijriDate } from "./types.js";
/** Convert a Gregorian date to a Hijri date. */
export declare function toHijriDate(date: Date): HijriDate;
/** Convert a Hijri date back to the Gregorian calendar. */
export declare function toGregorianDate(hijri: HijriDate): Date;
