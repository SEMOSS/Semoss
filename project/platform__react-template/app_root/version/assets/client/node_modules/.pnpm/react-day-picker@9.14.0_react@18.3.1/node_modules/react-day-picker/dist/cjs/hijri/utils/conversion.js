"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.toHijriDate = toHijriDate;
exports.toGregorianDate = toGregorianDate;
const hijri_converter_1 = require("@tabby_ai/hijri-converter");
const range_js_1 = require("./range.js");
/** Convert a Gregorian date to a Hijri date. */
function toHijriDate(date) {
    const clamped = (0, range_js_1.clampGregorianDate)(date);
    const { year, month, day } = (0, range_js_1.getGregorianDateParts)(clamped);
    // gregorianToHijri uses 1-indexed months
    const hijri = (0, hijri_converter_1.gregorianToHijri)({ year, month, day });
    return {
        year: hijri.year,
        monthIndex: hijri.month - 1, // Convert to 0-indexed
        day: hijri.day,
    };
}
/** Convert a Hijri date back to the Gregorian calendar. */
function toGregorianDate(hijri) {
    const clamped = (0, range_js_1.clampHijriDate)(hijri);
    // hijriToGregorian expects 1-indexed months. Probe down from the candidate
    // day to handle invalid month/day combinations without throwing.
    for (let day = clamped.day; day >= 1; day -= 1) {
        try {
            const gregorian = (0, hijri_converter_1.hijriToGregorian)({
                year: clamped.year,
                month: clamped.monthIndex + 1,
                day,
            });
            return (0, range_js_1.clampGregorianDate)(new Date(gregorian.year, gregorian.month - 1, gregorian.day));
        }
        catch {
            // Try a lower day for months that only have 29 days.
        }
    }
    // Fallback to the minimum supported Gregorian date if conversion probing
    // somehow fails for all days.
    return new Date(range_js_1.GREGORIAN_MIN_DATE);
}
