"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.checkGregorianRange = exports.checkHijriRange = void 0;
const ummalqura_1 = require("./ummalqura");
const isoFormat_1 = require("./isoFormat");
const dateHelpersHijri_1 = require("./dateHelpersHijri");
/**
 * Check date values if within valid range.
 */
function checkHijriRange(date) {
    // check year
    const [[minYear], [maxYear]] = ummalqura_1.HIJRI_RANGE;
    if (date.year < minYear || maxYear < date.year) {
        throw Error('date out of range');
    }
    // check month
    const maxMonths = 12;
    if (date.month < 1 || maxMonths < date.month) {
        throw Error(`month must be in 1..${maxMonths}`);
    }
    // check day
    const monthLen = (0, dateHelpersHijri_1.hijriDaysInMonth)(date.year, date.month);
    if (date.day < 1 || monthLen < date.day) {
        throw Error(`day must be in 1..${monthLen}`);
    }
}
exports.checkHijriRange = checkHijriRange;
/**
 * Check if Gregorian date is within valid range.
 */
function checkGregorianRange(date) {
    const [minDate, maxDate] = ummalqura_1.GREGORIAN_RANGE;
    const isoCurrentDate = (0, isoFormat_1.isoFormat)(date.year, date.month, date.day);
    if ((0, isoFormat_1.isoFormat)(...minDate) > isoCurrentDate ||
        (0, isoFormat_1.isoFormat)(...maxDate) < isoCurrentDate) {
        throw Error('date out of range');
    }
}
exports.checkGregorianRange = checkGregorianRange;
