"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.setMonth = setMonth;
const conversion_js_1 = require("../utils/conversion.js");
const daysInMonth_js_1 = require("../utils/daysInMonth.js");
function setMonth(date, monthIndex) {
    const hijri = (0, conversion_js_1.toHijriDate)(date);
    // Handle overflow/underflow of monthIndex
    // Note: monthIndex argument is absolute month index for the year.
    // E.g. setMonth(..., 13) sets to Safar next year.
    let targetYear = hijri.year;
    let targetMonth = monthIndex;
    if (targetMonth > 11 || targetMonth < 0) {
        targetYear += Math.floor(targetMonth / 12);
        targetMonth = targetMonth % 12;
        if (targetMonth < 0) {
            targetMonth += 12;
        }
    }
    const daysInTargetMonth = (0, daysInMonth_js_1.getDaysInMonth)(targetYear, targetMonth);
    const day = Math.min(hijri.day, daysInTargetMonth);
    return (0, conversion_js_1.toGregorianDate)({
        year: targetYear,
        monthIndex: targetMonth,
        day,
    });
}
