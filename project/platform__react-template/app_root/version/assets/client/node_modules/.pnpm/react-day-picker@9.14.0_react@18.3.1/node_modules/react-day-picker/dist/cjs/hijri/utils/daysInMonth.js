"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDaysInMonth = getDaysInMonth;
const conversion_js_1 = require("./conversion.js");
const range_js_1 = require("./range.js");
const MAX_DAY_IN_HIJRI_MONTH = 30;
const MIN_DAY_IN_HIJRI_MONTH = 29;
function getDaysInMonth(year, monthIndex) {
    const clamped = (0, range_js_1.clampHijriDate)({ year, monthIndex, day: 1 });
    for (let day = MAX_DAY_IN_HIJRI_MONTH; day >= MIN_DAY_IN_HIJRI_MONTH; day -= 1) {
        const candidateDate = (0, conversion_js_1.toGregorianDate)({
            year: clamped.year,
            monthIndex: clamped.monthIndex,
            day,
        });
        const roundTrip = (0, conversion_js_1.toHijriDate)(candidateDate);
        if (roundTrip.year === clamped.year &&
            roundTrip.monthIndex === clamped.monthIndex &&
            roundTrip.day === day) {
            return day;
        }
    }
    return MIN_DAY_IN_HIJRI_MONTH;
}
