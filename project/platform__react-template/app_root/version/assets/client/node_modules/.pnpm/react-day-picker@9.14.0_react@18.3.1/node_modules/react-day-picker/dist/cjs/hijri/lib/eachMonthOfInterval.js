"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.eachMonthOfInterval = eachMonthOfInterval;
const date_fns_1 = require("date-fns");
const conversion_js_1 = require("../utils/conversion.js");
function eachMonthOfInterval(interval) {
    const start = (0, date_fns_1.toDate)(interval.start);
    const end = (0, date_fns_1.toDate)(interval.end);
    if (end.getTime() < start.getTime()) {
        throw new RangeError("Invalid interval");
    }
    const startDate = (0, conversion_js_1.toHijriDate)(start);
    const endDate = (0, conversion_js_1.toHijriDate)(end);
    const months = [];
    let currentYear = startDate.year;
    let currentMonth = startDate.monthIndex;
    const endYear = endDate.year;
    const endMonth = endDate.monthIndex;
    while (currentYear < endYear ||
        (currentYear === endYear && currentMonth <= endMonth)) {
        months.push((0, conversion_js_1.toGregorianDate)({ year: currentYear, monthIndex: currentMonth, day: 1 }));
        currentMonth += 1;
        if (currentMonth > 11) {
            currentMonth = 0;
            currentYear += 1;
        }
    }
    return months;
}
