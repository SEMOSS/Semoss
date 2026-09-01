"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.eachYearOfInterval = eachYearOfInterval;
const date_fns_1 = require("date-fns");
const conversion_js_1 = require("../utils/conversion.js");
function eachYearOfInterval(interval) {
    const start = (0, date_fns_1.toDate)(interval.start);
    const end = (0, date_fns_1.toDate)(interval.end);
    if (end.getTime() < start.getTime()) {
        return [];
    }
    const startYear = (0, conversion_js_1.toHijriDate)(start).year;
    const endYear = (0, conversion_js_1.toHijriDate)(end).year;
    const years = [];
    for (let year = startYear; year <= endYear; year += 1) {
        years.push((0, conversion_js_1.toGregorianDate)({ year, monthIndex: 0, day: 1 }));
    }
    return years;
}
