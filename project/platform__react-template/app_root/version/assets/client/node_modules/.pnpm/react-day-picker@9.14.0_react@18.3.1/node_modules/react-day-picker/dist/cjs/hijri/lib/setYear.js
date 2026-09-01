"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.setYear = setYear;
const conversion_js_1 = require("../utils/conversion.js");
const daysInMonth_js_1 = require("../utils/daysInMonth.js");
function setYear(date, year) {
    const hijri = (0, conversion_js_1.toHijriDate)(date);
    const daysInTargetMonth = (0, daysInMonth_js_1.getDaysInMonth)(year, hijri.monthIndex);
    const day = Math.min(hijri.day, daysInTargetMonth);
    return (0, conversion_js_1.toGregorianDate)({
        year,
        monthIndex: hijri.monthIndex,
        day,
    });
}
