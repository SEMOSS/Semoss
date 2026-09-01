"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.endOfMonth = endOfMonth;
const conversion_js_1 = require("../utils/conversion.js");
const daysInMonth_js_1 = require("../utils/daysInMonth.js");
function endOfMonth(date) {
    const hijri = (0, conversion_js_1.toHijriDate)(date);
    const day = (0, daysInMonth_js_1.getDaysInMonth)(hijri.year, hijri.monthIndex);
    return (0, conversion_js_1.toGregorianDate)({
        year: hijri.year,
        monthIndex: hijri.monthIndex,
        day,
    });
}
