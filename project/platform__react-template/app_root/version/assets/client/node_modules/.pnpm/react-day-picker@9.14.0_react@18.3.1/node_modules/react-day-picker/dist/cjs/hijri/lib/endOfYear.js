"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.endOfYear = endOfYear;
const conversion_js_1 = require("../utils/conversion.js");
const daysInMonth_js_1 = require("../utils/daysInMonth.js");
function endOfYear(date) {
    const hijri = (0, conversion_js_1.toHijriDate)(date);
    const day = (0, daysInMonth_js_1.getDaysInMonth)(hijri.year, 11);
    return (0, conversion_js_1.toGregorianDate)({
        year: hijri.year,
        monthIndex: 11,
        day,
    });
}
