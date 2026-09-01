"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.startOfMonth = startOfMonth;
const conversion_js_1 = require("../utils/conversion.js");
function startOfMonth(date) {
    const hijri = (0, conversion_js_1.toHijriDate)(date);
    return (0, conversion_js_1.toGregorianDate)({
        year: hijri.year,
        monthIndex: hijri.monthIndex,
        day: 1,
    });
}
