"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.startOfYear = startOfYear;
const conversion_js_1 = require("../utils/conversion.js");
function startOfYear(date) {
    const hijri = (0, conversion_js_1.toHijriDate)(date);
    return (0, conversion_js_1.toGregorianDate)({
        year: hijri.year,
        monthIndex: 0,
        day: 1,
    });
}
