"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isSameYear = isSameYear;
const conversion_js_1 = require("../utils/conversion.js");
function isSameYear(dateLeft, dateRight) {
    const hijriLeft = (0, conversion_js_1.toHijriDate)(dateLeft);
    const hijriRight = (0, conversion_js_1.toHijriDate)(dateRight);
    return hijriLeft.year === hijriRight.year;
}
