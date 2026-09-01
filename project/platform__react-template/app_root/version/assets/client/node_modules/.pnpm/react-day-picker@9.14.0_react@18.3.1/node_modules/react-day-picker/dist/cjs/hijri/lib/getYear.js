"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getYear = getYear;
const conversion_js_1 = require("../utils/conversion.js");
function getYear(date) {
    return (0, conversion_js_1.toHijriDate)(date).year;
}
