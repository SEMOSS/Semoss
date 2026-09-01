"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.newDate = newDate;
const conversion_js_1 = require("../utils/conversion.js");
function newDate(year, monthIndex, day) {
    return (0, conversion_js_1.toGregorianDate)({ year, monthIndex, day });
}
