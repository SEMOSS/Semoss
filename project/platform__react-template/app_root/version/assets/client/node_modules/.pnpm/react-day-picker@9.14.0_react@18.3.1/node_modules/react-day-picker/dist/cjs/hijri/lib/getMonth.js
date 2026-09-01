"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getMonth = getMonth;
const conversion_js_1 = require("../utils/conversion.js");
function getMonth(date) {
    return (0, conversion_js_1.toHijriDate)(date).monthIndex;
}
