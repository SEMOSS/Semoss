"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.addMonths = addMonths;
const conversion_js_1 = require("../utils/conversion.js");
const setMonth_js_1 = require("./setMonth.js");
function addMonths(date, amount) {
    const hijri = (0, conversion_js_1.toHijriDate)(date);
    return (0, setMonth_js_1.setMonth)(date, hijri.monthIndex + amount);
}
