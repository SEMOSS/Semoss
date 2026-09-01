"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.addYears = addYears;
const conversion_js_1 = require("../utils/conversion.js");
const setYear_js_1 = require("./setYear.js");
function addYears(date, amount) {
    const hijri = (0, conversion_js_1.toHijriDate)(date);
    return (0, setYear_js_1.setYear)(date, hijri.year + amount);
}
