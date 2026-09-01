"use strict";
/*
 * This file contains modifications to code from https://github.com/mhalshehri/hijri-converter
 * that is licensed under the MIT license.
 *
 * Copyright (c) 2018 Mohammed H Alshehri (@mhalshehri) and contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the 'Software'), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software.
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * This modified version of the Software is also licensed under the MIT license,
 * and is subject to the same conditions as the original version of the Software.
 *
 * Copyright (c) 2023 Tabby FZ-LLC
 *
 * A copy of the license can be found in the LICENSE file at the root of this
 * distribution.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.gregorianToHijri = void 0;
const checkDateRange_1 = require("./_lib/checkDateRange");
const dateHelpersHijri_1 = require("./_lib/dateHelpersHijri");
const julianDayHelpers_1 = require("./_lib/julianDayHelpers");
const bisect_1 = require("./_lib/utils/bisect");
const ummalqura_1 = require("./_lib/ummalqura");
/**
 * Return Hijri object for the corresponding Gregorian date.
 * @throws {Error} when date is out of supported Gregorian range.
 */
function gregorianToHijri(date) {
    (0, checkDateRange_1.checkGregorianRange)(date);
    const jdn = (0, dateHelpersHijri_1.gregorianToJulian)(date);
    const rjd = (0, julianDayHelpers_1.jdnToRjd)(jdn);
    const index = (0, bisect_1.bisect)(ummalqura_1.MONTH_STARTS, rjd) - 1;
    const months = index + ummalqura_1.HIJRI_OFFSET;
    const years = Math.floor(months / 12);
    const year = years + 1;
    const month = months - years * 12 + 1;
    const day = rjd - ummalqura_1.MONTH_STARTS[index] + 1;
    return { year, month, day };
}
exports.gregorianToHijri = gregorianToHijri;
