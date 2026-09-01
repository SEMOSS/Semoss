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
exports.rjdToJdn = exports.jdnToRjd = exports.ordinalToJdn = exports.jdnToOrdinal = void 0;
/**
 * Convert Julian day number (JDN) to date ordinal number.
 * @param {number} jdn - Julian day number (JDN).
 * @returns {number} Date ordinal number.
 */
function jdnToOrdinal(jdn) {
    return jdn - 1721425;
}
exports.jdnToOrdinal = jdnToOrdinal;
/**
 * Convert date ordinal number to Julian day number (JDN).
 * @param {number} don - Date ordinal number.
 * @returns {number} Julian day number (JDN).
 */
function ordinalToJdn(don) {
    return don + 1721425;
}
exports.ordinalToJdn = ordinalToJdn;
/**
 * Return Reduced Julian Day (RJD) number from Julian day number (JDN).
 * @param {number} jdn - Julian day number (JDN).
 * @returns {number} Reduced Julian Day (RJD) number.
 */
function jdnToRjd(jdn) {
    return jdn - 2400000;
}
exports.jdnToRjd = jdnToRjd;
/**
 * Return Julian day number (JDN) from Reduced Julian Day (RJD) number.
 * @param {number} rjd - Reduced Julian Day (RJD) number.
 * @returns {number} Julian day number (JDN).
 */
function rjdToJdn(rjd) {
    return rjd + 2400000;
}
exports.rjdToJdn = rjdToJdn;
