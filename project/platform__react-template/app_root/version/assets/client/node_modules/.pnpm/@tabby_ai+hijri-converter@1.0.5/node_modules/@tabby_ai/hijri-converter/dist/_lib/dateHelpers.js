"use strict";
/*
 * This file contains modifications to code that is licensed under
 * the PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2.
 *
 * Copyright © 2001-2023 Python Software Foundation. All rights reserved.
 *
 * 1. This LICENSE AGREEMENT is between the Python Software Foundation
 * ("PSF"), and the Individual or Organization ("Licensee") accessing and
 * otherwise using this software ("Python") in source or binary form and
 * its associated documentation.
 *
 * 2. Subject to the terms and conditions of this License Agreement, PSF hereby
 * grants Licensee a nonexclusive, royalty-free, world-wide license to reproduce,
 * analyze, test, perform and/or display publicly, prepare derivative works,
 * distribute, and otherwise use Python alone or in any derivative version,
 * provided, however, that PSF's License Agreement and PSF's notice of copyright,
 * i.e., "Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010,
 * 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023 Python Software Foundation;
 * All Rights Reserved" are retained in Python alone or in any derivative version
 * prepared by Licensee.
 *
 * 3. In the event Licensee prepares a derivative work that is based on
 * or incorporates Python or any part thereof, and wants to make
 * the derivative work available to others as provided herein, then
 * Licensee hereby agrees to include in any such work a brief summary of
 * the changes made to Python.
 *
 * 4. PSF is making Python available to Licensee on an "AS IS"
 * basis.  PSF MAKES NO REPRESENTATIONS OR WARRANTIES, EXPRESS OR
 * IMPLIED.  BY WAY OF EXAMPLE, BUT NOT LIMITATION, PSF MAKES NO AND
 * DISCLAIMS ANY REPRESENTATION OR WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR ANY PARTICULAR PURPOSE OR THAT THE USE OF PYTHON WILL NOT
 * INFRINGE ANY THIRD PARTY RIGHTS.
 *
 * 5. PSF SHALL NOT BE LIABLE TO LICENSEE OR ANY OTHER USERS OF PYTHON
 * FOR ANY INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES OR LOSS AS
 * A RESULT OF MODIFYING, DISTRIBUTING, OR OTHERWISE USING PYTHON,
 * OR ANY DERIVATIVE THEREOF, EVEN IF ADVISED OF THE POSSIBILITY THEREOF.
 *
 * 6. This License Agreement will automatically terminate upon a material
 * breach of its terms and conditions.
 *
 * 7. Nothing in this License Agreement shall be deemed to create any
 * relationship of agency, partnership, or joint venture between PSF and
 * Licensee.  This License Agreement does not grant permission to use PSF
 * trademarks or trade name in a trademark sense to endorse or promote
 * products or services of Licensee, or any third party.
 *
 * 8. By copying, installing or otherwise using Python, Licensee
 * agrees to be bound by the terms and conditions of this License
 * Agreement.
 *
 * This modified version of the Software is licensed under the MIT license.
 *
 * Copyright (c) 2023 Tabby FZ-LLC
 *
 * A copy of the license can be found in the LICENSE file at the root of this
 * distribution.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.ord2ymd = exports._daysInMonth = exports.isLeap = exports.DAYS_BEFORE_MONTH = void 0;
const divmod_1 = require("./utils/divmod");
// Constants for Gregorian calendar
const _DAYS_IN_MONTH = [-1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
exports.DAYS_BEFORE_MONTH = [-1];
let dbm = 0;
for (let i = 1; i < _DAYS_IN_MONTH.length; i++) {
    exports.DAYS_BEFORE_MONTH.push(dbm);
    dbm += _DAYS_IN_MONTH[i];
}
/**
 * year -> number of days before January 1st of year.
 * @param year
 */
function _daysBeforeYear(year) {
    const y = year - 1;
    return (y * 365 + Math.floor(y / 4) - Math.floor(y / 100) + Math.floor(y / 400));
}
const _DI400Y = _daysBeforeYear(401); // number of days in 400 years
const _DI100Y = _daysBeforeYear(101); // number of days in 100 years
const _DI4Y = _daysBeforeYear(5); // number of days in 4 years
/**
 * year -> 1 if leap year, else 0
 */
function isLeap(year) {
    return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
}
exports.isLeap = isLeap;
/**
 * year, month -> number of days in that month in that year.
 */
function _daysInMonth(year, month) {
    if (month < 1 || month > 12) {
        throw Error(`AssertionError: Expected: 1 <= month <= 12; Actual: month = ${month}`);
    }
    if (month === 2 && isLeap(year)) {
        return 29;
    }
    return _DAYS_IN_MONTH[month];
}
exports._daysInMonth = _daysInMonth;
/**
 * ordinal -> (year, month, day), considering 01-Jan-0001 as day 1.
 *
 * n is a 1-based index, starting at 1-Jan-1. The pattern of leap years
 * repeats exactly every 400 years. The basic strategy is to find the
 * closest 400-year boundary at or before n, then work with the offset
 * from that boundary to n. Life is much clearer if we subtract 1 from
 * n first -- then the values of n at 400-year boundaries are exactly
 * those divisible by _DI400Y:
 *
 *      D  M   Y            n              n-1
 *      -- --- ----        ----------     ----------------
 *      31 Dec -400        -_DI400Y       -_DI400Y -1
 *       1 Jan -399         -_DI400Y +1   -_DI400Y      400-year boundary
 *      ...
 *      30 Dec  000        -1             -2
 *      31 Dec  000         0             -1
 *       1 Jan  001         1              0            400-year boundary
 *       2 Jan  001         2              1
 *       3 Jan  001         3              2
 *      ...
 *      31 Dec  400         _DI400Y        _DI400Y -1
 *       1 Jan  401         _DI400Y +1     _DI400Y      400-year boundary
 *
 *
 */
function ord2ymd(n) {
    n -= 1;
    let n400, n100, n4, n1;
    [n400, n] = (0, divmod_1.divmod)(n, _DI400Y);
    let year = n400 * 400 + 1; // ..., -399, 1, 401, ...
    // Now n is the (non-negative) offset, in days, from January 1 of year, to
    // the desired date.  Now compute how many 100-year cycles precede n.
    // Note that it's possible for n100 to equal 4!  In that case 4 full
    // 100-year cycles precede the desired day, which implies the desired
    // day is December 31 at the end of a 400-year cycle.
    [n100, n] = (0, divmod_1.divmod)(n, _DI100Y);
    // Now compute how many 4-year cycles precede it.
    [n4, n] = (0, divmod_1.divmod)(n, _DI4Y);
    // And now how many single years. Again n1 can be 4, and again meaning
    // that the desired day is December 31 at the end of the 4-year cycle.
    [n1, n] = (0, divmod_1.divmod)(n, 365);
    year += n100 * 100 + n4 * 4 + n1;
    if (n1 === 4 || n100 === 4) {
        if (n !== 0) {
            throw Error(`AssertionError: Expected: n = 0; Actual: n = ${n}`);
        }
        return [year - 1, 12, 31];
    }
    // Now the year is correct, and n is the offset from January 1.  We find
    // the month via an estimate that's either exact or one too large.
    const leapYear = n1 === 3 && (n4 !== 24 || n100 === 3);
    if (leapYear !== isLeap(year)) {
        throw Error(`AssertionError: Expected: leapyear = ${isLeap(year)}; Actual: leapyear = ${leapYear}`);
    }
    let month = (n + 50) >> 5;
    let preceding = exports.DAYS_BEFORE_MONTH[month] + Number(month > 2 && leapYear);
    if (preceding > n) {
        // estimate is too large
        month -= 1;
        preceding -= _DAYS_IN_MONTH[month] + Number(month === 2 && leapYear);
    }
    n -= preceding;
    if (n < 0 || _daysInMonth(year, month) < n) {
        throw Error(`AssertionError: Expected: 0 <= n <= ${_daysInMonth(year, month)}; Actual: n = ${n}`);
    }
    // Now the year and month are correct, and n is the offset from the
    //  start of that month:  we're done!
    return [year, month, n + 1];
}
exports.ord2ymd = ord2ymd;
