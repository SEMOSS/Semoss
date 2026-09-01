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
exports.bisect = void 0;
/**
 * Locate the insertion point for `x` in `a` to maintain sorted order.
 * The parameters `lo` and `hi` may be used to specify a subset of the list
 * which should be considered; by default the entire list is used.
 * If `x` is already present in `a`, the insertion point will be after (to the
 * right of) any existing entries. The return value is suitable for use as
 * the first parameter to Array.splice() assuming that `a` is already sorted.
 *
 * This is part of Python's algorithm of the `bisectRight` function.
 *
 * @throws {Error} if `lo` is negative number
 */
function bisect(a, x, lo = 0, hi) {
    if (lo < 0)
        throw Error('lo must be non-negative');
    if (hi === undefined)
        hi = a.length;
    while (lo < hi) {
        const mid = Math.floor((lo + hi) / 2);
        if (x < a[mid]) {
            hi = mid;
        }
        else {
            lo = mid + 1;
        }
    }
    return lo;
}
exports.bisect = bisect;
