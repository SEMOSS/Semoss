export declare const DAYS_BEFORE_MONTH: number[];
/**
 * year -> 1 if leap year, else 0
 */
export declare function isLeap(year: number): boolean;
/**
 * year, month -> number of days in that month in that year.
 */
export declare function _daysInMonth(year: number, month: number): number;
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
export declare function ord2ymd(n: number): [number, number, number];
