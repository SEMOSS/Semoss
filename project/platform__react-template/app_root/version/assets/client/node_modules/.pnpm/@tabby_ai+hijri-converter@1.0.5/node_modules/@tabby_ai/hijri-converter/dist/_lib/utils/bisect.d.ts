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
export declare function bisect<T>(a: T[], x: T, lo?: number, hi?: number): number;
