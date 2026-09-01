const MAX_HIJRI_DAY = 30;
const MONTHS_PER_YEAR = 12;
const HIJRI_MIN = { year: 1343, monthIndex: 0, day: 1 };
const HIJRI_MAX = { year: 1500, monthIndex: 11, day: 30 };
const GREGORIAN_MIN = { year: 1924, month: 8, day: 1 };
const GREGORIAN_MAX = { year: 2077, month: 11, day: 16 };
function compareGregorianDates(left, right) {
    if (left.year !== right.year) {
        return left.year - right.year;
    }
    if (left.month !== right.month) {
        return left.month - right.month;
    }
    return left.day - right.day;
}
function createDate(parts) {
    return new Date(parts.year, parts.month - 1, parts.day);
}
function normalizeHijriMonth(year, monthIndex) {
    let normalizedYear = year + Math.trunc(monthIndex / MONTHS_PER_YEAR);
    let normalizedMonth = monthIndex % MONTHS_PER_YEAR;
    if (normalizedMonth < 0) {
        normalizedMonth += MONTHS_PER_YEAR;
        normalizedYear -= 1;
    }
    return { year: normalizedYear, monthIndex: normalizedMonth };
}
/** Returns Gregorian date parts while preserving years < 100. */
export function getGregorianDateParts(date) {
    const useUTC = date.getFullYear() < 100;
    return {
        year: useUTC ? date.getUTCFullYear() : date.getFullYear(),
        month: (useUTC ? date.getUTCMonth() : date.getMonth()) + 1,
        day: useUTC ? date.getUTCDate() : date.getDate(),
    };
}
/** Internal Gregorian lower bound supported by the Hijri converter. */
export const GREGORIAN_MIN_DATE = createDate(GREGORIAN_MIN);
/** Internal Gregorian upper bound supported by the Hijri converter. */
export const GREGORIAN_MAX_DATE = createDate(GREGORIAN_MAX);
/** Clamp a Gregorian date to the supported Hijri conversion range. */
export function clampGregorianDate(date) {
    const parts = getGregorianDateParts(date);
    if (compareGregorianDates(parts, GREGORIAN_MIN) < 0) {
        return createDate(GREGORIAN_MIN);
    }
    if (compareGregorianDates(parts, GREGORIAN_MAX) > 0) {
        return createDate(GREGORIAN_MAX);
    }
    return date;
}
/** Returns whether the provided Gregorian date was clamped. */
export function wasGregorianDateClamped(date) {
    return clampGregorianDate(date) !== date;
}
/** Clamp a Hijri date to the supported range and normalize month overflow. */
export function clampHijriDate(date) {
    const normalized = normalizeHijriMonth(date.year, date.monthIndex);
    const candidate = {
        year: normalized.year,
        monthIndex: normalized.monthIndex,
        day: Math.min(Math.max(date.day, 1), MAX_HIJRI_DAY),
    };
    if (candidate.year < HIJRI_MIN.year) {
        return { ...HIJRI_MIN };
    }
    if (candidate.year > HIJRI_MAX.year) {
        return { ...HIJRI_MAX };
    }
    if (candidate.year === HIJRI_MIN.year &&
        candidate.monthIndex < HIJRI_MIN.monthIndex) {
        return { ...HIJRI_MIN };
    }
    if (candidate.year === HIJRI_MAX.year &&
        candidate.monthIndex > HIJRI_MAX.monthIndex) {
        return { ...HIJRI_MAX };
    }
    if (candidate.year === HIJRI_MIN.year &&
        candidate.monthIndex === HIJRI_MIN.monthIndex &&
        candidate.day < HIJRI_MIN.day) {
        return { ...HIJRI_MIN };
    }
    if (candidate.year === HIJRI_MAX.year &&
        candidate.monthIndex === HIJRI_MAX.monthIndex &&
        candidate.day > HIJRI_MAX.day) {
        return { ...HIJRI_MAX };
    }
    return candidate;
}
/** Returns whether the provided Hijri date was clamped. */
export function wasHijriDateClamped(date) {
    const clamped = clampHijriDate(date);
    return (clamped.year !== date.year ||
        clamped.monthIndex !== date.monthIndex ||
        clamped.day !== date.day);
}
