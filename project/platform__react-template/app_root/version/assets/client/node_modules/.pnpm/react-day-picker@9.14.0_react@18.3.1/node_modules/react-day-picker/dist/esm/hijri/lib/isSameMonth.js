import { toHijriDate } from "../utils/conversion.js";
export function isSameMonth(dateLeft, dateRight) {
    const hijriLeft = toHijriDate(dateLeft);
    const hijriRight = toHijriDate(dateRight);
    return (hijriLeft.year === hijriRight.year &&
        hijriLeft.monthIndex === hijriRight.monthIndex);
}
