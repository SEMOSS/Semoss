import { toGregorianDate, toHijriDate } from "../utils/conversion.js";
import { getDaysInMonth } from "../utils/daysInMonth.js";
export function endOfMonth(date) {
    const hijri = toHijriDate(date);
    const day = getDaysInMonth(hijri.year, hijri.monthIndex);
    return toGregorianDate({
        year: hijri.year,
        monthIndex: hijri.monthIndex,
        day,
    });
}
