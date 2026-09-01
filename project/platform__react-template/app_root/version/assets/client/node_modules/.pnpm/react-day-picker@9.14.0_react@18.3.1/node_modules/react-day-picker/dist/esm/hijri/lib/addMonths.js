import { toHijriDate } from "../utils/conversion.js";
import { setMonth } from "./setMonth.js";
export function addMonths(date, amount) {
    const hijri = toHijriDate(date);
    return setMonth(date, hijri.monthIndex + amount);
}
