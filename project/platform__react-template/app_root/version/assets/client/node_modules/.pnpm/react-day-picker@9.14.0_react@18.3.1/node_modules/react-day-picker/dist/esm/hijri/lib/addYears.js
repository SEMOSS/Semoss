import { toHijriDate } from "../utils/conversion.js";
import { setYear } from "./setYear.js";
export function addYears(date, amount) {
    const hijri = toHijriDate(date);
    return setYear(date, hijri.year + amount);
}
