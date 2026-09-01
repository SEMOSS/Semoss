import { toHijriDate } from "../utils/conversion.js";
export function getYear(date) {
    return toHijriDate(date).year;
}
