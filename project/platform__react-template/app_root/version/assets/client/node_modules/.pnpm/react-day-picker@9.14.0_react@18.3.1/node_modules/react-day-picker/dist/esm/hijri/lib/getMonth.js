import { toHijriDate } from "../utils/conversion.js";
export function getMonth(date) {
    return toHijriDate(date).monthIndex;
}
