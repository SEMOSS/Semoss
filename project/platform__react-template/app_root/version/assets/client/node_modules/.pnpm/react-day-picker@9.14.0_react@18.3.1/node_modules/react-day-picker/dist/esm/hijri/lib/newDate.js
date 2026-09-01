import { toGregorianDate } from "../utils/conversion.js";
export function newDate(year, monthIndex, day) {
    return toGregorianDate({ year, monthIndex, day });
}
