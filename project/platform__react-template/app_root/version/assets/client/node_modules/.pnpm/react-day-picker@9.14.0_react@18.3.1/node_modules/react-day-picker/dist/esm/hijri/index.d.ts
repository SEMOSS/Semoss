import type { Locale } from "date-fns";
import React from "react";
import { DateLib, type DateLibOptions } from "../index.js";
import type { DayPickerProps } from "../types/props.js";
/**
 * Render the Hijri (Umm al-Qura) calendar.
 *
 * Defaults:
 *
 * - `locale`: `ar-SA`
 * - `dir`: `rtl`
 * - `numerals`: `arab`
 * - `startMonth`: `1924-08-01`
 * - `endMonth`: `2077-11-16`
 *
 * Out-of-range date props are clamped to the supported Umm al-Qura conversion
 * range, preventing runtime conversion errors.
 */
export declare function DayPicker(props: DayPickerProps & {
    locale?: Locale;
    dir?: DayPickerProps["dir"];
    numerals?: DayPickerProps["numerals"];
    dateLib?: DayPickerProps["dateLib"];
}): React.JSX.Element;
/** Returns the date library used in the Hijri calendar. */
export declare const getDateLib: (options?: DateLibOptions) => DateLib;
export { arSA } from "../locale/ar-SA.js";
export { enUS } from "../locale/en-US.js";
