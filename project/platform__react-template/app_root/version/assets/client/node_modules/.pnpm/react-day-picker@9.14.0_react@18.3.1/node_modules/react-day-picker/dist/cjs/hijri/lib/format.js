"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.format = format;
const conversion_js_1 = require("../utils/conversion.js");
const range_js_1 = require("../utils/range.js");
const fallbackLocaleData_js_1 = require("./fallbackLocaleData.js");
const DEFAULT_LOCALE_CODE = "ar-SA";
const BASE_NUMBERING_SYSTEM = "latn";
const UMM_AL_QURA_CALENDAR = "islamic-umalqura";
const getLocaleCode = (options) => {
    return options?.locale?.code ?? DEFAULT_LOCALE_CODE;
};
const formatWithUmmAlQura = (date, localeCode, options) => {
    try {
        return new Intl.DateTimeFormat(localeCode, {
            ...options,
            calendar: UMM_AL_QURA_CALENDAR,
            numberingSystem: BASE_NUMBERING_SYSTEM,
        }).format(date);
    }
    catch {
        return undefined;
    }
};
const formatNumber = (value) => {
    return value.toString();
};
const formatPaddedNumber = (value) => {
    return formatNumber(value).padStart(2, "0");
};
const formatMonthName = (date, localeCode, width) => {
    const formatted = formatWithUmmAlQura(date, localeCode, {
        month: width,
    });
    return formatted ?? (0, fallbackLocaleData_js_1.getFallbackMonthName)(date, localeCode, width);
};
const formatWeekdayName = (date, localeCode, width) => {
    const formatted = formatWithUmmAlQura(date, localeCode, {
        weekday: width,
    });
    return formatted ?? (0, fallbackLocaleData_js_1.getFallbackWeekdayName)(date, localeCode, width);
};
const formatDateStyle = (date, localeCode, style) => {
    const formatted = formatWithUmmAlQura(date, localeCode, {
        dateStyle: style,
    });
    if (formatted) {
        return formatted;
    }
    const hijri = (0, conversion_js_1.toHijriDate)(date);
    const monthName = (0, fallbackLocaleData_js_1.getFallbackMonthName)(date, localeCode, "long");
    switch (style) {
        case "full":
            return `${(0, fallbackLocaleData_js_1.getFallbackWeekdayName)(date, localeCode, "long")}, ${monthName} ${hijri.day}, ${hijri.year}`;
        case "long":
            return `${monthName} ${hijri.day}, ${hijri.year}`;
        case "medium":
            return `${formatPaddedNumber(hijri.day)} ${monthName} ${hijri.year}`;
        case "short":
            return `${hijri.monthIndex + 1}/${hijri.day}/${hijri.year}`;
    }
};
const buildTimeFormat = (date, localeCode, formatStr) => {
    const hour12 = formatStr.includes("a");
    const formatted = formatWithUmmAlQura(date, localeCode, {
        hour: "numeric",
        minute: "numeric",
        hour12,
    });
    if (formatted) {
        return formatted;
    }
    try {
        return new Intl.DateTimeFormat(localeCode, {
            hour: "numeric",
            minute: "numeric",
            hour12,
            numberingSystem: BASE_NUMBERING_SYSTEM,
        }).format(date);
    }
    catch {
        const minutes = formatPaddedNumber(date.getMinutes());
        if (hour12) {
            const hour = date.getHours() % 12 || 12;
            const period = date.getHours() >= 12 ? "PM" : "AM";
            return `${hour}:${minutes} ${period}`;
        }
        return `${formatNumber(date.getHours())}:${minutes}`;
    }
};
/** Hijri calendar formatting override. */
function format(date, formatStr, options) {
    const extendedOptions = options;
    const localeCode = getLocaleCode(extendedOptions);
    const hijri = (0, conversion_js_1.toHijriDate)(date);
    const gregorian = (0, range_js_1.getGregorianDateParts)(date);
    const isOutOfRange = (0, range_js_1.clampGregorianDate)(date) !== date;
    const numericDate = isOutOfRange
        ? {
            year: gregorian.year,
            monthIndex: gregorian.month - 1,
            day: gregorian.day,
        }
        : hijri;
    switch (formatStr) {
        case "LLLL y":
        case "LLLL yyyy":
            return `${formatMonthName(date, localeCode, "long")} ${formatNumber(numericDate.year)}`;
        case "LLLL":
            return formatMonthName(date, localeCode, "long");
        case "LLL":
            return formatMonthName(date, localeCode, "short");
        case "PPP":
            return formatDateStyle(date, localeCode, "long");
        case "PPPP":
            return formatDateStyle(date, localeCode, "full");
        case "PP":
            return formatDateStyle(date, localeCode, "medium");
        case "P":
            return formatDateStyle(date, localeCode, "short");
        case "cccc":
            return formatWeekdayName(date, localeCode, "long");
        case "ccc":
            return formatWeekdayName(date, localeCode, "short");
        case "ccccc":
        case "cccccc":
            return formatWeekdayName(date, localeCode, "narrow");
        case "yyyy":
        case "y":
            return formatNumber(numericDate.year);
        case "yyyy-MM":
            return `${formatNumber(numericDate.year)}-${formatPaddedNumber(numericDate.monthIndex + 1)}`;
        case "yyyy-MM-dd":
            return `${formatNumber(numericDate.year)}-${formatPaddedNumber(numericDate.monthIndex + 1)}-${formatPaddedNumber(numericDate.day)}`;
        case "MM":
            return formatPaddedNumber(numericDate.monthIndex + 1);
        case "M":
            return formatNumber(numericDate.monthIndex + 1);
        case "dd":
            return formatPaddedNumber(numericDate.day);
        case "d":
            return formatNumber(numericDate.day);
        default:
            if (/[Hh]/.test(formatStr) && /m/.test(formatStr)) {
                return buildTimeFormat(date, localeCode, formatStr);
            }
            return formatDateStyle(date, localeCode, "medium");
    }
}
