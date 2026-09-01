"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getFallbackWeekdayName = exports.getFallbackMonthName = exports.getFallbackLocaleCode = void 0;
const conversion_js_1 = require("../utils/conversion.js");
const fallbackMonthNames = {
    en: {
        long: [
            "Muharram",
            "Safar",
            "Rabi I",
            "Rabi II",
            "Jumada I",
            "Jumada II",
            "Rajab",
            "Shaban",
            "Ramadan",
            "Shawwal",
            "Dhu al-Qadah",
            "Dhu al-Hijjah",
        ],
        short: [
            "Muh",
            "Saf",
            "Rab-I",
            "Rab-II",
            "Jum-I",
            "Jum-II",
            "Raj",
            "Sha",
            "Ram",
            "Shw",
            "Dhu-Q",
            "Dhu-H",
        ],
        narrow: ["M", "S", "R", "R", "J", "J", "R", "S", "R", "S", "D", "D"],
    },
    ar: {
        long: [
            "محرم",
            "صفر",
            "ربيع الأول",
            "ربيع الآخر",
            "جمادى الأولى",
            "جمادى الآخرة",
            "رجب",
            "شعبان",
            "رمضان",
            "شوال",
            "ذو القعدة",
            "ذو الحجة",
        ],
        short: [
            "محرم",
            "صفر",
            "ربيع ١",
            "ربيع ٢",
            "جمادى ١",
            "جمادى ٢",
            "رجب",
            "شعبان",
            "رمضان",
            "شوال",
            "ذو القعدة",
            "ذو الحجة",
        ],
        narrow: ["م", "ص", "ر", "ر", "ج", "ج", "ر", "ش", "ر", "ش", "ذ", "ذ"],
    },
};
const fallbackWeekdayNames = {
    en: {
        long: [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ],
        short: ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
        narrow: ["S", "M", "T", "W", "T", "F", "S"],
    },
    ar: {
        long: [
            "الأحد",
            "الاثنين",
            "الثلاثاء",
            "الأربعاء",
            "الخميس",
            "الجمعة",
            "السبت",
        ],
        short: ["أحد", "اثن", "ثلا", "أرب", "خمي", "جمع", "سبت"],
        narrow: ["ح", "ن", "ث", "ر", "خ", "ج", "س"],
    },
};
const getFallbackLocaleCode = (localeCode) => {
    return localeCode.toLowerCase().startsWith("ar") ? "ar" : "en";
};
exports.getFallbackLocaleCode = getFallbackLocaleCode;
const getFallbackMonthName = (date, localeCode, width) => {
    const hijri = (0, conversion_js_1.toHijriDate)(date);
    const locale = (0, exports.getFallbackLocaleCode)(localeCode);
    return fallbackMonthNames[locale][width][hijri.monthIndex];
};
exports.getFallbackMonthName = getFallbackMonthName;
const getFallbackWeekdayName = (date, localeCode, width) => {
    const locale = (0, exports.getFallbackLocaleCode)(localeCode);
    return fallbackWeekdayNames[locale][width][date.getDay()];
};
exports.getFallbackWeekdayName = getFallbackWeekdayName;
