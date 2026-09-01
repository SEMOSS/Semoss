export type FallbackLocaleCode = "ar" | "en";
export type IntlNameWidth = "long" | "short" | "narrow";
export declare const getFallbackLocaleCode: (localeCode: string) => FallbackLocaleCode;
export declare const getFallbackMonthName: (date: Date, localeCode: string, width: IntlNameWidth) => string;
export declare const getFallbackWeekdayName: (date: Date, localeCode: string, width: IntlNameWidth) => string;
