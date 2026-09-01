"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.enUS = exports.arSA = exports.getDateLib = void 0;
exports.DayPicker = DayPicker;
const react_1 = __importDefault(require("react"));
const index_js_1 = require("../index.js");
const ar_SA_js_1 = require("../locale/ar-SA.js");
const hijriDateLib = __importStar(require("./lib/index.js"));
const range_js_1 = require("./utils/range.js");
function clampDateProp(date) {
    if (!date) {
        return undefined;
    }
    return (0, range_js_1.clampGregorianDate)(date);
}
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
function DayPicker(props) {
    const { dateLib: dateLibProp, ...dayPickerProps } = props;
    const hasStartBound = props.startMonth !== undefined ||
        props.fromMonth !== undefined ||
        props.fromYear !== undefined;
    const hasEndBound = props.endMonth !== undefined ||
        props.toMonth !== undefined ||
        props.toYear !== undefined;
    const clampedProps = {
        ...dayPickerProps,
        month: clampDateProp(props.month),
        defaultMonth: clampDateProp(props.defaultMonth),
        today: clampDateProp(props.today),
        startMonth: hasStartBound
            ? clampDateProp(props.startMonth)
            : new Date(range_js_1.GREGORIAN_MIN_DATE),
        endMonth: hasEndBound
            ? clampDateProp(props.endMonth)
            : new Date(range_js_1.GREGORIAN_MAX_DATE),
        fromMonth: clampDateProp(props.fromMonth),
        toMonth: clampDateProp(props.toMonth),
    };
    return (react_1.default.createElement(index_js_1.DayPicker, { ...clampedProps, locale: props.locale ?? ar_SA_js_1.arSA, numerals: props.numerals ?? "arab", dir: props.dir ?? "rtl", dateLib: { ...hijriDateLib, ...dateLibProp } }));
}
/** Returns the date library used in the Hijri calendar. */
const getDateLib = (options) => {
    return new index_js_1.DateLib(options, hijriDateLib);
};
exports.getDateLib = getDateLib;
var ar_SA_js_2 = require("../locale/ar-SA.js");
Object.defineProperty(exports, "arSA", { enumerable: true, get: function () { return ar_SA_js_2.arSA; } });
var en_US_js_1 = require("../locale/en-US.js");
Object.defineProperty(exports, "enUS", { enumerable: true, get: function () { return en_US_js_1.enUS; } });
