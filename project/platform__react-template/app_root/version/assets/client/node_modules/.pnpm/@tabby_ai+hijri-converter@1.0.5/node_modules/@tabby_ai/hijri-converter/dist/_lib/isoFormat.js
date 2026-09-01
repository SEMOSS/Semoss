"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isoFormat = void 0;
/**
 * Return date in ISO format i.e. `YYYY-MM-DD`.
 */
function isoFormat(year, month, day) {
    return [
        String(year).padStart(4, '0'),
        String(month).padStart(2, '0'),
        String(day).padStart(2, '0'),
    ].join('-');
}
exports.isoFormat = isoFormat;
