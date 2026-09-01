"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.divmod = void 0;
/**
 * Take two (non-complex) numbers as arguments and return a pair of numbers
 * consisting of their quotient and remainder when using integer division.
 */
function divmod(a, b) {
    return [Math.floor(a / b), a % b];
}
exports.divmod = divmod;
