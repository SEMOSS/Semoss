/**
 * Convert Julian day number (JDN) to date ordinal number.
 * @param {number} jdn - Julian day number (JDN).
 * @returns {number} Date ordinal number.
 */
export declare function jdnToOrdinal(jdn: number): number;
/**
 * Convert date ordinal number to Julian day number (JDN).
 * @param {number} don - Date ordinal number.
 * @returns {number} Julian day number (JDN).
 */
export declare function ordinalToJdn(don: number): number;
/**
 * Return Reduced Julian Day (RJD) number from Julian day number (JDN).
 * @param {number} jdn - Julian day number (JDN).
 * @returns {number} Reduced Julian Day (RJD) number.
 */
export declare function jdnToRjd(jdn: number): number;
/**
 * Return Julian day number (JDN) from Reduced Julian Day (RJD) number.
 * @param {number} rjd - Reduced Julian Day (RJD) number.
 * @returns {number} Julian day number (JDN).
 */
export declare function rjdToJdn(rjd: number): number;
