/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Reading loosely typed values: text that may be missing or blank, text that
 * must be there, objects that stand for text, and text that stands for a
 * boolean or a number, as they arrive from configuration, request bodies, JSON
 * and pixel inputs.
 *
 * <p>
 * Blank means empty once trimmed with {@link String#trim()}, the same way every
 * method here reads it.
 * </p>
 */
public final class ValueUtils {

	private ValueUtils() {

	}

	/**
	 * Check whether text is missing, such as an optional parameter that was left
	 * out or sent as spaces. Blank means empty once trimmed with
	 * {@link String#trim()}.
	 *
	 * @param value the text
	 * @return whether it is null, or empty once trimmed
	 */
	public static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}

	/**
	 * Read optional text, such as an optional request parameter, so that missing
	 * and blank text both read as null and present text loses its leading and
	 * trailing spaces.
	 *
	 * @param value the text
	 * @return the text trimmed, or null when it is null or blank
	 */
	public static String trimToNull(String value) {
		if (value == null) {
			return null;
		}
		String trimmed = value.trim();
		return trimmed.isEmpty() ? null : trimmed;
	}

	/**
	 * Read an optional value as trimmed text, such as a field from a
	 * {@code Map<String, Object>} request body or JSON. The value is read through
	 * {@link Object#toString()}, so a value whose text is null or blank reads as
	 * null.
	 *
	 * @param value the value
	 * @return the value as text, trimmed, or null when it is null or blank
	 */
	public static String trimToNull(Object value) {
		return value == null ? null : trimToNull(value.toString());
	}

	/**
	 * Read text that must be there, such as a required request parameter or id.
	 *
	 * @param value   the text
	 * @param message the error message when it is missing
	 * @return the text trimmed
	 * @throws IllegalArgumentException with the message when the text is null or
	 *                                  blank
	 */
	public static String requireNonBlank(String value, String message) {
		String trimmed = trimToNull(value);
		if (trimmed == null) {
			throw new IllegalArgumentException(message);
		}
		return trimmed;
	}

	/**
	 * Read text that may be missing as trimmed text, for callers that would rather
	 * hold an empty string than null. Blank text reads as an empty string.
	 *
	 * @param value the text
	 * @return the text trimmed, or an empty string when it is null
	 */
	public static String trimToEmpty(String value) {
		return value == null ? "" : value.trim();
	}

	/**
	 * Read a value that may be missing as trimmed text, such as a field from a
	 * {@code Map<String, Object>} request body. The value is read through
	 * {@link Object#toString()} and nothing else, so numbers, booleans, JSON and
	 * collections are not parsed, and spaces inside the text stay.
	 *
	 * @param value the value
	 * @return the value as text, trimmed, or an empty string when it or its text is
	 *         null
	 */
	public static String trimToEmpty(Object value) {
		return value == null ? "" : trimToEmpty(value.toString());
	}

	/**
	 * Build a key to compare text by, such as a filter or a lookup label. The lower
	 * case uses the root locale, so it does not depend on the default locale, and
	 * it is not full Unicode case folding. Spaces inside the text, punctuation and
	 * accents stay as they are.
	 *
	 * @param value the text
	 * @return the text trimmed and in lower case, or an empty string when it is
	 *         null
	 */
	public static String lowerCaseTrimToEmpty(String value) {
		return trimToEmpty(value).toLowerCase(Locale.ROOT);
	}

	/**
	 * Read a value as text without changing it, such as an id that may be missing.
	 * Unlike {@link #trimToNull(Object)}, the text is not trimmed and blank text is
	 * kept.
	 *
	 * @param value the value
	 * @return the value as text, or null when it is null
	 */
	public static String toStringOrNull(Object value) {
		return value == null ? null : value.toString();
	}

	/**
	 * Read a value as text without changing it, for callers that would rather hold
	 * an empty string than null. Unlike {@link #trimToEmpty(Object)}, the text is
	 * not trimmed.
	 *
	 * @param value the value
	 * @return the value as text, or an empty string when it is null
	 */
	public static String toStringOrEmpty(Object value) {
		return value == null ? "" : value.toString();
	}

	/**
	 * Read a value as text that must have content while keeping its spacing, such
	 * as a display label whose spaces matter. Blank text reads as null, as with
	 * {@link #trimToNull(Object)}, but present text is returned untrimmed.
	 *
	 * @param value the value
	 * @return the value as text, as it is, or null when it is null or blank
	 */
	public static String toNonBlankString(Object value) {
		if (value == null) {
			return null;
		}
		String text = value.toString();
		return isBlank(text) ? null : text;
	}

	/**
	 * Choose between a value and its fallback, such as a typed configuration value
	 * or a display label whose spacing should stay. The chosen one is returned as
	 * it is, and the fallback is never read.
	 *
	 * @param <T>      the type of the value
	 * @param value    the value
	 * @param fallback what a null or blank value means, even when it is itself null
	 *                 or blank
	 * @return the value, or the fallback when the value or its text is null or
	 *         blank
	 */
	public static <T> T defaultIfBlank(T value, T fallback) {
		return trimToNull(value) == null ? fallback : value;
	}

	/**
	 * Read a flag that may come as text, such as a request parameter or a
	 * configuration entry, using the fallback when it is missing. Only {@code true}
	 * in any case reads as true, so other text such as {@code yes} or {@code 1}
	 * reads as false.
	 *
	 * @param value    the text, such as {@code "true"}
	 * @param fallback what a null or blank value means
	 * @return true when the text is {@code true} in any case, false for anything
	 *         else, and the fallback when it is null or blank
	 */
	public static boolean parseBoolean(String value, boolean fallback) {
		String trimmed = trimToNull(value);
		return trimmed == null ? fallback : Boolean.parseBoolean(trimmed);
	}

	/**
	 * Read a whole number. A number written as a decimal with nothing after the
	 * point, such as {@code 10.0} from JSON, is the whole number it is.
	 *
	 * @param value the text
	 * @return the number, or null when the text is null or blank
	 * @throws NumberFormatException when the text is not a whole number that fits
	 *                               an int
	 */
	public static Integer parseWholeNumber(String value) {
		String trimmed = trimToNull(value);
		if (trimmed == null) {
			return null;
		}
		try {
			return new BigDecimal(trimmed).intValueExact();
		} catch (ArithmeticException e) {
			throw new NumberFormatException("Not a whole number: " + trimmed);
		}
	}

	/**
	 * Read a number that may come as a number or as text, such as a value read from
	 * JSON or a form.
	 *
	 * @param value the value
	 * @return the number as it is, the text read as a decimal, or null when the
	 *         value is null or blank
	 * @throws NumberFormatException when the value is text that is not a number,
	 *                               which callers can answer as a bad request
	 */
	public static Number toNumberOrNull(Object value) {
		if (value instanceof Number number) {
			return number;
		}
		String trimmed = trimToNull(value);
		if (trimmed == null) {
			return null;
		}
		try {
			return Double.valueOf(trimmed);
		} catch (NumberFormatException e) {
			throw new NumberFormatException("Not a number: " + trimmed);
		}
	}

	/**
	 * Read a positive whole number that may come as a number or as text, such as a
	 * token amount read from request parameters, without failing on anything else.
	 *
	 * @param value the value
	 * @return the number's long value (a decimal number is cut to a whole one), or
	 *         the text read as a whole number; null when the value is null, not a
	 *         number, or not above 0
	 */
	public static Long toPositiveLongOrNull(Object value) {
		long number;
		if (value instanceof Number numberValue) {
			number = numberValue.longValue();
		} else {
			String trimmed = trimToNull(value);
			if (trimmed == null) {
				return null;
			}
			try {
				number = Long.parseLong(trimmed);
			} catch (NumberFormatException e) {
				return null;
			}
		}
		return number > 0 ? number : null;
	}

	/**
	 * Read a set of values that may come as one value, a collection, nested
	 * collections, or comma separated text, or any mixture of them.
	 *
	 * @param value       the value
	 * @param splitCommas whether one value can be a comma separated list, which a
	 *                    file name cannot be, since it can hold a comma
	 * @return the values as text, each trimmed, without blanks, in order
	 */
	public static List<String> splitValues(Object value, boolean splitCommas) {
		List<String> values = new ArrayList<>();
		addValues(values, value, splitCommas);
		return values;
	}

	/**
	 * Add a value to the list, walking into collections and splitting text on
	 * commas when asked. Each entry is trimmed and blank entries are dropped.
	 *
	 * @param values      the list to add to
	 * @param value       the value, a collection of values, or null
	 * @param splitCommas whether text is split on commas
	 */
	private static void addValues(List<String> values, Object value, boolean splitCommas) {
		if (value == null) {
			return;
		}
		if (value instanceof Collection<?> collection) {
			for (Object item : collection) {
				addValues(values, item, splitCommas);
			}
			return;
		}
		String[] entries = splitCommas ? value.toString().split(",") : new String[] { value.toString() };
		for (String entry : entries) {
			String trimmed = trimToNull(entry);
			if (trimmed != null) {
				values.add(trimmed);
			}
		}
	}
}
