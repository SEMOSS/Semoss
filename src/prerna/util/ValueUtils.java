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
	 * @param value the text
	 * @return whether it is null, or empty once trimmed
	 */
	public static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}

	/**
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
	 * @param value the text
	 * @return the text trimmed, or an empty string when it is null
	 */
	public static String trimToEmpty(String value) {
		return value == null ? "" : value.trim();
	}

	/**
	 * @param value the value
	 * @return the value as text, or null when it is null
	 */
	public static String toStringOrNull(Object value) {
		return value == null ? null : value.toString();
	}

	/**
	 * @param value the value
	 * @return the value as text, or an empty string when it is null
	 */
	public static String toStringOrEmpty(Object value) {
		return value == null ? "" : value.toString();
	}

	/**
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
