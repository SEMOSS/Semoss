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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Checks reading text that must be there, objects as trimmed text, fallbacks
 * for blank values, keys to compare text by, and numbers that may come as text.
 */
@Isolated("Changes the default locale")
class ValueUtilsUnitTests {

	private static final String MESSAGE = "Must define the group id";

	private static final String EM_SPACE = "\u2003";

	@Test
	void requireNonBlank_returnsTheTextTrimmed() {
		assertEquals("Sales", ValueUtils.requireNonBlank("  Sales\t", MESSAGE));
		assertEquals("Sales Team", ValueUtils.requireNonBlank("Sales Team", MESSAGE));
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { " ", "\t", " \n " })
	void requireNonBlank_rejectsMissingTextWithTheMessage(String value) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> ValueUtils.requireNonBlank(value, MESSAGE));
		assertEquals(MESSAGE, e.getMessage());
	}

	@Test
	void toNumberOrNull_keepsNumbersAndReadsText() {
		assertEquals(10, ValueUtils.toNumberOrNull(10));
		assertEquals(1.5, ValueUtils.toNumberOrNull(" 1.5 "));
		assertEquals(100.0, ValueUtils.toNumberOrNull("100"));
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { " ", "\t" })
	void toNumberOrNull_readsNothingAsNull(String value) {
		assertNull(ValueUtils.toNumberOrNull(value));
	}

	@Test
	void toNumberOrNull_rejectsTextThatIsNotANumber() {
		NumberFormatException e = assertThrows(NumberFormatException.class, () -> ValueUtils.toNumberOrNull("lots"));
		assertEquals("Not a number: lots", e.getMessage());
	}

	@Test
	void toPositiveLongOrNull_readsPositiveWholeNumbersFromNumbersAndText() {
		assertEquals(8192L, ValueUtils.toPositiveLongOrNull(8192));
		assertEquals(3_000_000_000L, ValueUtils.toPositiveLongOrNull(3_000_000_000L));
		assertEquals(5L, ValueUtils.toPositiveLongOrNull(5.9));
		assertEquals(4096L, ValueUtils.toPositiveLongOrNull(" 4096 "));
	}

	@Test
	void toPositiveLongOrNull_readsAnythingElseAsNull() {
		assertNull(ValueUtils.toPositiveLongOrNull(null));
		assertNull(ValueUtils.toPositiveLongOrNull(" "));
		assertNull(ValueUtils.toPositiveLongOrNull("lots"));
		assertNull(ValueUtils.toPositiveLongOrNull("1.5"));
		assertNull(ValueUtils.toPositiveLongOrNull(0));
		assertNull(ValueUtils.toPositiveLongOrNull(-10));
		assertNull(ValueUtils.toPositiveLongOrNull("-10"));
		assertNull(ValueUtils.toPositiveLongOrNull(Boolean.TRUE));
	}

	@Test
	void trimToEmpty_readsAValueAsTrimmedText() {
		assertEquals("", ValueUtils.trimToEmpty((Object) null));
		assertEquals("", ValueUtils.trimToEmpty((Object) ""));
		assertEquals("", ValueUtils.trimToEmpty((Object) " \t\n "));
		assertEquals("Sales Team", ValueUtils.trimToEmpty((Object) "  Sales Team\t"));
		assertEquals("Sales  Team", ValueUtils.trimToEmpty((Object) "Sales  Team"));
		assertEquals("42", ValueUtils.trimToEmpty(Integer.valueOf(42)));
		assertEquals("false", ValueUtils.trimToEmpty(Boolean.FALSE));
		assertEquals("Sales", ValueUtils.trimToEmpty(new StringBuilder("  Sales  ")));
		assertEquals(EM_SPACE, ValueUtils.trimToEmpty((Object) EM_SPACE));
	}

	@Test
	void trimToEmpty_readsTextAndANullLiteralAsText() {
		assertEquals("", ValueUtils.trimToEmpty(null));
		assertEquals("", ValueUtils.trimToEmpty((String) null));
		assertEquals("", ValueUtils.trimToEmpty(" \t\n "));
		assertEquals("Sales Team", ValueUtils.trimToEmpty("  Sales Team\t"));
		assertEquals("Sales  Team", ValueUtils.trimToEmpty("Sales  Team"));
	}

	@Test
	void trimToEmpty_readsAValueWithoutTextAsEmpty() {
		assertEquals("", ValueUtils.trimToEmpty(new Described(null)));
	}

	@Test
	void trimToEmpty_letsAFailureToReadTheValuePropagate() {
		IllegalStateException e = assertThrows(IllegalStateException.class,
				() -> ValueUtils.trimToEmpty(new Unreadable()));
		assertEquals(Unreadable.MESSAGE, e.getMessage());
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { " ", " \t\n " })
	void defaultIfBlank_usesTheFallbackForMissingText(String value) {
		String fallback = "default";
		assertSame(fallback, ValueUtils.defaultIfBlank(value, fallback));
	}

	@Test
	void defaultIfBlank_keepsSuppliedTextWithItsSpaces() {
		String value = "  supplied  ";
		assertSame(value, ValueUtils.defaultIfBlank(value, "default"));
	}

	@Test
	void defaultIfBlank_returnsTheFallbackAsItIs() {
		assertNull(ValueUtils.defaultIfBlank("", null));
		String blank = "  ";
		assertSame(blank, ValueUtils.defaultIfBlank(null, blank));
	}

	@Test
	void defaultIfBlank_keepsZeroAndFalse() {
		Integer zero = Integer.valueOf(0);
		assertSame(zero, ValueUtils.defaultIfBlank(zero, Integer.valueOf(10)));
		assertSame(Boolean.FALSE, ValueUtils.defaultIfBlank(Boolean.FALSE, Boolean.TRUE));
	}

	@Test
	void defaultIfBlank_returnsTheChosenObjectUnchanged() {
		StringBuilder text = new StringBuilder(" text ");
		StringBuilder spaces = new StringBuilder("   ");
		StringBuilder fallback = new StringBuilder("fallback");

		assertSame(text, ValueUtils.defaultIfBlank(text, fallback));
		assertSame(fallback, ValueUtils.defaultIfBlank(spaces, fallback));

		assertEquals(" text ", text.toString());
		assertEquals("   ", spaces.toString());
		assertEquals("fallback", fallback.toString());
	}

	@Test
	void defaultIfBlank_keepsTextThatTrimDoesNotRemove() {
		assertSame(EM_SPACE, ValueUtils.defaultIfBlank(EM_SPACE, "default"));
	}

	@Test
	void defaultIfBlank_neverReadsTheFallback() {
		Described fallback = new Described("fallback");
		Described present = new Described("present");

		assertSame(fallback, ValueUtils.defaultIfBlank(null, fallback));
		assertSame(fallback, ValueUtils.defaultIfBlank(new Described(" "), fallback));
		assertSame(present, ValueUtils.defaultIfBlank(present, fallback));

		assertEquals(0, fallback.reads);
	}

	@Test
	void defaultIfBlank_usesTheFallbackForAValueWithoutText() {
		String fallback = "default";
		assertSame(fallback, ValueUtils.defaultIfBlank(new Described(null), fallback));
	}

	@Test
	void defaultIfBlank_letsAFailureToReadTheValuePropagate() {
		IllegalStateException e = assertThrows(IllegalStateException.class,
				() -> ValueUtils.defaultIfBlank(new Unreadable(), "default"));
		assertEquals(Unreadable.MESSAGE, e.getMessage());
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { " ", " \t\n " })
	void lowerCaseTrimToEmpty_readsMissingTextAsEmpty(String value) {
		assertEquals("", ValueUtils.lowerCaseTrimToEmpty(value));
	}

	@Test
	void lowerCaseTrimToEmpty_trimsAndLowerCasesText() {
		assertEquals("sales team", ValueUtils.lowerCaseTrimToEmpty("  Sales Team\t"));
		assertEquals("a  b-c_1", ValueUtils.lowerCaseTrimToEmpty("  A  B-C_1  "));
		assertEquals(EM_SPACE + "title" + EM_SPACE, ValueUtils.lowerCaseTrimToEmpty(EM_SPACE + "TITLE" + EM_SPACE));
	}

	@ParameterizedTest
	@ValueSource(strings = { "en-US", "tr-TR" })
	void lowerCaseTrimToEmpty_isIndependentOfTheDefaultLocale(String languageTag) {
		Locale previous = Locale.getDefault();
		try {
			Locale.setDefault(Locale.forLanguageTag(languageTag));
			assertEquals("title", ValueUtils.lowerCaseTrimToEmpty("TITLE"));
		} finally {
			Locale.setDefault(previous);
		}
	}

	/**
	 * A value with the given text, counting how often its text is read.
	 */
	private static final class Described {

		private final String text;
		private int reads;

		private Described(String text) {
			this.text = text;
		}

		@Override
		public String toString() {
			reads++;
			return text;
		}
	}

	/**
	 * A value whose text cannot be read.
	 */
	private static final class Unreadable {

		private static final String MESSAGE = "No text";

		@Override
		public String toString() {
			throw new IllegalStateException(MESSAGE);
		}
	}
}
