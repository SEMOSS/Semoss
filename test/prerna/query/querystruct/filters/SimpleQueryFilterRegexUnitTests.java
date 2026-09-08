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
package prerna.query.querystruct.filters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.google.re2j.PatternSyntaxException;

class SimpleQueryFilterRegexUnitTests {

	@Test
	void matchesWhenOtherFilterContainsRegex() {
		SimpleQueryFilter literalFilter = filter("==", "release-warning.log");
		SimpleQueryFilter regexFilter = filter("?like", "warn(?:ing)?");

		assertTrue(literalFilter.isOverlappingRegexValues(regexFilter, true));
	}

	@Test
	void matchesWhenThisFilterContainsRegexAndComparisonIsBidirectional() {
		SimpleQueryFilter regexFilter = filter("?like", "warn(?:ing)?");
		SimpleQueryFilter literalFilter = filter("==", "release-warning.log");

		assertTrue(regexFilter.isOverlappingRegexValues(literalFilter, true));
		assertFalse(regexFilter.isOverlappingRegexValues(literalFilter, false));
	}

	@Test
	void preservesCaseInsensitiveMatching() {
		SimpleQueryFilter literalFilter = filter("==", "BUILD FAILURE");
		SimpleQueryFilter regexFilter = filter("?like", "failure");

		assertTrue(literalFilter.isOverlappingRegexValues(regexFilter, true));
	}

	@Test
	void rejectsUnsupportedLookaroundExpressions() {
		SimpleQueryFilter literalFilter = filter("==", "secretvalue");
		SimpleQueryFilter regexFilter = filter("?like", "secret(?=value)");

		assertThrows(PatternSyntaxException.class,
				() -> literalFilter.isOverlappingRegexValues(regexFilter, true));
	}

	@Test
	void evaluatesNestedQuantifiersInLinearTime() {
		SimpleQueryFilter literalFilter = filter("==", "a".repeat(200_000) + "!");
		SimpleQueryFilter regexFilter = filter("?like", "(a+)+$");

		assertTimeoutPreemptively(Duration.ofSeconds(1),
				() -> assertFalse(literalFilter.isOverlappingRegexValues(regexFilter, true)));
	}

	private static SimpleQueryFilter filter(String comparator, String value) {
		return SimpleQueryFilter.makeColToValFilter("TABLE__COLUMN", comparator, value);
	}
}
