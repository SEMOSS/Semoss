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
package prerna.query.parsers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.query.querystruct.SelectQueryStruct;

/**
 * Covers the opaque parser, which keeps each selector as raw text rather than
 * decomposing it, and is used where the query only needs to be described rather
 * than rewritten.
 */
public class OpaqueSqlParserUnitTests {

	private static SelectQueryStruct parse(String sql) {
		try {
			return new OpaqueSqlParser().processQuery(sql);
		} catch (Exception e) {
			throw new AssertionError("Expected to parse: " + sql, e);
		}
	}

	@Test
	void eachSelectItemProducesExactlyOneSelector() {
		assertEquals(3, parse("select a, b, c from t").getSelectors().size());
	}

	@Test
	void expressionsAndAliasesEachCountOnce() {
		assertEquals(3, parse("select a, (b + 1) as x, count(c) as n from t").getSelectors().size());
	}

	@Test
	void starCollapsesToASingleOpaqueSelector() {
		assertEquals(1, parse("select * from t").getSelectors().size());
	}

	@Test
	void aliasesAreCarriedOntoTheSelectors() {
		SelectQueryStruct qs = parse("select a, (b + 1) as x from t");

		assertTrue(qs.getSelectors().toString().contains("x"), () -> qs.getSelectors().toString());
	}

	@ParameterizedTest
	@ValueSource(strings = { "select a from t where (a = 1)", "select a from t where (a = 1 and b = 2)",
			"select a from t where (a = 1) or b = 2", "select a from t1 join t2 on t1.id = t2.id",
			"select a from t where a in ('p','q')" })
	void filterShapesAreHandledWithoutBlowingUp(String sql) {
		assertTrue(parse(sql).getSelectors().size() > 0, () -> "no selectors for " + sql);
	}

	@Test
	void aParenthesizedPredicateIsDescendedInto() {
		// the parser reaches inside the parentheses, so the filter is not silently
		// dropped the way it would be if the wrapper type went unrecognised
		SelectQueryStruct qs = parse("select a from tab where (a = 'x')");

		assertTrue(!qs.getExplicitFilters().isEmpty() || !qs.getImplicitFilters().isEmpty(),
				"expected the parenthesized filter to register");
	}
}
