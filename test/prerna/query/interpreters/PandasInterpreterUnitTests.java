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
package prerna.query.interpreters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;

class PandasInterpreterUnitTests {
	@Test
	void nullOnlyComparisonsAlwaysProduceAFullColumnMask() {
		for (SemossDataType type : List.of(SemossDataType.INT, SemossDataType.DOUBLE,
				SemossDataType.STRING, SemossDataType.DATE, SemossDataType.TIMESTAMP)) {
			for (String comparator : List.of("==", "!=")) {
				String expression = render(type, SimpleQueryFilter.makeColToValFilter("ID", comparator, null));
				assertFalse(expression.isBlank(), type + " null filter must not produce an empty mask");
				assertTrue(expression.contains(".isna()"), expression);
				assertFalse(expression.contains(".isin"), expression);
				assertFalse(expression.contains("pd.to_datetime"), expression);
				assertTrue(expression.contains("~") == comparator.equals("!="), expression);
			}
		}
	}

	@Test
	void numericPurgePredicateHasNoEmptyOrOperand() {
		String expression = render(SemossDataType.INT, new OrQueryFilter(
				SimpleQueryFilter.makeColToValFilter("ID", "!=", 3),
				SimpleQueryFilter.makeColToValFilter("ID", "==", null)));
		assertTrue(expression.contains(".isna()"), expression);
		assertFalse(expression.matches("(?s).*\\|\\s*\\(\\s*\\).*"), expression);
		assertTrue(expression.contains("x!=3"), expression);
	}

	private String render(SemossDataType type, IQueryFilter filter) {

		PandasInterpreter interpreter = new PandasInterpreter();
		interpreter.setDataTableName("frame", "framew.cache['data']");
		interpreter.setDataTypeMap(Map.of("ID", type));
		StringBuilder expression = new StringBuilder();
		interpreter.addFilters(List.of(filter), "frame", expression, true);
		return expression.toString();
	}

	@Test
	void numericNotInRequiresEveryValueToDiffer() {
		String expression = render(SemossDataType.INT, SimpleQueryFilter.makeColToValFilter("ID", "!=", List.of(1, 2, 3)));
		assertTrue(expression.contains(" & "), expression);
		assertFalse(expression.contains(" | "), expression);
		String equality = render(SemossDataType.INT, SimpleQueryFilter.makeColToValFilter("ID", "==", List.of(1, 2, 3)));
		assertTrue(equality.contains(" | "), equality);
	}

	@Test
	void nullMembershipUsesOrForEqualityAndAndForInequality() {
		for (SemossDataType type : List.of(SemossDataType.INT, SemossDataType.STRING, SemossDataType.DATE)) {
			Object value = type == SemossDataType.INT ? 1 : type == SemossDataType.DATE ? "2026-10-14" : "active";
			String equality = render(type, SimpleQueryFilter.makeColToValFilter("ID", "==", java.util.Arrays.asList(value, null)));
			assertTrue(equality.contains(".isna()") && equality.contains(" | "), equality);
			String inequality = render(type, SimpleQueryFilter.makeColToValFilter("ID", "!=", java.util.Arrays.asList(value, null)));
			assertTrue(inequality.contains(".isna()") && inequality.contains(" & "), inequality);
		}
	}
}