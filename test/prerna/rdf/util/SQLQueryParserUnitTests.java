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
package prerna.rdf.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers the parser that turns a SQL SELECT into nodes, properties and triples
 * for the RDF side.
 */
public class SQLQueryParserUnitTests {

	private static SQLQueryParser parsed(String sql) {
		SQLQueryParser parser = new SQLQueryParser(sql);
		parser.setQuery(sql);
		parser.parseQuery();
		return parser;
	}

	@Test
	void tablesAndAliasesAreCollected() {
		Map<String, String> nodes = parsed(
				"select t.title, n.nominated from Title t join Nominated n on t.Title = n.Title_FK")
				.getNodesFromQuery();

		assertFalse(nodes.isEmpty(), "expected the joined tables to register as nodes");
	}

	@Test
	void anExplicitJoinBecomesATriple() {
		List<String[]> triples = parsed(
				"select t.title, n.nominated from Title t join Nominated n on t.Title = n.Title_FK").getTriplesData();

		assertFalse(triples.isEmpty(), "expected the ON predicate to produce a triple");
	}

	@Test
	void anImplicitJoinInTheWhereClauseBecomesATriple() {
		List<String[]> triples = parsed(
				"select n.nominated, t.title from Title t, Nominated n where n.Title_FK = t.Title").getTriplesData();

		assertFalse(triples.isEmpty(), "expected the WHERE predicate to produce a triple");
	}

	@ParameterizedTest
	@ValueSource(strings = { "select t.title from Title t where (t.title = 'a' or t.title = 'b')",
			"select t.title from Title t where (t.title = 'a' and t.title = 'b')",
			"select t.title from Title t where t.title = 'a'",
			"select n.nominated from Title t, Nominated n where (n.Title_FK = t.Title)" })
	void parenthesizedWhereClausesAreDescendedInto(String sql) {
		// a parenthesized predicate is its own node type, and mistaking it for a plain
		// binary expression used to blow up here
		assertNotNull(parsed(sql).getReturnVariables(), () -> "failed on " + sql);
	}

	@ParameterizedTest
	@ValueSource(strings = { "select t.title from Title t where t.budget > 1",
			"select t.title from Title t where t.budget >= 1 and t.title = 'a'",
			"select t.title from Title t where t.title is not null",
			"select t.title from Title t where t.title is null", "select t.title from Title t where t.title like 'a%'",
			"select t.title from Title t where (t.budget > 1 or t.budget < 0)" })
	void whereClausesOtherThanEqualityAreWalkedWithoutBlowingUp(String sql) {
		// the walk stops when the left hand side is no longer a comparison, so a
		// non equality operator no longer runs off the end of the expression
		assertNotNull(parsed(sql).getReturnVariables(), () -> "failed on " + sql);
	}

	@Test
	void anUnqualifiedColumnInTheWhereClauseIsToleratedRatherThanNullPointing() {
		assertNotNull(parsed("select t.title from Title t where budget > 1").getReturnVariables());
	}

	@Test
	void returnVariablesSurviveAUnion() {
		assertNotNull(parsed("select t.title as TITLE from Title t union select t.title as TITLE from Title t")
				.getReturnVariables());
	}

	@Test
	void anAggregateInTheProjectionIsDetected() {
		assertTrue(parsed("select count(n.nominated) as c from Nominated n").hasAggregateFunction());
	}

	@Test
	void aPlainProjectionIsNotAnAggregate() {
		assertFalse(parsed("select n.nominated from Nominated n").hasAggregateFunction());
	}

	@Test
	void unparseableSqlIsLoggedRatherThanThrown() {
		// parseQuery swallows parse failures by design, leaving the parser empty
		assertTrue(parsed("this is not sql at all").getTriplesData().isEmpty());
	}
}
