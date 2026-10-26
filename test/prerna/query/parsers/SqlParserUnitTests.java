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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import prerna.query.querystruct.GenExpression;

/**
 * Covers the expression tree SqlParser builds: that a query survives a round
 * trip through it, that the constructs the tree models are actually reached,
 * and that constants get discovered and swapped as parameters.
 */
public class SqlParserUnitTests {

	private static GenExpressionWrapper parse(String sql) {
		try {
			return new SqlParser().processQuery(sql);
		} catch (Exception e) {
			throw new AssertionError("Expected to parse: " + sql, e);
		}
	}

	/**
	 * Render the tree back out with its parameters filled in with the original
	 * constants.
	 */
	private static String renderWithConstants(String sql) {
		try {
			GenExpressionWrapper wrapper = parse(sql);
			wrapper.fillParameters();
			return wrapper.generateQuery(true);
		} catch (Exception e) {
			throw new AssertionError("Expected to render: " + sql, e);
		}
	}

	private static String normalize(String sql) {
		return sql.replaceAll("\\s+", " ").trim().toUpperCase();
	}

	// ----- round trip -----

	@ParameterizedTest
	@ValueSource(strings = { "select a from t", "select distinct a, b from t", "select a from t where a = 'x'",
			"select a from t where (a = 'x' and b > 1) or c = 2", "select a from t1 join t2 on t1.id = t2.id",
			"select a from t1 left join t2 on t1.id = t2.id", "select a from t1, t2", "select a from t1 cross join t2",
			"select a from (select b as a from t) q", "select a from t where a in ('p','q')",
			"select a from t where a in (select b from u)", "select a from t where a between 1 and 5",
			"select count(a), max(b) from t group by c", "select a from t order by a desc",
			"select a from t limit 10 offset 5", "select a from t1 union select b from t2",
			"select case when a = 1 then 'x' else 'y' end as c from t", "select a from t where a is null" })
	void queriesSurviveARoundTrip(String sql) {
		// generateQuery(true) re-parses its own output, so this proves the render is
		// still valid SQL as well as non-empty
		String rendered = renderWithConstants(sql);

		assertNotNull(rendered);
		assertFalse(rendered.trim().isEmpty(), () -> "rendered nothing for " + sql);
	}

	@Test
	void aParenthesizedPredicateIsPreservedRatherThanDropped() {
		String rendered = renderWithConstants("select a from t where (a = 'x' and b > 1) or c = 2");

		// all three comparisons have to survive, not just the unparenthesized one
		assertTrue(normalize(rendered).contains("A = 'X'"), () -> rendered);
		assertTrue(normalize(rendered).contains("B > 1"), () -> rendered);
		assertTrue(normalize(rendered).contains("C = 2"), () -> rendered);
	}

	@ParameterizedTest
	@ValueSource(strings = { "select a from t1, t2", "select a from t1 cross join t2",
			"select a from t1 join t2 using (id)" })
	void joinsWithoutAnOnClauseDoNotBlowUp(String sql) {
		assertNotNull(parse(sql).root, () -> "no tree built for " + sql);
	}

	// ----- what the tree knows about the query -----

	@Test
	void realTablesAndTheirColumnsAreReported() {
		Map<String, List<GenExpression>> tableColumns = new SqlParser().getTableColumns(
				"select q.a, count(q.b) from (select a, b from t1 join t2 on t1.id = t2.id where t1.c in (1,2)) q "
						+ "group by q.a");

		assertNotNull(tableColumns);
		// t1 is a real table; q is a derived one and must not appear
		assertTrue(tableColumns.containsKey("t1"), () -> "tables were " + tableColumns.keySet());
		assertFalse(tableColumns.containsKey("q"), () -> "tables were " + tableColumns.keySet());
	}

	@Test
	void selectorsAreReportedPerNestingLevel() {
		Map<Integer, List<GenExpression>> levels = new SqlParser()
				.getLevelColumns("select q.a from (select a from t) q");

		assertNotNull(levels);
		assertFalse(levels.isEmpty(), "expected at least the outer level");
	}

	@Test
	void subSelectsAreCounted() {
		assertEquals(0, parse("select a from t").numSubSelects);
		assertTrue(parse("select a from (select a from t) q").numSubSelects > 0);
	}

	@Test
	void theFromTableAliasIsResolvedBackToTheTableName() {
		GenExpressionWrapper wrapper = parse("select t.a from tab t join usr u on t.id = u.id");

		assertEquals("tab", wrapper.tableAlias.get("t"));
	}

	@Test
	void joinedTableAliasesAreResolvedBackToTheTableName() {
		GenExpressionWrapper wrapper = parse("select t.a from tab t join usr u on t.id = u.id");

		assertEquals("usr", wrapper.tableAlias.get("u"));
	}

	@Test
	void aColumnQualifiedByAJoinAliasIsAttributedToTheRealTable() {
		GenExpressionWrapper wrapper = parse("select t.a from tab t join usr u on t.id = u.id where u.name = 'x'");

		assertTrue(wrapper.getParams().stream().anyMatch(p -> "usr".equals(p.getTableName())),
				() -> "tables were " + wrapper.getParams().stream().map(ParamStructDetails::getTableName).toList());
	}

	@Test
	void aUnionIsModelledAsASingleOperationOverBothSides() {
		String rendered = renderWithConstants("select a from t1 union select b from t2");

		assertTrue(normalize(rendered).contains("UNION"), () -> rendered);
		assertTrue(normalize(rendered).contains("T1"), () -> rendered);
		assertTrue(normalize(rendered).contains("T2"), () -> rendered);
	}

	// ----- parameterization -----

	@Test
	void constantsComparedAgainstAColumnBecomeParameters() {
		GenExpressionWrapper wrapper = parse(
				"select a from tab t where t.a = 'x' and t.c between 1 and 5 and t.d in ('p','q')");

		Map<String, Object> params = wrapper.getAllParamNames();

		assertEquals(4, params.size(), () -> "params were " + params.keySet());
		assertTrue(params.keySet().stream().anyMatch(k -> k.contains("between.start")),
				() -> "params were " + params.keySet());
		assertTrue(params.keySet().stream().anyMatch(k -> k.contains("between.end")),
				() -> "params were " + params.keySet());
		assertTrue(params.keySet().stream().anyMatch(k -> k.contains("in")), () -> "params were " + params.keySet());
	}

	@Test
	void theOriginalConstantIsKeptAsTheParametersCurrentValue() {
		GenExpressionWrapper wrapper = parse("select a from tab t where t.a = 'x'");

		String key = wrapper.getAllParamNames().keySet().iterator().next();

		assertEquals("'x'", String.valueOf(wrapper.getCurrentValueOfParam(key)));
	}

	@Test
	void settingAParameterChangesTheRenderedQuery() throws Exception {
		GenExpressionWrapper wrapper = parse("select a from tab t where t.a = 'x'");
		String key = wrapper.getAllParamNames().keySet().iterator().next();

		assertTrue(wrapper.setCurrentValueOfParam(key, "'zzz'"));
		wrapper.fillParameters();

		String rendered = wrapper.generateQuery(true);
		assertTrue(rendered.contains("'zzz'"), () -> rendered);
		assertFalse(rendered.contains("'x'"), () -> rendered);
	}

	@Test
	void anUnknownParameterNameIsRejectedRatherThanSilentlyIgnored() {
		GenExpressionWrapper wrapper = parse("select a from tab t where t.a = 'x'");

		assertFalse(wrapper.setCurrentValueOfParam("no_such_param", "1"));
		assertEquals(null, wrapper.getCurrentValueOfParam("no_such_param"));
	}

	@Test
	void unfilledParametersRenderAsPlaceholders() throws Exception {
		GenExpressionWrapper wrapper = parse("select a from tab t where t.a = 'x'");

		String rendered = wrapper.generateQuery(false);

		assertTrue(rendered.contains("<") && rendered.contains(">"), () -> rendered);
	}

	@Test
	void aCaseExpressionIsNotParameterized() {
		// the branches of a CASE are rendered verbatim, so its constants must not be
		// pulled out as parameters
		GenExpressionWrapper wrapper = parse("select case when a = 1 then 'x' else 'y' end as c from tab");

		assertTrue(wrapper.getAllParamNames().isEmpty(), () -> "params were " + wrapper.getAllParamNames().keySet());
	}

	@Test
	void columnsReadByAFunctionCanBeLookedUp() {
		GenExpressionWrapper wrapper = parse("select count(t.b) as n from tab t group by t.a");

		assertTrue(wrapper.getColumnsForFunction("count").stream().anyMatch(c -> c.endsWith("b")),
				() -> "columns were " + wrapper.getColumnsForFunction("count"));
	}

	@Test
	void transformingAQueryKeepsTheNamedParametersAsPlaceholders() {
		String sql = "select a from tab t where t.a = 'x' and t.b = 'y'";
		List<ParamStructDetails> params = parse(sql).getParams();
		assertEquals(2, params.size());

		// hand back one of the two, so that one stays a placeholder for the caller to
		// substitute while the other is filled back in with its original constant
		String transformed = GenExpressionWrapper.transformQueryWithParams(sql, params.subList(0, 1));

		assertNotNull(transformed);
		assertTrue(transformed.contains("<"), () -> transformed);
		assertTrue(transformed.contains("'x'") || transformed.contains("'y'"), () -> transformed);
	}

	@Test
	void transformingWithNoParametersRendersRunnableSql() throws Exception {
		String sql = "select a from tab t where t.a = 'x'";

		String transformed = GenExpressionWrapper.transformQueryWithParams(sql, List.of());

		// nothing was held back, so every constant is filled and the result parses
		assertNotNull(CCJSqlParserUtil.parse(transformed));
		assertTrue(transformed.contains("'x'"), () -> transformed);
	}
}
