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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import prerna.query.parsers.ParamStructDetails.LEVEL;
import prerna.query.parsers.ParamStructDetails.QUOTE;

/**
 * Covers the wrapper that holds everything the parser learned about a query:
 * the parameter index chain, the scope rules that decide which comparisons a
 * user defined parameter claims, and the lineage walk that resolves a
 * projection back to a physical column.
 */
public class GenExpressionWrapperUnitTests {

	private static final String TWO_TABLE_QUERY = "select c.acctid from clms c join mbrshp m on c.id = m.id "
			+ "where c.acctid = 'a1' and m.acctid = 'a2' and c.acctid > 'a0'";

	private static GenExpressionWrapper parse(String sql) {
		try {
			return new SqlParser().processQuery(sql);
		} catch (Exception e) {
			throw new AssertionError("Expected to parse: " + sql, e);
		}
	}

	/** Build the user facing parameter definition the fill routine expects. */
	private static Map<ParamStructDetails, ParamStruct> lookup(ParamStructDetails details, String userName,
			String modelDisplay) {
		ParamStruct struct = new ParamStruct();
		struct.setParamName(userName);
		struct.setModelDisplay(modelDisplay);
		Map<ParamStructDetails, ParamStruct> map = new HashMap<>();
		map.put(details, struct);
		return map;
	}

	/**
	 * A parameter definition claiming the given scope over one of the parsed
	 * parameters.
	 */
	private static ParamStructDetails claim(ParamStructDetails parsed, LEVEL level) {
		ParamStructDetails claim = new ParamStructDetails();
		claim.setLevel(level);
		claim.setColumnName(parsed.getColumnName());
		claim.setTableName(parsed.getTableName());
		claim.setOperator(parsed.getOperator());
		claim.setuOperator(parsed.getuOperator());
		return claim;
	}

	private static ParamStructDetails findParsed(GenExpressionWrapper wrapper, String table, String operator) {
		return wrapper.getParams().stream()
				.filter(p -> table.equals(p.getTableName()) && operator.equals(p.getOperator())).findFirst()
				.orElseThrow(() -> new AssertionError("no parsed param for " + table + " " + operator));
	}

	// ----- the index chain -----

	@Test
	void theIndexChainIsPopulatedAtEveryLevel() {
		GenExpressionWrapper wrapper = parse(TWO_TABLE_QUERY);

		assertTrue(wrapper.columnTableIndex.containsKey("acctid"),
				() -> "column level was " + wrapper.columnTableIndex.keySet());
		assertEquals(2, wrapper.columnTableIndex.get("acctid").size(),
				() -> "expected both tables under acctid, got " + wrapper.columnTableIndex.get("acctid"));
		assertTrue(wrapper.columnTableOperatorIndex.containsKey("clms_acctid"),
				() -> "table level was " + wrapper.columnTableOperatorIndex.keySet());
		assertEquals(3, wrapper.operatorTableColumnParamIndex.size(),
				() -> "operator level was " + wrapper.operatorTableColumnParamIndex.keySet());
	}

	@Test
	void replacingAtColumnLevelReachesEveryTableAndOperator() {
		GenExpressionWrapper wrapper = parse(TWO_TABLE_QUERY);

		wrapper.replaceColumn("acctid", "'ZZZ'");

		assertTrue(wrapper.getParams().stream().allMatch(p -> "'ZZZ'".equals(p.getCurrentValue())),
				() -> "values were " + wrapper.getAllParamNames());
	}

	@Test
	void replacingAtTableLevelLeavesTheOtherTableAlone() {
		GenExpressionWrapper wrapper = parse(TWO_TABLE_QUERY);

		wrapper.replaceTableColumn("clms_acctid", "'ZZZ'");

		assertTrue(wrapper.getParams().stream().filter(p -> "clms".equals(p.getTableName()))
				.allMatch(p -> "'ZZZ'".equals(p.getCurrentValue())));
		assertTrue(wrapper.getParams().stream().filter(p -> "mbrshp".equals(p.getTableName()))
				.noneMatch(p -> "'ZZZ'".equals(p.getCurrentValue())));
	}

	@Test
	void replacingAtOperatorLevelTouchesOnlyThatComparison() {
		GenExpressionWrapper wrapper = parse(TWO_TABLE_QUERY);
		ParamStructDetails target = findParsed(wrapper, "clms", "=");

		wrapper.replaceTableColumnOperator(target.getParamKey(), "'ZZZ'");

		assertEquals("'ZZZ'", target.getCurrentValue());
		assertEquals(1, wrapper.getParams().stream().filter(p -> "'ZZZ'".equals(p.getCurrentValue())).count());
	}

	// ----- scope rules for user defined parameter names -----

	@ParameterizedTest
	@CsvSource({ "COLUMN, 3", "TABLE, 2", "OPERATOR, 1", "OPERATORU, 1" })
	void aUserParameterClaimsExactlyTheComparisonsInItsScope(String level, int expectedClaimed) {
		GenExpressionWrapper wrapper = parse(TWO_TABLE_QUERY);
		int parsedCount = wrapper.getParams().size();
		ParamStructDetails claim = claim(findParsed(wrapper, "clms", "="), LEVEL.valueOf(level));

		wrapper.fillParameters(List.of(claim), lookup(claim, "myParam", "text"));

		// every claimed parameter is removed from the fill map, so what is left is what
		// fell back to its original constant
		assertEquals(parsedCount - expectedClaimed, wrapper.paramToExpressionMap.size(),
				() -> level + " claimed the wrong number of comparisons");
	}

	@Test
	void aClaimedParameterRendersUnderItsUserFacingName() throws Exception {
		GenExpressionWrapper wrapper = parse("select a from clms c where c.acctid = 'a1'");
		ParamStructDetails claim = claim(findParsed(wrapper, "clms", "="), LEVEL.OPERATORU);

		wrapper.fillParameters(List.of(claim), lookup(claim, "myParam", "text"));

		String rendered = wrapper.generateQuery(false);
		assertTrue(rendered.contains("<myParam>"), () -> rendered);
		assertFalse(rendered.contains("'a1'"), () -> rendered);
	}

	@Test
	void unclaimedParametersFallBackToTheOriginalConstant() throws Exception {
		GenExpressionWrapper wrapper = parse("select a from clms c where c.acctid = 'a1' and c.other = 'b1'");
		ParamStructDetails parsed = wrapper.getParams().stream().filter(p -> "acctid".equals(p.getColumnName()))
				.findFirst().orElseThrow();
		ParamStructDetails claim = claim(parsed, LEVEL.OPERATORU);

		wrapper.fillParameters(List.of(claim), lookup(claim, "myParam", "text"));

		String rendered = wrapper.generateQuery(false);
		assertTrue(rendered.contains("<myParam>"), () -> rendered);
		assertTrue(rendered.contains("b1"), () -> rendered);
	}

	@Test
	void aDatasourceLevelParameterIsIgnoredBecauseItIsNotPartOfTheQuery() {
		GenExpressionWrapper wrapper = parse("select a from clms c where c.acctid = 'a1'");
		int before = wrapper.paramToExpressionMap.size();
		ParamStructDetails claim = claim(findParsed(wrapper, "clms", "="), LEVEL.DATASOURCE);

		wrapper.fillParameters(List.of(claim), lookup(claim, "myParam", "text"));

		assertEquals(before, wrapper.paramToExpressionMap.size(), "nothing should have been claimed");
	}

	/**
	 * Render one parameter under a user facing name and return the resulting SQL.
	 */
	private static String renderUnderUserName(QUOTE quote, String modelDisplay) throws Exception {
		GenExpressionWrapper wrapper = parse("select a from clms c where c.acctid = 'a1'");
		ParamStructDetails parsed = findParsed(wrapper, "clms", "=");
		parsed.setQuote(quote);
		ParamStructDetails claim = claim(parsed, LEVEL.OPERATORU);

		wrapper.fillParameters(List.of(claim), lookup(claim, "myParam", modelDisplay));
		return wrapper.generateQuery(false);
	}

	@Test
	void aSingleQuotedParameterKeepsItsQuotes() throws Exception {
		assertTrue(renderUnderUserName(QUOTE.SINGLE, "text").contains("'<myParam>'"));
	}

	@Test
	void aDoubleQuotedParameterKeepsItsQuotes() throws Exception {
		assertTrue(renderUnderUserName(QUOTE.DOUBLE, "text").contains("\"<myParam>\""));
	}

	@Test
	void anUnquotedParameterGetsNoQuotes() throws Exception {
		String rendered = renderUnderUserName(QUOTE.NO, "text");

		assertTrue(rendered.contains("<myParam>"), () -> rendered);
		assertFalse(rendered.contains("'<myParam>'"), () -> rendered);
	}

	@Test
	void anArrayStyleDisplayTypeSuppressesQuotingSoTheFrontEndCanSupplyIt() throws Exception {
		// checklist is in PARAM_FILL_USE_ARRAY_TYPES, meaning the caller sends a list
		String rendered = renderUnderUserName(QUOTE.SINGLE, "checklist");

		assertTrue(rendered.contains("<myParam>"), () -> rendered);
		assertFalse(rendered.contains("'<myParam>'"), () -> rendered);
	}

	// ----- lineage -----

	@Test
	void aProjectionResolvesToItsPhysicalTableAndColumn() {
		GenExpressionWrapper wrapper = parse("select q.aliased from (select realcol as aliased from realtab) q");

		assertEquals("realtab.realcol", GenExpressionWrapper.getPhysicalColumnName(wrapper.root, "aliased"));
	}

	@Test
	void anUnknownProjectionResolvesToNothingRatherThanThrowing() {
		GenExpressionWrapper wrapper = parse("select a from t");

		assertEquals(null, GenExpressionWrapper.getPhysicalColumnName(wrapper.root, "no_such_column"));
	}

	@Test
	void lineageReportsTheLevelsItWalkedThrough() {
		GenExpressionWrapper wrapper = parse("select q.aliased from (select realcol as aliased from realtab) q");

		Object[] lineage = GenExpressionWrapper.getLineage(wrapper.root, "aliased", null, null, null, 0);

		assertEquals(4, lineage.length);
		assertNotNull(lineage[0]);
		assertNotNull(lineage[1]);
		assertTrue(((List<?>) lineage[2]).size() > 0, "expected at least one referencing expression");
		assertTrue((Integer) lineage[3] >= 0);
	}

	// ----- function bookkeeping -----

	@Test
	void callsToAFunctionAreTrackedByName() {
		GenExpressionWrapper wrapper = parse("select count(t.a), count(t.b), max(t.c) from tab t");

		assertEquals(2, wrapper.functionExpressionMapper.get("count").size());
		assertEquals(1, wrapper.functionExpressionMapper.get("max").size());
	}

	@Test
	void wrappingASelectorInAFunctionKeepsTheSelectorCount() throws Exception {
		GenExpressionWrapper wrapper = parse("select a, b from tab");
		int before = wrapper.root.nselectors.size();

		wrapper.addFunctionToSelector(wrapper.root, "a", "sum");

		assertEquals(before, wrapper.root.nselectors.size());
		String rendered = wrapper.generateQuery(false);
		assertTrue(rendered.toLowerCase().contains("sum"), () -> rendered);
	}

	// ----- filters and rendering -----

	@Test
	void theSelectsRecordedAgainstATableAreTheSelectsThatReadFromIt() {
		GenExpressionWrapper wrapper = parse("select a from tab where a = 'x'");

		assertTrue(wrapper.tableSelect.containsKey("tab"));
		assertFalse(wrapper.tableSelect.get("tab").contains(null), "tableSelect should not hold nulls");
		assertTrue(wrapper.tableSelect.get("tab").contains(wrapper.root),
				"expected the select that owns the FROM to be recorded");
	}

	@Test
	void anExtraRowFilterIsAndedOntoAnExistingWhereClause() throws Exception {
		GenExpressionWrapper wrapper = parse("select a from tab where a = 'x'");
		wrapper.fillParameters();

		Map<String, prerna.query.querystruct.GenExpression> extra = new HashMap<>();
		extra.put("tab", parse("select a from tab where b = 'y'").root.filter);
		wrapper.addRowFilter(extra);

		String rendered = wrapper.generateQuery(false);
		assertTrue(rendered.toUpperCase().contains("AND"), () -> rendered);
		assertTrue(rendered.contains("'x'"), () -> rendered);
	}

	@Test
	void printOutputAndGenerateQueryAgreeOnTheRenderedSql() throws Exception {
		GenExpressionWrapper wrapper = parse("select a from tab where a = 'x'");
		wrapper.fillParameters();

		assertEquals(wrapper.generateQuery(false), wrapper.printOutput());
	}

	@Test
	void aRenderedQueryWithAllParametersFilledParsesBack() throws Exception {
		GenExpressionWrapper wrapper = parse(TWO_TABLE_QUERY);
		wrapper.fillParameters();

		assertNotNull(CCJSqlParserUtil.parse(wrapper.generateQuery(true)));
	}

	@Test
	void removingAColumnDropsItFromTheSelectorsAndGroupings() throws Exception {
		GenExpressionWrapper wrapper = parse("select a, b from tab group by a, b");
		wrapper.fillParameters();

		wrapper.appendParameter(new ArrayList<>(List.of("b")), new HashMap<>());

		String rendered = wrapper.generateQuery(false);
		assertTrue(rendered.contains("a"), () -> rendered);
		assertFalse(rendered.matches("(?s).*\\bb\\b.*"), () -> rendered);
	}
}
