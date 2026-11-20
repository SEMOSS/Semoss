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
package prerna.query.interpreters.sql;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.AuthProvider;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.query.querystruct.selectors.QueryTypedColumnSelector;
import prerna.rdf.engine.wrappers.RawPreparedRDBMSSelectWrapper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.JdbcTestDatabase;
import prerna.util.QueryExecutionUtility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class ParameterizedSqlInterpreterUnitTests {

	private IRDBMSEngine engine() {
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.isBasic()).thenReturn(true);
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));
		when(engine.getDatabaseZoneId()).thenReturn(ZoneOffset.UTC);
		return engine;
	}

	private static SelectQueryStruct query() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ITEMS__ID", "id"));
		qs.addSelector(new QueryColumnSelector("ITEMS__V", "value"));
		qs.addOrderBy("ITEMS__ID");
		return qs;
	}

	@Test
	void capturesValuesInSqlOrderAndRecompilesWithoutChangingTheInput() throws Exception {
		IRDBMSEngine engine = engine();
		var interpreter = new ParameterizedSqlInterpreter(engine);
		SelectQueryStruct qs = query();
		String value = "O'Brien ? \u03bb";
		List<String> supplied = new ArrayList<>(List.of(value, "second"));
		qs.addSelector(new QueryConstantSelector("selected ?", "constant"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", "==", supplied));
		BetweenQueryFilter between = new BetweenQueryFilter();
		between.setColumn(new QueryColumnSelector("ITEMS__ID"));
		between.setStart(2);
		between.setEnd(8);
		qs.addExplicitFilter(between);
		qs.setLimit(3);
		qs.setOffSet(1);
		var compiled = interpreter.compile(qs);
		assertEquals(List.of("selected ?", value, "second", 2, 8, 3L, 1L), compiled.parameters());
		assertTrue(compiled.sql().contains("CAST(? AS VARCHAR) AS \"constant\""));
		assertTrue(compiled.sql().contains(" IN (?, ?)"));
		assertTrue(compiled.sql().endsWith("LIMIT ? OFFSET ?"));
		assertFalse(compiled.sql().contains(value));
		assertEquals(0, compiled.query().limit());
		assertEquals(compiled, interpreter.compile(qs));
		assertEquals(List.of(value, "second"), supplied);
		supplied.set(0, "replacement");
		assertEquals(value, compiled.parameters().get(1));
		assertEquals(compiled.sql(), interpreter.compile(qs).sql());
		assertThrows(UnsupportedOperationException.class, () -> compiled.parameters().add("extra"));
		verify(engine, never()).getConnection();
	}

	@Test
	void constantDerivedAliasesRemainIndependentOfValuesIncludingNull() {
		var interpreter = new ParameterizedSqlInterpreter(engine());
		SelectQueryStruct qs = query();
		QueryConstantSelector constant = new QueryConstantSelector("first");
		qs.addSelector(constant);
		var first = interpreter.compile(qs);
		constant.setConstant("second");
		assertEquals(first.sql(), interpreter.compile(qs).sql());
		assertFalse(first.sql().contains("first"));
		constant.setConstant(null);
		assertEquals(first.sql(), interpreter.compile(qs).sql());
		assertNull(interpreter.compile(qs).parameters().get(0));
	}

	@Test
	void recursiveFragmentsRepeatBindingsWhenAnExpressionAppearsTwice() {
		SelectQueryStruct qs = query();
		var coalesce = QueryFunctionSelector.makeCol2ValCoalesceSelector("ITEMS__V", "fallback ?", "label");
		qs.addSelector(coalesce);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(coalesce, "==", "match", PixelDataType.CONST_STRING));
		var compiled = new ParameterizedSqlInterpreter(engine()).compile(qs);
		assertEquals(List.of("fallback ?", "fallback ?", "match"), compiled.parameters());
		assertEquals(3, compiled.sql().chars().filter(c -> c == '?').count());
	}

	@Test
	void metadataMappingsAndPrimaryKeysUsePhysicalIdentifiersAndTypedBindings() {
		IRDBMSEngine engine = engine();
		when(engine.isBasic()).thenReturn(false);
		when(engine.getPhysicalUriFromPixelSelector("People")).thenReturn("http://example/PEOPLE");
		when(engine.getPhysicalUriFromPixelSelector("People__age")).thenReturn("http://example/AGE/PEOPLE");
		when(engine.getLegacyPrimKey4Table("http://example/PEOPLE")).thenReturn("PERSON_ID");
		when(engine.getDataTypes("http://semoss.org/ontologies/Concept/AGE/PEOPLE")).thenReturn("TYPE:INT");
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("People"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("People__age", ">", "21"));
		var compiled = new ParameterizedSqlInterpreter(engine).compile(qs);
		assertTrue(compiled.sql().contains("t0.PERSON_ID AS \"People\" FROM PEOPLE t0"));
		assertTrue(compiled.sql().contains("t0.AGE > ?"));
		assertEquals(List.of(21L), compiled.parameters());
	}

	@Test
	void preservesCombinedFiltersAndOverrideRules() {
		var interpreter = new ParameterizedSqlInterpreter(engine());
		SelectQueryStruct qs = query();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__ID", "==", 1));
		qs.addImplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__ID", "==", 2));
		qs.getFrameImplicitFilters().addFilters(SimpleQueryFilter.makeColToValFilter("ITEMS__V", "!=", "frame"));
		qs.getPanelImplicitFilters().addFilters(SimpleQueryFilter.makeColToValFilter("ITEMS__V", "!=", "panel"));
		assertEquals(List.of(1, 2, "frame", "panel"), interpreter.compile(qs).parameters());
		qs.setOverrideImplicit(true);
		assertEquals(List.of(1), interpreter.compile(qs).parameters());
		qs.ignoreFilters = true;
		assertTrue(interpreter.compile(qs).parameters().isEmpty());
	}

	@ParameterizedTest
	@CsvSource({ "==,IS NULL, OR ,IN", "!=,IS NOT NULL, AND ,NOT IN" })
	void nullAndMembershipFiltersPreserveMixedValues(String comparator, String nullPredicate, String junction,
			String membership) {
		SelectQueryStruct qs = query();
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ITEMS__V", comparator, Arrays.asList(null, "", "null", null)));
		var compiled = new ParameterizedSqlInterpreter(engine()).compile(qs);
		assertEquals(List.of("", "null"), compiled.parameters());
		assertTrue(compiled.sql().contains(nullPredicate));
		assertTrue(compiled.sql().contains(junction.trim()));
		assertTrue(compiled.sql().contains(membership + " (?, ?)"));
	}

	@ParameterizedTest
	@CsvSource({ "==,1=0", "!=,1=1" })
	void emptyMembershipHasPortableMeaning(String comparator, String predicate) {
		SelectQueryStruct qs = query();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__ID", comparator, List.of()));
		var compiled = new ParameterizedSqlInterpreter(engine()).compile(qs);
		assertTrue(compiled.sql().contains(predicate));
		assertTrue(compiled.parameters().isEmpty());
	}

	@ParameterizedTest
	@CsvSource({ "?like,%mixed%", "?nlike,%mixed%", "?begins,mixed%", "?nbegins,mixed%", "?ends,%mixed",
			"?nends,%mixed" })
	void searchPatternsAreBoundAndCaseNormalized(String comparator, String pattern) {
		SelectQueryStruct qs = query();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", comparator, "MiXeD"));
		var compiled = new ParameterizedSqlInterpreter(engine()).compile(qs);
		assertEquals(List.of(pattern), compiled.parameters());
		assertFalse(compiled.sql().contains("MiXeD"));
		assertTrue(compiled.sql().contains(comparator.startsWith("?n") ? "NOT LIKE ?" : "LIKE ?"));
	}

	@Test
	void typedDatesNumbersAndBooleansAreJdbcValues() {
		SelectQueryStruct qs = query();
		for (var item : List.of(new Object[] { SemossDataType.DATE, "2026-10-01", java.sql.Date.valueOf("2026-10-01") },
				new Object[] { SemossDataType.TIMESTAMP, "2026-10-01T12:13:14",
						Timestamp.valueOf("2026-10-01 12:13:14") },
				new Object[] { SemossDataType.DOUBLE, "1.25", new BigDecimal("1.25") },
				new Object[] { SemossDataType.BOOLEAN, "true", true })) {
			qs.getExplicitFilters().clear();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
					new QueryTypedColumnSelector("ITEMS__V", (SemossDataType) item[0]), "==", item[1],
					PixelDataType.CONST_STRING));
			assertEquals(List.of(item[2]), new ParameterizedSqlInterpreter(engine()).compile(qs).parameters());
		}
	}

	@Test
	void reversesValueToColumnComparisonsAndPreservesBooleanGrouping() {
		SelectQueryStruct qs = query();
		OrQueryFilter or = new OrQueryFilter();
		or.addFilter(new SimpleQueryFilter(new NounMetadata(3, PixelDataType.CONST_INT), "<",
				new NounMetadata(new QueryColumnSelector("ITEMS__ID"), PixelDataType.COLUMN)));
		AndQueryFilter and = new AndQueryFilter();
		and.addFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", "==", "a"));
		and.addFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__ID", "<=", 2));
		or.addFilter(and);
		qs.addExplicitFilter(or);
		var compiled = new ParameterizedSqlInterpreter(engine()).compile(qs);
		assertEquals(List.of(3, "a", 2), compiled.parameters());
		assertTrue(compiled.sql().contains("(t0.ID > ? OR (t0.V IN (?) AND t0.ID <= ?))"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "raw", "opaque", "function", "subquery", "identifier", "comparator", "sort", "unjoined" })
	void unsupportedStructureFailsBeforeGettingAConnection(String feature) throws Exception {
		SelectQueryStruct qs = query();
		switch (feature) {
		case "raw" -> qs.setCustomFrom("SELECT 1");
		case "opaque" -> qs.addSelector(new QueryOpaqueSelector());
		case "function" ->
			qs.addSelector(QueryFunctionSelector.makeFunctionSelector("arbitrary", "ITEMS__ID", "value"));
		case "subquery" -> qs.addExplicitFilter(
				new SimpleQueryFilter(new NounMetadata(new QueryColumnSelector("ITEMS__ID"), PixelDataType.COLUMN),
						"==", new NounMetadata(query(), PixelDataType.QUERY_STRUCT)));
		case "identifier" -> qs.addSelector(new QueryColumnSelector("ITEMS__invalid name"));
		case "comparator" -> qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__ID", "unknown", 1));
		case "sort" -> qs.addOrderBy("ITEMS__V", "unknown");
		case "unjoined" -> qs.addSelector(new QueryColumnSelector("OTHER__ID"));
		}
		IRDBMSEngine engine = engine();
		assertThrows(IllegalArgumentException.class, () -> new ParameterizedSqlInterpreter(engine).execute(qs));
		verify(engine, never()).getConnection();
	}

	@Test
	void supportsDifferentDialectsAndRejectsRawQueryStructs() {
		IRDBMSEngine engine = engine();
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.SQLITE));
		assertNotNull(new ParameterizedSqlInterpreter(engine).compile(query()));
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));
		assertThrows(IllegalArgumentException.class,
				() -> new ParameterizedSqlInterpreter(engine).compile(new HardSelectQueryStruct()));
	}

	@ParameterizedTest
	@EnumSource(RdbmsTypeEnum.class)
	void compilesTemplatesAndPaginationForEverySqlDialect(RdbmsTypeEnum dialect) throws Exception {
		IRDBMSEngine engine = engine();
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(dialect));
		SelectQueryStruct qs = query();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", "==", "user input"));
		qs.setLimit(10);
		qs.setOffSet(2);
		var compiled = new ParameterizedSqlInterpreter(engine).compile(qs);
		assertFalse(compiled.sql().contains("user input"));
		assertEquals("user input", compiled.parameters().get(0));
		assertEquals(compiled.parameters().size(), compiled.sql().chars().filter(c -> c == '?').count());
		assertEquals(compiled.countQuery().parameters().size(),
				compiled.countQuery().sql().chars().filter(c -> c == '?').count());
		List<Long> pagination = switch (dialect) {
		case SQL_SERVER, ORACLE, DB2, DERBY, HIVE, TRINO, ATHENA -> List.of(2L, 10L);
		case TERADATA, SYNAPSE -> List.of(2L, 12L);
		default -> List.of(10L, 2L);
		};
		assertEquals(pagination, compiled.parameters().subList(1, 3));
		String ending = switch (dialect) {
		case SQL_SERVER, ORACLE, DB2, DERBY -> "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
		case HIVE -> "LIMIT ?, ?";
		case TRINO, ATHENA -> "OFFSET ? LIMIT ?";
		case TERADATA, SYNAPSE ->
			"parameterized_row_number > ? AND parameterized_row_number <= ? ORDER BY parameterized_row_number";
		default -> "LIMIT ? OFFSET ?";
		};
		assertTrue(compiled.sql().endsWith(ending), compiled.sql());
		String quote = switch (dialect) {
		case BIG_QUERY, HIVE, IMPALA, SPARK, DATABRICKS, MYSQL, MARIADB -> "`";
		default -> "\"";
		};
		assertTrue(compiled.sql().contains(" AS " + quote + "id" + quote));
		verify(engine, never()).getConnection();
	}

	@Test
	void sqlServerPaginationAddsRequiredOrderWithoutMutatingInputAndCountDropsUnusedOrder() {
		IRDBMSEngine engine = engine();
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.SQL_SERVER));
		SelectQueryStruct qs = query();
		qs.getOrderBy().clear();
		qs.setLimit(5);
		var compiled = new ParameterizedSqlInterpreter(engine).compile(qs);
		assertTrue(compiled.sql().endsWith("ORDER BY \"id\" OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"));
		assertEquals(List.of(0L, 5L), compiled.parameters());
		assertTrue(qs.getOrderBy().isEmpty());
		qs.setLimit(-1);
		qs.addOrderBy("ITEMS__ID");
		compiled = new ParameterizedSqlInterpreter(engine).compile(qs);
		assertTrue(compiled.sql().contains("ORDER BY"));
		assertFalse(compiled.countQuery().sql().contains("ORDER BY"));
	}

	@ParameterizedTest
	@EnumSource(RdbmsTypeEnum.class)
	void dialectsCompileLimitOnlyOffsetOnlyAndSearchCasts(RdbmsTypeEnum dialect) {
		IRDBMSEngine engine = engine();
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(dialect));
		var interpreter = new ParameterizedSqlInterpreter(engine);
		SelectQueryStruct qs = query();
		qs.getOrderBy().clear();
		qs.setOffSet(4);
		var offset = interpreter.compile(qs);
		assertTrue(offset.parameters().contains(4L));
		assertEquals(offset.parameters().size(), offset.sql().chars().filter(c -> c == '?').count());
		qs.setOffSet(-1);
		qs.setLimit(7);
		var limit = interpreter.compile(qs);
		assertTrue(limit.parameters().contains(7L));
		assertEquals(limit.parameters().size(), limit.sql().chars().filter(c -> c == '?').count());
		qs.setLimit(-1);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__ID", "?like", "12"));
		var search = interpreter.compile(qs);
		assertEquals(List.of("%12%"), search.parameters());
		assertTrue(search.sql().contains("CAST(t0.ID AS "));
		if (List.of(RdbmsTypeEnum.BIG_QUERY, RdbmsTypeEnum.HIVE, RdbmsTypeEnum.IMPALA, RdbmsTypeEnum.SPARK,
				RdbmsTypeEnum.DATABRICKS).contains(dialect)) {
			assertTrue(search.sql().contains(" AS STRING)"));
		}
	}

	@ParameterizedTest
	@CsvSource({ "Mean,AVG", "Average,AVG", "UniqueAvg,AVG", "UniqueCount,COUNT", "UniqueSum,SUM" })
	void semossFunctionNamesUseDialectSyntaxAndDistinct(String function, String expected) {
		SelectQueryStruct qs = query();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(function, "ITEMS__ID", "n"));
		var compiled = new ParameterizedSqlInterpreter(engine()).compile(qs);
		assertTrue(compiled.sql()
				.contains(expected + "(" + (function.startsWith("Unique") ? "DISTINCT " : "") + "t0.ID)"));
	}

	@Test
	void snapshotsMutableDatesAndNormalizesSupportedJavaScalarTypes() {
		var timestamp = Timestamp.valueOf("2026-10-01 12:13:14.123456789");
		var date = java.sql.Date.valueOf("2026-10-01");
		var local = timestamp.toLocalDateTime();
		var inputs = List.of(timestamp, date, local.toLocalDate(), local, ZonedDateTime.of(local, ZoneOffset.UTC),
				OffsetDateTime.of(local, ZoneOffset.UTC), Instant.parse("2026-10-01T12:13:14Z"),
				new java.util.Date(1000), new BigInteger("9223372036854775808"));
		SelectQueryStruct qs = query();
		for (int i = 0; i < inputs.size(); i++) {
			qs.addSelector(new QueryConstantSelector(inputs.get(i), "c" + i));
		}
		var compiled = new ParameterizedSqlInterpreter(engine()).compile(qs);
		assertEquals(timestamp, compiled.parameters().get(0));
		assertNotSame(timestamp, compiled.parameters().get(0));
		assertNotSame(date, compiled.parameters().get(1));
		assertEquals(new BigDecimal("9223372036854775808"), compiled.parameters().get(8));
		timestamp.setTime(0);
		date.setTime(0);
		assertEquals(Timestamp.valueOf(local), compiled.parameters().get(0));
		assertEquals(java.sql.Date.valueOf(local.toLocalDate()), compiled.parameters().get(1));
	}

	@Test
	void invalidValuesAndUnsafeOptionsAreRejectedBeforeExecution() throws Exception {
		IRDBMSEngine engine = engine();
		var interpreter = new ParameterizedSqlInterpreter(engine);
		SelectQueryStruct qs = query();
		qs.addSelector(new QueryConstantSelector(new Object(), "invalid"));
		assertThrows(IllegalArgumentException.class, () -> interpreter.execute(qs));
		SelectQueryStruct multiple = query();
		multiple.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__ID", ">", List.of(1, 2)));
		assertThrows(IllegalArgumentException.class, () -> interpreter.execute(multiple));
		SelectQueryStruct nullOrdered = query();
		nullOrdered.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ITEMS__ID", ">", null, PixelDataType.NULL_VALUE));
		assertThrows(IllegalArgumentException.class, () -> interpreter.execute(nullOrdered));
		SelectQueryStruct invalidBoolean = query();
		invalidBoolean.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				new QueryTypedColumnSelector("ITEMS__ACTIVE", SemossDataType.BOOLEAN), "==", "perhaps",
				PixelDataType.CONST_STRING));
		assertThrows(IllegalArgumentException.class, () -> interpreter.execute(invalidBoolean));
		verify(engine, never()).getConnection();
	}

	@Test
	void rejectsMalformedJoinsAndProjectionAliases() {
		var interpreter = new ParameterizedSqlInterpreter(engine());
		SelectQueryStruct duplicate = query();
		duplicate.addSelector(new QueryColumnSelector("ITEMS__TEAM", "id"));
		assertThrows(IllegalArgumentException.class, () -> interpreter.compile(duplicate));
		SelectQueryStruct inferred = query();
		inferred.addRelation("ITEMS", "TEAMS", "inner.join");
		assertThrows(IllegalArgumentException.class, () -> interpreter.compile(inferred));
		SelectQueryStruct self = query();
		self.addRelation("ITEMS__ID", "ITEMS__TEAM", "inner.join");
		assertThrows(IllegalArgumentException.class, () -> interpreter.compile(self));
	}

	@Test
	void semossDatesAndTypedNullTokensPreserveDateAndNullMeaning() {
		var interpreter = new ParameterizedSqlInterpreter(engine());
		SelectQueryStruct qs = query();
		qs.addSelector(new QueryConstantSelector(new SemossDate(LocalDate.of(2026, 10, 1), ZoneOffset.UTC), "day"));
		qs.addSelector(new QueryConstantSelector(new SemossDate(LocalDateTime.of(2026, 10, 1, 12, 0), ZoneOffset.UTC),
				"time"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(new QueryTypedColumnSelector("ITEMS__ID", SemossDataType.INT),
						"==", List.of("null", "nan", "", "2"), PixelDataType.CONST_STRING));
		var compiled = interpreter.compile(qs);
		assertEquals(List.of(java.sql.Date.valueOf("2026-10-01"), Timestamp.valueOf("2026-10-01 12:00:00"), 2L),
				compiled.parameters());
		assertTrue(compiled.sql().contains("t0.ID IS NULL OR t0.ID IN (?)"));
		SelectQueryStruct midnight = query();
		midnight.addSelector(new QueryConstantSelector(
				new SemossDate(LocalDateTime.of(2026, 10, 1, 0, 0), ZoneOffset.UTC), "midnight"));
		assertInstanceOf(Timestamp.class, interpreter.compile(midnight).parameters().get(0));

		qs.addImplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", "==", "implicit"));
		qs.setOverrideImplicit(true);
		assertEquals("implicit", interpreter.compile(qs).parameters().get(3));
	}

	@Test
	void emptyBooleanGroupsAndColumnComparisonsExecuteWithoutValueParameters() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			when(db.engine.isBasic()).thenReturn(true);
			when(db.engine.getDatabaseZoneId()).thenReturn(ZoneOffset.UTC);
			db.execute("CREATE TABLE ITEMS (ID INT, V VARCHAR(20))", "INSERT INTO ITEMS VALUES (1, 'one')");
			var interpreter = new ParameterizedSqlInterpreter(db.engine);
			SelectQueryStruct qs = query();
			qs.addExplicitFilter(new AndQueryFilter());
			qs.addExplicitFilter(
					new SimpleQueryFilter(new NounMetadata(new QueryColumnSelector("ITEMS__ID"), PixelDataType.COLUMN),
							"==", new NounMetadata(new QueryColumnSelector("ITEMS__ID"), PixelDataType.COLUMN)));
			try (var wrapper = interpreter.execute(db.connection, qs, 2)) {
				assertEquals(1, wrapper.getNumRows());
			}
			qs.addExplicitFilter(new OrQueryFilter());
			try (var wrapper = interpreter.execute(qs)) {
				assertFalse(wrapper.hasNext());
				assertEquals(0, wrapper.getNumRows());
			}
		}
	}

	@ParameterizedTest
	@EnumSource(value = RdbmsTypeEnum.class, names = { "SYNAPSE", "TERADATA" })
	void windowPaginationPreservesDistinctRowsHeadersAndCount(RdbmsTypeEnum dialect) throws Exception {
		try (var db = new JdbcTestDatabase()) {
			when(db.engine.isBasic()).thenReturn(true);
			when(db.engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(dialect));
			when(db.engine.getDatabaseZoneId()).thenReturn(ZoneOffset.UTC);
			db.execute("CREATE TABLE ITEMS (ID INT, V VARCHAR(20))",
					"INSERT INTO ITEMS VALUES (1, 'one'),(2, 'two'),(2, 'two'),(3, 'three')");
			SelectQueryStruct qs = query();
			qs.setDistinct(true);
			qs.setOffSet(1);
			qs.setLimit(1);
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__ID", ">", 0));
			var interpreter = new ParameterizedSqlInterpreter(db.engine);
			try (var wrapper = interpreter.execute(qs)) {
				assertArrayEquals(new String[] { "id", "value" }, wrapper.getHeaders());
				assertEquals(1, wrapper.getNumRows());
				assertArrayEquals(new Object[] { 2, "two" }, wrapper.next().getValues());
				assertFalse(wrapper.hasNext());
			}
			qs.addOrderBy("ITEMS__UNSELECTED");
			assertThrows(IllegalArgumentException.class, () -> interpreter.compile(qs));
			qs.getOrderBy().clear();
			qs.setOffSet(Long.MAX_VALUE);
			assertThrows(ArithmeticException.class, () -> interpreter.compile(qs));
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "left.outer.join", "right.outer.join" })
	void explicitOuterJoinsExecuteAndRetainUnmatchedRows(String kind) throws Exception {
		try (var db = new JdbcTestDatabase()) {
			when(db.engine.isBasic()).thenReturn(true);
			when(db.engine.getDatabaseZoneId()).thenReturn(ZoneOffset.UTC);
			db.execute("CREATE TABLE ITEMS(ID INT, V VARCHAR(20), TEAM INT)", "CREATE TABLE TEAMS(ID INT)",
					"INSERT INTO ITEMS VALUES (1,'one',10)", "INSERT INTO TEAMS VALUES (10),(20)");
			SelectQueryStruct qs = query();
			qs.addRelation("ITEMS__TEAM", "TEAMS__ID", kind);
			try (var wrapper = new ParameterizedSqlInterpreter(db.engine).execute(qs)) {
				assertEquals(kind.startsWith("left") ? 1 : 2, wrapper.getNumRows());
			}
		}
	}

	@Test
	void bigQueryQuotesCompleteProjectPathsAndRejectsNonIdentifierFragments() {
		IRDBMSEngine engine = engine();
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.BIG_QUERY));
		var interpreter = new ParameterizedSqlInterpreter(engine);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("my-project.analytics.ITEMS__ID", "id"));
		assertTrue(interpreter.compile(qs).sql().contains("FROM `my-project.analytics.ITEMS` t0"));
		SelectQueryStruct invalid = new SelectQueryStruct();
		invalid.addSelector(new QueryColumnSelector("my-project.analytics.invalid name__ID", "id"));
		assertThrows(IllegalArgumentException.class, () -> interpreter.compile(invalid));
	}

	@Test
	void h2ExecutesLookupDynamicListJoinGroupAndBoundPagination() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			when(db.engine.isBasic()).thenReturn(true);
			when(db.engine.getDatabaseZoneId()).thenReturn(ZoneOffset.UTC);
			roundTrip(db.engine);
		}
	}

	/** Shared assertions for the database execution fixtures. */
	public static void roundTrip(IRDBMSEngine engine) throws Exception {
		boolean sqlServer = engine.getQueryUtil().getDbType() == RdbmsTypeEnum.SQL_SERVER;
		try (var statement = engine.getConnection().createStatement()) {
			statement.execute("CREATE TABLE ITEMS (ID INT PRIMARY KEY, V "
					+ (sqlServer ? "NVARCHAR(100)" : "VARCHAR(100)") + ", TEAM INT, D DATE, TS "
					+ (sqlServer ? "DATETIME2" : "TIMESTAMP") + ", ACTIVE " + (sqlServer ? "BIT" : "BOOLEAN") + ")");
			statement.execute("CREATE TABLE TEAMS (ID INT PRIMARY KEY, LABEL VARCHAR(100))");
			statement.execute("INSERT INTO TEAMS VALUES (10, 'group')");
		}
		QueryExecutionUtility.executeBatch(engine, "INSERT INTO ITEMS VALUES (?, ?, ?, ?, ?, ?)", List.of(1, 2, 3),
				(ps, id) -> {
					ps.setInt(1, id);
					ps.setString(2, id == 1 ? "O'Brien ? \u03bb" : id == 2 ? null : "");
					ps.setInt(3, 10);
					ps.setDate(4, java.sql.Date.valueOf("2026-10-01"));
					ps.setTimestamp(5, Timestamp.valueOf("2026-10-01 12:13:14"));
					ps.setBoolean(6, true);
				});
		var interpreter = new ParameterizedSqlInterpreter(engine);
		SelectQueryStruct lookup = query();
		lookup.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", "==", "O'Brien ? \u03bb"));
		try (var wrapper = interpreter.execute(lookup)) {
			assertEquals(1, wrapper.getNumRows());
			assertArrayEquals(new String[] { "id", "value" }, wrapper.getHeaders());
			assertEquals("O'Brien ? \u03bb", wrapper.next().getValues()[1]);
			assertFalse(wrapper.hasNext());
			wrapper.reset();
			assertTrue(wrapper.hasNext());
		}
		SelectQueryStruct list = query();
		list.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ITEMS__ID", "==", List.of(1, 2, 3), PixelDataType.CONST_INT));
		list.setLimit(2);
		list.setOffSet(1);
		try (var wrapper = interpreter.execute(list)) {
			assertEquals(2, wrapper.getNumRows());
			assertArrayEquals(new Object[] { 2, null }, wrapper.next().getValues());
			assertArrayEquals(new Object[] { 3, "" }, wrapper.next().getValues());
			assertFalse(wrapper.hasNext());
		}
		SelectQueryStruct joined = query();
		joined.addRelation("ITEMS__TEAM", "TEAMS__ID", "inner.join");
		joined.addSelector(new QueryColumnSelector("TEAMS__LABEL", "team"));
		try (var wrapper = interpreter.execute(joined)) {
			assertEquals(3, wrapper.getNumRows());
			assertEquals("group", wrapper.next().getValues()[2]);
		}
		SelectQueryStruct grouped = new SelectQueryStruct();
		grouped.addSelector(new QueryColumnSelector("ITEMS__TEAM", "team"));
		var count = QueryFunctionSelector.makeFunctionSelector("Count", "ITEMS__ID", "n");
		grouped.addSelector(count);
		grouped.addGroupBy(new QueryColumnSelector("ITEMS__TEAM"));
		grouped.addHavingFilter(SimpleQueryFilter.makeColToValFilter(count, ">", 1, PixelDataType.CONST_INT));
		try (var wrapper = interpreter.execute(grouped)) {
			assertEquals(1, wrapper.getNumRows());
			assertEquals(3L, ((Number) wrapper.next().getValues()[1]).longValue());
		}
		for (String comparator : List.of("?like", "?begins", "?ends")) {
			SelectQueryStruct search = query();
			search.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", comparator,
					comparator.equals("?like") ? "BRIEN" : comparator.equals("?begins") ? "O'" : "\u03bb"));
			try (var wrapper = interpreter.execute(search)) {
				assertEquals(1, wrapper.next().getValues()[0]);
				assertFalse(wrapper.hasNext());
				assertEquals(1, wrapper.getNumRows());
			}
		}
		QueryExecutionUtility.executeUpdate(engine, "UPDATE ITEMS SET V=? WHERE ID=3",
				ps -> ps.setString(1, "path\\file"));
		SelectQueryStruct slash = query();
		slash.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", "?like", "\\"));
		try (var wrapper = interpreter.execute(slash)) {
			assertEquals(3, wrapper.next().getValues()[0]);
			assertFalse(wrapper.hasNext());
		}
		QueryExecutionUtility.executeUpdate(engine, "UPDATE ITEMS SET V=? WHERE ID=3", ps -> ps.setString(1, ""));
		SelectQueryStruct nullable = query();
		nullable.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", "==", Arrays.asList(null, "")));
		try (var wrapper = interpreter.execute(nullable)) {
			assertEquals(2, wrapper.getNumRows());
			assertEquals(2, wrapper.next().getValues()[0]);
			assertEquals(3, wrapper.next().getValues()[0]);
			assertFalse(wrapper.hasNext());
		}
		SelectQueryStruct computed = query();
		QueryIfSelector conditional = new QueryIfSelector();
		conditional.setCondition(SimpleQueryFilter.makeColToValFilter("ITEMS__ID", "==", 1));
		conditional.setPrecedent(new QueryConstantSelector("first ?"));
		conditional.setAntecedent(new QueryConstantSelector("other"));
		conditional.setAlias("category");
		computed.addSelector(conditional);
		QueryArithmeticSelector arithmetic = new QueryArithmeticSelector();
		arithmetic.setLeftSelector(new QueryColumnSelector("ITEMS__ID"));
		arithmetic.setRightSelector(new QueryConstantSelector(2));
		arithmetic.setMathExpr("/");
		arithmetic.setAlias("ratio");
		computed.addSelector(arithmetic);
		try (var wrapper = interpreter.execute(computed)) {
			assertEquals(3, wrapper.getNumRows());
			Object[] row = wrapper.next().getValues();
			assertEquals("first ?", row[2]);
			assertEquals(0.5, ((Number) row[3]).doubleValue(), 0.000001);
			assertEquals("other", wrapper.next().getValues()[2]);
		}
		// Compare results with the existing interpreter for a structured lookup.
		SelectQueryStruct parity = query();
		parity.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__ID", "==", 1));
		var legacy = engine.getQueryUtil().getInterpreter(engine);
		legacy.setQueryStruct(parity);
		List<String> legacyRows = QueryExecutionUtility.queryList(engine, legacy.composeQuery(), ps -> {
		}, rs -> rs.getString(2));
		assertEquals(1, legacyRows.size());
		try (var wrapper = interpreter.execute(parity)) {
			assertEquals(legacyRows.get(0), wrapper.next().getValues()[1]);
			assertFalse(wrapper.hasNext());
		}
		SelectQueryStruct dates = query();
		dates.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__D", "==", LocalDate.of(2026, 10, 1),
				PixelDataType.CONST_DATE));
		dates.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__TS", "==",
				LocalDateTime.of(2026, 10, 1, 12, 13, 14), PixelDataType.CONST_TIMESTAMP));
		dates.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ITEMS__ACTIVE", "==", true, PixelDataType.BOOLEAN));
		try (var wrapper = interpreter.execute(dates)) {
			assertEquals(3, wrapper.getNumRows());
		}
	}

	@Test
	void derivedAndMembershipSubqueriesKeepSqlOrderAndIndependentAliases() throws Exception {
		SelectQueryStruct derived = new SelectQueryStruct();
		derived.addSelector(new QueryColumnSelector("TEAMS__ID", "team_id"));
		var inner = new QueryConstantSelector("inner value");
		inner.setAlias("note");
		derived.addSelector(inner);
		derived.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("TEAMS__LABEL", "==", "group"));
		derived.addOrderBy("team_id");
		SelectQueryStruct membership = new SelectQueryStruct();
		membership.addSelector(new QueryColumnSelector("TEAMS__ID"));
		membership.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("TEAMS__LABEL", "!=", "excluded"));
		SelectQueryStruct outer = query();
		var constant = new QueryConstantSelector("outer value");
		constant.setAlias("constant");
		outer.addSelector(constant);
		outer.addSelector(new QueryColumnSelector("derived__note", "note"));
		outer.addRelation(new SubqueryRelationship(derived, "derived", "left.outer.join",
				new String[] { "derived__team_id", "ITEMS__TEAM", "=" }));
		outer.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ITEMS__TEAM", "==", membership));
		outer.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("derived__note", "==", "inner value"));
		outer.setLimit(2);
		outer.setOffSet(1);
		var compiled = new ParameterizedSqlInterpreter(engine()).compile(outer);
		assertEquals(List.of("outer value", "inner value", "group", "excluded", "inner value", 2L, 1L),
				compiled.parameters());
		assertEquals(compiled.parameters(), compiled.countQuery().parameters());
		assertEquals(1, compiled.sql().split("ORDER BY", -1).length - 1);
		assertEquals(1, derived.getOrderBy().size());
		try (var db = new JdbcTestDatabase()) {
			when(db.engine.isBasic()).thenReturn(true);
			db.execute("CREATE TABLE ITEMS (ID INT, V VARCHAR, TEAM INT)", "CREATE TABLE TEAMS (ID INT, LABEL VARCHAR)",
					"INSERT INTO ITEMS VALUES (1, 'one', 10), (2, 'two', 10), (3, 'three', 10)",
					"INSERT INTO TEAMS VALUES (10, 'group')");
			try (var wrapper = new ParameterizedSqlInterpreter(db.engine).execute(outer)) {
				assertEquals(2, wrapper.getNumRows());
				assertArrayEquals(new Object[] { 2, "two", "outer value", "inner value" }, wrapper.next().getValues());
			}
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "==", "!=", ">" })
	void membershipAndScalarSubqueriesUseTheRequestedComparison(String comparator) {
		var nested = new SelectQueryStruct();
		nested.addSelector(new QueryColumnSelector("TEAMS__ID"));
		nested.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("TEAMS__LABEL", "==", "group"));
		var qs = query();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ITEMS__TEAM", comparator, nested));
		var result = new ParameterizedSqlInterpreter(engine()).compile(qs);
		assertTrue(result.sql().contains(
				comparator.equals("==") ? " IN (SELECT" : comparator.equals("!=") ? " NOT IN (SELECT" : " > (SELECT"));
		assertEquals(List.of("group"), result.parameters());
	}

	@ParameterizedTest
	@ValueSource(strings = { "INT", "INTEGER", "DECIMAL(12, 2)", "NVARCHAR(200)", "TIMESTAMP" })
	void castsUseValidatedStructuralTypesAndBoundValues(String type) {
		var cast = QueryFunctionSelector.makeFunctionSelector("Cast", new QueryConstantSelector("7"), "converted");
		cast.setDataType(type);
		var qs = query();
		qs.addSelector(cast);
		var result = new ParameterizedSqlInterpreter(engine()).compile(qs);
		assertTrue(result.sql().contains("CAST(? AS " + type + ")"));
		assertEquals(List.of("7"), result.parameters());
	}

	@ParameterizedTest
	@ValueSource(strings = { "INT extra", "CUSTOM_TYPE", "VARCHAR(size)", "DECIMAL(-1)", "INT(1.5)" })
	void unrecognizedCastTypesAreRejected(String type) {
		var cast = QueryFunctionSelector.makeFunctionSelector("Cast", "ITEMS__V", "converted");
		cast.setDataType(type);
		var qs = query();
		qs.addSelector(cast);
		assertThrows(IllegalArgumentException.class, () -> new ParameterizedSqlInterpreter(engine()).compile(qs));
	}

	@Test
	void enumValuesUseTheirStoredTextAndConstantPredicatesStayBound() {
		var qs = query();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", "==", AuthProvider.MICROSOFT));
		qs.addExplicitFilter(new SimpleQueryFilter(new NounMetadata(1, PixelDataType.CONST_INT), "==",
				new NounMetadata(0, PixelDataType.CONST_INT)));
		var result = new ParameterizedSqlInterpreter(engine()).compile(qs);
		assertEquals(List.of(AuthProvider.MICROSOFT.toString(), 1, 0), result.parameters());
		assertTrue(result.sql().contains("? = ?"));
	}

	@Test
	void typedDerivedColumnsUseTheirProjectionTypeWithoutLookingUpFictitiousTables() {
		var nested = new SelectQueryStruct();
		var cast = QueryFunctionSelector.makeFunctionSelector("Cast", "ITEMS__V", "number");
		cast.setDataType("INT");
		nested.addSelector(cast);
		var qs = query();
		qs.addRelation(new SubqueryRelationship(nested, "derived", "inner.join",
				new String[] { "ITEMS__ID", "derived__number", "=" }));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("derived__number", "==", "17"));
		assertEquals(List.of(17L), new ParameterizedSqlInterpreter(engine()).compile(qs).parameters());
	}

	@ParameterizedTest
	@ValueSource(strings = { "unknown_column", "duplicate_alias", "missing_condition", "malformed_condition",
			"disconnected", "cyclic", "invalid_endpoint", "invalid_alias", "empty_root_condition" })
	void invalidDerivedRelationshipsFailDuringCompilation(String shape) {
		var nested = new SelectQueryStruct();
		nested.addSelector(new QueryColumnSelector("TEAMS__ID", "team_id"));
		var qs = query();
		var relation = new SubqueryRelationship(nested, "derived", "left.outer.join",
				new String[] { "ITEMS__TEAM", "derived__team_id", "=" });
		qs.addRelation(relation);
		switch (shape) {
		case "unknown_column" -> qs.addSelector(new QueryColumnSelector("derived__missing", "missing"));
		case "duplicate_alias" -> qs.addRelation(new SubqueryRelationship(nested, "derived", "inner.join",
				new String[] { "ITEMS__TEAM", "derived__team_id", "=" }));
		case "missing_condition" -> relation.setJoinOnDetails(List.of());
		case "malformed_condition" -> relation.addJoinOn(new String[] { "ITEMS__ID" });
		case "disconnected" -> relation.addJoinOn("OTHER__ID", "derived__team_id", "=");
		case "cyclic" -> relation.setQs(qs);
		case "invalid_endpoint" -> relation.addJoinOn("ITEMS", "derived__team_id", "=");
		case "invalid_alias" -> relation.setQueryAlias("invalid alias");
		case "empty_root_condition" -> relation.setJoinOnDetails(Collections.singletonList(new String[] {}));
		}
		assertThrows(IllegalArgumentException.class, () -> new ParameterizedSqlInterpreter(engine()).compile(qs));
	}

	@Test
	void cyclicMembershipSubqueriesFailWithoutRecursingIndefinitely() {
		var qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ITEMS__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ITEMS__ID", "==", qs));
		assertThrows(IllegalArgumentException.class, () -> new ParameterizedSqlInterpreter(engine()).compile(qs));
	}

	@ParameterizedTest
	@ValueSource(strings = { "~", "!~", "~*", "!~*" })
	void postgresRegexOperatorsBindTheirPatternsAndOtherDialectsRejectThem(String operator) {
		var qs = query();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__V", operator, "O'Brien"));
		var engine = engine();
		assertThrows(IllegalArgumentException.class, () -> new ParameterizedSqlInterpreter(engine).compile(qs));
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.POSTGRES));
		var result = new ParameterizedSqlInterpreter(engine).compile(qs);
		assertEquals(List.of("O'Brien"), result.parameters());
		assertTrue(result.sql().contains(" " + operator + " ?"));
	}

	@Test
	void h2CountsTypedConstantProjectionsWithoutTruncatingStringsOrDecimalScale() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			when(db.engine.isBasic()).thenReturn(true);
			when(db.engine.getDatabaseZoneId()).thenReturn(ZoneOffset.UTC);
			db.execute("CREATE TABLE ITEMS (ID INT, V VARCHAR)", "INSERT INTO ITEMS VALUES (1, 'one')");
			var qs = query();
			var constants = Arrays.asList(true, (byte) 1, (short) 2, 3, 4L, 1.5f, 2.5d, new BigDecimal("0.000001"),
					new BigDecimal("1E+10"), "long ".repeat(2000), null);
			for (int i = 0; i < constants.size(); i++) {
				qs.addSelector(new QueryConstantSelector(constants.get(i), "c" + i));
			}
			var compiled = new ParameterizedSqlInterpreter(db.engine).compile(qs);
			try (var wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(db.engine, compiled, 0)) {
				assertEquals(1, wrapper.getNumRows());
				Object[] row = wrapper.next().getValues();
				assertEquals(true, row[2]);
				assertEquals("long ".repeat(2000), row[11]);
				assertNull(row[12]);
			}
			try (var ps = db.connection.prepareStatement(compiled.sql())) {
				for (int i = 0; i < compiled.parameters().size(); i++) {
					ps.setObject(i + 1, compiled.parameters().get(i));
				}
				try (var rs = ps.executeQuery()) {
					assertTrue(rs.next());
					assertEquals(new BigDecimal("0.000001"), rs.getBigDecimal(10));
					assertEquals(new BigDecimal("10000000000"), rs.getBigDecimal(11));
				}
			}
		}
	}
}
