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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.query.parsers.SqlQueryPolicyParser.Analysis;
import prerna.query.parsers.SqlQueryPolicyParser.Operation;

/**
 * Covers the policy inventory the SQL guardrail is built on: which operations a
 * statement performs, which relations and identifiers it touches, and the
 * shape-based risk signals such as SELECT * and cartesian joins.
 */
public class SqlQueryPolicyParserUnitTests {

	private static Analysis parse(String sql) {
		try {
			return SqlQueryPolicyParser.parse(sql, false);
		} catch (Exception e) {
			throw new AssertionError("Expected to parse: " + sql, e);
		}
	}

	// ----- the constructs that drove the parser upgrade -----

	@ParameterizedTest
	@ValueSource(strings = { "CREATE INDEX IF NOT EXISTS idx_a ON tbl (col)",
			"CREATE UNIQUE INDEX IF NOT EXISTS idx_b ON tbl (a, b)", "CREATE INDEX idx_c ON tbl (col)" })
	void createIndexIsParsedAndClassified(String sql) {
		Analysis analysis = parse(sql);

		assertEquals(1, analysis.statementCount);
		assertTrue(analysis.operations.contains(Operation.CREATE_INDEX),
				() -> "operations were " + analysis.operations);
		assertTrue(analysis.relations.contains("TBL"), () -> "relations were " + analysis.relations);
	}

	@Test
	void ifNotExistsIsSupportedOnCreateTableToo() {
		Analysis analysis = parse("CREATE TABLE IF NOT EXISTS tbl (a int)");

		assertTrue(analysis.operations.contains(Operation.CREATE_TABLE));
		assertTrue(analysis.relations.contains("TBL"));
	}

	// ----- operation classification -----

	@ParameterizedTest
	@CsvSource(delimiter = '|', value = { "select a from t                              | SELECT",
			"insert into t (a) values (1)                 | INSERT",
			"update t set a = 1 where id = 2              | UPDATE",
			"delete from t where id = 2                   | DELETE",
			"truncate table t                             | TRUNCATE",
			"create table t (a int)                       | CREATE_TABLE",
			"create view v as select * from t             | CREATE_VIEW",
			"drop table t                                 | DROP",
			"alter table t add column b int               | ALTER_TABLE",
			"replace into t (a) values (1)                | REPLACE",
			"values (1,2)                                 | VALUES",
			"explain select * from t                      | EXPLAIN",
			"select * into newt from t                    | SELECT_INTO",
			"select a from t for update                   | SELECT_FOR_UPDATE" })
	void operationIsClassified(String sql, String expected) {
		assertTrue(parse(sql).operations.contains(Operation.valueOf(expected)),
				() -> sql + " produced " + parse(sql).operations);
	}

	@ParameterizedTest
	@ValueSource(strings = { "show tables", "show index from t", "show databases", "show columns from t" })
	void metadataStatementsAreClassifiedAsShow(String sql) {
		// SHOW TABLES and SHOW INDEX parse to their own statement types rather than to
		// ShowStatement, so missing them here would report the operation as UNKNOWN
		assertTrue(parse(sql).operations.contains(Operation.SHOW), () -> sql + " produced " + parse(sql).operations);
	}

	@Test
	void insertOnDuplicateKeyIsAlsoAnUpsert() {
		Analysis analysis = parse("insert into t (a) values (1) on duplicate key update a = 2");

		assertTrue(analysis.operations.contains(Operation.INSERT));
		assertTrue(analysis.operations.contains(Operation.UPSERT));
	}

	@Test
	void mergeReadsTheUsingSource() {
		Analysis analysis = parse("merge into t using u on (t.id = u.id) when matched then update set t.a = u.a");

		assertTrue(analysis.operations.contains(Operation.MERGE));
		assertTrue(analysis.relations.containsAll(java.util.Set.of("T", "U")),
				() -> "relations were " + analysis.relations);
	}

	@Test
	void everyStatementInABatchIsInspected() {
		Analysis analysis = parse("select a from t1; delete from t2; drop table t3");

		assertEquals(3, analysis.statementCount);
		assertTrue(
				analysis.operations.containsAll(java.util.Set.of(Operation.SELECT, Operation.DELETE, Operation.DROP)));
	}

	// ----- shape signals the guardrail policy keys off -----

	@ParameterizedTest
	@ValueSource(strings = { "select * from t1, t2", "select a from t1 cross join t2" })
	void joinsWithoutAPredicateAreCartesian(String sql) {
		assertTrue(parse(sql).cartesianJoin, () -> sql + " was not flagged as cartesian");
	}

	@ParameterizedTest
	@ValueSource(strings = { "select a from t1 join t2 on t1.id = t2.id",
			"select a from t1 join t2 on t1.id = t2.id and t1.k = t2.k", "select a from t1 join t2 using (id)" })
	void joinsWithAPredicateAreNotCartesian(String sql) {
		assertFalse(parse(sql).cartesianJoin, () -> sql + " was wrongly flagged as cartesian");
	}

	@Test
	void joinsAreCounted() {
		assertEquals(2, parse("select a from t1 join t2 on t1.id = t2.id join t3 on t2.id = t3.id").joinCount);
	}

	@ParameterizedTest
	@ValueSource(strings = { "select * from t", "select t.* from t", "select a, t.* from t" })
	void selectStarIsDetected(String sql) {
		assertTrue(parse(sql).selectStar, () -> sql + " did not register as a star select");
	}

	@Test
	void namedColumnsAreNotAStarSelect() {
		assertFalse(parse("select a, b from t").selectStar);
	}

	@Test
	void recursiveCteIsDetected() {
		assertTrue(parse("with recursive c as (select 1 as n from dual) select * from c").recursiveCte);
	}

	@Test
	void nonRecursiveCteIsNot() {
		assertFalse(parse("with c as (select n from dual) select * from c").recursiveCte);
	}

	@Test
	void unqualifiedDeleteAndUpdateAreFlagged() {
		assertTrue(parse("delete from t").deleteWithoutWhere);
		assertTrue(parse("update t set a = 1").updateWithoutWhere);

		assertFalse(parse("delete from t where id = 1").deleteWithoutWhere);
		assertFalse(parse("update t set a = 1 where id = 1").updateWithoutWhere);
	}

	// ----- identifier inventory -----

	@Test
	void relationsAreCollectedThroughSubqueriesAndJoins() {
		Analysis analysis = parse(
				"select a from t1 join (select b from t2) q on t1.id = q.b " + "where t1.c in (select c from t3)");

		assertTrue(analysis.relations.containsAll(java.util.Set.of("T1", "T2", "T3")),
				() -> "relations were " + analysis.relations);
	}

	@Test
	void functionsAndContextKeywordsAreCollected() {
		Analysis analysis = parse("select count(a), max(b), current_user from t");

		assertTrue(analysis.functions.containsAll(java.util.Set.of("COUNT", "MAX")),
				() -> "functions were " + analysis.functions);
		assertTrue(analysis.keywords.contains("CURRENT_USER"), () -> "keywords were " + analysis.keywords);
	}

	@Test
	void aDelimitedColumnNamedLikeAKeywordStaysAColumn() {
		Analysis analysis = parse("select \"USER\" from t");

		assertFalse(analysis.keywords.contains("USER"), () -> "keywords were " + analysis.keywords);
	}

	// ----- failure handling -----

	@Test
	void unparseableSqlRaisesRatherThanGuessing() {
		assertThrows(Exception.class, () -> SqlQueryPolicyParser.parse("this is not sql at all", false));
	}

	@Test
	void squareBracketQuotingIsHonouredWhenRequested() throws Exception {
		Analysis analysis = SqlQueryPolicyParser.parse("select [a] from [t]", true);

		assertTrue(analysis.relations.contains("T"), () -> "relations were " + analysis.relations);
	}
}
