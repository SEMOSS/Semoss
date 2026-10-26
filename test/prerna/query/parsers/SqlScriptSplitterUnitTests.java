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

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SqlScriptSplitterUnitTests {

	@Test
	void eachStatementKeepsItsSubmittedTextThroughItsSemicolon() {
		List<SqlScriptSplitter.Statement> statements = SqlScriptSplitter
				.split("CREATE TABLE a (id INT);\n\n  INSERT INTO a VALUES (1) ;\nSELECT * FROM a");

		assertEquals(List.of("CREATE TABLE a (id INT);", "INSERT INTO a VALUES (1) ;", "SELECT * FROM a"),
				texts(statements));
	}

	@Test
	void semicolonsInsideQuotesAndCommentsDoNotEndAStatement() {
		String script = """
				INSERT INTO notes VALUES ('a; b', 'it''s; fine');
				-- a comment; with a semicolon
				UPDATE "odd;name" SET x = 1 /* also; here */ WHERE id = 2;
				""";

		List<String> texts = texts(SqlScriptSplitter.split(script));

		assertEquals(2, texts.size());
		assertEquals("INSERT INTO notes VALUES ('a; b', 'it''s; fine');", texts.get(0));
		assertEquals(List.of("SELECT `odd;col` FROM t;", "SELECT 2;"),
				texts(SqlScriptSplitter.split("SELECT `odd;col` FROM t; SELECT 2;")));
		assertTrue(texts.get(1).startsWith("-- a comment; with a semicolon"));
		assertTrue(texts.get(1).endsWith("WHERE id = 2;"));
	}

	@Test
	void aLineCommentEndsAtACarriageReturnToo() {
		assertEquals(List.of("-- note\rUPDATE a SET x = 1;", "SELECT 2;"),
				texts(SqlScriptSplitter.split("-- note\rUPDATE a SET x = 1; SELECT 2;")));
	}

	@Test
	void dollarQuotedFunctionBodiesStayInOneStatement() {
		String function = """
				CREATE FUNCTION touch() RETURNS trigger AS $body$
				BEGIN
				  NEW.updated := now();
				  RETURN NEW;
				END;
				$body$ LANGUAGE plpgsql;""";
		String block = "DO $$ BEGIN PERFORM 1; END $$;";

		assertEquals(List.of(function, block), texts(SqlScriptSplitter.split(function + "\n" + block)));
	}

	@Test
	void positionalParametersAndDollarsInsideIdentifiersAreNotDollarQuotes() {
		assertEquals(List.of("SELECT $1, price$usd FROM t;", "SELECT 2;"),
				texts(SqlScriptSplitter.split("SELECT $1, price$usd FROM t; SELECT 2;")));
	}

	@Test
	void backslashEscapesOnlyApplyToEscapeStrings() {
		assertEquals(List.of("SELECT E'it\\'s; ok';", "SELECT 'a\\';", "SELECT 2;"),
				texts(SqlScriptSplitter.split("SELECT E'it\\'s; ok'; SELECT 'a\\'; SELECT 2;")));
	}

	@Test
	void nestedBlockCommentsAreSkippedWhole() {
		assertEquals(List.of("/* outer /* inner; */ still; comment */ SELECT 1;"),
				texts(SqlScriptSplitter.split("/* outer /* inner; */ still; comment */ SELECT 1;")));
	}

	@Test
	void semicolonsInsideParenthesesDoNotEndAStatement() {
		String rule = "CREATE RULE r AS ON INSERT TO a DO ALSO (INSERT INTO b VALUES (1); INSERT INTO c VALUES (2));";

		assertEquals(List.of(rule), texts(SqlScriptSplitter.split(rule)));
	}

	@Test
	void piecesWithOnlyCommentsOrWhitespaceAreNotStatements() {
		assertEquals(List.of("SELECT 1;"), texts(SqlScriptSplitter.split(";; SELECT 1; -- trailing note\n  ;")));
		assertTrue(SqlScriptSplitter.split("  -- nothing here\n").isEmpty());
	}

	@Test
	void leadingWordsSkipCommentsAndAreUpperCase() {
		SqlScriptSplitter.Statement statement = SqlScriptSplitter
				.split("/* note */ create unique index concurrently idx_a ON a (id);").get(0);

		assertEquals(List.of("CREATE", "UNIQUE", "INDEX", "CONCURRENTLY", "IDX_A"), statement.leadingWords());
	}

	@Test
	void theBodyLeavesOutTheTerminatingSemicolon() {
		List<SqlScriptSplitter.Statement> statements = SqlScriptSplitter
				.split("DELETE FROM a WHERE id = 1 ;\nSELECT 1");

		assertEquals("DELETE FROM a WHERE id = 1", statements.get(0).body());
		assertEquals("SELECT 1", statements.get(1).body());
	}

	@ParameterizedTest
	@ValueSource(strings = { "CREATE TABLE a (id INT);", "-- add a column\nalter table a ADD b INT;",
			"/* gone */ DROP TABLE a;", "TRUNCATE TABLE a;", "RENAME TABLE a TO b;", "COMMENT ON TABLE a IS 'x';",
			"GRANT SELECT ON a TO someone;", "REVOKE SELECT ON a FROM someone;", "CREATE TEMPORARY TABLE t (id INT);" })
	void statementsThatChangeStructureOrPermissionsAreDataDefinition(String sql) {
		assertTrue(SqlScriptSplitter.split(sql).get(0).isDataDefinition(), sql);
	}

	@ParameterizedTest
	@ValueSource(strings = { "INSERT INTO a VALUES ('CREATE');", "UPDATE a SET note = 'DROP TABLE a';",
			"-- CREATE TABLE a\nDELETE FROM a;", "SELECT create_date FROM a;", "LOCK TABLE a IN EXCLUSIVE MODE;",
			"WITH x AS (SELECT 1) SELECT * FROM x;" })
	void dataChangesAndReadsAreNotDataDefinition(String sql) {
		assertFalse(SqlScriptSplitter.split(sql).get(0).isDataDefinition(), sql);
	}

	@ParameterizedTest
	@ValueSource(strings = { "SELECT 1;", "WITH x AS (SELECT 1) SELECT * FROM x;", "INSERT INTO a VALUES (1);",
			"UPDATE a SET note = 'CREATE';", "-- DROP TABLE a\nDELETE FROM a;",
			"MERGE INTO a USING b ON (a.id = b.id) " + "WHEN MATCHED THEN UPDATE SET a.note = b.note;",
			"REPLACE INTO a VALUES (1);", "EXPLAIN SELECT 1;" })
	void queriesAndRowChangesAreRowStatements(String sql) {
		assertTrue(SqlScriptSplitter.split(sql).get(0).isRowStatement(), sql);
	}

	@ParameterizedTest
	@ValueSource(strings = { "CREATE TABLE a (id INT);", "LOCK TABLES a WRITE;", "UNLOCK TABLES;", "FLUSH TABLES;",
			"CALL refresh_totals();", "CHECKPOINT;", "SET PASSWORD = 'secret';", "OPTIMIZE TABLE a;" })
	void statementsThatDoNotOnlyReadOrChangeRowsAreNotRowStatements(String sql) {
		assertFalse(SqlScriptSplitter.split(sql).get(0).isRowStatement(), sql);
	}

	@ParameterizedTest
	@ValueSource(strings = { "SELECT 'open", "SELECT \"open", "SELECT `open", "SELECT $$open", "SELECT 1 /* open" })
	void unclosedQuotesAndCommentsAreRejected(String script) {
		assertThrows(IllegalArgumentException.class, () -> SqlScriptSplitter.split(script));
	}

	@Test
	void sqlStandardFunctionBodiesAreRejectedRatherThanSplitWrongly() {
		assertThrows(IllegalArgumentException.class, () -> SqlScriptSplitter
				.split("CREATE FUNCTION one() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1; END;"));
	}

	private static List<String> texts(List<SqlScriptSplitter.Statement> statements) {
		return statements.stream().map(statement -> statement.text()).toList();
	}
}
