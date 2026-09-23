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
package prerna.engine.impl.guardrail;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;

class SqlQueryGuardrailEngineUnitTests {

	@Test
	void permitsParsedReadsAtReadOnlyAccess() {
		GuardrailNounMetadata result = evaluate("SELECT id, name FROM customers WHERE active = true",
				SqlQueryGuardrailEngine.CallerAccess.READ_ONLY);

		assertTrue(result.isPass());
		assertEquals(List.of("SELECT"), details(result).get("operations"));
		assertEquals("PARSED", details(result).get("parserStatus"));
	}

	@Test
	void ordinaryWritesRequireEditAccess() {
		String insert = "INSERT INTO audit_log(id, message) VALUES (1, 'created')";

		assertFalse(evaluate(insert, SqlQueryGuardrailEngine.CallerAccess.READ_ONLY).isPass());
		assertTrue(evaluate(insert, SqlQueryGuardrailEngine.CallerAccess.EDIT).isPass());
	}

	@ParameterizedTest
	@ValueSource(strings = { "DELETE FROM orders WHERE id = 42", "TRUNCATE TABLE staging_orders" })
	void deleteAndTruncateFollowEnginePermissionsByDefault(String query) {
		assertFalse(evaluate(query, SqlQueryGuardrailEngine.CallerAccess.READ_ONLY).isPass());
		assertTrue(evaluate(query, SqlQueryGuardrailEngine.CallerAccess.EDIT).isPass());
	}

	@ParameterizedTest
	@ValueSource(strings = { "DELETE FROM orders", "UPDATE orders SET status = 'cancelled'" })
	void unboundedModificationIsDeniedEvenForOwner(String query) {
		GuardrailNounMetadata result = evaluate(query, SqlQueryGuardrailEngine.CallerAccess.OWNER);

		assertFalse(result.isPass());
		assertTrue(violationValues(result).stream().anyMatch(value -> value.endsWith("_WITHOUT_WHERE")));
	}

	@ParameterizedTest
	@ValueSource(strings = { "waitfor delay 0:0:20", "WAITFOR DELAY '00:00:20'", "COPY users TO '/tmp/users.csv'",
			"DO $$ BEGIN NULL; END $$" })
	void unsupportedVendorOrControlStatementsFailClosed(String query) {
		GuardrailNounMetadata result = evaluate(query, SqlQueryGuardrailEngine.CallerAccess.OWNER);

		assertFalse(result.isPass());
		assertEquals("FAILED", details(result).get("parserStatus"));
		assertTrue(violationValues(result).contains("GENERIC"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "CALL privileged_procedure()", "GRANT SELECT ON users TO public" })
	void routineAndPrivilegeStatementsAreDeniedEvenThoughTheyParse(String query) {
		// these parse into real statement types rather than failing, so they have to be
		// denied on their own merits rather than riding on a parser failure
		GuardrailNounMetadata result = evaluate(query, SqlQueryGuardrailEngine.CallerAccess.OWNER);

		assertFalse(result.isPass());
		assertEquals("PARSED", details(result).get("parserStatus"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "SELECT DB_NAME()", "SELECT @@VERSION", "SELECT version()", "SELECT current_database()",
			"SELECT current_user", "SELECT pg_sleep(20)", "SELECT sleep(20)", "SELECT load_file('/etc/passwd')",
			"SELECT pg_catalog.pg_sleep(20)" })
	void dialectFingerprintDelayAndFileFunctionsAreDeniedFromAst(String query) {
		GuardrailNounMetadata result = evaluate(query, SqlQueryGuardrailEngine.CallerAccess.OWNER);

		assertFalse(result.isPass());
		assertEquals("PARSED", details(result).get("parserStatus"));
		assertTrue(violations(result).stream().anyMatch(violation -> "FUNCTION".equals(violation.get("type"))
				|| "VARIABLE".equals(violation.get("type")) || "KEYWORD".equals(violation.get("type"))));
	}

	@Test
	void sqlTextInsideAStringOrCommentIsNotReparsed() {
		String query = "SELECT 'select @@VERSION; waitfor delay 0:0:20' AS example /* DB_NAME() */";

		assertTrue(evaluate(query, SqlQueryGuardrailEngine.CallerAccess.READ_ONLY).isPass());
	}

	@ParameterizedTest
	@ValueSource(strings = { "INSERT INTO audit_log(id) VALUES (pg_sleep(20))",
			"UPDATE audit_log SET id = pg_sleep(20) WHERE id = 1", "SELECT COALESCE((SELECT pg_sleep(20)), 0)" })
	void dangerousFunctionsAreFoundInNestedAstExpressions(String query) {
		GuardrailNounMetadata result = evaluate(query, SqlQueryGuardrailEngine.CallerAccess.OWNER);

		assertFalse(result.isPass());
		assertEquals("PARSED", details(result).get("parserStatus"));
		assertTrue(violations(result).stream().anyMatch(violation -> "FUNCTION".equals(violation.get("type"))));
	}

	@ParameterizedTest
	@ValueSource(strings = { ";", ";;", "/* no executable SQL */" })
	void inputsWithoutAnExecutableAstFailClosed(String query) {
		assertFalse(evaluate(query, SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
	}

	@Test
	void stackedStatementsAreDeniedEvenWhenEachOperationWouldBeAllowed() {
		GuardrailNounMetadata result = evaluate("SELECT 1; SELECT 2", SqlQueryGuardrailEngine.CallerAccess.OWNER);

		assertFalse(result.isPass());
		assertTrue(violations(result).stream().anyMatch(violation -> "MULTI_STATEMENT".equals(violation.get("type"))));
	}

	@Test
	void systemCatalogsRequireOwnerByDefault() {
		for (String query : List.of("SELECT name FROM sys.tables", "SELECT name FROM master.sys.tables",
				"SELECT table_name FROM app.information_schema.tables", "SELECT setting FROM pg_settings")) {
			assertFalse(evaluate(query, SqlQueryGuardrailEngine.CallerAccess.EDIT).isPass());
			assertTrue(evaluate(query, SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "EXEC xp_cmdshell('whoami')", "EXEC master.dbo.xp_cmdshell('whoami')",
			"EXEC sp_executesql('SELECT 1')" })
	void dangerousRoutinesAreDeniedIncludingQualifiedNames(String query) {
		GuardrailNounMetadata result = evaluate(query, SqlQueryGuardrailEngine.CallerAccess.OWNER);

		assertFalse(result.isPass());
		assertEquals("PARSED", details(result).get("parserStatus"));
		assertTrue(violations(result).stream().anyMatch(violation -> "ROUTINE".equals(violation.get("type"))));
	}

	@Test
	void selectIntoAndForUpdateRequireEditAccess() {
		assertFalse(evaluate("SELECT id INTO archived_ids FROM orders", SqlQueryGuardrailEngine.CallerAccess.READ_ONLY)
				.isPass());
		assertTrue(evaluate("SELECT id INTO archived_ids FROM orders", SqlQueryGuardrailEngine.CallerAccess.EDIT)
				.isPass());
		assertFalse(
				evaluate("SELECT id FROM orders FOR UPDATE", SqlQueryGuardrailEngine.CallerAccess.READ_ONLY).isPass());
		assertTrue(evaluate("SELECT id FROM orders FOR UPDATE", SqlQueryGuardrailEngine.CallerAccess.EDIT).isPass());
	}

	@Test
	void operationPolicyCanRequireOwnerForDelete() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.OPERATION_POLICY_KEY, "{\"DELETE\":\"OWNER\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertFalse(engine.evaluateQuery("DELETE FROM orders WHERE id = 42", SqlQueryGuardrailEngine.CallerAccess.EDIT)
				.isPass());
		assertTrue(engine.evaluateQuery("DELETE FROM orders WHERE id = 42", SqlQueryGuardrailEngine.CallerAccess.OWNER)
				.isPass());
	}

	@Test
	void configuredOperationCatchAllCannotGrantPlatformAccess() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.OPERATION_POLICY_KEY,
				"{\"SELECT\":\"FOLLOW_ENGINE_PERMISSIONS\",\"*\":\"DENY\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertFalse(engine.evaluateQuery("SELECT id FROM orders", SqlQueryGuardrailEngine.CallerAccess.NONE).isPass());
		assertTrue(
				engine.evaluateQuery("SELECT id FROM orders", SqlQueryGuardrailEngine.CallerAccess.READ_ONLY).isPass());
		assertFalse(engine.evaluateQuery("UPDATE orders SET status = 'closed' WHERE id = 1",
				SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
		assertFalse(engine.evaluateQuery("TRUNCATE TABLE orders", SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
	}

	@Test
	void followEnginePermissionsProvidesExactExceptionToGroupRestriction() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.OPERATION_POLICY_KEY,
				"{\"DDL\":\"DENY\",\"TRUNCATE\":\"FOLLOW_ENGINE_PERMISSIONS\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertFalse(
				engine.evaluateQuery("TRUNCATE TABLE orders", SqlQueryGuardrailEngine.CallerAccess.READ_ONLY).isPass());
		assertTrue(engine.evaluateQuery("TRUNCATE TABLE orders", SqlQueryGuardrailEngine.CallerAccess.EDIT).isPass());
		assertFalse(engine.evaluateQuery("CREATE TABLE audit_log(id INT)", SqlQueryGuardrailEngine.CallerAccess.OWNER)
				.isPass());
	}

	@Test
	void operationPolicyCanLeaveOnlyInsertAtItsPlatformMinimum() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.OPERATION_POLICY_KEY,
				"{\"WRITE\":\"DENY\",\"INSERT\":\"FOLLOW_ENGINE_PERMISSIONS\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertFalse(engine
				.evaluateQuery("INSERT INTO orders(id) VALUES (1)", SqlQueryGuardrailEngine.CallerAccess.READ_ONLY)
				.isPass());
		assertTrue(engine.evaluateQuery("INSERT INTO orders(id) VALUES (1)", SqlQueryGuardrailEngine.CallerAccess.EDIT)
				.isPass());
		assertFalse(engine.evaluateQuery("UPDATE orders SET status = 'closed' WHERE id = 1",
				SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
		assertFalse(engine.evaluateQuery("DELETE FROM orders WHERE id = 1", SqlQueryGuardrailEngine.CallerAccess.OWNER)
				.isPass());
	}

	@Test
	void insertWithDuplicateUpdateAlsoRequiresUpsertPolicy() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.OPERATION_POLICY_KEY,
				"{\"WRITE\":\"DENY\",\"INSERT\":\"FOLLOW_ENGINE_PERMISSIONS\",\"DELETE\":\"DENY\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		GuardrailNounMetadata result = engine.evaluateQuery(
				"INSERT INTO orders(id, status) VALUES (1, 'open') ON DUPLICATE KEY UPDATE status = 'open'",
				SqlQueryGuardrailEngine.CallerAccess.OWNER);

		assertFalse(result.isPass());
		assertEquals(List.of("INSERT", "UPSERT"), details(result).get("operations"));
	}

	@Test
	void unsupportedOperationPolicyKeysAreRejected() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.OPERATION_POLICY_KEY, "{\"DESTROY\":\"DENY\"}");

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> configured(properties));
		assertTrue(exception.getMessage().contains("unsupported key: DESTROY"));
	}

	@Test
	void obsoleteAllowRequirementIsRejected() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.OPERATION_POLICY_KEY, "{\"SELECT\":\"ALLOW\"}");

		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> configured(properties));
		assertTrue(exception.getMessage().contains("FOLLOW_ENGINE_PERMISSIONS"));
	}

	@Test
	void identifierPoliciesCanRequireOwnerInsteadOfDenying() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.FUNCTION_POLICY_KEY, "{\"CURRENT_DATABASE\":\"OWNER\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertFalse(
				engine.evaluateQuery("SELECT current_database()", SqlQueryGuardrailEngine.CallerAccess.EDIT).isPass());
		assertTrue(
				engine.evaluateQuery("SELECT current_database()", SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
	}

	@Test
	void parserFailurePolicyCanBeRestrictedToOwners() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.PARSER_FAILURE_POLICY_KEY, "OWNER");
		SqlQueryGuardrailEngine engine = configured(properties);

		// a statement that genuinely does not parse, so the parser failure policy is
		// what decides the outcome
		assertFalse(
				engine.evaluateQuery("WAITFOR DELAY '00:00:20'", SqlQueryGuardrailEngine.CallerAccess.EDIT).isPass());
		assertTrue(
				engine.evaluateQuery("WAITFOR DELAY '00:00:20'", SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
	}

	@Test
	void sqlServerDialectEnablesSquareBracketQuotation() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.SQL_DIALECT_KEY, "SQL_SERVER");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertTrue(engine.evaluateQuery("SELECT [Order ID] FROM [Sales Orders] WHERE [Order ID] = 1",
				SqlQueryGuardrailEngine.CallerAccess.READ_ONLY).isPass());
	}

	@Test
	void configurableStructurePoliciesUseTheParsedAst() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.SELECT_STAR_POLICY_KEY, "DENY");
		properties.setProperty(SqlQueryGuardrailEngine.CARTESIAN_JOIN_POLICY_KEY, "OWNER");
		properties.setProperty(SqlQueryGuardrailEngine.MAX_JOINS_KEY, "1");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertFalse(engine.evaluateQuery("SELECT * FROM orders", SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
		assertFalse(engine.evaluateQuery("SELECT a.id FROM a CROSS JOIN b", SqlQueryGuardrailEngine.CallerAccess.EDIT)
				.isPass());
		assertFalse(engine.evaluateQuery("SELECT a.id FROM a JOIN b ON a.id=b.id JOIN c ON b.id=c.id",
				SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
		assertFalse(engine
				.evaluateQuery("INSERT INTO archive SELECT * FROM orders", SqlQueryGuardrailEngine.CallerAccess.OWNER)
				.isPass());
	}

	@Test
	void policiesSupportExplicitAllowlistMode() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.PROTECT_UNMATCHED_IDENTIFIERS_KEY, "false");
		properties.setProperty(SqlQueryGuardrailEngine.OPERATION_POLICY_KEY,
				"{\"*\":\"DENY\",\"SELECT\":\"READ_ONLY\"}");
		properties.setProperty(SqlQueryGuardrailEngine.FUNCTION_POLICY_KEY,
				"{\"*\":\"DENY\",\"COUNT\":\"FOLLOW_ENGINE_PERMISSIONS\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertTrue(engine.evaluateQuery("SELECT COUNT(id) FROM orders", SqlQueryGuardrailEngine.CallerAccess.READ_ONLY)
				.isPass());
		assertFalse(
				engine.evaluateQuery("SELECT ABS(balance) FROM accounts", SqlQueryGuardrailEngine.CallerAccess.OWNER)
						.isPass());
		assertFalse(
				engine.evaluateQuery("INSERT INTO orders(id) VALUES (1)", SqlQueryGuardrailEngine.CallerAccess.OWNER)
						.isPass());
	}

	@Test
	void mostSpecificWildcardPolicyWinsDeterministically() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.PROTECT_UNMATCHED_IDENTIFIERS_KEY, "false");
		properties.setProperty(SqlQueryGuardrailEngine.FUNCTION_POLICY_KEY,
				"{\"*\":\"FOLLOW_ENGINE_PERMISSIONS\",\"PG_*\":\"DENY\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertFalse(engine.evaluateQuery("SELECT pg_sleep(1)", SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
		assertTrue(engine.evaluateQuery("SELECT abs(-1)", SqlQueryGuardrailEngine.CallerAccess.READ_ONLY).isPass());
	}

	@Test
	void configuredIdentifierCatchAllOverridesMoreSpecificDefaults() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.FUNCTION_POLICY_KEY, "{\"*\":\"FOLLOW_ENGINE_PERMISSIONS\"}");
		properties.setProperty(SqlQueryGuardrailEngine.RELATION_POLICY_KEY,
				"{\"TEST1\":\"FOLLOW_ENGINE_PERMISSIONS\",\"*\":\"DENY\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertTrue(engine.evaluateQuery("SELECT pg_sleep(1) FROM test1", SqlQueryGuardrailEngine.CallerAccess.READ_ONLY)
				.isPass());
		assertFalse(engine
				.evaluateQuery("SELECT name FROM pg_catalog.pg_tables", SqlQueryGuardrailEngine.CallerAccess.OWNER)
				.isPass());
	}

	@Test
	void unmatchedConfiguredIdentifierRulesFallBackToDefaults() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.FUNCTION_POLICY_KEY, "{\"ABS\":\"FOLLOW_ENGINE_PERMISSIONS\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertTrue(engine.evaluateQuery("SELECT abs(-1)", SqlQueryGuardrailEngine.CallerAccess.READ_ONLY).isPass());
		assertFalse(engine.evaluateQuery("SELECT pg_sleep(1)", SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
	}

	@Test
	void disablingIdentifierFallbackLeavesUnmatchedIdentifiersUnrestricted() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.PROTECT_UNMATCHED_IDENTIFIERS_KEY, "false");
		properties.setProperty(SqlQueryGuardrailEngine.FUNCTION_POLICY_KEY, "{\"ABS\":\"DENY\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertTrue(engine.evaluateQuery("SELECT pg_sleep(1)", SqlQueryGuardrailEngine.CallerAccess.READ_ONLY).isPass());
		assertFalse(engine.evaluateQuery("SELECT abs(-1)", SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
	}

	@Test
	void keywordCatchAllDoesNotTreatUnqualifiedOrQuotedColumnsAsKeywords() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.PROTECT_UNMATCHED_IDENTIFIERS_KEY, "false");
		properties.setProperty(SqlQueryGuardrailEngine.KEYWORD_POLICY_KEY, "{\"*\":\"DENY\"}");
		properties.setProperty(SqlQueryGuardrailEngine.RELATION_POLICY_KEY,
				"{\"TEST1\":\"FOLLOW_ENGINE_PERMISSIONS\",\"*\":\"DENY\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		assertTrue(engine.evaluateQuery("SELECT col1, customer_name FROM test1 WHERE active = true",
				SqlQueryGuardrailEngine.CallerAccess.OWNER).isPass());
		assertTrue(
				engine.evaluateQuery("SELECT \"CURRENT_USER\" FROM test1", SqlQueryGuardrailEngine.CallerAccess.OWNER)
						.isPass());
		assertTrue(
				engine.evaluateQuery("SELECT test1.current_user FROM test1", SqlQueryGuardrailEngine.CallerAccess.OWNER)
						.isPass());
	}

	@ParameterizedTest
	@ValueSource(strings = { "CURRENT_USER", "CURRENT_SCHEMA", "SESSION_USER", "SYSTEM_USER", "USER", "CURRENT_ROLE",
			"CURRENT_DATE", "LOCALTIME", "LOCALTIMESTAMP" })
	void keywordCatchAllStillAppliesToSqlContextExpressions(String expression) {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.PROTECT_UNMATCHED_IDENTIFIERS_KEY, "false");
		properties.setProperty(SqlQueryGuardrailEngine.KEYWORD_POLICY_KEY, "{\"*\":\"DENY\"}");
		SqlQueryGuardrailEngine engine = configured(properties);

		GuardrailNounMetadata result = engine.evaluateQuery("SELECT " + expression + " FROM test1",
				SqlQueryGuardrailEngine.CallerAccess.OWNER);

		assertFalse(result.isPass());
		assertTrue(violations(result).stream().anyMatch(
				violation -> "KEYWORD".equals(violation.get("type")) && expression.equals(violation.get("value"))));
	}

	@Test
	void queryLengthLimitIsEnforcedBeforeParsing() {
		Properties properties = new Properties();
		properties.setProperty(SqlQueryGuardrailEngine.MAX_QUERY_LENGTH_KEY, "10");
		SqlQueryGuardrailEngine engine = configured(properties);

		GuardrailNounMetadata result = engine.evaluateQuery("SELECT 1234567890",
				SqlQueryGuardrailEngine.CallerAccess.OWNER);
		assertFalse(result.isPass());
		assertTrue(violationValues(result).stream().anyMatch(value -> value.startsWith("17>10")));
	}

	@Test
	void exposesEmbeddedTypeAndReadyDatabasePipeline() {
		SqlQueryGuardrailEngine engine = new SqlQueryGuardrailEngine();
		engine.setEngineId("sql-policy-id");

		assertEquals(GuardrailTypeEnum.EMBEDDED_SQL_QUERY, engine.getGuardrailType());
		String markdown = engine.getDefaultMarkdown();
		assertTrue(markdown.contains("\"guardrailEngineId\": \"sql-policy-id\""));
		assertFalse(markdown.contains("targetEngineId"));
		assertTrue(markdown.contains("\"execQuery\""));
		assertTrue(markdown.contains("\"insertData\""));
		assertTrue(markdown.contains("\"removeData\""));
	}

	@Test
	void actualTargetDatabaseCannotBeSpoofedByTheOptionalDatabaseIdNoun() {
		SqlQueryGuardrailEngine engine = new SqlQueryGuardrailEngine();
		IEngine database = targetEngine(IEngine.CATALOG_TYPE.DATABASE, "actual-database-id");
		IEngine model = targetEngine(IEngine.CATALOG_TYPE.MODEL, "model-id");

		assertEquals("actual-database-id", engine.resolveDatabaseId(database, "spoofed-database-id"));
		assertEquals("fallback-database-id", engine.resolveDatabaseId(null, "fallback-database-id"));
		assertNull(engine.resolveDatabaseId(model, "spoofed-database-id"));
	}

	@Test
	void wildcardPolicyMatcherRequiresTheWholeIdentifier() {
		assertTrue(SqlQueryGuardrailEngine.wildcardMatches("PG_CATALOG.*", "PG_CATALOG.PG_TABLES"));
		assertTrue(SqlQueryGuardrailEngine.wildcardMatches("DBMS_*.*", "DBMS_LOCK.SLEEP"));
		assertFalse(SqlQueryGuardrailEngine.wildcardMatches("SYS.*", "APP.SYS.TABLES"));
	}

	private static SqlQueryGuardrailEngine configured(Properties properties) {
		SqlQueryGuardrailEngine engine = new SqlQueryGuardrailEngine();
		engine.configurePolicy(properties);
		return engine;
	}

	private static IEngine targetEngine(IEngine.CATALOG_TYPE catalogType, String engineId) {
		return (IEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(), new Class<?>[] { IEngine.class },
				(proxy, method, arguments) -> {
					if ("getCatalogType".equals(method.getName())) {
						return catalogType;
					}
					if ("getEngineId".equals(method.getName())) {
						return engineId;
					}
					return null;
				});
	}

	private static GuardrailNounMetadata evaluate(String query, SqlQueryGuardrailEngine.CallerAccess access) {
		return new SqlQueryGuardrailEngine().evaluateQuery(query, access);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> details(GuardrailNounMetadata result) {
		return (Map<String, Object>) result.getFullDetails();
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> violations(GuardrailNounMetadata result) {
		return (List<Map<String, Object>>) details(result).get("violations");
	}

	private static List<String> violationValues(GuardrailNounMetadata result) {
		return violations(result).stream().map(violation -> violation.get("value").toString()).toList();
	}
}
