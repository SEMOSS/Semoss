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
package prerna.prompt;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.util.JdbcTestDatabase;
import prerna.util.QueryExecutionUtility;

class PromptUtilsUnitTests {

	@Test
	void metadataReplacementIsAtomicAndPermissionValidationPrecedesWrite() throws Exception {
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			User user = mock(User.class, RETURNS_DEEP_STUBS);
			when(user.getPrimaryLoginToken().getId()).thenReturn("owner");
			queries.when(() -> QueryExecutionUtility.flushRsToMap(eq(db.engine), any(SelectQueryStruct.class)))
					.thenReturn(List.of(new HashMap<>(Map.of("CREATED_BY", "owner", "GLOBAL", false))));
			db.execute(
					"CREATE TABLE PROMPTMETA (PROMPT_ID VARCHAR, METAKEY VARCHAR, METAVALUE VARCHAR CHECK (METAVALUE <> 'reject'), METAORDER INT)",
					"INSERT INTO PROMPTMETA VALUES ('p','tag','original',0)");
			PromptUtils.updatePromptMetadata("p", Map.of("tag", List.of("ok", "reject")), user);
			assertEquals("original", db.value("SELECT METAVALUE FROM PROMPTMETA"));
			verify(db.connection).rollback();
			PromptUtils.updatePromptMetadata("p", Map.of("tag", List.of("  spaced  ", "")), user);
			assertEquals(2, db.count("PROMPTMETA"));
			queries.when(() -> QueryExecutionUtility.flushRsToMap(eq(db.engine), any(SelectQueryStruct.class)))
					.thenReturn(List.of());
			clearInvocations(db.engine);
			assertThrows(IllegalArgumentException.class,
					() -> PromptUtils.updatePromptMetadata("missing", Map.of(), user));
			verify(db.engine, never()).getConnection();
		}
	}

	@Test
	void tagReplacementKeepsOriginalWhenInsertionFails() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute(
					"CREATE TABLE PROMPTMETA (PROMPT_ID VARCHAR, METAKEY VARCHAR, METAVALUE VARCHAR CHECK (METAVALUE <> 'reject'), METAORDER INT)",
					"INSERT INTO PROMPTMETA VALUES ('p','tag','original',0)");
			PromptUtils.updatePromptTags("p", Map.of(), List.of("ok", "reject"));
			assertEquals("original", db.value("SELECT METAVALUE FROM PROMPTMETA"));
			PromptUtils.updatePromptTags("p", Map.of(), List.of("  exact  "));
			assertEquals("  exact  ", db.value("SELECT METAVALUE FROM PROMPTMETA"));
			PromptUtils.updatePromptTags("p", Map.of(), null);
			assertEquals(0, db.count("PROMPTMETA"));
		}
	}

	@Test
	void promptVersionsPreserveContextAndDeleteAllOwnedRows() throws Exception {
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(QueryExecutionUtility.class, CALLS_REAL_METHODS);
				var wrappers = mockStatic(prerna.rdf.engine.wrappers.WrapperManager.class)) {
			db.execute(
					"CREATE TABLE PROMPT (ID VARCHAR, TITLE VARCHAR, CONTEXT CLOB, VERSION INT, INTENT VARCHAR, CREATED_BY VARCHAR, DATE_CREATED TIMESTAMP, IS_LATEST BOOLEAN, GLOBAL BOOLEAN)",
					"CREATE TABLE PROMPTMETA (PROMPT_ID VARCHAR, METAKEY VARCHAR, METAVALUE VARCHAR, METAORDER INT)");
			var manager = mock(prerna.rdf.engine.wrappers.WrapperManager.class);
			var rows = mock(prerna.engine.api.IRawSelectWrapper.class);
			wrappers.when(prerna.rdf.engine.wrappers.WrapperManager::getInstance).thenReturn(manager);
			when(manager.getRawWrapper(eq(db.engine), any(SelectQueryStruct.class))).thenReturn(rows);
			User user = mock(User.class, RETURNS_DEEP_STUBS);
			when(user.getPrimaryLoginToken().getId()).thenReturn("owner");
			queries.when(() -> QueryExecutionUtility.flushRsToMap(eq(db.engine), any(SelectQueryStruct.class)))
					.thenReturn(List.of(new HashMap<>(Map.of("CREATED_BY", "owner", "GLOBAL", false))));
			Map<String, Object> details = new HashMap<>(
					Map.of("title", "title", "context", "  context  ", "intent", "intent", "tags", List.of("tag")));
			String id = PromptUtils.addPrompt(details, user, "owner");
			assertEquals("  context  ", db.value("SELECT CONTEXT FROM PROMPT"));
			details.put("id", id);
			details.put("context", "new context");
			PromptUtils.editPrompt(details, user);
			assertEquals(2, db.count("PROMPT"));
			assertEquals(1L, db.value("SELECT COUNT(*) FROM PROMPT WHERE IS_LATEST=TRUE"));
			assertEquals(id, PromptUtils.deletePrompt(id, user));
			assertEquals(0, db.count("PROMPT"));
			assertEquals(0, db.count("PROMPTMETA"));
		}
	}

	@Test
	void promptVersionWriteIsAtomicAndKeepsOneLatestVersion() throws Exception {
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			db.execute(
					"CREATE TABLE PROMPT (ID VARCHAR, TITLE VARCHAR CHECK (TITLE <> 'reject'), CONTEXT CLOB, VERSION INT, INTENT VARCHAR, CREATED_BY VARCHAR, DATE_CREATED TIMESTAMP, IS_LATEST BOOLEAN, GLOBAL BOOLEAN)",
					"CREATE TABLE PROMPTMETA (PROMPT_ID VARCHAR, METAKEY VARCHAR, METAVALUE VARCHAR, METAORDER INT)");
			User user = owner(db, queries);
			Map<String, Object> details = new HashMap<>(
					Map.of("title", "title", "context", "context", "tags", List.of()));
			String id = PromptUtils.addPrompt(details, user, "owner");
			assertEquals(0, db.value("SELECT VERSION FROM PROMPT"));
			details.put("id", id);
			details.put("title", "reject");
			db.manual();
			assertThrows(IllegalArgumentException.class, () -> PromptUtils.editPrompt(details, user));
			assertEquals(1, db.count("PROMPT"));
			assertEquals(true, db.value("SELECT IS_LATEST FROM PROMPT"));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
			details.put("title", "renamed");
			PromptUtils.editPrompt(details, user);
			assertEquals(2, db.count("PROMPT"));
			assertEquals(1L, db.value("SELECT COUNT(*) FROM PROMPT WHERE IS_LATEST"));
			assertEquals(1, db.value("SELECT VERSION FROM PROMPT WHERE IS_LATEST"));
			assertEquals("renamed", db.value("SELECT TITLE FROM PROMPT WHERE IS_LATEST"));
		}
	}

	@Test
	void promptDeleteRemovesThePromptAndItsMetadataTogether() throws Exception {
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			db.execute("CREATE TABLE PROMPT (ID VARCHAR, TITLE VARCHAR)",
					"CREATE TABLE PROMPTMETA (PROMPT_ID VARCHAR PRIMARY KEY, METAKEY VARCHAR)",
					"CREATE TABLE META_GUARD (PROMPT_ID VARCHAR REFERENCES PROMPTMETA(PROMPT_ID))",
					"INSERT INTO PROMPT VALUES ('p','title')", "INSERT INTO PROMPTMETA VALUES ('p','tag')",
					"INSERT INTO META_GUARD VALUES ('p')");
			User user = owner(db, queries);
			db.manual();
			assertEquals("p", PromptUtils.deletePrompt("p", user));
			assertEquals(1, db.count("PROMPT"));
			assertEquals(1, db.count("PROMPTMETA"));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
			db.execute("DELETE FROM META_GUARD");
			assertEquals("p", PromptUtils.deletePrompt("p", user));
			assertEquals(0, db.count("PROMPT"));
			assertEquals(0, db.count("PROMPTMETA"));
		}
	}

	@Test
	void initializerLooksUpTablesInReadsAndSeedsDefaultMetaKeysInOneWrite() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			runDdlOnTestConnection(db);
			db.manual();
			initialize(db);
			assertEquals(6, db.count("PROMPTMETAKEYS"));
			assertEquals("single", db.value("SELECT SINGLEMULTI FROM PROMPTMETAKEYS WHERE METAKEY='markdown'"));
			// a table and a column lookup for each of the three tables, then the seed
			verify(db.engine, times(7)).getConnection();
			verify(db.connection, times(6)).rollback();
			verify(db.connection, times(1)).commit();
			verify(db.engine, never()).commit();

			initialize(db);
			assertEquals(6, db.count("PROMPTMETAKEYS"));
			verify(db.connection, times(2)).commit();
		}
	}

	@Test
	void defaultMetaKeyFailureKeepsTheExistingRows() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			runDdlOnTestConnection(db);
			db.execute(
					"CREATE TABLE PROMPTMETAKEYS (METAKEY VARCHAR(255) CHECK (METAKEY <> 'domain'), SINGLEMULTI VARCHAR(255), DISPLAYORDER INT, DISPLAYOPTIONS VARCHAR(255), DEFAULTVALUES VARCHAR(500))",
					"INSERT INTO PROMPTMETAKEYS (METAKEY) VALUES ('kept'),('also kept')");
			assertDoesNotThrow(() -> initialize(db));
			assertEquals(2, db.count("PROMPTMETAKEYS"));
			assertEquals(1L, db.value("SELECT COUNT(*) FROM PROMPTMETAKEYS WHERE METAKEY='kept'"));
		}
	}

	private static User owner(JdbcTestDatabase db, org.mockito.MockedStatic<QueryExecutionUtility> queries) {
		User user = mock(User.class, RETURNS_DEEP_STUBS);
		when(user.getPrimaryLoginToken().getId()).thenReturn("owner");
		queries.when(() -> QueryExecutionUtility.flushRsToMap(eq(db.engine), any(SelectQueryStruct.class)))
				.thenReturn(List.of(new HashMap<>(Map.of("CREATED_BY", "owner", "GLOBAL", false))));
		return user;
	}

	private static void runDdlOnTestConnection(JdbcTestDatabase db) throws Exception {
		var util = spy(db.engine.getQueryUtil());
		doReturn(false).when(util).allowsIfExistsTableSyntax();
		doReturn(true).when(util).allowIfExistsIndexSyntax();
		when(db.engine.getQueryUtil()).thenReturn(util);
		doAnswer(invocation -> {
			db.execute((String) invocation.getArgument(0));
			return null;
		}).when(db.engine).insertData(anyString());
	}

	private static void initialize(JdbcTestDatabase db) throws Exception {
		Method initialize = PromptUtils.class.getDeclaredMethod("initialize", List.class);
		initialize.setAccessible(true);
		initialize.invoke(null, new PromptOwlCreator(db.engine.getQueryUtil()).getDBSchema());
	}
}
