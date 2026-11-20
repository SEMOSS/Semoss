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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
}
