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
package prerna.engine.impl.model.inferencetracking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;

class ModelInferenceLogsJdbcUnitTests {

	private void schema(JdbcTestDatabase db) throws Exception {
		db.execute(
				"CREATE TABLE WORKSPACE (WORKSPACE_ID VARCHAR, NAME VARCHAR, DESCRIPTION CLOB, SYSTEM_PROMPT CLOB, OWNER VARCHAR, IS_ACTIVE BOOLEAN, DATE_CREATED TIMESTAMP, DATE_UPDATED TIMESTAMP, CONFIG_JSON CLOB)",
				"CREATE TABLE WORKSPACE_RESOURCE (WORKSPACE_RESOURCE_ID VARCHAR, WORKSPACE_ID VARCHAR, RESOURCE_ID VARCHAR CHECK (RESOURCE_ID <> 'reject'), RESOURCE_TYPE VARCHAR, RESOURCE_SUBTYPE VARCHAR)",
				"CREATE TABLE ROOM (ROOM_ID VARCHAR, WORKSPACE_ID VARCHAR)");
	}

	private Map<String, String> resource(String resource) {
		return Map.of("workspace_resource_id", "wr", "workspace_id", "w", "resource_id", resource, "resource_type",
				"ENGINE", "resource_subtype", "MODEL");
	}

	@Test
	void workspaceAndResourcesCommitTogetherAndReplacementFailureRestoresBoth() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			db.manual();
			ModelInferenceLogsUtils.createNewWorkspaceEntry("w", "owner", "original", "  description  ", null,
					List.of(resource("ok")));
			db.connection.rollback();
			assertEquals(1, db.count("WORKSPACE"));
			assertEquals(1, db.count("WORKSPACE_RESOURCE"));
			assertEquals("  description  ", db.value("SELECT DESCRIPTION FROM WORKSPACE"));
			assertThrows(IllegalArgumentException.class, () -> ModelInferenceLogsUtils.updateWorkspaceEntry("w",
					"changed", null, "prompt", true, List.of(resource("reject"))));
			assertEquals("original", db.value("SELECT NAME FROM WORKSPACE"));
			assertEquals("ok", db.value("SELECT RESOURCE_ID FROM WORKSPACE_RESOURCE"));
			assertNotNull(ModelInferenceLogsUtils.findWorkspaceResource("w", "ok", "ENGINE"));
			assertNull(ModelInferenceLogsUtils.findWorkspaceResource("w", "missing", "ENGINE"));
			assertEquals(1, ModelInferenceLogsUtils.getWorkspaceResources("w", "ENGINE", null).size());
			ModelInferenceLogsUtils.deleteWorkspaceEntry("w");
			assertEquals(0, db.count("WORKSPACE"));
			assertEquals(0, db.count("WORKSPACE_RESOURCE"));
		}
	}

	@Test
	void importRejectsExistingWithoutReplacementAndPreservesOriginal() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			ModelInferenceLogsUtils.importWorkspaceEntry("w", "owner", "original", null, " system ", true, "{}",
					List.of(), false);
			assertThrows(java.sql.SQLException.class, () -> ModelInferenceLogsUtils.importWorkspaceEntry("w", "other",
					"changed", "d", "p", false, null, List.of(), false));
			assertEquals("original", db.value("SELECT NAME FROM WORKSPACE"));
			ModelInferenceLogsUtils.importWorkspaceEntry("w", "other", "changed", "", null, false, "  {}  ", List.of(),
					true);
			assertEquals("owner", db.value("SELECT OWNER FROM WORKSPACE"));
			assertEquals("  {}  ", db.value("SELECT CONFIG_JSON FROM WORKSPACE"));
			ModelInferenceLogsUtils.updateWorkspaceConfigJson("w", new org.json.JSONObject("{\"key\":1}"));
			assertEquals("{\"key\":1}", db.value("SELECT CONFIG_JSON FROM WORKSPACE"));
			ModelInferenceLogsUtils.updateWorkspaceCoreFields("w", "final", " description ", " prompt ");
			assertEquals("final", db.value("SELECT NAME FROM WORKSPACE"));
			assertEquals(" description ", db.value("SELECT DESCRIPTION FROM WORKSPACE"));
		}
	}

	@Test
	void batchReadsMaterializeBinaryTextAndRespectOwnership() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute(
					"CREATE TABLE MESSAGE (TRANSACTION_ID VARCHAR, USER_ID VARCHAR, AGENT_ID VARCHAR, DATE_CREATED TIMESTAMP, MESSAGE_DATA BLOB, MESSAGE_METHOD VARCHAR, ROOM_ID VARCHAR, MESSAGE_TYPE VARCHAR, MESSAGE_TOKENS INT, INPUT_TOKENS INT)");
			try (var ps = db.connection.prepareStatement(
					"INSERT INTO MESSAGE (TRANSACTION_ID,USER_ID,AGENT_ID,MESSAGE_DATA,MESSAGE_METHOD,ROOM_ID,MESSAGE_TYPE) VALUES (?,?,?,?,?,?,?)")) {
				ps.setString(1, "batch");
				ps.setString(2, "u");
				ps.setString(3, "model");
				ps.setBytes(4, "2".getBytes(java.nio.charset.StandardCharsets.UTF_8));
				ps.setString(5, "batch_submit");
				ps.setString(6, "mb_batch");
				ps.setString(7, "INPUT");
				ps.executeUpdate();
				ps.setString(1, "batch.custom");
				ps.setBytes(4, "  prompt  ".getBytes(java.nio.charset.StandardCharsets.UTF_8));
				ps.setString(5, "batch");
				ps.executeUpdate();
			}
			db.manual();
			assertTrue(ModelInferenceLogsUtils.userOwnsBatch("u", "batch"));
			assertFalse(ModelInferenceLogsUtils.userOwnsBatch("other", "batch"));
			assertEquals(2, ModelInferenceLogsUtils.getUserBatches("u", "model", 10).get(0).get("requestCount"));
			assertEquals(Map.of("custom", "  prompt  "), ModelInferenceLogsUtils.getBatchInputs("u", "batch"));
			ModelInferenceLogsUtils.updateBatchInputTokens("batch.custom", 7);
			db.connection.rollback();
			assertEquals(7, db.value("SELECT MESSAGE_TOKENS FROM MESSAGE WHERE TRANSACTION_ID='batch.custom'"));
			verify(db.connection, atLeast(4)).rollback();
		}
	}

	@Test
	void conversationUpdatesKeepOwnerFiltersAndJsonCategories() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute(
					"CREATE TABLE ROOM (INSIGHT_ID VARCHAR, ROOM_ID VARCHAR, ROOM_NAME VARCHAR, ROOM_CONTEXT CLOB, USER_ID VARCHAR, USER_NAME VARCHAR, USER_EMAIL_ID VARCHAR, AGENT_TYPE VARCHAR, AGENT_ID VARCHAR, IS_ACTIVE BOOLEAN, DATE_CREATED TIMESTAMP, PROJECT_ID VARCHAR, PROJECT_NAME VARCHAR, WORKSPACE_ID VARCHAR, OPTIONS CLOB, PARENT_ROOM_ID VARCHAR, MODEL_ID VARCHAR, PINNED BOOLEAN, SHARE_ID VARCHAR, UPDATED_AT TIMESTAMP, MESSAGES CLOB)");
			ModelInferenceLogsUtils.doCreateNewConversation("i", "r", "name", "  context  ", "u", null, null, null,
					null, true, "p", "project", null, Map.of("key", "value"), null);
			assertTrue(ModelInferenceLogsUtils.doCheckRoomExists("r"));
			assertTrue(ModelInferenceLogsUtils.doCheckRoomExistsForUser("r", "u"));
			assertFalse(ModelInferenceLogsUtils.doCheckRoomExistsForUser("r", "other"));
			assertEquals("name", ModelInferenceLogsUtils.doGetRoomName("u", "r"));
			assertNotNull(ModelInferenceLogsUtils.getRoomById("r", "u"));
			ModelInferenceLogsUtils.setRoomContext("r", "other", "wrong user");
			assertEquals("  context  ", db.value("SELECT ROOM_CONTEXT FROM ROOM"));
			ModelInferenceLogsUtils.setRoomContext("r", "u", null);
			assertNull(db.value("SELECT ROOM_CONTEXT FROM ROOM"));
			ModelInferenceLogsUtils.setRoomOptions("r", "u", Map.of("key", "next"));
			assertEquals("{\"key\":\"next\"}", db.value("SELECT OPTIONS FROM ROOM"));
			ModelInferenceLogsUtils.setRoomOptions("r", "u", null);
			assertNull(db.value("SELECT OPTIONS FROM ROOM"));
			assertTrue(ModelInferenceLogsUtils.doSetRoomToPinned("u", "r", true));
			assertTrue(ModelInferenceLogsUtils.doSetNameForRoomIfDefault("u", "r", "renamed", "name"));
			assertFalse(ModelInferenceLogsUtils.doSetNameForRoomIfDefault("u", "r", "wrong", "name"));
			assertTrue(ModelInferenceLogsUtils.doSetRoomToInactive("u", "r"));
			assertEquals(false, db.value("SELECT IS_ACTIVE FROM ROOM"));
		}
	}
}
