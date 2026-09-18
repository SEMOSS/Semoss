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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.OWLEngineFactory;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Schema-focused tests for the MEMORY / MEMORY_ACTION_ITEM / MEMORY_AUDIT /
 * MEMORY_RELATIONSHIP tables added to {@link ModelInferenceLogsOwlCreator}
 * and wired into {@link ModelInferenceLogsUtils}'s init/migration path.
 */
public class MemorySchemaUnitTests {

	private static final List<String> MEMORY_TABLES = List.of("MEMORY", "MEMORY_ACTION_ITEM", "MEMORY_AUDIT",
			"MEMORY_RELATIONSHIP");

	@Test
	void memoryTableIsDefinedWithExpectedColumns() {
		AbstractSqlQueryUtil queryUtil = mock(AbstractSqlQueryUtil.class);
		ModelInferenceLogsOwlCreator creator = new ModelInferenceLogsOwlCreator(queryUtil);

		Map<String, List<Pair<String, String>>> schemaByTable = new java.util.HashMap<>();
		for (Pair<String, List<Pair<String, String>>> table : creator.getDBSchema()) {
			schemaByTable.put(table.getValue0(), table.getValue1());
		}

		for (String tableName : MEMORY_TABLES) {
			assertTrue(schemaByTable.containsKey(tableName), tableName + " should be defined in the OWL schema");
		}

		List<String> memoryColumns = schemaByTable.get("MEMORY").stream().map(Pair::getValue0).toList();
		assertTrue(memoryColumns.containsAll(List.of("MEMORY_ID", "USER_ID", "ROOM_ID", "WORKSPACE_ID", "PROJECT_ID",
				"EVENT_TYPE", "CONTENT", "METADATA", "EMBEDDING", "PARENT_MEMORY_ID", "SUPERSEDES_MEMORY_ID",
				"DELETED", "DATE_CREATED", "DATE_UPDATED", "DELETED_AT")));

		List<String> actionItemColumns = schemaByTable.get("MEMORY_ACTION_ITEM").stream().map(Pair::getValue0)
				.toList();
		assertTrue(actionItemColumns.containsAll(List.of("ACTION_ITEM_ID", "MEMORY_ID", "CONTENT", "OWNER", "STATUS",
				"DUE_DATE", "USER_ID", "ROOM_ID", "WORKSPACE_ID")));

		List<String> auditColumns = schemaByTable.get("MEMORY_AUDIT").stream().map(Pair::getValue0).toList();
		assertTrue(auditColumns.containsAll(
				List.of("AUDIT_ID", "MEMORY_ID", "ACTION", "PREVIOUS_CONTENT", "PREVIOUS_METADATA", "USER_ID")));

		List<String> relationshipColumns = schemaByTable.get("MEMORY_RELATIONSHIP").stream().map(Pair::getValue0)
				.toList();
		assertTrue(relationshipColumns.containsAll(
				List.of("RELATIONSHIP_ID", "SOURCE_MEMORY_ID", "TARGET_MEMORY_ID", "RELATIONSHIP_TYPE", "WEIGHT")));
	}

	/**
	 * Mirrors {@code ModelInferenceLogsUtilsUnitTests#initModelInferenceLogsDatabase}
	 * but asserts specifically that the new MEMORY-family indexes are created
	 * during init, using the IF-NOT-EXISTS index syntax branch.
	 */
	@Test
	void initModelInferenceLogsDatabaseCreatesMemoryIndexes() throws Exception {
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		WriteOWLEngine owlEngine = mock(WriteOWLEngine.class);
		OWLEngineFactory owlFactory = mock(OWLEngineFactory.class);
		AbstractSqlQueryUtil queryUtil = mock(AbstractSqlQueryUtil.class);
		Connection conn = mock(Connection.class);
		Statement stmt = mock(Statement.class);

		Field registryField = SystemEngineRegistry.class.getDeclaredField("modelInferenceLogsDbHolder");
		registryField.setAccessible(true);
		registryField.set(null, (Supplier<IRDBMSEngine>) () -> engine);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<ConnectionUtils> connUtil = Mockito.mockStatic(ConnectionUtils.class)) {
			when(engine.getQueryUtil()).thenReturn(queryUtil);
			when(engine.getOWLEngineFactory()).thenReturn(owlFactory);
			when(owlFactory.getWriteOWL()).thenReturn(owlEngine);
			when(engine.getConnection()).thenReturn(conn);

			when(queryUtil.allowsIfExistsTableSyntax()).thenReturn(true);
			when(queryUtil.allowIfExistsIndexSyntax()).thenReturn(true);
			when(queryUtil.allowIfExistsAddConstraint()).thenReturn(true);
			when(queryUtil.createTableIfNotExists(anyString(), any(String[].class), any(String[].class)))
					.thenReturn("create table");
			when(queryUtil.createIndexIfNotExists(anyString(), anyString(), anyString())).thenReturn("create index");
			when(queryUtil.createIndexIfNotExists(anyString(), anyString(), any(List.class)))
					.thenReturn("create composite index");

			when(conn.createStatement()).thenReturn(stmt);
			when(stmt.execute(anyString())).thenReturn(true);
			when(conn.getAutoCommit()).thenReturn(false);

			ModelInferenceLogsUtils.initModelInferenceLogsDatabase();

			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_USER_ID_INDEX", "MEMORY", "USER_ID");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_ROOM_ID_INDEX", "MEMORY", "ROOM_ID");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_WORKSPACE_ID_INDEX", "MEMORY", "WORKSPACE_ID");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_EVENT_TYPE_INDEX", "MEMORY", "EVENT_TYPE");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_PARENT_MEMORY_ID_INDEX", "MEMORY",
					"PARENT_MEMORY_ID");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_DELETED_INDEX", "MEMORY", "DELETED");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_DATE_CREATED_INDEX", "MEMORY", "DATE_CREATED");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_ACTION_ITEM_MEMORY_ID_INDEX",
					"MEMORY_ACTION_ITEM", "MEMORY_ID");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_ACTION_ITEM_STATUS_INDEX",
					"MEMORY_ACTION_ITEM", "STATUS");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_ACTION_ITEM_USER_ID_INDEX",
					"MEMORY_ACTION_ITEM", "USER_ID");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_AUDIT_MEMORY_ID_INDEX", "MEMORY_AUDIT",
					"MEMORY_ID");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_RELATIONSHIP_SOURCE_INDEX",
					"MEMORY_RELATIONSHIP", "SOURCE_MEMORY_ID");
			verify(queryUtil, times(1)).createIndexIfNotExists("MEMORY_RELATIONSHIP_TARGET_INDEX",
					"MEMORY_RELATIONSHIP", "TARGET_MEMORY_ID");
		} finally {
			registryField.set(null, null);
		}
	}

	@AfterEach
	void clearRegistry() throws Exception {
		Field registryField = SystemEngineRegistry.class.getDeclaredField("modelInferenceLogsDbHolder");
		registryField.setAccessible(true);
		registryField.set(null, null);
	}

	@BeforeEach
	void resetInitFlag() throws Exception {
		Field initialized = ModelInferenceLogsUtils.class.getDeclaredField("initialized");
		initialized.setAccessible(true);
		initialized.setBoolean(null, false);
	}
}
