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
package prerna.masterdatabase.utility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;

class MasterDatabaseUtilityUnitTests {

	@Test
	void engineDateReadEndsManualTransactionAndHandlesMissingEngine() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE ENGINE (ID VARCHAR, MODIFIEDDATE TIMESTAMP)",
					"INSERT INTO ENGINE VALUES ('db', TIMESTAMP '2026-01-01 12:30:00')");
			db.manual();
			assertEquals(Timestamp.valueOf("2026-01-01 12:30:00").getTime(),
					MasterDatabaseUtility.getEngineDate("db").getTime());
			assertNull(MasterDatabaseUtility.getEngineDate("missing"));
			verify(db.connection, times(2)).rollback();
			assertFalse(db.connection.isClosed());
		}
	}

	@Test
	void positionReplacementRollsBackDeleteWhenBatchFails() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute(
					"CREATE TABLE METAMODELPOSITION (ENGINEID VARCHAR, TABLENAME VARCHAR, XPOS REAL CHECK (XPOS >= 0), YPOS REAL)",
					"INSERT INTO METAMODELPOSITION VALUES ('db','original',1,2)");
			MasterDatabaseUtility.saveMetamodelPositions("db", Map.of("new", Map.of("left", -1, "top", 2)));
			assertEquals("original", db.value("SELECT TABLENAME FROM METAMODELPOSITION"));
			verify(db.connection).rollback();
			MasterDatabaseUtility.saveMetamodelPositions("db", Map.of("new", Map.of("left", 3, "top", 4)));
			assertEquals("new", db.value("SELECT TABLENAME FROM METAMODELPOSITION"));
		}
	}

	@Test
	void suppliedConnectionParticipatesWithoutCommitOrClose() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE METAMODELPOSITION (ENGINEID VARCHAR, TABLENAME VARCHAR, XPOS REAL, YPOS REAL)");
			db.manual();
			MasterDatabaseUtility.saveMetamodelPositions("db", Map.of("t", Map.of("left", 3, "top", 4)), db.connection);
			verify(db.connection, never()).commit();
			db.connection.rollback();
			assertEquals(0, db.count("METAMODELPOSITION"));
		}
	}

	@Test
	void engineDateRetainsMaterializedValueWhenLaterCursorReadFails() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			var statement = mock(java.sql.PreparedStatement.class);
			var result = mock(java.sql.ResultSet.class);
			var timestamp = java.sql.Timestamp.valueOf("2024-01-02 03:04:05");
			doReturn(statement).when(db.connection)
					.prepareStatement("select modifieddate from engine e where e.id = ?");
			when(statement.executeQuery()).thenReturn(result);
			when(result.next()).thenReturn(true).thenThrow(new java.sql.SQLException("cursor failure"));
			when(result.getTimestamp(1)).thenReturn(timestamp);
			db.manual();
			assertEquals(new java.util.Date(timestamp.getTime()), MasterDatabaseUtility.getEngineDate("engine"));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
			verify(statement).close();
			verify(result).close();
		}
	}

	@Test
	void xrayConfigReadsEndManualTransactionsAndTreatAMissingTableAsNoConfigs() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			assertFalse(MasterDatabaseUtility.getXrayConfigList().containsKey("configList"));
			assertEquals("", MasterDatabaseUtility.getXrayConfigFile("missing"));
			db.execute("CREATE TABLE XRAYCONFIGS (FILENAME VARCHAR(800), CONFIG CLOB)",
					"INSERT INTO XRAYCONFIGS VALUES ('a','first'),('b','second')");
			db.manual();
			List<?> configList = (List<?>) MasterDatabaseUtility.getXrayConfigList().get("configList");
			assertEquals(Set.of("a", "b"), configList.stream().map(config -> ((Map<?, ?>) config).get("fileName"))
					.collect(Collectors.toSet()));
			assertEquals("second", MasterDatabaseUtility.getXrayConfigFile("b"));
			assertEquals("", MasterDatabaseUtility.getXrayConfigFile("missing"));
			verify(db.connection, times(3)).rollback();
			verify(db.connection, never()).commit();
			assertFalse(db.connection.isClosed());
		}
	}

	@Test
	void xrayReadsThrowWhenTheLocalMasterConnectionCannotBeAcquired() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			doThrow(new SQLException("down")).when(db.engine).getConnection();
			assertThrows(IllegalArgumentException.class, () -> MasterDatabaseUtility.databaseTranslator("s", "t"));
			assertThrows(IllegalArgumentException.class, () -> MasterDatabaseUtility.getXrayConfigList());
			assertThrows(IllegalArgumentException.class, () -> MasterDatabaseUtility.getXrayConfigFile("a"));
			assertThrows(IllegalArgumentException.class,
					() -> MasterDatabaseUtility.getXrayExisitingMetamodelRDBMS("mv"));
		}
	}

	@Test
	void translatorKeepsMappingsReadBeforeACursorFailure() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			var statement = mock(java.sql.PreparedStatement.class);
			var result = mock(java.sql.ResultSet.class);
			doReturn(statement).when(db.connection).prepareStatement(startsWith("SELECT e.engineName"));
			when(statement.executeQuery()).thenReturn(result);
			when(result.next()).thenReturn(true).thenReturn(true).thenThrow(new SQLException("cursor failure"));
			when(result.getString(3)).thenReturn("source");
			when(result.getString(7)).thenReturn("first").thenReturn("second");
			db.manual();
			assertEquals(Map.of("source", List.of("first", "second")),
					MasterDatabaseUtility.databaseTranslator("s", "t"));
			verify(statement).setString(1, "t");
			verify(statement).setString(2, "s");
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
			verify(statement).close();
			verify(result).close();
		}
	}

	@Test
	void metamodelReadsNodesAndEdgesInSeparateReads() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			createMetamodelTables(db, true, true);
			db.manual();
			Map<String, Object> metamodel = MasterDatabaseUtility.getXrayExisitingMetamodelRDBMS("mv");
			assertEquals(Set.of("mv-Title", "mv-Studio"), ((Map<?, ?>) metamodel.get("nodes")).keySet());
			assertEquals(Set.of("mv-Title-Title-mv-Studio-Studio"), ((Map<?, ?>) metamodel.get("edges")).keySet());
			verify(db.engine, times(2)).getConnection();
			verify(db.connection, times(2)).rollback();
			verify(db.connection, never()).commit();
		}
	}

	@Test
	void metamodelContinuesToTheEdgeReadAfterTheNodeReadFails() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			createMetamodelTables(db, false, true);
			db.manual();
			Map<String, Object> metamodel = MasterDatabaseUtility.getXrayExisitingMetamodelRDBMS("mv");
			assertEquals(Set.of("nodes", "edges"), metamodel.keySet());
			assertTrue(((Map<?, ?>) metamodel.get("nodes")).isEmpty());
			assertTrue(((Map<?, ?>) metamodel.get("edges")).isEmpty());
			verify(db.engine, times(2)).getConnection();
			verify(db.connection, times(2)).rollback();
		}
	}

	@Test
	void metamodelEdgeReadFailureLeavesTheResultEmpty() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			createMetamodelTables(db, true, false);
			assertTrue(MasterDatabaseUtility.getXrayExisitingMetamodelRDBMS("mv").isEmpty());
			verify(db.engine, times(2)).getConnection();
		}
	}

	@Test
	void metamodelThrowsWhenTheEdgeReadCannotAcquireAConnection() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			createMetamodelTables(db, true, true);
			when(db.engine.getConnection()).thenReturn(db.connection).thenThrow(new SQLException("down"));
			assertThrows(IllegalArgumentException.class,
					() -> MasterDatabaseUtility.getXrayExisitingMetamodelRDBMS("mv"));
		}
	}

	private static void createMetamodelTables(JdbcTestDatabase db, boolean concepts, boolean relations)
			throws Exception {
		db.execute("CREATE TABLE ENGINE (ID VARCHAR, ENGINENAME VARCHAR)", "INSERT INTO ENGINE VALUES ('e1','mv')",
				"CREATE TABLE ENGINECONCEPT (ENGINE VARCHAR, LOCALCONCEPTID VARCHAR, PHYSICALNAME VARCHAR, PHYSICALNAMEID VARCHAR, PARENTPHYSICALID VARCHAR, PROPERTY BOOLEAN)",
				"INSERT INTO ENGINECONCEPT VALUES ('e1','lc1','TITLE','p1','p1',FALSE),('e1','lc2','STUDIO','p2','p2',FALSE)");
		if (concepts) {
			db.execute("CREATE TABLE CONCEPT (LOCALCONCEPTID VARCHAR, CONCEPTUALNAME VARCHAR)",
					"INSERT INTO CONCEPT VALUES ('lc1','Title'),('lc2','Studio')");
		}
		if (relations) {
			db.execute("CREATE TABLE ENGINERELATION (ENGINE VARCHAR, SOURCECONCEPTID VARCHAR, TARGETCONCEPTID VARCHAR)",
					"INSERT INTO ENGINERELATION VALUES ('e1','p1','p2')");
		}
	}
}
