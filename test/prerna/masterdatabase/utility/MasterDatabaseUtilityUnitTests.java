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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Map;

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
}
