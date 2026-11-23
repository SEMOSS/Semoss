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
package prerna.reactor.apptest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RegressionH2DatabaseUnitTests {
    private static final String BASELINE = "CREATE TABLE TASKS (ID INTEGER PRIMARY KEY, TITLE VARCHAR(80));";
    private static final String SEED = "INSERT INTO TASKS (ID, TITLE) VALUES (1, 'Synthetic task');";
    private static final String SELECT = "SELECT TITLE FROM TASKS WHERE ID=1";
    private static final String UPDATE = "UPDATE TASKS SET TITLE='Done' WHERE ID=1";

    @TempDir
    Path root;

    @Test
    void usesIndependentFileDatabasesAndClosesBeforeRemovingThem() throws Exception {
        try (var source = RegressionH2Database.open(root, BASELINE, "", SEED);
                var first = RegressionH2Database.open(root, BASELINE, "", SEED);
                var second = RegressionH2Database.open(root, BASELINE,
                        "ALTER TABLE TASKS ADD COLUMN DONE BOOLEAN;", SEED)) {
            assertEquals(List.of(List.of("Synthetic task")), rows(first.execute(SELECT, 10)));
            assertEquals(true, first.execute(UPDATE, 10));
            assertEquals(List.of(List.of("Done")), rows(first.execute(SELECT, 10)));
            assertEquals(List.of(List.of("Synthetic task")), rows(second.execute(SELECT, 10)));
            assertEquals(List.of(List.of("Synthetic task")), rows(source.execute(SELECT, 10)));
        }
        try (var remaining = Files.list(root)) {
            assertEquals(0, remaining.count());
        }
    }

    @Test
    void rejectsUnsupportedFixtureAndRuntimeStatements() throws Exception {
        assertThrows(IOException.class, () -> RegressionH2Database.open(root, BASELINE, "",
                "INSERT INTO TASKS (ID,TITLE) VALUES (1, CURRENT_USER());"));
        assertThrows(IllegalArgumentException.class, () -> RegressionH2Database.isRead(SELECT + "; SELECT * FROM TASKS"));
        assertThrows(IllegalArgumentException.class, () -> RegressionH2Database.isRead("SELECT CURRENT_USER() FROM TASKS"));
        assertThrows(IllegalArgumentException.class, () -> RegressionH2Database.isRead("SELECT * FROM INFORMATION_SCHEMA.USERS"));
        assertTrue(RegressionH2Database.isRead(SELECT));
        assertFalse(RegressionH2Database.isRead(UPDATE));
        try (var remaining = Files.list(root)) {
            assertEquals(0, remaining.count());
        }
    }

    @Test
    void supportsCompositeConditionsSortsConstraintsAndMultilineSeedText() throws Exception {
        String schema = "CREATE TABLE BOARDS (ID VARCHAR(40) PRIMARY KEY);"
                + "CREATE TABLE TASKS (ID VARCHAR(40) PRIMARY KEY, BOARD_ID VARCHAR(40), TITLE VARCHAR(80),"
                + " STATUS VARCHAR(40) CHECK (STATUS IN ('Ready','Done')), POSITION BIGINT,"
                + " FOREIGN KEY (BOARD_ID) REFERENCES BOARDS(ID) ON DELETE CASCADE);";
        String seed = "INSERT INTO BOARDS VALUES ('b1');"
                + "INSERT INTO TASKS VALUES ('t1','b1','Line one\nLine two','Ready',1);";
        try (var database = RegressionH2Database.open(root, schema, "", seed)) {
            assertEquals(List.of(List.of("Line one\nLine two")), rows(database.execute("SELECT TITLE FROM TASKS ORDER BY POSITION, ID", 100)));
            database.execute("UPDATE TASKS SET STATUS='Done' WHERE ID='t1' AND BOARD_ID='b1'", 100);
            assertEquals(List.of(List.of("Done")), rows(database.execute("SELECT STATUS FROM TASKS WHERE ID='t1' AND BOARD_ID='b1'", 100)));
            assertThrows(java.sql.SQLException.class, () -> database.execute("UPDATE TASKS SET STATUS='Invalid' WHERE ID='t1'", 100));
        }
    }

    @Test
    void dispatchesOnlyScopedSqlThroughPixelAndRevokesTheCapability() throws Exception {
        String runId = UUID.randomUUID().toString();
        String otherRunId = UUID.randomUUID().toString();
        String capability;
        try (var database = RegressionH2Database.open(root, BASELINE, "", SEED);
                var otherDatabase = RegressionH2Database.open(root, BASELINE, "", SEED);
                var session = new RegressionPixelAccess(runId, "source", database, Instant.now().plusSeconds(30));
                var otherSession = new RegressionPixelAccess(otherRunId, "source", otherDatabase, Instant.now().plusSeconds(30))) {
            capability = session.secret();
            assertThrows(SecurityException.class, () -> RegressionPixelAccess.authenticate(otherRunId, session.secret()));
            assertThrows(SecurityException.class, () -> session.execute(pixel("other", SELECT), null));
            assertThrows(SecurityException.class, () -> session.execute(pixel("source", SELECT), "unrelated-insight"));
            assertThrows(SecurityException.class, () -> session.execute(pixel("source", SELECT) + "META | init;", null));
            assertThrows(SecurityException.class, () -> session.execute("META | init;", null));
            assertFalse(session.exercisedDatabase());
            var response = RegressionPixelAccess.authenticate(runId, capability).execute(pixel("source", SELECT), null);
            var output = (Map<?, ?>) ((List<?>) response.get("pixelReturn")).getFirst();
            assertEquals(List.of(List.of("Synthetic task")), rows(output.get("output")));
            session.execute(pixel("source", UPDATE), (String) response.get("insightID"));
            assertTrue(session.exercisedDatabase());
            assertFalse(otherSession.exercisedDatabase());
            assertEquals(List.of(List.of("Done")), rows(database.execute(SELECT, 10)));
            assertEquals(List.of(List.of("Synthetic task")), rows(otherDatabase.execute(SELECT, 10)));
        }
        assertThrows(SecurityException.class, () -> RegressionPixelAccess.authenticate(runId, capability));
    }

    private static String pixel(String engine, String sql) {
        return "SqlQuery(database=[\"" + engine + "\"], query=[\"" + sql + "\"]);";
    }

    private static Object rows(Object output) {
        return ((Map<?, ?>) ((Map<?, ?>) output).get("data")).get("values");
    }
}
