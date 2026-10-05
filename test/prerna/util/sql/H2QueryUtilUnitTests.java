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
package prerna.util.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * These exercise a real H2 2.2.220 driver/engine (no mocks) specifically
 * because the 1.4.200 -> 2.2.220 upgrade changed INFORMATION_SCHEMA shapes and
 * removed/reinterpreted several connection settings; the goal is to prove the
 * rewritten queries and trimmed connection string actually work against the
 * new engine rather than assuming compatibility from release notes.
 */
class H2QueryUtilUnitTests {

	@Test
	void h2RoundTripsExactValues() throws Exception {
		try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:")) {
			NullableParameterBindingUnitTests.roundTrip(connection, new H2QueryUtil());
		}
	}

	/**
	 * H2Frame builds its additional connection properties from this exact
	 * string (post H2-2.x trim). H2 2.x throws "Unsupported connection setting"
	 * at connect time for any leftover PageStore-only key, so this proves the
	 * trimmed string still opens a real file-backed connection.
	 */
	@Test
	void trimmedAdditionalPropsStillOpenAConnection(@TempDir Path tempDir) throws Exception {
		File dbFile = tempDir.resolve("H2_Store_TEST.mv.db").toFile();
		dbFile.createNewFile();

		Map<String, Object> connDetails = new HashMap<>();
		connDetails.put(AbstractSqlQueryUtil.HOSTNAME, dbFile.getAbsolutePath());
		connDetails.put(AbstractSqlQueryUtil.ADDITIONAL, "CACHE_SIZE=65536");

		H2QueryUtil util = new H2QueryUtil();
		String connectionUrl = util.setConnectionDetailsfromMap(connDetails);

		try (Connection connection = AbstractSqlQueryUtil.makeConnection(RdbmsTypeEnum.H2_DB, connectionUrl, "sa",
				"")) {
			assertTrue(connection.isValid(2));
		}
	}

	@Test
	void informationSchemaQueriesMatchH2TwoPointXSchema() throws Exception {
		H2QueryUtil util = new H2QueryUtil();
		try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:")) {
			try (Statement stmt = connection.createStatement()) {
				stmt.execute("CREATE TABLE PARENT (ID INT PRIMARY KEY)");
				stmt.execute("CREATE TABLE WIDGET (ID INT PRIMARY KEY, NAME VARCHAR(200), QTY INT, PARENT_ID INT)");
				stmt.execute("CREATE INDEX WIDGET_NAME_INDEX ON WIDGET(NAME)");
				stmt.execute("ALTER TABLE WIDGET ADD CONSTRAINT WIDGET_QTY_CHECK CHECK (QTY >= 0)");
				stmt.execute(
						"ALTER TABLE WIDGET ADD CONSTRAINT WIDGET_PARENT_FK FOREIGN KEY (PARENT_ID) REFERENCES PARENT(ID)");
			}

			// tableExistsQuery, exercised through the shared helper
			assertTrue(util.tableExists(connection, "WIDGET", null, null));

			try (ResultSet rs = connection.createStatement()
					.executeQuery(util.tableConstraintExistsQuery("WIDGET_QTY_CHECK", "WIDGET", null, null))) {
				assertTrue(rs.next(), "tableConstraintExistsQuery should find the CHECK constraint");
			}

			try (ResultSet rs = connection.createStatement()
					.executeQuery(util.referentialConstraintExistsQuery("WIDGET_PARENT_FK", null, null))) {
				assertTrue(rs.next(), "referentialConstraintExistsQuery should find the FK constraint");
			}

			Set<String> columns = new HashSet<>();
			try (ResultSet rs = connection.createStatement()
					.executeQuery(util.getAllColumnDetails("WIDGET", null, null))) {
				while (rs.next()) {
					columns.add(rs.getString("COLUMN_NAME"));
					// DATA_TYPE replaced TYPE_NAME in H2 2.x - getString throws if the column is absent
					rs.getString("DATA_TYPE");
				}
			}
			assertEquals(Set.of("ID", "NAME", "QTY", "PARENT_ID"), columns);

			try (ResultSet rs = connection.createStatement()
					.executeQuery(util.columnDetailsQuery("WIDGET", "NAME", null, null))) {
				assertTrue(rs.next());
				assertEquals("NAME", rs.getString("COLUMN_NAME"));
			}

			boolean foundIndex = false;
			try (ResultSet rs = connection.createStatement().executeQuery(util.getIndexList(null, null))) {
				while (rs.next()) {
					if ("WIDGET_NAME_INDEX".equalsIgnoreCase(rs.getString("INDEX_NAME"))) {
						foundIndex = true;
						assertEquals("WIDGET", rs.getString("TABLE_NAME"));
					}
				}
			}
			assertTrue(foundIndex, "getIndexList should surface the created index");

			// getIndexDetails must return (TABLE_NAME, COLUMN_NAME) in that order - callers
			// such as RdbmsCsvUploadReactor.findIndexes rely on positional access
			try (ResultSet rs = connection.createStatement()
					.executeQuery(util.getIndexDetails("WIDGET_NAME_INDEX", "WIDGET", null, null))) {
				assertTrue(rs.next());
				assertEquals("WIDGET", rs.getString(1));
				assertEquals("NAME", rs.getString(2));
			}

			// allIndexForTableQuery must return (INDEX_NAME, COLUMN_NAME) in that order -
			// RdbmsModifier.addIndex relies on positional access. WIDGET also has H2's
			// own auto-created primary-key index, so this must only check that our
			// explicit index is present among the rows, not that it is the only one.
			boolean foundColumn = false;
			try (ResultSet rs = connection.createStatement()
					.executeQuery(util.allIndexForTableQuery("WIDGET", null, null))) {
				while (rs.next()) {
					if ("WIDGET_NAME_INDEX".equalsIgnoreCase(rs.getString(1))
							&& "NAME".equalsIgnoreCase(rs.getString(2))) {
						foundColumn = true;
					}
				}
			}
			assertTrue(foundColumn, "allIndexForTableQuery should list the indexed column");
		}
	}

	/**
	 * AuditDatabase.init() creates AUDIT_TABLE with a bare "IDENTITY" column
	 * type. Confirms H2 2.x still accepts that literal keyword and still
	 * auto-generates values for it, rather than assuming so.
	 */
	@Test
	void bareIdentityColumnTypeStillValid() throws Exception {
		try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:")) {
			try (Statement stmt = connection.createStatement()) {
				stmt.execute("CREATE TABLE AUDIT_TABLE (AUTO_INCREMENT IDENTITY, ID VARCHAR(50))");
				stmt.execute("INSERT INTO AUDIT_TABLE (ID) VALUES ('abc')");
			}
			try (ResultSet rs = connection.createStatement()
					.executeQuery("SELECT AUTO_INCREMENT FROM AUDIT_TABLE WHERE ID = 'abc'")) {
				assertTrue(rs.next());
				assertTrue(rs.getLong(1) > 0);
			}
		}
	}
}
