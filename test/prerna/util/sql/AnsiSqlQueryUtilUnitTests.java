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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

class AnsiSqlQueryUtilUnitTests {

	@Test
	void legacyClobInputsAndEncodingContractsRemainCompatible() throws Exception {
		H2QueryUtil util = new H2QueryUtil();
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:legacy_" + UUID.randomUUID())) {
			try (var statement = connection.createStatement()) {
				statement.execute("CREATE TABLE LEGACY (VALUE CLOB)");
			}
			java.sql.Clob clob = connection.createClob();
			try {
				clob.setString(1, "  clob  ");
				for (Object value : Arrays.asList(null, "", "  text  ", Map.of("html", "<value>"), clob)) {
					try (var ps = connection.prepareStatement("INSERT INTO LEGACY VALUES (?)")) {
						util.handleInsertionOfClob(connection, ps, value, 1, gson);
						ps.executeUpdate();
					}
				}
				try (var ps = connection.prepareStatement("SELECT VALUE FROM LEGACY"); var rs = ps.executeQuery()) {
					for (String expected : Arrays.asList(null, "", "  text  ", gson.toJson(Map.of("html", "<value>")),
							"  clob  ")) {
						assertTrue(rs.next());
						assertEquals(expected, rs.getString(1));
					}
					assertFalse(rs.next());
				}
			} finally {
				clob.free();
			}
		}
		PreparedStatement statement = mock(PreparedStatement.class);
		new PostgresQueryUtil().handleInsertionOfClob(statement, null, 1, gson);
		verify(statement).setNull(1, Types.LONGVARCHAR);
	}
}
