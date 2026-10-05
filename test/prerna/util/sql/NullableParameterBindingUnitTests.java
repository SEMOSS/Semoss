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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

class NullableParameterBindingUnitTests {

	static void roundTrip(Connection connection, AbstractSqlQueryUtil util) throws Exception {
		Gson gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
		try (var statement = connection.createStatement()) {
			statement.execute("CREATE TABLE BIND_VALUES (ID INT PRIMARY KEY, SHORT_TEXT VARCHAR(200), LONG_TEXT "
					+ util.getClobDataTypeName() + ", JSON_TEXT " + util.getClobDataTypeName() + ", RAW_BYTES "
					+ util.getBlobDataTypeName() + ", EMPTY_AS_NULL VARCHAR(200))");
		}
		List<String> strings = Arrays.asList(null, "", "   ", " leading and trailing ",
				"\u00e9\u4e16\u754c\ud83d\ude00");
		List<byte[]> binaries = Arrays.asList(null, new byte[0], new byte[] { 0, -1, -128, 1, -61, 40 },
				new byte[] { 1, 2, 3 }, new byte[] { 0 });
		List<Object> json = Arrays.asList(null, Map.of("text", "<value>"), List.of("a", 2), Map.of(), List.of());
		for (int i = 0; i < strings.size(); i++) {
			String large = i == 4 ? strings.get(i).repeat(25000) : strings.get(i);
			try (var ps = connection.prepareStatement("INSERT INTO BIND_VALUES VALUES (?, ?, ?, ?, ?, ?)")) {
				ps.setInt(1, i);
				util.setNullableString(ps, 2, strings.get(i));
				util.setNullableLargeText(ps, 3, large);
				util.setNullableJson(ps, 4, json.get(i), gson);
				util.setNullableBinary(ps, 5, binaries.get(i));
				util.setStringEmptyAsNullable(ps, 6, strings.get(i));
				ps.executeUpdate();
			}
			try (var ps = connection.prepareStatement("SELECT * FROM BIND_VALUES WHERE ID=?")) {
				ps.setInt(1, i);
				try (var rs = ps.executeQuery()) {
					assertTrue(rs.next());
					assertEquals(strings.get(i), rs.getString(2));
					assertEquals(large, rs.getString(3));
					assertEquals(json.get(i) == null ? null : gson.toJson(json.get(i)), rs.getString(4));
					assertArrayEquals(binaries.get(i), rs.getBytes(5));
					assertEquals(i < 2 ? null : strings.get(i), rs.getString(6));
				}
			}
		}
		String serialized = "  {\"list\":[1,2],\"value\":null}  ";
		try (var ps = connection.prepareStatement("UPDATE BIND_VALUES SET JSON_TEXT=? WHERE ID=0")) {
			util.setNullableLargeText(ps, 1, serialized);
			ps.executeUpdate();
		}
		try (var ps = connection.prepareStatement("SELECT JSON_TEXT FROM BIND_VALUES WHERE ID=0");
				var rs = ps.executeQuery()) {
			assertTrue(rs.next());
			assertEquals(serialized, rs.getString(1));
		}
	}

	@Test
	void nullJdbcTypesFollowStorageIndependentlyOfNonNullValues() throws Exception {
		AbstractSqlQueryUtil[] utils = { new H2QueryUtil(), new PostgresQueryUtil(), new SQLiteQueryUtil(),
				new MicrosoftSqlServerQueryUtil() };
		int[] textTypes = { Types.CLOB, Types.LONGVARCHAR, Types.VARCHAR, Types.LONGVARCHAR };
		int[] binaryTypes = { Types.BLOB, Types.BINARY, Types.BLOB, Types.LONGVARBINARY };
		for (int i = 0; i < utils.length; i++) {
			PreparedStatement ps = mock(PreparedStatement.class);
			utils[i].setNullableString(ps, 1, null);
			utils[i].setNullableLargeText(ps, 2, null);
			utils[i].setNullableBinary(ps, 3, null);
			utils[i].setNullableJson(ps, 4, null, new Gson());
			verify(ps).setNull(1, Types.VARCHAR);
			verify(ps).setNull(2, textTypes[i]);
			verify(ps).setNull(3, binaryTypes[i]);
			verify(ps).setNull(4, textTypes[i]);
		}
	}

}
