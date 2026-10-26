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
package prerna.engine.impl.rdbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.rdbms.RdbmsConnectionHelper.ColumnMetadata;
import prerna.util.sql.RdbmsTypeEnum;

class RdbmsConnectionHelperUnitTests {

	@Test
	void manyTablesThatAreMuchOfTheSchemaAreReadInOneCall() throws Exception {
		List<String[]> tables = tables(20, "public");
		List<Map<String, String>> rows = new ArrayList<>();
		for (String[] table : tables) {
			rows.add(column(table[0], "public", "id", "INTEGER"));
			rows.add(column(table[0], "public", "note", "VARCHAR"));
		}
		rows.add(column("not_asked_for", "public", "id", "INTEGER"));
		DatabaseMetaData meta = metadataReturning(rows);

		Map<String, List<ColumnMetadata>> columns = RdbmsConnectionHelper.readColumns(meta, tables, 40, "db", "public",
				RdbmsTypeEnum.POSTGRES);

		verify(meta, times(1)).getColumns(any(), any(), any(), any());
		verify(meta).getColumns(eq("db"), eq("public"), isNull(), isNull());
		assertEquals(20, columns.size());
		assertEquals(List.of(new ColumnMetadata("id", "INTEGER"), new ColumnMetadata("note", "VARCHAR")),
				columns.get("t0"));
		assertFalse(columns.containsKey("not_asked_for"));
	}

	@Test
	void aFewTablesOfALargeSchemaAreReadOneAtATime() throws Exception {
		List<String[]> tables = tables(3, "public");
		List<Map<String, String>> rows = new ArrayList<>();
		for (String[] table : tables) {
			rows.add(column(table[0], "public", "id", "INTEGER"));
		}
		DatabaseMetaData meta = metadataReturning(rows);

		Map<String, List<ColumnMetadata>> columns = RdbmsConnectionHelper.readColumns(meta, tables, 1_000, "db",
				"public", RdbmsTypeEnum.POSTGRES);

		verify(meta, never()).getColumns(any(), any(), isNull(), any());
		verify(meta, times(3)).getColumns(any(), any(), anyString(), any());
		assertEquals(List.of(new ColumnMetadata("id", "INTEGER")), columns.get("t2"));
	}

	@Test
	void aTableKeepsOnlyItsOwnColumnsWhenItsNameMatchesOthersAsAPattern() throws Exception {
		// in getColumns, ORDER_ITEMS is a pattern where _ matches any character
		List<String[]> tables = List.<String[]>of(new String[] { "ORDER_ITEMS", "TABLE", "public" });
		DatabaseMetaData meta = metadataReturning(List.of(column("ORDER_ITEMS", "public", "id", "INTEGER"),
				column("ORDERXITEMS", "public", "other", "VARCHAR")));

		Map<String, List<ColumnMetadata>> columns = RdbmsConnectionHelper.readColumns(meta, tables, 2, "db", "public",
				RdbmsTypeEnum.POSTGRES);

		assertEquals(List.of(new ColumnMetadata("id", "INTEGER")), columns.get("ORDER_ITEMS"));
	}

	@Test
	void aTableOfTheSameNameInAnotherSchemaIsLeftOut() throws Exception {
		List<String[]> tables = tables(20, "sales");
		List<Map<String, String>> rows = new ArrayList<>();
		for (String[] table : tables) {
			rows.add(column(table[0], "sales", "id", "INTEGER"));
		}
		rows.add(column("t0", "archive", "archived_on", "DATE"));
		DatabaseMetaData meta = metadataReturning(rows);

		Map<String, List<ColumnMetadata>> columns = RdbmsConnectionHelper.readColumns(meta, tables, 20, "db", null,
				RdbmsTypeEnum.POSTGRES);

		assertEquals(List.of(new ColumnMetadata("id", "INTEGER")), columns.get("t0"));
	}

	@Test
	void aFailedSingleCallFallsBackToOneCallPerTable() throws Exception {
		List<String[]> tables = tables(20, "public");
		DatabaseMetaData meta = mock(DatabaseMetaData.class);
		when(meta.getColumns(any(), any(), any(), any())).thenAnswer(invocation -> {
			String table = invocation.getArgument(2);
			if (table == null) {
				throw new SQLException("the schema is too large to describe at once");
			}
			return resultSet(List.of(column(table, "public", "id", "INTEGER")));
		});

		Map<String, List<ColumnMetadata>> columns = RdbmsConnectionHelper.readColumns(meta, tables, 20, "db", "public",
				RdbmsTypeEnum.POSTGRES);

		verify(meta, times(20)).getColumns(any(), any(), anyString(), any());
		assertEquals(20, columns.size());
	}

	@Test
	void oneCallIsUsedForEnoughTablesThatAreEnoughOfTheSchemaButNeverForOracle() {
		int least = RdbmsConnectionHelper.MIN_TABLES_FOR_SCHEMA_READ;
		assertTrue(RdbmsConnectionHelper.readsWholeSchema(least, least * 4, RdbmsTypeEnum.POSTGRES));
		assertFalse(RdbmsConnectionHelper.readsWholeSchema(least - 1, least - 1, RdbmsTypeEnum.POSTGRES));
		// 25 tables of a 50,000 table schema
		assertFalse(RdbmsConnectionHelper.readsWholeSchema(25, 50_000, RdbmsTypeEnum.SNOWFLAKE));
		// Oracle lists the tables of every owner, so one call would read the whole
		// database
		assertFalse(RdbmsConnectionHelper.readsWholeSchema(500, 500, RdbmsTypeEnum.ORACLE));
	}

	/** Tables t0, t1 and so on, as the table listing gives them */
	private static List<String[]> tables(int count, String schema) {
		List<String[]> tables = new ArrayList<>();
		for (int index = 0; index < count; index++) {
			tables.add(new String[] { "t" + index, "TABLE", schema });
		}
		return tables;
	}

	private static Map<String, String> column(String table, String schema, String name, String type) {
		return Map.of("TABLE_NAME", table, "TABLE_SCHEM", schema, "COLUMN_NAME", name, "TYPE_NAME", type);
	}

	/**
	 * Metadata whose getColumns answers as a database does: every row for no table,
	 * and the rows of the tables the pattern matches for a table, which the tests
	 * keep simple by returning every row too
	 */
	private static DatabaseMetaData metadataReturning(List<Map<String, String>> rows) throws SQLException {
		DatabaseMetaData meta = mock(DatabaseMetaData.class);
		when(meta.getColumns(any(), any(), any(), any())).thenAnswer(invocation -> resultSet(rows));
		return meta;
	}

	/**
	 * A forward-only result set over the rows, read by column label as JDBC does,
	 * ignoring case
	 */
	private static ResultSet resultSet(List<Map<String, String>> rows) {
		int[] cursor = { -1 };
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[] { ResultSet.class },
				(proxy, method, args) -> switch (method.getName()) {
				case "next" -> ++cursor[0] < rows.size();
				case "getString" -> rows.get(cursor[0]).get(((String) args[0]).toUpperCase(Locale.ROOT));
				case "close" -> null;
				case "hashCode" -> System.identityHashCode(proxy);
				case "equals" -> proxy == args[0];
				case "toString" -> "fake result set";
				default -> throw new UnsupportedOperationException(method.getName());
				});
	}
}
