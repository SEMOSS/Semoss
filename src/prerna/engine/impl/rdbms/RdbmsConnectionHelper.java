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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.zaxxer.hikari.HikariDataSource;

import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class RdbmsConnectionHelper {

	private static final Logger classLogger = LogManager.getLogger(RdbmsConnectionHelper.class);

	/**
	 * The fewest tables whose columns {@link #readColumns} reads in one call for
	 * the whole schema. For fewer tables, a call per table is only a handful of
	 * round trips, which costs less than reading every column of the schema.
	 */
	static final int MIN_TABLES_FOR_SCHEMA_READ = 20;
	/**
	 * The smallest share of the schema's tables whose columns {@link #readColumns}
	 * reads in one call for the whole schema. One call reads the columns of every
	 * table in the schema, so for 25 tables out of a 50,000 table schema, as a
	 * large warehouse can have, 25 calls are faster. The single call is only worth
	 * it when the tables asked for are a good part of what it reads.
	 */
	static final double MIN_SHARE_FOR_SCHEMA_READ = 0.25;

	/**
	 * One column of a table or view, as the database's metadata describes it.
	 *
	 * @param name the column's name
	 * @param type the database's type name for the column
	 */
	public record ColumnMetadata(String name, String type) {
	}

	private RdbmsConnectionHelper() {

	}

	/**
	 * 
	 * @param driver
	 * @param connectURI
	 * @param userName
	 * @param password
	 * @return
	 * @throws SQLException
	 */
	public static HikariDataSource getDataSourceFromPool(String driver, String connectURI, String userName,
			String password) throws SQLException {
		HikariDataSource ds = new HikariDataSource();
		ds.setDriverClassName(driver);
		ds.setJdbcUrl(connectURI);
		ds.setUsername(userName);
		ds.setPassword(password);
		return ds;
	}

	/**
	 * Try to predict the current schema for a given database connection
	 * 
	 * @param meta
	 * @param con
	 * @return
	 */
	public static String getSchema(DatabaseMetaData meta, Connection con, String connectionUrl,
			RdbmsTypeEnum rdbmsType) {
		String schema = null;
		String driverName = null;
		try {
			driverName = meta.getDriverName();
			if (driverName != null) {
				driverName = driverName.toLowerCase();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to read driver name from database metadata", e);
		}

		// in oracle
		// the datbase/schema/user are all considered the same thing
		// so here, if we want to filter
		// we use the user name
		if ((driverName != null && driverName.contains("oracle"))
				|| (rdbmsType != null && rdbmsType == RdbmsTypeEnum.ORACLE)) {
			try {
				schema = meta.getUserName();
			} catch (SQLException e) {
				classLogger.error("Failed to read user name from database metadata for Oracle schema lookup", e);
			}
		}

		if (schema != null) {
			return schema;
		}

		// THIS IS BECAUSE ONLY JAVA 7 REQUIRES
		// THIS METHOD OT BE IMPLEMENTED ON THE
		// DRIVERS
		try {
			if (meta.getJDBCMajorVersion() >= 7) {
				schema = con.getSchema();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to read schema directly from JDBC connection", e);
		}

		// hive doesn't support getURL
		if (rdbmsType != RdbmsTypeEnum.HIVE) {
			String url = null;
			try {
				url = meta.getURL();
			} catch (SQLException e) {
				classLogger.error("Failed to read JDBC URL from database metadata", e);
			}

			schema = predictSchemaFromUrl(url);
			if (schema != null) {
				return schema;
			}
		}
		schema = predictSchemaFromUrl(connectionUrl);
		if (schema != null) {
			return schema;
		}

		// add logic for when schema is called database
		if ((driverName != null && driverName.contains("teradata"))
				|| (rdbmsType != null && rdbmsType == RdbmsTypeEnum.TERADATA)) {
			schema = predictSchemaAsDatbaseFromUrl(connectionUrl);
		}
		if (schema != null) {
			return schema;
		}

		String truncatedUrl = connectionUrl;
		if (rdbmsType != null) {
			truncatedUrl = connectionUrl.substring(rdbmsType.getUrlPrefix().length());
		}

		if (schema == null) {
			// try schema...
			try (ResultSet schemaRs = meta.getSchemas()) {
				while (schemaRs.next()) {
					String tableSchema = schemaRs.getString(1);
					if (truncatedUrl.contains(tableSchema)) {
						schema = tableSchema;
						break;
					}
				}
			} catch (SQLException e) {
				classLogger.error("Failed to read the database schemas from metadata while inferring the schema "
						+ "from the URL", e);
			}
		}

		// try catalog...
		if (schema == null) {
			try (ResultSet catalogRs = meta.getCatalogs()) {
				while (catalogRs.next()) {
					String tableSchema = catalogRs.getString(1);
					if (truncatedUrl.contains(tableSchema)) {
						schema = tableSchema;
						break;
					}
				}
			} catch (SQLException e) {
				classLogger.error("Failed to read the database catalogs from metadata while inferring the schema "
						+ "from the URL", e);
			}
		}

		return schema;
	}

	/**
	 * Get tables result set for the given connection, metadata parser, and catalog
	 * / schema filters. Must return a result set containing table_name and
	 * table_type (with values 'TABLE' or 'VIEW'). The caller owns the result set,
	 * and the statement Oracle's listing runs on, and closes both, as with
	 * try-with-resources.
	 * 
	 * @param con
	 * @param meta
	 * @param catalogFilter
	 * @param schemaFilter
	 * @param driver
	 * @return
	 * @throws SQLException
	 */
	public static ResultSet getTables(Connection con, Statement stmt, DatabaseMetaData meta, String catalogFilter,
			String schemaFilter, RdbmsTypeEnum driver) throws SQLException {
		ResultSet tablesRs;
		if (driver == RdbmsTypeEnum.ORACLE) {
			String userName = meta.getUserName();
			String query = """
					SELECT TABLE_NAME AS "table_name", 'TABLE' AS "table_type", '%s' AS "table_schem"
					FROM ALL_TABLES
					WHERE OWNER NOT IN ('SYS', 'SYSTEM', 'WMSYS', 'XDB', 'CTXSYS', 'LBASYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'LBACSYS', 'GSMADMIN_INTERNAL', 'ORDDATA')
					   OR TABLESPACE_NAME NOT IN ('SYSAUX', 'SYSTEM')
					UNION
					SELECT VIEW_NAME AS "table_name", 'VIEW' AS "table_type", '%s' AS "table_schem"
					FROM ALL_VIEWS
					WHERE OWNER NOT IN ('SYS', 'SYSTEM', 'WMSYS', 'XDB', 'CTXSYS', 'LBASYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'LBACSYS', 'GSMADMIN_INTERNAL', 'ORDDATA')
					"""
					.formatted(userName, userName);
			tablesRs = stmt.executeQuery(query);
		} else if (driver == RdbmsTypeEnum.ATHENA || driver == RdbmsTypeEnum.REDSHIFT) {
			tablesRs = meta.getTables(catalogFilter, schemaFilter, null,
					new String[] { "TABLE", "EXTERNAL TABLE", "EXTERNAL_TABLE", "VIEW" });
		} else if (driver == RdbmsTypeEnum.MYSQL) {
			// these take the schema as a proper regex search
			tablesRs = meta.getTables(catalogFilter, "^" + schemaFilter + "$", null, new String[] { "TABLE", "VIEW" });
		} else if (driver == RdbmsTypeEnum.CASSANDRA) {
			if (catalogFilter.isEmpty()) {
				tablesRs = meta.getTables("cassandra", schemaFilter, null, new String[] { "TABLE", "VIEW" });
			} else {
				tablesRs = meta.getTables(catalogFilter, schemaFilter, null, new String[] { "TABLE", "VIEW" });
			}
		} else if (driver == RdbmsTypeEnum.POSTGRES) {
			tablesRs = meta.getTables(catalogFilter, schemaFilter, null,
					new String[] { "TABLE", "VIEW", "MATERIALIZED VIEW" });
		} else if (driver == RdbmsTypeEnum.H2_DB) {
			if (schemaFilter == null || schemaFilter.isBlank()) {
				schemaFilter = "PUBLIC";
			}
			tablesRs = meta.getTables(catalogFilter, schemaFilter, null, new String[] { "TABLE", "VIEW" });
		} else {
			// these do not take in the schema as a proper regex search
			tablesRs = meta.getTables(catalogFilter, schemaFilter, null, new String[] { "TABLE", "VIEW" });
		}
		return tablesRs;
	}

	/**
	 * Get the keys to grab from the result set from calling {@link #getTables}
	 * First key is table name, second key is table type, the third is the table
	 * schema
	 * 
	 * @param driver
	 * @return
	 */
	public static String[] getTableKeys(RdbmsTypeEnum driver) {
		String[] arr = new String[4];
		if (driver == RdbmsTypeEnum.SNOWFLAKE || driver == RdbmsTypeEnum.CLICKHOUSE || driver == RdbmsTypeEnum.ATHENA
				|| driver == RdbmsTypeEnum.CASSANDRA || driver == RdbmsTypeEnum.OPEN_SEARCH) {
			arr[0] = "TABLE_NAME";
			arr[1] = "TABLE_TYPE";
			arr[2] = "TABLE_SCHEM";
			arr[3] = "TABLE_CAT";
		} else {
			arr[0] = "table_name";
			arr[1] = "table_type";
			arr[2] = "table_schem";
			arr[3] = "table_cat";
		}
		return arr;
	}

	/**
	 * Get columns result set for the given metadata parser, table or view name, and
	 * catalog / schema filters. Must return a result set containing column_name and
	 * type_name. A null table or view name reads the columns of every table and
	 * view the filters cover. The name is a pattern, so the result can also hold
	 * the columns of a table whose name only resembles it; {@link #readColumns}
	 * drops those. The caller owns the result set and closes it, as with
	 * try-with-resources.
	 * 
	 * @param meta
	 * @param tableOrView
	 * @param catalogFilter
	 * @param schemaFilter
	 * @param driver
	 * @return
	 * @throws SQLException
	 */
	public static ResultSet getColumns(DatabaseMetaData meta, String tableOrView, String catalogFilter,
			String schemaFilter, RdbmsTypeEnum driver) throws SQLException {
		ResultSet columnsRs;
		if (driver == RdbmsTypeEnum.ORACLE) { // || driver == RdbmsTypeEnum.SQL_SERVER) {
			// do not pass in schema
			columnsRs = meta.getColumns(catalogFilter, null, tableOrView, null);
		} else if (driver == RdbmsTypeEnum.SNOWFLAKE) {
			if (schemaFilter != null) {
				schemaFilter = schemaFilter.replace("_", "\\_");
			}
			if (tableOrView != null) {
				tableOrView = tableOrView.replace("_", "\\_");
			}
			columnsRs = meta.getColumns(catalogFilter, schemaFilter, tableOrView, null);
		} else if (driver == RdbmsTypeEnum.CASSANDRA) {
			if (catalogFilter.isEmpty()) {
				catalogFilter = "cassandra";
			}
			columnsRs = meta.getColumns(catalogFilter, schemaFilter, tableOrView, null);
		} else {
			columnsRs = meta.getColumns(catalogFilter, schemaFilter, tableOrView, null);
		}
		return columnsRs;
	}

	/**
	 * Get the keys to grab from the result set from calling {@link #getColumns}
	 * First key is column name, second key is column type
	 * 
	 * @param driver
	 * @return
	 */
	public static String[] getColumnKeys(RdbmsTypeEnum driver) {
		String[] arr = new String[2];
		if (driver == RdbmsTypeEnum.SNOWFLAKE || driver == RdbmsTypeEnum.CLICKHOUSE || driver == RdbmsTypeEnum.CASSANDRA
				|| driver == RdbmsTypeEnum.OPEN_SEARCH) {
			arr[0] = "COLUMN_NAME";
			arr[1] = "TYPE_NAME";
		} else {
			arr[0] = "column_name";
			arr[1] = "type_name";
		}
		return arr;
	}

	/**
	 * Reads the columns of the given tables and views, each in its column order.
	 *
	 * <p>
	 * Every {@link DatabaseMetaData#getColumns} call is a round trip to the
	 * database, and on a remote database, such as Snowflake, Oracle or PostgreSQL,
	 * a metadata call can take from tens of milliseconds to seconds. A call per
	 * table makes a schema of 500 tables cost 500 round trips before anything else
	 * is read. So when many tables are asked for, and they are a good part of the
	 * schema, their columns are read in one call for the whole schema, with no
	 * table name, and only the rows of the tables asked for are kept. JDBC returns
	 * those rows ordered by table and column position, so each table keeps its
	 * column order.
	 * </p>
	 *
	 * <p>
	 * Otherwise each table is read on its own, since reading every column of a
	 * large schema to get a few of its tables is slower: see
	 * {@link #MIN_TABLES_FOR_SCHEMA_READ} and {@link #MIN_SHARE_FOR_SCHEMA_READ}.
	 * Oracle is always read a table at a time: {@link #getTables} lists its tables
	 * from every owner and {@link #getColumns} does not limit its call to a schema,
	 * so one call would read every column in the database. When the single call
	 * fails, the tables are read one at a time instead. The calls run one after
	 * another on the given connection.
	 * </p>
	 *
	 * <p>
	 * The table name in {@code getColumns} is a pattern, where {@code _} matches
	 * any character, so a call for ORDER_ITEMS can also return the columns of
	 * ORDERXITEMS. Either way, only the rows of the table asked for are kept.
	 * </p>
	 *
	 * @param meta             the connection's metadata
	 * @param tables           the tables and views to read, each as its name, type
	 *                         and schema, as {@link #getTables} lists them
	 * @param schemaTableCount how many tables and views the schema's listing held,
	 *                         before any filter
	 * @param catalogFilter    the catalog the tables are in, or null
	 * @param schemaFilter     the schema the tables are in, or null
	 * @param driver           the database type, for its metadata quirks
	 * @return each table's columns in order, by table name; a table whose columns
	 *         could not be read is left out
	 */
	public static Map<String, List<ColumnMetadata>> readColumns(DatabaseMetaData meta, List<String[]> tables,
			int schemaTableCount, String catalogFilter, String schemaFilter, RdbmsTypeEnum driver) {
		if (readsWholeSchema(tables.size(), schemaTableCount, driver)) {
			try {
				return readColumnsOfSchema(meta, tables, catalogFilter, schemaFilter, driver);
			} catch (SQLException e) {
				classLogger.warn(
						"Failed to read the columns of {} tables in one call, so reading them a table at a time",
						tables.size(), e);
			}
		}

		Map<String, List<ColumnMetadata>> columns = new LinkedHashMap<>();
		for (String[] table : tables) {
			try {
				columns.put(table[0], readColumnsOfTable(meta, table[0], catalogFilter, schemaFilter, driver));
			} catch (SQLException e) {
				classLogger.error("Failed to retrieve columns for {}", Utility.cleanLogString(table[0]), e);
			}
		}
		return columns;
	}

	/**
	 * Whether {@link #readColumns} reads the columns in one call for the whole
	 * schema: for at least {@link #MIN_TABLES_FOR_SCHEMA_READ} tables that are at
	 * least {@link #MIN_SHARE_FOR_SCHEMA_READ} of the schema, and never for Oracle.
	 *
	 * @param tableCount       how many tables are asked for
	 * @param schemaTableCount how many tables and views the schema holds
	 * @param driver           the database type
	 * @return true for one call for the whole schema
	 */
	static boolean readsWholeSchema(int tableCount, int schemaTableCount, RdbmsTypeEnum driver) {
		return driver != RdbmsTypeEnum.ORACLE && tableCount >= MIN_TABLES_FOR_SCHEMA_READ
				&& tableCount >= MIN_SHARE_FOR_SCHEMA_READ * schemaTableCount;
	}

	/**
	 * Reads the columns of the given tables in one call for the whole schema,
	 * keeping only the rows of those tables. A row of a table with the same name in
	 * another schema is dropped when both schemas are known.
	 *
	 * @return each table's columns in order, by table name, every table included
	 * @throws SQLException when the call fails, or the driver does not report each
	 *                      row's table
	 */
	private static Map<String, List<ColumnMetadata>> readColumnsOfSchema(DatabaseMetaData meta, List<String[]> tables,
			String catalogFilter, String schemaFilter, RdbmsTypeEnum driver) throws SQLException {
		Map<String, List<ColumnMetadata>> columns = new LinkedHashMap<>();
		Map<String, String> listedSchemas = new HashMap<>();
		for (String[] table : tables) {
			columns.put(table[0], new ArrayList<>());
			listedSchemas.put(table[0], table.length > 2 ? table[2] : null);
		}
		String[] columnKeys = getColumnKeys(driver);
		try (ResultSet columnsRs = getColumns(meta, null, catalogFilter, schemaFilter, driver)) {
			while (columnsRs.next()) {
				String tableName = columnsRs.getString("TABLE_NAME");
				List<ColumnMetadata> tableColumns = columns.get(tableName);
				if (tableColumns == null) {
					// a table that was not asked for
					continue;
				}
				String listedSchema = listedSchemas.get(tableName);
				String rowSchema = stringOrNull(columnsRs, "TABLE_SCHEM");
				if (listedSchema != null && rowSchema != null && !listedSchema.equals(rowSchema)) {
					continue;
				}
				tableColumns.add(
						new ColumnMetadata(columnsRs.getString(columnKeys[0]), columnsRs.getString(columnKeys[1])));
			}
		}
		return columns;
	}

	/**
	 * Reads the columns of one table, keeping only the rows of that table, since
	 * its name is a pattern that can match others.
	 *
	 * @return the table's columns in order
	 * @throws SQLException when the call fails
	 */
	private static List<ColumnMetadata> readColumnsOfTable(DatabaseMetaData meta, String tableName,
			String catalogFilter, String schemaFilter, RdbmsTypeEnum driver) throws SQLException {
		List<ColumnMetadata> columns = new ArrayList<>();
		String[] columnKeys = getColumnKeys(driver);
		try (ResultSet columnsRs = getColumns(meta, tableName, catalogFilter, schemaFilter, driver)) {
			while (columnsRs.next()) {
				// a row whose table the driver does not report is kept
				String rowTable = stringOrNull(columnsRs, "TABLE_NAME");
				if (rowTable != null && !rowTable.equalsIgnoreCase(tableName)) {
					continue;
				}
				columns.add(new ColumnMetadata(columnsRs.getString(columnKeys[0]), columnsRs.getString(columnKeys[1])));
			}
		}
		return columns;
	}

	/**
	 * A column of the current row, or null when the driver does not report it.
	 */
	private static String stringOrNull(ResultSet resultSet, String columnLabel) {
		try {
			return resultSet.getString(columnLabel);
		} catch (SQLException e) {
			return null;
		}
	}

	private static String predictSchemaFromUrl(String url) {
		String schema = null;
		if (url == null) {
			return schema;
		}

		if (url.contains("?currentSchema=")) {
			Pattern p = Pattern.compile("currentSchema=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if (m.find()) {
				schema = m.group(0);
				schema = schema.replace("currentSchema=", "");
				return schema;
			}
		}

		if (url.contains(";currentSchema=")) {
			Pattern p = Pattern.compile("currentSchema=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if (m.find()) {
				schema = m.group(0);
				schema = schema.replace("currentSchema=", "");
				return schema;
			}
		}

		if (url.contains("&currentSchema=")) {
			Pattern p = Pattern.compile("currentSchema=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if (m.find()) {
				schema = m.group(0);
				schema = schema.replace("currentSchema=", "");
				return schema;
			}
		}

		if (url.contains("?schema=")) {
			Pattern p = Pattern.compile("schema=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if (m.find()) {
				schema = m.group(0);
				schema = schema.replace("schema=", "");
				return schema;
			}
		}

		if (url.contains(";schema=")) {
			Pattern p = Pattern.compile("schema=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if (m.find()) {
				schema = m.group(0);
				schema = schema.replace("schema=", "");
				return schema;
			}
		}

		if (url.contains("&schema=")) {
			Pattern p = Pattern.compile("schema=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if (m.find()) {
				schema = m.group(0);
				schema = schema.replace("schema=", "");
				return schema;
			}
		}

		return schema;
	}

	private static String predictSchemaAsDatbaseFromUrl(String url) {
		String schema = null;

		if (url.contains("/DATABASE=")) {
			Pattern p = Pattern.compile("DATABASE=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if (m.find()) {
				schema = m.group(0);
				schema = schema.replace("DATABASE=", "");
				return schema;
			}
		}

		return schema;
	}

}
