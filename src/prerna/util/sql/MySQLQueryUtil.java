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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MySQLQueryUtil extends AnsiSqlQueryUtil {

	MySQLQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.MYSQL);
	}

	MySQLQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.MYSQL);
	}

	@Override
	public String getEscapeKeyword(String selector) {
		return "`" + selector + "`";
	}

	/////////////////////////////////////////////////////////////////////////////////////

	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
		return false;
	}

	@Override
	public boolean allowIfExistsIndexSyntax() {
		return false;
	}

	/////////////////////////////////////////////////////////////////////////////////////

	@Override
	public String modColumnType(String tableName, String columnName, String dataType) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if (isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " MODIFY COLUMN " + columnName + " " + dataType + ";";
	}

	@Override
	public String dropIndex(String indexName, String tableName) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		return "ALTER TABLE " + tableName + " DROP INDEX " + indexName;
	}

	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query database scripts
	 */

	@Override
	public String tableExistsQuery(String tableName, String database, String schema) {
		return "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='" + schema
				+ "' AND TABLE_NAME='" + tableName + "';";
	}

	@Override
	public String tableConstraintExistsQuery(String constraintName, String tableName, String database, String schema) {
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = '"
				+ constraintName + "' AND TABLE_NAME = '" + tableName + "' AND CONSTRAINT_SCHEMA='" + schema + "'";
	}

	@Override
	public String referentialConstraintExistsQuery(String constraintName, String database, String schema) {
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_NAME = '"
				+ constraintName + "' AND CONSTRAINT_SCHEMA='" + schema + "'";
	}

	@Override
	public String getAllColumnDetails(String tableName, String database, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='"
				+ schema + "' AND TABLE_NAME = '" + tableName + "';";
	}

	@Override
	public String columnDetailsQuery(String tableName, String columnName, String database, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='"
				+ schema + "' AND TABLE_NAME = '" + tableName + "' AND COLUMN_NAME='" + columnName + "';";
	}

	@Override
	public String getIndexList(String database, String schema) {
		return "SELECT DISTINCT INDEX_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='" + schema
				+ "';";
	}

	@Override
	public String getIndexDetails(String indexName, String tableName, String database, String schema) {
		return "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='" + schema
				+ "' AND INDEX_NAME='" + indexName + "' AND TABLE_NAME='" + tableName + "';";
	}

	@Override
	public String allIndexForTableQuery(String tableName, String database, String schema) {
		return "SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='" + schema
				+ "' AND TABLE_NAME='" + tableName + "';";
	}

	@Override
	public String alterTableDropColumns(String tableName, Collection<String> columnNames) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}

		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " DROP COLUMN ");
		int i = 0;
		for (String newColumn : columnNames) {
			if (i > 0) {
				alterString.append(", DROP COLUMN ");
			}

			// should escape keywords
			if (isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}

			alterString.append(newColumn);

			i++;
		}
		alterString.append(";");
		return alterString.toString();
	}

	@Override
	public boolean supportsPartitioning() {
		return true;
	}

	@Override
	public boolean isTablePartitioned(Connection conn, String tableName) throws SQLException {
		// Check INFORMATION_SCHEMA.PARTITIONS for any partition rows for this table.
		String schema = (this.database != null && !this.database.isEmpty()) ? this.database : conn.getCatalog();
		String sql = "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, schema);
			ps.setString(2, tableName);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getInt("cnt") > 0;
				}
			}
		}
		return false;
	}

	@Override
	public List<String> getCreatePartitionedTableSql(String tableName, String partitionColumn, String columnDefinitions,
			PartitionFrequency freq, int ahead) {
		List<String> sqls = new ArrayList<>();

		// base CREATE TABLE statement
		String createBase = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + columnDefinitions + ")";

		// Partition clause
		LocalDate start = LocalDate.now().withDayOfMonth(1);
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");

		StringBuilder partitionSb = new StringBuilder(" PARTITION BY RANGE COLUMNS (" + partitionColumn + ") (");

		// create partitions for 'ahead' months
		for (int i = 0; i < ahead; i++) {
			LocalDate boundary = start.plusMonths(i + 1); // partition upper bound is next month's 1st
			String pname = tableName + "_"
					+ String.format("%04d_%02d", start.plusMonths(i).getYear(), start.plusMonths(i).getMonthValue());
			partitionSb.append("PARTITION `").append(pname).append("` VALUES LESS THAN ('").append(boundary.format(fmt))
					.append("')");
			if (i < ahead - 1) {
				partitionSb.append(", ");
			}
		}
		// Add a default max partition
		partitionSb.append(", PARTITION `").append(tableName).append("_max` VALUES LESS THAN (MAXVALUE))");

		String createStatement = createBase + partitionSb.toString();
		sqls.add(createStatement);

		return sqls;
	}

	/**
	 * Ensure future partitions for MySQL by executing ALTER TABLE
	 */
	@Override
	public void getEnsureFuturePartitionsSql(Connection conn, String tableName, String partitionColumn,
			PartitionFrequency freq, int ahead) {
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		LocalDate start = LocalDate.now().withDayOfMonth(1);

		// determine existing partitions
		String schema = null;
		try {
			schema = (this.database != null && !this.database.isEmpty()) ? this.database : conn.getCatalog();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		String listSql = "SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL";
		Set<String> existing = new HashSet<>();
		try (PreparedStatement ps = conn.prepareStatement(listSql)) {
			ps.setString(1, schema);
			ps.setString(2, tableName);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					String p = rs.getString("PARTITION_NAME");
					if (p != null) {
						existing.add(p.toLowerCase());
					}
				}
			}
		} catch (SQLException e) {
			return;
		}

		// For each needed month, if missing, run ALTER TABLE ADD PARTITION
		for (int i = 0; i < ahead; i++) {
			LocalDate monthStart = start.plusMonths(i);
			String partName = tableName + "_"
					+ String.format("%04d_%02d", monthStart.getYear(), monthStart.getMonthValue());
			if (existing.contains(partName.toLowerCase())) {
				continue;
			}

			LocalDate boundary = monthStart.plusMonths(1);
			String alterSql = String.format("ALTER TABLE `%s` ADD PARTITION (PARTITION `%s` VALUES LESS THAN ('%s'))",
					tableName, partName, boundary.format(fmt));
			try (Statement st = conn.createStatement()) {
				st.execute(alterSql);
			} catch (SQLException e) {
				// Non-fatal;
			}
		}
	}

	@Override
	public List<String> getConvertTableToPartitionedSql(Connection conn, String tableName, String partitionColumn,
			String columnDefinitions, PartitionFrequency freq, int ahead) throws SQLException {
		return new ArrayList<>(); // empty -> PartitionManager will skip conversion
	}
}
