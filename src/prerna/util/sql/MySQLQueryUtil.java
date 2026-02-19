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

import java.util.Collection;

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
	public boolean supportsTablePartitioning() {
		return true;
	}

	@Override
	public String createPartitionedTable(String tableName, String[] colNames, String[] types, String partitionColumn,
			String partitionInterval) {
		// should escape keywords
		tableName = cleanTableName(tableName);
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}

		String columnName = colNames[0];
		if (isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}

		StringBuilder retString = new StringBuilder("CREATE TABLE ").append(tableName).append(" (").append(columnName)
				.append(" ").append(types[0]);
		for (int colIndex = 1; colIndex < colNames.length; colIndex++) {
			columnName = colNames[colIndex];
			if (isSelectorKeyword(columnName)) {
				columnName = getEscapeKeyword(columnName);
			}
			retString.append(" , ").append(columnName).append("  ").append(types[colIndex]);
		}

		// Add partition definition with default partition
		retString.append(") PARTITION BY RANGE (TO_DAYS(").append(partitionColumn).append(")) (");
		retString.append("\tPARTITION p_default VALUES LESS THAN (MAXVALUE)\n");
		retString.append(");");

		return retString.toString();
	}

	@Override
	public String createPartitionedTableIfNotExists(String tableName, String[] colNames, String[] types,
			String partitionColumn, String partitionInterval) {
		// MySQL doesn't support IF NOT EXISTS for partitioned tables directly
		return createPartitionedTable(tableName, colNames, types, partitionColumn, partitionInterval);
	}

	@Override
	public String addTablePartition(String tableName, String partitionName, String partitionValue) {
		// For monthly partitioning - partitionValue should be in YYYYMM format
		int partitionId = Integer.parseInt(partitionValue);
		int nextPartitionId = partitionId + 1;

		return "ALTER TABLE " + tableName + " ADD PARTITION (PARTITION " + partitionName + " VALUES LESS THAN ("
				+ nextPartitionId + "));";
	}

	/**
	 * Ensure monthly partitions exist for next N months
	 * 
	 * @param months Number of months to ensure partitions for
	 * @return List of SQL statements to add missing partitions
	 */
	public java.util.List<String> ensureMonthlyPartitions(int months) {
		java.util.List<String> statements = new java.util.ArrayList<>();
		java.time.LocalDate start = java.time.LocalDate.now().withDayOfMonth(1);
		java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd");

		for (int i = 0; i < months; i++) {
			java.time.LocalDate e = start.plusMonths(i + 1);
			String partName = String.format("p%04d%02d", e.getYear(), e.getMonthValue());
			statements.add(String.format(
					"ALTER TABLE AUDIT_LOGS ADD PARTITION (PARTITION %s VALUES LESS THAN (TO_DAYS('%s')))", partName,
					e.format(fmt)));
		}

		return statements;
	}

	/**
	 * Create comprehensive partitioned AUDIT_LOGS table with default partition and
	 * monthly partitions
	 * 
	 * @param auditLogColDefs Column definitions string
	 * @param start           Start date for partition creation
	 * @param months          Number of months to create partitions for
	 * @return List of SQL statements to execute
	 */
	public java.util.List<String> createComprehensiveMySqlPartitions(String auditLogColDefs, java.time.LocalDate start,
			int months) {
		java.util.List<String> statements = new java.util.ArrayList<>();

		// Create partitioned table with default partition
		statements.add("CREATE TABLE IF NOT EXISTS AUDIT_LOGS (" + auditLogColDefs
				+ ") PARTITION BY RANGE (TO_DAYS(LOG_TIMESTAMP)) (PARTITION p_default VALUES LESS THAN (MAXVALUE))");

		// Add monthly partitions
		java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd");
		for (int i = 0; i < months; i++) {
			java.time.LocalDate e = start.plusMonths(i + 1);
			String partName = String.format("p%04d%02d", e.getYear(), e.getMonthValue());
			statements.add(String.format(
					"ALTER TABLE AUDIT_LOGS ADD PARTITION (PARTITION %s VALUES LESS THAN (TO_DAYS('%s')))", partName,
					e.format(fmt)));
		}

		return statements;
	}

	/**
	 * Convert existing non-partitioned table to partitioned table using ALTER TABLE
	 * PARTITION BY
	 * 
	 * @param auditLogColDefs Column definitions (not used in MySQL conversion)
	 * @param months          Number of months for partitions
	 * @return SQL statement for in-place conversion
	 */
	public String convertToPartitionedTable(String auditLogColDefs, int months) {
		java.time.LocalDate startMon = java.time.LocalDate.now().withDayOfMonth(1);
		java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd");

		StringBuilder partDefs = new StringBuilder();
		for (int i = 0; i < months; i++) {
			java.time.LocalDate e = startMon.plusMonths(i + 1);
			String partName = String.format("p%04d%02d", e.getYear(), e.getMonthValue());
			partDefs.append(String.format("PARTITION %s VALUES LESS THAN (TO_DAYS('%s'))", partName, e.format(fmt)));
			if (i < months - 1) {
				partDefs.append(", ");
			}
		}

		if (partDefs.length() > 0) {
			partDefs.append(", PARTITION p_max VALUES LESS THAN (MAXVALUE)");
		}

		return "ALTER TABLE AUDIT_LOGS PARTITION BY RANGE (TO_DAYS(LOG_TIMESTAMP)) (" + partDefs.toString() + ")";
	}

	@Override
	public String dropTablePartition(String tableName, String partitionName) {
		return "ALTER TABLE " + tableName + " DROP PARTITION " + partitionName + ";";
	}
}
