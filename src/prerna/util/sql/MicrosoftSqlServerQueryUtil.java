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
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.MicrosoftSqlServerInterpreter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MicrosoftSqlServerQueryUtil extends AnsiSqlQueryUtil {

	MicrosoftSqlServerQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.SQL_SERVER);
	}

	MicrosoftSqlServerQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.SQL_SERVER);
	}

	@Override
	public void initTypeConverstionMap() {
		super.initTypeConverstionMap();
		typeConversionMap.put("TIMESTAMP", "DATETIME");
		typeConversionMap.put("BOOLEAN", "BIT");
		typeConversionMap.put("DOUBLE", "DECIMAL(20,4)");
	}

	@Override
	public IQueryInterpreter getInterpreter(IDatabaseEngine engine) {
		return new MicrosoftSqlServerInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new MicrosoftSqlServerInterpreter(frame);
	}

	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if (configMap == null || configMap.isEmpty()) {
			throw new RuntimeException("Configuration map is null or empty");
		}

		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);
		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		this.database = (String) configMap.get(AbstractSqlQueryUtil.DATABASE);
		this.schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);
		this.username = (String) configMap.get(AbstractSqlQueryUtil.USERNAME);
		this.password = (String) configMap.get(AbstractSqlQueryUtil.PASSWORD);

		return buildConnectionString();
	}

	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if (prop == null || prop.isEmpty()) {
			throw new RuntimeException("Properties object is null or empty");
		}

		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		this.database = (String) prop.get(AbstractSqlQueryUtil.DATABASE);
		this.schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);
		this.username = (String) prop.get(AbstractSqlQueryUtil.USERNAME);
		this.password = (String) prop.get(AbstractSqlQueryUtil.PASSWORD);

		return buildConnectionString();
	}

	@Override
	public String buildConnectionString() {
		if (this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			return this.connectionUrl;
		}

		if (this.hostname == null || this.hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}

		if (this.database == null || this.database.isEmpty()) {
			throw new RuntimeException("Must pass in database name");
		}

		String port = getPort();
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}

		this.connectionUrl = this.dbType.getUrlPrefix() + "://" + this.hostname + port + ";databaseName="
				+ this.database;

		if (this.additionalProps != null && !this.additionalProps.isEmpty()) {
			if (!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
				this.connectionUrl += ";" + this.additionalProps;
			} else {
				this.connectionUrl += this.additionalProps;
			}
		}

		return this.connectionUrl;
	}

	@Override
	public StringBuilder getFirstRow(StringBuilder query) {
		String strquery = query.toString();
		strquery = strquery.replaceFirst("(?i)SELECT", "SELECT TOP 1");
		query = new StringBuilder();
		query.append(strquery);
		return query;
	}

	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {
		if (offset > 0 && limit > 0) {
			query = query.append(" OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY");
		} else if (offset > 0) {
			query = query.append(" OFFSET " + offset + " ROWS ");
		} else if (limit > 0) {
			query = query.append(" OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY");
		}

		return query;
	}

	@Override
	public StringBuffer addLimitOffsetToQuery(StringBuffer query, long limit, long offset) {
		if (offset > 0 && limit > 0) {
			query = query.append(" OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY");
		} else if (offset > 0) {
			query = query.append(" OFFSET " + offset + " ROWS ");
		} else if (limit > 0) {
			query = query.append(" OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY");
		}

		return query;
	}

	@Override
	public String removeDuplicatesFromTable(String tableName, String fullColumnNameList) {
		return "SELECT DISTINCT " + fullColumnNameList + " INTO " + tableName + "_TEMP " + " FROM " + tableName
				+ " WHERE " + tableName + " IS NOT NULL AND LTRIM(RTRIM(" + tableName + ")) <> ''";
	}

	/////////////////////////////////////////////////////////////////////////////////////

	@Override
	public String getGroupConcatFunctionSyntax() {
		return "STRING_AGG";
	}

	@Override
	public String processGroupByFunction(String selectExpression, String separator, boolean distinct) {
//		if(distinct) {
//			return getSqlFunctionSyntax(QueryFunctionHelper.GROUP_CONCAT) + "(DISTINCT " + selectExpression + ", '" + separator + "')";
//		} else {
		return getSqlFunctionSyntax(QueryFunctionHelper.GROUP_CONCAT) + "(" + selectExpression + ", '" + separator
				+ "')";
//		}
	}

	@Override
	public boolean allowBooleanDataType() {
		return false;
	}

	@Override
	public String getDateWithTimeDataType() {
		return "DATETIME";
	}

	@Override
	public String getCurrentDate() {
		return "GETDATE()";
	}

	@Override
	public String getCurrentTimestamp() {
		return "CURRENT_TIMESTAMP";
	}

	@Override
	public String getDoubleDataTypeName() {
		return "FLOAT";
	}

	@Override
	public boolean allowBlobDataType() {
		return false;
	}

	@Override
	public String getBlobDataTypeName() {
		return "VARBINARY(MAX)";
	}

	@Override
	public String getClobDataTypeName() {
		return "VARCHAR(MAX)";
	}

	@Override
	public String getBooleanDataTypeName() {
		return "BIT";
	}

	@Override
	public String getRegexLikeFunctionSyntax() {
		return "PATINDEX";
	}

	@Override
	public IQueryFilter getSearchRegexFilter(String columnQs, String searchTerm) {
		// WHERE PATINDEX ('%pattern%',expression) != 0
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction("PATINDEX");
		fun.addInnerSelector(new QueryConstantSelector("%" + searchTerm + "%"));
		fun.addInnerSelector(new QueryColumnSelector(columnQs));
		NounMetadata lComparison = new NounMetadata(fun, PixelDataType.COLUMN);
		NounMetadata rComparison = new NounMetadata(0, PixelDataType.CONST_INT);
		SimpleQueryFilter filter = new SimpleQueryFilter(lComparison, "!=", rComparison);
		return filter;
	}

	@Override
	public String buildDateDiffFunctionSyntax(String timeUnit, String dateTimeField1, String dateTimeField2) {
		return "DATEDIFF(" + timeUnit.toLowerCase() + "," + dateTimeField1 + "," + dateTimeField2 + ")";
	}

	@Override
	public boolean allowsIfExistsTableSyntax() {
		return false;
	}

	@Override
	public boolean allowIfExistsAddConstraint() {
		return false;
	}

	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
		return false;
	}

	@Override
	public boolean allowIfExistsIndexSyntax() {
		return false;
	}

	@Override
	public boolean savePointAutoRelease() {
		// do not call release savepoint method - will throw error/exception
		return true;
	}

	@Override
	public String tableExistsQuery(String tableName, String database, String schema) {
		return "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='" + database
				+ "' AND TABLE_SCHEMA='" + schema + "' AND TABLE_NAME='" + tableName + "'";
	}

	@Override
	public String tableConstraintExistsQuery(String constraintName, String tableName, String database, String schema) {
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = '"
				+ constraintName + "' AND TABLE_NAME = '" + tableName + "' AND TABLE_CATALOG='" + database
				+ "' AND TABLE_SCHEMA='" + schema + "'";
	}

	@Override
	public String referentialConstraintExistsQuery(String constraintName, String database, String schema) {
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_NAME = '"
				+ constraintName + "' AND CONSTRAINT_CATALOG='" + database + "' AND CONSTRAINT_SCHEMA='" + schema + "'";
	}

	@Override
	public String getAllColumnDetails(String tableName, String database, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG='"
				+ database + "' AND TABLE_SCHEMA='" + schema + "' AND TABLE_NAME='" + tableName + "'";
	}

	@Override
	public String columnDetailsQuery(String tableName, String columnName, String database, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG='"
				+ database + "' AND TABLE_SCHEMA='" + schema + "' AND TABLE_NAME='" + tableName + "' AND COLUMN_NAME='"
				+ columnName.toUpperCase() + "'";
	}

	@Override
	public String getIndexDetails(String indexName, String tableName, String database, String schema) {
		return "SELECT ix.name as IndexName, tab.name as TableName, COL_NAME(ix.object_id, ixc.column_id) as ColumnName, "
				+ "ix.type_desc, ix.is_disabled FROM sys.indexes ix "
				+ "INNER JOIN sys.index_columns ixc ON  ix.object_id = ixc.object_id and ix.index_id = ixc.index_id "
				+ "INNER JOIN sys.tables tab ON ix.object_id = tab.object_id " + "WHERE "
				+ "ix.is_primary_key = 0 " /* Remove Primary Keys */
				+ "AND ix.is_unique = 0 " /* Remove Unique Keys */
				+ "AND ix.is_unique_constraint = 0 " /* Remove Unique Constraints */
				+ "AND tab.is_ms_shipped = 0" /* Remove SQL Server Default Tables */
				+ "AND ix.name='" + indexName + "' " + "AND tab.name='" + tableName + "'";
	}

	@Override
	public String alterTableName(String tableName, String newTableName) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if (isSelectorKeyword(newTableName)) {
			newTableName = getEscapeKeyword(newTableName);
		}
		return "sp_rename '" + tableName + "', '" + newTableName + "';";
	}

	@Override
	public String alterTableAddColumn(String tableName, String newColumn, String newColType) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if (isSelectorKeyword(newColumn)) {
			newColumn = getEscapeKeyword(newColumn);
		}
		return "ALTER TABLE " + tableName + " ADD " + newColumn + " " + newColType + ";";
	}

	@Override
	public String alterTableAddColumnWithDefault(String tableName, String newColumn, String newColType,
			Object defualtValue) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if (isSelectorKeyword(newColumn)) {
			newColumn = getEscapeKeyword(newColumn);
		}
		return "ALTER TABLE " + tableName + " ADD " + newColumn + " " + newColType + " DEFAULT '" + defualtValue + "';";
	}

	@Override
	public String alterTableAddColumns(String tableName, String[] newColumns, String[] newColTypes) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}

		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " ADD ");
		for (int i = 0; i < newColumns.length; i++) {
			if (i > 0) {
				alterString.append(", ");
			}

			String newColumn = newColumns[i];
			// should escape keywords
			if (isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}

			alterString.append(newColumn + "  " + newColTypes[i]);
		}
		alterString.append(";");
		return alterString.toString();
	}

	@Override
	public String alterTableAddColumns(String tableName, Map<String, String> newColToTypeMap) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}

		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " ADD ");
		int i = 0;
		for (String newColumn : newColToTypeMap.keySet()) {
			String newColType = newColToTypeMap.get(newColumn);
			if (i > 0) {
				alterString.append(", ");
			}

			// should escape keywords
			if (isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}

			alterString.append(newColumn + "  " + newColType);

			i++;
		}
		alterString.append(";");
		return alterString.toString();
	}

	@Override
	public String alterTableAddColumnsWithDefaults(String tableName, String[] newColumns, String[] newColTypes,
			Object[] defaultValues) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}

		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " ADD ");
		for (int i = 0; i < newColumns.length; i++) {
			if (i > 0) {
				alterString.append(", ");
			}

			String newColumn = newColumns[i];
			// should escape keywords
			if (isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}

			alterString.append(newColumn + "  " + newColTypes[i]);

			// add default values
			if (defaultValues[i] != null) {
				alterString.append(" DEFAULT ");
				if (defaultValues[i] instanceof String) {
					alterString.append("'").append(defaultValues[i]).append("'");
				} else {
					alterString.append(defaultValues[i]);
				}
			}
		}
		alterString.append(";");
		return alterString.toString();
	}

	@Override
	public String modColumnNotNull(String tableName, String columnName, String dataType) {
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if (isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " " + dataType + " NOT NULL";
	}

	@Override
	public String modColumnName(String tableName, String curColName, String newColName) {
		return "sp_rename '" + tableName + "." + curColName + "', '" + newColName + "', 'COLUMN';";
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
				alterString.append(", ");
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
	public String dropIndex(String indexName, String tableName) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		return "DROP INDEX " + tableName + "." + indexName + ";";
	}

	@Override
	public String copyTable(String newTableName, String oldTableName) {
		if (isSelectorKeyword(newTableName)) {
			newTableName = getEscapeKeyword(newTableName);
		}
		if (isSelectorKeyword(oldTableName)) {
			oldTableName = getEscapeKeyword(oldTableName);
		}
		return "SELECT * INTO " + newTableName + " FROM " + oldTableName;
	}

	@Override
	public String insertIntoTable(String tableName, String[] columnNames, String[] types, Object[] values) {
		if (columnNames.length != types.length) {
			throw new UnsupportedOperationException("Headers and types must have the same length");
		}
		if (columnNames.length != values.length) {
			throw new UnsupportedOperationException("Headers and values must have the same length");
		}

		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}

		// only loop 1 time around both arrays since length must always match
		StringBuilder inserter = new StringBuilder("INSERT INTO " + tableName + " (");
		StringBuilder template = new StringBuilder();

		for (int colIndex = 0; colIndex < columnNames.length; colIndex++) {
			String columnName = columnNames[colIndex];
			String type = types[colIndex];
			Object value = values[colIndex];

			if (colIndex > 0) {
				inserter.append(", ");
				template.append(", ");
			}

			if (isSelectorKeyword(columnName)) {
				columnName = getEscapeKeyword(columnName);
			}

			// always jsut append the column name
			inserter.append(columnName);

			if (value == null) {
				// append null without quotes
				template.append("null");
				continue;
			}

			// we do not have a null
			// now we care how we insert based on the type of the value
			SemossDataType dataType = SemossDataType.convertStringToDataType(type);
			if (dataType == SemossDataType.INT || dataType == SemossDataType.DOUBLE) {
				// append as is
				template.append(value);
			} else if (dataType == SemossDataType.BOOLEAN || dataType == SemossDataType.STRING
					|| dataType == SemossDataType.FACTOR) {
				template.append("'").append(escapeForSQLStatement(value + "")).append("'");
			} else if (dataType == SemossDataType.DATE) {
				if (value instanceof SemossDate) {
					Date d = ((SemossDate) value).getDate();
					if (d == null) {
						template.append(null + "");
					} else {
						template.append("'").append(((SemossDate) value).getFormatted("yyyy-MM-dd")).append("'");
					}
				} else if (value instanceof java.sql.Date) {
					template.append("'").append(value.toString()).append("'");
				} else {
					SemossDate dateValue = SemossDate.genDateObj(value + "");
					if (dateValue == null) {
						template.append(null + "");
					} else {
						template.append("'").append(dateValue.getFormatted("yyyy-MM-dd")).append("'");
					}
				}
			} else if (dataType == SemossDataType.TIMESTAMP) {
				if (value instanceof SemossDate) {
					Date d = ((SemossDate) value).getDate();
					if (d == null) {
						template.append(null + "");
					} else {
						template.append("'").append(((SemossDate) value).getFormatted("yyyy-MM-dd HH:mm:ss"))
								.append("'");
					}
				} else if (value instanceof java.sql.Timestamp) {
					template.append("'").append(value.toString()).append("'");
				} else {
					SemossDate dateValue = SemossDate.genTimeStampDateObj(value + "");
					if (dateValue == null) {
						template.append(null + "");
					} else {
						template.append("'").append(dateValue.getFormatted("yyyy-MM-dd HH:mm:ss")).append("'");
					}
				}
			}
		}

		inserter.append(")  VALUES (").append(template).append(")");
		return inserter.toString();
	}

	@Override
	public String getDatabaseMetadataCatalogFilter() {
		return this.database;
	}

	@Override
	public String getDatabaseMetadataSchemaFilter() {
		return this.schema;
	}

	@Override
	public boolean supportsPartitioning() {
		// SQL Server supports partitioning via partition functions and schemes
		return true;
	}

	@Override
	public boolean isTablePartitioned(Connection conn, String tableName) throws SQLException {
		// Check entries in sys.partitions
		String sql = "SELECT COUNT(DISTINCT partition_id) AS cnt " + "FROM sys.partitions p "
				+ "JOIN sys.objects o ON p.object_id = o.object_id " + "WHERE o.name = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, tableName);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getInt("cnt") > 1;
				}
			}
		}
		return false;
	}

	@Override
	public List<String> getCreatePartitionedTableSql(String tableName, String partitionColumn, String columnDefinitions,
			PartitionFrequency freq, int ahead) {
		List<String> sqls = new ArrayList<>();
		// Names
		String pfName = "PF_" + tableName;
		String psName = "PS_" + tableName;
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");

		// Build initial boundaries (months ahead)
		LocalDate start = LocalDate.now().withDayOfMonth(1);
		StringBuilder boundaries = new StringBuilder();
		for (int i = 0; i < ahead; i++) {
			LocalDate boundary = start.plusMonths(i + 1);
			boundaries.append("'").append(boundary.format(fmt)).append("'");
			if (i < ahead - 1) {
				boundaries.append(", ");
			}
		}

		// 1) Create partition function (RANGE RIGHT)
		String createPf = String.format("CREATE PARTITION FUNCTION %s (DATETIME) AS RANGE RIGHT FOR VALUES (%s)",
				pfName, boundaries.toString());
		sqls.add(createPf);

		// 2) Create partition scheme (map to PRIMARY for all ranges)
		String createPs = String.format("CREATE PARTITION SCHEME %s AS PARTITION %s ALL TO ([PRIMARY])", psName,
				pfName);
		sqls.add(createPs);

		// 3) Create table on partition scheme
		String createTable = String.format("CREATE TABLE %s (%s) ON %s(%s)", tableName, columnDefinitions, psName,
				partitionColumn);
		sqls.add(createTable);

		return sqls;
	}

	@Override
	public void getEnsureFuturePartitionsSql(Connection conn, String tableName, String partitionColumn,
			PartitionFrequency freq, int ahead) {
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		String pfName = "PF_" + tableName;

		// Try to list existing boundaries for partition function
		String listBoundaries = "SELECT CONVERT(varchar(10), rv.value, 120) AS boundary "
				+ "FROM sys.partition_functions pf "
				+ "JOIN sys.partition_range_values rv ON pf.function_id = rv.function_id " + "WHERE pf.name = ? "
				+ "ORDER BY rv.boundary_id";
		Set<String> existing = new HashSet<>();
		try (PreparedStatement ps = conn.prepareStatement(listBoundaries)) {
			ps.setString(1, pfName);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					existing.add(rs.getString("boundary"));
				}
			}
		} catch (SQLException e) {
			// fallback will attempt to SPLIT and ignore duplicates
		}

		LocalDate start = LocalDate.now().withDayOfMonth(1);
		for (int i = 0; i < ahead; i++) {
			LocalDate boundary = start.plusMonths(i + 1); // split at first day of next month
			String boundaryStr = boundary.format(fmt);
			if (existing.contains(boundaryStr)) {
				continue;
			}

			String splitSql = String.format("ALTER PARTITION FUNCTION %s() SPLIT RANGE ( '%s' )", pfName, boundaryStr);
			try (Statement st = conn.createStatement()) {
				st.execute(splitSql);
			} catch (SQLException e) {
				// non-fatal: duplicate split will raise error; log and continue
			}
		}
	}

	@Override
	public List<String> getConvertTableToPartitionedSql(Connection conn, String tableName, String partitionColumn,
			String columnDefinitions, PartitionFrequency freq, int ahead) throws SQLException {
		return new ArrayList<>(); // empty -> PartitionManager will skip conversion for MSSQL
	}
}
