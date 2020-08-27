package prerna.util.sql;

import java.sql.Connection;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.sablecc2.om.Join;

public class AnsiSqlQueryUtil extends AbstractSqlQueryUtil {

	AnsiSqlQueryUtil() {
		super();
	}
	
	AnsiSqlQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	AnsiSqlQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
	@Override
	public void enhanceConnection(Connection con) {
		// default do nothing
	}
	
	@Override
	public void initTypeConverstionMap() {
		typeConversionMap.put("INT", "INT");
		typeConversionMap.put("LONG", "BIGINT");
		
		typeConversionMap.put("NUMBER", "DOUBLE");
		typeConversionMap.put("FLOAT", "DOUBLE");
		typeConversionMap.put("DOUBLE", "DOUBLE");

		typeConversionMap.put("DATE", "DATE");
		typeConversionMap.put("TIMESTAMP", "TIMESTAMP");
		
		typeConversionMap.put("STRING", "VARCHAR(800)");
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Query helper for interpreters
	 * Here is the abstract which is for typical ANSI SQL
	 * However, each query util can override and use its
	 * own specific function names
	 */
	
	@Override
	public String getSqlFunctionSyntax(String inputFunction) {
		if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.MIN)) {
			return getMinFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.MAX)) {
			return getMaxFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.MEAN) 
				|| inputFunction.equalsIgnoreCase(QueryFunctionHelper.AVERAGE_1) 
				|| inputFunction.equalsIgnoreCase(QueryFunctionHelper.AVERAGE_2)) { 
			return getAvgFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_MEAN) 
				|| inputFunction.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_AVERAGE_1) 
				|| inputFunction.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_AVERAGE_2)) {
			return getAvgFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.MEDIAN)) {
			return getMedianFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.SUM) 
				|| inputFunction.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_SUM)) {
			return getSumFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.STDEV_1) 
				|| inputFunction.equalsIgnoreCase(QueryFunctionHelper.STDEV_2)) {
			return getStdevFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.COUNT)) {
			return getCountFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_COUNT)) {
			return getCountFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.CONCAT)) {
			return getConcatFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.GROUP_CONCAT)) {
			return getGroupConcatFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_GROUP_CONCAT)) {
			return getGroupConcatFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.LOWER)) {
			return getLowerFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.COALESCE)) {
			return getCoalesceFunctionSyntax();
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.REGEXP_LIKE)) {
			return getRegexLikeFunctionSyntax();
		// date functions
		} else if(inputFunction.equalsIgnoreCase(QueryFunctionHelper.MONTH_NAME)) {
			return getMonthNameFunctionSyntax();
		}
		
		return inputFunction;
	}
	
	@Override
	public String getMinFunctionSyntax() {
		return "MIN";
	}
	
	@Override
	public String getMaxFunctionSyntax() {
		return "MAX";
	}
	
	@Override
	public String getAvgFunctionSyntax() {
		return "AVG";
	}
	
	@Override
	public String getMedianFunctionSyntax() {
		return "MEDIAN";
	}
	
	@Override
	public String getSumFunctionSyntax() {
		return "SUM";
	}
	
	@Override
	public String getStdevFunctionSyntax() {
		return "STDDEV_SAMP";
	}
	
	@Override
	public String getCountFunctionSyntax() {
		return "COUNT";
	}
	
	@Override
	public String getConcatFunctionSyntax() {
		return "CONCAT";
	}
	
	@Override
	public String getGroupConcatFunctionSyntax() {
		return "GROUP_CONCAT";
	}
	
	@Override
	public String getLowerFunctionSyntax() {
		return "LOWER";
	}
	
	@Override
	public String getCoalesceFunctionSyntax() {
		return "COALESCE";
	}
	
	@Override
	public String getRegexLikeFunctionSyntax() {
		return "REGEXP_LIKE";
	}
	
	@Override
	public String getMonthNameFunctionSyntax() {
		return "MONTHNAME";
	}
	
	/////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////

	/*
	 * This section is intended for modifications to select queries to pull data
	 */
	
	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {
		if(limit > 0) {
			query = query.append(" LIMIT "+limit);
		}
		if(offset > 0) {
			query = query.append(" OFFSET "+offset);
		}
		return query;
	}
	
	@Override
	public String removeDuplicatesFromTable(String tableName, String fullColumnNameList){
		return "CREATE TABLE " + tableName + "_TEMP AS "
					+ "(SELECT DISTINCT " + fullColumnNameList
					+ " FROM " + tableName + " WHERE " + tableName 
					+ " IS NOT NULL AND TRIM(" + tableName + ") <> '' )";
	}
	
	public String createNewTableFromJoiningTables(
			String returnTableName, 
			String leftTableName, 
			Map<String, SemossDataType> leftTableTypes,
			String rightTableName, 
			Map<String, SemossDataType> rightTableTypes, 
			List<Join> joins,
			Map<String, String> leftTableAlias,
			Map<String, String> rightTableAlias) 
	{
		final String LEFT_TABLE_ALIAS = "A";
		final String RIGHT_TABLE_ALIAS = "B";
		
		// 1) get the join portion of the sql syntax
		
		// keep a list of the right table join cols
		// so we know not to include them in the new table
		Set<String> rightTableJoinCols = new HashSet<String>();
		
		StringBuilder joinString = new StringBuilder();
		int numJoins = joins.size();
		for(int jIdx = 0; jIdx < numJoins; jIdx++) {
			Join j = joins.get(jIdx);
			String leftTableJoinCol = j.getLColumn();
			if(leftTableJoinCol.contains("__")) {
				leftTableJoinCol = leftTableJoinCol.split("__")[1];
			}
			String rightTableJoinCol = j.getRColumn();
			if(rightTableJoinCol.contains("__")) {
				rightTableJoinCol = rightTableJoinCol.split("__")[1];
			}
			
			// keep track of join columns on the right table
			rightTableJoinCols.add(rightTableJoinCol.toUpperCase());
			
			String joinType = j.getJoinType();
			String joinSql = null;
			if(joinType.equalsIgnoreCase("inner.join")) {
				joinSql = "INNER JOIN";
			} else if(joinType.equalsIgnoreCase("left.outer.join")) {
				joinSql = "LEFT OUTER JOIN";
			} else if(joinType.equalsIgnoreCase("right.outer.join")) {
				joinSql = "RIGHT OUTER JOIN";
			} else if(joinType.equalsIgnoreCase("outer.join")) {
				joinSql = "FULL OUTER JOIN";
			} else {
				joinSql = "INNER JOIN";
			}
			
			if(jIdx != 0) {
				joinString.append(" AND ");
			} else {
				joinString.append(joinSql).append(" ").append(rightTableName)
							.append(" AS ").append(RIGHT_TABLE_ALIAS)
							.append(" ON (");
			}
			
			// need to make sure the data types are good to go
			SemossDataType leftColType = leftTableTypes.get(leftTableName + "__" + leftTableJoinCol);
			// the right column types are not tablename__colname...
			SemossDataType rightColType = rightTableTypes.get(rightTableJoinCol);
			
			if(leftColType == rightColType) {
				joinString.append(" ")
						.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" = ")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol);
			} else {
				if(leftColType == SemossDataType.DOUBLE && rightColType == SemossDataType.INT) {
					// left is double
					// right is int 
					// need to cast the right hand side
					joinString.append(" ")
						.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" = CAST(")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol)
						.append(" AS DOUBLE)");
				} else if(leftColType == SemossDataType.INT && rightColType == SemossDataType.DOUBLE) {
					// left is int
					// right is double
					// need to cast the left hand side
					joinString.append(" ")
						.append("CAST(").append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" AS DOUBLE) = ")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol);
				} else if( (leftColType == SemossDataType.INT || leftColType == SemossDataType.DOUBLE)  && rightColType == SemossDataType.STRING) {
					// one is a number
					// other is a string
					// convert the string to a number
					joinString.append(" ")
						.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" = CAST(")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol)
						.append(" AS DOUBLE)");
				} else if( (rightColType == SemossDataType.INT || rightColType == SemossDataType.DOUBLE ) && leftColType == SemossDataType.STRING) {
					// one is a number
					// other is a string
					// convert the string to a number
					joinString.append(" CAST(")
						.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" AS DOUBLE) =")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol);
				} else {
					// not sure... just make everything a string
					joinString.append(" CAST(")
					.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
					.append(" AS VARCHAR(800)) = CAST(")
					.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol)
					.append(" AS VARCHAR(800))");
				}
			}
		}
		joinString.append(")");
		
		// 2) get the create table and the selector portions
		Set<String> leftTableHeaders = leftTableTypes.keySet();
		Set<String> rightTableHeaders = rightTableTypes.keySet();
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE ").append(returnTableName).append(" AS ( SELECT ");
		
		// select all the columns from the left side
		int counter = 0;
		int size = leftTableHeaders.size();
		for(String leftTableCol : leftTableHeaders) {
			if(leftTableCol.contains("__")) {
				leftTableCol = leftTableCol.split("__")[1];
			}
			sql.append(LEFT_TABLE_ALIAS).append(".").append(leftTableCol);
			// add the alias if there
			if(leftTableAlias.containsKey(leftTableCol)) {
				sql.append(" AS ").append(leftTableAlias.get(leftTableCol));
			}
			if(counter + 1 < size) {
				sql.append(", ");
			}
			counter++;
		}
		
		// select the columns from the right side which are not part of the join!!!
		for(String rightTableCol : rightTableHeaders) {
			if(rightTableCol.contains("__")) {
				rightTableCol = rightTableCol.split("__")[1];
			}
			if(rightTableJoinCols.contains(rightTableCol.toUpperCase())) {
				counter++;
				continue;
			}
			sql.append(", ").append(RIGHT_TABLE_ALIAS).append(".").append(rightTableCol);
			// add the alias if there
			if(rightTableAlias.containsKey(rightTableCol)) {
				sql.append(" AS ").append(rightTableAlias.get(rightTableCol));
			}
			counter++;
		}
		
		// 3) combine everything
		
		sql.append(" FROM ").append(leftTableName).append(" AS ").append(LEFT_TABLE_ALIAS).append(" ")
				.append(joinString.toString()).append(" )");

		return sql.toString();
	}
	
	@Override
	public String createInsertPreparedStatementString(String tableName, String[] columns) {
		// generate the sql for the prepared statement
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(tableName).append(" (");
		for (int colIndex = 0; colIndex < columns.length; colIndex++) {
			String columnName = columns[colIndex];
			if(isSelectorKeyword(columnName)) {
				columnName = getEscapeKeyword(columnName);
			}
			sql.append(columnName);
			if( (colIndex+1) != columns.length) {
				sql.append(", ");
			}
		}
		sql.append(") VALUES (?"); 
		// remember, we already assumed one col
		for (int colIndex = 1; colIndex < columns.length; colIndex++) {
			sql.append(", ?");
		}
		sql.append(")");
		
		return sql.toString();
	}

	@Override
	public String createUpdatePreparedStatementString(String tableName, String[] columnsToUpdate, String[] whereColumns) {
		// generate the sql for the prepared statement
		StringBuilder sql = new StringBuilder("UPDATE ");
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		sql.append(tableName).append(" SET ");
		String columnName = columnsToUpdate[0];
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		sql.append(columnName).append(" = ?");
		for (int colIndex = 1; colIndex < columnsToUpdate.length; colIndex++) {
			sql.append(", ");
			columnName = columnsToUpdate[colIndex];
			if(isSelectorKeyword(columnName)) {
				columnName = getEscapeKeyword(columnName);
			}
			sql.append(columnName).append(" = ?");
		}
		if(whereColumns.length > 0) {
			columnName = whereColumns[0];
			if(isSelectorKeyword(columnName)) {
				columnName = getEscapeKeyword(columnName);
			}
			sql.append(" WHERE ").append(columnName).append(" = ?");
			for (int colIndex = 1; colIndex < whereColumns.length; colIndex++) {
				sql.append(" AND ");
				columnName = whereColumns[colIndex];
				if(isSelectorKeyword(columnName)) {
					columnName = getEscapeKeyword(columnName);
				}
				sql.append(columnName).append(" = ?");
			}
			sql.append("");
		}
		return sql.toString();
	}
	
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Some booleans so we know if we can use optimized scripts
	 * Or if we have to query the engine directly
	 * 
	 * Set it up in the base such that we will check these values
	 * in the "if exists" methods so that the implementations that override these
	 * methods only need to override these booleans and not every subsequent method
	 */
	
	@Override
	public boolean allowArrayDatatype() {
		return true;
	}
	
	@Override
	public boolean allowBooleanDataType() {
		return true;
	}
	
	@Override
	public String getDateWithTimeDataType() {
		return "TIMESTAMP";
	}
	
	@Override
	public boolean allowBlobDataType() {
		return true;
	}
	
	@Override
	public String getBlobReplacementDataType() {
		throw new UnsupportedOperationException("The blob replacement has not been identified for this RDBMS type");
	}
	
	@Override
	public boolean allowBlobJavaObject() {
		return true;
	}
	
	@Override
	public boolean allowAddColumn() {
		return true;
	}
	
	@Override
	public boolean allowMultiAddColumn() {
		return true;
	}
	
	@Override
	public boolean allowRedefineColumn() {
		return true;
	}
	
	@Override
	public boolean allowDropColumn() {
		return true;
	}
	
	@Override
	public boolean allowsIfExistsTableSyntax() {
		return true;
	}

	@Override
	public boolean allowIfExistsIndexSyntax() {
		return true;
	}
	
	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
		return true;
	}
	
	@Override
	public boolean allowIfExistsAddConstraint() {
		return true;
	}
	
	/////////////////////////////////////////////////////////////////////////
	
	/*
	 * Create table scripts
	 */
	
	@Override
	public String createTable(String tableName, String[] colNames, String[] types) {
		// should escape keywords
		tableName = cleanTableName(tableName);
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		String columName = colNames[0];
		if(isSelectorKeyword(columName)) {
			columName = getEscapeKeyword(columName);
		}
		
		StringBuilder retString = new StringBuilder("CREATE TABLE ").append(tableName).append(" (").append(columName).append(" ").append(types[0]);
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			columName = colNames[colIndex];
			if(isSelectorKeyword(columName)) {
				columName = getEscapeKeyword(columName);
			}
			retString.append(" , ").append(columName).append("  ").append(types[colIndex]);
		}
		retString = retString.append(");");
		return retString.toString();
	}
	
	@Override
	public String createTableWithDefaults(String tableName, String [] colNames, String [] types, Object[] defaultValues) {
		// should escape keywords
		tableName = cleanTableName(tableName);
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		String columName = colNames[0];
		if(isSelectorKeyword(columName)) {
			columName = getEscapeKeyword(columName);
		}
		
		StringBuilder retString = new StringBuilder("CREATE TABLE ").append(tableName).append(" (").append(columName).append(" ").append(types[0]);
		if(defaultValues[0] != null) {
			retString.append(" DEFAULT ");
			if(defaultValues[0] instanceof String) {
				retString.append("'").append(defaultValues[0]).append("'");
			} else {
				retString.append(defaultValues[0]);
			}
		}
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			columName = colNames[colIndex];
			if(isSelectorKeyword(columName)) {
				columName = getEscapeKeyword(columName);
			}
			retString.append(" , ").append(columName).append("  ").append(types[colIndex]);
			// add default values
			if(defaultValues[colIndex] != null) {
				retString.append(" DEFAULT ");
				if(defaultValues[colIndex] instanceof String) {
					retString.append("'").append(defaultValues[colIndex]).append("'");
				} else {
					retString.append(defaultValues[colIndex]);
				}
			}
		}
		retString.append(");");
		return retString.toString();
	}
	
	@Override
	public String createTableWithCustomConstraints(String tableName, String [] colNames, String [] types, Object[] customConstraints) {
		// should escape keywords
		tableName = cleanTableName(tableName);
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		String columName = colNames[0];
		if(isSelectorKeyword(columName)) {
			columName = getEscapeKeyword(columName);
		}
		
		StringBuilder retString = new StringBuilder("CREATE TABLE ").append(tableName).append(" (").append(columName).append(" ").append(types[0]);
		if(customConstraints[0] != null) {
			retString.append(" ").append(customConstraints[0]).append(" ");
		}
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			columName = colNames[colIndex];
			if(isSelectorKeyword(columName)) {
				columName = getEscapeKeyword(columName);
			}
			retString.append(" , ").append(columName).append("  ").append(types[colIndex]);
			// add default values
			if(customConstraints[colIndex] != null) {
				retString.append(" ").append(customConstraints[colIndex]).append(" ");
			}
		}
		retString.append(");");
		return retString.toString();
	}
	
	@Override
	public String createTableIfNotExists(String tableName, String[] colNames, String[] types) {
		if(!allowsIfExistsTableSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists table syntax");
		}
		
		// should escape keywords
		tableName = cleanTableName(tableName);
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		String columName = colNames[0];
		if(isSelectorKeyword(columName)) {
			columName = getEscapeKeyword(columName);
		}
		
		StringBuilder retString = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (").append(columName).append(" ").append(types[0]);
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			columName = colNames[colIndex];
			if(isSelectorKeyword(columName)) {
				columName = getEscapeKeyword(columName);
			}
			retString.append(" , ").append(columName).append("  ").append(types[colIndex]);
		}
		retString = retString.append(");");
		return retString.toString();
	}
	
	@Override
	public String createTableIfNotExistsWithDefaults(String tableName, String [] colNames, String [] types, Object[] defaultValues) {
		if(!allowsIfExistsTableSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists table syntax");
		}

		// should escape keywords
		tableName = cleanTableName(tableName);
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		String columName = colNames[0];
		if(isSelectorKeyword(columName)) {
			columName = getEscapeKeyword(columName);
		}
		
		StringBuilder retString = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (").append(columName).append(" ").append(types[0]);
		if(defaultValues[0] != null) {
			retString.append(" DEFAULT ");
			if(defaultValues[0] instanceof String) {
				retString.append("'").append(defaultValues[0]).append("'");
			} else {
				retString.append(defaultValues[0]);
			}
		}
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			columName = colNames[colIndex];
			if(isSelectorKeyword(columName)) {
				columName = getEscapeKeyword(columName);
			}
			retString.append(" , ").append(columName).append("  ").append(types[colIndex]);
			// add default values
			if(defaultValues[colIndex] != null) {
				retString.append(" DEFAULT ");
				if(defaultValues[colIndex] instanceof String) {
					retString.append("'").append(defaultValues[colIndex]).append("'");
				} else {
					retString.append(defaultValues[colIndex]);
				}
			}
		}
		retString.append(");");
		return retString.toString();
	}
	
	@Override
	public String createTableIfNotExistsWithCustomConstraints(String tableName, String [] colNames, String [] types, Object[] customConstraints) {
		if(!allowsIfExistsTableSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists table syntax");
		}

		// should escape keywords
		tableName = cleanTableName(tableName);
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		String columName = colNames[0];
		if(isSelectorKeyword(columName)) {
			columName = getEscapeKeyword(columName);
		}
		
		StringBuilder retString = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (").append(columName).append(" ").append(types[0]);
		if(customConstraints[0] != null) {
			retString.append(" ").append(customConstraints[0]).append(" ");
		}
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			columName = colNames[colIndex];
			if(isSelectorKeyword(columName)) {
				columName = getEscapeKeyword(columName);
			}
			retString.append(" , ").append(columName).append("  ").append(types[colIndex]);
			// add default values
			if(customConstraints[colIndex] != null) {
				retString.append(" ").append(customConstraints[colIndex]).append(" ");
			}
		}
		retString.append(");");
		return retString.toString();
	}
	
	/*
	 * Drop table scripts
	 */

	@Override
	public String dropTable(String tableName) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		return "DROP TABLE " + tableName + ";";
	}
	
	@Override
	public String dropTableIfExists(String tableName) {
		if(!allowsIfExistsTableSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists table syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		return "DROP TABLE IF EXISTS " + tableName + ";";
	}
	
	/*
	 * Alter table scripts
	 */
	
	@Override
	public String alterTableName(String tableName, String newTableName) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(newTableName)) {
			newTableName = getEscapeKeyword(newTableName);
		}
		return "ALTER TABLE " + tableName + " RENAME TO " + newTableName;
	}
	
	/*
	 * Single add column
	 */

	@Override
	public String alterTableAddColumn(String tableName, String newColumn, String newColType) {
		if(!allowAddColumn()) {
			throw new UnsupportedOperationException("Does not support add column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(newColumn)) {
			newColumn = getEscapeKeyword(newColumn);
		}
		return "ALTER TABLE " + tableName + " ADD COLUMN " + newColumn + " " + newColType + ";";
	}

	@Override
	public String alterTableAddColumnWithDefault(String tableName, String newColumn, String newColType, Object defualtValue) {
		if(!allowAddColumn()) {
			throw new UnsupportedOperationException("Does not support add column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(newColumn)) {
			newColumn = getEscapeKeyword(newColumn);
		}
		return "ALTER TABLE " + tableName + " ADD COLUMN " + newColumn + " " + newColType + " DEFAULT '" + defualtValue + "';";
	}

	@Override
	public String alterTableAddColumnIfNotExists(String tableName, String newColumn, String newColType) {
		if(!allowAddColumn()) {
			throw new UnsupportedOperationException("Does not support add column syntax");
		}
		if(!allowIfExistsModifyColumnSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists column syntax");
		}

		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(newColumn)) {
			newColumn = getEscapeKeyword(newColumn);
		}
		return "ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS " + newColumn + " " + newColType + ";";
	}

	@Override
	public String alterTableAddColumnIfNotExistsWithDefault(String tableName, String newColumn, String newColType, Object defualtValue) {
		if(!allowAddColumn()) {
			throw new UnsupportedOperationException("Does not support add column syntax");
		}
		if(!allowIfExistsModifyColumnSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(newColumn)) {
			newColumn = getEscapeKeyword(newColumn);
		}
		return "ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS " + newColumn + " " + newColType + " DEFAULT '" + defualtValue + "';";
	}
	
	/*
	 * Multi add column
	 */
	
	@Override
	public String alterTableAddColumns(String tableName, String[] newColumns, String[] newColTypes) {
		if(!allowMultiAddColumn()) {
			throw new UnsupportedOperationException("Does not support multi add column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		
		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " ADD (");
		for (int i = 0; i < newColumns.length; i++) {
			if (i > 0) {
				alterString.append(", ");
			}
			
			String newColumn = newColumns[i];
			// should escape keywords
			if(isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}
			
			alterString.append(newColumns[i] + "  " + newColTypes[i]);
		}
		alterString.append(");");
		return alterString.toString();
	}

	@Override
	public String alterTableAddColumnsWithDefaults(String tableName, String[] newColumns, String[] newColTypes, Object[] defaultValues) {
		if(!allowMultiAddColumn()) {
			throw new UnsupportedOperationException("Does not support multi add column syntax");
		}
		
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		
		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " ADD (");
		for (int i = 0; i < newColumns.length; i++) {
			if (i > 0) {
				alterString.append(", ");
			}
			
			String newColumn = newColumns[i];
			// should escape keywords
			if(isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}
			
			alterString.append(newColumns[i] + "  " + newColTypes[i]);
			
			// add default values
			if(defaultValues[i] != null) {
				alterString.append(" DEFAULT ");
				if(defaultValues[i]  instanceof String) {
					alterString.append("'").append(defaultValues[i]).append("'");
				} else {
					alterString.append(defaultValues[i]);
				}
			}
		}
		alterString.append(");");
		return alterString.toString();
	}

	@Override
	public String alterTableDropColumn(String tableName, String columnName) {
		if(!allowDropColumn()) {
			throw new UnsupportedOperationException("Does not support drop column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " DROP COLUMN " + columnName + ";";
	}

	@Override
	public String alterTableDropColumnIfExists(String tableName, String columnName) {
		if(!allowDropColumn()) {
			throw new UnsupportedOperationException("Does not support drop column syntax");
		}
		if(!allowIfExistsModifyColumnSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS " + columnName + ";";
	}
	
	@Override
	public String modColumnType(String tableName, String columnName, String dataType) {
		if(!allowRedefineColumn()) {
			throw new UnsupportedOperationException("Does not support redefinition of column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " " + dataType + ";";
	}
	
	@Override
	public String modColumnTypeWithDefault(String tableName, String columnName, String dataType, Object defualtValue) {
		if(!allowRedefineColumn()) {
			throw new UnsupportedOperationException("Does not support redefinition of column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " " + dataType + " " + defualtValue + ";";
	}
	
	@Override
	public String modColumnTypeIfExists(String tableName, String columnName, String dataType) {
		if(!allowRedefineColumn()) {
			throw new UnsupportedOperationException("Does not support redefinition of column syntax");
		}
		if(!allowIfExistsModifyColumnSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " ALTER COLUMN IF EXISTS " + columnName + " " + dataType + ";";
	}
	
	@Override
	public String modColumnTypeIfExistsWithDefault(String tableName, String columnName, String dataType, Object defualtValue) {
		if(!allowRedefineColumn()) {
			throw new UnsupportedOperationException("Does not support redefinition of column syntax");
		}
		if(!allowIfExistsModifyColumnSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " ALTER COLUMN IF EXISTS " + columnName + " " + dataType + " " + defualtValue + ";";
	}

	@Override
	public String createIndex(String indexName, String tableName, String columnName) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "CREATE INDEX " + indexName + " ON " + tableName + "(" + columnName + ");";
	}

	@Override
	public String createIndex(String indexName, String tableName, Collection<String> columns) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		
		StringBuilder builder = new StringBuilder();
		builder.append("CREATE INDEX ")
				.append(indexName)
				.append(" ON ")
				.append(tableName)
				.append("(");
		
		Iterator<String> colIt = columns.iterator();
		String columnName = colIt.next();
		// should escape keywords
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		builder.append(columnName);
		while(colIt.hasNext()) {
			columnName = colIt.next();
			// should escape keywords
			if(isSelectorKeyword(columnName)) {
				columnName = getEscapeKeyword(columnName);
			}
			builder.append(", ").append(columnName);
		}
		builder.append(");");
		return builder.toString();
	}

	@Override
	public String createIndexIfNotExists(String indexName, String tableName, String columnName) {
		if(!allowIfExistsIndexSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists index syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + tableName + "(" + columnName + ");";
	}

	@Override
	public String createIndexIfNotExists(String indexName, String tableName, Collection<String> columns) {
		if(!allowIfExistsIndexSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists index syntax");
		}
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		
		StringBuilder builder = new StringBuilder();
		builder.append("CREATE INDEX IF NOT EXISTS ")
				.append(indexName)
				.append(" ON ")
				.append(tableName)
				.append("(");
		
		Iterator<String> colIt = columns.iterator();
		String columnName = colIt.next();
		// should escape keywords
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		builder.append(columnName);
		while(colIt.hasNext()) {
			columnName = colIt.next();
			// should escape keywords
			if(isSelectorKeyword(columnName)) {
				columnName = getEscapeKeyword(columnName);
			}
			builder.append(", ").append(columnName);
		}
		builder.append(");");
		return builder.toString();
	}

	@Override
	public String dropIndex(String indexName, String tableName) {
		return "DROP INDEX " + indexName + ";";
	}

	@Override
	public String dropIndexIfExists(String indexName, String tableName) {
		if(!allowIfExistsIndexSyntax()) {
			throw new UnsupportedOperationException("Does not support if exists index syntax");
		}
		return "DROP INDEX IF EXISTS " + indexName + ";";
	}
	
	@Override
	public String insertIntoTable(String tableName, String[] columnNames, String[] types, Object[] values) {
		if(columnNames.length !=  types.length) {
			throw new UnsupportedOperationException("Headers and types must have the same length");
		}
		if(columnNames.length != values.length) {
			throw new UnsupportedOperationException("Headers and values must have the same length");
		}

		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}

		// only loop 1 time around both arrays since length must always match
		StringBuilder inserter = new StringBuilder("INSERT INTO " + tableName + " (");
		StringBuilder template = new StringBuilder();

		for (int colIndex = 0; colIndex < columnNames.length; colIndex++) {
			String columnName = columnNames[colIndex];
			String type = types[colIndex];
			Object value = values[colIndex];

			if(colIndex > 0) {
				inserter.append(", ");
				template.append(", ");
			}

			if(isSelectorKeyword(columnName)) {
				columnName = getEscapeKeyword(columnName);
			}

			// always jsut append the column name
			inserter.append(columnName);

			if(value == null) {
				// append null without quotes
				template.append("null");
				continue;
			}

			// we do not have a null
			// now we care how we insert based on the type of the value
			SemossDataType dataType = SemossDataType.convertStringToDataType(type);
			if(dataType == SemossDataType.BOOLEAN ||
					dataType == SemossDataType.INT ||
					dataType == SemossDataType.DOUBLE) {
				// append as is
				template.append(value);
			} else if(dataType == SemossDataType.STRING || dataType == SemossDataType.FACTOR) {
				template.append("'").append(escapeForSQLStatement(value + "")).append("'");
			} else if(dataType == SemossDataType.DATE) {
				if(value instanceof SemossDate) {
					Date d = ((SemossDate) value).getDate();
					if(d == null) {
						template.append(null + "");
					} else {
						template.append("'").append(((SemossDate) value).getFormatted("yyyy-MM-dd")).append("'");
					}
				} else {
					SemossDate dateValue = SemossDate.genDateObj(value + "");
					if(dateValue == null) {
						template.append(null + "");
					} else {
						template.append("'").append(dateValue.getFormatted("yyyy-MM-dd")).append("'");
					}
				}
			} else if(dataType == SemossDataType.TIMESTAMP) {
				if(value instanceof SemossDate) {
					Date d = ((SemossDate) value).getDate();
					if(d == null) {
						template.append(null + "");
					} else {
						template.append("'").append(((SemossDate) value).getFormatted("yyyy-MM-dd HH:mm:ss")).append("'");
					}
				} else {
					SemossDate dateValue = SemossDate.genTimeStampDateObj(value + "");
					if(dateValue == null) {
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
	public String deleteAllRowsFromTable(String tableName) {
		return "DELETE FROM " + tableName; 
	}
	
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query database scripts
	 */
	
	@Override
	public String tableExistsQuery(String tableName, String schema) {
		// there is no commonality that i have found for this
		throw new UnsupportedOperationException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}
	
	@Override
	public String getAllColumnDetails(String tableName, String schema) {
		// there is no commonality that i have found for this
		throw new UnsupportedOperationException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}

	@Override
	public String columnDetailsQuery(String tableName, String columnName, String schema) {
		// there is no commonality that i have found for this
		throw new UnsupportedOperationException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}

	@Override
	public String getIndexList(String schema) {
		// there is no commonality that i have found for this
		throw new UnsupportedOperationException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}

	@Override
	public String getIndexDetails(String indexName, String tableName, String schema) {
		// there is no commonality that i have found for this
		throw new UnsupportedOperationException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}
	
	@Override
	public String allIndexForTableQuery(String tableName, String schema) {
		// there is no commonality that i have found for this
		throw new UnsupportedOperationException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}
	
	@Override
	public String constraintExistsQuery(String constraintName) {
		// there is no commonality that i have found for this
		throw new UnsupportedOperationException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}
}
