package prerna.ds.util;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import prerna.algorithm.api.SemossDataType;
import prerna.sablecc2.om.Join;
import prerna.util.Utility;

public class RdbmsQueryBuilder {

	public static String escapeForSQLStatement(String s) {
		if(s == null) {
			return s;
		}
		return s.replaceAll("'", "''");
	}
	
	public static String escapeRegexCharacters(String s) {
		s = s.trim();
		s = s.replace("(", "\\(");
		s = s.replace(")", "\\)");
		return s;
	}
	
	
	/******************************
	 * CREATE QUERIES
	 ******************************/
	
	// CREATE TABLE TABLE_NAME (COLUMN1 TYPE1, COLUMN2, TYPE2, ...)
	public static String makeCreate(String tableName, String[] colNames, String[] types) {
		StringBuilder retString = new StringBuilder("CREATE TABLE "+ tableName + " (" + colNames[0] + " " + types[0]);
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			retString = retString.append(" , " + colNames[colIndex] + "  " + types[colIndex]);
		}
		retString = retString.append(")");
		return retString.toString();
	}
	
	/**
	 * Create table if not exists
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @return
	 */
	public static String makeOptionalCreate(String tableName, String [] colNames, String [] types) {
		StringBuilder retString = new StringBuilder("CREATE TABLE IF NOT EXISTS "+ tableName + " (" + colNames[0] + " " + types[0]);
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			retString = retString.append(" , " + colNames[colIndex] + "  " + types[colIndex]);
		}
		retString = retString.append(")");
		return retString.toString();
	}
	
	/**
	 * Create table if not exists
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @param defaultValues
	 * @return
	 */
	public static String makeOptionalCreateWithDefault(String tableName, String [] colNames, String [] types, Object[] defaultValues) {
		StringBuilder retString = new StringBuilder("CREATE TABLE IF NOT EXISTS "+ tableName + " (" + colNames[0] + " " + types[0]);
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			retString.append(" , " + colNames[colIndex] + "  " + types[colIndex]);
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
		retString = retString.append(")");
		return retString.toString();
	}
	
	/**
	 * Generate an insert query
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @param data
	 * @return
	 */
	public static String makeInsert(String tableName, String [] colNames, String [] types, Object [] data) {
		StringBuilder retString = new StringBuilder("INSERT INTO "+ tableName + " (" + colNames[0]);
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			retString.append(" , " + colNames[colIndex]);
		}
		String type = types[0].toLowerCase();
		String prefix = "";
		if(type.contains("varchar") || type.contains("timestamp") || type.contains("date") || type.contains("clob") || type.equals("string")) {
			if(data[0] != null) {
				prefix = "'";
			} else {
				prefix = "";
			}
		} else {
			prefix = "";
		}
		retString.append(") VALUES (" + prefix + data[0] + prefix);
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			type = types[colIndex].toLowerCase();
			if(type.contains("varchar") || type.contains("timestamp") || type.contains("date") || type.contains("clob") || type.equals("string")) {
				if(data[colIndex] != null) {
					prefix = "'";
				} else {
					prefix = "";
				}
			} else {
				prefix = "";
			}
			retString.append(" , " + prefix + data[colIndex] + prefix);
		}
		retString.append(")");
		return retString.toString();
	}
	
	/**
	 * Create the syntax to merge 2 tables together
	 * @param returnTableName			The return table name
	 * @param returnTableTypes 
	 * @param leftTableName				The left table
	 * @param leftTableTypes			The {header -> type} of the left table
	 * @param rightTableName			The right table name
	 * @param rightTableTypes			The {header -> type} of the right table
	 * @param joins						The joins between the right and left table
	 * @return
	 */
	public static String createNewTableFromJoiningTables(
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
			String leftTableJoinCol = j.getSelector();
			if(leftTableJoinCol.contains("__")) {
				leftTableJoinCol = leftTableJoinCol.split("__")[1];
			}
			String rightTableJoinCol = j.getQualifier();
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
				if( (leftColType == SemossDataType.INT || leftColType == SemossDataType.DOUBLE)  && rightColType == SemossDataType.STRING) {
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
	
	/**
	 * Column names must match between the 2 tables!
	 * @param leftTableName
	 * @param mergeTable
	 * @param columnNames
	 * @return
	 */
	public static String makeMergeIntoQuery(String leftTableName, String mergeTable, String[] keyColumns, String[] columnNames) {
		StringBuilder sql = new StringBuilder("MERGE INTO ");
		sql.append(leftTableName).append(" KEY(").append(keyColumns[0]);
		for(int i = 1; i < keyColumns.length; i++) {
			sql.append(",").append(keyColumns[i]);
		}
		sql.append(") (SELECT ").append(columnNames[0]);
		for(int i = 1; i < columnNames.length; i++) {
			sql.append(",").append(columnNames[i]);
		}
		sql.append(" FROM ").append(mergeTable).append(")");
		return sql.toString();
	}
	
	
	/******************************
	 * END CREATE QUERIES
	 ******************************/
	
	
	
	
	/******************************
	 * READ QUERIES
	 ******************************/

	//	SELECT SELECTOR1, SELECTOR2 FROM TABLE_NAME
	//  SELECT DISTINCT SELECTOR1, SELECTOR2 FROM TABLE_NAME
	public static String makeSelect(String tableName, List<String> selectors, boolean distinct) {

		StringBuilder selectStatement = new StringBuilder("SELECT ");
		if(distinct) {
			selectStatement.append("DISTINCT ");
		}

		for (int i = 0; i < selectors.size(); i++) {
			String selector = selectors.get(i);

			if (i > 0) {
				selectStatement.append(", ");
			} 
			selectStatement.append(selector);
		}
		
		selectStatement.append(" FROM " + tableName);
		return selectStatement.toString();
	}
	
	
	// SELECT AVERAGE(COLUMN1) FROM TABLE_NAME
	public static String makeFunction(String column, String function, String tableName) {
		String functionString = "SELECT ";
		switch (function.toUpperCase()) {
		case "COUNT":
			functionString += "COUNT(" + column + ")";
			break;
		case "AVERAGE":
			functionString += "AVG(" + column + ")";
			break;
		case "MIN":
			functionString += "MIN(" + column + ")";
			break;
		case "MAX":
			functionString += "MAX(" + column + ")";
			break;
		case "SUM":
			functionString += "SUM(" + column + ")";
			break;
		default:
			functionString += column;
		}

		functionString += " FROM " + tableName;
		return functionString;
	}
	
	/******************************
	 * END READ QUERIES
	 ******************************/
	
	
	
	
	/******************************
	 * UPDATE QUERIES
	 ******************************/
	
	/**
	 * Generate an alter statement to add new columns, taking into consideration joins
	 * and new column alias's
	 * @param tableName
	 * @param existingColumns
	 * @param newColumns
	 * @param joins
	 * @param newColumnAlias
	 * @return
	 */
	public static String alterMissingColumns(String tableName, 
			Map<String, SemossDataType> newColumnsToTypeMap, 
			List<Join> joins,
			Map<String, String> newColumnAlias) {
		
		List<String> newColumnsToAdd = new Vector<String>();
		List<String> newColumnsToAddTypes = new Vector<String>();
		
		// get all the join columns
		List<String> joinColumns = new Vector<String>();
		for(Join j : joins) {
			String columnName = j.getQualifier();
			if(columnName.contains("__")) {
				columnName = columnName.split("__")[1];
			}
			joinColumns.add(columnName);
		}
		
		for(String newColumn : newColumnsToTypeMap.keySet()) {
			SemossDataType newColumnType = newColumnsToTypeMap.get(newColumn);
			// modify the header
			if(newColumn.contains("__")) {
				newColumn = newColumn.split("__")[1];
			}
			// if its a join column, ignore it
			if(joinColumns.contains(newColumn)) {
				continue;
			}
			// not a join column
			// check if it has an alias
			// and then add
			if(newColumnAlias.containsKey(newColumn)) {
				newColumnsToAdd.add(newColumnAlias.get(newColumn));
			} else {
				newColumnsToAdd.add(newColumn);
			}
			// and store the type at the same index 
			// in its list
			newColumnsToAddTypes.add(SemossDataType.convertDataTypeToString(newColumnType));
		}
		
		return makeAlter(tableName, newColumnsToAdd.toArray(new String[]{}), newColumnsToAddTypes.toArray(new String[]{}));
	}
	
	
	//ALTER TABLE TABLE_NAME ADD(COLUMN1 TYPE1, COLUMN2 TYPE2, ...);
	public static String makeAlter(String tableName, String[] newHeaders, String[] newTypes) {
		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " ADD (");

		for (int i = 0; i < newHeaders.length; i++) {
			if (i > 0) {
				alterString.append(", ");
			} 
			alterString.append(newHeaders[i] + "  " + newTypes[i]);
		}

		alterString.append(")");
		return alterString.toString();
	}
	
	// UPDATE TABLE_NAME SET NEWCOL1 = NEWVAL1, NEWCOL2 = NEWVAL2 WHERE JOINCOL1 = JOINVAL1 AND JOINCOL2 = JOINVAL2
	public static String makeUpdate(String tableName, Object[] joinColumn, Object[] newColumn, Object[] joinValue, Object[] newValue) {

		String updateQuery = "UPDATE " + tableName + " SET ";

		for (int i = 0; i < newColumn.length; i++) {
			String valueInstance = RdbmsFrameUtility.cleanInstance(newValue[i].toString());
			if (i == 0) {
				updateQuery += newColumn[i].toString() + "=" + "'" + valueInstance + "'";
			} else {
				updateQuery += ", " + newColumn[i].toString() + "=" + "'" + valueInstance + "'";
			}
		}

		updateQuery += " WHERE ";

		for (int i = 0; i < joinColumn.length; i++) {
			String joinInstance = null;
			if (joinValue[i] != null) {
				joinInstance = RdbmsFrameUtility.cleanInstance(joinValue[i].toString());
			}
			if (i == 0) {
				if (joinInstance != null) {
					updateQuery += joinColumn[i].toString() + "=" + "'" + joinInstance + "'";
				} else {
					updateQuery += joinColumn[i].toString() + " IS NULL";
				}
			} else {
				if (joinInstance != null) {
					updateQuery += " AND " + joinColumn[i].toString() + "=" + "'" + joinInstance + "'";
				} else {
					updateQuery += " AND " + joinColumn[i].toString() + " IS NULL";

				}
			}
		}

		return updateQuery;
	}
	
	// make an insert query
	public static String makeInsert(String[] headers, String[] types, String[] values, Hashtable<String, String> defaultValues, String tableName) {
		StringBuilder inserter = new StringBuilder("INSERT INTO " + tableName + " (");
		StringBuilder template = new StringBuilder("(");

		for (int colIndex = 0; colIndex < headers.length; colIndex++) {
			// I need to find the type and based on that adjust what I want
			String type = types[colIndex];

			// if null on integer - empty
			// if null on string - ''
			// if currenncy - empty
			// if date - empty

			String name = headers[colIndex];
			StringBuilder thisTemplate = new StringBuilder(name);

			if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double")) {
				// if(!defaultValues.containsKey(type))
				String value = values[colIndex];
				if (value == null || value.length() == 0) {
					value = "null";
				}
				thisTemplate = new StringBuilder(value);
				// else
				// thisTemplate = new StringBuilder(defaultValues.get(type));
			}

			else if (type.equalsIgnoreCase("date")) {
				String value = values[colIndex];
				if (value == null || value.length() == 0) {
					value = "null";
					thisTemplate = new StringBuilder(value);
				} else {
					value = value.replace("'", "''");
					thisTemplate = new StringBuilder("'" + value + "'");
				}
			} else {
				// if(value != null)
				// comments will come in handy some day
				// if(!defaultValues.containsKey(type))
				String value = values[colIndex];
				value = value.replace("'", "''");
				thisTemplate = new StringBuilder("'" + value + "'");
				// else
				// thisTemplate = new StringBuilder("'" +
				// defaultValues.get(type) + "'");
			}
			if (colIndex == 0) {
				inserter.append(name);// = new StringBuilder(inserter + name);
				template.append(thisTemplate);// = new StringBuilder(template +
												// "" + thisTemplate);
			} else {
				inserter.append(" , " + name);// = new StringBuilder(inserter +
												// " , " + name );
				template.append(" , " + thisTemplate); // = new
														// StringBuilder(template
														// + " , " +
														// thisTemplate);
			}
		}

		inserter.append(")  VALUES  ");// = new StringBuilder(inserter + ")
										// VALUES ");
		// template.append(inserter + "" + template + ")");// = new
		// StringBuilder(inserter + "" + template + ")");
		inserter.append(template + ")");
		// System.out.println("Insert Values: " +template);

		// return template.toString();
		return inserter.toString();
	}
		
	// ALTER TABLE TABLE_NAME ALTER COLUMN OLD_COLUMN RENAME TO NEW_COLUMN
	public static String makeRenameColumn(String fromColumn, String toColumn, String tableName) {
		return "ALTER TABLE " + tableName + " ALTER COLUMN " + fromColumn + " RENAME TO " + toColumn;
	}
	
	// ALTER TABLE OLD_TABLE_NAME RENAME TO NEW_TABLE_NAME
	public static String makeRenameTable(String oldTable, String newTable) {
		return "ALTER TABLE " + oldTable + " RENAME TO " + newTable;
	}
	
	
	public static String createInsertPreparedStatementString(final String TABLE_NAME, final String[] columns) {
		// generate the sql for the prepared statement
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(TABLE_NAME).append(" (").append(columns[0]);
		for (int colIndex = 1; colIndex < columns.length; colIndex++) {
			sql.append(", ");
			sql.append(columns[colIndex]);
		}
		sql.append(") VALUES (?"); // remember, we already assumed one col
		for (int colIndex = 1; colIndex < columns.length; colIndex++) {
			sql.append(", ?");
		}
		sql.append(")");
		
		return sql.toString();
	}
	
	
	public static String createUpdatePreparedStatementString(final String TABLE_NAME, final String[] columnsToUpdate, final String[] whereColumns) {
		// generate the sql for the prepared statement
		StringBuilder sql = new StringBuilder("UPDATE ");
		sql.append(TABLE_NAME).append(" SET ").append(columnsToUpdate[0]).append(" = ?");
		for (int colIndex = 1; colIndex < columnsToUpdate.length; colIndex++) {
			sql.append(", ");
			sql.append(columnsToUpdate[colIndex]).append(" = ?");
		}
		if(whereColumns.length > 0) {
			sql.append(" WHERE ").append(whereColumns[0]).append(" = ?");
			for (int colIndex = 1; colIndex < whereColumns.length; colIndex++) {
				sql.append(" AND ");
				sql.append(whereColumns[colIndex]).append(" = ?");
			}
			sql.append("");
		}
		return sql.toString();
	}
	
	
	public static String createMergePreparedStatementString(final String TABLE_NAME, final String[] keyColumns, final String[] updateColumns) {
		StringBuilder sql = new StringBuilder("MERGE INTO ");
		sql.append(TABLE_NAME);
		
		//Add update columns
		sql.append("(");
		for(int i = 0; i < updateColumns.length; i++) {
			if(i > 0) {
				sql.append(", ");
			}
			sql.append(updateColumns[i]);
		}
		sql.append(") ");
		
		//Add key columns
		if(keyColumns != null && keyColumns.length > 0) {
			sql.append("KEY(");
			for(int i = 0; i < keyColumns.length; i++) {
				if(i > 0) {
					sql.append(", ");
				}
				sql.append(keyColumns[i]);
				
			}
			sql.append(") ");
		}
		return sql.toString();
		
		//Add values
//		sql.append("VALUES (?"); // remember, we already assumed one col
//		for (int colIndex = 0; colIndex < updateColumns.length; colIndex++) {
//			sql.append(", ?");
//		}
//		sql.append(")");
	}
	
	
	/******************************
	 * END UPDATE QUERIES
	 ******************************/
	
	
	
	
	/******************************
	 * DELETE QUERIES
	 ******************************/
	
	// DROP TABLE TABLE_NAME
	public static String makeDropTable(String name) {
		return "DROP TABLE " + name;
	}

	
	// DROP VIEW VIEW_NAME
	public static String makeDropView(String name) {
		return "DROP VIEW " + name;
	}
	
	
	// ALTER TABLE TABLE_NAME DROP COLUMN COLUMN1
	public static String makeDropColumn(String column, String tableName) {
		return "ALTER TABLE " + tableName + " DROP COLUMN " + column;
	}
	
	
	// DELETE FROM TABLE_NAME WHERE COLUMN1 = VAL1 AND COLUMN2 = VAL2
	public static String makeDeleteData(String tableName, String[] columnName, Object[] values) {
		StringBuilder deleteQuery = new StringBuilder("DELETE FROM " + tableName + " WHERE ");
		for (int i = 0; i < columnName.length; i++) {
			if (i > 0) {
				deleteQuery.append(" AND ");
			}
			deleteQuery.append(columnName[i] + " = '" + values[i] + "'");
		}
		return deleteQuery.toString();
	}
	
	/******************************
	 * END DELETE QUERIES
	 ******************************/

	
	
	
	/******************************
	 * FILTER QUERIES
	 ******************************/
	
	public static String getQueryStringList(List<Object> values) {
		String listString = "(";

		for (int i = 0; i < values.size(); i++) {
			Object value = values.get(i);
			value = RdbmsFrameUtility.cleanInstance(value.toString());
			listString += "'" + value + "'";
			if (i < values.size() - 1) {
				listString += ", ";
			}
		}

		listString += ")";
		return listString;
	}

	/******************************
	 * FILTER QUERIES
	 ******************************/
	
	
	public static String createTableFromFile(String fileName, Map <String, String> conceptTypes)
	{
		// if the fileName db exists delete it
		// I also need to think about multi-user ?
		// may be not, not until they move to the new version ok
		String dbName = fileName;
		dbName = fileName.replace(".csv", "");
		dbName = fileName.replace(".tsv", "");
		
		try {
			File file = new File(dbName + ".mv.db");
			if(file.exists()) {
				FileUtils.forceDelete(file);
			}
			file = new File(dbName + ".trace.db");
			if(file.exists()) {
				FileUtils.forceDelete(file);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		
		StringBuffer dropTable = new StringBuffer("DROP TABLE IF EXISTS ");
		StringBuffer createString = new StringBuffer("CREATE TABLE ");
		StringBuffer selectString = new StringBuffer("SELECT ");
		 
		Iterator <String> keys = conceptTypes.keySet().iterator();
		int count = 0;
		while(keys.hasNext())
		{
			String name = keys.next();
			String tableName = Utility.getInstanceName(name);
			String type = conceptTypes.get(name);
			name = Utility.getClassName(name);
			
			if(count == 0) {
				createString.append(tableName + " (");
				dropTable.append(tableName + "; ");
			}
			type = type.replace("TYPE:", "");

			StringBuffer tempSelect = new StringBuffer(""); 
			
			if(name.contains("UNIQUE_ROW_ID")) {
				tempSelect.append("ROWNUM()");
			} else {
				if(type.equalsIgnoreCase("DOUBLE") || type.equalsIgnoreCase("FLOAT") || type.equalsIgnoreCase("NUMBER"))
					tempSelect.append("CONVERT(" + name + ", " +"Double)");
				else if(type.equalsIgnoreCase("Integer"))
					tempSelect.append("CONVERT(" + name + ", " +"Int)");
				else if(type.equalsIgnoreCase("Date"))
					tempSelect.append("CONVERT(" + name + ", " +"Date)");
				else if(type.equalsIgnoreCase("Bigint") || type.equalsIgnoreCase("Long"))
					tempSelect.append("CONVERT(" + name + ", " +"Bigint)");
				else if(type.equalsIgnoreCase("boolean"))
					tempSelect.append("CONVERT(" + name + ", " +"boolean)");
				else //if(type.contains("varchar"))
					tempSelect.append(name);
			}
			if(count == 0) {
				createString.append(name + " " + type);
				selectString.append(tempSelect);
			} else {
				createString.append(", " + name + " " + type);
				selectString.append(", " + tempSelect);
			}
			count++; 
		}
		
		createString.append(") AS ").append(selectString).append(" from CSVREAD('" + fileName + "');");
		dropTable.append(createString);
		return dropTable.toString();
	}
	
	
}

