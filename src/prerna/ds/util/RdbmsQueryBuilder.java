package prerna.ds.util;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.sablecc2.om.Join;

public class RdbmsQueryBuilder {

	
	/******************************
	 * CREATE QUERIES
	 ******************************/
	
	// CREATE TABLE TABLE_NAME (COLUMN1 TYPE1, COLUMN2, TYPE2, ...)
	public static String makeCreate(String tableName, String[] headers, String[] types) {
		StringBuilder createString = new StringBuilder("CREATE TABLE " + tableName + " (");

		for (int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			String header = headers[headerIndex];

			if (headerIndex > 0) {
				createString.append(", ");
			}
			createString.append(header + "  " + types[headerIndex]);
		}

		createString.append(");");

		return createString.toString();
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
			Map<String, DATA_TYPES> leftTableTypes,
			String rightTableName, 
			Map<String, DATA_TYPES> rightTableTypes, 
			List<Join> joins) 
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
			rightTableJoinCols.add(rightTableJoinCol);
			
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
			IMetaData.DATA_TYPES leftColType = leftTableTypes.get(leftTableName + "__" + leftTableJoinCol);
			// the right column types are not tablename__colname...
			IMetaData.DATA_TYPES rightColType = rightTableTypes.get(rightTableJoinCol);
			
			if(leftColType == rightColType) {
				joinString.append(" ")
						.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" = ")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol);
			} else {
				if(leftColType == IMetaData.DATA_TYPES.NUMBER && rightColType == IMetaData.DATA_TYPES.STRING) {
					// one is a number
					// other is a string
					// convert the string to a number
					joinString.append(" ")
						.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" = CAST(")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol)
						.append(" AS DOUBLE)");
				} else if(rightColType == IMetaData.DATA_TYPES.NUMBER && leftColType == IMetaData.DATA_TYPES.STRING) {
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
		
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE ").append(returnTableName).append(" AS ( SELECT ");
		
		// select all the columns from the left side
		Set<String> leftTableHeaders = leftTableTypes.keySet();
		int counter = 0;
		int size = leftTableHeaders.size();
		for(String leftTableCol : leftTableHeaders) {
			if(leftTableCol.contains("__")) {
				leftTableCol = leftTableCol.split("__")[1];
			}
			sql.append(LEFT_TABLE_ALIAS).append(".").append(leftTableCol);
			if(counter + 1 < size) {
				sql.append(", ");
			}
			counter++;
		}
		
		// select the columns from the right side which are not part of the join!!!
		Set<String> rightTableHeaders = rightTableTypes.keySet();
		for(String rightTableCol : rightTableHeaders) {
			if(rightTableCol.contains("__")) {
				rightTableCol = rightTableCol.split("__")[1];
			}
			if(rightTableJoinCols.contains(rightTableCol)) {
				counter++;
				continue;
			}
			sql.append(", ").append(RIGHT_TABLE_ALIAS).append(".").append(rightTableCol);
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
	public static String makeMergeIntoQuery(String leftTableName, String mergeTable, String[] columnNames) {
		StringBuilder sql = new StringBuilder("MERGE INTO ");
		sql.append(leftTableName).append(" KEY(").append(columnNames[0]);
		for(int i = 1; i < columnNames.length; i++) {
			sql.append(",").append(columnNames[i]);
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
}

