package prerna.ds.util;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import prerna.algorithm.api.SemossDataType;
import prerna.sablecc2.om.Join;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Deprecated
public class RdbmsQueryBuilder {

	private static final Logger classLogger = LogManager.getLogger(RdbmsQueryBuilder.class);

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
	 * Generate an insert query
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @param data
	 * @return
	 */
	@Deprecated
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
			Map<String, String> newColumnAlias,
			AbstractSqlQueryUtil util) {
		
		List<String> newColumnsToAdd = new Vector<String>();
		List<String> newColumnsToAddTypes = new Vector<String>();
		
		// get all the join columns
		List<String> joinColumns = new Vector<String>();
		for(Join j : joins) {
			String columnName = j.getRColumn();
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
			newColumnsToAddTypes.add(util.cleanType(newColumnType.toString()));
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

	// DELETE FROM TABLE_NAME WHERE COLUMN1 = VAL1 AND COLUMN2 = VAL2
	@Deprecated
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

	
	public static String createTableFromFile(String fileName, Map <String, String> conceptTypes)
	{
		// if the fileName db exists delete it
		// I also need to think about multi-user ?
		// may be not, not until they move to the new version ok
		String normalizedFileName = Utility.normalizePath(fileName);
		String dbName = normalizedFileName;
		dbName = normalizedFileName.replace(".csv", "");
		dbName = normalizedFileName.replace(".tsv", "");
		
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
			classLogger.error(Constants.STACKTRACE, e);
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

