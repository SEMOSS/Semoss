package prerna.ds;

import java.util.Hashtable;
import java.util.List;

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
			selectStatement.append(" DISTINCT ");
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

		functionString += "FROM " + tableName;
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
			String joinInstance = RdbmsFrameUtility.cleanInstance(joinValue[i].toString());
			if (i == 0) {
				updateQuery += joinColumn[i].toString() + "=" + "'" + joinInstance + "'";
			} else {
				updateQuery += " AND " + joinColumn[i].toString() + "=" + "'" + joinInstance + "'";
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
	public static String makeDeleteData(String tableName, String[] columnName, String[] values) {
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
}
