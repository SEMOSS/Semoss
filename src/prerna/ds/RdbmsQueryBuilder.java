package prerna.ds;

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
