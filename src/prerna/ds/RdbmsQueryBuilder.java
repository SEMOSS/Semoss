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
	
	/******************************
	 * END UPDATE QUERIES
	 ******************************/
	
	
	
	
	/******************************
	 * DELETE QUERIES
	 ******************************/
	
	// drop a table
	public static String makeDropTable(String name) {
		return "DROP TABLE " + name;
	}

	// drop a view
	public static String makeDropView(String name) {
		return "DROP VIEW " + name;
	}
	
	public static String makeDropColumn(String column, String tableName) {
		return "ALTER TABLE " + tableName + " DROP COLUMN " + column;
	}
	
	/******************************
	 * END DELETE QUERIES
	 ******************************/
}
