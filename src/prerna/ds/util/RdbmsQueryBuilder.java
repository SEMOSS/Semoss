package prerna.ds.util;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;
import prerna.util.Utility;

@Deprecated
public class RdbmsQueryBuilder {

	private static final Logger classLogger = LogManager.getLogger(RdbmsQueryBuilder.class);

	public static String escapeForSQLStatement(String s) {
		if(s == null) {
			return s;
		}
		return s.replaceAll("'", "''");
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

