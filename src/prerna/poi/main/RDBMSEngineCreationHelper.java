package prerna.poi.main;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.sql.SQLQueryUtil;

public class RDBMSEngineCreationHelper {

	private RDBMSEngineCreationHelper() {
		
	}
	
	public static void insertAllTablesAsInsights(IEngine rdbmsEngine, SQLQueryUtil queryUtil)
	{		
		// get all the tables names in the database
		String getAllTablesQuery = "SHOW TABLES FROM PUBLIC";
		if(queryUtil != null) {
			getAllTablesQuery = queryUtil.getDialectAllTables();
		}
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(rdbmsEngine, getAllTablesQuery);
		String[] names = wrapper.getVariables();
		Set<String> tableNames = new HashSet<String>();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String tableName = ss.getVar("TABLE_NAME") + "";
			tableNames.add(tableName);
		}
		
		insertNewTablesAsInsights(rdbmsEngine, tableNames);
	}
	
	public static void insertNewTablesAsInsights(IEngine rdbmsEngine, Set<String> newTables) {
		String engineName = rdbmsEngine.getEngineName();
		InsightAdministrator admin = new InsightAdministrator(rdbmsEngine.getInsightDatabase());
		
		//determine the # where the new questions should start
		String insightName = ""; 
		String sql = ""; 
		String[] pkqlRecipeToSave = null;
		String layout = ""; 

		try {
			for(String newTable : newTables) {
				String cleanTableName = cleanTableName(newTable);
				insightName = "Show all from " + cleanTableName;
				layout = "Grid";
				sql = "SELECT * FROM " + cleanTableName;
				pkqlRecipeToSave = new String[3];
				pkqlRecipeToSave[0] = "data.frame('grid');";
				pkqlRecipeToSave[1] = "data.import(api:" + engineName + ".query(<query>" + sql + "</query>));";
				pkqlRecipeToSave[2] = "panel[0].viz ( Grid , [ ] , { 'offset' : 0 , 'limit' : 1000 } );";

				admin.addInsight(insightName, layout, pkqlRecipeToSave);
			}
		} catch(RuntimeException e) {
			System.out.println("caught exception while adding question.................");
			e.printStackTrace();
		}
	}
	
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(IEngine rdbmsEngine, SQLQueryUtil queryUtil) {
		// get the metadata from the connection
		RDBMSNativeEngine rdbms = (RDBMSNativeEngine) rdbmsEngine;
		DatabaseMetaData meta = rdbms.getConnectionMetadata();
		
		// table that will store 
		// table_name -> {
		// 					colname1 -> coltype,
		//					colname2 -> coltype,
		//				}
		Map<String, Map<String, String>> tableColumnMap = new Hashtable<String, Map<String, String>>();
		
		// get all the tables
		ResultSet tables = null;
		try {
			tables = meta.getTables(null, null, null, new String[]{"TABLE"});
			while(tables.next()) {
				// get the table name
				String table = tables.getString("table_name");
				// keep a map of the columns
				Map<String, String> colDetails = new HashMap<String, String>();
				// iterate through the columns
				
				ResultSet keys = null;
				try {
					keys = meta.getColumns(null, null, table, null);
					while(keys.next()) {
						colDetails.put(keys.getString("column_name"), keys.getString("type_name").toUpperCase());
					}
				} catch(SQLException e) {
					e.printStackTrace();
				} finally {
					try {
						if(keys != null) {
							keys.close();
						}
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				}
				tableColumnMap.put(table, colDetails);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(tables != null) {
					tables.close();
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}

		return tableColumnMap;
	}

	/**
	 * Remove all non alpha-numeric underscores from form name
	 * @param s
	 * @return
	 */
	public static String cleanTableName(String s) {
		s = s.trim();
		s = s.replaceAll(" ", "_");
		s = s.replaceAll("[^a-zA-Z0-9\\_]", ""); // matches anything that is not alphanumeric or underscore
		while(s.contains("__")){
			s = s.replace("__", "_");
		}
		// can't start with a digit in rdbms
		// have it start with an underscore and it will work
		if(Character.isDigit(s.charAt(0))) {
			s = "_" + s;
		}
		
		return s;
	}
	
	public static String escapeForSQLStatement(String s) {
		return s.replaceAll("'", "''");
	}

	public static boolean conceptExists(IEngine engine, String tableName, String colName, Object instanceValue) {
		String query = "SELECT DISTINCT " + colName + " FROM " + tableName + " WHERE " + colName + "='" + RDBMSEngineCreationHelper.escapeForSQLStatement(instanceValue + "") + "'";
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			return true;
		}
		return false;
	}
}
