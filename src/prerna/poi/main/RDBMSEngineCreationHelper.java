package prerna.poi.main;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import prerna.auth.SecurityUpdateUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;

public class RDBMSEngineCreationHelper {

	private RDBMSEngineCreationHelper() {
		
	}
	
	public static void insertAllTablesAsInsights(IEngine rdbmsEngine) {		
		// get all the tables names in the database
		Map<String, Map<String, String>> existingMetaModel = getExistingRDBMSStructure(rdbmsEngine);
		insertAllTablesAsInsights(rdbmsEngine, existingMetaModel);
	}
	
	public static void insertAllTablesAsInsights(IEngine rdbmsEngine, Map<String, Map<String, String>> existingMetaModel) {
		insertNewTablesAsInsights(rdbmsEngine, existingMetaModel, existingMetaModel.keySet());
	}
	
	public static void insertNewTablesAsInsights(IEngine rdbmsEngine, Set<String> newTables) {
		insertNewTablesAsInsights(rdbmsEngine, getExistingRDBMSStructure(rdbmsEngine, newTables), newTables);
	}
	
	public static void insertNewTablesAsInsights(IEngine rdbmsEngine,  Map<String, Map<String, String>> existingMetaModel, Set<String> newTables) {
		String engineName = rdbmsEngine.getEngineId();
		InsightAdministrator admin = new InsightAdministrator(rdbmsEngine.getInsightDatabase());
		
		//determine the # where the new questions should start
		String insightName = ""; 
		String[] recipeArray = null;
		String layout = ""; 

		try {
			for(String newTable : newTables) {
				String cleanTableName = cleanTableName(newTable);
				insightName = "Show first 500 records from " + cleanTableName;
				layout = "Grid";
				recipeArray = new String[5];
				recipeArray[0] = "AddPanel(0);";
				recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
				recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
				StringBuilder importPixel = new StringBuilder("Database(\"" + engineName + "\") | Select(");
				Map<String, String> colToTypes = existingMetaModel.get(newTable);
				// inconsistent case for rdbms existingMetamodel passed in from reactors and building it from DatabaseMetaData 
				if(colToTypes == null) {
				   colToTypes = existingMetaModel.get(newTable.toUpperCase());
				}

				Set<String> columnNames = colToTypes.keySet();
				int size = columnNames.size();
				int counter = 0;
				for(String col : columnNames) {
					importPixel.append(newTable).append("__").append(col);
					if(counter + 1 != size) {
						importPixel.append(", ");
					}
					counter++;
				}
				importPixel.append(") | Limit(500) | Import();"); 
				recipeArray[3] = importPixel.toString();
				
				StringBuilder viewPixel = new StringBuilder("Frame() | Select(");
				counter = 0;
				for(String col : columnNames) {
					viewPixel.append("f$").append(col);
					if(counter + 1 != size) {
						viewPixel.append(", ");
					}
					counter++;
				}
				viewPixel.append(") | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[");
				counter = 0;
				for(String col : columnNames) {
					viewPixel.append("\"").append(col).append("\"");
					if(counter + 1 != size) {
						viewPixel.append(", ");
					}
					counter++;
				}
				viewPixel.append("]}}}) | Collect(500);"); 
				recipeArray[4] = viewPixel.toString();
				String id = admin.addInsight(insightName, layout, recipeArray);
				SecurityUpdateUtils.addInsight(rdbmsEngine.getEngineId(), id, insightName, false, layout);
			}
		} catch(RuntimeException e) {
			System.out.println("caught exception while adding question.................");
			e.printStackTrace();
		}
	}
	
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(IEngine rdbmsEngine) {
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
	
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(IEngine rdbmsEngine, Set<String> tablesToRetrieve) {
		// get the metadata from the connection
		RDBMSNativeEngine rdbms = (RDBMSNativeEngine) rdbmsEngine;
		DatabaseMetaData meta = rdbms.getConnectionMetadata();
		
		// table that will store 
		// table_name -> {
		// 					colname1 -> coltype,
		//					colname2 -> coltype,
		//				}
		Map<String, Map<String, String>> tableColumnMap = new Hashtable<String, Map<String, String>>();
		Set<String> tablesUpperCase = new HashSet<String>();
		for(String table: tablesToRetrieve) {
			tablesUpperCase.add(table.toUpperCase());
		}

		// get all the tables
		ResultSet tables = null;
		try {
			tables = meta.getTables(null, null, null, new String[]{"TABLE"});
			while(tables.next()) {
				// get the table name
				String table = tables.getString("table_name");
				if(!tablesUpperCase.contains(table)) {
					continue;
				}
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
