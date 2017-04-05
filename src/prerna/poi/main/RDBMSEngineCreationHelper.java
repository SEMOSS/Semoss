package prerna.poi.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.QuestionAdministrator;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.RDBMSUtility;
import prerna.util.sql.SQLQueryUtil;

public class RDBMSEngineCreationHelper {

	private RDBMSEngineCreationHelper() {
		
	}
	
	public static void writeDefaultQuestionSheet(String engineName, Set<String> tables) {
		// file location
		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
		String fileName = dbBaseFolder + System.getProperty("file.separator") + "db" + System.getProperty("file.separator") + engineName + System.getProperty("file.separator") + engineName + "_Questions.properties";

		Properties prop = new Properties();
		prop.put("PERSPECTIVE", "Generic-Perspective");
		String genericQueries = "GQ0";
		String questionName = "Explore an instance of a selected node type";
		String exploreQuery = "SELECT X.@Concept-Concept:Concept@ AS @Concept-Concept:Concept@ "
				+ "From @Concept-Concept:Concept@ X WHERE X.@Concept-Concept:Concept@='@Instance-Instance:Instance@'";
		String conceptParamQuery = "SELECT DISTINCT (COALESCE(?DisplayName, ?PhysicalName) AS ?entity) WHERE "
				+ " { {?PhysicalName <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
				+ " FILTER (?PhysicalName != <http://semoss.org/ontologies/Concept>) "
				+ " OPTIONAL{?PhysicalName <http://semoss.org/ontologies/DisplayName> ?DisplayName } }";
		String instanceParamQuery = "SELECT Distinct @Concept@ FROM @Concept@";
		
		prop.put("GQ0", questionName);
		prop.put("GQ0" + "_LAYOUT", "prerna.ui.components.playsheets.GraphPlaySheet");
		prop.put("GQ0" + "_QUERY", exploreQuery);
		prop.put("GQ0_Instance_DEPEND", "Concept");
		prop.put("GQ0_Concept_QUERY", conceptParamQuery);
		prop.put("GQ0_Concept_DB_QUERY", "FALSE");
		prop.put("GQ0_Instance_QUERY", instanceParamQuery);
		
//		List<DataMakerComponent> dmcList = new ArrayList<DataMakerComponent>();
//		DataMakerComponent dmc = new DataMakerComponent(rdbmsEngine, exploreQuery);
//		dmcList.add(dmc);
//		
//		List<SEMOSSParam> params = new ArrayList<SEMOSSParam>();
//		SEMOSSParam p1 = new SEMOSSParam();
//		p1.setName("Concept");
//		p1.setDbQuery(false);
//		p1.setQuery(conceptParamQuery);
//		params.add(p1);
//		SEMOSSParam p2 = new SEMOSSParam();
//		p2.setName("Instance");
//		p2.setDbQuery(true);
//		p2.setDepends("Concept");
//		p2.setQuery(instanceParamQuery);
//		params.add(p2);
//
//		questionAdmin.addQuestion(questionName, "Generic-Perspective", dmcList, "Graph", "0", "GraphDataModel", true, null, params, null);
		
		int questionOrder = 1;
		String layout = "prerna.ui.components.playsheets.GridPlaySheet";
		for(String tableName : tables)
		{
			genericQueries += ";" + "GQ" + questionOrder;		
			questionName = "Show all from " + tableName;
			String sql = "SELECT * FROM " + tableName;
			prop.put("GQ" + questionOrder, questionName);
			prop.put("GQ" + questionOrder + "_LAYOUT", layout);
			prop.put("GQ" + questionOrder + "_QUERY", sql);
			
//			dmcList = new ArrayList<DataMakerComponent>();
//			dmc = new DataMakerComponent(rdbmsEngine, sql);
//			dmcList.add(dmc);
//			questionAdmin.addQuestion(questionName, "Generic-Perspective", dmcList, layout, questionOrder + "", "TinkerFrame", true, null, null, null);
			
			questionOrder++;
		}
		prop.put("Generic-Perspective", genericQueries);

		FileOutputStream fo = null;
		try {
			File file = new File(fileName);
			fo = new FileOutputStream(file);
			prop.store(fo, "Questions for RDBMS");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fo != null) {
				try {
					fo.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public static void writeDefaultQuestionSheet(IEngine rdbmsEngine, SQLQueryUtil queryUtil)
	{		
//		QuestionAdministrator questionAdmin = new QuestionAdministrator(((AbstractEngine)rdbmsEngine));
		
		String engineName = rdbmsEngine.getEngineName();
		// get all the tables names in the database
		String getAllTablesQuery = "SHOW TABLES FROM PUBLIC";
		if(queryUtil != null)
			getAllTablesQuery = queryUtil.getDialectAllTables();
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(rdbmsEngine, getAllTablesQuery);
		String[] names = wrapper.getVariables();
		Set<String> tableNames = new HashSet<String>();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String tableName = ss.getVar("TABLE_NAME") + "";
			tableNames.add(tableName);
		}
		
		writeDefaultQuestionSheet(engineName, tableNames);
	}
	
	
	public static void addToExistingQuestionFile(IEngine rdbmsEngine, Set<String> newTables, SQLQueryUtil queryUtil) {
		
		QuestionAdministrator questionAdmin = new QuestionAdministrator(((AbstractEngine)rdbmsEngine));
		// get all the tables names in the database
		String getAllTablesQuery = "SHOW TABLES FROM PUBLIC";
		if(queryUtil != null)
			getAllTablesQuery = queryUtil.getDialectAllTables();
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(rdbmsEngine, getAllTablesQuery);
		String[] names = wrapper.getVariables();
		Set<String> tableNames = new HashSet<String>();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String tableName = ss.getVar("TABLE_NAME") + "";
			tableNames.add(tableName);
		}
		
		//determine the # where the new questions should start
		int newTableSeq = tableNames.size() - newTables.size();
		newTableSeq = newTableSeq + 1; //we need to add 1 to the question order to account for  the explore a concept question
		String order = "";
		String question = ""; 
		String sql = ""; 
		String layout = ""; 

		try {
			for(String newTable : newTables) {
				order = Integer.toString(newTableSeq+1);
				String cleanTableName = cleanTableName(newTable);
				question = "Show all from " + cleanTableName;
				layout = "Grid";
				sql = "SELECT * FROM " + cleanTableName;
				List<DataMakerComponent> dmcList = new ArrayList<DataMakerComponent>();
				DataMakerComponent dmc = new DataMakerComponent(rdbmsEngine, sql);
				dmcList.add(dmc);
				questionAdmin.addQuestion(question, "Generic-Perspective", dmcList, layout, order, "TinkerFrame", true, null, null, null);
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
	
	public static String writePropFile(String engineName, SQLQueryUtil queryUtil)
	{
		Properties prop = new Properties();
		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
		
		if(queryUtil.getDatabaseType().equals(SQLQueryUtil.DB_TYPE.H2_DB)) {
			prop.put(Constants.CONNECTION_URL, RDBMSUtility.getH2BaseConnectionURL());			
		} else {
			prop.put(Constants.CONNECTION_URL, queryUtil.getConnectionURL(dbBaseFolder,engineName));
		}
		prop.put(Constants.ENGINE, engineName);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER,queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.TEMP_CONNECTION_URL, queryUtil.getTempConnectionURL());
		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());
		prop.put("TEMP", "TRUE");

		// write this to a file
		String tempFile = dbBaseFolder + "/db/" + engineName + "/conn.prop";
		File file = null;
		FileOutputStream fo = null;
		try {
			file = new File(tempFile);
			file.createNewFile();
			fo = new FileOutputStream(file);
			prop.store(fo, "Temporary Properties file for the RDBMS");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fo != null) {
				try {
					fo.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return tempFile;
	}
}
