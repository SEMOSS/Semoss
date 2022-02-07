package prerna.util;

import java.sql.SQLException;
import java.util.Properties;

import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class ProjectUtils {
	
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String PROJECT_DIRECTORY;
	static {
		PROJECT_DIRECTORY = DIR_SEPARATOR + Constants.PROJECT_FOLDER + DIR_SEPARATOR;
	}

	//	public static void importFromEngine(String engineId) {
	//
	//	}


	/**
	 * Generate an empty insight database
	 * @param appName
	 * @return
	 */
	public static RDBMSNativeEngine generateInsightsDatabase(String projectId, String projectName) {
		String rdbmsTypeStr = DIHelper.getInstance().getProperty(Constants.DEFAULT_INSIGHTS_RDBMS);
		if(rdbmsTypeStr == null) {
			// default will be h2
			rdbmsTypeStr = "H2_DB";
		}
		RdbmsTypeEnum rdbmsType = RdbmsTypeEnum.valueOf(rdbmsTypeStr);

		Properties prop = new Properties();

		/*
		 * This must be either H2 or SQLite
		 */

		String connectionUrl = getNewInsightDatabaseConnectionUrl(rdbmsType, projectId, projectName);
		prop.put(Constants.CONNECTION_URL, connectionUrl);
		if(rdbmsType == RdbmsTypeEnum.SQLITE) {
			// sqlite has no username/password
			prop.put(Constants.USERNAME, "");
			prop.put(Constants.PASSWORD, "");
		} else {
			prop.put(Constants.USERNAME, "sa");
			prop.put(Constants.PASSWORD, "");
		}
		prop.put(Constants.DRIVER, rdbmsType.getDriver());
		prop.put(Constants.RDBMS_TYPE, rdbmsType.getLabel());
		prop.put("TEMP", "TRUE");
		RDBMSNativeEngine insightEngine = new RDBMSNativeEngine();
		insightEngine.setProp(prop);
		// opening will work since we directly injected the prop map
		// this way i do not need to write it to disk and then recreate it later
		insightEngine.openDB(null);
		insightEngine.setBasic(true);

		runInsightCreateTableQueries(insightEngine);
		return insightEngine;
	}




	/**
	 * Run the create table queries for the insights database
	 * @param insightEngine
	 */
	public static void runInsightCreateTableQueries(RDBMSNativeEngine insightEngine) {
		// CREATE TABLE QUESTION_ID (ID VARCHAR(50), QUESTION_NAME VARCHAR(255), QUESTION_PERSPECTIVE VARCHAR(225), QUESTION_LAYOUT VARCHAR(225), QUESTION_ORDER INT, QUESTION_DATA_MAKER VARCHAR(225), QUESTION_MAKEUP CLOB, QUESTION_PROPERTIES CLOB, QUESTION_OWL CLOB, QUESTION_IS_DB_QUERY BOOLEAN, DATA_TABLE_ALIGN VARCHAR(500), QUESTION_PKQL ARRAY)
		AbstractSqlQueryUtil queryUtil = insightEngine.getQueryUtil();
		String[] columns = null;
		String[] types = null;
		final String BOOLEAN_DATATYPE = queryUtil.getBooleanDataTypeName();
		final String CLOB_DATATYPE = queryUtil.getClobDataTypeName();
		
		try {
			if(!queryUtil.tableExists(insightEngine.getConnection(), "QUESTION_ID", insightEngine.getSchema())) {
				columns = new String[]{"ID", "QUESTION_NAME", "QUESTION_PERSPECTIVE", "QUESTION_LAYOUT", "QUESTION_ORDER", 
						"QUESTION_DATA_MAKER", "QUESTION_MAKEUP", "DATA_TABLE_ALIGN", "HIDDEN_INSIGHT", 
						"CACHEABLE", "CACHE_MINUTES", "CACHE_ENCRYPT", "QUESTION_PKQL"};
				types = new String[]{"VARCHAR(50)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "INT", 
						"VARCHAR(255)", CLOB_DATATYPE, "VARCHAR(500)", BOOLEAN_DATATYPE, 
						BOOLEAN_DATATYPE, "INT", BOOLEAN_DATATYPE, "ARRAY"};
				// this is annoying
				// need to adjust if the engine allows array data types
				if(!queryUtil.allowArrayDatatype()) {
					types[types.length-1] = CLOB_DATATYPE;
				}

				insightEngine.insertData(queryUtil.createTable("QUESTION_ID", columns, types));
			}

			// adding new insight metadata
			if(!queryUtil.tableExists(insightEngine.getConnection(), "INSIGHTMETA", insightEngine.getSchema())) {
				columns = new String[] { "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"};
				types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE, "INT"};
				insightEngine.insertData(queryUtil.createTable("INSIGHTMETA", columns, types));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		/*
		 * NOTE : THESE TABLES ARE LEGACY!!!!
		 */

		{
			/*
			 * Whenever we are finally done / have removed all our playsheet insights
			 * We can officially delete this portion
			 */

			// CREATE TABLE PARAMETER_ID (PARAMETER_ID VARCHAR(255), PARAMETER_LABEL VARCHAR(255), PARAMETER_TYPE VARCHAR(225), PARAMETER_DEPENDENCY VARCHAR(225), PARAMETER_QUERY VARCHAR(2000), PARAMETER_OPTIONS VARCHAR(2000), PARAMETER_IS_DB_QUERY BOOLEAN, PARAMETER_MULTI_SELECT BOOLEAN, PARAMETER_COMPONENT_FILTER_ID VARCHAR(255), PARAMETER_VIEW_TYPE VARCHAR(255), QUESTION_ID_FK INT)

			try {
				if(!queryUtil.tableExists(insightEngine.getConnection(), "PARAMETER_ID", insightEngine.getSchema())) {
					columns = new String[]{"PARAMETER_ID", "PARAMETER_LABEL", "PARAMETER_TYPE", "PARAMETER_DEPENDENCY", "PARAMETER_QUERY", 
							"PARAMETER_OPTIONS", "PARAMETER_IS_DB_QUERY", "PARAMETER_MULTI_SELECT", "PARAMETER_COMPONENT_FILTER_ID", "PARAMETER_VIEW_TYPE", "QUESTION_ID_FK"};
					types = new String[]{"VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(2000)", "VARCHAR(2000)", BOOLEAN_DATATYPE,
							BOOLEAN_DATATYPE, "VARCHAR(255)", "VARCHAR(255)", "INT"};
					insightEngine.insertData(queryUtil.createTable("PARAMETER_ID", columns, types));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

			try {
				if(!queryUtil.tableExists(insightEngine.getConnection(), "UI", insightEngine.getSchema())) {
					columns = new String[]{"QUESTION_ID_FK", "UI_DATA"};
					types = new String[]{"INT", CLOB_DATATYPE};
					insightEngine.insertData(queryUtil.createTable("UI", columns, types));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}


		insightEngine.commit();
	}



	/**
	 * Get the default connection url for the insights database
	 * NOTE : ONLY ALLOWING FOR H2 OR SQLITE STORAGE OPTIONS AT THE MOMENT
	 * NOTE : THIS IS THE ACTUAL FULL CONNECITON URL
	 * TODO: expand how we store this information to be able to keep in another database option / shared database
	 * @param appName
	 * @return
	 */
	private static String getNewInsightDatabaseConnectionUrl(RdbmsTypeEnum rdbmsType, String projectId, String projectName) {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);

		String connectionUrl = null;
		if(rdbmsType == RdbmsTypeEnum.SQLITE) {
			// append .sqlite so it looks nicer - realize it is not required
			connectionUrl = "jdbc:sqlite:" + baseFolder + PROJECT_DIRECTORY + SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + "insights_database.sqlite";
		} else {
			connectionUrl = "jdbc:h2:nio:" + baseFolder + PROJECT_DIRECTORY + SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + "insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		}
		// regardless of OS, connection url is always /
		connectionUrl = connectionUrl.replace('\\', '/');
		return connectionUrl;
	}


}
