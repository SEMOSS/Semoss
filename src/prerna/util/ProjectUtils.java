package prerna.util;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class ProjectUtils {
	
	private static final Logger classLogger = LogManager.getLogger(ProjectUtils.class);
	
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
	 * @throws Exception 
	 */
	public static RDBMSNativeEngine generateInsightsDatabase(String projectId, String projectName) throws Exception {
		String rdbmsTypeStr = DIHelper.getInstance().getProperty(Constants.DEFAULT_INSIGHTS_RDBMS);
		if(rdbmsTypeStr == null) {
			// default will be h2
			rdbmsTypeStr = "H2_DB";
		}
		RdbmsTypeEnum rdbmsType = RdbmsTypeEnum.valueOf(rdbmsTypeStr);

		Properties insightSmssProp = new Properties();
		/*
		 * This must be either H2 or SQLite
		 */
		insightSmssProp.putAll(getNewInsightDatabaseConnectionPropValues(rdbmsType, projectId, projectName));
		RDBMSNativeEngine insightEngine = new RDBMSNativeEngine();
		insightEngine.setBasic(true);
		insightEngine.open(insightSmssProp);
		insightEngine.setEngineId(projectId + Constants.RDBMS_INSIGHTS_ENGINE_SUFFIX);

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
		final String TIMESTAMP_DATATYPE = queryUtil.getDateWithTimeDataType();
		
		try {
			if(!queryUtil.tableExists(insightEngine.getConnection(), InsightAdministrator.TABLE_NAME, insightEngine.getDatabase(), insightEngine.getSchema())) {
				columns = new String[]{InsightAdministrator.QUESTION_ID_COL, InsightAdministrator.QUESTION_NAME_COL, "QUESTION_PERSPECTIVE", 
						"QUESTION_LAYOUT", "QUESTION_ORDER", "QUESTION_DATA_MAKER", "QUESTION_MAKEUP", "DATA_TABLE_ALIGN", 
						InsightAdministrator.HIDDEN_INSIGHT_COL, InsightAdministrator.CACHEABLE_COL, InsightAdministrator.CACHE_MINUTES_COL,
						InsightAdministrator.CACHE_CRON_COL, InsightAdministrator.CACHED_ON_COL, InsightAdministrator.CACHE_ENCRYPT_COL,
						InsightAdministrator.QUESTION_PKQL_COL, InsightAdministrator.SCHEMA_NAME_COL
					};
				types = new String[]{"VARCHAR(50)", "VARCHAR(255)", "VARCHAR(255)", 
						"VARCHAR(255)", "INT", "VARCHAR(255)", CLOB_DATATYPE, "VARCHAR(500)", 
						BOOLEAN_DATATYPE, BOOLEAN_DATATYPE, "INT", 
						"VARCHAR(25)", TIMESTAMP_DATATYPE, BOOLEAN_DATATYPE, 
						"ARRAY", "VARCHAR(255)"
					};
				// this is annoying
				// need to adjust if the engine allows array data types
				if(!queryUtil.allowArrayDatatype()) {
					types[types.length-1] = CLOB_DATATYPE;
				}

				insightEngine.insertData(queryUtil.createTable("QUESTION_ID", columns, types));
			}

			// adding new insight metadata
			if(!queryUtil.tableExists(insightEngine.getConnection(), "INSIGHTMETA", insightEngine.getDatabase(), insightEngine.getSchema())) {
				columns = new String[] { "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"};
				types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE, "INT"};
				insightEngine.insertData(queryUtil.createTable("INSIGHTMETA", columns, types));
			}

		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
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
				if(!queryUtil.tableExists(insightEngine.getConnection(), "PARAMETER_ID", insightEngine.getDatabase(), insightEngine.getSchema())) {
					columns = new String[]{"PARAMETER_ID", "PARAMETER_LABEL", "PARAMETER_TYPE", "PARAMETER_DEPENDENCY", "PARAMETER_QUERY", 
							"PARAMETER_OPTIONS", "PARAMETER_IS_DB_QUERY", "PARAMETER_MULTI_SELECT", "PARAMETER_COMPONENT_FILTER_ID", "PARAMETER_VIEW_TYPE", "QUESTION_ID_FK"};
					types = new String[]{"VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(2000)", "VARCHAR(2000)", BOOLEAN_DATATYPE,
							BOOLEAN_DATATYPE, "VARCHAR(255)", "VARCHAR(255)", "INT"};
					insightEngine.insertData(queryUtil.createTable("PARAMETER_ID", columns, types));
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}

			try {
				if(!queryUtil.tableExists(insightEngine.getConnection(), "UI", insightEngine.getDatabase(), insightEngine.getSchema())) {
					columns = new String[]{"QUESTION_ID_FK", "UI_DATA"};
					types = new String[]{"INT", CLOB_DATATYPE};
					insightEngine.insertData(queryUtil.createTable("UI", columns, types));
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
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
	public static final Map<String, Object> getNewInsightDatabaseConnectionPropValues(RdbmsTypeEnum rdbmsType, String projectId, String projectName) {
		Map<String, Object> retMap = new HashMap<>();
		retMap.put(Constants.DRIVER, rdbmsType.getDriver());
		retMap.put(Constants.RDBMS_TYPE, rdbmsType.getLabel());
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		if(baseFolder.endsWith("/") || baseFolder.endsWith("\\")) {
			baseFolder += Constants.PROJECT_FOLDER + DIR_SEPARATOR;
		} else {
			baseFolder += PROJECT_DIRECTORY;
		}
		
		String hostname = null;
		
		if(rdbmsType == RdbmsTypeEnum.SQLITE) {
			// sqlite has no username/password
			// append .sqlite so it looks nicer - realize it is not required
			hostname = baseFolder + SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + "insights_database.sqlite";
			
			retMap.put(AbstractSqlQueryUtil.USERNAME, "");
			retMap.put(AbstractSqlQueryUtil.PASSWORD, "");
		} else {
			// h2 only has username
			hostname = baseFolder + SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + "insights_database.mv.db";
			
			retMap.put(AbstractSqlQueryUtil.USERNAME, "sa");
			retMap.put(AbstractSqlQueryUtil.PASSWORD, "");
			retMap.put(AbstractSqlQueryUtil.FORCE_FILE, true);
			retMap.put(AbstractSqlQueryUtil.ADDITIONAL, "query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768");
		}
		
		// regardless of OS, connection url is always /
		hostname = hostname.replace('\\', '/');
		retMap.put(AbstractSqlQueryUtil.HOSTNAME, hostname);
		
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(rdbmsType);
		String connectionUrl = queryUtil.setConnectionDetailsfromMap(retMap);
		retMap.put(AbstractSqlQueryUtil.CONNECTION_URL, connectionUrl);
		return retMap;
	}
	
	/**
	 * Generate an empty insight database
	 * @param appName
	 * @return
	 * @throws Exception 
	 */
	public static RDBMSNativeEngine generateInsightsDatabase(String projectId, RdbmsTypeEnum rdbmsType, String folderLocation) throws Exception {
		if(rdbmsType == null) {
			String rdbmsTypeStr = DIHelper.getInstance().getProperty(Constants.DEFAULT_INSIGHTS_RDBMS);
			if(rdbmsTypeStr == null) {
				// default will be h2
				rdbmsTypeStr = "H2_DB";
			}
			rdbmsType = RdbmsTypeEnum.valueOf(rdbmsTypeStr);
		}
		
		Properties insightSmssProp = new Properties();
		/*
		 * This must be either H2 or SQLite
		 */
		insightSmssProp.putAll(getNewInsightDatabaseConnectionPropValues(rdbmsType, folderLocation));
		RDBMSNativeEngine insightEngine = new RDBMSNativeEngine();
		insightEngine.setBasic(true);
		insightEngine.open(insightSmssProp);
		insightEngine.setEngineId(projectId + Constants.RDBMS_INSIGHTS_ENGINE_SUFFIX);

		runInsightCreateTableQueries(insightEngine);
		return insightEngine;
	}
	
	/**
	 * 
	 * @param rdbmsType
	 * @param folderLocation
	 * @return
	 */
	public static final Map<String, Object> getNewInsightDatabaseConnectionPropValues(RdbmsTypeEnum rdbmsType, String folderLocation) {
		Map<String, Object> retMap = new HashMap<>();
		retMap.put(Constants.DRIVER, rdbmsType.getDriver());
		retMap.put(Constants.RDBMS_TYPE, rdbmsType.getLabel());
		retMap.put("TEMP", "TRUE");
		
		folderLocation = Utility.normalizePath(folderLocation);
		
		String hostname = null;
		
		if(rdbmsType == RdbmsTypeEnum.SQLITE) {
			// sqlite has no username/password
			// append .sqlite so it looks nicer - realize it is not required
			hostname = folderLocation + DIR_SEPARATOR + "insights_database.sqlite";
			
			retMap.put(AbstractSqlQueryUtil.USERNAME, "");
			retMap.put(AbstractSqlQueryUtil.PASSWORD, "");
		} else {
			// h2 only has username
			hostname = folderLocation + DIR_SEPARATOR + "insights_database.mv.db";
			
			retMap.put(AbstractSqlQueryUtil.USERNAME, "sa");
			retMap.put(AbstractSqlQueryUtil.PASSWORD, "");
			retMap.put(AbstractSqlQueryUtil.FORCE_FILE, true);
			retMap.put(AbstractSqlQueryUtil.ADDITIONAL, "query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768");
		}
		
		// regardless of OS, connection url is always /
		hostname = hostname.replace('\\', '/');
		retMap.put(AbstractSqlQueryUtil.HOSTNAME, hostname);
		
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(rdbmsType);
		String connectionUrl = queryUtil.setConnectionDetailsfromMap(retMap);
		retMap.put(AbstractSqlQueryUtil.CONNECTION_URL, connectionUrl);
		return retMap;
	}

}
