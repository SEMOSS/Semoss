package prerna.engine.impl;

import java.io.File;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class EngineInsightsHelper {

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private EngineInsightsHelper() {
		
	}
	
	/**
	 * Load the insights rdbms engine using the main engine properties
	 * @param mainEngineProp
	 * @return
	 */
	public static RDBMSNativeEngine loadInsightsEngine(Properties mainEngineProp, Logger logger) {
		String engineId = mainEngineProp.getProperty(Constants.ENGINE);
		String engineName = mainEngineProp.getProperty(Constants.ENGINE_ALIAS);

		String rdbmsInsightsTypeStr = mainEngineProp.getProperty(Constants.RDBMS_INSIGHTS_TYPE, "H2_DB");
		RdbmsTypeEnum rdbmsInsightsType = RdbmsTypeEnum.valueOf(rdbmsInsightsTypeStr);
		String insightDatabaseLoc = SmssUtilities.getInsightsRdbmsFile(mainEngineProp).getAbsolutePath();
		return loadInsightsEngine(engineId, engineName, rdbmsInsightsType, insightDatabaseLoc, logger);
	}
	
	/**
	 * Load the insights rdbms engine
	 * @param engineId
	 * @param engineName
	 * @param rdbmsInsightsType
	 * @param insightDatabaseLoc
	 * @param logger
	 * @return
	 */
	private static RDBMSNativeEngine loadInsightsEngine(String engineId, String engineName, RdbmsTypeEnum rdbmsInsightsType, String insightDatabaseLoc, Logger logger) {
		if(insightDatabaseLoc == null || !new File(insightDatabaseLoc).exists()) {
			// make a new database
			RDBMSNativeEngine insightsRdbms = (RDBMSNativeEngine) UploadUtilities.generateInsightsDatabase(engineId, engineName);
			UploadUtilities.addExploreInstanceInsight(engineId, engineName, insightsRdbms);
			return insightsRdbms;
		}
		RDBMSNativeEngine insightsRdbms = new RDBMSNativeEngine();
		Properties prop = new Properties();
		prop.put(Constants.DRIVER, rdbmsInsightsType.getDriver());
		prop.put(Constants.RDBMS_TYPE, rdbmsInsightsType.getLabel());
		String connURL = null;
		logger.info("Insight rdbms database location is " + Utility.cleanLogString(insightDatabaseLoc));
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		// decrypt the password
		String propFile = baseFolder + java.nio.file.FileSystems.getDefault().getSeparator() + "db" + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId) + ".smss";

		String pass = insightsRdbms.decryptPass(propFile, true);
		if(pass == null) {
			pass = "";
		}
		
		if(rdbmsInsightsType == RdbmsTypeEnum.SQLITE) {
			connURL = rdbmsInsightsType.getUrlPrefix() + ":" + insightDatabaseLoc;
			prop.put(Constants.USERNAME, "");
			prop.put(Constants.PASSWORD, pass);
		} else {
			connURL = rdbmsInsightsType.getUrlPrefix() + ":nio:" + insightDatabaseLoc.replace(".mv.db", "");
			prop.put(Constants.USERNAME, "sa");
			prop.put(Constants.PASSWORD, pass);
		}
		logger.info("Insight rdbms database url is " + Utility.cleanLogString(connURL));
		prop.put(Constants.CONNECTION_URL, connURL);

		insightsRdbms.setProp(prop);
		insightsRdbms.openDB(null);
		insightsRdbms.setEngineId(engineId + "_INSIGHTS_RDBMS");
		
		AbstractSqlQueryUtil queryUtil = insightsRdbms.getQueryUtil();
		String tableExistsQuery = queryUtil.tableExistsQuery("QUESTION_ID", insightsRdbms.getSchema());
		boolean tableExists = false;
		IRawSelectWrapper wrapper = null;
		try {
			wrapper  = WrapperManager.getInstance().getRawWrapper(insightsRdbms, tableExistsQuery);
			tableExists = wrapper.hasNext();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		if(!tableExists) {
			// well, you already created the file
			// need to run the queries to make this
			UploadUtilities.runInsightCreateTableQueries(insightsRdbms);
		} else {
			
			// adding new insight metadata
			try {
				if(!queryUtil.tableExists(insightsRdbms.getConnection(), "INSIGHTMETA", insightsRdbms.getSchema())) {
					String[] columns = new String[] { "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"};
					String[] types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "CLOB", "INT"};
					try {
						insightsRdbms.insertData(queryUtil.createTable("INSIGHTMETA", columns, types));
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
//			// okay, might need to do some updates
//			String q = "SELECT TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='QUESTION_ID' and COLUMN_NAME='ID'";
//			IRawSelectWrapper wrap = WrapperManager.getInstance().getRawWrapper(insightsRdbms, q);
//			while(wrap.hasNext()) {
//				String val = wrap.next().getValues()[0] + "";
//				if(!val.equals("VARCHAR")) {
//					String update = "ALTER TABLE QUESTION_ID ALTER COLUMN ID VARCHAR(50);";
//					try {
//						insightsRdbms.insertData(update);
//					} catch (SQLException e) {
//						e.printStackTrace();
//					}
//					insightsRdbms.commit();
//				}
//			}
//			wrap.cleanUp();
//			
//			// previous alter column ... might be time to delete this ? 11/8/2018 
//			String update = "ALTER TABLE QUESTION_ID ADD COLUMN IF NOT EXISTS HIDDEN_INSIGHT BOOLEAN DEFAULT FALSE";								
//			try {
//				insightsRdbms.insertData(update);
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//			insightsRdbms.commit();
//
//			// THIS IS FOR LEGACY !!!!
//			// TODO: EVENTUALLY WE WILL DELETE THIS
//			// TODO: EVENTUALLY WE WILL DELETE THIS
//			// TODO: EVENTUALLY WE WILL DELETE THIS
//			// TODO: EVENTUALLY WE WILL DELETE THIS
//			// TODO: EVENTUALLY WE WILL DELETE THIS
//			InsightsDatabaseUpdater3CacheableColumn.update(engineId, insightsRdbms);
			
		}
		
		insightsRdbms.setBasic(true);
		return insightsRdbms;
	}
	
}
