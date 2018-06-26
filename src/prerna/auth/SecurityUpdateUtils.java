package prerna.auth;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SecurityUpdateUtils extends AbstractSecurityUtils {

	private static final Logger LOGGER = Logger.getLogger(SecurityUpdateUtils.class);
	
	/**
	 * Only used for static references
	 */
	private SecurityUpdateUtils() {
		
	}
	
	/**
	 * Add an entire engine into the security db
	 * @param appId
	 */
	public static void addApp(String appId) {
		if(appId.equals(Constants.LOCAL_MASTER_DB_NAME) || appId.equals(Constants.SECURITY_DB)) {
			// dont add local master or security db to security db
			return;
		}
		// does security db contain the engine
		if(containsEngine(appId)) {
			LOGGER.info("Security DB already contains app");
			return;
		}
		
		String smssFile = DIHelper.getInstance().getCoreProp().getProperty(appId + "_" + Constants.STORE);
		Properties prop = Utility.loadProperties(smssFile);
		String appName = prop.getProperty(Constants.ENGINE_ALIAS);
		if(appName == null) {
			appName = appId;
		}
		
		// need to add engine into the security db
		String[] typeAndCost = getAppTypeAndCost(prop);
		addEngine(appId, appName, typeAndCost[0], typeAndCost[1], true);
		
		File dbfile = SmssUtilities.getInsightsRdbmsFile(prop);
		String dbLocation = dbfile.getAbsolutePath();
		String jdbcURL = "jdbc:h2:" + dbLocation.replace(".mv.db", "") + ";query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		String userName = "sa";
		String password = "";
		RDBMSNativeEngine rne = new RDBMSNativeEngine();
		rne.makeConnection(jdbcURL, userName, password);
		
		// make a prepared statement
		PreparedStatement ps = securityDb.bulkInsertPreparedStatement(new String[]{"INSIGHT","ENGINEID","INSIGHTID","INSIGHTNAME","GLOBAL"});
		// keep a batch size so we dont get heapspace
		final int batchSize = 5000;
		int count = 0;
					
		String query = "SELECT DISTINCT ID, QUESTION_NAME FROM QUESTION_ID";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rne, query);
		while(wrapper.hasNext()) {
			Object[] row = wrapper.next().getValues();
			try {
				ps.setString(1, appId);
				ps.setString(2, row[0].toString());
				ps.setString(3, row[1].toString());
				ps.setBoolean(4, true);
				ps.addBatch();
				
				// batch commit based on size
				if (++count % batchSize == 0) {
					LOGGER.info("Executing batch .... row num = " + count);
					ps.executeBatch();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		// well, we are done looping through now
		LOGGER.info("Executing final batch .... row num = " + count);
		try {
			ps.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		} // insert any remaining records
		try {
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Utility method to get the engine type and cost for storage
	 * @param prop
	 * @return
	 */
	public static String[] getAppTypeAndCost(Properties prop) {
		String app_type = null;
		String app_cost = null;
		// the whole app cost stuff is completely made up...
		// but it will look cool so we are doing it
		String eType = prop.getProperty(Constants.ENGINE_TYPE);
		if(eType.equals("prerna.engine.impl.rdbms.RDBMSNativeEngine")) {
			String rdbmsType = prop.getProperty(Constants.RDBMS_TYPE);
			if(rdbmsType == null) {
				rdbmsType = "H2_DB";
			}
			rdbmsType = rdbmsType.toUpperCase();
			app_type = rdbmsType;
			if(rdbmsType.equals("TERADATA") || rdbmsType.equals("DB2")) {
				app_cost = "$$";
			} else {
				app_cost = "";
			}
		} else if(eType.equals("prerna.engine.impl.rdbms.ImpalaEngine")) {
			app_type = "IMPALA";
			app_cost = "$$$";
		} else if(eType.equals("prerna.engine.impl.rdf.BigDataEngine")) {
			app_type = "RDF";
			app_cost = "";
		} else if(eType.equals("prerna.engine.impl.rdf.RDFFileSesameEngine")) {
			app_type = "RDF";
			app_cost = "";
		} else if(eType.equals("prerna.ds.datastax.DataStaxGraphEngine")) {
			app_type = "DATASTAX";
			app_cost = "$$$";
		} else if(eType.equals("prerna.engine.impl.solr.SolrEngine")) {
			app_type = "SOLR";
			app_cost = "$$";
		} else if(eType.equals("prerna.engine.impl.tinker.TinkerEngine")) {
			String tinkerDriver = prop.getProperty(Constants.TINKER_DRIVER);
			if(tinkerDriver.equalsIgnoreCase("neo4j")) {
				app_type = "NEO4J";
				app_cost = "";
			} else {
				app_type = "TINKER";
				app_cost = "";
			}
		} else if(eType.equals("prerna.engine.impl.json.JsonAPIEngine") || eType.equals("prerna.engine.impl.json.JsonAPIEngine2")) {
			app_type = "JSON";
			app_cost = "";
		} else if(eType.equals("prerna.engine.impl.app.AppEngine")) {
			app_type = "APP";
			app_cost = "$";
		}
		
		return new String[]{app_type, app_cost};
	}
	
	public static void deleteApp(String appId) {
		String deleteQuery = "DELETE FROM ENGINE WHERE ID='" + appId + "'";
		securityDb.removeData(deleteQuery);
		deleteQuery = "DELETE FROM INSIGHT WHERE ENGINEID='" + appId + "'";
		securityDb.removeData(deleteQuery);
		deleteQuery = "DELETE FROM ENGINEPERMISSION WHERE ENGINE='" + appId + "'";
		securityDb.removeData(deleteQuery);
		deleteQuery = "DELETE FROM ENGINEMETA WHERE ENGINE='" + appId + "'";
		securityDb.removeData(deleteQuery);
	}
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Adding data
	 */
	
	/**
	 * Add an engine into the security database
	 * Default to set as not global
	 */
	public static void addEngine(String engineId, String engineName, String engineType, String engineCost) {
		addEngine(engineId, engineName, engineType, engineCost, false);
	}
	
	public static void addEngine(String engineId, String engineName, String engineType, String engineCost, boolean global) {
		String query = "INSERT INTO ENGINE (NAME, ID, TYPE, COST, GLOBAL) "
				+ "VALUES ('" + engineName + "', '" + engineId + "', '" + engineType + "', '" + engineCost + "', " + global + ")";
		securityDb.insertData(query);
		securityDb.commit();
	}
	
	/**
	 * Add an insight into the security db
	 * @param engineId
	 * @param insightId
	 * @param insightName
	 * @param global
	 */
	public static void addInsight(String engineId, String insightId, String insightName, boolean global) {
		String query = "INSERT INTO INSIGHT (ENGINEID, INSIGHTID, INSIGHTNAME, GLOBAL) "
				+ "VALUES ('" + engineId + "', '" + insightId + "', '" + insightName + "', " + global + ")";
		securityDb.insertData(query);
		securityDb.commit();
	}
	
	/**
	 * 
	 * @param appId
	 * @param metaValues
	 * @param metaType
	 * @throws SQLException
	 */
	public static void setEngineMeta(String engineId, String metaType, List<String> metaValues) {
		metaType = metaType.toLowerCase();
		// first delete existing values
		String deleteQuery = "DELETE FROM ENGINEMETA WHERE ENGINEID='" + engineId + "' AND KEY='" + metaType + "'";
		securityDb.removeData(deleteQuery);
		// second set new values
		try {
			PreparedStatement ps = securityDb.bulkInsertPreparedStatement(new Object[]{"ENGINEMETA", "ENGINEID", "KEY", "VALUE"});
			boolean added = false;
			for (String val : metaValues) {
				if(val == null || val.isEmpty()) {
					continue;
				}
				ps.setString(1, engineId);
				ps.setString(2, metaType);
				ps.setString(3, val);
				ps.addBatch();
				added = true;
			}
			if(added) {
				ps.executeBatch();
			}
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
