package prerna.auth;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
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
		if(ignoreEngine(appId)) {
			// dont add local master or security db to security db
			return;
		}
		String smssFile = DIHelper.getInstance().getCoreProp().getProperty(appId + "_" + Constants.STORE);
		Properties prop = Utility.loadProperties(smssFile);
		
		boolean reloadInsights = false;
		if(prop.containsKey(Constants.RELOAD_INSIGHTS)) {
			String booleanStr = prop.get(Constants.RELOAD_INSIGHTS).toString();
			reloadInsights = Boolean.parseBoolean(booleanStr);
		}
		
		boolean global = true;
		if(prop.containsKey(Constants.HIDDEN_DATABASE) && "true".equalsIgnoreCase(prop.get(Constants.HIDDEN_DATABASE).toString().trim()) ) {
			global = false;
		}
		
		boolean engineExists = containsEngineId(appId);
		if(engineExists && !reloadInsights) {
			LOGGER.info("Security database already contains app");
			return;
		} else if(engineExists) {
			// delete values if currently present
			deleteInsightsForRecreation(appId);
		}
		
		String appName = prop.getProperty(Constants.ENGINE_ALIAS);
		if(appName == null) {
			appName = appId;
		}
		
		// need to add engine into the security db
		if(!engineExists) {
			String[] typeAndCost = getAppTypeAndCost(prop);
			addEngine(appId, appName, typeAndCost[0], typeAndCost[1], global);
		} else {
			//TODO: do i need to update the global tag ???
		}
		
		File dbfile = SmssUtilities.getInsightsRdbmsFile(prop);
		String dbLocation = dbfile.getAbsolutePath();
		String jdbcURL = "jdbc:h2:" + dbLocation.replace(".mv.db", "") + ";query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		String userName = "sa";
		String password = "";
		RDBMSNativeEngine rne = new RDBMSNativeEngine();
		rne.makeConnection(jdbcURL, userName, password);
		
		// make a prepared statement
		PreparedStatement ps = securityDb.bulkInsertPreparedStatement(
				new String[]{"INSIGHT","ENGINEID","INSIGHTID","INSIGHTNAME","GLOBAL","EXECUTIONCOUNT", "CREATEDON","LASTMODIFIEDON","LAYOUT"});
		// keep a batch size so we dont get heapspace
		final int batchSize = 5000;
		int count = 0;
		
		LocalDateTime now = LocalDateTime.now();
		
		String query = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_LAYOUT FROM QUESTION_ID";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rne, query);
		while(wrapper.hasNext()) {
			Object[] row = wrapper.next().getValues();
			try {
				ps.setString(1, appId);
				ps.setString(2, row[0].toString());
				ps.setString(3, row[1].toString());
				ps.setBoolean(4, true);
				ps.setLong(5, 0);
				ps.setTimestamp(6, java.sql.Timestamp.valueOf(now));
				ps.setTimestamp(7, java.sql.Timestamp.valueOf(now));
				ps.setString(8, row[2].toString());
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
		LOGGER.info("Finished adding engine = " + appId);
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
	
	/**
	 * Delete just the insights for an engine
	 * @param appId
	 */
	public static void deleteInsightsForRecreation(String appId) {
		String deleteQuery = "DELETE FROM INSIGHT WHERE ENGINEID='" + appId + "'";
		securityDb.removeData(deleteQuery);
	}
	
	/**
	 * Delete all values
	 * @param appId
	 */
	public static void deleteApp(String appId) {
		if(ignoreEngine(appId)) {
			// dont add local master or security db to security db
			return;
		}
		String deleteQuery = "DELETE FROM ENGINE WHERE ID='" + appId + "'";
		securityDb.removeData(deleteQuery);
		deleteQuery = "DELETE FROM INSIGHT WHERE ENGINEID='" + appId + "'";
		securityDb.removeData(deleteQuery);
		deleteQuery = "DELETE FROM ENGINEPERMISSION WHERE ENGINE='" + appId + "'";
		securityDb.removeData(deleteQuery);
		deleteQuery = "DELETE FROM ENGINEMETA WHERE ENGINE='" + appId + "'";
		securityDb.removeData(deleteQuery);
		
		//TODO: add the other tables...
	}
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Adding engine
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
	 * Adds user as owner for a given engine, giving him/her all permissions.
	 * 
	 * @param engineName	Name of engine user is being added as owner for
	 * @param userId		ID of user being made owner
	 * @return				true or false for successful addition
	 */
	public static Boolean addEngineAndOwner(String engineId, String engineName, String userId) {
		//Add the engine to the ENGINE table
		//String engineID = UUID.randomUUID().toString();
		String query = "INSERT INTO Engine(NAME, ID) VALUES ('" + engineName + "','" + engineId + "');";
		Statement stmt = securityDb.execUpdateAndRetrieveStatement(query, false);
		int id = -1;
		ResultSet rs = null;
		try {
			rs = stmt.getGeneratedKeys();
			while (rs.next()) 
			{
			   id = rs.getInt(1);
			   if(id < 1) {
				   return false;
			   }
			}
		} catch(SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		//Add the user to the permissions table as the owner for the engine
		query = "INSERT INTO EnginePermission (USER, PERMISSION, ENGINE) VALUES ('" + userId + "', " + EnginePermission.OWNER.getId() + ", '" + engineId + "');";
		Statement stmt2 = securityDb.execUpdateAndRetrieveStatement(query, true);
		if(stmt2 != null) {
			securityDb.commit();
			return true;
		}

		return false;
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Adding Insight
	 */
	
	/**
	 * 
	 * @param engineId
	 * @param insightId
	 * @param insightName
	 * @param global
	 * @param exCount
	 * @param createdOn
	 * @param lastModified
	 * @param layout
	 */
	public static void addInsight(String engineId, String insightId, String insightName, boolean global, String layout) {
		LocalDateTime now = LocalDateTime.now();
		String nowString = java.sql.Timestamp.valueOf(now).toString();
		String insightQuery = "INSERT INTO INSIGHT (ENGINEID, INSIGHTID, INSIGHTNAME, GLOBAL, EXECUTIONCOUNT, CREATEDON, LASTMODIFIEDON, LAYOUT) "
				+ "VALUES ('" + engineId + "', '" + insightId + "', '" + insightName + "', " + global + " ," + 0 + " ,'" + nowString + "' ,'" + nowString + "','" + layout + "')";
		securityDb.insertData(insightQuery);
		securityDb.commit();
	}
	
	/**
	 * 
	 * @param engineId
	 * @param insightId
	 * @param insightName
	 * @param global
	 * @param exCount
	 * @param createdOn
	 * @param lastModified
	 * @param layout
	 */
	public static void addUserInsightCreator(String userId, String engineId, String insightId) {
//		String checkQ = "SELECT DISTINCT USERINSIGHTPERMISSION.USERID, USERINSIGHTPERMISSION.ENGINEID, USERINSIGHTPERMISSION.INSIGHTID FROM USERINSIGHTPERMISSION WHERE "
//				+ "USERINSIGHTPERMISSION.USERID='" + userId + "' AND USERINSIGHTPERMISSION.ENGINEID='" + engineId + "' "
//				+ "AND USERINSIGHTPERMISSION.INSIGHTID='" + insightId + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, checkQ);
//		if(wrapper.hasNext()) {
//			wrapper.cleanUp();
//		} else {
			String insightQuery = "INSERT INTO USERINSIGHTPERMISSION (USERID, ENGINEID, INSIGHTID, PERMISSION) "
					+ "VALUES ('" + userId + "', '" + engineId + "', '" + insightId + "', " + 1 + ");";
			securityDb.insertData(insightQuery);
			securityDb.commit();
//		}
	}
	
	/**
	 * 
 	 * @param engineId
	 * @param insightId
	 * @param insightName
	 * @param global
	 * @param exCount
	 * @param lastModified
	 * @param layout
	 */
	public static void updateInsight(String engineId, String insightId, String insightName, boolean global, String layout) {
		LocalDateTime now = LocalDateTime.now();
		String nowString = java.sql.Timestamp.valueOf(now).toString();
		String query = "UPDATE INSIGHT SET INSIGHTNAME='" + insightName + "', GLOBAL=" + global + ", LASTMODIFIEDON='" + nowString 
				+ "', LAYOUT='" + layout + "'  WHERE INSIGHTID = '" + insightId + "' AND ENGINEID='" + engineId + "'"; 
		securityDb.insertData(query);
		securityDb.commit();
	}
	
	/**
	 * 
	 * @param engineId
	 * @param insightId
	 */
	public static void deleteInsight(String engineId, String insightId) {
		String query = "DELETE FROM INSIGHT WHERE INSIGHTID ='" + insightId + "' AND ENGINEID='" + engineId + "'";
		securityDb.insertData(query);
		query = "DELETE FROM USERINSIGHTPERMISSION  WHERE INSIGHTID ='" + insightId + "' AND ENGINEID='" + engineId + "'";
		securityDb.insertData(query);
		securityDb.commit();
	}
	
	/**
	 * 
	 * @param engineId
	 * @param insightId
	 */
	public static void deleteInsight(String engineId, String... insightId) {
		String insightFilter = createFilter(insightId);
		String query = "DELETE FROM INSIGHT WHERE INSIGHTID " + insightFilter + " AND ENGINEID='" + engineId + "'";
		securityDb.insertData(query);
		query = "DELETE FROM USERINSIGHTPERMISSION WHERE INSIGHTID " + insightFilter + " AND ENGINEID='" + engineId + "'";
		securityDb.insertData(query);
		securityDb.commit();
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Adding engine meta
	 */
	
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
	
	/**
	 * Update the total execution count
	 * @param engineId
	 * @param insightId
	 */
	public static void updateExecutionCount(String engineId, String insightId) {
		String updateQuery = "UPDATE INSIGHT SET EXECUTIONCOUNT = EXECUTIONCOUNT + 1 "
				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "'";
		securityDb.insertData(updateQuery);
	}

}
