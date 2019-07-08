package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.auth.AccessPermission;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.EngineInsightsHelper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
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
		
		boolean global = true;
		if(prop.containsKey(Constants.HIDDEN_DATABASE) && "true".equalsIgnoreCase(prop.get(Constants.HIDDEN_DATABASE).toString().trim()) ) {
			global = false;
		}
		
		addApp(appId, global);
	}
	
	/**
	 * Add an entire engine into the security db
	 * @param appId
	 */
	public static void addApp(String appId, boolean global) {
		if(ignoreEngine(appId)) {
			// dont add local master or security db to security db
			return;
		}
		String smssFile = DIHelper.getInstance().getCoreProp().getProperty(appId + "_" + Constants.STORE);
		Properties prop = Utility.loadProperties(smssFile);

		String appName = prop.getProperty(Constants.ENGINE_ALIAS);
		if(appName == null) {
			appName = appId;
		}
		
		boolean reloadInsights = false;
		if(prop.containsKey(Constants.RELOAD_INSIGHTS)) {
			String booleanStr = prop.get(Constants.RELOAD_INSIGHTS).toString();
			reloadInsights = Boolean.parseBoolean(booleanStr);
		}
		
		String[] typeAndCost = getAppTypeAndCost(prop);
		boolean engineExists = containsEngineId(appId);
		if(engineExists && !reloadInsights) {
			LOGGER.info("Security database already contains app with alias = " + appName);
			return;
		} else if(!engineExists) {
			addEngine(appId, appName, typeAndCost[0], typeAndCost[1], global);
		} else if(engineExists) {
			// delete values if currently present
			deleteInsightsForRecreation(appId);
			// update engine properties anyway ... in case global was shifted for example
			updateEngine(appId, appName, typeAndCost[0], typeAndCost[1], global);
		}
		
		LOGGER.info("Security database going to add app with alias = " + appName);
		
		// load just the insights database
		// first see if engine is already loaded
		boolean engineLoaded = false;
		RDBMSNativeEngine rne = null;
		if(Utility.engineLoaded(appId)) {
			engineLoaded = true;
			rne = Utility.getEngine(appId).getInsightDatabase();
		} else {
			rne = EngineInsightsHelper.loadInsightsEngine(prop, LOGGER);
		}
		
		// i need to delete any current insights for the app
		// before i start to insert new insights
		String deleteQuery = "DELETE FROM INSIGHT WHERE ENGINEID='" + appId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// if we are doing a reload
		// we will want to remove unnecessary insights
		// from the insight permissions
		boolean existingInsightPermissions = true;
		Set<String> insightPermissionIds = null;
		if(reloadInsights) {
			// need to flush out the current insights w/ permissions
			// will keep the same permissions
			// and perform a delta
			LOGGER.info("Reloading app. Retrieving existing insights with permissions");
			String insightsWPer = "SELECT INSIGHTID FROM USERINSIGHTPERMISSION WHERE ENGINEID='" + appId + "'";
			IRawSelectWrapper insightWPerWrapper = WrapperManager.getInstance().getRawWrapper(securityDb, insightsWPer);
			insightPermissionIds = flushToSetString(insightWPerWrapper, false);
			if(insightPermissionIds.isEmpty()) {
				existingInsightPermissions = true;
			}
		}
		
		// make a prepared statement
		PreparedStatement ps = securityDb.bulkInsertPreparedStatement(
				new String[]{"INSIGHT","ENGINEID","INSIGHTID","INSIGHTNAME","GLOBAL","EXECUTIONCOUNT","CREATEDON","LASTMODIFIEDON","LAYOUT", "CACHEABLE"});
		// keep a batch size so we dont get heapspace
		final int batchSize = 5000;
		int count = 0;
		
		LocalDateTime now = LocalDateTime.now();
		Timestamp timeStamp = java.sql.Timestamp.valueOf(now);
		
//		String query = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_LAYOUT, HIDDEN_INSIGHT, CACHEABLE FROM QUESTION_ID WHERE HIDDEN_INSIGHT=false";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rne, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("QUESTION_ID__ID"));
		qs.addSelector(new QueryColumnSelector("QUESTION_ID__QUESTION_NAME"));
		qs.addSelector(new QueryColumnSelector("QUESTION_ID__QUESTION_LAYOUT"));
		qs.addSelector(new QueryColumnSelector("QUESTION_ID__HIDDEN_INSIGHT"));
		qs.addSelector(new QueryColumnSelector("QUESTION_ID__CACHEABLE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("QUESTION_ID__HIDDEN_INSIGHT", "==", false, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rne, qs);
		while(wrapper.hasNext()) {
			Object[] row = wrapper.next().getValues();
			try {
				ps.setString(1, appId);
				String insightId = row[0].toString();
				ps.setString(2, insightId);
				ps.setString(3, row[1].toString());
				ps.setBoolean(4, !((boolean) row[3]));
				ps.setLong(5, 0);
				ps.setTimestamp(6, timeStamp);
				ps.setTimestamp(7, timeStamp);
				ps.setString(8, row[2].toString());
				ps.setBoolean(9, (boolean) row[4]);
				ps.addBatch();
				
				// batch commit based on size
				if (++count % batchSize == 0) {
					LOGGER.info("Executing batch .... row num = " + count);
					ps.executeBatch();
				}
				
				if(reloadInsights && existingInsightPermissions) {
					insightPermissionIds.remove(insightId);
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
		
		// close the connection to the insights
		// if the engine is not already loaded 
		// since the openDb method will load it
		if(!engineLoaded && rne != null) {
			rne.closeDB();
		}
		
		if(reloadInsights) {
			LOGGER.info("Modifying force reload to false");
			Utility.changePropMapFileValue(smssFile, Constants.RELOAD_INSIGHTS, "false");	
			
			// need to remove existing insights w/ permissions that do not exist anymore
			if(existingInsightPermissions && !insightPermissionIds.isEmpty()) {
				LOGGER.info("Removing insights with permissions that no longer exist");
				String deleteInsightPermissionQuery = "DELETE FROM USERINSIGHTPERMISSION "
					+ "WHERE ENGINEID='" + appId + "'"
					+ " AND INSIGHTID " + createFilter(insightPermissionIds);
				try {
					securityDb.removeData(deleteInsightPermissionQuery);
					securityDb.commit();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
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
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
		String deleteQuery = "DELETE FROM ENGINE WHERE ENGINEID='" + appId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		deleteQuery = "DELETE FROM INSIGHT WHERE ENGINEID='" + appId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		deleteQuery = "DELETE FROM ENGINEPERMISSION WHERE ENGINEID='" + appId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		deleteQuery = "DELETE FROM ENGINEMETA WHERE ENGINEID='" + appId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		deleteQuery = "DELETE FROM WORKSPACEENGINE WHERE ENGINEID='" + appId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		deleteQuery = "DELETE FROM ASSETENGINE WHERE ENGINEID='" + appId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
//		//TODO: add the other tables...
		if(AbstractSecurityUtils.securityEnabled){
			removeDb(appId);
		}
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
		addEngine(engineId, engineName, engineType, engineCost, !securityEnabled);
	}
	
	public static void addEngine(String engineId, String engineName, String engineType, String engineCost, boolean global) {
		String query = "INSERT INTO ENGINE (ENGINENAME, ENGINEID, TYPE, COST, GLOBAL) "
				+ "VALUES ('" + RdbmsQueryBuilder.escapeForSQLStatement(engineName) + "', '" + engineId + "', '" + engineType + "', '" + engineCost + "', " + global + ")";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void updateEngine(String engineId, String engineName, String engineType, String engineCost, boolean global) {
		String query = "UPDATE ENGINE SET "
				+ "ENGINENAME='" + RdbmsQueryBuilder.escapeForSQLStatement(engineName) 
				+ "', TYPE='" + engineType 
				+ "', COST='" + engineCost 
				+ "', GLOBAL=" + global
				+ " WHERE ENGINEID='" + engineId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void addEngineOwner(String engineId, String userId) {
		String query = "INSERT INTO ENGINEPERMISSION (USERID, PERMISSION, ENGINEID, VISIBILITY) VALUES ('" + RdbmsQueryBuilder.escapeForSQLStatement(userId) + "', " + AccessPermission.OWNER.getId() + ", '" + engineId + "', TRUE);";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Set an engine and all its insights to be global
	 * @param engineId
	 */
	public static void setEngineCompletelyGlobal(String engineId) {
		String update1 = "UPDATE ENGINE SET GLOBAL=TRUE WHERE ENGINEID='" + engineId + "'";
		try {
			securityDb.insertData(update1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		String update2 = "UPDATE INSIGHT SET GLOBAL=TRUE WHERE ENGINEID='" + engineId + "'";
		try {
			securityDb.insertData(update2);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
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
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
		try {
			securityDb.insertData(updateQuery);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * @param userName	String representing the name of the user to add
	 */
	public static boolean addOAuthUser(AccessToken newUser) throws IllegalArgumentException {
		// see if the user was added by an admin
		// this means it could be on the ID or the EMAIL
		// but name is the admin_added_user constant
		String query = "SELECT ID FROM USER WHERE "
				+ "NAME='" + ADMIN_ADDED_USER + "' AND "
				// this matching the ID field to the email because admin added user only sets the id field
				+ "(ID='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getId()) + "' OR ID='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getEmail()) + "')";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()) {
				// this was the old id that was added when the admin 
				String oldId = RdbmsQueryBuilder.escapeForSQLStatement(wrapper.next().getValues()[0].toString());
				
				String newId = RdbmsQueryBuilder.escapeForSQLStatement(newUser.getId());
				// this user was added by the user
				// and we need to update
				String updateQuery = "UPDATE USER SET "
						+ "ID='"+ newId + "', "
						+ "NAME='"+ RdbmsQueryBuilder.escapeForSQLStatement(newUser.getName()) + "', "
						+ "USERNAME='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getUsername()) + "', "
						+ "EMAIL='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getEmail()) + "', "
						+ "TYPE='" + newUser.getProvider() + "' "
						+ "WHERE ID='" + oldId + "';";
				try {
					securityDb.insertData(updateQuery);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				// need to update any other permissions that were set for this user
				updateQuery = "UPDATE ENGINEPERMISSION SET USERID='" +  newId +"' WHERE USERID='" + oldId + "'";
				try {
					securityDb.insertData(updateQuery);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				// need to update all the places the user id is used
				updateQuery = "UPDATE USERINSIGHTPERMISSION SET USERID='" +  newId +"' WHERE USERID='" + oldId + "'";
				try {
					securityDb.insertData(updateQuery);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				securityDb.commit();
			} else {
				// not added by admin
				// lets see if he exists or not
				boolean isNewUser = SecurityQueryUtils.checkUserExist(newUser.getId());
				if(!isNewUser) {
					// need to synchronize the adding of new users
					// so that we do not enter here from different threads 
					// and add the same user twice
					synchronized(SecurityUpdateUtils.class) {
						// need to prevent 2 threads attempting to add the same user
						isNewUser = SecurityQueryUtils.checkUserExist(newUser.getId());
						if(!isNewUser) {
							query = "INSERT INTO USER (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PUBLISHER) VALUES ('" + 
									RdbmsQueryBuilder.escapeForSQLStatement(newUser.getId()) + "', '" + 
									RdbmsQueryBuilder.escapeForSQLStatement(newUser.getName()) + "', '" + 
									RdbmsQueryBuilder.escapeForSQLStatement(newUser.getUsername()) + "', '" + 
									RdbmsQueryBuilder.escapeForSQLStatement(newUser.getEmail()) + "', '" + 
									newUser.getProvider() + "', " + 
									"FALSE, " + 
									!adminSetPublisher() + ");";
							try {
								securityDb.insertData(query);
								securityDb.commit();
							} catch (SQLException e) {
								e.printStackTrace();
							}
							return true;
						}
					}
				}
			}
		} finally {
			wrapper.cleanUp();
		}
		
		return false;
	}
	
	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * @param userName	String representing the name of the user to add
	 */
	public static boolean registerUser(String id, boolean admin, boolean publisher) throws IllegalArgumentException{
		boolean isNewUser = SecurityQueryUtils.checkUserExist(id);
		if(!isNewUser) {			
			String query = "INSERT INTO USER (ID, NAME, ADMIN, PUBLISHER) VALUES "
					+ "('" + RdbmsQueryBuilder.escapeForSQLStatement(id) + "', '" + ADMIN_ADDED_USER + "', " + admin + ", " + publisher + ");";
			try {
				securityDb.insertData(query);
				securityDb.commit();
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
			return true;
		} else {
			return false;
		}
	}
	
	/*
	 * Engines
	 */
	
	/**
	 * Change the user visibility (show/hide) for a database. Without removing its permissions.
	 * @param user
	 * @param engineId
	 * @param visibility
	 * @throws SQLException 
	 */
	public static void setDbVisibility(User user, String engineId, boolean visibility) throws SQLException {
		if(!SecurityAppUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("The user doesn't have the permission to modify his visibility of this app.");
		}
		String userFilters = getUserFilters(user);
		String query = "SELECT ENGINEID FROM ENGINEPERMISSION WHERE "
				+ "ENGINEID = '" + engineId + "' "
				+ "AND USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()){
				query = "UPDATE ENGINEPERMISSION SET VISIBILITY = '" + visibility + "' WHERE "
						+ "ENGINEID = '" + engineId + "' "
						+ "AND USERID IN " + userFilters;
				
				securityDb.insertData(query);
				
			} else {
				// need to insert
				StringBuilder inserts = new StringBuilder();
				// we will set the permission to read only
				for(AuthProvider loginType : user.getLogins()) {
					String userId = user.getAccessToken(loginType).getId();
					inserts.append("INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION) VALUES ('")
						.append(userId).append("', '").append(engineId).append("', ").append(visibility).append(", 3);");
				}

				securityDb.insertData(inserts.toString());
			}
		} finally {
			wrapper.cleanUp();
		}
		securityDb.commit();
	}
	
	/**
	 * Set if the database is public to all users on this instance
	 * @param user
	 * @param engineId
	 * @param isPublic
	 * @return
	 * @throws SQLException 
	 */
	public static boolean makeRequest(User user, String engineId, int requestedPermission) throws SQLException {
		// make sure this person isn't requesting multiple times
		if(!SecurityQueryUtils.getGlobalEngineIds().contains(engineId)) {
			throw new IllegalArgumentException("Cannot request access to an app that is not public");
		}
		
		StringBuilder builder = new StringBuilder();
		String[] colNames = new String[]{"ID", "SUBMITTEDBY", "ENGINE", "PERMISSION"};
		String[] types = new String[]{"VARCHAR(100)", "VARCHAR(255)", "VARCHAR(255)", "INT"};
		
		List<Map<String, Object>> existingRequests = SecurityQueryUtils.getUserAccessRequestsByProvider(user, engineId);
		if(existingRequests.isEmpty()) {
			// brand new requests
			String requestId = UUID.randomUUID().toString();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider provider : logins) {
				Object[] data = new Object[]{requestId, user.getAccessToken(provider).getId(), engineId, requestedPermission};
				builder.append(RdbmsQueryBuilder.makeInsert("ACCESSREQUEST", colNames, types, data)).append(";");
			}
			
			securityDb.insertData(builder.toString());
		} else {
			// are there new logins for this request that are needed, or not?
			List<String> userIds = new Vector<String>();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider provider : logins) {
				userIds.add(user.getAccessToken(provider).getId());
			}
			
			Set<String> uniqueRequestIds = new HashSet<String>();
			List<String> curUserIds = new Vector<String>();
			for(Map<String, Object> requestObj : existingRequests) {
				uniqueRequestIds.add(requestObj.get("ID").toString());
				curUserIds.add(requestObj.get("SUBMITTEDBY").toString());
			}
			
			// do a minus
			userIds.removeAll(curUserIds);
			if(userIds.isEmpty()) {
				// nothing to add
				// return false that we have done nothing
				return false;
			} else {
				if(uniqueRequestIds.size() == 1) {
					String requestId = uniqueRequestIds.iterator().next();
					for(String userId : userIds) {
						Object[] data = new Object[]{requestId, userId, engineId, requestedPermission};				
						builder.append(RdbmsQueryBuilder.makeInsert("ACCESSREQUEST", colNames, types, data)).append(";");
					}
					securityDb.insertData(builder.toString());
				} else {
					// we will update all the ids to be the same
					String newRequestId = UUID.randomUUID().toString();
					// first insert the new records
					for(String userId : userIds) {
						Object[] data = new Object[]{newRequestId, userId, engineId, requestedPermission};				
						builder.append(RdbmsQueryBuilder.makeInsert("ACCESSREQUEST", colNames, types, data)).append(";");
					}
					securityDb.insertData(builder.toString());
					
					// now update the old ones since we have the same user
					String updateQuery = "UPDATE ACCESSREQUEST SET ID='" + newRequestId + "' WHERE ID IN " + createFilter(uniqueRequestIds);
					securityDb.insertData(updateQuery);
				}
			}
		}
		
		return true;
	}
	
	/**
	 * Remove a database and all the permissions related to it.
	 * @param engineName
	 */
	public static void removeDb(String engineId) {
		//DELETE USERPERMISSIONS
		String query = "DELETE FROM ENGINEPERMISSION WHERE ENGINEID = '?1'; DELETE FROM GROUPENGINEPERMISSION WHERE ENGINE = '?1'; DELETE FROM ENGINE WHERE ENGINEID = '?1'";
		query = query.replace("?1", engineId);
		
		System.out.println("Executing security query: " + query);
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
	}

}
