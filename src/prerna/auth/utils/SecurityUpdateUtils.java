package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermission;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.EngineInsightsHelper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RDBMSUtility;

public class SecurityUpdateUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityUpdateUtils.class);

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
		appId = RdbmsQueryBuilder.escapeForSQLStatement(appId);
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
			logger.info("Security database already contains app with alias = " + Utility.cleanLogString(appName));
			return;
		} else if(!engineExists) {
			addEngine(appId, appName, typeAndCost[0], typeAndCost[1], global);
		} else if(engineExists) {
			// delete values if currently present
			deleteInsightsForRecreation(appId);
			// update engine properties anyway ... in case global was shifted for example
			updateEngine(appId, appName, typeAndCost[0], typeAndCost[1], global);
		}
		
		logger.info("Security database going to add app with alias = " + Utility.cleanLogString(appName));
		
		// load just the insights database
		// first see if engine is already loaded
		boolean engineLoaded = false;
		RDBMSNativeEngine rne = null;
		if(Utility.engineLoaded(appId)) {
			engineLoaded = true;
			rne = Utility.getEngine(appId).getInsightDatabase();
		} else {
			rne = EngineInsightsHelper.loadInsightsEngine(prop, logger);
		}
		
		// i need to delete any current insights for the app
		// before i start to insert new insights
		String deleteQuery = "DELETE FROM INSIGHT WHERE ENGINEID='" + appId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
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
			logger.info("Reloading app. Retrieving existing insights with permissions");
			String insightsWPer = "SELECT INSIGHTID FROM USERINSIGHTPERMISSION WHERE ENGINEID='" + appId + "'";
			insightPermissionIds = QueryExecutionUtility.flushToSetString(securityDb, insightsWPer, false);
			if(insightPermissionIds.isEmpty()) {
				existingInsightPermissions = true;
			}
		}
		
		AbstractSqlQueryUtil securityQueryUtil = securityDb.getQueryUtil();
		// make a prepared statement
		PreparedStatement ps = null;
		try {
			ps = securityDb.bulkInsertPreparedStatement(
					new String[]{
							// table name
							"INSIGHT", 
							// column names
							"ENGINEID","INSIGHTID","INSIGHTNAME","GLOBAL","EXECUTIONCOUNT","CREATEDON",
							"LASTMODIFIEDON","LAYOUT", "CACHEABLE", "RECIPE"});
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
		qs.addSelector(new QueryColumnSelector("QUESTION_ID__QUESTION_PKQL"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("QUESTION_ID__HIDDEN_INSIGHT", "==", false, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(rne, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				try {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, appId);
					String insightId = row[0].toString();
					ps.setString(parameterIndex++, insightId);
					ps.setString(parameterIndex++, row[1].toString());
					ps.setBoolean(parameterIndex++, !((boolean) row[3]));
					ps.setLong(parameterIndex++, 0);
					ps.setTimestamp(parameterIndex++, timeStamp);
					ps.setTimestamp(parameterIndex++, timeStamp);
					ps.setString(parameterIndex++, row[2].toString());
					ps.setBoolean(parameterIndex++, (boolean) row[4]);
					
					// need to determine if our input is a clob
					// and if the database allows a clob data type
					// use the utility method generated
					Object pixelObject = row[5];
					RDBMSUtility.handleInsertionOfClobInput(securityDb, securityQueryUtil, ps, parameterIndex++, pixelObject, securityGson);

					// add to ps
					ps.addBatch();
					// batch commit based on size
					if (++count % batchSize == 0) {
						logger.info("Executing batch .... row num = " + count);
						ps.executeBatch();
					}
					
					if(reloadInsights && insightPermissionIds != null && existingInsightPermissions) {
						insightPermissionIds.remove(insightId);
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		// well, we are done looping through now
		logger.info("Executing final batch .... row num = " + count);
		try {
			ps.executeBatch();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} // insert any remaining records
		try {
			ps.close();
			if(securityDb.isConnectionPooling()) {
				try {
					ps.getConnection().close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		count = 0;
		// same for insight meta
		// i need to delete any current insights for the app
		// before i start to insert new insights
		deleteQuery = "DELETE FROM INSIGHTMETA WHERE ENGINEID='" + appId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		try {
			ps = securityDb.bulkInsertPreparedStatement(
					new String[]{"INSIGHTMETA","ENGINEID","INSIGHTID","METAKEY","METAVALUE","METAORDER"});
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		
		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__INSIGHTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(rne, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow data = wrapper.next();
				Object[] row = data.getValues();
				Object[] raw = data.getRawValues();
				try {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, appId);
					ps.setString(parameterIndex++, row[0].toString());
					ps.setString(parameterIndex++, row[1].toString());

					// need to determine if our input is a clob
					// and if the database allows a clob data type
					// use the utility method generated
					Object metaValue = raw[2];
					RDBMSUtility.handleInsertionOfClobInput(securityDb, securityQueryUtil, ps, parameterIndex++, metaValue, securityGson);
					
					// add the order
					ps.setInt(parameterIndex++, ((Number) row[3]).intValue());
					
					// add to ps
					ps.addBatch();
					// batch commit based on size
					if (++count % batchSize == 0) {
						logger.info("Executing batch .... row num = " + count);
						ps.executeBatch();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		// well, we are done looping through now
		logger.info("Executing final batch .... row num = " + count);
		try {
			ps.executeBatch();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} // insert any remaining records
		try {
			ps.close();
			if(securityDb.isConnectionPooling()) {
				try {
					ps.getConnection().close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		// close the connection to the insights
		// if the engine is not already loaded 
		// since the openDb method will load it
		if(!engineLoaded && rne != null) {
			rne.closeDB();
		}
		
		if(reloadInsights) {
			logger.info("Modifying force reload to false");
			Utility.changePropMapFileValue(smssFile, Constants.RELOAD_INSIGHTS, "false");	
			
			// need to remove existing insights w/ permissions that do not exist anymore
			if(existingInsightPermissions && !insightPermissionIds.isEmpty()) {
				logger.info("Removing insights with permissions that no longer exist");
				String deleteInsightPermissionQuery = "DELETE FROM USERINSIGHTPERMISSION "
					+ "WHERE ENGINEID='" + appId + "'"
					+ " AND INSIGHTID " + createFilter(insightPermissionIds);
				try {
					securityDb.removeData(deleteInsightPermissionQuery);
					securityDb.commit();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		logger.info("Finished adding engine = " + Utility.cleanLogString(appId));
	}
	
	/**
	 * Utility method to get the engine type and cost for storage
	 * @param prop
	 * @return
	 */
	public static String[] getAppTypeAndCost(Properties prop) {
		String appType = null;
		String appCost = null;
		// the whole app cost stuff is completely made up...
		// but it will look cool so we are doing it
		String eType = prop.getProperty(Constants.ENGINE_TYPE);
		if(eType.equals("prerna.engine.impl.rdbms.RDBMSNativeEngine")) {
			String rdbmsType = prop.getProperty(Constants.RDBMS_TYPE);
			if(rdbmsType == null) {
				rdbmsType = "H2_DB";
			}
			rdbmsType = rdbmsType.toUpperCase();
			appType = rdbmsType;
			if(rdbmsType.equals("TERADATA") || rdbmsType.equals("DB2")) {
				appCost = "$$";
			} else {
				appCost = "";
			}
		} else if(eType.equals("prerna.engine.impl.rdbms.ImpalaEngine")) {
			appType = "IMPALA";
			appCost = "$$$";
		} else if(eType.equals("prerna.engine.impl.rdf.BigDataEngine")) {
			appType = "RDF";
			appCost = "";
		} else if(eType.equals("prerna.engine.impl.rdf.RDFFileSesameEngine")) {
			appType = "RDF";
			appCost = "";
		} else if(eType.equals("prerna.ds.datastax.DataStaxGraphEngine")) {
			appType = "DATASTAX";
			appCost = "$$$";
		} else if(eType.equals("prerna.engine.impl.solr.SolrEngine")) {
			appType = "SOLR";
			appCost = "$$";
		} else if(eType.equals("prerna.engine.impl.tinker.TinkerEngine")) {
			String tinkerDriver = prop.getProperty(Constants.TINKER_DRIVER);
			if(tinkerDriver.equalsIgnoreCase("neo4j")) {
				appType = "NEO4J";
				appCost = "";
			} else {
				appType = "TINKER";
				appCost = "";
			}
		} else if(eType.equals("prerna.engine.impl.json.JsonAPIEngine") || eType.equals("prerna.engine.impl.json.JsonAPIEngine2")) {
			appType = "JSON";
			appCost = "";
		} else if(eType.equals("prerna.engine.impl.app.AppEngine")) {
			appType = "APP";
			appCost = "$";
		}
		
		return new String[]{appType, appCost};
	}
	
	/**
	 * Delete just the insights for an engine
	 * @param appId
	 */
	public static void deleteInsightsForRecreation(String appId) {
		appId = RdbmsQueryBuilder.escapeForSQLStatement(appId);
		String deleteQuery = "DELETE FROM INSIGHT WHERE ENGINEID='" + appId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * Delete all values
	 * @param appId
	 */
	public static void deleteApp(String appId) {
		appId = RdbmsQueryBuilder.escapeForSQLStatement(appId);
		if(ignoreEngine(appId)) {
			// dont add local master or security db to security db
			return;
		}
		List<String> deletes = new Vector<>();
		deletes.add("DELETE FROM ENGINE WHERE ENGINEID=?");
		deletes.add("DELETE FROM INSIGHT WHERE ENGINEID=?");
		deletes.add("DELETE FROM ENGINEPERMISSION WHERE ENGINEID=?");
		deletes.add("DELETE FROM ENGINEMETA WHERE ENGINEID=?");
		deletes.add("DELETE FROM WORKSPACEENGINE WHERE ENGINEID=?");
		deletes.add("DELETE FROM ASSETENGINE WHERE ENGINEID=?");
		deletes.add("DELETE FROM ASSETENGINE WHERE ENGINEID=?");
		// TODO: add the other tables...

		for(String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(deleteQuery);
				ps.setString(1, appId);
				ps.execute();
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(ps != null) {
					try {
						ps.close();
						if(securityDb.isConnectionPooling()) {
							try {
								ps.getConnection().close();
							} catch (SQLException e) {
								logger.error(Constants.STACKTRACE, e);
							}
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
		securityDb.commit();
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
				+ "VALUES (?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, engineName);
			ps.setString(parameterIndex++, engineId);
			ps.setString(parameterIndex++, engineType);
			ps.setString(parameterIndex++, engineCost);
			ps.setBoolean(parameterIndex++, global);
			ps.execute();
			securityDb.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	public static void updateEngine(String engineId, String engineName, String engineType, String engineCost, boolean global) {
		String query = "UPDATE ENGINE SET ENGINENAME=?, TYPE=?, COST=?, GLOBAL=? WHERE ENGINEID=?";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, engineName);
			ps.setString(parameterIndex++, engineType);
			ps.setString(parameterIndex++, engineCost);
			ps.setBoolean(parameterIndex++, global);
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			securityDb.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	public static void addEngineOwner(String engineId, String userId) {
		String query = "INSERT INTO ENGINEPERMISSION (USERID, PERMISSION, ENGINEID, VISIBILITY) VALUES (?,?,?,?)";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, userId);
			ps.setInt(parameterIndex++, AccessPermission.OWNER.getId());
			ps.setString(parameterIndex++, engineId);
			ps.setBoolean(parameterIndex++, true);
			ps.execute();
			securityDb.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Set an engine and all its insights to be global
	 * @param engineId
	 */
	public static void setEngineCompletelyGlobal(String engineId) {
		{
			String update1 = "UPDATE ENGINE SET GLOBAL=? WHERE ENGINEID=?";
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(update1);
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, true);
				ps.setString(parameterIndex++, engineId);
				ps.execute();
				securityDb.commit();
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(ps != null) {
					try {
						ps.close();
						if(securityDb.isConnectionPooling()) {
							try {
								ps.getConnection().close();
							} catch (SQLException e) {
								logger.error(Constants.STACKTRACE, e);
							}
						}
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		{
			String update1 = "UPDATE INSIGHT SET GLOBAL=? WHERE ENGINEID=?";
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(update1);
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, true);
				ps.setString(parameterIndex++, engineId);
				ps.execute();
				securityDb.commit();
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(ps != null) {
					try {
						ps.close();
						if(securityDb.isConnectionPooling()) {
							try {
								ps.getConnection().close();
							} catch (SQLException e) {
								logger.error(Constants.STACKTRACE, e);
							}
						}
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * @param userName	String representing the name of the user to add
	 */
	public static boolean addOAuthUser(AccessToken newUser) throws IllegalArgumentException {
		// lower case the emails coming in
		if(newUser.getEmail() != null) {
			newUser.setEmail(newUser.getEmail().toLowerCase());
		}
					
		// see if the user was added by an admin
		// this means it could be on the ID or the EMAIL
		// but name is the admin_added_user constant
		String query = "SELECT ID FROM SMSS_USER WHERE "
				+ "(NAME='" + ADMIN_ADDED_USER + "' OR USERNAME='" + ADMIN_ADDED_USER + "')"
				+ " AND "
				// this matching the ID field to the email because admin added user only sets the id field
				+ "(ID='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getId()) + "' OR ID='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getEmail()) + "')";
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			if(wrapper.hasNext()) {
				// this was the old id that was added when the admin 
				String oldId = RdbmsQueryBuilder.escapeForSQLStatement(wrapper.next().getValues()[0].toString());
				String newId = RdbmsQueryBuilder.escapeForSQLStatement(newUser.getId());
				// this user was added by the user
				// and we need to update
				{
					UpdateQueryStruct uqs = new UpdateQueryStruct();
					uqs.setEngine(securityDb);
					uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", oldId));
					
					List<IQuerySelector> selectors = new Vector<>();
					selectors.add(new QueryColumnSelector("SMSS_USER__ID"));
					selectors.add(new QueryColumnSelector("SMSS_USER__NAME"));
					selectors.add(new QueryColumnSelector("SMSS_USER__USERNAME"));
					selectors.add(new QueryColumnSelector("SMSS_USER__EMAIL"));
					selectors.add(new QueryColumnSelector("SMSS_USER__TYPE"));

					List<Object> values = new Vector<>();
					values.add(newId);
					values.add(newUser.getName());
					values.add(newUser.getUsername());
					values.add(newUser.getEmail());
					values.add(newUser.getProvider().toString());

					uqs.setSelectors(selectors);
					uqs.setValues(values);
					
					UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(uqs);
					String updateQuery = updateInterp.composeQuery();
					
					try {
						securityDb.insertData(updateQuery);
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
				
				// need to update any other permissions that were set for this user
				String updateQuery = "UPDATE ENGINEPERMISSION SET USERID='" +  newId +"' WHERE USERID='" + oldId + "'";
				try {
					securityDb.insertData(updateQuery);
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				
				// need to update all the places the user id is used
				updateQuery = "UPDATE USERINSIGHTPERMISSION SET USERID='" +  newId +"' WHERE USERID='" + oldId + "'";
				try {
					securityDb.insertData(updateQuery);
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				
				securityDb.commit();
			} else {
				// not added by admin
				// lets see if he exists or not
				boolean userExists = SecurityQueryUtils.checkUserExist(newUser.getId());

				if (userExists) {
					logger.info("oauth user already exists");
					return false;
				}

				// need to synchronize the adding of new users
				// so that we do not enter here from different threads 
				// and add the same user twice
				synchronized(SecurityUpdateUtils.class) {
					// need to prevent 2 threads attempting to add the same user
					userExists = SecurityQueryUtils.checkUserExist(newUser.getId());
					if(!userExists) {
						query = "INSERT INTO SMSS_USER (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PUBLISHER) VALUES (?,?,?,?,?,?,?)";
						PreparedStatement ps = null;
						try {
							ps = securityDb.getPreparedStatement(query);
							int parameterIndex = 1;
							ps.setString(parameterIndex++, newUser.getId());
							ps.setString(parameterIndex++, newUser.getName());
							ps.setString(parameterIndex++, newUser.getUsername());
							ps.setString(parameterIndex++, newUser.getEmail());
							ps.setString(parameterIndex++, newUser.getProvider().toString());
							ps.setBoolean(parameterIndex++, false);
							ps.setBoolean(parameterIndex++, !adminSetPublisher());
							ps.execute();
							securityDb.commit();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						} finally {
							if(ps != null) {
								try {
									ps.close();
									if(securityDb.isConnectionPooling()) {
										try {
											ps.getConnection().close();
										} catch (SQLException e) {
											logger.error(Constants.STACKTRACE, e);
										}
									}
								} catch (SQLException e) {
									logger.error(Constants.STACKTRACE, e);
								}
							}
						}
						
						return true;
					}
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return false;
	}
	
	/**
	 * Update OAuth user credentials
	 * @param existingUser
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static boolean updateOAuthUser(AccessToken existingToken) throws IllegalArgumentException {
		// lower case the emails coming in
		if(existingToken.getEmail() != null) {
			existingToken.setEmail(existingToken.getEmail().toLowerCase());
		}
		String name = existingToken.getName();
		String username = existingToken.getUsername();
		String email = existingToken.getEmail();
		
		UpdateQueryStruct uqs = new UpdateQueryStruct();
		uqs.setEngine(securityDb);
		uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", existingToken.getId()));
		
		List<IQuerySelector> selectors = new Vector<>();
		selectors.add(new QueryColumnSelector("SMSS_USER__NAME"));
		selectors.add(new QueryColumnSelector("SMSS_USER__USERNAME"));
		selectors.add(new QueryColumnSelector("SMSS_USER__EMAIL"));
		List<Object> values = new Vector<>();
		values.add(name);
		values.add(username);
		values.add(email);
		
		uqs.setSelectors(selectors);
		uqs.setValues(values);
		
		UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(uqs);
		String updateQuery = updateInterp.composeQuery();

		try {
			securityDb.insertData(updateQuery);
			return true;
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return false;
	}

	/**
 	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * @param id
	 * @param name
	 * @param email
	 * @param password
	 * @param type
	 * @param admin
	 * @param publisher
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static boolean registerUser(String id, String name, String email, String password, String type, boolean admin,
			boolean publisher) throws IllegalArgumentException {
		boolean isNewUser = SecurityQueryUtils.checkUserExist(id);
		if(isNewUser) {
			return false;
		}
		
		String userName = ADMIN_ADDED_USER;
		boolean isNative = false;
		String salt = null;
		String hashedPassword = null;
		if (type != null) {
			isNative = type.toLowerCase().equals("native");
			if (isNative) {
				if (name != null && !name.isEmpty()) {
					userName = id;
					salt = SecurityQueryUtils.generateSalt();
					hashedPassword = (SecurityQueryUtils.hash(password, salt));
				}
			}
		}
		// if username or name is null
		// switch to admin_added_user
		// the {@link #addOAuthUser} will fill these in when the user 
		// logins from their provider
		if(userName == null) userName = ADMIN_ADDED_USER;
		if(name == null) name = ADMIN_ADDED_USER;
		if(email == null) email = "";
		if(hashedPassword == null) hashedPassword = "";
		if(salt == null) salt = "";
		if(type == null) type = "";

		String query = "INSERT INTO SMSS_USER (ID, USERNAME, NAME, EMAIL, PASSWORD, SALT, TYPE, ADMIN, PUBLISHER) VALUES (?,?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, id);
			ps.setString(parameterIndex++, userName);
			ps.setString(parameterIndex++, name);
			ps.setString(parameterIndex++, email.toLowerCase());
			ps.setString(parameterIndex++, hashedPassword);
			ps.setString(parameterIndex++, salt);
			ps.setString(parameterIndex++, type);
			ps.setBoolean(parameterIndex++, admin);
			ps.setBoolean(parameterIndex++, publisher);
			ps.execute();
			securityDb.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return true;
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
	 * @throws IllegalAccessException 
	 */
	public static void setDbVisibility(User user, String engineId, boolean visibility) throws SQLException, IllegalAccessException {
		if(!SecurityAppUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalAccessException("The user doesn't have the permission to modify his visibility of this app.");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIdFilters));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()){
				UpdateQueryStruct uqs = new UpdateQueryStruct();
				uqs.setEngine(securityDb);
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIdFilters));

				List<IQuerySelector> selectors = new Vector<>();
				selectors.add(new QueryColumnSelector("ENGINEPERMISSION__VISIBILITY"));
				List<Object> values = new Vector<>();
				values.add(visibility);
				uqs.setSelectors(selectors);
				uqs.setValues(values);
				
				UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(uqs);
				String updateQuery = updateInterp.composeQuery();
				securityDb.insertData(updateQuery);
				
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
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
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
			List<String> userIds = new Vector<>();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider provider : logins) {
				userIds.add(user.getAccessToken(provider).getId());
			}
			
			Set<String> uniqueRequestIds = new HashSet<>();
			List<String> curUserIds = new Vector<>();
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
}
