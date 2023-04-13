package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.PasswordRequirements;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.project.impl.ProjectHelper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SecurityUpdateUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityUpdateUtils.class);

	/**
	 * Only used for static references
	 */
	private SecurityUpdateUtils() {
		
	}
	
	/**
	 * Add an entire database into the security db
	 * @param databaseId
	 */
	public static void addDatabase(String databaseId) {
		if(ignoreDatabase(databaseId)) {
			// dont add local master or security db to security db
			return;
		}
		String smssFile = DIHelper.getInstance().getDbProperty(databaseId + "_" + Constants.STORE) + "";
		Properties prop = Utility.loadProperties(smssFile);
		
		boolean global = true;
		if(prop.containsKey(Constants.HIDDEN_DATABASE) && "true".equalsIgnoreCase(prop.get(Constants.HIDDEN_DATABASE).toString().trim()) ) {
			global = false;
		}
		
		addDatabase(databaseId, global);
	}
	
	/**
	 * Add an entire database into the security db
	 * @param databaseId
	 */
	public static void addDatabase(String databaseId, boolean global) {
		databaseId = RdbmsQueryBuilder.escapeForSQLStatement(databaseId);
		if(ignoreDatabase(databaseId)) {
			// dont add local master or security db to security db
			return;
		}
		String smssFile = DIHelper.getInstance().getDbProperty(databaseId + "_" + Constants.STORE) + "";
		Properties prop = Utility.loadProperties(smssFile);

		String databaseName = prop.getProperty(Constants.ENGINE_ALIAS);
		if(databaseName == null) {
			databaseName = databaseId;
		}
		
		String[] typeAndCost = getDatabaseTypeAndCost(prop);
		boolean engineExists = containsDatabaseId(databaseId);
		if(engineExists) {
			logger.info("Security database already contains database with unique id = " + Utility.cleanLogString(SmssUtilities.getUniqueName(prop)));
			return;
		} else {
			addDatabase(databaseId, databaseName, typeAndCost[0], typeAndCost[1], global);
		} 
		
		// TODO: need to see when we should be updating the database metadata
//		if(engineExists) {
//			// update database properties anyway ... in case global was shifted for example
//			updateDatabase(databaseId, databaseName, typeAndCost[0], typeAndCost[1], global);
//		}
		
		logger.info("Finished adding database = " + Utility.cleanLogString(databaseId));
	}
	
	/**
	 * Add an entire project into the security db - Expectation is not to call this method but addProject(projectId, boolean global = true)
	 * @param projectId
	 */
	public static void addProject(String projectId) {
		String smssFile = DIHelper.getInstance().getProjectProperty(projectId + "_" + Constants.STORE) + "";
		Properties prop = Utility.loadProperties(smssFile);
		
		boolean global = true;
		if(prop.containsKey(Constants.HIDDEN_DATABASE) && "true".equalsIgnoreCase(prop.get(Constants.HIDDEN_DATABASE).toString().trim()) ) {
			global = false;
		}
		
		addProject(projectId, global);
	}
	
	/**
	 * Add an entire project into the security db
	 * @param appId
	 */
	public static void addProject(String projectId, boolean global) {
		projectId = RdbmsQueryBuilder.escapeForSQLStatement(projectId);
		String smssFile = DIHelper.getInstance().getProjectProperty(projectId + "_" + Constants.STORE) + "";
		Properties prop = Utility.loadProperties(smssFile);

		String projectName = prop.getProperty(Constants.PROJECT_ALIAS);
		if(projectName == null) {
			projectName = projectId;
		}
		
		boolean reloadInsights = false;
		if(prop.containsKey(Constants.RELOAD_INSIGHTS)) {
			String booleanStr = prop.get(Constants.RELOAD_INSIGHTS).toString();
			reloadInsights = Boolean.parseBoolean(booleanStr);
		}
		
		// TODO: we do not need type and cost for project
		String[] typeAndCost = new String[] {"",""};
		boolean projectExists = containsProjectId(projectId);
 		if(projectExists && !reloadInsights) {
			logger.info("Security database already contains project with unique id = " + Utility.cleanLogString(SmssUtilities.getUniqueName(prop)));
			return;
		} else if(!projectExists) {
			addProject(projectId, projectName, typeAndCost[0], typeAndCost[1], global);
		} else if(projectExists) {
			// delete values if currently present
			deleteInsightsFromProjectForRecreation(projectId);
			// update project properties anyway ... in case global was shifted for example
			updateProject(projectId, projectName, typeAndCost[0], typeAndCost[1], global);
		}
		
		logger.info("Security database going to add project with alias = " + Utility.cleanLogString(projectName));
		
		// load just the insights database
		// first see if engine is already loaded
		boolean projectLoaded = false;
		RDBMSNativeEngine rne = null;
		if(Utility.projectLoaded(projectId)) {
			rne = Utility.getProject(projectId).getInsightDatabase();
		} else {
			rne  = ProjectHelper.loadInsightsEngine(prop, logger);
		}
		
		// i need to delete any current insights for the project
		// before i start to insert new insights
		String deleteQuery = "DELETE FROM INSIGHT WHERE PROJECTID='" + projectId + "'";
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
			String insightsWPer = "SELECT INSIGHTID FROM USERINSIGHTPERMISSION WHERE PROJECTID='" + projectId + "'";
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
							"PROJECTID","INSIGHTID","INSIGHTNAME","GLOBAL","EXECUTIONCOUNT","CREATEDON",
							"LASTMODIFIEDON","LAYOUT",
							"CACHEABLE","CACHEMINUTES","CACHECRON","CACHEDON","CACHEENCRYPT",
							"RECIPE", 
							"SCHEMANAME"});
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		// keep a batch size so we dont get heapspace
		final int batchSize = 5000;
		int count = 0;
		
		LocalDateTime now = LocalDateTime.now();
		Timestamp timeStamp = java.sql.Timestamp.valueOf(now);
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));

//		String query = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_LAYOUT, HIDDEN_INSIGHT, CACHEABLE FROM QUESTION_ID WHERE HIDDEN_INSIGHT=false";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rne, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_ID_COL));
		qs.addSelector(new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_NAME_COL));
		qs.addSelector(new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_LAYOUT_COL));
		qs.addSelector(new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.HIDDEN_INSIGHT_COL));
		qs.addSelector(new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHEABLE_COL));
		qs.addSelector(new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHE_MINUTES_COL));
		qs.addSelector(new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHE_CRON_COL));
		qs.addSelector(new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHED_ON_COL));
		qs.addSelector(new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHE_ENCRYPT_COL));
		qs.addSelector(new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_PKQL_COL));
		qs.addSelector(new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.SCHEMA_NAME_COL));
//		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("QUESTION_ID__HIDDEN_INSIGHT", "==", false, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(rne, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				try {
					// grab the insight rdbms values
					int index = 0;
					String insightId = row[index++].toString();
					String insightName = row[index++].toString();
					String insightLayout = row[index++].toString();
					Boolean isPrivate = (Boolean) row[index++];
					if(isPrivate == null) {
						isPrivate = false;
					}
					Boolean cacheable = (Boolean) row[index++];
					if(cacheable == null) {
						cacheable = true;
					}
					Integer cacheMinutes = (Integer) row[index++];
					if(cacheMinutes == null) {
						cacheMinutes = -1;
					}
					String cacheCron = (String) row[index++];
					SemossDate cachedOn = (SemossDate) row[index++];
					Boolean cacheEncrypt = (Boolean) row[index++];
					if(cacheEncrypt == null) {
						cacheEncrypt = false;
					}
					Object pixelObject = row[index++];
					String schemaName = (String) row[index++];

					// insert prepared statement into security db
					int parameterIndex = 1;
					ps.setString(parameterIndex++, projectId);
					ps.setString(parameterIndex++, insightId);
					ps.setString(parameterIndex++, insightName);
					ps.setBoolean(parameterIndex++, !isPrivate);
					ps.setLong(parameterIndex++, 0);
					ps.setTimestamp(parameterIndex++, timeStamp, cal);
					ps.setTimestamp(parameterIndex++, timeStamp, cal);
					ps.setString(parameterIndex++, insightLayout);
					ps.setBoolean(parameterIndex++, cacheable);
					ps.setInt(parameterIndex++, cacheMinutes);
					if(cacheCron == null) {
						ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
					} else {
						ps.setString(parameterIndex++, cacheCron);
					}
					if(cachedOn == null) {
						ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
					} else {
						ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(cachedOn.getLocalDateTime()), cal);
					}

					ps.setBoolean(parameterIndex++, cacheEncrypt);

					// **** WITH RECENT UPDATES - THE RAW WRAPPER SHOULD NOT BE GIVING US BACK A CLOB
					// need to determine if our input is a clob
					// and if the database allows a clob data type
					// use the utility method generated
//					RDBMSUtility.handleInsertionOfClobInput(securityDb, securityQueryUtil, ps, parameterIndex++, pixelObject, securityGson);
					securityQueryUtil.handleInsertionOfClob(ps.getConnection(), ps, pixelObject, parameterIndex++, securityGson);
					
					
					if(schemaName == null) {
						ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
					} else {
						ps.setString(parameterIndex++, schemaName);
					}
					
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
		deleteQuery = "DELETE FROM INSIGHTMETA WHERE PROJECTID='" + projectId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		try {
			ps = securityDb.bulkInsertPreparedStatement(
					new String[]{"INSIGHTMETA","PROJECTID","INSIGHTID","METAKEY","METAVALUE","METAORDER"});
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
					ps.setString(parameterIndex++, projectId);
					ps.setString(parameterIndex++, row[0].toString());
					ps.setString(parameterIndex++, row[1].toString());

					// need to determine if our input is a clob
					// and if the database allows a clob data type
					// use the utility method generated
					Object metaValue = raw[2];
//					RDBMSUtility.handleInsertionOfClobInput(securityDb, securityQueryUtil, ps, parameterIndex++, metaValue, securityGson);
					securityQueryUtil.handleInsertionOfClob(ps.getConnection(), ps, metaValue, parameterIndex++, securityGson);
					
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
		if(!projectLoaded && rne != null) {
			rne.closeDB();
		}
		
		if(reloadInsights) {
			logger.info("Modifying force reload to false");
			Utility.changePropMapFileValue(smssFile, Constants.RELOAD_INSIGHTS, "false");	
			
			// need to remove existing insights w/ permissions that do not exist anymore
			if(existingInsightPermissions && !insightPermissionIds.isEmpty()) {
				logger.info("Removing insights with permissions that no longer exist");
				String deleteInsightPermissionQuery = "DELETE FROM USERINSIGHTPERMISSION "
					+ "WHERE PROJECTID='" + projectId + "'"
					+ " AND INSIGHTID " + createFilter(insightPermissionIds);
				try {
					securityDb.removeData(deleteInsightPermissionQuery);
					securityDb.commit();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		logger.info("Finished adding project = " + Utility.cleanLogString(projectId));
	}
	
	/**
	 * Utility method to get the database type and cost for storage
	 * @param prop
	 * @return
	 */
	public static String[] getDatabaseTypeAndCost(Properties prop) {
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
	 * Delete just the insights for a project
	 * @param appId
	 */
	public static void deleteInsightsFromProjectForRecreation(String projectId) {
		projectId = RdbmsQueryBuilder.escapeForSQLStatement(projectId);
		String deleteQuery = "DELETE FROM INSIGHT WHERE PROJECTID='" + projectId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * Delete all values
	 * @param databaseId
	 */
	public static void deleteDatabase(String databaseId) {
		List<String> deletes = new Vector<>();
		deletes.add("DELETE FROM ENGINE WHERE ENGINEID=?");
//		deletes.add("DELETE FROM INSIGHT WHERE ENGINEID=?");
		deletes.add("DELETE FROM ENGINEPERMISSION WHERE ENGINEID=?");
		deletes.add("DELETE FROM ENGINEMETA WHERE ENGINEID=?");
//		deletes.add("DELETE FROM WORKSPACEENGINE WHERE ENGINEID=?");
//		deletes.add("DELETE FROM ASSETENGINE WHERE ENGINEID=?");

		for(String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(deleteQuery);
				ps.setString(1, databaseId);
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
	
	/**
	 * Delete all values
	 * @param projectId
	 */
	public static void deleteProject(String projectId) {
		List<String> deletes = new Vector<>();
		deletes.add("DELETE FROM PROJECT WHERE PROJECTID=?");
		deletes.add("DELETE FROM INSIGHT WHERE PROJECTID=?");
		deletes.add("DELETE FROM PROJECTPERMISSION WHERE PROJECTID=?");
		deletes.add("DELETE FROM PROJECTMETA WHERE PROJECTID=?");
		deletes.add("DELETE FROM WORKSPACEENGINE WHERE PROJECTID=?");
		deletes.add("DELETE FROM ASSETENGINE WHERE PROJECTID=?");
		// TODO: add the other tables...

		for(String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(deleteQuery);
				ps.setString(1, projectId);
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
	 * Adding database
	 */
	
	/**
	 * Add a database into the security database
	 * Default to set as not global
	 */
	public static void addDatabase(String databaseId, String databaseName, String dbType, String dbCost) {
		addDatabase(databaseId, databaseName, dbType, dbCost, !securityEnabled);
	}
	
	public static void addDatabase(String databaseId, String databaseName, String dbType, String dbCost, boolean global) {
		String query = "INSERT INTO ENGINE (ENGINENAME, ENGINEID, TYPE, COST, GLOBAL, DISCOVERABLE) "
				+ "VALUES (?,?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, databaseName);
			ps.setString(parameterIndex++, databaseId);
			ps.setString(parameterIndex++, dbType);
			ps.setString(parameterIndex++, dbCost);
			ps.setBoolean(parameterIndex++, global);
			ps.setBoolean(parameterIndex++, false);
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
	
	public static void updateDatabase(String databaseId, String databaseName, String dbType, String dbCost, boolean global, boolean discoverable) {
		String query = "UPDATE ENGINE SET ENGINENAME=?, TYPE=?, COST=?, GLOBAL=?, DISCOVERABLE=? WHERE ENGINEID=?";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, databaseName);
			ps.setString(parameterIndex++, dbType);
			ps.setString(parameterIndex++, dbCost);
			ps.setBoolean(parameterIndex++, global);
			ps.setBoolean(parameterIndex++, discoverable);
			ps.setString(parameterIndex++, databaseId);
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
	
	public static void addDatabaseOwner(String databaseId, String userId) {
		String query = "INSERT INTO ENGINEPERMISSION (USERID, PERMISSION, ENGINEID, VISIBILITY) VALUES (?,?,?,?)";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, userId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.OWNER.getId());
			ps.setString(parameterIndex++, databaseId);
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
	 * Set a database to be global
	 * @param databaseId
	 */
	public static void setDatabaseCompletelyGlobal(String databaseId) {
		{
			String update1 = "UPDATE ENGINE SET GLOBAL=? WHERE ENGINEID=?";
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(update1);
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, true);
				ps.setString(parameterIndex++, databaseId);
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
		
//		{
//			String update1 = "UPDATE INSIGHT SET GLOBAL=? WHERE ENGINEID=?";
//			PreparedStatement ps = null;
//			try {
//				ps = securityDb.getPreparedStatement(update1);
//				int parameterIndex = 1;
//				ps.setBoolean(parameterIndex++, true);
//				ps.setString(parameterIndex++, databaseId);
//				ps.execute();
//				securityDb.commit();
//			} catch (SQLException e) {
//				logger.error(Constants.STACKTRACE, e);
//			} finally {
//				if(ps != null) {
//					try {
//						ps.close();
//						if(securityDb.isConnectionPooling()) {
//							try {
//								ps.getConnection().close();
//							} catch (SQLException e) {
//								logger.error(Constants.STACKTRACE, e);
//							}
//						}
//					} catch (SQLException e) {
//						logger.error(Constants.STACKTRACE, e);
//					}
//				}
//			}
//		}
	}
	
	
	
	/*
	 * Adding project
	 */
	
	/**
	 * Add a project into the security database
	 * Default to set as not global
	 */
	public static void addProject(String projectId, String projectName, String projectType, String projectCost) {
		addProject(projectId, projectName, projectType, projectCost, !securityEnabled);
	}
	
	public static void addProject(String projectID, String projectName, String projectType, String projectCost, boolean global) {
		String query = "INSERT INTO PROJECT (PROJECTNAME, PROJECTID, TYPE, COST, GLOBAL, DISCOVERABLE) "
				+ "VALUES (?,?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, projectName);
			ps.setString(parameterIndex++, projectID);
			ps.setString(parameterIndex++, projectType);
			ps.setString(parameterIndex++, projectCost);
			ps.setBoolean(parameterIndex++, global);
			ps.setBoolean(parameterIndex++, false);
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
	
	public static void updateProject(String projectID, String projectName, String projectType, String projectCost, boolean global) {
		String query = "UPDATE PROJECT SET PROJECTNAME=?, TYPE=?, COST=?, GLOBAL=? WHERE PROJECTID=?";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, projectName);
			ps.setString(parameterIndex++, projectType);
			ps.setString(parameterIndex++, projectCost);
			ps.setBoolean(parameterIndex++, global);
			ps.setString(parameterIndex++, projectID);
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
	
	public static void addProjectOwner(String projectId, String userId) {
		String query = "INSERT INTO PROJECTPERMISSION (USERID, PERMISSION, PROJECTID, VISIBILITY) VALUES (?,?,?,?)";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, userId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.OWNER.getId());
			ps.setString(parameterIndex++, projectId);
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
	 * Set a project and all its insights to be global
	 * @param projectId
	 */
	public static void setProjectCompletelyGlobal(String projectId) {
		{
			String update1 = "UPDATE PROJECT SET GLOBAL=? WHERE PROJECTID=?";
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(update1);
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, true);
				ps.setString(parameterIndex++, projectId);
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
			String update1 = "UPDATE INSIGHT SET GLOBAL=? WHERE PROJECTID=?";
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(update1);
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, true);
				ps.setString(parameterIndex++, projectId);
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
		if(newUser.getId() == null || newUser.getId().isEmpty()) {
			throw new IllegalArgumentException("User id for the token is null or empty. Must provide a valid id.");
		}
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
					Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
					java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());

					String updateQuery = "UPDATE SMSS_USER SET ID=?, NAME=?, USERNAME=?, EMAIL=?, TYPE=?, LASTLOGIN=? "
							+ "WHERE ID=?";
					PreparedStatement ps = null;
					try {
						int parameterIndex = 1;
						ps = securityDb.getPreparedStatement(updateQuery);
						ps.setString(parameterIndex++, newId);
						if(newUser.getName() == null) {
							ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
						} else {
							ps.setString(parameterIndex++, newUser.getName());
						}
						if(newUser.getUsername() == null) {
							ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
						} else {
							ps.setString(parameterIndex++, newUser.getUsername());
						}
						if(newUser.getEmail() == null) {
							ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
						} else {
							ps.setString(parameterIndex++, newUser.getEmail());
						}
						ps.setString(parameterIndex++, newUser.getProvider().toString());
						ps.setTimestamp(parameterIndex++, timestamp, cal);
						ps.setString(parameterIndex++, oldId);
						ps.execute();
						if(!ps.getConnection().getAutoCommit()) {
							ps.getConnection().commit();
						}
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					} finally {
						if(ps != null) {
							ps.close();
						}
						if(ps != null && securityDb.isConnectionPooling()) {
							ps.getConnection().close();
						}
					}
				}
				
				// need to update any other permissions that were set for this user
				String[] queries = new String[] {
						"UPDATE ENGINEPERMISSION SET USERID=? WHERE USERID=?",
						"UPDATE PROJECTPERMISSION SET USERID=? WHERE USERID=?",
						"UPDATE USERINSIGHTPERMISSION SET USERID=? WHERE USERID=?",
				};
				for(String updateQuery : queries) {
					PreparedStatement ps = null;
					try {
						int parameterIndex = 1;
						ps = securityDb.getPreparedStatement(updateQuery);
						ps.setString(parameterIndex++, newId);
						ps.setString(parameterIndex++, oldId);
						ps.execute();
						if(!ps.getConnection().getAutoCommit()) {
							ps.getConnection().commit();
						}
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					} finally {
						if(ps != null) {
							ps.close();
						}
						if(ps != null && securityDb.isConnectionPooling()) {
							ps.getConnection().close();
						}
					}
				}
				
			} else {
				// not added by admin
				// lets see if he exists or not
				boolean userExists = SecurityQueryUtils.checkUserExist(newUser.getId());
				if (userExists) {
					logger.info("User " + newUser.getId() + " already exists");
					return validateUserLogin(newUser);
				}

				// need to synchronize the adding of new users
				// so that we do not enter here from different threads 
				// and add the same user twice
				synchronized(SecurityUpdateUtils.class) {
					
					// also add the max user limit check
					String userLimitStr = DIHelper.getInstance().getProperty(Constants.MAX_USER_LIMIT);
					if(userLimitStr != null && !userLimitStr.trim().isEmpty()) {
						try {
							int userLimit = Integer.parseInt(userLimitStr);
							int currentUserCount = SecurityQueryUtils.getApplicationUserCount();
							
							if(userLimit > 0 && currentUserCount+1 > userLimit) {
								throw new SemossPixelException("User limit exceeded the max value of " + userLimit);
							}
						} catch(NumberFormatException e) {
							logger.error(Constants.STACKTRACE, e);
							logger.error("User limit is not a valid numeric value");
						}
					}
					
					// need to prevent 2 threads attempting to add the same user
					userExists = SecurityQueryUtils.checkUserExist(newUser.getId());
					if(!userExists) {
						Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
						java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());

						query = "INSERT INTO SMSS_USER (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PUBLISHER, EXPORTER, DATECREATED, LASTLOGIN) "
								+ "VALUES (?,?,?,?,?,?,?,?,?,?)";
						PreparedStatement ps = null;
						try {
							ps = securityDb.getPreparedStatement(query);
							int parameterIndex = 1;
							ps.setString(parameterIndex++, newUser.getId());
							if(newUser.getName() == null) {
								ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
							} else {
								ps.setString(parameterIndex++, newUser.getName());
							}
							if(newUser.getUsername() == null) {
								ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
							} else {
								ps.setString(parameterIndex++, newUser.getUsername());
							}
							if(newUser.getEmail() == null) {
								ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
							} else {
								ps.setString(parameterIndex++, newUser.getEmail());
							}
							ps.setString(parameterIndex++, newUser.getProvider().toString());
							// we never add ADMIN this way
							ps.setBoolean(parameterIndex++, false);
							ps.setBoolean(parameterIndex++, !adminSetPublisher());
							ps.setBoolean(parameterIndex++, !adminSetExporter());
							ps.setTimestamp(parameterIndex++, timestamp, cal);
							ps.setTimestamp(parameterIndex++, timestamp, cal);
							ps.execute();
							if(!ps.getConnection().getAutoCommit()) {
								ps.getConnection().commit();
							}
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
	 * 
	 * @param newUser
	 * @return
	 * @throws Exception 
	 */
	public static boolean validateUserLogin(AccessToken newUser) throws Exception {
		// make sure user is not locked out
		Object[] lastLoginDetails = SecurityQueryUtils.getUserLockAndLastLoginAndLastPassReset(newUser.getId(), newUser.getProvider());
		if(lastLoginDetails != null) {
			Boolean isLocked = (Boolean) lastLoginDetails[0];
			if(isLocked == null) {
				isLocked = false;
			}
			SemossDate lastLogin = null;
			SemossDate lastPassReset = null;
			if(lastLoginDetails[1] != null) {
				Object potentialDateValue = lastLoginDetails[1];
				if(potentialDateValue instanceof SemossDate) {
					lastLogin = (SemossDate) potentialDateValue;
				} else if(potentialDateValue instanceof String) {
					lastLogin = SemossDate.genTimeStampDateObj(potentialDateValue + "");
				}
			}
			if(lastLoginDetails[2] != null) {
				Object potentialDateValue = lastLoginDetails[2];
				if(potentialDateValue instanceof SemossDate) {
					lastPassReset = (SemossDate) potentialDateValue;
				} else if(potentialDateValue instanceof String) {
					lastPassReset = SemossDate.genTimeStampDateObj(potentialDateValue + "");
				}
			}
			
			int daysToLock = PasswordRequirements.getInstance().getDaysToLock();
			int daysToResetPass = PasswordRequirements.getInstance().getPasswordExpirationDays();
			
			newUser.setLocked(isLocked);
			newUser.setLastLogin(lastLogin);
			newUser.setLastPasswordReset(lastPassReset);
			
			if(isLocked) {
				logger.info("User " + newUser.getId() + " is locked");
				return false;
			} 
			
			if(daysToLock > 0 && lastLogin != null) {
				// check to make sure user is not locked
				TimeZone tz = TimeZone.getTimeZone(Utility.getApplicationTimeZoneId());
				LocalDateTime currentTime = Instant.ofEpochMilli(new Date().getTime()).atZone(tz.toZoneId()).toLocalDateTime();
				if(currentTime.isAfter(lastLogin.getLocalDateTime().plusDays(daysToLock))) {
					logger.info("User " + newUser.getId() + " is now locked due to not logging in for over " + daysToLock + " days");
					// we should lock the account
					SecurityUpdateUtils.lockUserAccount(true, newUser.getId(), newUser.getProvider());
					newUser.setLocked(true);
					return false;
				}
			}
			
//			if(daysToResetPass > 0) {
//				// check to make sure user is not locked
//				TimeZone tz = TimeZone.getTimeZone(Utility.getApplicationTimeZoneId());
//				LocalDateTime currentTime = Instant.ofEpochMilli(new Date().getTime()).atZone(tz.toZoneId()).toLocalDateTime();
//				if(currentTime.isAfter(lastLogin.getLocalDateTime().plusDays(daysToResetPass))) {
//					logger.info("User " + newUser.getId() + " is now locked due to not resetting password for over " + daysToResetPass + " days");
//					// we should lock the account
//					SecurityUpdateUtils.lockUserAccount(true, newUser.getId(), newUser.getProvider());
//					newUser.setLocked(true);
//					return false;
//				}
//			}
		}
		
		// if not locked
		// update the last success login
		if(!newUser.isLocked()) {
			SecurityUpdateUtils.updateUserLastLogin(newUser.getId(), newUser.getProvider());
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
	
	public static void lockUserAccount(boolean isLocked, String userId, AuthProvider type) {
		String updateQuery = "UPDATE SMSS_USER SET LOCKED=? WHERE ID=? AND TYPE=?";
		PreparedStatement ps = null;
		try {
			int parameterIndex = 1;
			ps = securityDb.getPreparedStatement(updateQuery);
			ps.setBoolean(parameterIndex++, isLocked);
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, type.toString());
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(ps != null && securityDb.isConnectionPooling()) {
				try {
					ps.getConnection().close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	public static void updateUserLastLogin(String userId, AuthProvider type) {
		// update the user last login
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
		String updateQuery = "UPDATE SMSS_USER SET LASTLOGIN=? WHERE ID=? AND TYPE=?";
		PreparedStatement ps = null;
		try {
			int parameterIndex = 1;
			ps = securityDb.getPreparedStatement(updateQuery);
			ps.setTimestamp(parameterIndex++, timestamp, cal);
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, type.toString());
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(ps != null && securityDb.isConnectionPooling()) {
				try {
					ps.getConnection().close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
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
	 * @param exporter
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static boolean registerUser(String id, String name, String email, String password, String type, String phone, String phoneextension, String countrycode, boolean admin, boolean publisher, boolean exporter) throws IllegalArgumentException {
		boolean isExistingUser = SecurityQueryUtils.checkUserExist(id);
		if(isExistingUser) {
			return false;
		}
		
		// also add the max user limit check
		String userLimitStr = DIHelper.getInstance().getProperty(Constants.MAX_USER_LIMIT);
		if(userLimitStr != null && !userLimitStr.trim().isEmpty()) {
			try {
				int userLimit = Integer.parseInt(userLimitStr);
				int currentUserCount = SecurityQueryUtils.getApplicationUserCount();
				
				if(userLimit > 0 && currentUserCount+1 > userLimit) {
					throw new SemossPixelException("User limit exceeded the max value of " + userLimit);
				}
			} catch(NumberFormatException e) {
				logger.error(Constants.STACKTRACE, e);
				logger.error("User limit is not a valid numeric value");
			}
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
		if(phone == null) phone = "";
		if(phoneextension == null) phoneextension = "";
		if(countrycode == null) countrycode = "";
		 
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
		
		String query = "INSERT INTO SMSS_USER (ID, USERNAME, NAME, EMAIL, PASSWORD, SALT, TYPE, "
				+ "PHONE, PHONEEXTENSION, COUNTRYCODE, ADMIN, PUBLISHER, EXPORTER, DATECREATED) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
			ps.setString(parameterIndex++, phone);
			ps.setString(parameterIndex++, phoneextension);
			ps.setString(parameterIndex++, countrycode);
			ps.setBoolean(parameterIndex++, admin);
			ps.setBoolean(parameterIndex++, publisher);
			ps.setBoolean(parameterIndex++, exporter);
			ps.setTimestamp(parameterIndex++, timestamp, cal);
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
	 * @param databaseId
	 * @param visibility
	 * @throws SQLException 
	 * @throws IllegalAccessException 
	 */
	public static void setDbVisibility(User user, String databaseId, boolean visibility) throws SQLException, IllegalAccessException {
		if(!SecurityUserDatabaseUtils.userCanViewDatabase(user, databaseId)) {
			throw new IllegalAccessException("The user doesn't have the permission to modify his visibility of this app.");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIdFilters));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()){
				UpdateQueryStruct uqs = new UpdateQueryStruct();
				uqs.setEngine(securityDb);
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
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
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO ENGINEPERMISSION "
						+ "(USERID, ENGINEID, VISIBILITY, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if(ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set app visibility");
				}
				try {
					// we will set the permission to read only
					for(AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, databaseId);
						ps.setBoolean(parameterIndex++, visibility);
						// default favorite as false
						ps.setBoolean(parameterIndex++, false);
						ps.setInt(parameterIndex++, 3);
	
						ps.addBatch();
					}
					ps.executeBatch();
				} catch(Exception e) {
					logger.error(Constants.STACKTRACE, e);
					throw e;
				} finally {
					if(ps != null) {
						ps.close();
					}
				}
			}
			
			securityDb.commit();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
	}
	
	/**
	 * Change the user favorite (is favorite / not favorite) for a database. Without removing its permissions.
	 * @param user
	 * @param databaseId
	 * @param visibility
	 * @throws SQLException 
	 * @throws IllegalAccessException 
	 */
	public static void setDbFavorite(User user, String databaseId, boolean isFavorite) throws SQLException, IllegalAccessException {
		if (!SecurityUserDatabaseUtils.userCanViewDatabase(user, databaseId)) {
			throw new IllegalAccessException("The user doesn't have the permission to modify his visibility of this database.");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIdFilters));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()){
				UpdateQueryStruct uqs = new UpdateQueryStruct();
				uqs.setEngine(securityDb);
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIdFilters));

				List<IQuerySelector> selectors = new Vector<>();
				selectors.add(new QueryColumnSelector("ENGINEPERMISSION__FAVORITE"));
				List<Object> values = new Vector<>();
				values.add(isFavorite);
				uqs.setSelectors(selectors);
				uqs.setValues(values);
				
				UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(uqs);
				String updateQuery = updateInterp.composeQuery();
				securityDb.insertData(updateQuery);
				
			} else {
				// need to insert
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO ENGINEPERMISSION "
						+ "(USERID, ENGINEID, VISIBILITY, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if(ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set app visibility");
				}
				try {
					// we will set the permission to read only
					for(AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, databaseId);
						// default visibility as true
						ps.setBoolean(parameterIndex++, true);
						ps.setBoolean(parameterIndex++, isFavorite);
						ps.setInt(parameterIndex++, 3);
	
						ps.addBatch();
					}
					ps.executeBatch();
				} catch(Exception e) {
					logger.error(Constants.STACKTRACE, e);
					throw e;
				} finally {
					if(ps != null) {
						ps.close();
					}
				}
			}
			
			// commit regardless of insert or update
			securityDb.commit();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
	}
	
	/**
	 * Set if the database is public to all users on this instance
	 * @param user
	 * @param databaseId
	 * @param isPublic
	 * @return
	 * @throws SQLException 
	 */
	public static boolean makeRequest(User user, String databaseId, int requestedPermission) throws SQLException {
		// make sure this person isn't requesting multiple times
		if(!SecurityDatabaseUtils.getGlobalDatabaseIds().contains(databaseId)) {
			throw new IllegalArgumentException("Cannot request access to an app that is not public");
		}
		
		StringBuilder builder = new StringBuilder();
		String[] colNames = new String[]{"ID", "SUBMITTEDBY", "ENGINE", "PERMISSION"};
		String[] types = new String[]{"VARCHAR(100)", "VARCHAR(255)", "VARCHAR(255)", "INT"};
		
		List<Map<String, Object>> existingRequests = SecurityQueryUtils.getUserAccessRequestsByProvider(user, databaseId);
		if(existingRequests.isEmpty()) {
			// brand new requests
			String requestId = UUID.randomUUID().toString();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider provider : logins) {
				Object[] data = new Object[]{requestId, user.getAccessToken(provider).getId(), databaseId, requestedPermission};
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
						Object[] data = new Object[]{requestId, userId, databaseId, requestedPermission};				
						builder.append(RdbmsQueryBuilder.makeInsert("ACCESSREQUEST", colNames, types, data)).append(";");
					}
					securityDb.insertData(builder.toString());
				} else {
					// we will update all the ids to be the same
					String newRequestId = UUID.randomUUID().toString();
					// first insert the new records
					for(String userId : userIds) {
						Object[] data = new Object[]{newRequestId, userId, databaseId, requestedPermission};				
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
