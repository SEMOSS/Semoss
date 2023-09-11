package prerna.auth.utils;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.parser.ParserException;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.ProjectUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class SecurityProjectUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityProjectUtils.class);

	/**
	 * Add an entire project into the security db - Expectation is not to call this method but addProject(projectId, boolean global = true)
	 * @param projectId
	 * @throws Exception 
	 */
	public static void addProject(String projectId, User user) throws Exception {
		// default project is not global
		addProject(projectId, false, user);
	}
	
	/**
	 * Add an entire project into the security db
	 * @param appId
	 * @throws Exception 
	 */
	public static void addProject(String projectId, boolean global, User user) throws Exception {
		String smssFile = DIHelper.getInstance().getProjectProperty(projectId + "_" + Constants.STORE) + "";
		Properties prop = Utility.loadProperties(smssFile);

		String projectName = prop.getProperty(Constants.PROJECT_ALIAS);
		if(projectName == null) {
			projectName = projectId;
		}
		
		boolean hasPortal = Boolean.parseBoolean(prop.getProperty(Settings.PUBLIC_HOME_ENABLE));
		String portalName = prop.getProperty(Settings.PORTAL_NAME);
		
		boolean reloadInsights = false;
		if(prop.containsKey(Constants.RELOAD_INSIGHTS)) {
			String booleanStr = prop.get(Constants.RELOAD_INSIGHTS).toString();
			reloadInsights = Boolean.parseBoolean(booleanStr);
		}
		
		// TODO: we do not need type and cost for project
		String[] typeAndCost = new String[] {"",""};
		boolean projectExists = containsProjectId(projectId);
 		if(projectExists && !reloadInsights) {
			classLogger.info("Security database already contains project with unique id = " + Utility.cleanLogString(SmssUtilities.getUniqueName(prop)));
			return;
		} else if(!projectExists) {
			addProject(projectId, projectName, typeAndCost[0], typeAndCost[1], hasPortal, portalName, global, user);
		} else if(projectExists) {
			// delete values if currently present
			deleteInsightsFromProjectForRecreation(projectId);
			// update project properties anyway ... in case global was shifted for example
			updateProject(projectId, projectName, typeAndCost[0], typeAndCost[1], hasPortal, portalName, global);
		}
		
		classLogger.info("Security database going to add project with alias = " + Utility.cleanLogString(projectName));
		
		// load just the insights database
		// first see if engine is already loaded
		boolean projectLoaded = false;
		RDBMSNativeEngine rne = null;
		if(Utility.projectLoaded(projectId)) {
			rne = Utility.getProject(projectId).getInsightDatabase();
		} else {
			rne  = ProjectHelper.loadInsightsEngine(prop, classLogger);
		}
		
		// i need to delete any current insights for the project
		// before i start to insert new insights
		String deleteQuery = "DELETE FROM INSIGHT WHERE PROJECTID='" + projectId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
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
			classLogger.info("Reloading app. Retrieving existing insights with permissions");
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
			classLogger.error(Constants.STACKTRACE, e);
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
						classLogger.info("Executing batch .... row num = " + count);
						ps.executeBatch();
					}
					
					if(reloadInsights && insightPermissionIds != null && existingInsightPermissions) {
						insightPermissionIds.remove(insightId);
					}
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// well, we are done looping through now
		classLogger.info("Executing final batch .... row num = " + count);
		try {
			ps.executeBatch();
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		// commit
		try {
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		
		count = 0;
		// same for insight meta
		// i need to delete any current insights for the app
		// before i start to insert new insights
		deleteQuery = "DELETE FROM INSIGHTMETA WHERE PROJECTID='" + projectId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
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
						classLogger.info("Executing batch .... row num = " + count);
						ps.executeBatch();
					}
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// well, we are done looping through now
		classLogger.info("Executing final batch .... row num = " + count);
		try {
			ps.executeBatch();
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		try {
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		
		// close the connection to the insights
		// if the engine is not already loaded 
		// since the open method will load it
		if(!projectLoaded && rne != null) {
			rne.close();
		}
		
		if(reloadInsights) {
			classLogger.info("Modifying force reload to false");
			try {
				Utility.changePropertiesFileValue(smssFile, Constants.RELOAD_INSIGHTS, "false");
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}	
			
			// need to remove existing insights w/ permissions that do not exist anymore
			if(existingInsightPermissions && !insightPermissionIds.isEmpty()) {
				
				//TODO:
				//TODO:
				//TODO:
				//TODO:
				
				classLogger.info("Removing insights with permissions that no longer exist");
				String deleteInsightPermissionQuery = "DELETE FROM USERINSIGHTPERMISSION "
					+ "WHERE PROJECTID='" + projectId + "'"
					+ " AND INSIGHTID " + createFilter(insightPermissionIds);
				try {
					securityDb.removeData(deleteInsightPermissionQuery);
					securityDb.commit();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		classLogger.info("Finished adding project = " + Utility.cleanLogString(projectId));
	}
	
	/**
	 * 
	 * @param projectID
	 * @param projectName
	 * @param projectType
	 * @param projectCost
	 * @param hasPortal
	 * @param portalName
	 * @param global
	 * @param user
	 */
	public static void addProject(String projectID, String projectName, 
			String projectType, String projectCost, 
			boolean hasPortal, String portalName,
			boolean global, User user) {
		String query = "INSERT INTO PROJECT (PROJECTNAME, PROJECTID, TYPE, COST, GLOBAL, DISCOVERABLE, CREATEDBY, CREATEDBYTYPE, DATECREATED, HASPORTAL, PORTALNAME) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

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
			if(user != null) {
				AuthProvider ap = user.getPrimaryLogin();
				AccessToken token = user.getAccessToken(ap);
				ps.setString(parameterIndex++, token.getId());
				ps.setString(parameterIndex++, ap.toString());
			} else {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			}
			ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
			ps.setBoolean(parameterIndex++, hasPortal);
			if(portalName != null) {
				ps.setString(parameterIndex++, portalName);
			} else {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			}
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
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
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	public static void updateProject(String projectID, String projectName, String projectType, String projectCost, boolean hasPortal, String portalName, boolean global) {
		String query = "UPDATE PROJECT SET PROJECTNAME=?, TYPE=?, COST=?, GLOBAL=?, HASPORTAL=?, PORTALNAME=? WHERE PROJECTID=?";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, projectName);
			ps.setString(parameterIndex++, projectType);
			ps.setString(parameterIndex++, projectCost);
			ps.setBoolean(parameterIndex++, global);
			ps.setBoolean(parameterIndex++, hasPortal);
			if(portalName != null) {
				ps.setString(parameterIndex++, portalName);
			} else {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			}
			ps.setString(parameterIndex++, projectID);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Delete just the insights for a project
	 * @param appId
	 */
	public static void deleteInsightsFromProjectForRecreation(String projectId) {
		String deleteQuery = "DELETE FROM INSIGHT WHERE PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred deleting the insights for project " + projectId);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param projectId
	 * @return
	 * @throws Exception 
	 */
	public static File createInsightsDatabase(String projectId, String folderPath) throws Exception {
		
		// TODO: potentially take into consideration playsheet legacy insights
		
		IProject project = Utility.getProject(projectId);
		RdbmsTypeEnum insightType = project.getInsightDatabase().getQueryUtil().getDbType();
		
		RDBMSNativeEngine newInsightDatabase = ProjectUtils.generateInsightsDatabase(insightType, folderPath);
		ProjectUtils.runInsightCreateTableQueries(newInsightDatabase);
		
		InsightAdministrator admin = new InsightAdministrator(newInsightDatabase);
		{
			boolean error = false;
			PreparedStatement insertPs = null;
			
			String iprefix = "INSIGHT__";
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector(iprefix+"INSIGHTID"));
			qs.addSelector(new QueryColumnSelector(iprefix+"INSIGHTNAME"));
			qs.addSelector(new QueryColumnSelector(iprefix+"LAYOUT"));
			qs.addSelector(new QueryColumnSelector(iprefix+"CREATEDON"));
			qs.addSelector(new QueryColumnSelector(iprefix+"LASTMODIFIEDON"));
			qs.addSelector(new QueryColumnSelector(iprefix+"GLOBAL"));
			qs.addSelector(new QueryColumnSelector(iprefix+"CACHEABLE"));
			qs.addSelector(new QueryColumnSelector(iprefix+"CACHEMINUTES"));
			qs.addSelector(new QueryColumnSelector(iprefix+"CACHECRON"));
			qs.addSelector(new QueryColumnSelector(iprefix+"CACHEDON"));
			qs.addSelector(new QueryColumnSelector(iprefix+"CACHEENCRYPT"));
			qs.addSelector(new QueryColumnSelector(iprefix+"RECIPE"));
			qs.addSelector(new QueryColumnSelector(iprefix+"SCHEMANAME"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(iprefix+"PROJECTID", "==", projectId));
			IRawSelectWrapper wrapper = null;
			try {
				insertPs = admin.getAddInsightPreparedStatement();

				wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
	
				while(wrapper.hasNext()) {
					Object[] row = wrapper.next().getValues();
					
					int index = 0;
					String insightId = (String) row[index++];
					String insightName = (String) row[index++];
					String insightLayout = (String) row[index++];
					SemossDate createdOn = (SemossDate) row[index++];
					SemossDate lastModifiedOn = (SemossDate) row[index++];
					Boolean global = (Boolean) row[index++];
					Boolean cacheable = (Boolean) row[index++];
					int cacheMinutes = ((Number) row[index++]).intValue();
					String cacheCron = (String) row[index++];
					SemossDate sdCachedOn = (SemossDate) row[index++];
					LocalDateTime cachedOn = null;
					if(sdCachedOn != null) {
						cachedOn = sdCachedOn.getLocalDateTime();
					}
					boolean cacheEncrypt = (Boolean) row[index++];
					String pixelRecipe = (String) row[index++];
					String schemaName = (String) row[index++];
					
					List<String> pixelList = null;
	
					if(pixelRecipe != null && !pixelRecipe.isEmpty() && !pixelRecipe.equals("null")) {
						List<String> pixel = securityGson.fromJson(pixelRecipe, List.class);
						int pixelSize = pixel.size();
						pixelList = new ArrayList<>(pixelSize);
						for(int i = 0; i < pixelSize; i++) {
							String pixelString = pixel.get(i).toString();
							List<String> breakdown;
							try {
								breakdown = PixelUtility.parsePixel(pixelString);
								pixelList.addAll(breakdown);
							} catch (ParserException | LexerException | IOException e) {
								classLogger.error(Constants.STACKTRACE, e);
								throw new IllegalArgumentException("Error occurred parsing the pixel expression");
							}
						}
					} else {
						classLogger.warn("Cannot write insight id '"+insightId+"' with no pixel recipe");
						continue;
					}
	
					admin.batchInsight(insertPs, insightId, insightName, insightLayout, pixelList, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
				}
				
				insertPs.executeBatch();
				if(!insertPs.getConnection().getAutoCommit()) {
					insertPs.getConnection().commit();
				}
			} catch(Exception e) {
				error = true;
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error occured creating the insights database");
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
				if(error) {
					try {
						newInsightDatabase.close();
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
					String databaseFileLocation = newInsightDatabase.getSmssProp().getProperty(AbstractSqlQueryUtil.HOSTNAME);
					File databaseFile = new File(databaseFileLocation);
					if(databaseFile.exists() && databaseFile.isFile()) {
						databaseFile.delete();
					}
				}
			}
		}
		{
			PreparedStatement insertPs = null;

			String iprefix = "INSIGHTMETA__";
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector(iprefix+"INSIGHTID"));
			qs.addSelector(new QueryColumnSelector(iprefix+"METAKEY"));
			qs.addSelector(new QueryColumnSelector(iprefix+"METAVALUE"));
			qs.addSelector(new QueryColumnSelector(iprefix+"METAORDER"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(iprefix+"PROJECTID", "==", projectId));

			IRawSelectWrapper wrapper = null;
			try {
				insertPs = admin.getAddInsightMetaPreparedStatement();
				
				wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
	
				while(wrapper.hasNext()) {
					Object[] row = wrapper.next().getValues();
					
					int index = 0;
					String insightId = (String) row[index++];
					String metaKey = (String) row[index++];
					String metaValue = (String) row[index++];
					int metaOrder = ((Number) row[index++]).intValue();

					admin.batchInsightMetadata(insertPs, insightId, metaKey, metaValue, metaOrder);
				}
				
				insertPs.executeBatch();
				if(!insertPs.getConnection().getAutoCommit()) {
					insertPs.getConnection().commit();
				}
			} catch(Exception e) {
				// insight metadata is not as important, log the error
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
			}
		}
		
		// close the db so we can move it
		newInsightDatabase.close();

		String databaseFileLocation = newInsightDatabase.getSmssProp().getProperty(AbstractSqlQueryUtil.HOSTNAME);
		File databaseFile = new File(databaseFileLocation);
		return databaseFile;
	}
	
	/**
	 * Try to reconcile and get the engine id
	 * @return
	 */
	public static String testUserProjectIdForAlias(User user, String potentialId) {
		List<String> ids = new ArrayList<String>();
		
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT PROJECTPERMISSION.PROJECTID "
//				+ "FROM PROJECTPERMISSION INNER JOIN PROJECT ON PROJECT.PROJECTID=PROJECTPERMISSION.PROJECTID "
//				+ "WHERE PROJECT.PROJECTNAME='" + potentialId + "' AND PROJECTPERMISSION.USERID IN " + userFilters;
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "==", potentialId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "inner.join");

		ids = QueryExecutionUtility.flushToListString(securityDb, qs);
		if(ids.isEmpty()) {
//			query = "SELECT DISTINCT PROJECT.PROJECTID FROM PROJECT WHERE PROJECT.PROJECTNAME='" + potentialId + "' AND PROJECT.GLOBAL=TRUE";

			qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "==", potentialId));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			
			ids = QueryExecutionUtility.flushToListString(securityDb, qs);
		}
		
		if(ids.size() == 1) {
			potentialId = ids.get(0);
		} else if(ids.size() > 1) {
			throw new IllegalArgumentException("There are 2 projects with the name " + potentialId + ". Please pass in the correct id to know which source you want to load from");
		}
		
		return potentialId;
	}
	
	/**
	 * Get the engine alias for a id
	 * @return
	 */
	public static String getProjectAliasForId(String id) {
//		String query = "SELECT PROJECTNAME FROM PROJECT WHERE PROJECTID='" + id + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", id));
		List<String> results = QueryExecutionUtility.flushToListString(securityDb, qs);
		if(results.isEmpty()) {
			return null;
		}
		return results.get(0);
	}
	
	/**
	 * Get user databases + global databases 
	 * @param userId
	 * @return
	 */
	public static List<String> getFullUserProjectIds(User user) {
		List<String> databaseList = SecurityUserProjectUtils.getFullUserProjectIds(user);
		databaseList.addAll(getGlobalProjectIds());
		return databaseList.stream().distinct().sorted().collect(Collectors.toList());
	}
	
	/**
	 * Get global databases
	 * @return
	 */
	public static Set<String> getGlobalProjectIds() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		return QueryExecutionUtility.flushToSetString(securityDb, qs, false);
	}

	/**
	 * Get what permission the user has for a given app
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static String getActualUserProjectPermission(User user, String projectId) {
		return SecurityUserProjectUtils.getActualUserProjectPermission(user, projectId);
	}
	
	/**
	 * Get a list of the project ids
	 * @return
	 */
	public static List<String> getAllProjectIds() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	/**
	 * Get markdown for a given project
	 * @param user
	 * @param projectId
	 * @return
	 */
	public static String getProjectMarkdown(User user, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", Constants.MARKDOWN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__PROJECTID", "==", projectId));
		{
			SelectQueryStruct qs1 = new SelectQueryStruct();
			qs1.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
			{
				OrQueryFilter orFilter = new OrQueryFilter();
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__DISCOVERABLE", "==", Arrays.asList(true, null), PixelDataType.BOOLEAN));
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
				qs1.addExplicitFilter(orFilter);
			}
			qs1.addRelation("PROJECT", "PROJECTPERMISSION", "join");
			IRelation subQuery = new SubqueryRelationship(qs1, "PROJECT", "join", new String[] {"PROJECT__PROJECTID", "PROJECTMETA__PROJECTID", "="});
			qs.addRelation(subQuery);
		}
		return QueryExecutionUtility.flushToString(securityDb, qs);
	}
	
	/**
	 * Get the project permissions for a specific user
	 * @param singleUserId
	 * @param projectId
	 * @return
	 */
	public static Integer getUserProjectPermission(String singleUserId, String projectId) {
		return SecurityUserProjectUtils.getUserProjectPermission(singleUserId, projectId);
	}
	
	/**
	 * Get the project permissions for a specific user
	 * @param singleUserId
	 * @param projectId
	 * @return
	 */
	public static Map<String, Integer> getUserProjectPermissions(List<String> userIds, String projectId) {
		Map<String, Integer> retMap = new HashMap<String, Integer>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = getUserProjectPermissionsWrapper(userIds, projectId);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String userId = (String) data[0];
				Integer permission = (Integer) data[1];
				retMap.put(userId, permission);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return retMap;
	}
	
	/**
	 * Get the project permissions for a specific user
	 * @param singleUserId
	 * @param projectId
	 * @return
	 */
	public static IRawSelectWrapper getUserProjectPermissionsWrapper(List<String> userIds, String projectId) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}

	/**
	 * See if specific project is global
	 * @return
	 */
	public static boolean projectIsGlobal(String projectId) {
		//		String query = "SELECT ENGINEID FROM ENGINE WHERE GLOBAL=TRUE and ENGINEID='" + engineId + "'";
		//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return false;
	}
	
	/**
	 * See if specific project is global
	 * @return
	 */
	public static boolean projectHasPortal(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__HASPORTAL", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return false;
	}

	/**
	 * 
	 * @param projectId
	 * @return
	 */
	public static SemossDate getPortalPublishedTimestamp(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return (SemossDate) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return null;
	}
	
	public static void setPortalPublish(User user, String projectId) {
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String updateQ = "UPDATE PROJECT SET PORTALPUBLISHED=?, PORTALPUBLISHEDUSER=?, PORTALPUBLISHEDTYPE=? WHERE PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int i = 1;
			ps.setTimestamp(i++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
			ps.setString(i++, token.getId());
			ps.setString(i++, token.getProvider().toString());
			ps.setString(i++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param projectId
	 * @return
	 */
	public static SemossDate getReactorCompilationTimestamp(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return (SemossDate) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return null;
	}
	
	public static void setReactorCompilation(User user, String projectId) {
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String updateQ = "UPDATE PROJECT SET REACTORSCOMPILED=?, REACTORSCOMPILEDUSER=?, REACTORSCOMPILEDTYPE=? WHERE PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int i = 1;
			ps.setTimestamp(i++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
			ps.setString(i++, token.getId());
			ps.setString(i++, token.getProvider().toString());
			ps.setString(i++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	
	/**
	 * Determine if the user is the owner of a project
	 * @param userFilters
	 * @param engineId
	 * @return
	 */
	public static boolean userIsOwner(User user, String projectId) {
		return SecurityUserProjectUtils.userIsOwner(getUserFiltersQs(user), projectId)
				|| SecurityGroupProjectUtils.userGroupIsOwner(user, projectId);
	}

	/**
	 * Determine if a user can view a project
	 * @param user
	 * @param projectId
	 * @return
	 */
	public static boolean userCanViewProject(User user, String projectId) {
		return SecurityUserProjectUtils.userCanViewProject(user, projectId)
				|| SecurityGroupProjectUtils.userGroupCanViewProject(user, projectId);
	}

	/**
	 * Determine if the user can modify the database
	 * @param projectId
	 * @param userId
	 * @return
	 */
	public static boolean userCanEditProject(User user, String projectId) {
		return SecurityUserProjectUtils.userCanEditProject(user, projectId)
				|| SecurityGroupProjectUtils.userGroupCanEditProject(user, projectId);
	}
	
	/**
	 * Get the request pending database permission for a specific user
	 * @param singleUserId
	 * @param projectId
	 * @return
	 */
	public static Integer getUserAccessRequestProjectPermission(String userId, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__REQUEST_USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__PROJECTID", "==", projectId));
		return QueryExecutionUtility.flushToInteger(securityDb, qs);
	}

	/**
	 * Get Project max permission for a user
	 * @param userId
	 * @param projectId
	 * @return
	 */
	static int getMaxUserProjectPermission(User user, String projectId) {
		//		String userFilters = getUserFilters(user);
		//		// query the database
		//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
		//				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters + " ORDER BY PERMISSION";
		//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addOrderBy(new QueryColumnOrderBySelector("PROJECTPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return AccessPermissionEnum.READ_ONLY.getId();
				}
				int permission = ((Number) val).intValue();
				return permission;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}		
		return AccessPermissionEnum.READ_ONLY.getId();
	}

	/**
	 * Retrieve the list of users for a given project with parameters
	 * @param user
	 * @param projectId
	 * @param userId
	 * @param permission
	 * @param limit
	 * @param offset
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getProjectUsers(User user, String projectId, String userId, String permission, long limit, long offset) throws IllegalAccessException {
		if(!userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("The user does not have access to view this project");
		}
		boolean hasUserId = userId != null && !(userId=userId.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission=permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		if (hasUserId) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "?like", userId));
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "==", AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "PROJECTPERMISSION", "inner.join");
		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("PERMISSION__ID"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		if(limit > 0) {
			qs.setLimit(limit);
		}
		if(offset > 0) {
			qs.setOffSet(offset);
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	public static long getProjectUsersCount(User user, String projectId, String userId, String permission) throws IllegalAccessException {
		if(!userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("The user does not have access to view this project");
		}
		boolean hasUserId = userId != null && !(userId=userId.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission=permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
        fSelector.setAlias("count");
        fSelector.setFunction(QueryFunctionHelper.COUNT);
        fSelector.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
        qs.addSelector(fSelector);
        qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		if (hasUserId) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "?like", userId));
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "==", AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "PROJECTPERMISSION", "inner.join");
		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "inner.join");
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}
	
	/**
	 * 
	 * @param projectId
	 * @return
	 */
	public static List<Map<String, Object>> getFullProjectOwnersAndEditors(String projectId) {
		return SecurityUserProjectUtils.getFullProjectOwnersAndEditors(projectId);
	}
	
	/**
	 * 
	 * @param projectId
	 * @param userId
	 * @param permission
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> getFullProjectOwnersAndEditors(String projectId, String userId, String permission, long limit, long offset) {
		return SecurityUserProjectUtils.getFullProjectOwnersAndEditors(projectId, userId, permission, limit, offset);
	}

	/**
	 * 
	 * @param user
	 * @param newUserId
	 * @param projectId
	 * @param permission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void addProjectUser(User user, String newUserId, String projectId, String permission) throws IllegalAccessException {
		// make sure user can edit the app
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}
				
		// make sure user doesn't already exist for this insight
		if(getUserProjectPermission(newUserId, projectId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this project. Please edit the existing permission level.");
		}
		
		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			int newPermissionLvl = AccessPermissionEnum.getIdByPermission(permission);

			// cannot give some owner permission if i am just an editor
			if(AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException("Cannot give owner level access to this project since you are not currently an owner.");
			}
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, VISIBILITY, PERMISSION) VALUES(?,?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newUserId);
			ps.setString(parameterIndex++, projectId);
			ps.setBoolean(parameterIndex++, true);
			ps.setInt(parameterIndex, AccessPermissionEnum.getIdByPermission(permission));
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred adding user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param user
	 * @param existingUserId
	 * @param projectId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editProjectUserPermission(User user, String existingUserId, String projectId, String newPermission) throws IllegalAccessException {
		// make sure user can edit the app
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserProjectPermission(existingUserId, projectId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify project permission for a user who does not currently have access to the project");
		}

		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);

		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this users project permission.");
			}

			// also, cannot give some owner permission if i am just an editor
			if(AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException("Cannot give owner level access to this project since you are not currently an owner.");
			}
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECTPERMISSION SET PERMISSION=? WHERE USERID=? AND PROJECTID=?");
			int parameterIndex = 1;
			//SET
			ps.setInt(parameterIndex++, newPermissionLvl);
			//WHERE
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred updating the user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param existingUserId
	 * @param projectId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editProjectUserPermissions(User user, String projectId, List<Map<String, String>> requests) throws IllegalAccessException {
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}
		
		
		// get userid of all requests
		List<String> existingUserIds = new ArrayList<String>();
	    for(Map<String,String> i:requests){
	    	existingUserIds.add(i.get("userid"));
	    }
			    
		// get user permissions to edit
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(existingUserIds, projectId);
		
		// make sure all users to edit currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the project: "+String.join(",", toRemoveUserIds));
		}
		
		// if user is not an owner, check to make sure they are not editting owner access
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			List<Integer> permissionList = new ArrayList<Integer>(existingUserPermission.values());
			if(permissionList.contains(AccessPermissionEnum.OWNER.getId())) {
				throw new IllegalArgumentException("As a non-owner, you cannot edit access of an owner.");
			}
		}
		
		// update user permissions in bulk
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECTPERMISSION SET PERMISSION = ? WHERE USERID = ? AND PROJECTID = ?");
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				//SET
				ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				//WHERE
				ps.setString(parameterIndex++, requests.get(i).get("userid"));
				ps.setString(parameterIndex++, projectId);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}	
	}
	
	/**
	 * Delete all values
	 * @param projectId
	 */
	public static void deleteProject(String projectId) {
		List<String> deletes = new ArrayList<>();
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
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}



	/**
	 * 
	 * @param user
	 * @param editedUserId
	 * @param projectId
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void removeProjectUser(User user, String existingUserId, String projectId) throws IllegalAccessException {
		// make sure user can edit the app
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserProjectPermission(existingUserId, projectId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the project");
		}

		// if i am not an ownerId
		// then i need to check if i can remove this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this users project permission.");
			}
		}

		String[] deletes = new String[] {
				"DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?",
				"DELETE FROM USERINSIGHTPERMISSION WHERE USERID=? AND PROJECTID=?"
		};
		for(String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(deleteQuery);
				int parameterIndex = 1;
				ps.setString(parameterIndex++, existingUserId);
				ps.setString(parameterIndex++, projectId);
				ps.execute();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("An error occurred removing the user permissions for the project and insights of this project");
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}


	/**
	 * Set if the project is public to all users on this instance
	 * @param user
	 * @param projectId
	 * @param global
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static boolean setProjectGlobal(User user, String projectId, boolean global) throws IllegalAccessException {
		if(!SecurityUserProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalAccessException("The user doesn't have the permission to set this project as global. Only the owner or an admin can perform this action.");
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECT SET GLOBAL=? WHERE PROJECTID=?");
			ps.setBoolean(1, global);
			ps.setString(2, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
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
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
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
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}
	
	/**
	 * Set project discoverable
	 * @param user
	 * @param projectId
	 * @param discoverable
	 * @return
	 * @throws IllegalAccessException
	 */
	public static boolean setProjectDiscoverable(User user, String projectId, boolean discoverable) throws IllegalAccessException {
		if(!SecurityProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalAccessException("The user doesn't have the permission to set this project as discoverable. Only the owner or an admin can perform this action.");
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECT SET DISCOVERABLE=? WHERE PROJECTID=?");
			ps.setBoolean(1, discoverable);
			ps.setString(2, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}

	/**
	 * update the project name
	 * @param user
	 * @param projectId
	 * @param isPublic
	 * @return
	 */
	public static boolean setProjectName(User user, String projectId, String newProjectName) {
		if(!SecurityUserProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalArgumentException("The user doesn't have the permission to change the project name. Only the owner or an admin can perform this action.");
		}
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECT SET PROJECTNAME=? WHERE PROJECTID=?");
			int parameterIndex = 1;
			// SET
			ps.setString(parameterIndex++, newProjectName);
			// WHERE
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred updating the project name");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}

	
	/*
	 * Project Dependencies
	 */
	
	/**
	 * Update the project dependencies
	 * Will delete existing values and then perform a bulk insert
	 * @param user
	 * @param projectId
	 * @param dependentEngineIds
	 */
	public static void updateProjectDependencies(User user, String projectId, List<String> dependentEngineIds) {
		// first do a delete
		String deleteQ = "DELETE FROM PROJECTDEPENDENCIES WHERE PROJECTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			int parameterIndex = 1;
			deletePs.setString(parameterIndex++, projectId);
			deletePs.execute();
			ConnectionUtils.commitConnection(deletePs.getConnection());
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}
		
		if(dependentEngineIds != null && !dependentEngineIds.isEmpty()) {
			AccessToken token = user.getPrimaryLoginToken();
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			// now we do the new insert with the order of the tags
			String query = securityDb.getQueryUtil().createInsertPreparedStatementString("PROJECTDEPENDENCIES", new String[]{"PROJECTID", "ENGINEID", "USERID", "TYPE", "DATEADDED"});
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(query);
				for(String depEngineId : dependentEngineIds) {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, projectId);
					ps.setString(parameterIndex++, depEngineId);
					ps.setString(parameterIndex++, token.getId());
					ps.setString(parameterIndex++, token.getName());
					ps.setTimestamp(parameterIndex++, timestamp, cal);
					ps.addBatch();
				}
				ps.executeBatch();
				ConnectionUtils.commitConnection(ps.getConnection());
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}

	/*
	 * Project Metadata
	 */

	/**
	 * Update the project metadata
	 * Will delete existing values and then perform a bulk insert
	 * @param projectId
	 * @param metadata
	 */
	public static void updateProjectMetadata(String projectId, Map<String, Object> metadata) {
		// first do a delete
		String deleteQ = "DELETE FROM PROJECTMETA WHERE METAKEY=? AND PROJECTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for(String field : metadata.keySet()) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, field);
				deletePs.setString(parameterIndex++, projectId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}
		
		// now we do the new insert with the order of the tags
		String query = securityDb.getQueryUtil().createInsertPreparedStatementString("PROJECTMETA", new String[]{"PROJECTID", "METAKEY", "METAVALUE", "METAORDER"});
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for(String field : metadata.keySet()) {
				Object val = metadata.get(field);
				List<Object> values = new ArrayList<>();
				if(val instanceof List) {
					values = (List<Object>) val;
				} else if(val instanceof Collection) {
					values.addAll( (Collection<Object>) val);
				} else {
					values.add(val);
				}
				
				for(int i = 0; i < values.size(); i++) {
					int parameterIndex = 1;
					Object fieldVal = values.get(i);
					
					ps.setString(parameterIndex++, projectId);
					ps.setString(parameterIndex++, field);
					ps.setString(parameterIndex++, fieldVal + "");
					ps.setInt(parameterIndex++, i);
					ps.addBatch();
				}
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Get the wrapper for additional project metadata
	 * @param projectId
	 * @param metaKeys
	 * @param ignoreMarkdown
	 * @return
	 * @throws Exception
	 */
	public static IRawSelectWrapper getProjectMetadataWrapper(Collection<String> projectId, List<String> metaKeys, boolean ignoreMarkdown) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAORDER"));
		// filters
		if(projectId != null && !projectId.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__PROJECTID", "==", projectId));
		}
		if(metaKeys != null && !metaKeys.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", metaKeys));
		}
		// exclude markdown metadata due to potential large data size
		if(ignoreMarkdown) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "!=", Constants.MARKDOWN));
		}
		// order
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAORDER"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}

	
	/**
	 * Get the metadata for a specific project
	 * @param projectId
	 * @return
	 */
	public static Map<String, Object> getAggregateProjectMetadata(String projectId, List<String> metaKeys, boolean ignoreMarkdown) {
		Map<String, Object> retMap = new HashMap<String, Object>();

		List<String> projectIds = new ArrayList<>();
		projectIds.add(projectId);

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = getProjectMetadataWrapper(projectIds, metaKeys, ignoreMarkdown);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String metaKey = (String) data[1];
				String metaValue = (String) data[2];

				// always send as array
				// if multi, send as array
				if(retMap.containsKey(metaKey)) {
					Object obj = retMap.get(metaKey);
					if(obj instanceof List) {
						((List) obj).add(metaValue);
					} else {
						List<Object> newList = new ArrayList<>();
						newList.add(obj);
						newList.add(metaValue);
						retMap.put(metaKey, newList);
					}
				} else {
					retMap.put(metaKey, metaValue);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return retMap;
	}
	/**
	 * Check if the user has access to the project
	 * @param projectId
	 * @param userId
	 * @return
	 * @throws Exception
	 */
	public static boolean checkUserHasAccessToProject(String projectId, String userId) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	
	/*
	 * Copying permissions
	 */
	
	/**
	 * Copy the project permissions from one project to another
	 * @param sourceProjectId
	 * @param targetProjectId
	 * @throws SQLException
	 */
	public static void copyProjectPermissions(String sourceProjectId, String targetProjectId) throws Exception {
		String insertTargetProjectPermissionSql = "INSERT INTO PROJECTPERMISSION (PROJECTID, USERID, PERMISSION, VISIBILITY) VALUES (?, ?, ?, ?)";
		PreparedStatement insertTargetProjectPermissionStatement = securityDb.getPreparedStatement(insertTargetProjectPermissionSql);
		
		// grab the permissions, filtered on the source engine id
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__VISIBILITY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", sourceProjectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				// now loop through all the permissions
				// but with the target engine id instead of the source engine id
				insertTargetProjectPermissionStatement.setString(1, targetProjectId);
				insertTargetProjectPermissionStatement.setString(2, (String) row[1]);
				insertTargetProjectPermissionStatement.setInt(3, ((Number) row[2]).intValue() );
				insertTargetProjectPermissionStatement.setBoolean(4, (Boolean) row[3]);
				// add to batch
				insertTargetProjectPermissionStatement.addBatch();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// first delete the current project permissions
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM PROJECTPERMISSION WHERE PROJECTID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, targetProjectId);
			// here we delete
			ps.execute();
			// now we insert
			insertTargetProjectPermissionStatement.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			if(!insertTargetProjectPermissionStatement.getConnection().getAutoCommit()) {
				insertTargetProjectPermissionStatement.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred transferring the project permissions");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertTargetProjectPermissionStatement);
		}
	}
	

	/**
	 * Returns List of users that have no access credentials to a given App.
	 * @param appID
	 * @return 
	 */
	public static List<Map<String, Object>> getProjectUsersNoCredentials(User user, String projectId) throws IllegalAccessException {
		/*
		 * Security check to make sure that the user can view the application provided. 
		 */
		if(!userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("The user does not have access to view this project");
		}	
		
		/*
		 * String Query = 
		 * "SELECT SMSS_USER.ID, SMSS_USER.USERNAME, SMSS_USER.NAME, SMSS_USER.EMAIL FROM SMSS_USER WHERE ID NOT IN 
		 * (SELECT e.USERID FROM ENGINEPERMISSION e WHERE e.ENGINEID = '"+ appID + "' e.PERMISSION IS NOT NULL);"
		 */
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME", "username"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		//Filter for sub-query
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("SMSS_USER__ID", "!=", subQs));
			//Sub-query itself
			subQs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID","==",projectId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "!=", null, PixelDataType.NULL_VALUE));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * 
	 * @param user
	 * @param projectIdFilters
	 * @param favoritesOnly
	 * @param portalsOnly
	 * @param projectMetadataFilter
	 * @param permissionFilters
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> getUserProjectList(
			User user, 
			List<String> projectIdFilters,
			boolean favoritesOnly, 
			boolean portalsOnly,
			Map<String,Object> projectMetadataFilter, 
			List<Integer> permissionFilters, 
			String searchTerm, 
			String limit, 
			String offset) {
		
		boolean hasSearchTerm = searchTerm != null && !(searchTerm=searchTerm.trim()).isEmpty();

		Collection<String> userIds = getUserFiltersQs(user);
		
		String groupProjectPermission = "GROUPPROJECTPERMISSION__";
		String projectPrefix = "PROJECT__";
		
		SelectQueryStruct qs1 = new SelectQueryStruct();
		// selectors
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"PROJECTID", "project_id"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"PROJECTNAME", "project_name"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"TYPE", "project_type"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"COST", "project_cost"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"GLOBAL", "project_global"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"DISCOVERABLE", "project_discoverable"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"CATALOGNAME", "project_catalog_name"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"CREATEDBY", "project_created_by"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"CREATEDBYTYPE", "project_created_by_type"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"DATECREATED", "project_date_created"));
		// dont forget reactors/portal information
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"HASPORTAL", "project_has_portal"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"PORTALNAME", "project_portal_name"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"PORTALPUBLISHED", "project_portal_published_date"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"PORTALPUBLISHEDUSER", "project_published_user"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"PORTALPUBLISHEDTYPE", "project_published_user_type"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"REACTORSCOMPILED", "project_reactors_compiled_date"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"REACTORSCOMPILEDUSER", "project_reactors_compiled_user"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix+"REACTORSCOMPILEDTYPE", "project_reactors_compiled_user_type"));
		// back to the others
		qs1.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "PROJECT__PROJECTNAME", "low_project_name"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__FAVORITE", "project_favorite"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__PERMISSION", "user_permission"));
		qs1.addSelector(new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION", "group_permission"));
		
		// this block is for max permissions
		// If both null - return null
		// if either not null - return the permission value that is not null
		// if both not null - return the max permissions (I.E lowest number)
		{
			AndQueryFilter and = new AndQueryFilter();
			and.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
			and.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
				
			AndQueryFilter and1 = new AndQueryFilter();
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "!=", null, PixelDataType.CONST_INT));
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
		
			AndQueryFilter and2 = new AndQueryFilter();
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "!=", null, PixelDataType.CONST_INT));
			
			SimpleQueryFilter maxPermFilter = SimpleQueryFilter.makeColToColFilter("USER_PERMISSIONS__PERMISSION", "<", "GROUP_PERMISSIONS__PERMISSION");
			
			QueryIfSelector qis3 = QueryIfSelector.makeQueryIfSelector(maxPermFilter,
						new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"),
						new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION"),
						"permission"
					);

			QueryIfSelector qis2 = QueryIfSelector.makeQueryIfSelector(and2,
						new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"),
						qis3,
						"permission"
					);
			
			QueryIfSelector qis1 = QueryIfSelector.makeQueryIfSelector(and1,
						new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION"),
						qis2,
						"permission"
					);
			
			QueryIfSelector qis = QueryIfSelector.makeQueryIfSelector(and,
						new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"),
						qis1,
						"permission"
					);
			
			qs1.addSelector(qis);
		}
				
		// add a join to get the user permission level, if favorite, and the visibility
		{
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID", "PROJECTID"));
			
			QueryFunctionSelector castFavorite = QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.CAST, "PROJECTPERMISSION__FAVORITE", "castFavorite");
	        castFavorite.setDataType(securityDb.getQueryUtil().getIntegerDataTypeName());
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, castFavorite, "FAVORITE"));
			
			QueryFunctionSelector castVisibility = QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.CAST, "PROJECTPERMISSION__VISIBILITY", "castVisibility");
			castVisibility.setDataType(securityDb.getQueryUtil().getIntegerDataTypeName());
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, castVisibility, "VISIBILITY"));
			
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, "PROJECTPERMISSION__PERMISSION", "PERMISSION"));
			qs2.addGroupBy(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID", "PROJECTID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
			IRelation subQuery = new SubqueryRelationship(qs2, "USER_PERMISSIONS", "left.outer.join", new String[] {"USER_PERMISSIONS__PROJECTID", "PROJECT__PROJECTID", "="});
			qs1.addRelation(subQuery);
		}
		
		// add a join to get the group permission level
		{
			SelectQueryStruct qs3 = new SelectQueryStruct();
			qs3.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));
			qs3.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, groupProjectPermission + "PERMISSION", "PERMISSION"));
			qs3.addGroupBy(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));
			
			// filter on groups
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupProjectOrFilters.addFilter(andFilter);
			}
			
			if (!groupProjectOrFilters.isEmpty()) {
				qs3.addExplicitFilter(groupProjectOrFilters);
			} else {
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", null));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", null));
				qs3.addExplicitFilter(andFilter1);
			}
			
			IRelation subQuery = new SubqueryRelationship(qs3, "GROUP_PERMISSIONS", "left.outer.join", new String[] {"GROUP_PERMISSIONS__PROJECTID", "PROJECT__PROJECTID", "="});
			qs1.addRelation(subQuery);
		}
		
		// filters
		OrQueryFilter orFilter = new OrQueryFilter();
		{
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix+"GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "!=", null, PixelDataType.CONST_INT));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "!=", null, PixelDataType.CONST_INT));
			qs1.addExplicitFilter(orFilter);
		}
		
		// filter based on permission filters
		if(permissionFilters != null && !permissionFilters.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "==", permissionFilters, PixelDataType.CONST_INT));
		}
		
		// only show those that are visible
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__VISIBILITY", "==", Arrays.asList(new Object[] {1, null}), PixelDataType.CONST_INT));
		// favorites only
		if(favoritesOnly) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__FAVORITE", "==", true, PixelDataType.BOOLEAN));
		}
		if(portalsOnly) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix+"HASPORTAL", "==", true, PixelDataType.BOOLEAN));
		}
		if(hasSearchTerm) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs1, projectPrefix+"PROJECTNAME", searchTerm);
		}
		
		// filtering by projectmeta key-value pairs (i.e. <tag>:value): for each pair, add in-filter against projectids from subquery
		if (projectMetadataFilter!=null && !projectMetadataFilter.isEmpty()) {
			for (String k : projectMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAVALUE", "==", projectMetadataFilter.get(k)));
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "==", subQs));
			}
		}
		
		{
			// first lets make sure we have any groups
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupProjectOrFilters.addFilter(andFilter);
			}
			// 4.a does the group have explicit access
			if(!groupProjectOrFilters.isEmpty()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", "==", subQs));
				
				// we need to have the insight filters
				subQs.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID"));
				subQs.addExplicitFilter(groupProjectOrFilters);
			}
		}
		
		// add the sort
		qs1.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
		
		Long long_limit = -1L;
		Long long_offset = -1L;
		if(limit != null && !limit.trim().isEmpty()) {
			long_limit = Long.parseLong(limit);
		}
		if(offset != null && !offset.trim().isEmpty()) {
			long_offset = Long.parseLong(offset);
		}
		qs1.setLimit(long_limit);
		qs1.setOffSet(long_offset);
	
		return QueryExecutionUtility.flushRsToMap(securityDb, qs1);
	}
	
	/**
	 * Get the list of the project ids that the user has access to 
	 * @param user
	 * @param includeGlobal
	 * @param includeDiscoverable
	 * @param includeExistingAccess
	 * @return
	 */
	public static List<String> getUserProjectIdList(User user, boolean includeGlobal, boolean includeDiscoverable, boolean includeExistingAccess) {
		String projectPrefix = "PROJECT__";
		String projectPermissionPrefix = "PROJECTPERMISSION__";
		String groupProjectPermissionPrefix = "GROUPPROJECTPERMISSION__";
		
		Collection<String> userIds = getUserFiltersQs(user);
		
		SelectQueryStruct qs1 = new SelectQueryStruct();
		
		// selectors
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTID", "project_id"));
		
		// filters
		OrQueryFilter orFilter = new OrQueryFilter();
		if(includeGlobal) {
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix + "GLOBAL", "==", true, PixelDataType.BOOLEAN));
		}
		if(includeDiscoverable) {
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix + "DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		}
		String existingAccessComparator = "==";
		if(!includeExistingAccess) {
			existingAccessComparator = "!=";
		}
		if(!includeExistingAccess && !includeDiscoverable) {
			throw new IllegalArgumentException("Fitler combinations can result in ids that the user does not have access to. Please adjust your parameters");
		}
		{
			// user access
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector(projectPermissionPrefix + "PROJECTID", "PROJECTID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(projectPermissionPrefix + "USERID", "==", userIds));
			orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", existingAccessComparator, qs2));
		}
		{
			// filter on groups
			OrQueryFilter groupEngineOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermissionPrefix + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermissionPrefix + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupEngineOrFilters.addFilter(andFilter);
			}
			
			if (!groupEngineOrFilters.isEmpty()) {
				SelectQueryStruct qs3 = new SelectQueryStruct();
				qs3.addSelector(new QueryColumnSelector(groupProjectPermissionPrefix + "PROJECTID", "PROJECTID"));
				qs3.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, groupProjectPermissionPrefix + "PERMISSION", "PERMISSION"));
				qs3.addExplicitFilter(groupEngineOrFilters);

				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", existingAccessComparator, qs3));
			}
		}
		
		qs1.addExplicitFilter(orFilter);
	
		return QueryExecutionUtility.flushToListString(securityDb, qs1);
	}
	
	/**
	 * Get all user engines and engine Ids regardless of it being hidden or not 
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllUserProjectList(User user) {	
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		List<Map<String, Object>> allGlobalEnginesMap = QueryExecutionUtility.flushRsToMap(securityDb, qs);

		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
		
		List<Map<String, Object>> engineMap = QueryExecutionUtility.flushRsToMap(securityDb, qs2);
		engineMap.addAll(allGlobalEnginesMap);
		return engineMap;
	}
	
	/**
	 * Get the list of the project information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getUserProjectList(User user, String projectFilter) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE","project_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		qs.addSelector(new QueryColumnSelector("PROJECT__GLOBAL", "project_global"));
		qs.addSelector(new QueryColumnSelector("PROJECT__DISCOVERABLE", "project_discoverable"));
		qs.addSelector(new QueryColumnSelector("PROJECT__CATALOGNAME", "project_catalog_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__CREATEDBY", "project_created_by"));
		qs.addSelector(new QueryColumnSelector("PROJECT__CREATEDBYTYPE", "project_created_by_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__DATECREATED", "project_date_created"));
		// dont forget reactors/portal information
		qs.addSelector(new QueryColumnSelector("PROJECT__HASPORTAL", "project_has_portal"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALNAME", "project_portal_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHED", "project_portal_published_date"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHEDUSER", "project_published_user"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHEDTYPE", "project_published_user_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILED", "project_reactors_compiled_date"));
		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILEDUSER", "project_reactors_compiled_user"));
		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILEDTYPE", "project_reactors_compiled_user_type"));
		// back to the others
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		fun.setAlias("low_project_name");
		qs.addSelector(fun);
		if(projectFilter != null && !projectFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
		}
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
			qs.addExplicitFilter(orFilter);
		}
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
//	/**
//	 * Get the list of the projects with an optional filter
//	 * @param userId
//	 * @return
//	 */
//	public static List<Map<String, Object>> getAllProjectList(String projectFilter, String limit, String offset) {
//		SelectQueryStruct qs = new SelectQueryStruct();
//		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE","project_type"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__CATALOGNAME", "project_catalog_name"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__CREATEDBY", "project_created_by"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__CREATEDBYTYPE", "project_created_by_type"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__DATECREATED", "project_date_created"));
//		// dont forget reactors/portal information
//		qs.addSelector(new QueryColumnSelector("PROJECT__HASPORTAL", "project_has_portal"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALNAME", "project_portal_name"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHED", "project_portal_published_date"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHEDUSER", "project_published_user"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHEDTYPE", "project_published_user_type"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILED", "project_reactors_compiled_date"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILEDUSER", "project_reactors_compiled_user"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILEDTYPE", "project_reactors_compiled_user_type"));
//		// back to the others
//		QueryFunctionSelector fun = new QueryFunctionSelector();
//		fun.setFunction(QueryFunctionHelper.LOWER);
//		fun.addInnerSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
//		fun.setAlias("low_project_name");
//		qs.addSelector(fun);
//		if(projectFilter != null && !projectFilter.isEmpty()) { 
//			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
//		}
//		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
//		
//		Long long_limit = -1L;
//		Long long_offset = -1L;
//		if(limit != null && !limit.trim().isEmpty()) {
//			long_limit = Long.parseLong(limit);
//		}
//		if(offset != null && !offset.trim().isEmpty()) {
//			long_offset = Long.parseLong(offset);
//		}
//		qs.setLimit(long_limit);
//		qs.setOffSet(long_offset);
//		
//		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
//	}
	
	/**
	 * Change the user visibility (show/hide) for a project. Without removing its permissions.
	 * @param user
	 * @param projectId
	 * @param visibility
	 * @throws SQLException 
	 * @throws IllegalAccessException 
	 */
	public static void setProjectVisibility(User user, String projectId, boolean visibility) throws SQLException, IllegalAccessException {
		if(!userCanViewProject(user, projectId)) {
			throw new IllegalAccessException("The user doesn't have the permission to modify his visibility of this project.");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIdFilters));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()){
				UpdateQueryStruct uqs = new UpdateQueryStruct();
				uqs.setEngine(securityDb);
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIdFilters));

				List<IQuerySelector> selectors = new ArrayList<>();
				selectors.add(new QueryColumnSelector("PROJECTPERMISSION__VISIBILITY"));
				List<Object> values = new ArrayList<>();
				values.add(visibility);
				uqs.setSelectors(selectors);
				uqs.setValues(values);
				
				UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(uqs);
				String updateQuery = updateInterp.composeQuery();
				securityDb.insertData(updateQuery);
				
			} else {
				// need to insert
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO PROJECTPERMISSION "
						+ "(USERID, PROJECTID, VISIBILITY, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if(ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set project visibility");
				}
				try {
					// we will set the permission to read only
					for(AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, projectId);
						ps.setBoolean(parameterIndex++, visibility);
						// default favorite as false
						ps.setBoolean(parameterIndex++, false);
						ps.setInt(parameterIndex++, 3);
	
						ps.addBatch();
					}
					ps.executeBatch();
					if(!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw e;
				} finally {
					if(ps != null) {
						ps.close();
					}
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Change the user favorite (is favorite / not favorite) for a project. Without removing its permissions.
	 * @param user
	 * @param projectId
	 * @param visibility
	 * @throws SQLException 
	 * @throws IllegalAccessException 
	 */
	public static void setProjectFavorite(User user, String projectId, boolean isFavorite) throws SQLException, IllegalAccessException {
		if(!projectIsGlobal(projectId)
				&& !userCanViewProject(user, projectId)) {
			throw new IllegalAccessException("The user doesn't have the permission to modify his visibility of this project.");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIdFilters));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()){
				UpdateQueryStruct uqs = new UpdateQueryStruct();
				uqs.setEngine(securityDb);
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIdFilters));

				List<IQuerySelector> selectors = new ArrayList<>();
				selectors.add(new QueryColumnSelector("PROJECTPERMISSION__FAVORITE"));
				List<Object> values = new ArrayList<>();
				values.add(isFavorite);
				uqs.setSelectors(selectors);
				uqs.setValues(values);
				
				UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(uqs);
				String updateQuery = updateInterp.composeQuery();
				securityDb.insertData(updateQuery);
				
			} else {
				// need to insert
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO PROJECTPERMISSION "
						+ "(USERID, PROJECTID, VISIBILITY, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if(ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set project visibility");
				}
				try {
					// we will set the permission to read only
					for(AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, projectId);
						// default visibility as true
						ps.setBoolean(parameterIndex++, true);
						ps.setBoolean(parameterIndex++, isFavorite);
						ps.setInt(parameterIndex++, 3);
	
						ps.addBatch();
					}
					ps.executeBatch();
					if(!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw e;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Change if this project has a portal or not
	 * @param user
	 * @param projectId
	 * @param visibility
	 * @throws SQLException 
	 * @throws IllegalAccessException 
	 */
	public static void setProjectPortal(User user, String projectId, boolean hasPortal, String portalName) throws SQLException, IllegalAccessException {
		if(!userIsOwner(user, projectId)) {
			throw new IllegalAccessException("The user doesn't have the permission to set if this project has a portal");
		}
		
		String query = "UPDATE PROJECT SET HASPORTAL=?, PORTALNAME=? WHERE PROJECTID=?";
		PreparedStatement ps = securityDb.getPreparedStatement(query);
		if(ps == null) {
			throw new IllegalArgumentException("Error generating prepared statement to set project visibility");
		}
		try {
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, hasPortal);
			if(portalName != null) {
				ps.setString(parameterIndex++, portalName);
			} else {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			}
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Get all projects for setting options that the user has access to
	 * @param usersId
	 * @return
	 */
//	public static List<Map<String, Object>> getAllUserProjectSettings(User user) {
//		return getAllUserProjectSettings(user, null);
//	}
	
//	/**
//	 * Get project settings - if projectFilter passed will filter to that project otherwise returns all
//	 * @param user
//	 * @param projectFilter
//	 * @return
//	 */
//	public static List<Map<String, Object>> getAllUserProjectSettings(User user, String projectFilter) {
//		SelectQueryStruct qs = new SelectQueryStruct();
//		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
//		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "PROJECT__PROJECTNAME", "low_project_name"));
//		qs.addSelector(new QueryColumnSelector("PROJECT__GLOBAL", "project_global"));
//		qs.addSelector(QueryFunctionSelector.makeCol2ValCoalesceSelector("PROJECTPERMISSION__VISIBILITY", true, "project_visibility"));
//		qs.addSelector(QueryFunctionSelector.makeCol2ValCoalesceSelector("PERMISSION__NAME", "READ_ONLY", "project_permission"));
//		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
//		if(projectFilter != null && !projectFilter.isEmpty()) {
//			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
//		}
//		qs.addRelation("PROJECT", "PROJECTPERMISSION", "inner.join");
//		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "left.outer.join");
//		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
//
//		Set<String> engineIdsIncluded = new HashSet<String>();
//		
//		List<Map<String, Object>> result = new Vector<Map<String, Object>>();
//
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
//			while(wrapper.hasNext()) {
//				IHeadersDataRow headerRow = wrapper.next();
//				String[] headers = headerRow.getHeaders();
//				Object[] values = headerRow.getValues();
//				
//				// store the engine ids
//				// we will exclude these later
//				// engine id is the first one to be returned
//				engineIdsIncluded.add(values[0].toString());
//				
//				Map<String, Object> map = new HashMap<String, Object>();
//				for(int i = 0; i < headers.length; i++) {
//					map.put(headers[i], values[i]);
//				}
//				result.add(map);
//			}
//		} catch (Exception e) {
//			logger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(wrapper != null) {
//				wrapper.cleanUp();
//			}
//		}
//		
//		// we dont need to run 2nd query if we are filtering to one db and already have it
//		if(projectFilter != null && !projectFilter.isEmpty() && !result.isEmpty()) {
//			qs = new SelectQueryStruct();
//			qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
//			qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
//			qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "PROJECT__PROJECTNAME", "low_project_name"));
//			qs.addSelector(QueryFunctionSelector.makeCol2ValCoalesceSelector("PROJECTPERMISSION__VISIBILITY", true, "project_visibility"));
//			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
//			// since some rdbms do not allow "not in ()" - we will only add if necessary
//			if(!engineIdsIncluded.isEmpty()) {
//				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "!=", new Vector<String>(engineIdsIncluded)));
//			}
//			if(projectFilter != null && !projectFilter.isEmpty()) {
//				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
//			}
//			qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
//			try {
//				wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
//				while(wrapper.hasNext()) {
//					IHeadersDataRow headerRow = wrapper.next();
//					String[] headers = headerRow.getHeaders();
//					Object[] values = headerRow.getValues();
//					
//					Map<String, Object> map = new HashMap<String, Object>();
//					for(int i = 0; i < headers.length; i++) {
//						map.put(headers[i], values[i]);
//					}
//					// add the others which we know
//					map.put("project_global", true);
//					map.put("project_permission", "READ_ONLY");
//					result.add(map);
//				}
//			} catch (Exception e) {
//				logger.error(Constants.STACKTRACE, e);
//			} finally {
//				if(wrapper != null) {
//					wrapper.cleanUp();
//				}
//			}
//			
//			// now we need to loop through and order the results
//			Collections.sort(result, new Comparator<Map<String, Object>>() {
//	
//				@Override
//				public int compare(Map<String, Object> o1, Map<String, Object> o2) {
//					String appName1 = o1.get("low_project_name").toString();
//					String appName2 = o2.get("low_project_name").toString();
//					return appName1.compareTo(appName2);
//				}
//			
//			});
//		}
//		
//		return result;
//	}

	///////////////////////////////////////////////
	///////////////////////////////////////////////
	/////////////////PROJECTS//////////////////////

	/**
	 * Return the projects the user has explicit access to
	 * 
	 * @param singleUserId
	 * @return
	 */
	public static Set<String> getProjectsUserHasExplicitAccess(User user) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		orFilter.addFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(orFilter);
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
		return QueryExecutionUtility.flushToSetString(securityDb, qs, false);
	}

	public static List<Map<String, Object>> getProjectInfo(Collection dbFilter) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", dbFilter));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get the list of projects the user does not have access to but can request
	 * 
	 * @param allUserProjects
	 * @throws Exception
	 */
	public static List<Map<String, Object>> getUserRequestableProjects(Collection<String> allUserProjects) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "!=", allUserProjects));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Determine if a user can request a project
	 * @param projectId
	 * @return
	 */
	public static boolean canRequestProject(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				// if you are here, you can request
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return false;
	}
	
	/**
	 * Retrieve the project owner
	 * @param user
	 * @param projectId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<String> getProjectOwners(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PERMISSION__ID", "==", AccessPermissionEnum.OWNER.getId()));
		qs.addRelation("SMSS_USER", "PROJECTPERMISSION", "inner.join");
		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
	
	/**
	 * 
	 * @return
	 */
	public static List<String> getAllMetakeys() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__METAKEY"));
		List<String> metakeys = QueryExecutionUtility.flushToListString(securityDb, qs);
		return metakeys;
	}
	
	/**
	 * 
	 * @param metakey
	 * @return
	 */
	public static List<Map<String, Object>> getMetakeyOptions(String metakey) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__METAKEY", "metakey"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__SINGLEMULTI", "single_multi"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__DISPLAYORDER", "display_order"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__DISPLAYOPTIONS", "display_options"));
		if (metakey != null && !metakey.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETAKEYS__METAKEY", "==", metakey));
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * 
	 * @param metaoptions
	 * @return
	 */
	public static boolean updateMetakeyOptions( List<Map<String,Object>> metaoptions) {
		boolean valid = false;
        PreparedStatement insertPs = null;
        String tableName = "PROJECTMETAKEYS";
        try {
			// first truncate table clean 
			String truncateSql = "DELETE FROM " + tableName + " WHERE 1=1";
			securityDb.removeData(truncateSql);
			insertPs = securityDb.bulkInsertPreparedStatement(new Object[] {tableName, Constants.METAKEY, Constants.SINGLE_MULTI, Constants.DISPLAY_ORDER, Constants.DISPLAY_OPTIONS} );
			// then insert latest options
			for (int i = 0; i < metaoptions.size(); i++) {
				insertPs.setString(1, (String) metaoptions.get(i).get("metakey"));
				insertPs.setString(2, (String) metaoptions.get(i).get("singlemulti"));
				insertPs.setInt(3, ((Number) metaoptions.get(i).get("order")).intValue());
				insertPs.setString(4, (String) metaoptions.get(i).get("displayoptions"));
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if(!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
			valid = true;
        } catch (SQLException e) {
        	classLogger.error(Constants.STACKTRACE, e);
        } finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
        }
		return valid;
	}
	
	/**
     * Get all the available engine metadata and their counts for given keys
     * @param engineFilters
     * @param metaKey
     * @return
     */
    public static List<Map<String, Object>> getAvailableMetaValues(List<String> projectFilters, List<String> metaKeys) {
        SelectQueryStruct qs = new SelectQueryStruct();
        // selectors
        qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAKEY"));
        qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE"));
        QueryFunctionSelector fSelector = new QueryFunctionSelector();
        fSelector.setAlias("count");
        fSelector.setFunction(QueryFunctionHelper.COUNT);
        fSelector.addInnerSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE"));
        qs.addSelector(fSelector);
        // filters
        qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", metaKeys));
        if(projectFilters != null && !projectFilters.isEmpty()) {
            qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__PROJECTID", "==", projectFilters));
        }
        // group
        qs.addGroupBy(new QueryColumnSelector("PROJECTMETA__METAKEY"));
        qs.addGroupBy(new QueryColumnSelector("PROJECTMETA__METAVALUE"));
        
        return QueryExecutionUtility.flushRsToMap(securityDb, qs);
    }
    
    
    /**
	 * set user access request
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param permission
	 * @return
	 */
	public static void setUserAccessRequest(String userId, String userType, String projectId, int permission) {
		// first mark previously undecided requests as old
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET APPROVER_DECISION = 'OLD' WHERE REQUEST_USERID=? AND REQUEST_TYPE=? AND PROJECTID=? AND APPROVER_DECISION='NEW_REQUEST'";
		PreparedStatement updatePs = null;
		try {
			int index = 1;
			updatePs = securityDb.getPreparedStatement(updateQ);
			updatePs.setString(index++, userId);
			updatePs.setString(index++, userType);
			updatePs.setString(index++, projectId);
			updatePs.execute();
			if(!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating user access request with detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, updatePs);
		}

		// now we do the new insert 
		String insertQ = "INSERT INTO PROJECTACCESSREQUEST (ID, REQUEST_USERID, REQUEST_TYPE, REQUEST_TIMESTAMP, PROJECTID, PERMISSION, APPROVER_DECISION) VALUES (?, ?,?,?,?,?,'NEW_REQUEST')";
		PreparedStatement insertPs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());

			int index = 1;
			insertPs = securityDb.getPreparedStatement(insertQ);
			insertPs.setString(index++, UUID.randomUUID().toString());
			insertPs.setString(index++, userId);
			insertPs.setString(index++, userType);
			insertPs.setTimestamp(index++, timestamp, cal);
			insertPs.setString(index++, projectId);
			insertPs.setInt(index++, permission);
			insertPs.execute();
			if(!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while adding user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}
	}
	
	/**
	 * Get the request pending database permission for a specific user
	 * @param singleUserId
	 * @param databaseId
	 * @return
	 */
	public static List<Map<String, Object>> getUserAccessRequestsByProject(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__ID"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__REQUEST_USERID"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__REQUEST_TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__REQUEST_TIMESTAMP"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__APPROVER_USERID"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__APPROVER_TYPE"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__APPROVER_DECISION"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__APPROVER_TIMESTAMP"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__APPROVER_DECISION", "==", "NEW_REQUEST"));
		qs.addRelation("PROJECTACCESSREQUEST__REQUEST_USERID", "SMSS_USER__ID", "inner.join");
		qs.addRelation("PROJECTACCESSREQUEST__REQUEST_TYPE", "SMSS_USER__TYPE", "inner.join");
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Approving user access requests and giving user access in permissions
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param requests
	 */
	public static void approveProjectUserAccessRequests(User user, String projectId, List<Map<String, String>> requests) throws IllegalAccessException {
		
		// make sure user has right permission level to approve access requests
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}
		
		// get user permissions of all requests
		List<String> permissions = new ArrayList<String>();
	    for(Map<String,String> i:requests){
	    	permissions.add(i.get("permission"));
	    }

		// if user is not an owner, check to make sure they cannot grant owner access
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalArgumentException("You cannot grant user access to others.");
		} else {
			if(!AccessPermissionEnum.isOwner(userPermissionLvl) && permissions.contains("OWNER")) {
				throw new IllegalArgumentException("As a non-owner, you cannot grant owner access.");
			}
		}
				
		// bulk delete
		String deleteQ = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				deletePs.setString(parameterIndex++, projectId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while deleting projectpermission with detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, projectId);
				insertPs.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				insertPs.setBoolean(parameterIndex++, true);
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if(!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}

		// now we do the new bulk update to projectaccessrequest table
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement updatePs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			updatePs = securityDb.getPreparedStatement(updateQ);
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			String userId = token.getId();
			String userType = token.getProvider().toString();
			for(int i=0; i<requests.size(); i++) {
				int index = 1;
				updatePs.setInt(index++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp, cal);
				
				updatePs.setString(index++, (String) requests.get(i).get("requestid"));
				
				updatePs.addBatch();
			}
			updatePs.executeBatch();
			if(!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, updatePs);
		}
	}
	
	/**
	 * Denying user access requests to project
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param requests
	 */
	public static void denyProjectUserAccessRequests(User user, String projectId, List<String> requestIdList) throws IllegalAccessException {
		
		// make sure user has right permission level to approve access requests
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// only project owners can deny user access requests
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			throw new IllegalArgumentException("Insufficient privileges to deny user access requests.");
		}
				
		// bulk update to projectaccessrequest table
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement ps = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			ps = securityDb.getPreparedStatement(updateQ);
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			String userId = token.getId();
			String userType = token.getProvider().toString();
			for(int i=0; i<requestIdList.size(); i++) {
				int index = 1;
				ps.setString(index++, userId);
				ps.setString(index++, userType);
				ps.setString(index++, "DENIED");
				ps.setTimestamp(index++, timestamp, cal);
				
				ps.setString(index++, requestIdList.get(i));
				
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param projectId
	 * @param permission
	 * @return
	 */
	public static void addProjectUserPermissions(User user, String projectId, List<Map<String,String>> permission) throws IllegalAccessException {
		
		// make sure user can edit the project
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}
		
		// check to make sure these users do not already have permissions to project
		// get list of userids from permission list map
		List<String> userIds = permission.stream().map(map -> map.get("userid")).collect(Collectors.toList());
		// this returns a list of existing permissions
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(userIds, projectId);
		if (!existingUserPermission.isEmpty()) {
			throw new IllegalArgumentException("The following users already have access to this project. Please edit the existing permission level: "+String.join(",", existingUserPermission.keySet()));
		}
		
		// if user is not an owner, check to make sure they are not adding owner access
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			List<String> permissionList = permission.stream().map(map -> map.get("permission")).collect(Collectors.toList());
			if(permissionList.contains("OWNER")) {
				throw new IllegalArgumentException("As a non-owner, you cannot add owner user access.");
			}
		}
		
		// insert new user permissions in bulk
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)");
			for(int i=0; i<permission.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, permission.get(i).get("userid"));
				ps.setString(parameterIndex++, projectId);
				ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission.get(i).get("permission")));
				ps.setBoolean(parameterIndex++, true);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param editedUserId
	 * @param projectId
	 * @return
	 */
	public static void removeProjectUsers(User user, List<String> existingUserIds, String projectId)  throws IllegalAccessException {
		// make sure user can edit the project
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}
		
		// get user permissions to remove
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(existingUserIds, projectId);
		
		// make sure all users to remove currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the project: "+String.join(",", toRemoveUserIds));
		}
		
		// if user is not an owner, check to make sure they are not removing owner access
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			List<Integer> permissionList = new ArrayList<Integer>(existingUserPermission.values());
			if(permissionList.contains(AccessPermissionEnum.OWNER.getId())) {
				throw new IllegalArgumentException("As a non-owner, you cannot remove access of an owner.");
			}
		}
		
		// first do a delete
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?");
			for(int i=0; i<existingUserIds.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, existingUserIds.get(i));
				ps.setString(parameterIndex++, projectId);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
}
