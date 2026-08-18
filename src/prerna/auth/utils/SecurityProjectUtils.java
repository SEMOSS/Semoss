/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.auth.utils;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import com.google.gson.Gson;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.notifications.NotificationDbUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.parser.ParserException;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EmailUtility;
import prerna.util.InsightsRDBMSUtils;
import prerna.util.NotificationConstants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class SecurityProjectUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityProjectUtils.class);

	/**
	 * Add an entire project into the security db - Expectation is not to call this
	 * method but addProject(projectId, boolean global = true)
	 * 
	 * @param projectId
	 * 
	 *                  PLEASE DEFINE GLOBAL
	 *                  {@link #addProject(String, boolean, User)}
	 * @throws Exception
	 */
	@Deprecated
	public static void addProject(String projectId, User user) throws Exception {
		// default project is not global
		addProject(projectId, false, user);
	}

	/**
	 * Add an entire project into the security db
	 * 
	 * @param appId
	 * @throws Exception
	 */
	public static void addProject(String projectId, boolean global, User user) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String smssFile = DIHelper.getInstance().getProjectProperty(projectId + "_" + Constants.STORE) + "";
		Properties prop = Utility.loadProperties(smssFile);

		String projectName = prop.getProperty(Constants.PROJECT_ALIAS);
		if (projectName == null) {
			projectName = projectId;
		}

		String displayName = prop.getProperty(Constants.PROJECT_DISPLAY_NAME);
		if (displayName == null || displayName.trim().isEmpty()) {
			displayName = projectName;
		}

		boolean reloadInsights = false;
		if (prop.containsKey(Constants.RELOAD_INSIGHTS)) {
			String booleanStr = prop.get(Constants.RELOAD_INSIGHTS).toString();
			reloadInsights = Boolean.parseBoolean(booleanStr);
		}

		// TODO: we do not need cost for project
		String[] typeAndCost = new String[] { prop.getOrDefault(Constants.PROJECT_ENUM_TYPE, "") + "", "" };
		boolean projectExists = containsProjectId(projectId);
		if (projectExists && !reloadInsights) {
			classLogger.info("Security database already contains project with unique id = {}",
					Utility.cleanLogString(SmssUtilities.getUniqueName(prop)));
			return;
		} else if (!projectExists) {
			addProject(projectId, projectName, displayName, typeAndCost[0], typeAndCost[1], global, user);
		} else if (projectExists) {
			// delete values if currently present
			deleteInsightsFromProjectForRecreation(projectId);
			// update project properties anyway ... in case global was shifted for example
			updateProject(projectId, projectName, typeAndCost[0], typeAndCost[1], global);
		}

		classLogger.info("Security database going to add project with alias = {}", Utility.cleanLogString(projectName));

		// load just the insights database
		// first see if engine is already loaded
		boolean projectLoaded = false;
		IRDBMSEngine rne = null;
		if (Utility.projectLoaded(projectId)) {
			rne = Utility.getProject(projectId).getInsightDatabase();
		} else {
			rne = ProjectHelper.loadInsightsEngine(prop, classLogger);
		}

		// i need to delete any current insights for the project
		// before i start to insert new insights
		String deleteQuery = "DELETE FROM INSIGHT WHERE PROJECTID='" + projectId + "'";
		try {
			securityDb.removeData(deleteQuery);
		} catch (SQLException e) {
			classLogger.error("Failed to create the project and initialize project security records", e);
		}

		// if we are doing a reload
		// we will want to remove unnecessary insights
		// from the insight permissions
		boolean existingInsightPermissions = true;
		Set<String> insightPermissionIds = null;
		if (reloadInsights) {
			// need to flush out the current insights w/ permissions
			// will keep the same permissions
			// and perform a delta
			classLogger.info("Reloading app. Retrieving existing insights with permissions");
			String insightsWPer = "SELECT INSIGHTID FROM USERINSIGHTPERMISSION WHERE PROJECTID='" + projectId + "'";
			insightPermissionIds = QueryExecutionUtility.flushToSetString(securityDb, insightsWPer, false);
			if (insightPermissionIds.isEmpty()) {
				existingInsightPermissions = true;
			}
		}

		AbstractSqlQueryUtil securityQueryUtil = securityDb.getQueryUtil();
		// make a prepared statement
		PreparedStatement ps = null;
		try {
			ps = securityDb.bulkInsertPreparedStatement(new String[] {
					// table name
					"INSIGHT",
					// column names
					"PROJECTID", "INSIGHTID", "INSIGHTNAME", "GLOBAL", "EXECUTIONCOUNT", "CREATEDON", "LASTMODIFIEDON",
					"LAYOUT", "CACHEABLE", "CACHEMINUTES", "CACHECRON", "CACHEDON", "CACHEENCRYPT", "RECIPE",
					"SCHEMANAME" });
		} catch (SQLException e) {
			classLogger.error("Failed to create the project and initialize project security records", e);
		}
		// keep a batch size so we dont get heapspace
		final int batchSize = 5000;
		int count = 0;

		Timestamp timeStamp = Utility.getCurrentSqlTimestampUTC();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(
				new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_ID_COL));
		qs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_NAME_COL));
		qs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_LAYOUT_COL));
		qs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.HIDDEN_INSIGHT_COL));
		qs.addSelector(
				new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHEABLE_COL));
		qs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHE_MINUTES_COL));
		qs.addSelector(
				new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHE_CRON_COL));
		qs.addSelector(
				new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHED_ON_COL));
		qs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHE_ENCRYPT_COL));
		qs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_PKQL_COL));
		qs.addSelector(
				new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.SCHEMA_NAME_COL));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rne, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				try {
					// grab the insight rdbms values
					int index = 0;
					String insightId = row[index++].toString();
					String insightName = row[index++].toString();
					String insightLayout = row[index++].toString();
					Boolean isPrivate = (Boolean) row[index++];
					if (isPrivate == null) {
						isPrivate = false;
					}
					Boolean cacheable = (Boolean) row[index++];
					if (cacheable == null) {
						cacheable = true;
					}
					Integer cacheMinutes = (Integer) row[index++];
					if (cacheMinutes == null) {
						cacheMinutes = -1;
					}
					String cacheCron = (String) row[index++];
					SemossDate cachedOn = (SemossDate) row[index++];
					Boolean cacheEncrypt = (Boolean) row[index++];
					if (cacheEncrypt == null) {
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
					ps.setTimestamp(parameterIndex++, timeStamp);
					ps.setTimestamp(parameterIndex++, timeStamp);
					ps.setString(parameterIndex++, insightLayout);
					ps.setBoolean(parameterIndex++, cacheable);
					ps.setInt(parameterIndex++, cacheMinutes);
					if (cacheCron == null) {
						ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
					} else {
						ps.setString(parameterIndex++, cacheCron);
					}
					if (cachedOn == null) {
						ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
					} else {
						ps.setTimestamp(parameterIndex++, Utility.getSqlTimestampUTC(cachedOn));
					}

					ps.setBoolean(parameterIndex++, cacheEncrypt);
					securityQueryUtil.handleInsertionOfClob(ps.getConnection(), ps, pixelObject, parameterIndex++,
							securityGson);

					if (schemaName == null) {
						ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
					} else {
						ps.setString(parameterIndex++, schemaName);
					}

					// add to ps
					ps.addBatch();
					// batch commit based on size
					if (++count % batchSize == 0) {
						classLogger.info("Executing batch .... row num = {}", count);
						ps.executeBatch();
					}

					if (reloadInsights && insightPermissionIds != null && existingInsightPermissions) {
						insightPermissionIds.remove(insightId);
					}
				} catch (SQLException e) {
					classLogger.error("Failed to create the project and initialize project security records", e);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to create the project and initialize project security records", e);
		}

		// well, we are done looping through now
		classLogger.info("Executing final batch .... row num = {}", count);
		try {
			ps.executeBatch();
		} catch (SQLException e) {
			classLogger.error("Failed to create the project and initialize project security records", e);
		}
		// commit
		try {
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to create the project and initialize project security records", e);
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
			classLogger.error("Failed to create the project and initialize project security records", e);
		}

		try {
			ps = securityDb.bulkInsertPreparedStatement(
					new String[] { "INSIGHTMETA", "PROJECTID", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER" });
		} catch (SQLException e1) {
			classLogger.error("Failed to create the project and initialize project security records", e1);
		}

		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__INSIGHTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rne, qs)) {
			while (wrapper.hasNext()) {
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
					securityQueryUtil.handleInsertionOfClob(ps.getConnection(), ps, metaValue, parameterIndex++,
							securityGson);

					// add the order
					ps.setInt(parameterIndex++, ((Number) row[3]).intValue());

					// add to ps
					ps.addBatch();
					// batch commit based on size
					if (++count % batchSize == 0) {
						classLogger.info("Executing batch .... row num = {}", count);
						ps.executeBatch();
					}
				} catch (SQLException e) {
					classLogger.error("Failed to create the project and initialize project security records", e);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to create the project and initialize project security records", e);
		}

		// well, we are done looping through now
		classLogger.info("Executing final batch .... row num = {}", count);
		try {
			ps.executeBatch();
		} catch (SQLException e) {
			classLogger.error("Failed to create the project and initialize project security records", e);
		}
		try {
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to create the project and initialize project security records", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		// close the connection to the insights
		// if the engine is not already loaded
		// since the open method will load it
		if (!projectLoaded && rne != null) {
			rne.close();
		}

		if (reloadInsights) {
			classLogger.info("Modifying force reload to false");
			try {
				Utility.changePropertiesFileValue(smssFile, Constants.RELOAD_INSIGHTS, "false");
			} catch (IOException e) {
				classLogger.error("Failed to create the project and initialize project security records", e);
			}

			// need to remove existing insights w/ permissions that do not exist anymore
			if (existingInsightPermissions && !insightPermissionIds.isEmpty()) {
				classLogger.info("Removing insights with permissions that no longer exist");
				String deleteInsightPermissionQuery = "DELETE FROM USERINSIGHTPERMISSION " + "WHERE PROJECTID='"
						+ projectId + "'" + " AND INSIGHTID " + createFilter(insightPermissionIds);
				try {
					securityDb.removeData(deleteInsightPermissionQuery);
					securityDb.commit();
				} catch (SQLException e) {
					classLogger.error("Failed to create the project and initialize project security records", e);
				}
			}
		}

		classLogger.info("Finished adding project = {}", Utility.cleanLogString(projectId));
	}

	/**
	 * 
	 * @param projectId
	 * @param projectName
	 * @param projectType
	 * @param projectCost
	 * @param portalName
	 * @param global
	 * @param user
	 */
	public static void addProject(String projectId, String projectName, String projectType, String projectCost,
			boolean global, User user) {
		addProject(projectId, projectName, projectName, projectType, projectCost, global, user);
	}

	public static void addProject(String projectId, String projectName, String projectDisplayName, String projectType,
			String projectCost, boolean global, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "INSERT INTO PROJECT (PROJECTID, PROJECTNAME, TYPE, COST, GLOBAL, DISCOVERABLE, IS_TEMPLATE, CREATEDBY, CREATEDBYTYPE, DATECREATED, DATELASTEDITED, PROJECTDISPLAYNAME) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, projectName);
			ps.setString(parameterIndex++, projectType);
			ps.setString(parameterIndex++, projectCost);
			ps.setBoolean(parameterIndex++, global);
			ps.setBoolean(parameterIndex++, false);
			ps.setBoolean(parameterIndex++, false);
			if (user != null) {
				AuthProvider ap = user.getPrimaryLogin();
				AccessToken token = user.getAccessToken(ap);
				ps.setString(parameterIndex++, token.getId());
				ps.setString(parameterIndex++, ap.toString());
			} else {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			}
			ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
			ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
			if (projectDisplayName == null || projectDisplayName.trim().isEmpty()) {
				ps.setString(parameterIndex++, projectName);
			} else {
				ps.setString(parameterIndex++, projectDisplayName);
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to create the project and initialize project security records", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void addProjectOwner(User user, String projectId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		String query = "INSERT INTO PROJECTPERMISSION (USERID, PERMISSION, PROJECTID, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED) VALUES (?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, userId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.OWNER.getId());
			ps.setString(parameterIndex++, projectId);
			ps.setBoolean(parameterIndex++, true);
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to add project owner", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void updateProject(String projectID, String projectName, String projectType, String projectCost,
			boolean global) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to update project properties in the security database", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * For updating Last Edited Date for Apps
	 * 
	 * @param projectID
	 */
	public static void updateProjectLastEditedDate(String projectID) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "UPDATE PROJECT SET DATELASTEDITED=? WHERE PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
			ps.setString(parameterIndex++, projectID);
			ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to update project last edited date", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Delete just the insights for a project
	 * 
	 * @param appId
	 */
	public static void deleteInsightsFromProjectForRecreation(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteQuery = "DELETE FROM INSIGHT WHERE PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to delete project insights before recreation", e);
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		// TODO: potentially take into consideration playsheet legacy insights

		IProject project = Utility.getProject(projectId);
		RdbmsTypeEnum insightType = project.getInsightDatabase().getQueryUtil().getDbType();

		IRDBMSEngine newInsightDatabase = InsightsRDBMSUtils.generateInsightsDatabase(projectId, insightType,
				folderPath);
		InsightsRDBMSUtils.runInsightCreateTableQueries(newInsightDatabase);

		InsightAdministrator admin = new InsightAdministrator(newInsightDatabase);
		{
			boolean error = false;
			PreparedStatement insertPs = null;

			String iprefix = "INSIGHT__";
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector(iprefix + "INSIGHTID"));
			qs.addSelector(new QueryColumnSelector(iprefix + "INSIGHTNAME"));
			qs.addSelector(new QueryColumnSelector(iprefix + "LAYOUT"));
			qs.addSelector(new QueryColumnSelector(iprefix + "CREATEDON"));
			qs.addSelector(new QueryColumnSelector(iprefix + "LASTMODIFIEDON"));
			qs.addSelector(new QueryColumnSelector(iprefix + "GLOBAL"));
			qs.addSelector(new QueryColumnSelector(iprefix + "CACHEABLE"));
			qs.addSelector(new QueryColumnSelector(iprefix + "CACHEMINUTES"));
			qs.addSelector(new QueryColumnSelector(iprefix + "CACHECRON"));
			qs.addSelector(new QueryColumnSelector(iprefix + "CACHEDON"));
			qs.addSelector(new QueryColumnSelector(iprefix + "CACHEENCRYPT"));
			qs.addSelector(new QueryColumnSelector(iprefix + "RECIPE"));
			qs.addSelector(new QueryColumnSelector(iprefix + "SCHEMANAME"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(iprefix + "PROJECTID", "==", projectId));

			try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
				insertPs = admin.getAddInsightPreparedStatement();
				while (wrapper.hasNext()) {
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
					if (sdCachedOn != null) {
						cachedOn = sdCachedOn.getLocalDateTime();
					}
					boolean cacheEncrypt = (Boolean) row[index++];
					String pixelRecipe = (String) row[index++];
					String schemaName = (String) row[index++];

					List<String> pixelList = null;

					if (pixelRecipe != null && !pixelRecipe.isEmpty() && !pixelRecipe.equals("null")) {
						List<String> pixel = securityGson.fromJson(pixelRecipe, List.class);
						int pixelSize = pixel.size();
						pixelList = new ArrayList<>(pixelSize);
						for (int i = 0; i < pixelSize; i++) {
							String pixelString = pixel.get(i).toString();
							List<String> breakdown;
							try {
								breakdown = PixelUtility.parsePixel(pixelString);
								pixelList.addAll(breakdown);
							} catch (ParserException | LexerException | IOException e) {
								classLogger.error("Failed to create or initialize the project insights database", e);
								throw new IllegalArgumentException("Error occurred parsing the pixel expression");
							}
						}
					} else {
						classLogger.warn("Cannot write insight id '{}' with no pixel recipe", insightId);
						continue;
					}

					admin.batchInsight(insertPs, insightId, insightName, insightLayout, pixelList, global, cacheable,
							cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
				}

				insertPs.executeBatch();
				if (!insertPs.getConnection().getAutoCommit()) {
					insertPs.getConnection().commit();
				}
			} catch (Exception e) {
				error = true;
				classLogger.error("Failed to create or initialize the project insights database", e);
				throw new IllegalArgumentException("Error occured creating the insights database");
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
				if (error) {
					try {
						newInsightDatabase.close();
					} catch (Exception e) {
						classLogger.error("Failed to create or initialize the project insights database", e);
					}
					String databaseFileLocation = newInsightDatabase.getSmssProp()
							.getProperty(AbstractSqlQueryUtil.HOSTNAME);
					File databaseFile = new File(databaseFileLocation);
					if (databaseFile.exists() && databaseFile.isFile()) {
						databaseFile.delete();
					}
				}
			}
		}
		{
			PreparedStatement insertPs = null;

			String iprefix = "INSIGHTMETA__";
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector(iprefix + "INSIGHTID"));
			qs.addSelector(new QueryColumnSelector(iprefix + "METAKEY"));
			qs.addSelector(new QueryColumnSelector(iprefix + "METAVALUE"));
			qs.addSelector(new QueryColumnSelector(iprefix + "METAORDER"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(iprefix + "PROJECTID", "==", projectId));

			try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
				insertPs = admin.getAddInsightMetaPreparedStatement();
				while (wrapper.hasNext()) {
					Object[] row = wrapper.next().getValues();

					int index = 0;
					String insightId = (String) row[index++];
					String metaKey = (String) row[index++];
					String metaValue = (String) row[index++];
					int metaOrder = ((Number) row[index++]).intValue();

					admin.batchInsightMetadata(insertPs, insightId, metaKey, metaValue, metaOrder);
				}

				insertPs.executeBatch();
				if (!insertPs.getConnection().getAutoCommit()) {
					insertPs.getConnection().commit();
				}
			} catch (Exception e) {
				// insight metadata is not as important, log the error
				classLogger.error("Failed to create or initialize the project insights database", e);
			} finally {
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
	 * 
	 * @return
	 */
	public static String testUserProjectIdForAlias(User user, String potentialId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<String> ids = new ArrayList<String>();

		// String userFilters = getUserFilters(user);
		// String query = "SELECT DISTINCT PROJECTPERMISSION.PROJECTID "
		// + "FROM PROJECTPERMISSION INNER JOIN PROJECT ON
		// PROJECT.PROJECTID=PROJECTPERMISSION.PROJECTID "
		// + "WHERE PROJECT.PROJECTNAME='" + potentialId + "' AND
		// PROJECTPERMISSION.USERID IN " + userFilters;
		//
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "==", potentialId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "inner.join");

		ids = QueryExecutionUtility.flushToListString(securityDb, qs);
		if (ids.isEmpty()) {
			// query = "SELECT DISTINCT PROJECT.PROJECTID FROM PROJECT WHERE
			// PROJECT.PROJECTNAME='" + potentialId + "' AND PROJECT.GLOBAL=TRUE";

			qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "==", potentialId));
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));

			ids = QueryExecutionUtility.flushToListString(securityDb, qs);
		}

		if (ids.size() == 1) {
			potentialId = ids.get(0);
		} else if (ids.size() > 1) {
			throw new IllegalArgumentException("There are 2 projects with the name " + potentialId
					+ ". Please pass in the correct id to know which source you want to load from");
		}

		return potentialId;
	}

	/**
	 * Get the engine alias for a id
	 *
	 * @return
	 */
	public static String getProjectAliasForId(String id) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// String query = "SELECT PROJECTNAME FROM PROJECT WHERE PROJECTID='" + id +
		// "'";
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", id));
		List<String> results = QueryExecutionUtility.flushToListString(securityDb, qs);
		if (results.isEmpty()) {
			return null;
		}
		return results.get(0);
	}

	/**
	 * Get the display name for a project id. Falls back to canonical name
	 * (PROJECTNAME) if display name is null or blank.
	 *
	 * @param id
	 * @return
	 */
	public static String getProjectDisplayNameForId(String id) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTDISPLAYNAME"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", id));
		List<Object[]> results = QueryExecutionUtility.flushRsToListOfObjArray(securityDb, qs);
		if (results.isEmpty()) {
			return null;
		}
		Object[] row = results.get(0);
		String displayName = row[0] != null ? row[0].toString() : null;
		String canonicalName = row[1] != null ? row[1].toString() : null;
		if (displayName == null || displayName.trim().isEmpty()) {
			return canonicalName;
		}
		return displayName;
	}

	/**
	 * Get the catalog type (PROJECT.TYPE, e.g. SKILL / WORKSPACE / CODE) for a
	 * project id. Returns null when the project row does not exist. This is a
	 * single securitydb query - unlike {@code Utility.getProject(id)} it never
	 * force-loads the project, so it is safe for validation loops.
	 *
	 * @param id project id
	 * @return the PROJECT.TYPE value, or null when the project is not cataloged
	 */
	public static String getProjectTypeForId(String id) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", id));
		List<String> results = QueryExecutionUtility.flushToListString(securityDb, qs);
		if (results.isEmpty()) {
			return null;
		}
		return results.get(0);
	}

	/**
	 * Set the display name for a project. Only the project owner can perform this
	 * action.
	 *
	 * @param user
	 * @param projectId
	 * @param newDisplayName
	 * @return
	 * @throws IllegalAccessException
	 */
	public static boolean setProjectDisplayName(User user, String projectId, String newDisplayName)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityUserProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to change the project display name. Only the owner can perform this action.");
		}
		if (newDisplayName == null || newDisplayName.trim().isEmpty()) {
			throw new IllegalArgumentException("Display name cannot be null or blank.");
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECT SET PROJECTDISPLAYNAME=? WHERE PROJECTID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newDisplayName);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project display name", e);
			throw new IllegalArgumentException("An error occurred updating the project display name");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		return true;
	}

	/**
	 * Get user databases + global databases
	 * 
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
	 * 
	 * @return
	 */
	public static Set<String> getGlobalProjectIds() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		return QueryExecutionUtility.flushToSetString(securityDb, qs, false);
	}

	/**
	 * Get what permission the user has for a given app
	 * 
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static String getActualUserProjectPermission(User user, String projectId) {
		String userPermission = SecurityUserProjectUtils.getActualUserProjectPermission(user, projectId);
		List<String> groupUserPermissions = SecurityUserProjectUtils.getActualGroupUserProjectPermission(user,
				projectId);
		return SecurityUserProjectUtils.getHighestProjectPermission(userPermission, groupUserPermissions);
	}

	/**
	 * Get a list of the project ids
	 * 
	 * @return
	 */
	public static List<String> getAllProjectIds() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	/**
	 * 
	 * @param projectId
	 * @return
	 */
	public static Map<String, Object> getProjectPortalDetailsMap(String projectId) {
		Map<String, Object> portalDetails = new HashMap<>();

		IProject project = Utility.getProject(projectId);
		portalDetails.put("project_is_published", project.isPublished());
		String url = Utility.getApplicationUrl() + "/" + Utility.getPublicHomeFolder() + "/" + projectId + "/"
				+ Constants.PORTALS_FOLDER + "/";
		portalDetails.put("project_portal_url", url);
		return portalDetails;
	}

	/**
	 * Get markdown for a given project
	 * 
	 * @param user
	 * @param projectId
	 * @return
	 */
	public static String getProjectMarkdown(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", Constants.MARKDOWN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__PROJECTID", "==", projectId));
		{
			SelectQueryStruct qs1 = new SelectQueryStruct();
			qs1.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
			{
				OrQueryFilter orFilter = new OrQueryFilter();
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__DISCOVERABLE", "==",
						Arrays.asList(true, null), PixelDataType.BOOLEAN));
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==",
						getUserFiltersQs(user)));
				qs1.addExplicitFilter(orFilter);
			}
			qs1.addRelation("PROJECT", "PROJECTPERMISSION", "join");
			IRelation subQuery = new SubqueryRelationship(qs1, "PROJECT", "join",
					new String[] { "PROJECT__PROJECTID", "PROJECTMETA__PROJECTID", "=" });
			qs.addRelation(subQuery);
		}
		return QueryExecutionUtility.flushToString(securityDb, qs);
	}

	/**
	 * Get the project permissions for a specific user
	 * 
	 * @param singleUserId
	 * @param projectId
	 * @return
	 */
	public static Integer getUserProjectPermission(String singleUserId, String projectId) {
		return SecurityUserProjectUtils.getUserProjectPermission(singleUserId, projectId);
	}

	/**
	 * Get the project permissions for a specific user
	 * 
	 * @param singleUserId
	 * @param projectId
	 * @return
	 */
	public static Map<String, Integer> getUserProjectPermissions(List<String> userIds, String projectId) {
		Map<String, Integer> retMap = new HashMap<String, Integer>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = getUserProjectPermissionsWrapper(userIds, projectId);
			while (wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String userId = (String) data[0];
				Integer permission = (Integer) data[1];
				retMap.put(userId, permission);
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve user project permissions", e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error("Failed to retrieve user project permissions", e);
				}
			}
		}
		return retMap;
	}

	/**
	 * Get the project permissions for a specific user
	 * 
	 * @param singleUserId
	 * @param projectId
	 * @return
	 */
	public static IRawSelectWrapper getUserProjectPermissionsWrapper(List<String> userIds, String projectId)
			throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}

	/**
	 * See if project exists
	 * 
	 * @return
	 */
	public static boolean projectExists(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Failed to check whether project exists", e);
		}
		return false;
	}

	/**
	 * See if specific project is global
	 * 
	 * @return
	 */
	public static boolean projectIsGlobal(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// String query = "SELECT ENGINEID FROM ENGINE WHERE GLOBAL=TRUE and ENGINEID='"
		// + engineId + "'";
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Failed to determine whether project is global", e);
		}
		return false;
	}

	/**
	 * Determine whether a project has been explicitly enabled as a template.
	 *
	 * @param projectId project identifier
	 * @return {@code true} only when the persisted template flag is true
	 */
	public static boolean projectIsTemplate(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECT__IS_TEMPLATE", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Failed to determine whether project is enabled as a template", e);
			return false;
		}
	}

	/**
	 * Determine whether a user may clone a project. Admins, owners, and editors
	 * may always clone. Viewers may only clone when the owner has explicitly
	 * enabled the project as a template.
	 *
	 * @param user      current user
	 * @param projectId project identifier
	 * @return whether the user may clone the project
	 */
	public static boolean userCanCloneProject(User user, String projectId) {
		if (SecurityAdminUtils.userIsAdmin(user)) {
			return true;
		}
		if (userCanEditProject(user, projectId)) {
			return true;
		}
		return userCanViewProject(user, projectId) && projectIsTemplate(projectId);
	}

	/**
	 * 
	 * @param projectId
	 * @return
	 */
	public static SemossDate getPortalPublishedTimestamp(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return (SemossDate) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve project portal published timestamp", e);
		}
		return null;
	}

	public static void setPortalPublish(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String updateQ = "UPDATE PROJECT SET DATELASTEDITED=?, PORTALPUBLISHED=?, PORTALPUBLISHEDUSER=?, PORTALPUBLISHEDTYPE=? WHERE PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int i = 1;
			ps.setTimestamp(i++, Utility.getCurrentSqlTimestampUTC());
			ps.setTimestamp(i++, Utility.getCurrentSqlTimestampUTC());
			ps.setString(i++, token.getId());
			ps.setString(i++, token.getProvider().toString());
			ps.setString(i++, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project portal published status", e);
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return (SemossDate) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve project reactor compilation timestamp", e);
		}
		return null;
	}

	public static void setReactorCompilation(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String updateQ = "UPDATE PROJECT SET REACTORSCOMPILED=?, REACTORSCOMPILEDUSER=?, REACTORSCOMPILEDTYPE=? WHERE PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int i = 1;
			ps.setTimestamp(i++, Utility.getCurrentSqlTimestampUTC());
			ps.setString(i++, token.getId());
			ps.setString(i++, token.getProvider().toString());
			ps.setString(i++, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project reactor compilation timestamp", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Determine if the user is the owner of a project
	 * 
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
	 * 
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
	 * 
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
	 * 
	 * @param singleUserId
	 * @param projectId
	 * @return
	 */
	public static Integer getUserAccessRequestProjectPermission(String userId, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__PERMISSION"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__REQUEST_USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__PROJECTID", "==", projectId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__APPROVER_DECISION", "==", "NEW_REQUEST"));
		return QueryExecutionUtility.flushToInteger(securityDb, qs);
	}

	/**
	 * Get Project max permission for a user
	 * 
	 * @param userId
	 * @param projectId
	 * @return
	 */
	static int getMaxUserProjectPermission(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// String userFilters = getUserFilters(user);
		// // query the database
		// String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM
		// ENGINEPERMISSION "
		// + "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters + " ORDER
		// BY PERMISSION";
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addOrderBy(new QueryColumnOrderBySelector("PROJECTPERMISSION__PERMISSION"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val == null) {
					return AccessPermissionEnum.READ_ONLY.getId();
				}
				int permission = ((Number) val).intValue();
				return permission;
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve project permission from user access request", e);
		}
		return AccessPermissionEnum.READ_ONLY.getId();
	}

	/**
	 * Retrieve the list of users for a given project with parameters
	 * 
	 * @param user
	 * @param projectId
	 * @param searchParam
	 * @param permission
	 * @param limit
	 * @param offset
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getProjectUsers(User user, String projectId, String searchParam,
			String permission, long limit, long offset) throws IllegalAccessException {
		if (!userCanViewProject(user, projectId)) {
			throw new IllegalAccessException("The user does not have access to view this project");
		}
		return SecurityUserProjectUtils.getProjectUsers(projectId, searchParam, permission, limit, offset);
	}

	public static long getProjectUsersCount(User user, String projectId, String searchParam, String permission)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!userCanViewProject(user, projectId)) {
			throw new IllegalAccessException("The user does not have access to view this project");
		}
		boolean hasSearchParam = searchParam != null && !(searchParam = searchParam.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission = permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("count");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(fSelector);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		if (hasSearchParam) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchParam));
			qs.addExplicitFilter(or);
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "==",
					AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "PROJECTPERMISSION", "inner.join");
		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "inner.join");
		return QueryExecutionUtility.flushToLong(securityDb, qs);
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
	public static void addProjectUser(User user, String newUserId, String projectId, String permission, String endDate)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// make sure user can edit the app
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// make sure user doesn't already exist for this insight
		if (getUserProjectPermission(newUserId, projectId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException(
					"This user already has access to this project. Please edit the existing permission level.");
		}

		// if i am not an owner
		// then i need to check if i can edit this users permission
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			int newPermissionLvl = AccessPermissionEnum.getIdByPermission(permission);

			// cannot give some owner permission if i am just an editor
			if (AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException(
						"Cannot give owner level access to this project since you are not currently an owner.");
			}
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, VISIBILITY, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newUserId);
			ps.setString(parameterIndex++, projectId);
			ps.setBoolean(parameterIndex++, true);
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission));
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			if (verifiedEndDate != null) {
				ps.setTimestamp(parameterIndex++, verifiedEndDate);
			} else {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to add project user", e);
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
	public static void editProjectUserPermission(User user, String existingUserId, String existingUserType,
			String projectId, String newPermission, String endDate) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// make sure user can edit the app
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserProjectPermission(existingUserId, projectId);
		if (existingUserPermission == null) {
			throw new IllegalAccessException(
					"Attempting to modify project permission for a user who does not currently have access to the project");
		}

		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);

		// if i am not an owner
		// then i need to check if i can edit this users permission
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if (AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException(
						"The user doesn't have the high enough permissions to modify this users project permission.");
			}

			// also, cannot give some owner permission if i am just an editor
			if (AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException(
						"Cannot give owner level access to this project since you are not currently an owner.");
			}
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"UPDATE PROJECTPERMISSION SET PERMISSION=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=?, DATEADDED=?, ENDDATE=? WHERE USERID=? AND PROJECTID=?");
			int parameterIndex = 1;
			// SET
			ps.setInt(parameterIndex++, newPermissionLvl);
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			// WHERE
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}

			// Adding Notification
			// Check notificationDb (conditional)
			if (Utility.isNotificationDatabaseEnabled()) {
				String existingPermission = AccessPermissionEnum.getPermissionValueById(existingUserPermission);
				NotificationDbUtils.createNotification(user, existingUserId, existingUserType, projectId,
						NotificationConstants.Type.PERMISSION_CHANGE, NotificationConstants.APP_CATALOG,
						NotificationConstants.Priority.MEDIUM, existingPermission, newPermission);
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project user permission", e);
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
	public static void editProjectUserPermissions(User user, String projectId, List<Map<String, String>> requests,
			String endDate) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// make sure user can edit the database
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// get userid of all requests
		List<String> existingUserIds = new ArrayList<String>();
		for (Map<String, String> i : requests) {
			existingUserIds.add(i.get("userid"));
		}

		// get user permissions to edit
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(existingUserIds,
				projectId);

		// make sure all users to edit currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for the following users who do not currently have access to the project: "
							+ String.join(",", toRemoveUserIds));
		}

		// if user is not an owner, check to make sure they are not editting owner
		// access
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			List<Integer> permissionList = new ArrayList<Integer>(existingUserPermission.values());
			if (permissionList.contains(AccessPermissionEnum.OWNER.getId())) {
				throw new IllegalArgumentException("As a non-owner, you cannot edit access of an owner.");
			}

			// also make sure, you are not adding an owner
			for (Map<String, String> req : requests) {
				if (AccessPermissionEnum.OWNER.getId() == AccessPermissionEnum
						.getIdByPermission(req.get("permission"))) {
					throw new IllegalArgumentException("As a non-owner, you cannot give a user access as an owner.");
				}
			}
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		// update user permissions in bulk
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"UPDATE PROJECTPERMISSION SET PERMISSION = ?, PERMISSIONGRANTEDBY = ?, PERMISSIONGRANTEDBYTYPE = ?, DATEADDED = ?, ENDDATE = ? WHERE USERID = ? AND PROJECTID = ?");
			for (int i = 0; i < requests.size(); i++) {
				int parameterIndex = 1;

				String newUserId = requests.get(i).get("userid");
				String newUserType = requests.get(i).get("type");
				String existingPermission = AccessPermissionEnum
						.getPermissionValueById(getUserProjectPermission(newUserId, projectId));
				// SET
				ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setTimestamp(parameterIndex++, startDate);
				ps.setTimestamp(parameterIndex++, verifiedEndDate);
				// WHERE
				ps.setString(parameterIndex++, requests.get(i).get("userid"));
				ps.setString(parameterIndex++, projectId);
				ps.addBatch();

				// Adding Notification
				if (Utility.isNotificationDatabaseEnabled()) {
					NotificationDbUtils.createNotification(user, newUserId, newUserType, projectId,
							NotificationConstants.Type.PERMISSION_CHANGE, NotificationConstants.APP_CATALOG,
							NotificationConstants.Priority.MEDIUM, existingPermission,
							requests.get(i).get("permission"));
				}
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project user permissions", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Delete all values
	 * 
	 * @param projectId
	 */
	public static void deleteProject(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<String> deletes = new ArrayList<>();
		deletes.add("DELETE FROM PROJECT WHERE PROJECTID=?");
		deletes.add("DELETE FROM INSIGHT WHERE PROJECTID=?");
		deletes.add("DELETE FROM PROJECTPERMISSION WHERE PROJECTID=?");
		deletes.add("DELETE FROM PROJECTMETA WHERE PROJECTID=?");
		deletes.add("DELETE FROM ASSETENGINE WHERE PROJECTID=?");
		deletes.add("DELETE FROM PROJECTACCESSREQUEST WHERE PROJECTID=?");
		deletes.add("DELETE FROM PROJECTDEPENDENCIES WHERE PROJECTID=?");
		for (String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(deleteQuery);
				ps.setString(1, projectId);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to delete project", e);
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
	public static void removeProjectUser(User user, String existingUserId, String projectId)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user can edit the app
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)
				&& !user.getPrimaryLoginToken().getId().equals(existingUserId)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserProjectPermission(existingUserId, projectId);
		if (existingUserPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for a user who does not currently have access to the project");
		}

		// if i am not an ownerId
		// then i need to check if i can remove this users permission
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if (AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException(
						"The user doesn't have the high enough permissions to modify this users project permission.");
			}
		}

		String[] deletes = new String[] { "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?",
				"DELETE FROM USERINSIGHTPERMISSION WHERE USERID=? AND PROJECTID=?" };
		for (String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(deleteQuery);
				int parameterIndex = 1;
				ps.setString(parameterIndex++, existingUserId);
				ps.setString(parameterIndex++, projectId);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to remove project user", e);
				throw new IllegalArgumentException(
						"An error occurred removing the user permissions for the project and insights of this project");
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}

	/**
	 * 
	 * @param userId
	 * @param projectId
	 * @return
	 */
	public static void removeExpiredProjectUser(String userId, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteQuery = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			throw new IllegalArgumentException("An error occurred removing the user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Set if the project is public to all users on this instance
	 * 
	 * @param user
	 * @param projectId
	 * @param global
	 * @return
	 * @throws IllegalAccessException
	 */
	public static boolean setProjectGlobal(User user, String projectId, boolean global) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityUserProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to set this project as global. Only the owner or an admin can perform this action.");
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECT SET GLOBAL=? WHERE PROJECTID=?");
			ps.setBoolean(1, global);
			ps.setString(2, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project global visibility setting", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}

	/**
	 * Set whether viewers may clone a project as a template.
	 *
	 * @param user       current user
	 * @param projectId  project identifier
	 * @param isTemplate whether the project is a template
	 * @return {@code true} when the flag is updated
	 * @throws IllegalAccessException when the user is not the project owner
	 */
	public static boolean setProjectTemplate(User user, String projectId, boolean isTemplate)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityUserProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalAccessException(
					"The user doesn't have permission to set this project as a template. Only the owner or an admin can perform this action.");
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECT SET IS_TEMPLATE=? WHERE PROJECTID=?");
			ps.setBoolean(1, isTemplate);
			ps.setString(2, projectId);
			int updatedRows = ps.executeUpdate();
			if (updatedRows != 1) {
				throw new IllegalArgumentException("Project does not exist");
			}
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update project template setting", e);
			throw new IllegalArgumentException("An error occurred setting the project template flag", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}

	/**
	 * Set a project and all its insights to be global
	 * 
	 * @param projectId
	 */
	public static void setProjectCompletelyGlobal(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		{
			String update1 = "UPDATE PROJECT SET GLOBAL=? WHERE PROJECTID=?";
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(update1);
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, true);
				ps.setString(parameterIndex++, projectId);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to set project visibility to globally accessible", e);
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
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to set project visibility to globally accessible", e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}

	/**
	 * Set project discoverable
	 * 
	 * @param user
	 * @param projectId
	 * @param discoverable
	 * @return
	 * @throws IllegalAccessException
	 */
	public static boolean setProjectDiscoverable(User user, String projectId, boolean discoverable)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to set this project as discoverable. Only the owner or an admin can perform this action.");
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECT SET DISCOVERABLE=? WHERE PROJECTID=?");
			ps.setBoolean(1, discoverable);
			ps.setString(2, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project discoverability setting", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}

	/**
	 * update the project name
	 * 
	 * @param user
	 * @param projectId
	 * @param isPublic
	 * @return
	 * @throws IllegalAccessException
	 */
	public static boolean setProjectName(User user, String projectId, String newProjectName)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityUserProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to change the project name. Only the owner or an admin can perform this action.");
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
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project name", e);
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
	 * Update the project dependencies Will delete existing values and then perform
	 * a bulk insert
	 * 
	 * @param user
	 * @param projectId
	 * @param dependentEngineIds
	 */
	@Deprecated
	public static void updateProjectDependenciesWithoutType(User user, String projectId,
			Collection<String> dependentEngineIds) {
		List<Map<String, Object>> depEngines = new ArrayList<>();
		for (String depEngineId : dependentEngineIds) {
			Map<String, Object> depEngine = new HashMap<>();
			depEngine.put("ENGINEID", depEngineId);
			IEngine engine = null;
			try {
				engine = Utility.getEngine(depEngineId);
				depEngine.put("ENGINETYPE", engine.getCatalogType().name());
			} catch (Exception ex) {
				// ignore
			}
			if (engine == null) {
				engine = Utility.getProject(depEngineId);
				depEngine.put("ENGINETYPE", IEngine.CATALOG_TYPE.PROJECT.name());
			}
			depEngines.add(depEngine);
		}
		updateProjectDependencies(user, projectId, depEngines);
	}

	/**
	 * Update the project dependencies Will delete existing values and then perform
	 * a bulk insert
	 * 
	 * @param user
	 * @param projectId
	 * @param dependentEngineIds
	 */
	public static void updateProjectDependencies(User user, String projectId,
			List<Map<String, Object>> dependentEngines) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// first do a delete
		String deleteQ = "DELETE FROM PROJECTDEPENDENCIES WHERE PROJECTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			int parameterIndex = 1;
			deletePs.setString(parameterIndex++, projectId);
			deletePs.execute();
			ConnectionUtils.commitConnection(deletePs.getConnection());
		} catch (Exception e) {
			classLogger.error("Failed to update project dependency mappings", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}

		if (dependentEngines != null && !dependentEngines.isEmpty()) {
			AccessToken token = user.getPrimaryLoginToken();
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			// now we do the new insert with the order of the tags
			String query = securityDb.getQueryUtil().createInsertPreparedStatementString("PROJECTDEPENDENCIES",
					new String[] { "PROJECTID", "ENGINEID", "ENGINETYPE", "USERID", "TYPE", "DATEADDED" });
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(query);
				for (Map<String, Object> depEngine : dependentEngines) {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, projectId);
					ps.setString(parameterIndex++, (String) depEngine.get("ENGINEID"));
					ps.setString(parameterIndex++, (String) depEngine.get("ENGINETYPE"));
					ps.setString(parameterIndex++, token.getId());
					ps.setString(parameterIndex++, token.getProvider().getLabel());
					ps.setTimestamp(parameterIndex++, timestamp);
					ps.addBatch();
				}
				ps.executeBatch();
				ConnectionUtils.commitConnection(ps.getConnection());
			} catch (Exception e) {
				classLogger.error("Failed to update project dependency mappings", e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}

	/**
	 * Remove dependency from project
	 * 
	 * @param user
	 * @param projectId
	 * @param dependentEngineId
	 */
	public static void removeProjectDependency(User user, String projectId, String dependentEngineId)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		if (!SecurityUserProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalAccessException("User does not have permissions to remove dependencies");
		}

		String deleteQ = "DELETE FROM PROJECTDEPENDENCIES WHERE PROJECTID=? AND ENGINEID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			int parameterIndex = 1;
			deletePs.setString(parameterIndex++, projectId);
			deletePs.setString(parameterIndex++, dependentEngineId);
			deletePs.execute();
			ConnectionUtils.commitConnection(deletePs.getConnection());
		} catch (Exception e) {
			classLogger.error("Failed to remove project dependency mappings", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}
	}

	/**
	 * Get project dependencies
	 * 
	 * @param projectId
	 * @return
	 */
	public static List<Map<String, Object>> getProjectDependencies(String projectId) {
		return getProjectDependencies(projectId, false);
	}

	/**
	 * Get project dependencies with optional subdependencies
	 * 
	 * @param projectId
	 * @param subdependencies if true, recursively fetch subdependencies
	 * @return
	 */
	public static List<Map<String, Object>> getProjectDependencies(String projectId, boolean subdependencies) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (subdependencies) {
			Set<String> visited = new HashSet<>();
			List<Map<String, Object>> allDependencies = new ArrayList<>();
			getProjectDependenciesRecursive(projectId, visited, allDependencies);
			return allDependencies;
		} else {
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__PROJECTID", "parent_id"));
			qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__ENGINEID", "engine_id"));
			qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__ENGINETYPE", "engine_type"));
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__PROJECTID", "==", projectId));
			return QueryExecutionUtility.flushRsToMap(securityDb, qs);
		}
	}

	/**
	 * Helper method to recursively fetch project dependencies
	 * 
	 * @param projectId
	 * @param visited         Set to track visited projects to avoid circular
	 *                        dependencies
	 * @param allDependencies Accumulated list of all dependencies
	 */
	private static void getProjectDependenciesRecursive(String projectId, Set<String> visited,
			List<Map<String, Object>> allDependencies) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// Avoid circular dependencies
		if (visited.contains(projectId)) {
			return;
		}
		visited.add(projectId);

		// Query direct dependencies
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__PROJECTID", "parent_id"));
		qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__ENGINETYPE", "engine_type"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__PROJECTID", "==", projectId));

		List<Map<String, Object>> directDependencies = QueryExecutionUtility.flushRsToMap(securityDb, qs);

		// Add direct dependencies and recurse for PROJECT type dependencies
		for (Map<String, Object> dependency : directDependencies) {
			allDependencies.add(dependency);
			String engineId = (String) dependency.get("engine_id");
			getProjectDependenciesRecursive(engineId, visited, allDependencies);
		}
	}

	/**
	 * Get project dependency details
	 * 
	 * @param projectId
	 * @return
	 */
	public static List<Map<String, Object>> getProjectDependencyDetails(String projectId) {
		return getProjectDependencyDetails(projectId, false);
	}

	/**
	 * Get project dependency details with optional subdependencies
	 * 
	 * @param projectId
	 * @param subdependencies if true, recursively fetch subdependencies
	 * @return
	 */
	public static List<Map<String, Object>> getProjectDependencyDetails(String projectId, boolean subdependencies) {
		if (subdependencies) {
			Set<String> visited = new HashSet<>();
			List<Map<String, Object>> allDependencies = new ArrayList<>();
			getProjectDependencyDetailsRecursive(projectId, visited, allDependencies);
			return allDependencies;
		} else {
			return getProjectDependencyDetailsQuery(projectId);
		}
	}

	/**
	 * Helper method to recursively fetch project dependency details
	 * 
	 * @param projectId
	 * @param visited         Set to track visited projects to avoid circular
	 *                        dependencies
	 * @param allDependencies Accumulated list of all dependencies
	 */
	private static void getProjectDependencyDetailsRecursive(String projectId, Set<String> visited,
			List<Map<String, Object>> allDependencies) {
		// Avoid circular dependencies
		if (visited.contains(projectId)) {
			return;
		}
		visited.add(projectId);

		// Query direct dependencies
		List<Map<String, Object>> directDependencies = getProjectDependencyDetailsQuery(projectId);

		// Add direct dependencies and recurse for PROJECT type dependencies
		for (Map<String, Object> dependency : directDependencies) {
			allDependencies.add(dependency);

			String engineId = (String) dependency.get("engine_id");
			getProjectDependencyDetailsRecursive(engineId, visited, allDependencies);
		}
	}

	/**
	 * Query to get project dependency details for a single project
	 * 
	 * @param projectId
	 * @return
	 */
	private static List<Map<String, Object>> getProjectDependencyDetailsQuery(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__PROJECTID", "parent_id"));
		qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__ENGINETYPE", "engine_type"));

		// Use conditional selectors based on engine type
		QueryIfSelector engineNameSelector = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
				new QueryColumnSelector("PROJECT__PROJECTNAME"), new QueryColumnSelector("ENGINE__ENGINENAME"),
				"engine_name");
		qs.addSelector(engineNameSelector);

		QueryIfSelector engineSubtypeSelector = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
				new QueryColumnSelector("PROJECT__TYPE"), new QueryColumnSelector("ENGINE__ENGINESUBTYPE"),
				"engine_subtype");
		qs.addSelector(engineSubtypeSelector);

		QueryIfSelector engineDateCreatedSelector = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
				new QueryColumnSelector("PROJECT__DATECREATED"), new QueryColumnSelector("ENGINE__DATECREATED"),
				"engine_date_created");
		qs.addSelector(engineDateCreatedSelector);

		QueryIfSelector engineDiscoverableSelector = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
				new QueryColumnSelector("PROJECT__DISCOVERABLE"), new QueryColumnSelector("ENGINE__DISCOVERABLE"),
				"engine_discoverable");
		qs.addSelector(engineDiscoverableSelector);

		QueryIfSelector engineGlobalSelector = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
				new QueryColumnSelector("PROJECT__GLOBAL"), new QueryColumnSelector("ENGINE__GLOBAL"), "engine_global");
		qs.addSelector(engineGlobalSelector);

		// Add joins with proper join conditions
		qs.addRelation("PROJECTDEPENDENCIES__ENGINEID", "ENGINE__ENGINEID", "left.outer.join");
		qs.addRelation("PROJECTDEPENDENCIES__ENGINEID", "PROJECT__PROJECTID", "left.outer.join");

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__PROJECTID", "==", projectId));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get project dependency details with user and group permissions
	 * 
	 * @param projectId
	 * @param user
	 * @return
	 */
	public static List<Map<String, Object>> getProjectDependencyDetails(String projectId, User user) {
		return getProjectDependencyDetails(projectId, user, false);
	}

	/**
	 * Get project dependency details with user and group permissions and optional
	 * subdependencies
	 * 
	 * @param projectId
	 * @param user
	 * @param subdependencies if true, recursively fetch subdependencies
	 * @return
	 */
	public static List<Map<String, Object>> getProjectDependencyDetails(String projectId, User user,
			boolean subdependencies) {
		if (subdependencies) {
			Set<String> visited = new HashSet<>();
			List<Map<String, Object>> allDependencies = new ArrayList<>();
			getProjectDependencyDetailsWithUserRecursive(projectId, user, visited, allDependencies);

			// Calculate can_view_dependencies for each dependency
			calculateCanViewDependencies(allDependencies);

			return allDependencies;
		} else {
			return getProjectDependencyDetailsWithUserQuery(projectId, user);
		}
	}

	/**
	 * Calculate can_view_dependencies field for each dependency by checking if user
	 * has view access to all subdependencies. Works from leaf nodes up the tree.
	 * Now accounts for both user permissions and group permissions.
	 * 
	 * @param allDependencies List of all dependencies (flat structure)
	 */
	private static void calculateCanViewDependencies(List<Map<String, Object>> allDependencies) {
		// Build a map of engineId -> dependency for quick lookup
		Map<String, Map<String, Object>> dependencyMap = new HashMap<>();
		for (Map<String, Object> dep : allDependencies) {
			String engineId = (String) dep.get("engine_id");
			dependencyMap.put(engineId, dep);
		}

		// Build a map of parent -> list of children directly from allDependencies
		// Since we already recursively fetched everything, we have all the parent-child
		// relationships
		Map<String, List<String>> childrenMap = new HashMap<>();

		for (Map<String, Object> dep : allDependencies) {
			String parentId = (String) dep.get("parent_id");
			String engineId = (String) dep.get("engine_id");

			// Add this engine as a child of its parent (skip self-references to prevent
			// infinite loops)
			if (parentId != null && !parentId.equals(engineId)) {
				childrenMap.computeIfAbsent(parentId, k -> new ArrayList<>()).add(engineId);
			}
		}

		// Calculate can_view_dependencies from leaf nodes up
		Map<String, Boolean> canViewCache = new HashMap<>();
		Set<String> visiting = new HashSet<>();
		for (Map<String, Object> dep : allDependencies) {
			String engineId = (String) dep.get("engine_id");
			boolean canView = calculateCanViewRecursive(engineId, dependencyMap, childrenMap, canViewCache, visiting);
			dep.put("can_view_dependencies", canView);
		}
	}

	/**
	 * Recursively calculate if user can view all subdependencies for a given engine
	 * 
	 * @param engineId      The engine ID to check
	 * @param dependencyMap Map of engineId -> dependency data
	 * @param childrenMap   Map of engineId -> list of child engine IDs
	 * @param canViewCache  Cache to avoid recalculating
	 * @param visiting      Set of engine IDs currently being processed (to detect
	 *                      circular dependencies)
	 * @return true if user has view access to this engine and all its
	 *         subdependencies
	 */
	private static boolean calculateCanViewRecursive(String engineId, Map<String, Map<String, Object>> dependencyMap,
			Map<String, List<String>> childrenMap, Map<String, Boolean> canViewCache, Set<String> visiting) {
		// Check cache first
		if (canViewCache.containsKey(engineId)) {
			return canViewCache.get(engineId);
		}

		// Detect circular dependency - if we're already visiting this node, there's a
		// cycle. Every node in the visiting set has already had its own
		// hasViewPermission
		// checked, so the back-edge doesn't introduce any new permission requirement.
		// Return true to avoid poisoning the result for all nodes in the cycle.
		if (visiting.contains(engineId)) {
			return true;
		}

		Map<String, Object> dependency = dependencyMap.get(engineId);
		if (dependency == null) {
			canViewCache.put(engineId, false);
			return false;
		}

		// Check if user has view permission for this engine
		// Account for both user permissions and group permissions
		boolean hasViewPermission = false;
		Integer permission = (Integer) dependency.get("permission");
		Integer groupPermission = (Integer) dependency.get("group_permission");
		Boolean isGlobal = (Boolean) dependency.get("engine_global");

		// User has view permission if they have any user permission, group permission,
		// or if engine is global
		hasViewPermission = (permission != null) || (groupPermission != null) || (isGlobal != null && isGlobal);

		// If this is a leaf node (no children), return the permission status
		List<String> children = childrenMap.get(engineId);
		if (children == null || children.isEmpty()) {
			canViewCache.put(engineId, hasViewPermission);
			return hasViewPermission;
		}

		// Mark this node as being visited to detect cycles
		visiting.add(engineId);

		try {
			// If this has children, check all subdependencies
			boolean allChildrenViewable = true;
			for (String childId : children) {
				if (!calculateCanViewRecursive(childId, dependencyMap, childrenMap, canViewCache, visiting)) {
					allChildrenViewable = false;
					break;
				}
			}

			// User can view dependencies if they have permission for this node AND all
			// children are viewable
			boolean result = hasViewPermission && allChildrenViewable;
			canViewCache.put(engineId, result);
			return result;
		} finally {
			// Remove from visiting set after processing
			visiting.remove(engineId);
		}
	}

	/**
	 * Helper method to recursively fetch project dependency details with user
	 * permissions
	 * 
	 * @param projectId
	 * @param user
	 * @param visited         Set to track visited projects to avoid circular
	 *                        dependencies
	 * @param allDependencies Accumulated list of all dependencies
	 */
	private static void getProjectDependencyDetailsWithUserRecursive(String projectId, User user, Set<String> visited,
			List<Map<String, Object>> allDependencies) {
		// Avoid circular dependencies
		if (visited.contains(projectId)) {
			return;
		}
		visited.add(projectId);

		// Query direct dependencies
		List<Map<String, Object>> directDependencies = getProjectDependencyDetailsWithUserQuery(projectId, user);

		// Add direct dependencies and recurse for PROJECT type dependencies
		for (Map<String, Object> dependency : directDependencies) {
			allDependencies.add(dependency);
			String engineId = (String) dependency.get("engine_id");
			getProjectDependencyDetailsWithUserRecursive(engineId, user, visited, allDependencies);
		}
	}

	/**
	 * Query to get project dependency details with user and group permissions for a
	 * single project
	 * 
	 * @param projectId
	 * @param user
	 * @return
	 */
	private static List<Map<String, Object>> getProjectDependencyDetailsWithUserQuery(String projectId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = user.getPrimaryLoginToken().getId();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__PROJECTID", "parent_id"));
		qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector("PROJECTDEPENDENCIES__ENGINETYPE", "engine_type"));

		// Use conditional selectors based on engine type
		QueryIfSelector engineNameSelector = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
				new QueryColumnSelector("PROJECT__PROJECTNAME"), new QueryColumnSelector("ENGINE__ENGINENAME"),
				"engine_name");
		qs.addSelector(engineNameSelector);

		QueryIfSelector engineSubtypeSelector = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
				new QueryColumnSelector("PROJECT__TYPE"), new QueryColumnSelector("ENGINE__ENGINESUBTYPE"),
				"engine_subtype");
		qs.addSelector(engineSubtypeSelector);

		QueryIfSelector engineDateCreatedSelector = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
				new QueryColumnSelector("PROJECT__DATECREATED"), new QueryColumnSelector("ENGINE__DATECREATED"),
				"engine_date_created");
		qs.addSelector(engineDateCreatedSelector);

		QueryIfSelector engineDiscoverableSelector = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
				new QueryColumnSelector("PROJECT__DISCOVERABLE"), new QueryColumnSelector("ENGINE__DISCOVERABLE"),
				"engine_discoverable");
		qs.addSelector(engineDiscoverableSelector);

		QueryIfSelector engineGlobalSelector = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
				new QueryColumnSelector("PROJECT__GLOBAL"), new QueryColumnSelector("ENGINE__GLOBAL"), "engine_global");
		qs.addSelector(engineGlobalSelector);

		// Add joins with proper join conditions
		qs.addRelation("PROJECTDEPENDENCIES__ENGINEID", "ENGINE__ENGINEID", "left.outer.join");
		qs.addRelation("PROJECTDEPENDENCIES__ENGINEID", "PROJECT__PROJECTID", "left.outer.join");

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__PROJECTID", "==", projectId));

		// METADATA sub-query - conditional based on type (ENGINEMETA vs PROJECTMETA)
		{
			// ENGINE metadata sub-query for description
			SelectQueryStruct engineMetaQs = new SelectQueryStruct();
			engineMetaQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID", "ENGINEID"));
			engineMetaQs.addSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE", "DESCRIPTION"));
			engineMetaQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", "description"));
			engineMetaQs.addGroupBy(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
			engineMetaQs.addGroupBy(new QueryColumnSelector("ENGINEMETA__METAVALUE"));

			SubqueryRelationship engineMetaRel = new SubqueryRelationship(engineMetaQs, "EM", "left.outer.join",
					new String[] { "EM__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs.addRelation(engineMetaRel);

			// PROJECT metadata sub-query for description
			SelectQueryStruct projectMetaQs = new SelectQueryStruct();
			projectMetaQs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID", "PROJECTID"));
			projectMetaQs.addSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE", "DESCRIPTION"));
			projectMetaQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", "description"));
			projectMetaQs.addGroupBy(new QueryColumnSelector("PROJECTMETA__PROJECTID"));
			projectMetaQs.addGroupBy(new QueryColumnSelector("PROJECTMETA__METAVALUE"));

			SubqueryRelationship projectMetaRel = new SubqueryRelationship(projectMetaQs, "PM", "left.outer.join",
					new String[] { "PM__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs.addRelation(projectMetaRel);

			// Use conditional selector for description
			QueryIfSelector descriptionSelector = QueryIfSelector.makeQueryIfSelector(
					SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
					new QueryColumnSelector("PM__DESCRIPTION"), new QueryColumnSelector("EM__DESCRIPTION"),
					"description");
			qs.addSelector(descriptionSelector);

			// ENGINE metadata sub-query for tags - aggregated since there can be multiple
			SelectQueryStruct engineTagsQs = new SelectQueryStruct();
			engineTagsQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID", "ENGINEID"));
			QueryFunctionSelector engineTagsAggregator = new QueryFunctionSelector();
			engineTagsAggregator.setFunction(QueryFunctionHelper.GROUP_CONCAT);
			engineTagsAggregator.addInnerSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
			engineTagsAggregator.setAlias("TAGS");
			engineTagsQs.addSelector(engineTagsAggregator);
			engineTagsQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", "tag"));
			engineTagsQs.addGroupBy(new QueryColumnSelector("ENGINEMETA__ENGINEID"));

			SubqueryRelationship engineTagsRel = new SubqueryRelationship(engineTagsQs, "ET", "left.outer.join",
					new String[] { "ET__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs.addRelation(engineTagsRel);

			// PROJECT metadata sub-query for tags - aggregated since there can be multiple
			SelectQueryStruct projectTagsQs = new SelectQueryStruct();
			projectTagsQs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID", "PROJECTID"));
			QueryFunctionSelector projectTagsAggregator = new QueryFunctionSelector();
			projectTagsAggregator.setFunction(QueryFunctionHelper.GROUP_CONCAT);
			projectTagsAggregator.addInnerSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE"));
			projectTagsAggregator.setAlias("TAGS");
			projectTagsQs.addSelector(projectTagsAggregator);
			projectTagsQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", "tag"));
			projectTagsQs.addGroupBy(new QueryColumnSelector("PROJECTMETA__PROJECTID"));

			SubqueryRelationship projectTagsRel = new SubqueryRelationship(projectTagsQs, "PT", "left.outer.join",
					new String[] { "PT__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs.addRelation(projectTagsRel);

			// Use conditional selector for tags
			QueryIfSelector tagsSelector = QueryIfSelector.makeQueryIfSelector(
					SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
					new QueryColumnSelector("PT__TAGS"), new QueryColumnSelector("ET__TAGS"), "tags");
			qs.addSelector(tagsSelector);
		}

		// PERMISSION sub-query - conditional based on type (ENGINEPERMISSION vs
		// PROJECTPERMISSION)
		{
			// ENGINE permissions sub-query
			SelectQueryStruct enginePermQs = new SelectQueryStruct();
			enginePermQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID", "ENGINEID"));
			enginePermQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION", "PERMISSION"));
			enginePermQs.addRelation("ENGINEPERMISSION__PERMISSION", "PERMISSION__ID", "inner.join");
			enginePermQs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "PERMISSION_NAME"));
			enginePermQs
					.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userId));
			enginePermQs.addGroupBy(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
			enginePermQs.addGroupBy(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
			enginePermQs.addGroupBy(new QueryColumnSelector("PERMISSION__NAME"));

			SubqueryRelationship enginePermRel = new SubqueryRelationship(enginePermQs, "EP", "left.outer.join",
					new String[] { "EP__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs.addRelation(enginePermRel);

			// PROJECT permissions sub-query
			SelectQueryStruct projectPermQs = new SelectQueryStruct();
			projectPermQs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID", "PROJECTID"));
			projectPermQs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION", "PERMISSION"));
			projectPermQs.addRelation("PROJECTPERMISSION__PERMISSION", "PERMISSION__ID", "inner.join");
			projectPermQs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "PERMISSION_NAME"));
			projectPermQs
					.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));
			projectPermQs.addGroupBy(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
			projectPermQs.addGroupBy(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
			projectPermQs.addGroupBy(new QueryColumnSelector("PERMISSION__NAME"));

			SubqueryRelationship projectPermRel = new SubqueryRelationship(projectPermQs, "PP", "left.outer.join",
					new String[] { "PP__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs.addRelation(projectPermRel);

			// Use conditional selectors for permissions
			QueryIfSelector permissionSelector = QueryIfSelector.makeQueryIfSelector(
					SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
					new QueryColumnSelector("PP__PERMISSION"), new QueryColumnSelector("EP__PERMISSION"), "permission");
			qs.addSelector(permissionSelector);

			QueryIfSelector permissionNameSelector = QueryIfSelector.makeQueryIfSelector(
					SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
					new QueryColumnSelector("PP__PERMISSION_NAME"), new QueryColumnSelector("EP__PERMISSION_NAME"),
					"permission_name");
			qs.addSelector(permissionNameSelector);
		}

		// GROUP PERMISSION sub-query - conditional based on type (GROUPENGINEPERMISSION
		// vs
		// GROUPPROJECTPERMISSION)
		{
			// Build the group filter once for reuse
			OrQueryFilter groupOrFilters = new OrQueryFilter();
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider login : logins) {
					AccessToken accessToken = user.getAccessToken(login);
					Collection<String> userGroups = accessToken.getUserGroups();
					String userGroupType = accessToken.getUserGroupType();
					Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
					if (!userCustomGroups.isEmpty()) {
						AndQueryFilter customAndFilter = new AndQueryFilter();
						customAndFilter.addFilter(
								SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", "CUSTOM"));
						customAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID",
								"==", userCustomGroups));
						groupOrFilters.addFilter(customAndFilter);
					}
					if (!userGroups.isEmpty()) {
						AndQueryFilter andFilter = new AndQueryFilter();
						andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==",
								userGroupType));
						andFilter.addFilter(
								SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", userGroups));
						groupOrFilters.addFilter(andFilter);
					}
				}
			}

			// ENGINE group permissions sub-query
			SelectQueryStruct engineGroupPermQs = new SelectQueryStruct();
			engineGroupPermQs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ENGINEID", "ENGINEID"));
			engineGroupPermQs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					"GROUPENGINEPERMISSION__PERMISSION", "PERMISSION"));
			engineGroupPermQs.addGroupBy(new QueryColumnSelector("GROUPENGINEPERMISSION__ENGINEID"));
			if (!groupOrFilters.isEmpty()) {
				engineGroupPermQs.addExplicitFilter(groupOrFilters);
			} else {
				AndQueryFilter nullFilter = new AndQueryFilter();
				nullFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", null));
				nullFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", null));
				engineGroupPermQs.addExplicitFilter(nullFilter);
			}

			SubqueryRelationship engineGroupPermRel = new SubqueryRelationship(engineGroupPermQs, "EGP",
					"left.outer.join", new String[] { "EGP__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs.addRelation(engineGroupPermRel);

			// PROJECT group permissions sub-query
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider login : logins) {
					AccessToken accessToken = user.getAccessToken(login);
					Collection<String> userGroups = accessToken.getUserGroups();
					String userGroupType = accessToken.getUserGroupType();
					Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
					if (!userCustomGroups.isEmpty()) {
						AndQueryFilter customAndFilter = new AndQueryFilter();
						customAndFilter.addFilter(
								SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", "CUSTOM"));
						customAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID",
								"==", userCustomGroups));
						groupProjectOrFilters.addFilter(customAndFilter);
					}
					if (!userGroups.isEmpty()) {
						AndQueryFilter andFilter = new AndQueryFilter();
						andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==",
								userGroupType));
						andFilter.addFilter(
								SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", userGroups));
						groupProjectOrFilters.addFilter(andFilter);
					}
				}
			}

			SelectQueryStruct projectGroupPermQs = new SelectQueryStruct();
			projectGroupPermQs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PROJECTID", "PROJECTID"));
			projectGroupPermQs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					"GROUPPROJECTPERMISSION__PERMISSION", "PERMISSION"));
			projectGroupPermQs.addGroupBy(new QueryColumnSelector("GROUPPROJECTPERMISSION__PROJECTID"));
			if (!groupProjectOrFilters.isEmpty()) {
				projectGroupPermQs.addExplicitFilter(groupProjectOrFilters);
			} else {
				AndQueryFilter nullFilter = new AndQueryFilter();
				nullFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", null));
				nullFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", null));
				projectGroupPermQs.addExplicitFilter(nullFilter);
			}

			SubqueryRelationship projectGroupPermRel = new SubqueryRelationship(projectGroupPermQs, "PGP",
					"left.outer.join", new String[] { "PGP__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs.addRelation(projectGroupPermRel);

			// Use conditional selector for group permission based on engine type
			QueryIfSelector groupPermissionSelector = QueryIfSelector.makeQueryIfSelector(
					SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
					new QueryColumnSelector("PGP__PERMISSION"), new QueryColumnSelector("EGP__PERMISSION"),
					"group_permission");
			qs.addSelector(groupPermissionSelector);
		}

		// ACCESS REQUEST sub-query - conditional based on type (ENGINEACCESSREQUEST vs
		// PROJECTACCESSREQUEST)
		{
			// ENGINE access requests sub-query
			SelectQueryStruct engineAccReqQs = new SelectQueryStruct();
			engineAccReqQs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__PERMISSION", "PERMISSION"));
			engineAccReqQs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__ENGINEID", "ENGINEID"));
			engineAccReqQs.addRelation("ENGINEACCESSREQUEST__ENGINEID", "ENGINE__ENGINEID", "inner.join");
			engineAccReqQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("ENGINEACCESSREQUEST__REQUEST_USERID", "==", userId));
			engineAccReqQs.addExplicitFilter(SimpleQueryFilter
					.makeColToValFilter("ENGINEACCESSREQUEST__APPROVER_DECISION", "==", "NEW_REQUEST"));

			SubqueryRelationship engineAReqRel = new SubqueryRelationship(engineAccReqQs, "EAR", "left.outer.join",
					new String[] { "EAR__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs.addRelation(engineAReqRel);

			// PROJECT access requests sub-query
			SelectQueryStruct projectAccReqQs = new SelectQueryStruct();
			projectAccReqQs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__PERMISSION", "PERMISSION"));
			projectAccReqQs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__PROJECTID", "PROJECTID"));
			projectAccReqQs.addRelation("PROJECTACCESSREQUEST__PROJECTID", "PROJECT__PROJECTID", "inner.join");
			projectAccReqQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__REQUEST_USERID", "==", userId));
			projectAccReqQs.addExplicitFilter(SimpleQueryFilter
					.makeColToValFilter("PROJECTACCESSREQUEST__APPROVER_DECISION", "==", "NEW_REQUEST"));

			SubqueryRelationship projectAReqRel = new SubqueryRelationship(projectAccReqQs, "PAR", "left.outer.join",
					new String[] { "PAR__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs.addRelation(projectAReqRel);

			// Use conditional selector for access permission
			QueryIfSelector accessPermissionSelector = QueryIfSelector.makeQueryIfSelector(
					SimpleQueryFilter.makeColToValFilter("PROJECTDEPENDENCIES__ENGINETYPE", "==", "PROJECT"),
					new QueryColumnSelector("PAR__PERMISSION"), new QueryColumnSelector("EAR__PERMISSION"),
					"access_permission");
			qs.addSelector(accessPermissionSelector);
		}

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * 
	 * @param requester
	 * @param projectId
	 * @param newUserId
	 * @param newUserType
	 * @param requestedPermission
	 * @param endDate
	 * @param usageRestriction
	 * @param usageFrequency
	 * @param maxTokens
	 * @param maxResponseTime
	 * @return
	 */
	public static Map<String, Object> propagateProjectPermission(User requester, String projectId, String newUserId,
			String newUserType, String requestedPermission, String endDate, String usageRestriction,
			String usageFrequency, int maxTokens, double maxResponseTime) {
		Map<String, Object> ret = new HashMap<String, Object>();

		// get the requested permission as a numeric -- it was passed as a string
		Integer requestedPermissionNumeric = AccessPermissionEnum.getIdByPermission(requestedPermission);

		List<String> alreadyHaveAccess = new ArrayList<>();
		List<String> requestAlreadyExists = new ArrayList<>();
		List<String> newRequestAdded = new ArrayList<>();
		List<String> accessGranted = new ArrayList<>();
		List<String> couldNotAddRequest = new ArrayList<>();

		// loop through the dependencies and process request according to the
		// requestor's permissions on each engine.
		List<Map<String, Object>> dependentEngines = SecurityProjectUtils.getProjectDependencies(projectId);
		for (int i = 0; i < dependentEngines.size(); i++) {
			Map<String, Object> dependentEngine = dependentEngines.get(i);
			String dependentEngineId = (String) dependentEngine.get("engine_id");
			String dependentEngineType = (String) dependentEngine.get("engine_type");

			Integer currentPendingUserPermission;
			Integer requesterEnginePermission;
			Integer currentNewUserPermission;
			boolean isEngine = false;

			if (dependentEngineType == null
					|| IEngine.CATALOG_TYPE.valueOf(dependentEngineType) != IEngine.CATALOG_TYPE.PROJECT) {
				isEngine = true;
				currentPendingUserPermission = SecurityEngineUtils.getUserAccessRequestEnginePermission(newUserId,
						dependentEngineId);
				requesterEnginePermission = SecurityEngineUtils
						.getUserEnginePermission(User.getSingleLogginName(requester), dependentEngineId);
				currentNewUserPermission = SecurityEngineUtils.getUserEnginePermission(newUserId, dependentEngineId);
			} else {
				currentPendingUserPermission = SecurityProjectUtils.getUserAccessRequestProjectPermission(newUserId,
						dependentEngineId);
				requesterEnginePermission = SecurityProjectUtils
						.getUserProjectPermission(User.getSingleLogginName(requester), dependentEngineId);
				currentNewUserPermission = SecurityProjectUtils.getUserProjectPermission(newUserId, dependentEngineId);
			}

			// if newUser is requesting permission which he/she already has access, take no
			// action
			if (currentNewUserPermission == requestedPermissionNumeric) {
				alreadyHaveAccess.add(dependentEngineId);
				classLogger.info("User already has {} access to {}", requestedPermission, dependentEngineId);
				// if newUser has already requested this access and it is still pending, take no
				// action
			} else if (currentPendingUserPermission != null
					&& requestedPermissionNumeric == currentPendingUserPermission) {
				requestAlreadyExists.add(dependentEngineId);
				classLogger.info("user has already requested {} access to {} and the request is pending.",
						requestedPermission, dependentEngineId);
				// if requester has insufficient privileges on the engine so forward request to
				// engine owner
			} else if (requesterEnginePermission == null || requesterEnginePermission == 3) {
				try {
					if (isEngine) {
						SecurityEngineUtils.setUserAccessRequest(newUserId, newUserType, dependentEngineId,
								"No Comment at this time", requestedPermissionNumeric, requester);
					} else {
						SecurityProjectUtils.setUserAccessRequest(newUserId, newUserType, dependentEngineId,
								"No Comment at this time", requestedPermissionNumeric, requester);
					}
					newRequestAdded.add(dependentEngineId);
					classLogger.info("User has forwarded {}'s request to the owner of engine {}", newUserId,
							dependentEngineId);
				} catch (Exception e) {
					couldNotAddRequest.add(dependentEngineId);
					classLogger.error("Failed to propagate project permissions to related artifacts", e);
				}
				// if the newUser has permissions on the engine but not to the level requested,
				// edit the existing record
			} else if (requesterEnginePermission < 3 && currentNewUserPermission != null
					&& currentNewUserPermission > requestedPermissionNumeric) {
				try {
					if (isEngine) {
						SecurityEngineUtils.editEngineUserPermission(requester, newUserId, newUserType,
								dependentEngineId, requestedPermission, endDate, usageRestriction, usageFrequency,
								maxTokens, maxResponseTime);
					} else {
						SecurityProjectUtils.editProjectUserPermission(requester, newUserId, newUserType,
								dependentEngineId, requestedPermission, endDate);
					}

					accessGranted.add(dependentEngineId);
					classLogger.info("User has updated permission for {} to {}", newUserId, dependentEngineId);
				} catch (IllegalAccessException e) {
					couldNotAddRequest.add(dependentEngineId);
					classLogger.error("Failed to propagate project permissions to related artifacts", e);
				}
				// if none of the above and requestor has proper permission, add user to the
				// engine permission database
			} else if (requesterEnginePermission < 3 && currentNewUserPermission == null) {
				try {
					if (isEngine) {
						SecurityEngineUtils.addEngineUser(requester, newUserId, dependentEngineId, requestedPermission,
								endDate, usageRestriction, usageFrequency, maxTokens, maxResponseTime);
					} else {
						SecurityProjectUtils.addProjectUser(requester, newUserId, dependentEngineId,
								requestedPermission, endDate);
					}

					accessGranted.add(dependentEngineId);
					classLogger.info("User has added {} to {}", newUserId, dependentEngineId);
				} catch (IllegalAccessException | IllegalArgumentException e) {
					couldNotAddRequest.add(dependentEngineId);
					classLogger.error("Failed to propagate project permissions to related artifacts", e);
				}
			} else {
				couldNotAddRequest.add(dependentEngineId);
				classLogger.info("User could not add or forward {}'s request for engine {}", newUserId,
						dependentEngineId);
			}
		}

		ret.put("Successfully processed permission propagation", true);
		ret.put("alreadyHaveAccess", alreadyHaveAccess);
		ret.put("requestAlreadyExists", requestAlreadyExists);
		ret.put("newRequestAdded", newRequestAdded);
		ret.put("accessGranted", accessGranted);
		ret.put("couldNotAddRequest", couldNotAddRequest);
		return ret;
	}

	/*
	 * Project Metadata
	 */

	/**
	 * Update the project metadata Will delete existing values and then perform a
	 * bulk insert
	 * 
	 * @param projectId
	 * @param metadata
	 */
	public static void updateProjectMetadata(String projectId, Map<String, Object> metadata) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// first do a delete
		String deleteQ = "DELETE FROM PROJECTMETA WHERE METAKEY=? AND PROJECTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for (String field : metadata.keySet()) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, field);
				deletePs.setString(parameterIndex++, projectId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project metadata", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}

		// now we do the new insert with the order of the tags
		String query = securityDb.getQueryUtil().createInsertPreparedStatementString("PROJECTMETA",
				new String[] { "PROJECTID", "METAKEY", "METAVALUE", "METAORDER" });
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for (String field : metadata.keySet()) {
				Object val = metadata.get(field);
				List<Object> values = new ArrayList<>();
				if (val instanceof List) {
					values = (List<Object>) val;
				} else if (val instanceof Collection) {
					values.addAll((Collection<Object>) val);
				} else {
					values.add(val);
				}

				for (int i = 0; i < values.size(); i++) {
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
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project metadata", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Get the wrapper for additional project metadata
	 * 
	 * @param projectId
	 * @param metaKeys
	 * @param ignoreMarkdown
	 * @return
	 * @throws Exception
	 */
	public static IRawSelectWrapper getProjectMetadataWrapper(Collection<String> projectId, List<String> metaKeys,
			boolean ignoreMarkdown) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAORDER"));
		// filters
		if (projectId != null && !projectId.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__PROJECTID", "==", projectId));
		}
		if (metaKeys != null && !metaKeys.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", metaKeys));
		}
		// exclude markdown metadata due to potential large data size
		if (ignoreMarkdown) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "!=", Constants.MARKDOWN));
		}
		// order
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAORDER"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}

	/**
	 * Get the metadata for a specific project
	 * 
	 * @param projectId
	 * @return
	 */
	public static Map<String, Object> getAggregateProjectMetadata(String projectId, List<String> metaKeys,
			boolean ignoreMarkdown) {
		Map<String, Object> retMap = new HashMap<String, Object>();

		List<String> projectIds = new ArrayList<>();
		projectIds.add(projectId);

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = getProjectMetadataWrapper(projectIds, metaKeys, ignoreMarkdown);
			while (wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String metaKey = (String) data[1];
				String metaValue = (String) data[2];

				// always send as array
				// if multi, send as array
				if (retMap.containsKey(metaKey)) {
					Object obj = retMap.get(metaKey);
					if (obj instanceof List) {
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
			classLogger.error("Failed to aggregate project metadata values", e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error("Failed to aggregate project metadata values", e);
				}
			}
		}

		return retMap;
	}

	/**
	 * Check if the user has access to the project
	 * 
	 * @param projectId
	 * @param userId
	 * @return
	 * @throws Exception
	 */
	public static boolean checkUserHasAccessToProject(String projectId, String userId) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			boolean isExpired = SecurityUserProjectUtils.projectPermissionIsExpired(userId, projectId);
			if (isExpired) {
				removeExpiredProjectUser(userId, projectId);
			}
		} catch (Exception e) {
			classLogger.error("Failed to verify user access to the project", e);
			throw e;
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Failed to verify user access to the project", e);
			throw e;
		}
	}

	/*
	 * Copying permissions
	 */

	/**
	 * Copy the project permissions from one project to another
	 * 
	 * @param sourceProjectId
	 * @param targetProjectId
	 * @throws SQLException
	 */
	public static void copyProjectPermissions(String sourceProjectId, String targetProjectId) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String insertTargetProjectPermissionSql = "INSERT INTO PROJECTPERMISSION (PROJECTID, USERID, PERMISSION, VISIBILITY) VALUES (?, ?, ?, ?)";
		PreparedStatement insertTargetProjectPermissionStatement = securityDb
				.getPreparedStatement(insertTargetProjectPermissionSql);

		// grab the permissions, filtered on the source engine id
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__VISIBILITY"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", sourceProjectId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				// now loop through all the permissions
				// but with the target engine id instead of the source engine id
				insertTargetProjectPermissionStatement.setString(1, targetProjectId);
				insertTargetProjectPermissionStatement.setString(2, (String) row[1]);
				insertTargetProjectPermissionStatement.setInt(3, ((Number) row[2]).intValue());
				insertTargetProjectPermissionStatement.setBoolean(4, (Boolean) row[3]);
				// add to batch
				insertTargetProjectPermissionStatement.addBatch();
			}
		} catch (Exception e) {
			classLogger.error("Failed to copy project permissions to the target project", e);
			throw e;
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
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			if (!insertTargetProjectPermissionStatement.getConnection().getAutoCommit()) {
				insertTargetProjectPermissionStatement.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to copy project permissions to the target project", e);
			throw new IllegalArgumentException("An error occurred transferring the project permissions");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertTargetProjectPermissionStatement);
		}
	}

	/**
	 * Returns List of users that have no access credentials to a given App.
	 * 
	 * @param user
	 * @param projectId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getProjectUsersNoCredentials(User user, String projectId, String searchTerm,
			long limit, long offset) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		/*
		 * Security check to make sure that the user can view the application provided.
		 */
		if (!userCanViewProject(user, projectId)) {
			throw new IllegalAccessException("The user does not have access to view this project");
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE", "type"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME", "username"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		// Filter for sub-query
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("SMSS_USER__ID", "!=", subQs));
			// Sub-query itself
			subQs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
			subQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "!=", null,
					PixelDataType.NULL_VALUE));
		}
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__NAME"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__EMAIL"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get the list of the engine information that the user has access to
	 * 
	 * @param user
	 * @param projectIdFilters
	 * @param favoritesOnly
	 * @param projectMetadataFilter
	 * @param permissionFilters
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> getUserProjectList(User user, List<String> projectTypes,
			List<String> projectIdFilters, boolean favoritesOnly, Map<String, Object> projectMetadataFilter,
			List<Integer> permissionFilters, String searchTerm, String limit, String offset) {
		return getUserProjectList(user, projectTypes, projectIdFilters, favoritesOnly, projectMetadataFilter,
				permissionFilters, searchTerm, limit, offset, null);
	}

	public static List<Map<String, Object>> getUserProjectList(User user, List<String> projectTypes,
			List<String> projectIdFilters, boolean favoritesOnly, Map<String, Object> projectMetadataFilter,
			List<Integer> permissionFilters, String searchTerm, String limit, String offset,
			Map<String, String> sortFields) {
		return getUserProjectList(user, projectTypes, projectIdFilters, favoritesOnly, projectMetadataFilter,
				permissionFilters, searchTerm, limit, offset, sortFields, false);
	}

	public static List<Map<String, Object>> getUserProjectList(User user, List<String> projectTypes,
			List<String> projectIdFilters, boolean favoritesOnly, Map<String, Object> projectMetadataFilter,
			List<Integer> permissionFilters, String searchTerm, String limit, String offset,
			Map<String, String> sortFields, boolean onlyTemplates) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		Collection<String> userIds = getUserFiltersQs(user);
		String groupProjectPermission = "GROUPPROJECTPERMISSION__";
		String projectPrefix = "PROJECT__";

		SelectQueryStruct qs1 = new SelectQueryStruct();
		// selectors
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTID", "project_id"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTNAME", "project_name"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTDISPLAYNAME", "project_display_name"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "TYPE", "project_type"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "COST", "project_cost"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "GLOBAL", "project_global"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "DISCOVERABLE", "project_discoverable"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "IS_TEMPLATE", "project_is_template"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "CATALOGNAME", "project_catalog_name"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "CREATEDBY", "project_created_by"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "CREATEDBYTYPE", "project_created_by_type"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "DATECREATED", "project_date_created"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "DATELASTEDITED", "project_date_last_edited"));

		// dont forget reactors/portal information
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHED", "project_portal_published_date"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHEDUSER", "project_published_user"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHEDTYPE", "project_published_user_type"));
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "REACTORSCOMPILED", "project_reactors_compiled_date"));
		qs1.addSelector(
				new QueryColumnSelector(projectPrefix + "REACTORSCOMPILEDUSER", "project_reactors_compiled_user"));
		qs1.addSelector(
				new QueryColumnSelector(projectPrefix + "REACTORSCOMPILEDTYPE", "project_reactors_compiled_user_type"));
		// back to the others
		qs1.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "PROJECT__PROJECTNAME",
				"low_project_name"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__FAVORITE", "project_favorite"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__PERMISSION", "user_permission"));
		qs1.addSelector(new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION", "group_permission"));

		// description from project metadata
		qs1.addSelector(new QueryColumnSelector("PROJECT_DESCRIPTION__DESCRIPTION", "project_description"));

		// this block is for max permissions
		// If both null - return null
		// if either not null - return the permission value that is not null
		// if both not null - return the max permissions (I.E lowest number)
		{
			AndQueryFilter and = new AndQueryFilter();
			and.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));
			and.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));

			AndQueryFilter and1 = new AndQueryFilter();
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "!=", null,
					PixelDataType.CONST_INT));
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));

			AndQueryFilter and2 = new AndQueryFilter();
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "!=", null,
					PixelDataType.CONST_INT));

			SimpleQueryFilter maxPermFilter = SimpleQueryFilter.makeColToColFilter("USER_PERMISSIONS__PERMISSION", "<",
					"GROUP_PERMISSIONS__PERMISSION");

			QueryIfSelector qis3 = QueryIfSelector.makeQueryIfSelector(maxPermFilter,
					new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"),
					new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION"), "permission");

			QueryIfSelector qis2 = QueryIfSelector.makeQueryIfSelector(and2,
					new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"), qis3, "permission");

			QueryIfSelector qis1 = QueryIfSelector.makeQueryIfSelector(and1,
					new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION"), qis2, "permission");

			QueryIfSelector qis = QueryIfSelector.makeQueryIfSelector(and,
					new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"), qis1, "permission");

			qs1.addSelector(qis);
		}

		// add a join to get the user permission level, if favorite, and the visibility
		{
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID", "PROJECTID"));

			QueryFunctionSelector castFavorite = QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.CAST,
					"PROJECTPERMISSION__FAVORITE", "castFavorite");
			castFavorite.setDataType(securityDb.getQueryUtil().getIntegerDataTypeName());
			qs2.addSelector(
					QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, castFavorite, "FAVORITE"));

			QueryFunctionSelector castVisibility = QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.CAST,
					"PROJECTPERMISSION__VISIBILITY", "castVisibility");
			castVisibility.setDataType(securityDb.getQueryUtil().getIntegerDataTypeName());
			qs2.addSelector(
					QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, castVisibility, "VISIBILITY"));

			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					"PROJECTPERMISSION__PERMISSION", "PERMISSION"));
			qs2.addGroupBy(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID", "PROJECTID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
			IRelation subQuery = new SubqueryRelationship(qs2, "USER_PERMISSIONS", "left.outer.join",
					new String[] { "USER_PERMISSIONS__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs1.addRelation(subQuery);
		}

		// add a join to get the group permission level
		{
			SelectQueryStruct qs3 = new SelectQueryStruct();
			qs3.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));
			qs3.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					groupProjectPermission + "PERMISSION", "PERMISSION"));
			qs3.addGroupBy(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));

			// filter on groups
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider login : logins) {
				AccessToken accessToken = user.getAccessToken(login);
				Collection<String> userGroups = accessToken.getUserGroups();
				String userGroupType = accessToken.getUserGroupType();
				Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
				if (!userCustomGroups.isEmpty()) {
					AndQueryFilter customAndFilter = new AndQueryFilter();
					customAndFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", "CUSTOM"));
					customAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==",
							userCustomGroups));
					groupProjectOrFilters.addFilter(customAndFilter);
				}
				if (!userGroups.isEmpty()) {
					AndQueryFilter andFilter = new AndQueryFilter();
					andFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", userGroupType));
					andFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", userGroups));
					groupProjectOrFilters.addFilter(andFilter);
				}
			}

			if (!groupProjectOrFilters.isEmpty()) {
				qs3.addExplicitFilter(groupProjectOrFilters);
			} else {
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", null));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", null));
				qs3.addExplicitFilter(andFilter1);
			}

			IRelation subQuery = new SubqueryRelationship(qs3, "GROUP_PERMISSIONS", "left.outer.join",
					new String[] { "GROUP_PERMISSIONS__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs1.addRelation(subQuery);
		}

		// add a join to get the project description from metadata
		{
			SelectQueryStruct descQs = new SelectQueryStruct();
			descQs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID", "PROJECTID"));
			descQs.addSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE", "DESCRIPTION"));
			descQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", "description"));
			IRelation descSubQuery = new SubqueryRelationship(descQs, "PROJECT_DESCRIPTION", "left.outer.join",
					new String[] { "PROJECT_DESCRIPTION__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs1.addRelation(descSubQuery);
		}

		// filters
		OrQueryFilter orFilter = new OrQueryFilter();
		{
			orFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter(projectPrefix + "GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "!=", null,
					PixelDataType.CONST_INT));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "!=", null,
					PixelDataType.CONST_INT));
			qs1.addExplicitFilter(orFilter);
		}

		if (projectTypes != null && !projectTypes.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix + "TYPE", "==", projectTypes));
		}

		if (onlyTemplates) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix + "IS_TEMPLATE", "==", true,
					PixelDataType.BOOLEAN));
		}

		if (projectIdFilters != null && !projectIdFilters.isEmpty()) {
			qs1.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(projectPrefix + "PROJECTID", "==", projectIdFilters));
		}

		// filter based on permission filters
		if (permissionFilters != null && !permissionFilters.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "==",
					permissionFilters, PixelDataType.CONST_INT));
		}

		// only show those that are visible
		// remember, user permissions cast this to int
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__VISIBILITY", "==",
				Arrays.asList(new Object[] { 1, null }), PixelDataType.CONST_INT));
		// favorites only
		// remember, user permissions cast this to int
		if (favoritesOnly) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__FAVORITE", "==", 1,
					PixelDataType.CONST_INT));
		}

		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTID", searchTerm));
			searchFilter.addFilter(
					securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTNAME", searchTerm));
			searchFilter.addFilter(
					securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTDISPLAYNAME", searchTerm));
			qs1.addExplicitFilter(searchFilter);
		}

		// filtering by projectmeta key-value pairs (i.e. <tag>:value): for each pair,
		// add in-filter against projectids from subquery
		if (projectMetadataFilter != null && !projectMetadataFilter.isEmpty()) {
			for (String k : projectMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAVALUE", "==",
						projectMetadataFilter.get(k)));
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "==", subQs));
			}
		}

		{
			// first lets make sure we have any groups
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider login : logins) {
				AccessToken accessToken = user.getAccessToken(login);
				Collection<String> userGroups = accessToken.getUserGroups();
				String userGroupType = accessToken.getUserGroupType();
				Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
				if (userGroups.isEmpty() && userCustomGroups.isEmpty()) {
					continue;
				}
				if (!userCustomGroups.isEmpty()) {
					AndQueryFilter customAndFilter = new AndQueryFilter();
					customAndFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", "CUSTOM"));
					customAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==",
							userCustomGroups));
					groupProjectOrFilters.addFilter(customAndFilter);
				}
				if (!userGroups.isEmpty()) {
					AndQueryFilter andFilter = new AndQueryFilter();
					andFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", userGroupType));
					andFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", userGroups));
					groupProjectOrFilters.addFilter(andFilter);
				}
			}
			// 4.a does the group have explicit access
			if (!groupProjectOrFilters.isEmpty()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", "==", subQs));

				// we need to have the insight filters
				subQs.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID"));
				subQs.addExplicitFilter(groupProjectOrFilters);
			}
		}

		// add the sort
		if (sortFields == null || sortFields.isEmpty()) {
			qs1.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
		} else {
			for (Map.Entry<String, String> sortEntry : sortFields.entrySet()) {
				String sortKey = sortEntry.getKey();
				String sortDirection = sortEntry.getValue();
				if (sortKey == null || sortDirection == null) {
					throw new SemossPixelException("Sort parameters cannot contain null keys or values");
				}
				String normalizedKey = sortKey.replace("_", "").toUpperCase();
				if ("PROJECTNAME".equals(normalizedKey)) {
					qs1.addOrderBy("low_project_name", sortDirection);
				} else if ("DATECREATED".equals(normalizedKey)) {
					qs1.addOrderBy(projectPrefix + "DATECREATED", sortDirection);
				} else if ("DATELASTEDITED".equals(normalizedKey)) {
					qs1.addOrderBy(projectPrefix + "DATELASTEDITED", sortDirection);
				} else {
					throw new SemossPixelException(
							"Invalid Sort Parameters passed: Only \"PROJECTNAME\", \"DATECREATED\", and \"DATELASTEDITED\" are supported");
				}
			}
		}

		Long long_limit = -1L;
		Long long_offset = -1L;
		if (limit != null && !limit.trim().isEmpty()) {
			long_limit = ((Number) Double.parseDouble(limit)).longValue();
		}
		if (offset != null && !offset.trim().isEmpty()) {
			long_offset = ((Number) Double.parseDouble(offset)).longValue();
		}
		qs1.setLimit(long_limit);
		qs1.setOffSet(long_offset);

		return QueryExecutionUtility.flushRsToMap(securityDb, qs1);
	}

	/**
	 * Get the list of the project ids that the user has access to
	 * 
	 * @param user
	 * @param includeGlobal
	 * @param includeDiscoverable
	 * @param includeExistingAccess
	 * @return
	 */
	public static List<String> getUserProjectIdList(User user, boolean includeGlobal, boolean includeDiscoverable,
			boolean includeExistingAccess) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String projectPrefix = "PROJECT__";
		String projectPermissionPrefix = "PROJECTPERMISSION__";
		String groupProjectPermissionPrefix = "GROUPPROJECTPERMISSION__";

		Collection<String> userIds = getUserFiltersQs(user);

		SelectQueryStruct qs1 = new SelectQueryStruct();

		// selectors
		qs1.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTID", "project_id"));

		// filters
		OrQueryFilter orFilter = new OrQueryFilter();
		if (includeGlobal) {
			orFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter(projectPrefix + "GLOBAL", "==", true, PixelDataType.BOOLEAN));
		}
		if (includeDiscoverable) {
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix + "DISCOVERABLE", "==", true,
					PixelDataType.BOOLEAN));
		}
		String existingAccessComparator = "==";
		if (!includeExistingAccess) {
			existingAccessComparator = "!=";
		}
		if (!includeExistingAccess && !includeDiscoverable) {
			throw new IllegalArgumentException(
					"Fitler combinations can result in ids that the user does not have access to. Please adjust your parameters");
		}
		{
			// user access
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector(projectPermissionPrefix + "PROJECTID", "PROJECTID"));
			qs2.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(projectPermissionPrefix + "USERID", "==", userIds));
			orFilter.addFilter(
					SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", existingAccessComparator, qs2));
		}
		{
			// filter on groups
			OrQueryFilter groupEngineOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider login : logins) {
				if (user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}

				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermissionPrefix + "TYPE", "==",
						user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermissionPrefix + "ID", "==",
						user.getAccessToken(login).getUserGroups()));
				groupEngineOrFilters.addFilter(andFilter);
			}

			if (!groupEngineOrFilters.isEmpty()) {
				SelectQueryStruct qs3 = new SelectQueryStruct();
				qs3.addSelector(new QueryColumnSelector(groupProjectPermissionPrefix + "PROJECTID", "PROJECTID"));
				qs3.addExplicitFilter(groupEngineOrFilters);

				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID",
						existingAccessComparator, qs3));
			}
		}

		qs1.addExplicitFilter(orFilter);

		return QueryExecutionUtility.flushToListString(securityDb, qs1);
	}

	/**
	 * Get all user engines and engine Ids regardless of it being hidden or not
	 * 
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllUserProjectList(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		List<Map<String, Object>> allGlobalEnginesMap = QueryExecutionUtility.flushRsToMap(securityDb, qs);

		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		qs2.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");

		List<Map<String, Object>> engineMap = QueryExecutionUtility.flushRsToMap(securityDb, qs2);
		engineMap.addAll(allGlobalEnginesMap);
		return engineMap;
	}

	/**
	 * Get the list of the project information that the user has access to
	 * 
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getUserProjectList(User user, String projectFilter) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTDISPLAYNAME", "project_display_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		qs.addSelector(new QueryColumnSelector("PROJECT__GLOBAL", "project_global"));
		qs.addSelector(new QueryColumnSelector("PROJECT__DISCOVERABLE", "project_discoverable"));
		qs.addSelector(new QueryColumnSelector("PROJECT__IS_TEMPLATE", "project_is_template"));
		qs.addSelector(new QueryColumnSelector("PROJECT__CATALOGNAME", "project_catalog_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__CREATEDBY", "project_created_by"));
		qs.addSelector(new QueryColumnSelector("PROJECT__CREATEDBYTYPE", "project_created_by_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__DATECREATED", "project_date_created"));
		qs.addSelector(new QueryColumnSelector("PROJECT__DATELASTEDITED", "project_date_last_edited"));

		// dont forget reactors/portal information
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHED", "project_portal_published_date"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHEDUSER", "project_published_user"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHEDTYPE", "project_published_user_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILED", "project_reactors_compiled_date"));
		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILEDUSER", "project_reactors_compiled_user"));
		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILEDTYPE", "project_reactors_compiled_user_type"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__FAVORITE", "project_favorite"));
		// for sorting
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "PROJECT__PROJECTNAME",
				"low_project_name"));
		// back to the others
		if (projectFilter != null && !projectFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
		}
		boolean addGroupProjectPermissionJoin = false;
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));

			Collection<String> groupIds = getUserGroupFiltersQs(user);
			if (!groupIds.isEmpty()) {
				addGroupProjectPermissionJoin = true;
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", groupIds));
			}
			qs.addExplicitFilter(orFilter);
		}
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
		if (addGroupProjectPermissionJoin) {
			qs.addRelation("PROJECT", "GROUPPROJECTPERMISSION", "left.outer.join");
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Determine if a user can request a project
	 * 
	 * @param projectId
	 * @return
	 */
	public static boolean projectIsDiscoverable(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECT__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				// if you are here, you can request
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Failed to determine whether project is discoverable", e);
		}
		return false;
	}

	/**
	 * Get the list of the project information that the user has access to
	 * 
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getDiscoverableProjectList(String projectFilter,
			List<String> projectTypeFilter) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTDISPLAYNAME", "project_display_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		qs.addSelector(new QueryColumnSelector("PROJECT__GLOBAL", "project_global"));
		qs.addSelector(new QueryColumnSelector("PROJECT__DISCOVERABLE", "project_discoverable"));
		qs.addSelector(new QueryColumnSelector("PROJECT__IS_TEMPLATE", "project_is_template"));
		qs.addSelector(new QueryColumnSelector("PROJECT__CATALOGNAME", "project_catalog_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__CREATEDBY", "project_created_by"));
		qs.addSelector(new QueryColumnSelector("PROJECT__CREATEDBYTYPE", "project_created_by_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__DATECREATED", "project_date_created"));
		qs.addSelector(new QueryColumnSelector("PROJECT__DATELASTEDITED", "project_date_last_edited"));
		// dont forget reactors/portal information
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHED", "project_portal_published_date"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHEDUSER", "project_published_user"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PORTALPUBLISHEDTYPE", "project_published_user_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILED", "project_reactors_compiled_date"));
		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILEDUSER", "project_reactors_compiled_user"));
		qs.addSelector(new QueryColumnSelector("PROJECT__REACTORSCOMPILEDTYPE", "project_reactors_compiled_user_type"));
		// for storting
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "PROJECT__PROJECTNAME",
				"low_project_name"));
		if (projectFilter != null && !projectFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
		}
		if (projectTypeFilter != null && !projectTypeFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__TYPE", "==", projectTypeFilter));
		}
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECT__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get the list of the project information that the user does not have access
	 * to, but is discoverable
	 * 
	 * @param user
	 * @param projectTypes
	 * @param projectFilters
	 * @param portalsOnly
	 * @param projectMetadataFilter
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> getUserDiscoverableProjectList(User user, List<String> projectTypes,
			List<String> projectFilters, Map<String, Object> projectMetadataFilter, String searchTerm, String limit,
			String offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Collection<String> userIds = getUserFiltersQs(user);

		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		SelectQueryStruct qs1 = new SelectQueryStruct();
		// selectors
		qs1.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__PROJECTDISPLAYNAME", "project_display_name"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__GLOBAL", "project_global"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__DISCOVERABLE", "project_discoverable"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__CATALOGNAME", "project_catalog_name"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__CREATEDBY", "project_created_by"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__CREATEDBYTYPE", "project_created_by_type"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__DATECREATED", "project_date_created"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__DATELASTEDITED", "project_date_last_edited"));
		qs1.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "PROJECT__PROJECTNAME",
				"low_project_name"));
		// only care about discoverable engines
		qs1.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECT__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs1.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", false, PixelDataType.BOOLEAN));
		// remove user permission access
		{
			SelectQueryStruct subQsUser = new SelectQueryStruct();
			subQsUser.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
			subQsUser.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "!=", subQsUser));
		}
		{
			// remove group permission access
			SelectQueryStruct subQsGroup = new SelectQueryStruct();
			subQsGroup.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PROJECTID"));
			OrQueryFilter orFilter = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider login : logins) {
				if (user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==",
						user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==",
						user.getAccessToken(login).getUserGroups()));
				orFilter.addFilter(andFilter);
			}
			if (!orFilter.isEmpty()) {
				subQsGroup.addExplicitFilter(orFilter);
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "!=", subQsGroup));
			}
		}
		// filters
		if (projectFilters != null && !projectFilters.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilters));
		}
		if (projectTypes != null && !projectTypes.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__TYPE", "==", projectTypes));
		}
		// optional word filter on the engine name
		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter("PROJECT__PROJECTID", searchTerm));
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter("PROJECT__PROJECTNAME", searchTerm));
			searchFilter.addFilter(
					securityDb.getQueryUtil().getSearchRegexFilter("PROJECT__PROJECTDISPLAYNAME", searchTerm));
			qs1.addExplicitFilter(searchFilter);
		}
		// filtering by enginemeta key-value pairs (i.e. <tag>:value): for each pair,
		// add in-filter against engineids from subquery
		if (projectMetadataFilter != null && !projectMetadataFilter.isEmpty()) {
			for (String k : projectMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAVALUE", "==",
						projectMetadataFilter.get(k)));
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "==", subQs));
			}
		}

		// add the sort
		qs1.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));

		Long long_limit = -1L;
		Long long_offset = -1L;
		if (limit != null && !limit.trim().isEmpty()) {
			long_limit = ((Number) Double.parseDouble(limit)).longValue();
		}
		if (offset != null && !offset.trim().isEmpty()) {
			long_offset = ((Number) Double.parseDouble(offset)).longValue();
		}
		qs1.setLimit(long_limit);
		qs1.setOffSet(long_offset);

		return QueryExecutionUtility.flushRsToMap(securityDb, qs1);
	}

	/**
	 * Change the user visibility (show/hide) for a project. Without removing its
	 * permissions.
	 * 
	 * @param user
	 * @param projectId
	 * @param visibility
	 * @throws SQLException
	 * @throws IllegalAccessException
	 */
	public static void setProjectVisibility(User user, String projectId, boolean visibility)
			throws SQLException, IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!userCanViewProject(user, projectId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to modify his visibility of this project.");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIdFilters));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				// need to update
				PreparedStatement ps = securityDb
						.getPreparedStatement("UPDATE PROJECTPERMISSION SET VISIBILITY=? WHERE USERID=?");
				if (ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set project visibility");
				}
				try {
					// we will set the permission to read only
					for (AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setBoolean(parameterIndex++, visibility);
						ps.setString(parameterIndex++, userId);
						ps.addBatch();
					}
					ps.executeBatch();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (Exception e) {
					classLogger.error("Failed to update project visibility", e);
					throw e;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			} else {
				// need to insert
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO PROJECTPERMISSION "
						+ "(USERID, PROJECTID, VISIBILITY, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if (ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set project visibility");
				}
				try {
					// we will set the permission to read only
					for (AuthProvider loginType : user.getLogins()) {
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
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (Exception e) {
					classLogger.error("Failed to update project visibility", e);
					throw e;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project visibility", e);
		}
	}

	/**
	 * Change the user favorite (is favorite / not favorite) for a project. Without
	 * removing its permissions.
	 * 
	 * @param user
	 * @param projectId
	 * @param visibility
	 * @throws SQLException
	 * @throws IllegalAccessException
	 */
	public static void setProjectFavorite(User user, String projectId, boolean isFavorite)
			throws SQLException, IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!projectIsGlobal(projectId) && !userCanViewProject(user, projectId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to modify his visibility of this project.");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIdFilters));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				// need to update
				PreparedStatement ps = securityDb
						.getPreparedStatement("UPDATE PROJECTPERMISSION SET FAVORITE=? WHERE USERID=? AND PROJECTID=?");
				if (ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set project favorite");
				}
				try {
					// we will set the permission to read only
					for (AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setBoolean(parameterIndex++, isFavorite);
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, projectId);
						ps.addBatch();
					}
					ps.executeBatch();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (Exception e) {
					classLogger.error("Failed to update project favorite", e);
					throw e;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			} else {
				// need to insert
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO PROJECTPERMISSION "
						+ "(USERID, PROJECTID, VISIBILITY, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if (ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set project favorite");
				}
				try {
					// we will set the permission to read only
					for (AuthProvider loginType : user.getLogins()) {
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
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (Exception e) {
					classLogger.error("Failed to update project favorite", e);
					throw e;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project favorite", e);
		}
	}

	///////////////////////////////////////////////
	///////////////////////////////////////////////
	///////////////// PROJECTS/////////////////////

	/**
	 * Return the projects the user has explicit access to
	 * 
	 * @param singleUserId
	 * @return
	 */
	public static Set<String> getProjectsUserHasExplicitAccess(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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

	/**
	 * Return if user has explicit permissions to this project
	 * 
	 * @param user
	 * @param projectId
	 * @return
	 */
	public static boolean userHasExplicitAccess(User user, String projectId) {
		return SecurityUserProjectUtils.getUserProjectPermission(user, projectId) != null;
	}

	public static List<Map<String, Object>> getProjectInfo(Collection dbFilter) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "!=", allUserProjects));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECT__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Determine if a user can request a project
	 * 
	 * @param projectId
	 * @return
	 */
	public static boolean canRequestProject(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECT__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				// if you are here, you can request
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Failed to determine whether the user can request project access", e);
		}
		return false;
	}

	/**
	 * Retrieve the project owner
	 * 
	 * @param user
	 * @param projectId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<String> getProjectOwners(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PERMISSION__ID", "==", AccessPermissionEnum.OWNER.getId()));
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__METAKEY", "metakey"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__SINGLEMULTI", "single_multi"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__DISPLAYORDER", "display_order"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__DISPLAYOPTIONS", "display_options"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__DEFAULTVALUES", "display_values"));
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
	public static boolean updateMetakeyOptions(List<Map<String, Object>> metaoptions) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean valid = false;
		PreparedStatement insertPs = null;
		String tableName = "PROJECTMETAKEYS";
		try {
			// first truncate table clean
			String truncateSql = "DELETE FROM " + tableName + " WHERE 1=1";
			securityDb.removeData(truncateSql);
			insertPs = securityDb.bulkInsertPreparedStatement(new Object[] { tableName, Constants.METAKEY,
					Constants.SINGLE_MULTI, Constants.DISPLAY_ORDER, Constants.DISPLAY_OPTIONS });
			// then insert latest options
			for (int i = 0; i < metaoptions.size(); i++) {
				insertPs.setString(1, (String) metaoptions.get(i).get("metakey"));
				insertPs.setString(2, (String) metaoptions.get(i).get("singlemulti"));
				insertPs.setInt(3, ((Number) metaoptions.get(i).get("order")).intValue());
				insertPs.setString(4, (String) metaoptions.get(i).get("displayoptions"));
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if (!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
			valid = true;
		} catch (Exception e) {
			classLogger.error("Failed to update metadata key options", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}
		return valid;
	}

	/**
	 * Get all the available engine metadata and their counts for given keys
	 * 
	 * @param engineFilters
	 * @param metaKey
	 * @return
	 */
	public static List<Map<String, Object>> getAvailableMetaValues(List<String> projectFilters, List<String> metaKeys) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
		if (projectFilters != null && !projectFilters.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__PROJECTID", "==", projectFilters));
		}
		// group
		qs.addGroupBy(new QueryColumnSelector("PROJECTMETA__METAKEY"));
		qs.addGroupBy(new QueryColumnSelector("PROJECTMETA__METAVALUE"));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * set user access request
	 * 
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param requestReason
	 * @param permission
	 * @param user
	 */
	public static void setUserAccessRequest(String userId, String userType, String projectId, String requestReason,
			int permission, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// first mark previously undecided requests as old
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET APPROVER_DECISION = 'OLD' WHERE REQUEST_USERID=? AND REQUEST_TYPE=? AND PROJECTID=? AND APPROVER_DECISION='NEW_REQUEST'";
		PreparedStatement updatePs = null;
		AbstractSqlQueryUtil securityQueryUtil = securityDb.getQueryUtil();
		try {
			int index = 1;
			updatePs = securityDb.getPreparedStatement(updateQ);
			updatePs.setString(index++, userId);
			updatePs.setString(index++, userType);
			updatePs.setString(index++, projectId);
			updatePs.execute();
			if (!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update the user access request record", e);
			throw new IllegalArgumentException(
					"An error occurred while updating user access request with detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, updatePs);
		}

		// grab user info who is submitting request
		Pair<String, String> requesterDetails = User.getPrimaryUserIdAndTypePair(user);

		// now we do the new insert
		String insertQ = "INSERT INTO PROJECTACCESSREQUEST "
				+ "(ID, REQUEST_USERID, REQUEST_TYPE, REQUEST_TIMESTAMP, REQUEST_REASON, PROJECTID, PERMISSION, SUBMITTED_BY_USERID, SUBMITTED_BY_TYPE, APPROVER_DECISION) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,'NEW_REQUEST')";
		PreparedStatement insertPs = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();

			int index = 1;
			insertPs = securityDb.getPreparedStatement(insertQ);
			insertPs.setString(index++, UUID.randomUUID().toString());
			insertPs.setString(index++, userId);
			insertPs.setString(index++, userType);
			insertPs.setTimestamp(index++, timestamp);
			securityQueryUtil.handleInsertionOfClob(insertPs.getConnection(), insertPs, requestReason, index++,
					new Gson());
			insertPs.setString(index++, projectId);
			insertPs.setInt(index++, permission);
			insertPs.setString(index++, requesterDetails.getValue0());
			insertPs.setString(index++, requesterDetails.getValue1());
			insertPs.execute();
			if (!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update the user access request record", e);
			throw new IllegalArgumentException(
					"An error occurred while adding user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}
	}

	/**
	 * 
	 * @param user
	 * @param projectId
	 * @return
	 */
	public static int getUserPendingAccessRequest(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// grab user info who is submitting request
		Pair<String, String> requesterDetails = User.getPrimaryUserIdAndTypePair(user);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__APPROVER_DECISION"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__REQUEST_USERID", "==",
				requesterDetails.getValue0()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__REQUEST_TYPE", "==",
				requesterDetails.getValue1()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__PROJECTID", "==", projectId));
		qs.addOrderBy("PROJECTACCESSREQUEST__REQUEST_TIMESTAMP", "desc");
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				String mostRecentAction = (String) values[0];
				if (!mostRecentAction.equals("APPROVED") && !mostRecentAction.equals("DENIED")
						&& !mostRecentAction.equals("OLD")) {
					return ((Number) values[1]).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve pending user access requests", e);
		}

		return -1;
	}

	/**
	 * Get the request pending database permission for a specific user
	 * 
	 * @param singleUserId
	 * @param databaseId
	 * @return
	 */
	public static List<Map<String, Object>> getUserAccessRequestsByProject(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__APPROVER_DECISION", "==", "NEW_REQUEST"));
		qs.addRelation("PROJECTACCESSREQUEST__REQUEST_USERID", "SMSS_USER__ID", "inner.join");
		qs.addRelation("PROJECTACCESSREQUEST__REQUEST_TYPE", "SMSS_USER__TYPE", "inner.join");
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Approving user access requests and giving user access in permissions
	 * 
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param requests
	 */
	public static void approveProjectUserAccessRequests(User user, String projectId, List<Map<String, String>> requests,
			String endDate) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// make sure user has right permission level to approve access requests
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// get user permissions of all requests
		List<String> permissions = new ArrayList<String>();
		for (Map<String, String> i : requests) {
			permissions.add(i.get("permission"));
		}

		// if user is not an owner, check to make sure they cannot grant owner access
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalArgumentException("You cannot grant user access to others.");
		} else {
			if (!AccessPermissionEnum.isOwner(userPermissionLvl) && permissions.contains("OWNER")) {
				throw new IllegalArgumentException("As a non-owner, you cannot grant owner access.");
			}
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		// bulk delete
		String deleteQ = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for (int i = 0; i < requests.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, requests.get(i).get("userid"));
				deletePs.setString(parameterIndex++, projectId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve project user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while deleting projectpermission with detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for (int i = 0; i < requests.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, projectId);
				insertPs.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				insertPs.setBoolean(parameterIndex++, true);
				insertPs.setString(parameterIndex++, userDetails.getValue0());
				insertPs.setString(parameterIndex++, userDetails.getValue1());
				insertPs.setTimestamp(parameterIndex++, startDate);
				insertPs.setTimestamp(parameterIndex++, verifiedEndDate);
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if (!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve project user access requests", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}

		// now we do the new bulk update to projectaccessrequest table
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement updatePs = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			updatePs = securityDb.getPreparedStatement(updateQ);
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			String userId = token.getId();
			String userType = token.getProvider().toString();
			for (int i = 0; i < requests.size(); i++) {
				int index = 1;
				updatePs.setInt(index++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp);

				updatePs.setString(index++, requests.get(i).get("requestid"));

				updatePs.addBatch();
			}
			updatePs.executeBatch();
			if (!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}

			// Adding Notification
			if (Utility.isNotificationDatabaseEnabled()) {
				for (int i = 0; i < requests.size(); i++) {
					NotificationDbUtils.createNotification(user, requests.get(i).get("userid"),
							requests.get(i).get("type"), projectId, NotificationConstants.Type.REQUEST_APPROVAL,
							NotificationConstants.APP_CATALOG, NotificationConstants.Priority.MEDIUM, null,
							requests.get(i).get("permission"));
					// Adding email notification
					EmailUtility.sendAccessRequestApprovalEmailNotification(user, requests.get(i).get("userid"),
							projectId, requests.get(i).get("permission"), EmailUtility.RESOURCE_TYPE.PROJECT);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve project user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, updatePs);
		}
	}

	/**
	 * Denying user access requests to project
	 * 
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param requests
	 */
	public static void denyProjectUserAccessRequests(User user, String projectId, List<String> requestIdList)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		// make sure user has right permission level to approve access requests
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// only project owners can deny user access requests
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to deny user access requests.");
		}

		// bulk update to projectaccessrequest table
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement ps = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			ps = securityDb.getPreparedStatement(updateQ);
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			String userId = token.getId();
			String userType = token.getProvider().toString();
			for (int i = 0; i < requestIdList.size(); i++) {
				int index = 1;
				ps.setString(index++, userId);
				ps.setString(index++, userType);
				ps.setString(index++, "DENIED");
				ps.setTimestamp(index++, timestamp);

				ps.setString(index++, requestIdList.get(i));

				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}

			// Adding Notification
			if (Utility.isNotificationDatabaseEnabled()) {
				for (int i = 0; i < requestIdList.size(); i++) {
					String requestId = requestIdList.get(i);
					List<Map<String, Object>> deniedUserDetails = getUserDetailsFromProjectAccessRequest(requestId);
					String permission = AccessPermissionEnum
							.getPermissionValueById((Integer) deniedUserDetails.get(i).get("permission"));
					NotificationDbUtils.createNotification(user, (String) deniedUserDetails.get(i).get("userId"),
							(String) deniedUserDetails.get(i).get("type"), projectId,
							NotificationConstants.Type.REQUEST_DENIAL, NotificationConstants.APP_CATALOG,
							NotificationConstants.Priority.MEDIUM, null, permission);
				}
			}

		} catch (Exception e) {
			classLogger.error("Failed to deny project user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while updating user access request detailed message = " + e.getMessage());
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
	public static void addProjectUserPermissions(User user, String projectId, List<Map<String, String>> permission,
			String endDate) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// make sure user can edit the project
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// check to make sure these users do not already have permissions to project
		// get list of userids from permission list map
		List<String> userIds = permission.stream().map(map -> map.get("userid")).collect(Collectors.toList());
		// this returns a list of existing permissions
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(userIds,
				projectId);
		if (!existingUserPermission.isEmpty()) {
			throw new IllegalArgumentException(
					"The following users already have access to this project. Please edit the existing permission level: "
							+ String.join(",", existingUserPermission.keySet()));
		}

		// if user is not an owner, check to make sure they are not adding owner access
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			List<String> permissionList = permission.stream().map(map -> map.get("permission"))
					.collect(Collectors.toList());
			if (permissionList.contains("OWNER")) {
				throw new IllegalArgumentException("As a non-owner, you cannot add owner user access.");
			}
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		// insert new user permissions in bulk
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)");
			for (int i = 0; i < permission.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, permission.get(i).get("userid"));
				ps.setString(parameterIndex++, projectId);
				ps.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission(permission.get(i).get("permission")));
				ps.setBoolean(parameterIndex++, true);
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setTimestamp(parameterIndex++, startDate);
				ps.setTimestamp(parameterIndex++, verifiedEndDate);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}

			// Adding Notification
			if (Utility.isNotificationDatabaseEnabled()) {
				for (int i = 0; i < permission.size(); i++) {
					NotificationDbUtils.createNotification(user, permission.get(i).get("userid"),
							permission.get(i).get("type"), projectId, NotificationConstants.Type.USER_ADDITION,
							NotificationConstants.APP_CATALOG, NotificationConstants.Priority.MEDIUM, null,
							permission.get(i).get("permission"));
				}
			}

		} catch (Exception e) {
			classLogger.error("Failed to add project user permissions", e);
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
	public static void removeProjectUsers(User user, List<String> existingUserIds, String projectId)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user can edit the project
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// get user permissions to remove
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(existingUserIds,
				projectId);

		// make sure all users to remove currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for the following users who do not currently have access to the project: "
							+ String.join(",", toRemoveUserIds));
		}

		// if user is not an owner, check to make sure they are not removing owner
		// access
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			List<Integer> permissionList = new ArrayList<Integer>(existingUserPermission.values());
			if (permissionList.contains(AccessPermissionEnum.OWNER.getId())) {
				throw new IllegalArgumentException("As a non-owner, you cannot remove access of an owner.");
			}
		}

		// first do a delete
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?");
			for (int i = 0; i < existingUserIds.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, existingUserIds.get(i));
				ps.setString(parameterIndex++, projectId);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to remove project users", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Get userDetails by using user's project access request
	 * 
	 * @param requestId
	 * @return List of user details
	 */
	public static List<Map<String, Object>> getUserDetailsFromProjectAccessRequest(String projectRequestId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__REQUEST_USERID", "userId"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__REQUEST_TYPE", "type"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__PERMISSION", "permission"));
		qs.addRelation("PROJECTACCESSREQUEST__PERMISSION", "PERMISSION__ID", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__ID", "==", projectRequestId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get all authors for a specific project (for app-related notifications)
	 * 
	 * @param projectId
	 * @return
	 */
	public static List<Map<String, Object>> getProjectAuthors(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "userId"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE", "userType"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "==", 1));
		qs.addRelation("SMSS_USER", "PROJECTPERMISSION", "inner.join");

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * 
	 * @param projectIds
	 * @return
	 */
	public static Map<String, String> getProjectNamesByIds(Collection<String> projectIds) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, String> projectMap = new HashMap<>();
		if (projectIds == null || projectIds.isEmpty()) {
			return projectMap;
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "name"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectIds));

		List<Map<String, Object>> resultList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		if (resultList != null) {
			for (Map<String, Object> row : resultList) {
				projectMap.put(String.valueOf(row.get("id")), String.valueOf(row.get("name")));
			}
		}
		return projectMap;
	}

}
