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
import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionErrorRules;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionProblem;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
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
import prerna.query.querystruct.filters.IQueryFilter;
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
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
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

	private static final String INSUFFICIENT_PRIVILEGES = "Insufficient privileges to modify this project's permissions.";
	private static final String NOT_HIGH_ENOUGH = "The user doesn't have the high enough permissions to modify this users project permission.";

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
		boolean isTemplate = Boolean.parseBoolean(prop.getProperty(Constants.IS_TEMPLATE, "false"));

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
			addProject(projectId, projectName, displayName, typeAndCost[0], typeAndCost[1], global, isTemplate, user);
		} else if (projectExists) {
			// update project properties anyway ... in case global was shifted for example
			updateProject(projectId, projectName, typeAndCost[0], typeAndCost[1], global);
		}

		classLogger.info("Security database going to add project with alias = {}", Utility.cleanLogString(projectName));

		// load just the insights database
		// first see if engine is already loaded
		boolean projectLoaded = false;
		IRDBMSEngine rne;
		if (Utility.projectLoaded(projectId)) {
			rne = Utility.getProject(projectId).getInsightDatabase();
		} else {
			rne = ProjectHelper.loadInsightsEngine(prop, classLogger);
		}

		// if we are doing a reload
		// we will want to remove unnecessary insights
		// from the insight permissions
		Set<String> insightPermissionIds = new HashSet<>();
		if (reloadInsights) {
			// need to flush out the current insights w/ permissions
			// will keep the same permissions
			// and perform a delta
			classLogger.info("Reloading app. Retrieving existing insights with permissions");
			SelectQueryStruct permissionsQs = new SelectQueryStruct();
			permissionsQs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__INSIGHTID"));
			permissionsQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
			insightPermissionIds.addAll(QueryExecutionUtility.flushToSetString(securityDb, permissionsQs, false));
		}

		AbstractSqlQueryUtil securityQueryUtil = securityDb.getQueryUtil();
		String insertInsight = securityQueryUtil.createInsertPreparedStatementString("INSIGHT",
				new String[] { "PROJECTID", "INSIGHTID", "INSIGHTNAME", "GLOBAL", "EXECUTIONCOUNT", "CREATEDON",
						"LASTMODIFIEDON", "LAYOUT", "CACHEABLE", "CACHEMINUTES", "CACHECRON", "CACHEDON",
						"CACHEENCRYPT", "RECIPE", "SCHEMANAME" });
		String insertInsightMeta = securityQueryUtil.createInsertPreparedStatementString("INSIGHTMETA",
				new String[] { "PROJECTID", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER" });
		Timestamp timeStamp = Utility.getCurrentSqlTimestampUTC();

		SelectQueryStruct insightsQs = new SelectQueryStruct();
		insightsQs.addSelector(
				new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_ID_COL));
		insightsQs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_NAME_COL));
		insightsQs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_LAYOUT_COL));
		insightsQs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.HIDDEN_INSIGHT_COL));
		insightsQs.addSelector(
				new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHEABLE_COL));
		insightsQs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHE_MINUTES_COL));
		insightsQs.addSelector(
				new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHE_CRON_COL));
		insightsQs.addSelector(
				new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHED_ON_COL));
		insightsQs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.CACHE_ENCRYPT_COL));
		insightsQs.addSelector(new QueryColumnSelector(
				InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.QUESTION_PKQL_COL));
		insightsQs.addSelector(
				new QueryColumnSelector(InsightAdministrator.TABLE_NAME + "__" + InsightAdministrator.SCHEMA_NAME_COL));

		SelectQueryStruct metaQs = new SelectQueryStruct();
		metaQs.addSelector(new QueryColumnSelector("INSIGHTMETA__INSIGHTID"));
		metaQs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAKEY"));
		metaQs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE"));
		metaQs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));

		final int INSIGHT_BATCH_SIZE = 5000;
		// replace the project's insights and their metadata in one transaction,
		// so a failure leaves the rows that were there
		boolean insightsSaved = false;
		try {
			QueryExecutionUtility.write(securityDb, connection -> {
				QueryExecutionUtility.executeUpdate(connection, "DELETE FROM INSIGHT WHERE PROJECTID=?",
						statement -> statement.setString(1, projectId));
				int insights = QueryExecutionUtility.executeBatchFromQuery(connection, insertInsight, rne, insightsQs,
						INSIGHT_BATCH_SIZE, (statement, row) -> {
							String insightId = bindInsight(statement, row, projectId, timeStamp, securityQueryUtil);
							// an insight that still exists keeps its permissions
							insightPermissionIds.remove(insightId);
						});
				QueryExecutionUtility.executeUpdate(connection, "DELETE FROM INSIGHTMETA WHERE PROJECTID=?",
						statement -> statement.setString(1, projectId));
				int metaRows = QueryExecutionUtility.executeBatchFromQuery(connection, insertInsightMeta, rne, metaQs,
						INSIGHT_BATCH_SIZE,
						(statement, row) -> bindInsightMeta(statement, row, projectId, securityQueryUtil));
				classLogger.info("Saved {} insights and {} insight metadata rows", insights, metaRows);
				return null;
			});
			insightsSaved = true;
		} catch (Exception e) {
			classLogger.error("Failed to create the project and initialize project security records", e);
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
			if (insightsSaved && !insightPermissionIds.isEmpty()) {
				classLogger.info("Removing insights with permissions that no longer exist");
				try {
					QueryExecutionUtility.executeBatch(securityDb,
							"DELETE FROM USERINSIGHTPERMISSION WHERE PROJECTID=? AND INSIGHTID=?", insightPermissionIds,
							(statement, deleteId) -> {
								statement.setString(1, projectId);
								statement.setString(2, deleteId);
							});
					QueryExecutionUtility.executeBatch(securityDb,
							"DELETE FROM USERINSIGHTPREFERENCE WHERE PROJECTID=? AND INSIGHTID=?", insightPermissionIds,
							(statement, deleteId) -> {
								statement.setString(1, projectId);
								statement.setString(2, deleteId);
							});
				} catch (RuntimeException e) {
					throw e;
				} catch (Exception e) {
					classLogger.error("Failed to create the project and initialize project security records", e);
				}
			}
		}

		classLogger.info("Finished adding project = {}", Utility.cleanLogString(projectId));
	}

	/**
	 * Binds an INSIGHT row from a row of the project's insights database.
	 *
	 * @return the insight's id
	 */
	private static String bindInsight(PreparedStatement statement, IHeadersDataRow row, String projectId,
			Timestamp timeStamp, AbstractSqlQueryUtil queryUtil) throws SQLException, UnsupportedEncodingException {
		Object[] values = row.getValues();
		// grab the insight rdbms values
		int index = 0;
		String insightId = values[index++].toString();
		String insightName = values[index++].toString();
		String insightLayout = values[index++].toString();
		Boolean isPrivate = (Boolean) values[index++];
		if (isPrivate == null) {
			isPrivate = false;
		}
		Boolean cacheable = (Boolean) values[index++];
		if (cacheable == null) {
			cacheable = true;
		}
		Integer cacheMinutes = (Integer) values[index++];
		if (cacheMinutes == null) {
			cacheMinutes = -1;
		}
		String cacheCron = (String) values[index++];
		SemossDate cachedOn = (SemossDate) values[index++];
		Boolean cacheEncrypt = (Boolean) values[index++];
		if (cacheEncrypt == null) {
			cacheEncrypt = false;
		}
		Object pixelObject = values[index++];
		String schemaName = (String) values[index++];

		int parameterIndex = 1;
		statement.setString(parameterIndex++, projectId);
		statement.setString(parameterIndex++, insightId);
		statement.setString(parameterIndex++, insightName);
		statement.setBoolean(parameterIndex++, !isPrivate);
		statement.setLong(parameterIndex++, 0);
		statement.setTimestamp(parameterIndex++, timeStamp);
		statement.setTimestamp(parameterIndex++, timeStamp);
		statement.setString(parameterIndex++, insightLayout);
		statement.setBoolean(parameterIndex++, cacheable);
		statement.setInt(parameterIndex++, cacheMinutes);
		queryUtil.setNullableString(statement, parameterIndex++, cacheCron);
		if (cachedOn == null) {
			statement.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
		} else {
			statement.setTimestamp(parameterIndex++, Utility.getSqlTimestampUTC(cachedOn));
		}
		statement.setBoolean(parameterIndex++, cacheEncrypt);
		queryUtil.handleInsertionOfClob(statement.getConnection(), statement, pixelObject, parameterIndex++,
				securityGson);
		queryUtil.setNullableString(statement, parameterIndex++, schemaName);
		return insightId;
	}

	/**
	 * Binds an INSIGHTMETA row from a row of the project's insights database.
	 */
	private static void bindInsightMeta(PreparedStatement statement, IHeadersDataRow row, String projectId,
			AbstractSqlQueryUtil queryUtil) throws SQLException, UnsupportedEncodingException {
		Object[] values = row.getValues();
		int parameterIndex = 1;
		statement.setString(parameterIndex++, projectId);
		statement.setString(parameterIndex++, values[0].toString());
		statement.setString(parameterIndex++, values[1].toString());
		// the value can be a clob, which goes in as it was read
		queryUtil.handleInsertionOfClob(statement.getConnection(), statement, row.getRawValues()[2], parameterIndex++,
				securityGson);
		statement.setInt(parameterIndex++, ((Number) values[3]).intValue());
	}

	/**
	 * Adds a project to the security database with its name as its display name.
	 * The project is not discoverable and not a template, and the user, when given,
	 * is recorded as its creator.
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
		addProject(projectId, projectName, projectDisplayName, projectType, projectCost, global, false, user);
	}

	public static void addProject(String projectId, String projectName, String projectDisplayName, String projectType,
			String projectCost, boolean global, boolean isTemplate, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "INSERT INTO PROJECT (PROJECTID, PROJECTNAME, TYPE, COST, GLOBAL, DISCOVERABLE, IS_TEMPLATE, CREATEDBY, CREATEDBYTYPE, DATECREATED, DATELASTEDITED, PROJECTDISPLAYNAME) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";

		try {
			QueryExecutionUtility.executeUpdate(securityDb, query, ps -> {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, projectName);
				ps.setString(parameterIndex++, projectType);
				ps.setString(parameterIndex++, projectCost);
				ps.setBoolean(parameterIndex++, global);
				ps.setBoolean(parameterIndex++, false);
				ps.setBoolean(parameterIndex++, isTemplate);
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
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to create the project and initialize project security records", e);
		}
	}

	public static void addProjectOwner(User user, String projectId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		String query = "INSERT INTO PROJECTPERMISSION (USERID, PERMISSION, PROJECTID, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED) VALUES (?,?,?,?,?,?)";
		try {
			QueryExecutionUtility.executeUpdate(securityDb, query, ps -> {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, userId);
				ps.setInt(parameterIndex++, AccessPermissionEnum.OWNER.getId());
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to add project owner", e);
		}
	}

	public static void updateProject(String projectID, String projectName, String projectType, String projectCost,
			boolean global) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "UPDATE PROJECT SET PROJECTNAME=?, TYPE=?, COST=?, GLOBAL=? WHERE PROJECTID=?";
		try {
			QueryExecutionUtility.executeUpdate(securityDb, query, ps -> {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, projectName);
				ps.setString(parameterIndex++, projectType);
				ps.setString(parameterIndex++, projectCost);
				ps.setBoolean(parameterIndex++, global);
				ps.setString(parameterIndex++, projectID);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update project properties in the security database", e);
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
		try {
			QueryExecutionUtility.executeUpdate(securityDb, query, ps -> {
				int parameterIndex = 1;
				ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
				ps.setString(parameterIndex++, projectID);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update project last edited date", e);
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
		try {
			QueryExecutionUtility.executeUpdate(securityDb, deleteQuery, ps -> {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, projectId);
			});
		} catch (Exception e) {
			classLogger.error("Failed to delete project insights before recreation", e);
			throw new IllegalArgumentException("An error occurred deleting the insights for project " + projectId);
		}
	}

	/**
	 * Builds a new insights database for the project in the given folder from the
	 * insights and insight metadata stored in the security database, and returns
	 * its file once the database is closed.
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

		try {
			QueryExecutionUtility.executeUpdate(securityDb, "UPDATE PROJECT SET PROJECTDISPLAYNAME=? WHERE PROJECTID=?",
					ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, newDisplayName);
						ps.setString(parameterIndex++, projectId);
					});
		} catch (Exception e) {
			classLogger.error("Failed to update project display name", e);
			throw new IllegalArgumentException("An error occurred updating the project display name");
		}

		UserAuditTrailUtils.recordProjectLifecycle(user, AuditEventType.PROJECT_DISPLAY_NAME_UPDATE, projectId,
				newDisplayName, Map.of("field", "displayName", "displayName", newDisplayName));
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
	 * Reads the project's portal details: whether the project is published, and the
	 * URL of its portal folder.
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
	 * Determine whether a user may clone a project. The user must be able to view
	 * the project and the owner must have explicitly enabled it as a template.
	 *
	 * @param user      current user
	 * @param projectId project identifier
	 * @return whether the user may clone the project
	 */
	public static boolean userCanCloneProject(User user, String projectId) {
		return userCanViewProject(user, projectId) && projectIsTemplate(projectId);
	}

	/**
	 * Reads back a PROJECT timestamp column that holds a timestamp.
	 *
	 * The columns these queries target are written with
	 * {@link Utility#getCurrentSqlTimestampUTC()}, which stores the UTC wall clock
	 * with no offset attached to it. Reading the value straight off the result set
	 * returns those same fields, so what comes back compares directly against
	 * another UTC wall clock with no zone applied on either side. Going through a
	 * query wrapper instead would attach a zone on the way out and shift the value
	 * by that zone's offset.
	 *
	 * @param projectId project to read the timestamp for
	 * @param selectQ   single column select taking the project id as its only
	 *                  parameter
	 * @param label     name of the timestamp, used when logging a failure
	 * @return the stored UTC wall clock, or null when the project has no row or the
	 *         column is empty
	 */
	private static LocalDateTime getProjectUtcTimestamp(String projectId, String selectQ, String label) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			return QueryExecutionUtility.queryOne(securityDb, selectQ, ps -> ps.setString(1, projectId), rs -> {
				Timestamp storedValue = rs.getTimestamp(1);
				if (storedValue != null) {
					return storedValue.toLocalDateTime();
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to retrieve the {} timestamp for project '{}'", label, projectId, e);
		}
		return null;
	}

	/**
	 * The cluster timestamp for this project's portal: when a container last
	 * reported changing the portal content.
	 *
	 * @param projectId project to read the timestamp for
	 * @return the UTC wall clock of the last reported change, or null when no
	 *         container has reported one
	 */
	public static LocalDateTime getPortalPublishedTimestamp(String projectId) {
		String selectQ = "SELECT PORTALPUBLISHED FROM PROJECT WHERE PROJECTID=?";
		return getProjectUtcTimestamp(projectId, selectQ, "portal published");
	}

	/**
	 * Gives a project a cluster timestamp for its portal when it does not have one.
	 *
	 * A container works out whether the portal copy it serves is current by
	 * comparing the cluster timestamp against its own local timestamp, so the
	 * comparison needs both to exist. A project that has never had a content change
	 * reported has no cluster timestamp, and nothing about the folder itself
	 * records when it last changed, which leaves every container free to keep
	 * serving whatever it published first. Recording the current time supplies the
	 * reference point that later comparisons work from, and reads as a change to
	 * any container whose local timestamp is older, resyncing those containers
	 * once.
	 *
	 * Only PORTALPUBLISHED is written. The user columns stay empty because no user
	 * asked for this, and DATELASTEDITED is untouched because nothing was edited.
	 * The IS NULL guard leaves the value to whichever container gets there first,
	 * so containers racing to record it settle on one time rather than overwriting
	 * each other.
	 *
	 * @param projectId project to give a cluster timestamp
	 * @return true when the project carries a cluster timestamp afterwards,
	 *         including when another container recorded it first, false when it
	 *         could not be written
	 */
	public static boolean initPortalPublishedTimestamp(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String updateQ = "UPDATE PROJECT SET PORTALPUBLISHED=? WHERE PROJECTID=? AND PORTALPUBLISHED IS NULL";
		try {
			QueryExecutionUtility.executeUpdate(securityDb, updateQ, ps -> {
				ps.setTimestamp(1, Utility.getCurrentSqlTimestampUTC());
				ps.setString(2, projectId);
			});
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to record an initial cluster timestamp for project '{}'", projectId, e);
			return false;
		}
	}

	/**
	 * Moves the cluster timestamp for this project's portal to now, recording that
	 * the portal content changed.
	 *
	 * Every other container compares this against its own local timestamp for when
	 * it last published the portal into public_home, to decide whether the copy it
	 * serves is still current. This is the db write on its own; routes reporting a
	 * write go through ProjectPortalsHelper, which owns when a change counts as
	 * one.
	 *
	 * @param user      user performing the write
	 * @param projectId project that was written to
	 */
	public static void setPortalPublish(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String updateQ = "UPDATE PROJECT SET DATELASTEDITED=?, PORTALPUBLISHED=?, PORTALPUBLISHEDUSER=?, PORTALPUBLISHEDTYPE=? WHERE PROJECTID=?";
		try {
			QueryExecutionUtility.executeUpdate(securityDb, updateQ, ps -> {
				int i = 1;
				ps.setTimestamp(i++, Utility.getCurrentSqlTimestampUTC());
				ps.setTimestamp(i++, Utility.getCurrentSqlTimestampUTC());
				ps.setString(i++, token.getId());
				ps.setString(i++, token.getProvider().toString());
				ps.setString(i++, projectId);
			});
		} catch (Exception e) {
			classLogger.error("Failed to update project portal published status", e);
		}
	}

	/**
	 * The cluster timestamp for this project's custom reactors: when a container
	 * last reported compiling them.
	 *
	 * @param projectId project to read the timestamp for
	 * @return the UTC wall clock of the last reported compilation, or null when no
	 *         container has reported one
	 */
	public static LocalDateTime getReactorCompilationTimestamp(String projectId) {
		String selectQ = "SELECT REACTORSCOMPILED FROM PROJECT WHERE PROJECTID=?";
		return getProjectUtcTimestamp(projectId, selectQ, "reactor compilation");
	}

	/**
	 * Records that the project's reactors were compiled now, by the user's primary
	 * login.
	 * 
	 * @param user
	 * @param projectId
	 */
	public static void setReactorCompilation(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String updateQ = "UPDATE PROJECT SET REACTORSCOMPILED=?, REACTORSCOMPILEDUSER=?, REACTORSCOMPILEDTYPE=? WHERE PROJECTID=?";
		try {
			QueryExecutionUtility.executeUpdate(securityDb, updateQ, ps -> {
				int i = 1;
				ps.setTimestamp(i++, Utility.getCurrentSqlTimestampUTC());
				ps.setString(i++, token.getId());
				ps.setString(i++, token.getProvider().toString());
				ps.setString(i++, projectId);
			});
		} catch (Exception e) {
			classLogger.error("Failed to update project reactor compilation timestamp", e);
		}
	}

	/**
	 * Whether the user is an owner of the project, which the UI calls Author.
	 * Grants that have ended do not count.
	 *
	 * @return whether the user is an owner of the project now, directly or through
	 *         a team; see {@link CurrentPermissionLevel}
	 */
	public static boolean userIsOwner(User user, String projectId) {
		return CurrentPermissionLevel.isOwner(user, PermissionTargetResource.project(projectId));
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
	 * Whether the user can edit the project, as an editor or an owner. Grants that
	 * have ended do not count.
	 *
	 * @return whether the user is an editor or an owner of the project now,
	 *         directly or through a team; see {@link CurrentPermissionLevel}
	 */
	public static boolean userCanEditProject(User user, String projectId) {
		return CurrentPermissionLevel.canEdit(user, PermissionTargetResource.project(projectId));
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
	 * Gives a person access to a project, as a permission batch. The project's
	 * editors can; only its owners give Author access.
	 *
	 * @param user       the user giving access
	 * @param newUserId  the person
	 * @param projectId  the project
	 * @param permission the level, by name; anything else is Read-Only
	 * @param endDate    when the access ends, or null
	 * @throws IllegalAccessException when the user cannot give the access
	 */
	public static void addProjectUser(User user, String newUserId, String projectId, String permission, String endDate)
			throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.project(projectId),
				List.of(PermissionChange
						.add(PermissionSubject.ofUser(newUserId, null), new PermissionGrant(permission, endDate))),
				PermissionBatchOptions.userMode(),
				new PermissionErrorRules(endDate)
						.illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, INSUFFICIENT_PRIVILEGES)
						.illegalArgument(PermissionProblem.ErrorCode.ALREADY_HAS_ACCESS,
								"This user already has access to this project. Please edit the existing permission level.")
						.illegalAccess(PermissionProblem.ErrorCode.GIVES_AUTHOR,
								"Cannot give owner level access to this project since you are not currently an owner.")
						.illegalArgument(PermissionProblem.ErrorCode.WRITE_FAILED,
								"An error occurred adding user permissions for this project"));
	}

	/**
	 * Changes a person's access to a project, as a permission batch, and tells
	 * them. The project's editors can; only its owners change an Author's access or
	 * give Author access.
	 *
	 * @param user             the user changing the access
	 * @param existingUserId   the person
	 * @param existingUserType the person's login type, for the notification
	 * @param projectId        the project
	 * @param newPermission    the level, by name; anything else is Read-Only
	 * @param endDate          when the access ends, or null for never
	 * @throws IllegalAccessException when the user cannot change the access, or the
	 *                                person has none
	 */
	public static void editProjectUserPermission(User user, String existingUserId, String existingUserType,
			String projectId, String newPermission, String endDate) throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.project(projectId),
				List.of(PermissionChange.change(PermissionSubject.ofUser(existingUserId, existingUserType),
						new PermissionGrant(newPermission, endDate))),
				PermissionBatchOptions.userMode().notifying(false, true),
				new PermissionErrorRules(endDate)
						.illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, INSUFFICIENT_PRIVILEGES)
						.illegalAccess(PermissionProblem.ErrorCode.NO_ACCESS,
								"Attempting to modify project permission for a user who does not currently have access to the project")
						.illegalAccess(PermissionProblem.ErrorCode.CHANGES_AUTHOR, NOT_HIGH_ENOUGH)
						.illegalAccess(PermissionProblem.ErrorCode.GIVES_AUTHOR,
								"Cannot give owner level access to this project since you are not currently an owner.")
						.illegalArgument(PermissionProblem.ErrorCode.WRITE_FAILED,
								"An error occurred updating the user permissions for this project"));
	}

	/**
	 * Changes many people's access to a project together, as a permission batch,
	 * and tells them. Each request has {@code userid}, {@code permission} and
	 * {@code type}; every row gets the same end date.
	 *
	 * @param user      the user changing the access
	 * @param projectId the project
	 * @param requests  the changes
	 * @param endDate   when the access ends, or null for never
	 * @throws IllegalAccessException when the user cannot change people's access
	 */
	public static void editProjectUserPermissions(User user, String projectId, List<Map<String, String>> requests,
			String endDate) throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.project(projectId),
				PermissionBatchUtils.peopleChanges(PermissionChange.Action.CHANGE, requests, endDate),
				PermissionBatchOptions.userMode().notifying(false, true),
				new PermissionErrorRules(endDate)
						.illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, INSUFFICIENT_PRIVILEGES)
						.illegalArgumentWithIds(PermissionProblem.ErrorCode.NO_ACCESS,
								"Attempting to modify user permission for the following users who do not currently have access to the project: ")
						.illegalArgument(PermissionProblem.ErrorCode.CHANGES_AUTHOR,
								"As a non-owner, you cannot edit access of an owner.")
						.illegalArgument(PermissionProblem.ErrorCode.GIVES_AUTHOR,
								"As a non-owner, you cannot give a user access as an owner.")
						.illegalArgument(PermissionProblem.ErrorCode.WRITE_FAILED,
								"An error occurred updating the user permissions for this project"));
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
		deletes.add("DELETE FROM USERPROJECTPREFERENCE WHERE PROJECTID=?");
		deletes.add("DELETE FROM USERINSIGHTPREFERENCE WHERE PROJECTID=?");
		deletes.add("DELETE FROM PROJECTMETA WHERE PROJECTID=?");
		deletes.add("DELETE FROM ASSETENGINE WHERE PROJECTID=?");
		deletes.add("DELETE FROM PROJECTACCESSREQUEST WHERE PROJECTID=?");
		deletes.add("DELETE FROM PROJECTDEPENDENCIES WHERE PROJECTID=?");
		for (String deleteQuery : deletes) {
			try {
				QueryExecutionUtility.executeUpdate(securityDb, deleteQuery, ps -> ps.setString(1, projectId));
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				classLogger.error("Failed to delete project", e);
			}
		}
	}

	/**
	 * Takes a person's access to a project away, with their permissions on its
	 * insights, as a permission batch. The project's editors can, and anyone can
	 * leave; only its owners take away an Author's access.
	 *
	 * @param user           the user taking the access away
	 * @param existingUserId the person
	 * @param projectId      the project
	 * @throws IllegalAccessException when the user cannot take the access away
	 */
	public static void removeProjectUser(User user, String existingUserId, String projectId)
			throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.project(projectId),
				List.of(PermissionChange.remove(PermissionSubject.ofUser(existingUserId, null))),
				PermissionBatchOptions.userMode().allowingSelfRemoval().removingInsightPermissions(),
				new PermissionErrorRules()
						.illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, INSUFFICIENT_PRIVILEGES)
						.illegalArgument(PermissionProblem.ErrorCode.NO_ACCESS,
								"Attempting to modify user permission for a user who does not currently have access to the project")
						.illegalAccess(PermissionProblem.ErrorCode.CHANGES_AUTHOR, NOT_HIGH_ENOUGH)
						.illegalArgument(PermissionProblem.ErrorCode.WRITE_FAILED,
								"An error occurred removing the user permissions for the project and insights of this project"));
	}

	/**
	 * Deletes the user's grant on the project. Called when that grant has expired.
	 * 
	 * @param userId
	 * @param projectId
	 * @return
	 */
	public static void removeExpiredProjectUser(String userId, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteQuery = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
		try {
			QueryExecutionUtility.executeUpdate(securityDb, deleteQuery, ps -> {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, projectId);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalArgumentException("An error occurred removing the user permissions for this project");
		}
		UserAuditTrailUtils.recordExpiredPermissionRemoval(AuditTargetType.PROJECT, projectId, projectId, null, null,
				userId);
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

		try {
			QueryExecutionUtility.executeUpdate(securityDb, "UPDATE PROJECT SET GLOBAL=? WHERE PROJECTID=?", ps -> {
				ps.setBoolean(1, global);
				ps.setString(2, projectId);
			});
			UserAuditTrailUtils.recordProjectLifecycle(user, AuditEventType.PROJECT_GLOBAL_UPDATE, projectId, null,
					Map.of("field", "global", "global", global));
		} catch (Exception e) {
			classLogger.error("Failed to update project global visibility setting", e);
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
		if (!SecurityUserProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalAccessException(
					"The user doesn't have permission to set this project as a template. Only the owner or an admin can perform this action.");
		}
		boolean updated = setProjectTemplate(projectId, isTemplate);
		UserAuditTrailUtils.recordProjectLifecycle(user, AuditEventType.PROJECT_TEMPLATE_UPDATE, projectId, null,
				Map.of("field", "template", "template", isTemplate));
		return updated;
	}

	/**
	 * System variant of {@link #setProjectTemplate(User, String, boolean)} with no
	 * permission check. Used at boot to heal the template flag on platform projects
	 * from their smss; must never be exposed to user input.
	 *
	 * @param projectId  project identifier
	 * @param isTemplate whether the project is a template
	 * @return {@code true} when the flag is updated
	 */
	public static boolean setProjectTemplate(String projectId, boolean isTemplate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		try {
			QueryExecutionUtility.write(securityDb, connection -> {
				try (PreparedStatement ps = connection
						.prepareStatement("UPDATE PROJECT SET IS_TEMPLATE=? WHERE PROJECTID=?")) {
					ps.setBoolean(1, isTemplate);
					ps.setString(2, projectId);
					int updatedRows = ps.executeUpdate();
					if (updatedRows != 1) {
						throw new IllegalArgumentException("Project does not exist");
					}

				}
				return null;
			});
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update project template setting", e);
			throw new IllegalArgumentException("An error occurred setting the project template flag", e);
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
		try {
			QueryExecutionUtility.write(securityDb, connection -> {
				{
					String update1 = "UPDATE PROJECT SET GLOBAL=? WHERE PROJECTID=?";

					QueryExecutionUtility.executeUpdate(connection, update1, ps -> {
						int parameterIndex = 1;
						ps.setBoolean(parameterIndex++, true);
						ps.setString(parameterIndex++, projectId);
					});

				}

				{
					String update1 = "UPDATE INSIGHT SET GLOBAL=? WHERE PROJECTID=?";

					QueryExecutionUtility.executeUpdate(connection, update1, ps -> {
						int parameterIndex = 1;
						ps.setBoolean(parameterIndex++, true);
						ps.setString(parameterIndex++, projectId);
					});

				}
				return null;
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to set project visibility to globally accessible", e);
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

		try {
			QueryExecutionUtility.executeUpdate(securityDb, "UPDATE PROJECT SET DISCOVERABLE=? WHERE PROJECTID=?",
					ps -> {
						ps.setBoolean(1, discoverable);
						ps.setString(2, projectId);
					});
			UserAuditTrailUtils.recordProjectLifecycle(user, AuditEventType.PROJECT_DISCOVERABLE_UPDATE, projectId,
					null, Map.of("field", "discoverable", "discoverable", discoverable));
		} catch (Exception e) {
			classLogger.error("Failed to update project discoverability setting", e);
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
		try {
			QueryExecutionUtility.executeUpdate(securityDb, "UPDATE PROJECT SET PROJECTNAME=? WHERE PROJECTID=?",
					ps -> {
						int parameterIndex = 1;
						// SET
						ps.setString(parameterIndex++, newProjectName);
						// WHERE
						ps.setString(parameterIndex++, projectId);
					});
		} catch (Exception e) {
			classLogger.error("Failed to update project name", e);
			throw new IllegalArgumentException("An error occurred updating the project name");
		}
		UserAuditTrailUtils.recordProjectLifecycle(user, AuditEventType.PROJECT_RENAME, projectId, newProjectName,
				Map.of("field", "name", "name", newProjectName));
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
		try {
			QueryExecutionUtility.write(securityDb, connection -> {
				// first do a delete
				String deleteQ = "DELETE FROM PROJECTDEPENDENCIES WHERE PROJECTID=?";

				QueryExecutionUtility.executeUpdate(connection, deleteQ, deletePs -> {
					int parameterIndex = 1;
					deletePs.setString(parameterIndex++, projectId);
				});

				if (dependentEngines != null && !dependentEngines.isEmpty()) {
					AccessToken token = user.getPrimaryLoginToken();
					java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
					// now we do the new insert with the order of the tags
					String query = securityDb.getQueryUtil().createInsertPreparedStatementString("PROJECTDEPENDENCIES",
							new String[] { "PROJECTID", "ENGINEID", "ENGINETYPE", "USERID", "TYPE", "DATEADDED" });

					QueryExecutionUtility.executeBatch(connection, query, dependentEngines, (ps, depEngine) -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, projectId);
						ps.setString(parameterIndex++, (String) depEngine.get("ENGINEID"));
						ps.setString(parameterIndex++, (String) depEngine.get("ENGINETYPE"));
						ps.setString(parameterIndex++, token.getId());
						ps.setString(parameterIndex++, token.getProvider().getLabel());
						ps.setTimestamp(parameterIndex++, timestamp);
					});

				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to update project dependency mappings", e);
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
		try {
			QueryExecutionUtility.executeUpdate(securityDb, deleteQ, deletePs -> {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, projectId);
				deletePs.setString(parameterIndex++, dependentEngineId);
			});
		} catch (Exception e) {
			classLogger.error("Failed to remove project dependency mappings", e);
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
	 * Gives a person the requested access to the engines and projects a project
	 * depends on, or asks their owners for it. On a model, a person added gets no
	 * usage limit, and a person whose access changes keeps theirs.
	 *
	 * @param requester           the user asking
	 * @param projectId           the project
	 * @param newUserId           the person
	 * @param newUserType         the person's login type
	 * @param requestedPermission the level, by name
	 * @param endDate             when the access ends, or null
	 * @return the engines and projects by what was done for each
	 */
	public static Map<String, Object> propagateProjectPermission(User requester, String projectId, String newUserId,
			String newUserType, String requestedPermission, String endDate) {
		return propagate(requester, projectId, newUserId, newUserType, requestedPermission, endDate, null);
	}

	/**
	 * Gives a person the requested access to the engines and projects a project
	 * depends on, or asks their owners for it. Only a model's owners set a limit
	 * that differs from the current one.
	 *
	 * @param requester           the user asking
	 * @param projectId           the project
	 * @param newUserId           the person
	 * @param newUserType         the person's login type
	 * @param requestedPermission the level, by name
	 * @param endDate             when the access ends, or null
	 * @param usageLimit          the person's model usage limit on each model given
	 *                            or changed, or null for no limit defined
	 * @return the engines and projects by what was done for each
	 */
	public static Map<String, Object> propagateProjectPermission(User requester, String projectId, String newUserId,
			String newUserType, String requestedPermission, String endDate, ModelUsageLimit usageLimit) {
		return propagate(requester, projectId, newUserId, newUserType, requestedPermission, endDate,
				new PermissionGrant().withUsageLimit(usageLimit));
	}

	/**
	 * Does the work of both propagateProjectPermission methods: for each engine and
	 * project the project depends on, it gives the person the requested level, or
	 * files an access request for its owners when the requester is Read-Only or has
	 * no access there.
	 *
	 * @param limit the person's model usage limit on the models: a grant that gives
	 *              {@code usageLimit}
	 *              ({@link PermissionGrant#isUsageLimitGiven()}), or null for none,
	 *              so a person whose access changes keeps theirs
	 */
	private static Map<String, Object> propagate(User requester, String projectId, String newUserId, String newUserType,
			String requestedPermission, String endDate, PermissionGrant limit) {
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
			// only a model has usage limits
			PermissionGrant engineLimit = IEngine.CATALOG_TYPE.MODEL.name().equals(dependentEngineType) ? limit : null;

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
						editDependentEngineUser(requester, newUserId, newUserType, dependentEngineId,
								requestedPermission, endDate, engineLimit);
					} else {
						SecurityProjectUtils.editProjectUserPermission(requester, newUserId, newUserType,
								dependentEngineId, requestedPermission, endDate);
					}

					accessGranted.add(dependentEngineId);
					classLogger.info("User has updated permission for {} to {}", newUserId, dependentEngineId);
				} catch (IllegalAccessException | IllegalArgumentException e) {
					couldNotAddRequest.add(dependentEngineId);
					classLogger.error("Failed to propagate project permissions to related artifacts", e);
				}
				// if none of the above and requestor has proper permission, add user to the
				// engine permission database
			} else if (requesterEnginePermission < 3 && currentNewUserPermission == null) {
				try {
					if (isEngine) {
						addDependentEngineUser(requester, newUserId, dependentEngineId, requestedPermission, endDate,
								engineLimit);
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

	/**
	 * Gives a person access to an engine a project depends on, with the usage limit
	 * given, or none.
	 *
	 * @param limit on a model, the limit the propagation gives; null for none
	 */
	private static void addDependentEngineUser(User requester, String newUserId, String engineId, String level,
			String endDate, PermissionGrant limit) throws IllegalAccessException {
		SecurityEngineUtils.addEngineUser(requester, newUserId, engineId, level, endDate,
				limit == null ? null : limit.getUsageLimit());
	}

	/**
	 * Changes a person's access to an engine a project depends on. Their usage
	 * limit is kept unless one is given.
	 *
	 * @param limit on a model, the limit the propagation gives; null for none
	 */
	private static void editDependentEngineUser(User requester, String newUserId, String newUserType, String engineId,
			String level, String endDate, PermissionGrant limit) throws IllegalAccessException {
		if (limit != null && limit.isUsageLimitGiven()) {
			SecurityEngineUtils.editEngineUserPermission(requester, newUserId, newUserType, engineId, level, endDate,
					limit.getUsageLimit());
		} else {
			SecurityEngineUtils.editEngineUserPermission(requester, newUserId, newUserType, engineId, level, endDate);
		}
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
		try {
			QueryExecutionUtility.write(securityDb, connection -> {
				// first do a delete
				String deleteQ = "DELETE FROM PROJECTMETA WHERE METAKEY=? AND PROJECTID=?";

				QueryExecutionUtility.executeBatch(connection, deleteQ, metadata.keySet(), (deletePs, field) -> {
					int parameterIndex = 1;
					deletePs.setString(parameterIndex++, field);
					deletePs.setString(parameterIndex++, projectId);
				});

				// now we do the new insert with the order of the tags
				String query = securityDb.getQueryUtil().createInsertPreparedStatementString("PROJECTMETA",
						new String[] { "PROJECTID", "METAKEY", "METAVALUE", "METAORDER" });

				QueryExecutionUtility.executeBatch(connection, query, ps -> {
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
				});
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to update project metadata", e);
			throw new IllegalStateException("Unable to update project metadata", e);
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
	 * Ids of the projects with a metadata value, such as every project tagged
	 * {@code collaboration}. METAVALUE is a CLOB, which not every database compares
	 * with =, so the values are matched here.
	 */
	public static List<String> getProjectIdsWithMetaValue(String metaKey, String metaValue) {
		try {
			List<String[]> rows = QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(),
					"SELECT PROJECTID, METAVALUE FROM PROJECTMETA WHERE METAKEY = ?", ps -> ps.setString(1, metaKey),
					rs -> new String[] { rs.getString(1), rs.getString(2) });
			Set<String> ids = new LinkedHashSet<>();
			for (String[] row : rows) {
				if (row[1] != null && metaValue.equals(row[1].trim())) {
					ids.add(row[0]);
				}
			}
			return new ArrayList<>(ids);
		} catch (Exception e) {
			classLogger.error("Failed to read projects by metadata value", e);
			throw new IllegalStateException("Unable to read project metadata", e);
		}
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
		return getUserProjectList(user, projectTypes, projectIdFilters, favoritesOnly, projectMetadataFilter,
				permissionFilters, searchTerm, limit, offset, sortFields, onlyTemplates, null, null);
	}

	/**
	 * Get the projects the user can see: the global ones, and those the user or one
	 * of their groups holds a grant on, leaving out any the user has hidden.
	 *
	 * @param user                  the user; their ids come from every login in the
	 *                              session
	 * @param projectTypes          keep only the projects of these types; null or
	 *                              empty for every type
	 * @param projectIdFilters      keep only these project ids; null or empty for
	 *                              every project
	 * @param favoritesOnly         keep only the user's favorites
	 * @param projectMetadataFilter keep only the projects whose metadata holds each
	 *                              of these key and value pairs; null or empty for
	 *                              no filter
	 * @param permissionFilters     keep only the projects on which the user's own
	 *                              grant is one of these
	 *                              {@link prerna.auth.AccessPermissionEnum} ids,
	 *                              ignoring group grants; null or empty for no
	 *                              filter
	 * @param searchTerm            keep only the projects whose id, name, or
	 *                              display name matches; null or blank for no
	 *                              search
	 * @param limit                 the most projects to return; null or empty for
	 *                              no limit
	 * @param offset                how many projects to skip; null or empty for
	 *                              none
	 * @param sortFields            sort keys and directions: {@code PROJECTNAME},
	 *                              {@code DATECREATED}, or {@code DATELASTEDITED}
	 *                              mapped to {@code ASC} or {@code DESC}; null or
	 *                              empty to sort by name
	 * @param onlyTemplates         keep only the template projects
	 * @param effectivePermissions  keep only the projects whose effective
	 *                              permission is one of these
	 *                              {@link prerna.auth.AccessPermissionEnum} ids:
	 *                              the better of the user's own grant and their
	 *                              groups' grant, with a global project the user
	 *                              holds no grant on counting as read only; null or
	 *                              empty for no filter
	 * @param createdBy             keep only the projects created by one of these
	 *                              (login id, login type) pairs, as
	 *                              {@link User#getUserIdAndType(User)} returns
	 *                              them; null or empty for no filter
	 * @return one map per project, with its details, the user's and their groups'
	 *         grants, and the effective {@code permission}
	 * @throws IllegalArgumentException when a level in {@code effectivePermissions}
	 *                                  is not an
	 *                                  {@link prerna.auth.AccessPermissionEnum} id
	 */
	public static List<Map<String, Object>> getUserProjectList(User user, List<String> projectTypes,
			List<String> projectIdFilters, boolean favoritesOnly, Map<String, Object> projectMetadataFilter,
			List<Integer> permissionFilters, String searchTerm, String limit, String offset,
			Map<String, String> sortFields, boolean onlyTemplates, Collection<Integer> effectivePermissions,
			Collection<Pair<String, String>> createdBy) {
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
		qs1.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, QueryFunctionSelector
				.makeCoalesceSelector("PROJECT__PROJECTDISPLAYNAME", "PROJECT__PROJECTNAME", "display_name"),
				"low_project_name"));
		qs1.addSelector(
				QueryFunctionSelector.makeCol2ValCoalesceSelector("USER_PREFERENCES__FAVORITE", 0, "project_favorite"));
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

		// add a join to get the user permission level
		{
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID", "PROJECTID"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					"PROJECTPERMISSION__PERMISSION", "PERMISSION"));
			qs2.addGroupBy(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID", "PROJECTID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
			IRelation subQuery = new SubqueryRelationship(qs2, "USER_PERMISSIONS", "left.outer.join",
					new String[] { "USER_PERMISSIONS__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs1.addRelation(subQuery);
		}

		// add a join to get if favorite and the visibility
		qs1.addRelation(getUserPreferencesSubquery(securityDb, user));

		// add a join to get the group permission level
		{
			SelectQueryStruct qs3 = new SelectQueryStruct();
			qs3.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));
			qs3.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					groupProjectPermission + "PERMISSION", "PERMISSION"));
			qs3.addGroupBy(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));

			qs3.addExplicitFilter(getUserGroupPermissionFilter(user, groupProjectPermission));

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
		if (effectivePermissions != null && !effectivePermissions.isEmpty()) {
			qs1.addExplicitFilter(getEffectivePermissionFilter("USER_PERMISSIONS__PERMISSION",
					"GROUP_PERMISSIONS__PERMISSION", projectPrefix + "GLOBAL", effectivePermissions));
		}
		if (createdBy != null && !createdBy.isEmpty()) {
			qs1.addExplicitFilter(
					getCreatedByFilter(projectPrefix + "CREATEDBY", projectPrefix + "CREATEDBYTYPE", createdBy));
		}

		// only show those that are visible
		// remember, user preferences cast this to int
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PREFERENCES__VISIBILITY", "==",
				Arrays.asList(new Object[] { 1, null }), PixelDataType.CONST_INT));
		// favorites only
		// remember, user preferences cast this to int
		if (favoritesOnly) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PREFERENCES__FAVORITE", "==", 1,
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
		// always add a secondary sort by project_id
		qs1.addOrderBy(new QueryColumnOrderBySelector("project_id"));

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
	 * @param projectTypes
	 * @param includeGlobal
	 * @param includeDiscoverable
	 * @param includeExistingAccess
	 * @return
	 */
	public static List<String> getUserProjectIdList(User user, List<String> projectTypes, boolean includeGlobal,
			boolean includeDiscoverable, boolean includeExistingAccess) {
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
		if (projectTypes != null && !projectTypes.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix + "TYPE", "==", projectTypes));
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
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(projectPermissionPrefix + "PERMISSION", "!=",
					null, PixelDataType.CONST_INT));
			IQueryFilter directAccess = SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID",
					existingAccessComparator, qs2);
			if (includeExistingAccess) {
				orFilter.addFilter(directAccess);
			} else {
				qs1.addExplicitFilter(directAccess);
			}
		}
		{
			SelectQueryStruct qs3 = new SelectQueryStruct();
			qs3.addSelector(new QueryColumnSelector(groupProjectPermissionPrefix + "PROJECTID", "PROJECTID"));
			qs3.addExplicitFilter(getUserGroupPermissionFilter(user, groupProjectPermissionPrefix));
			IQueryFilter groupAccess = SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID",
					existingAccessComparator, qs3);
			if (includeExistingAccess) {
				orFilter.addFilter(groupAccess);
			} else {
				qs1.addExplicitFilter(groupAccess);
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
		qs.addSelector(
				QueryFunctionSelector.makeCol2ValCoalesceSelector("USER_PREFERENCES__FAVORITE", 0, "project_favorite"));
		// for sorting
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, QueryFunctionSelector
				.makeCoalesceSelector("PROJECT__PROJECTDISPLAYNAME", "PROJECT__PROJECTNAME", "display_name"),
				"low_project_name"));
		// back to the others
		if (projectFilter != null && !projectFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
		}
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			AndQueryFilter directAccess = new AndQueryFilter();
			directAccess.addFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
			directAccess.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "!=", null,
					PixelDataType.CONST_INT));
			orFilter.addFilter(directAccess);

			SelectQueryStruct groupAccess = new SelectQueryStruct();
			groupAccess.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PROJECTID"));
			groupAccess.addExplicitFilter(getUserGroupPermissionFilter(user, "GROUPPROJECTPERMISSION__"));
			orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "==", groupAccess));
			qs.addExplicitFilter(orFilter);
		}
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
		qs.addRelation(getUserPreferencesSubquery(securityDb, user));
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * A left join, as USER_PREFERENCES, to the user's preferences on each project:
	 * FAVORITE is 1 when any of the user's logins favorited it, and VISIBILITY is 1
	 * when any of them shows it and 0 when they all hide it, both cast to int. A
	 * project the user has no preferences on joins to null.
	 *
	 * @param securityDb the security database
	 * @param user       the user; their logins come from the session
	 * @return the join to add to a query on PROJECT
	 */
	private static IRelation getUserPreferencesSubquery(IRDBMSEngine securityDb, User user) {
		SelectQueryStruct prefQs = new SelectQueryStruct();
		prefQs.addSelector(new QueryColumnSelector("USERPROJECTPREFERENCE__PROJECTID", "PROJECTID"));

		QueryFunctionSelector castFavorite = QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.CAST,
				"USERPROJECTPREFERENCE__FAVORITE", "castFavorite");
		castFavorite.setDataType(securityDb.getQueryUtil().getIntegerDataTypeName());
		prefQs.addSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, castFavorite, "FAVORITE"));
		QueryFunctionSelector castVisibility = QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.CAST,
				"USERPROJECTPREFERENCE__VISIBILITY", "castVisibility");
		castVisibility.setDataType(securityDb.getQueryUtil().getIntegerDataTypeName());
		prefQs.addSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, castVisibility, "VISIBILITY"));
		prefQs.addGroupBy(new QueryColumnSelector("USERPROJECTPREFERENCE__PROJECTID", "PROJECTID"));
		prefQs.addExplicitFilter(
				getUserIdAndTypeFilter(user, "USERPROJECTPREFERENCE__USERID", "USERPROJECTPREFERENCE__TYPE"));
		return new SubqueryRelationship(prefQs, "USER_PREFERENCES", "left.outer.join",
				new String[] { "USER_PREFERENCES__PROJECTID", "PROJECT__PROJECTID", "=" });
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
	 * Show or hide a project in the user's lists, for each of their logins. The
	 * visibility is saved in USERPROJECTPREFERENCE, apart from the user's
	 * permissions, and changes only this project.
	 *
	 * @param user       the user
	 * @param projectId  the project
	 * @param visibility true to show the project, false to hide it
	 * @throws IllegalAccessException when the user cannot view the project
	 * @throws Exception              when the visibility cannot be saved
	 */
	public static void setProjectVisibility(User user, String projectId, boolean visibility) throws Exception {
		if (!userCanViewProject(user, projectId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to modify his visibility of this project.");
		}
		setUserPreference(user, "USERPROJECTPREFERENCE", List.of(Pair.with("PROJECTID", projectId)), "VISIBILITY",
				visibility);
	}

	/**
	 * Favorite or unfavorite a project for each of the user's logins. The favorite
	 * is saved in USERPROJECTPREFERENCE, apart from the user's permissions, so a
	 * user can favorite a public project they have no direct access to.
	 *
	 * @param user       the user
	 * @param projectId  the project
	 * @param isFavorite true to favorite the project, false to unfavorite it
	 * @throws IllegalAccessException when the project is not public and the user
	 *                                cannot view it
	 * @throws Exception              when the favorite cannot be saved
	 */
	public static void setProjectFavorite(User user, String projectId, boolean isFavorite) throws Exception {
		if (!projectIsGlobal(projectId) && !userCanViewProject(user, projectId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to modify his visibility of this project.");
		}
		setUserPreference(user, "USERPROJECTPREFERENCE", List.of(Pair.with("PROJECTID", projectId)), "FAVORITE",
				isFavorite);
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
	 * Lists every metadata key defined for projects.
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
	 * Reads the settings of the project metadata keys: whether each takes one value
	 * or many, its display order and options, and its default values. Reads only
	 * the given key when one is passed, or every key otherwise.
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
	 * Replaces every project metadata key definition with the given ones, each with
	 * its key, single or multi setting, display order and display options, in one
	 * write.
	 * 
	 * @param metaoptions
	 * @return
	 */
	public static boolean updateMetakeyOptions(List<Map<String, Object>> metaoptions) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String tableName = "PROJECTMETAKEYS";
		try {
			return QueryExecutionUtility.write(securityDb, connection -> {
				// first truncate table clean
				String truncateSql = "DELETE FROM " + tableName + " WHERE 1=1";
				QueryExecutionUtility.executeUpdate(connection, truncateSql, deletePs -> {
				});
				try (PreparedStatement insertPs = connection.prepareStatement(securityDb.getQueryUtil()
						.createInsertPreparedStatementString(tableName, new String[] { Constants.METAKEY,
								Constants.SINGLE_MULTI, Constants.DISPLAY_ORDER, Constants.DISPLAY_OPTIONS }))) {
					// then insert latest options
					for (int i = 0; i < metaoptions.size(); i++) {
						insertPs.setString(1, (String) metaoptions.get(i).get("metakey"));
						insertPs.setString(2, (String) metaoptions.get(i).get("singlemulti"));
						insertPs.setInt(3, ((Number) metaoptions.get(i).get("order")).intValue());
						insertPs.setString(4, (String) metaoptions.get(i).get("displayoptions"));
						insertPs.addBatch();
					}
					insertPs.executeBatch();

					return true;
				}
			});
		} catch (Exception e) {
			classLogger.error("Failed to update metadata key options", e);
		}
		return false;
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
		try {
			QueryExecutionUtility.write(securityDb, connection -> {
				// first mark previously undecided requests as old
				String updateQ = "UPDATE PROJECTACCESSREQUEST SET APPROVER_DECISION = 'OLD' WHERE REQUEST_USERID=? AND REQUEST_TYPE=? AND PROJECTID=? AND APPROVER_DECISION='NEW_REQUEST'";
				AbstractSqlQueryUtil securityQueryUtil = securityDb.getQueryUtil();
				try {
					int index = 1;
					try (PreparedStatement updatePs = connection.prepareStatement(updateQ)) {
						updatePs.setString(index++, userId);
						updatePs.setString(index++, userType);
						updatePs.setString(index++, projectId);
						updatePs.execute();

					}
				} catch (Exception e) {
					classLogger.error("Failed to update the user access request record", e);
					throw new IllegalArgumentException(
							"An error occurred while updating user access request with detailed message = "
									+ e.getMessage());
				}

				// grab user info who is submitting request
				Pair<String, String> requesterDetails = User.getPrimaryUserIdAndTypePair(user);

				// now we do the new insert
				String insertQ = "INSERT INTO PROJECTACCESSREQUEST "
						+ "(ID, REQUEST_USERID, REQUEST_TYPE, REQUEST_TIMESTAMP, REQUEST_REASON, PROJECTID, PERMISSION, SUBMITTED_BY_USERID, SUBMITTED_BY_TYPE, APPROVER_DECISION) "
						+ "VALUES (?,?,?,?,?,?,?,?,?,'NEW_REQUEST')";
				try {
					java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();

					int index = 1;
					try (PreparedStatement insertPs = connection.prepareStatement(insertQ)) {
						insertPs.setString(index++, UUID.randomUUID().toString());
						insertPs.setString(index++, userId);
						insertPs.setString(index++, userType);
						insertPs.setTimestamp(index++, timestamp);
						securityQueryUtil.setNullableLargeText(insertPs, index++, requestReason);
						insertPs.setString(index++, projectId);
						insertPs.setInt(index++, permission);
						insertPs.setString(index++, requesterDetails.getValue0());
						insertPs.setString(index++, requesterDetails.getValue1());
						insertPs.execute();

					}
				} catch (Exception e) {
					classLogger.error("Failed to update the user access request record", e);
					throw new IllegalArgumentException(
							"An error occurred while adding user access request detailed message = " + e.getMessage());
				}
				return null;
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update the user access request record", e);
			throw new IllegalArgumentException(
					"An error occurred while updating user access request with detailed message = " + e.getMessage());
		}
	}

	/**
	 * The level the user asked for in their pending access request on the project,
	 * from their primary login's newest request that is not approved, denied or
	 * old, or -1 when there is none.
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
	 * Approves access requests, as a permission batch: each person gets the level
	 * they asked for, and the requests are marked approved in the same save. The
	 * project's editors can; only its owners give or change Author access.
	 *
	 * @param user      the user approving
	 * @param projectId the project
	 * @param requests  the requests, each with {@code requestid}, {@code userid},
	 *                  {@code type} and {@code permission}
	 * @param endDate   when the access ends, or null
	 *
	 * @throws IllegalAccessException when the user cannot change people's access
	 */
	public static void approveProjectUserAccessRequests(User user, String projectId, List<Map<String, String>> requests,
			String endDate) throws IllegalAccessException {
		PermissionTargetResource target = PermissionTargetResource.project(projectId);
		PermissionBatchUtils.saveChanges(user, target, PermissionBatchUtils.approvalChanges(target, requests, endDate),
				PermissionBatchOptions.userMode().approving(target, requests),
				PermissionErrorRules.forApprovals(endDate).illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION,
						"Insufficient privileges to modify this project's permissions."));

		// notifications and emails go out after the save, and a failure does not
		// undo it
		if (Utility.isNotificationDatabaseEnabled()) {
			for (Map<String, String> request : requests) {
				try {
					NotificationDbUtils.createNotification(user, request.get("userid"), request.get("type"), projectId,
							NotificationConstants.Type.REQUEST_APPROVAL, NotificationConstants.APP_CATALOG,
							NotificationConstants.Priority.MEDIUM, null, request.get("permission"),
							NotificationConstants.DisplaySurface.BELL);
					EmailUtility.sendAccessRequestApprovalEmailNotification(user, request.get("userid"), projectId,
							request.get("permission"), EmailUtility.RESOURCE_TYPE.PROJECT);
				} catch (Exception e) {
					classLogger.warn("Could not tell {} their request for project {} was approved",
							request.get("userid"), projectId, e);
				}
			}
		}
	}

	/**
	 * Denies access requests to a project: each is marked denied, by the user, and
	 * each requester is told. Only the project's owners can, directly or through a
	 * team.
	 *
	 * @param user          the user denying
	 * @param projectId     the project
	 * @param requestIdList the requests' ids
	 * @throws IllegalAccessException when the user is not an editor or not an owner
	 *                                of the project
	 */
	public static void denyProjectUserAccessRequests(User user, String projectId, List<String> requestIdList)
			throws IllegalAccessException {
		PermissionBatchUtils.denyRequests(user, PermissionTargetResource.project(projectId), requestIdList,
				PermissionBatchOptions.userMode(),
				new PermissionErrorRules()
						.illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, INSUFFICIENT_PRIVILEGES)
						.illegalAccess(PermissionProblem.ErrorCode.OWNERS_ONLY,
								"Insufficient privileges to deny user access requests.")
						.illegalArgument(PermissionProblem.ErrorCode.WRITE_FAILED,
								"An error occurred while updating user access request"));

		// notifications go out after the save, and a failure does not undo it
		if (Utility.isNotificationDatabaseEnabled()) {
			for (String requestId : requestIdList) {
				try {
					for (Map<String, Object> request : getUserDetailsFromProjectAccessRequest(requestId)) {
						NotificationDbUtils.createNotification(user, (String) request.get("userId"),
								(String) request.get("type"), projectId, NotificationConstants.Type.REQUEST_DENIAL,
								NotificationConstants.APP_CATALOG, NotificationConstants.Priority.MEDIUM, null,
								AccessPermissionEnum.getPermissionValueById((Integer) request.get("permission")),
								NotificationConstants.DisplaySurface.BELL);
					}
				} catch (Exception e) {
					classLogger.warn("Could not tell the requester that request {} for project {} was denied",
							requestId, projectId, e);
				}
			}
		}
	}

	/**
	 * Gives many people access to a project together, as a permission batch, and
	 * tells them. Each entry has {@code userid}, {@code permission} and, when
	 * known, {@code type}; every row gets the same end date.
	 *
	 * @param user       the user giving access
	 * @param projectId  the project
	 * @param permission the people and their levels
	 * @param endDate    when the access ends, or null
	 * @throws IllegalAccessException when the user cannot give access
	 */
	public static void addProjectUserPermissions(User user, String projectId, List<Map<String, String>> permission,
			String endDate) throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.project(projectId),
				PermissionBatchUtils.peopleChanges(PermissionChange.Action.ADD, permission, endDate),
				PermissionBatchOptions.userMode().notifying(true, false),
				new PermissionErrorRules(endDate)
						.illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, INSUFFICIENT_PRIVILEGES)
						.illegalArgumentWithIds(PermissionProblem.ErrorCode.ALREADY_HAS_ACCESS,
								"The following users already have access to this project. Please edit the existing permission level: ")
						.illegalArgument(PermissionProblem.ErrorCode.GIVES_AUTHOR,
								"As a non-owner, you cannot add owner user access.")
						.illegalArgument(PermissionProblem.ErrorCode.WRITE_FAILED,
								"An error occurred adding user permissions for this project"));
	}

	/**
	 * Takes many people's access to a project away together, as a permission batch.
	 * Their permissions on its insights stay.
	 *
	 * @param user            the user taking the access away
	 * @param existingUserIds the people
	 * @param projectId       the project
	 * @throws IllegalAccessException when the user cannot change people's access
	 */
	public static void removeProjectUsers(User user, List<String> existingUserIds, String projectId)
			throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.project(projectId),
				PermissionChange.forPeople(PermissionChange.Action.REMOVE, existingUserIds, null),
				PermissionBatchOptions.userMode(),
				new PermissionErrorRules()
						.illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, INSUFFICIENT_PRIVILEGES)
						.illegalArgumentWithIds(PermissionProblem.ErrorCode.NO_ACCESS,
								"Attempting to modify user permission for the following users who do not currently have access to the project: ")
						.illegalArgument(PermissionProblem.ErrorCode.CHANGES_AUTHOR,
								"As a non-owner, you cannot remove access of an owner.")
						.illegalArgument(PermissionProblem.ErrorCode.WRITE_FAILED,
								"An error occurred removing the users from this project"));
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
	 * Maps each of the given project ids to its project name. Ids with no project
	 * are left out, and no ids give an empty map.
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
