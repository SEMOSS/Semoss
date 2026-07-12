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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.PasswordRequirements;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.project.api.IProject;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class SecurityAdminUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityAdminUtils.class);

	private static SecurityAdminUtils instance = new SecurityAdminUtils();

	private SecurityAdminUtils() {

	}

	public static SecurityAdminUtils getInstance(User user) {
		if (user == null) {
			return null;
		}
		if (userIsAdmin(user)) {
			return instance;
		}
		return null;
	}

	/**
	 * Check if the user is an admin
	 * 
	 * @param userId String representing the id of the user to check
	 */
	public static Boolean userIsAdmin(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Failed to verify whether the user has admin access", e);
		}

		return false;
	}

	/**
	 * See if the user is an admin
	 * 
	 * @param userId
	 * @param type
	 * @return
	 */
	public boolean userIsAdmin(String userId, String type) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__TYPE", "==", type));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Failed to verify whether the user has admin access", e);
		}

		return false;
	}

	public boolean otherAdminsExist(String userId, String type) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				if ((row[0] + "").equals(userId) && (row[1] + "").equals(type)) {
					continue;
				} else {
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to check whether another admin exists", e);
		}

		return false;
	}

	/*
	 * all other methods should be on the instance so that we cannot bypass security
	 * easily
	 */

	/**
	 * Get all users
	 * 
	 * @param offset
	 * @param limit
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUsers(String searchTerm, long limit, long offset)
			throws IllegalArgumentException {
		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		final String SMSS_USER_PREFIX = "SMSS_USER__";
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "ID", "id"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "TYPE", "type"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "NAME", "name"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "USERNAME", "username"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "EMAIL", "email"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "ADMIN", "admin"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "PUBLISHER", "publisher"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "EXPORTER", "exporter"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "PHONE", "phone"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "PHONEEXTENSION", "phoneextension"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "COUNTRYCODE", "countrycode"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "MODELUSAGERESTRICTION", "model_usage_restriction"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "MODELMAXTOKENS", "model_max_tokens"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "MODELMAXRESPONSETIME", "model_max_response_time"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "MODELUSAGEFREQUENCY", "model_usage_frequency"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "LOCKED", "locked"));
		qs.addOrderBy(new QueryColumnOrderBySelector(SMSS_USER_PREFIX + "NAME"));
		qs.addOrderBy(new QueryColumnOrderBySelector(SMSS_USER_PREFIX + "TYPE"));
		if (hasSearchTerm) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "ID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "NAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "USERNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "EMAIL", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return getSimpleQuery(qs);
	}

	/**
	 * Get all API users
	 *
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllAPIUsers(String searchTerm, long limit, long offset)
			throws IllegalArgumentException {
		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		final String SMSS_USER_PREFIX = "SMSS_USER__";
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "ID", "id"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "TYPE", "type"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "NAME", "name"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "USERNAME", "username"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "EMAIL", "email"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "ADMIN", "admin"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "PUBLISHER", "publisher"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "EXPORTER", "exporter"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "PHONE", "phone"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "PHONEEXTENSION", "phoneextension"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "COUNTRYCODE", "countrycode"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "MODELUSAGERESTRICTION", "model_usage_restriction"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "MODELMAXTOKENS", "model_max_tokens"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "MODELMAXRESPONSETIME", "model_max_response_time"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "MODELUSAGEFREQUENCY", "model_usage_frequency"));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PREFIX + "DATECREATED", "date_created"));
		qs.addOrderBy(new QueryColumnOrderBySelector(SMSS_USER_PREFIX + "NAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "TYPE", "==",
				AuthProvider.API_USER.toString()));
		if (hasSearchTerm) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "ID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "NAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "USERNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "EMAIL", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return getSimpleQuery(qs);
	}

	/**
	 * Get all user engines
	 * 
	 * @param userId
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserEngines(String userId, List<String> engineTypes, String searchTerm,
			long limit, long offset) throws IllegalArgumentException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID", "user_id"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION", "app_permission"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "app_display_name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "app_permission"));
		// Additive engine_* keys (keep app_* above so legacy BI keeps working) +
		// type/subtype
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "engine_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "engine_display_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "engine_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "engine_subtype"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "engine_permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userId));
		if (engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
		}
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEDISPLAYNAME", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		qs.addRelation("ENGINEPERMISSION", "ENGINE", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER,
				"ENGINE__ENGINEDISPLAYNAME", "low_engine_name"));
		qs.addOrderBy(new QueryColumnOrderBySelector("low_engine_name"));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get the engines a given user does NOT have access to (the inverse of
	 * getAllUserEngines). Used by admins to grant a user new engine access.
	 *
	 * @param userId
	 * @param engineTypes optional list of engine types to constrain to
	 * @param searchTerm  optional search across engine id/name/display name
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getUserEnginesNoCredentials(String userId, List<String> engineTypes,
			String searchTerm, long limit, long offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "engine_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "engine_display_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "engine_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "engine_subtype"));
		// Anti-join: exclude engines this user already has an ENGINEPERMISSION for
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "!=", subQs));
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "!=", null,
					PixelDataType.NULL_VALUE));
		}
		if (engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
		}
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEDISPLAYNAME", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER,
				"ENGINE__ENGINEDISPLAYNAME", "low_engine_name"));
		qs.addOrderBy(new QueryColumnOrderBySelector("low_engine_name"));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get all user projects
	 *
	 * @param userId
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserProjects(String userId, List<String> projectTypes, String searchTerm,
			long limit, long offset) throws IllegalArgumentException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID", "user_id"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION", "project_permission"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTDISPLAYNAME", "project_display_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "project_permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));
		if (projectTypes != null && !projectTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__TYPE", "==", projectTypes));
		}
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTDISPLAYNAME", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		qs.addRelation("PROJECTPERMISSION", "PROJECT", "inner.join");
		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "inner.join");
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER,
				"PROJECT__PROJECTDISPLAYNAME", "low_project_name"));
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get the projects a given user does NOT have access to (the inverse of
	 * getAllUserProjects). Used by admins to grant a user new project access.
	 *
	 * @param userId
	 * @param searchTerm optional search across project id/name/display name
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getUserProjectsNoCredentials(String userId, List<String> projectTypes,
			String searchTerm, long limit, long offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTDISPLAYNAME", "project_display_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		// Anti-join: exclude projects this user already has a PROJECTPERMISSION for
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "!=", subQs));
			subQs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "!=", null,
					PixelDataType.NULL_VALUE));
		}
		if (projectTypes != null && !projectTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__TYPE", "==", projectTypes));
		}
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTDISPLAYNAME", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER,
				"PROJECT__PROJECTDISPLAYNAME", "low_project_name"));
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get all user insights
	 *
	 * @param user
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserInsights(User user, List<String> projectFilter, String searchTerm,
			long limit, long offset) throws IllegalArgumentException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();
		boolean hasProjectFilters = projectFilter != null && !projectFilter.isEmpty();
		Collection<String> userIds = getUserFiltersQs(user);

		String insightPrefix = "INSIGHT__";
		String projectPrefix = "PROJECT__";
		String userInsightPrefix = "USERINSIGHTPERMISSION__";
		String userProjectPrefix = "PROJECTPERMISSION__";
		String groupProjectPermission = "GROUPPROJECTPERMISSION__";
		String groupInsightPermission = "GROUPINSIGHTPERMISSION__";

		SelectQueryStruct qs = new SelectQueryStruct();
		// TODO: delete the below 3 in the future once FE moves to project_
		qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "app_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "app_insight_id"));
		// base selectors
		qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__GLOBAL", "project_global"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "project_insight_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "view_count"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LAYOUT", "layout"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEMINUTES", "cacheMinutes"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHECRON", "cacheCron"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEDON", "cachedOn"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEENCRYPT", "cacheEncrypt"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		// lower name for sorting
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER,
				insightPrefix + "INSIGHTNAME", "low_name"));
		// add the USER PERMISSIONS subquery returns
		qs.addSelector(new QueryColumnSelector("INSIGHT_USER_PERMISSIONS__PERMISSION", "insight_permission"));
		qs.addSelector(new QueryColumnSelector("PROJECT_USER_PERMISSIONS__PERMISSION", "project_permission"));
		qs.addSelector(new QueryColumnSelector("INSIGHT_GROUP_PERMISSIONS__PERMISSION", "insight_group_permission"));
		qs.addSelector(new QueryColumnSelector("PROJECT_GROUP_PERMISSIONS__PERMISSION", "project_group_permission"));
		qs.addSelector(new QueryColumnSelector("INSIGHT_USER_PERMISSIONS__FAVORITE", "insight_favorite"));

		// if user project owner - return owner
		// if group project owner - return owner
		// if user and group null - return User (which will be null, but that is
		// desired)
		// if user or group null - return non null permission level
		// if both non null - return max permissions
		{
			// setup
			AndQueryFilter and = new AndQueryFilter();
			and.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_GROUP_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));
			and.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_USER_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));

			AndQueryFilter and1 = new AndQueryFilter();
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_GROUP_PERMISSIONS__PERMISSION", "!=", null,
					PixelDataType.CONST_INT));
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_USER_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));

			AndQueryFilter and2 = new AndQueryFilter();
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_GROUP_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_USER_PERMISSIONS__PERMISSION", "!=", null,
					PixelDataType.CONST_INT));

			SimpleQueryFilter maxPermFilter = SimpleQueryFilter.makeColToColFilter(
					"INSIGHT_USER_PERMISSIONS__PERMISSION", "<", "INSIGHT_GROUP_PERMISSIONS__PERMISSION");
			SimpleQueryFilter userOwnerFilter = SimpleQueryFilter.makeColToValFilter(
					"PROJECT_USER_PERMISSIONS__PERMISSION", "==", AccessPermissionEnum.OWNER.getId(),
					PixelDataType.CONST_INT);
			SimpleQueryFilter groupOwnerFilter = SimpleQueryFilter.makeColToValFilter(
					"PROJECT_GROUP_PERMISSIONS__PERMISSION", "==", AccessPermissionEnum.OWNER.getId(),
					PixelDataType.CONST_INT);

			// logic
			QueryIfSelector qis5 = QueryIfSelector.makeQueryIfSelector(maxPermFilter,
					new QueryColumnSelector("INSIGHT_USER_PERMISSIONS__PERMISSION"),
					new QueryColumnSelector("INSIGHT_GROUP_PERMISSIONS__PERMISSION"), "permission");

			QueryIfSelector qis4 = QueryIfSelector.makeQueryIfSelector(and2,
					new QueryColumnSelector("INSIGHT_USER_PERMISSIONS__PERMISSION"), qis5, "permission");

			QueryIfSelector qis3 = QueryIfSelector.makeQueryIfSelector(and1,
					new QueryColumnSelector("INSIGHT_GROUP_PERMISSIONS__PERMISSION"), qis4, "permission");

			QueryIfSelector qis2 = QueryIfSelector.makeQueryIfSelector(and,
					new QueryColumnSelector("INSIGHT_USER_PERMISSIONS__PERMISSION"), qis3, "permission");

			QueryIfSelector qis1 = QueryIfSelector.makeQueryIfSelector(groupOwnerFilter,
					new QueryConstantSelector(AccessPermissionEnum.OWNER.getId()), qis2, "permission");

			QueryIfSelector qis = QueryIfSelector.makeQueryIfSelector(userOwnerFilter,
					new QueryConstantSelector(AccessPermissionEnum.OWNER.getId()), qis1, "permission");

			qs.addSelector(qis);
		}

		// add PROJECT relation
		qs.addRelation("PROJECT", "INSIGHT", "inner.join");
		// add a join to get the user permission level and if favorite
		{
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector(userInsightPrefix + "INSIGHTID", "INSIGHTID"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX,
					userInsightPrefix + "FAVORITE", "FAVORITE"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					userInsightPrefix + "PERMISSION", "PERMISSION"));
			qs2.addGroupBy(new QueryColumnSelector(userInsightPrefix + "INSIGHTID", "INSIGHTID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userInsightPrefix + "USERID", "==", userIds));
			IRelation subQuery = null;
			subQuery = new SubqueryRelationship(qs2, "INSIGHT_USER_PERMISSIONS", "left.outer.join",
					new String[] { "INSIGHT_USER_PERMISSIONS__INSIGHTID", insightPrefix + "INSIGHTID", "=" });
			qs.addRelation(subQuery);
		}
		// add a join to get the user project permission
		{
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector(userProjectPrefix + "PROJECTID", "PROJECTID"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					userProjectPrefix + "PERMISSION", "PERMISSION"));
			qs2.addGroupBy(new QueryColumnSelector(userProjectPrefix + "PROJECTID", "PROJECTID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "USERID", "==", userIds));
			IRelation subQuery = new SubqueryRelationship(qs2, "PROJECT_USER_PERMISSIONS", "left.outer.join",
					new String[] { "PROJECT_USER_PERMISSIONS__PROJECTID", insightPrefix + "PROJECTID", "=" });
			qs.addRelation(subQuery);
		}

		// add a join to get the group insight permission level
		{
			SelectQueryStruct qs3 = new SelectQueryStruct();
			qs3.addSelector(new QueryColumnSelector(groupInsightPermission + "INSIGHTID", "INSIGHTID"));
			qs3.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					groupInsightPermission + "PERMISSION", "PERMISSION"));
			qs3.addGroupBy(new QueryColumnSelector(groupInsightPermission + "INSIGHTID", "INSIGHTID"));

			// filter on groups
			OrQueryFilter groupInsightOrFilters = new OrQueryFilter();
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider login : logins) {
				if (user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "TYPE", "==",
						user.getAccessToken(login).getUserGroupType()));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "ID", "==",
						user.getAccessToken(login).getUserGroups()));
				groupInsightOrFilters.addFilter(andFilter1);

				AndQueryFilter andFilter2 = new AndQueryFilter();
				andFilter2.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==",
						user.getAccessToken(login).getUserGroupType()));
				andFilter2.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==",
						user.getAccessToken(login).getUserGroups()));
				groupProjectOrFilters.addFilter(andFilter2);
			}

			if (!groupInsightOrFilters.isEmpty()) {
				qs3.addExplicitFilter(groupInsightOrFilters);
			} else {
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "TYPE", "==", null));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "ID", "==", null));
				qs3.addExplicitFilter(andFilter1);
			}

			IRelation subQuery = new SubqueryRelationship(qs3, "INSIGHT_GROUP_PERMISSIONS", "left.outer.join",
					new String[] { "INSIGHT_GROUP_PERMISSIONS__INSIGHTID", "INSIGHT__INSIGHTID", "=" });
			qs.addRelation(subQuery);
		}

		// add a join to get the group project permission level
		{
			SelectQueryStruct qs4 = new SelectQueryStruct();
			qs4.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));
			qs4.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					groupProjectPermission + "PERMISSION", "PERMISSION"));
			qs4.addGroupBy(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));

			// filter on groups
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider login : logins) {
				if (user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}

				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==",
						user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==",
						user.getAccessToken(login).getUserGroups()));
				groupProjectOrFilters.addFilter(andFilter);
			}

			if (!groupProjectOrFilters.isEmpty()) {
				qs4.addExplicitFilter(groupProjectOrFilters);
			} else {
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", null));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", null));
				qs4.addExplicitFilter(andFilter1);
			}

			IRelation subQuery = new SubqueryRelationship(qs4, "PROJECT_GROUP_PERMISSIONS", "left.outer.join",
					new String[] { "PROJECT_GROUP_PERMISSIONS__PROJECTID", insightPrefix + "PROJECTID", "=" });
			qs.addRelation(subQuery);
		}

		// remove hidden projects
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", "!=", subQs));

			// fill in the sub query with the single return + filters
			subQs.addSelector(new QueryColumnSelector(userProjectPrefix + "PROJECTID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "VISIBILITY", "==", false,
					PixelDataType.BOOLEAN));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "USERID", "==", userIds));
		}

		// filter the insight ids based on
		OrQueryFilter orFilter = new OrQueryFilter();
		qs.addExplicitFilter(orFilter);
		// 1 - insights i have access to
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(insightPrefix + "INSIGHTID", "==", subQs));

			// fill in the sub query with the single return + filters
			subQs.addSelector(new QueryColumnSelector(userInsightPrefix + "INSIGHTID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userInsightPrefix + "USERID", "==", userIds));
		}
		// 2 - insight that are global within projects i have access to
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(insightPrefix + "INSIGHTID", "==", subQs));

			// fill in the sub query with the single return + filters
			subQs.addSelector(new QueryColumnSelector(insightPrefix + "INSIGHTID"));
			subQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(insightPrefix + "GLOBAL", "==", true, PixelDataType.BOOLEAN));
			SelectQueryStruct subQs2 = new SelectQueryStruct();
			// store the subquery
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery(insightPrefix + "PROJECTID", "==", subQs2));

			subQs2.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTID"));
			// joins
			subQs2.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
			// project and insight must be global must be global
			OrQueryFilter projectSubset = new OrQueryFilter();
			projectSubset.addFilter(
					SimpleQueryFilter.makeColToValFilter(projectPrefix + "GLOBAL", "==", true, PixelDataType.BOOLEAN));
			projectSubset.addFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "USERID", "==", userIds));
			subQs2.addExplicitFilter(projectSubset);
		}
		// 3 insights where i am the owner of the project
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(insightPrefix + "PROJECTID", "==", subQs));

			// fill in the sub query with the single return + filters
			subQs.addSelector(new QueryColumnSelector(userProjectPrefix + "PROJECTID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "USERID", "==", userIds));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "PERMISSION", "==",
					AccessPermissionEnum.OWNER.getId(), PixelDataType.CONST_INT));
		}
		// 4 insights i have access to from group permissions
		{
			// first lets make sure we have any groups
			OrQueryFilter groupInsightOrFilters = new OrQueryFilter();
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider login : logins) {
				if (user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "TYPE", "==",
						user.getAccessToken(login).getUserGroupType()));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "ID", "==",
						user.getAccessToken(login).getUserGroups()));
				groupInsightOrFilters.addFilter(andFilter1);

				AndQueryFilter andFilter2 = new AndQueryFilter();
				andFilter2.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==",
						user.getAccessToken(login).getUserGroupType()));
				andFilter2.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==",
						user.getAccessToken(login).getUserGroups()));
				groupProjectOrFilters.addFilter(andFilter2);
			}
			// 4.a does the group have explicit access
			if (!groupInsightOrFilters.isEmpty()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(insightPrefix + "INSIGHTID", "==", subQs));

				// we need to have the insight filters
				subQs.addSelector(new QueryColumnSelector(groupInsightPermission + "INSIGHTID"));
				subQs.addExplicitFilter(groupInsightOrFilters);
			}
			// 4.b does the group have project owner access
			if (!groupProjectOrFilters.isEmpty()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", "==", subQs));

				// we need to have the insight filters
				subQs.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID"));
				subQs.addExplicitFilter(groupProjectOrFilters);
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "PERMISSION",
						"==", AccessPermissionEnum.OWNER.getId(), PixelDataType.CONST_INT));
			}
		}
		// optional filters
		// on the project
		if (hasProjectFilters) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(insightPrefix + "PROJECTID", "==", projectFilter));
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(projectPrefix + "PROJECTID", "==", projectFilter));
		}
		// optional word filter on the engine name
		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter.addFilter(
					securityDb.getQueryUtil().getSearchRegexFilter(insightPrefix + "INSIGHTNAME", searchTerm));
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(insightPrefix + "INSIGHTID", searchTerm));
			qs.addExplicitFilter(searchFilter);
		}
		qs.addOrderBy("low_name");
		;
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	// TODO >>> Kunal: update below method
	/**
	 * Get all user databases
	 * 
	 * @param userId
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserInsightAccess(String projectId, String userId)
			throws IllegalArgumentException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT DISTINCT " + "INSIGHT.INSIGHTID AS \"insight_id\", "
				+ "INSIGHT.INSIGHTNAME AS \"insight_name\", " + "INSIGHT.GLOBAL AS \"insight_public\", "
				+ "INSIGHT.PROJECTID AS \"project_id\", " + "SUB_Q.NAME AS \"insight_permission\", "
				+ "SUB_Q.USERID AS \"user_id\" " + "FROM INSIGHT LEFT OUTER JOIN ( "
				+ "SELECT USERINSIGHTPERMISSION.INSIGHTID, " + "PERMISSION.NAME, " + "USERINSIGHTPERMISSION.USERID "
				+ "FROM USERINSIGHTPERMISSION "
				+ "INNER JOIN PERMISSION on USERINSIGHTPERMISSION.PERMISSION=PERMISSION.ID "
				+ "WHERE USERINSIGHTPERMISSION.PROJECTID = '" + projectId + "' AND USERINSIGHTPERMISSION.USERID = '"
				+ userId + "'" + ") AS SUB_Q ON SUB_Q.INSIGHTID = INSIGHT.INSIGHTID " + "WHERE INSIGHT.PROJECTID = '"
				+ projectId + "' ORDER BY INSIGHT.INSIGHTNAME";

		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setQuery(query);
		qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Update user information.
	 * 
	 * @param adminId
	 * @param userInfo
	 * @return
	 * @throws IllegalArgumentException
	 */
	public boolean editUser(Map<String, Object> userInfo) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// input fields
		String userId = userInfo.get("id") != null ? userInfo.get("id").toString() : "";
		if (userId == null || userId.isEmpty()) {
			throw new NullPointerException("Must provide a unique and non-empty user id");
		}
		String type = userInfo.get("type") != null ? userInfo.get("type").toString() : "";
		if (type == null || type.isEmpty()) {
			throw new NullPointerException("Must provide the user type");
		}
		// only relevant for native type
		String password = userInfo.get("password") != null ? userInfo.get("password").toString() : "";
		// other values
		String name = userInfo.get("name") != null ? userInfo.get("name").toString() : "";
		String email = userInfo.get("email") != null ? userInfo.get("email").toString().trim().toLowerCase() : "";
		String username = userInfo.get("username") != null ? userInfo.get("username").toString() : "";
		// no one uses these... oh well
		String phone = userInfo.get("phone") != null ? userInfo.get("phone").toString() : "";
		String phoneExtension = userInfo.get("phoneextension") != null ? userInfo.get("phoneextension").toString() : "";
		String countryCode = userInfo.get("countrycode") != null ? userInfo.get("countrycode").toString() : "";
		// model restrictions
		String modelUsageRestriction = userInfo.get("model_usage_restriction") != null
				? userInfo.get("model_usage_restriction").toString()
				: null;
		String modelUsageFrequency = userInfo.get("model_usage_frequency") != null
				? userInfo.get("model_usage_frequency").toString()
				: null;
		Integer modelMaxTokens = null;
		if (userInfo.get("model_max_tokens") != null) {
			try {
				modelMaxTokens = ((Number) userInfo.get("model_max_tokens")).intValue();
			} catch (ClassCastException e) {
				classLogger.error("Failed to update user account information in the security database", e);
				throw new IllegalArgumentException("model_max_tokens must be a valid integer value");
			}
		}
		Double modelMaxResponseTime = null;
		if (userInfo.get("model_max_response_time") != null) {
			try {
				modelMaxResponseTime = ((Number) userInfo.get("model_max_response_time")).doubleValue();
			} catch (ClassCastException e) {
				classLogger.error("Failed to update user account information in the security database", e);
				throw new IllegalArgumentException("model_max_response_time must be a valid double value");
			}
		}
		// the boolean values
		Boolean adminValue = Boolean.FALSE;
		if (userInfo.containsKey("admin")) {
			if (userInfo.get("admin") instanceof Number) {
				adminValue = ((Number) userInfo.get("admin")).intValue() == 1;
			} else {
				adminValue = Boolean.parseBoolean(userInfo.get("admin") + "");
			}
		}
		Boolean publisherValue = Boolean.FALSE;
		if (userInfo.containsKey("publisher")) {
			if (userInfo.get("publisher") instanceof Number) {
				publisherValue = ((Number) userInfo.get("publisher")).intValue() == 1;
			} else {
				publisherValue = Boolean.parseBoolean(userInfo.get("publisher") + "");
			}
		}
		Boolean exporterValue = Boolean.FALSE;
		if (userInfo.containsKey("exporter")) {
			if (userInfo.get("exporter") instanceof Number) {
				exporterValue = ((Number) userInfo.get("exporter")).intValue() == 1;
			} else {
				exporterValue = Boolean.parseBoolean(userInfo.get("exporter") + "");
			}
		}

		String newSalt = null;
		String newHashPass = null;

		// cannot edit a user to match another user when native... would cause some
		// serious issues :/
		// so we will check if you are switching to a native
		boolean isNative = false;
		if (type != null && !type.isEmpty()) {
			isNative = type.equalsIgnoreCase("NATIVE");
		} else {
			isNative = SecurityQueryUtils.isUserType(userId, AuthProvider.NATIVE);
		}
		if (isNative) {
			// username cannot be changed and must match the userid
			if (!userId.equals(username)) {
				throw new IllegalArgumentException(
						"For native users, the id and the username must match and cannot be udpated");
			}
		} else {
			password = null;
		}

		boolean updatePassword = isNative && password != null && !password.isEmpty();

		// grab and validate all these errors together...
		// TODO: should combine with the above errors as well
		String error = "";
		if (email != null && !email.isEmpty()) {
			try {
				validEmail(email, false);
			} catch (Exception e) {
				classLogger.error("Failed to update user account information in the security database", e);
				error += e.getMessage();
			}
		}
		if (updatePassword) {
			try {
				validPassword(userId, AuthProvider.NATIVE, password);
			} catch (Exception e) {
				classLogger.error("Failed to update user account information in the security database", e);
				error += e.getMessage();
			}
			if (error.isEmpty()) {
				newSalt = AbstractSecurityUtils.generateSalt();
				newHashPass = AbstractSecurityUtils.hash(password, newSalt);
			}
		}
		if (phone != null && !phone.isEmpty()) {
			try {
				phone = formatPhone(phone);
			} catch (Exception e) {
				classLogger.error("Failed to update user account information in the security database", e);
				error += e.getMessage();
			}
		}

		if (error != null && !error.isEmpty()) {
			throw new IllegalArgumentException(error);
		}

		/**
		 * Create ps and add updated rows to ps and values
		 */

		String[] whereCol = { "ID", "TYPE" };
		String[] columnsToUpdate = null;
		if (updatePassword) {
			columnsToUpdate = new String[] { "EMAIL", "USERNAME", "NAME", "ADMIN", "PUBLISHER", "EXPORTER", "PHONE",
					"PHONEEXTENSION", "COUNTRYCODE", "MODELUSAGERESTRICTION", "MODELMAXTOKENS", "MODELMAXRESPONSETIME",
					"MODELUSAGEFREQUENCY", "PASSWORD", "SALT" };
		} else {
			columnsToUpdate = new String[] { "EMAIL", "USERNAME", "NAME", "ADMIN", "PUBLISHER", "EXPORTER", "PHONE",
					"PHONEEXTENSION", "COUNTRYCODE", "MODELUSAGERESTRICTION", "MODELMAXTOKENS", "MODELMAXRESPONSETIME",
					"MODELUSAGEFREQUENCY" };
		}

		String editUserQuery = securityDb.getQueryUtil().createUpdatePreparedStatementString("SMSS_USER",
				columnsToUpdate, whereCol);
		PreparedStatement editUserPs = null;
		try {
			editUserPs = securityDb.getPreparedStatement(editUserQuery);
			int i = 1;
			editUserPs.setString(i++, email);
			editUserPs.setString(i++, username);
			editUserPs.setString(i++, name);
			editUserPs.setBoolean(i++, adminValue);
			editUserPs.setBoolean(i++, publisherValue);
			editUserPs.setBoolean(i++, exporterValue);
			editUserPs.setString(i++, phone);
			editUserPs.setString(i++, phoneExtension);
			editUserPs.setString(i++, countryCode);

			if (modelUsageRestriction == null || (modelUsageRestriction = modelUsageRestriction.trim()).isEmpty()) {
				editUserPs.setNull(i++, java.sql.Types.VARCHAR);
			} else {
				editUserPs.setString(i++, modelUsageRestriction);
			}
			if (modelMaxTokens == null) {
				editUserPs.setNull(i++, java.sql.Types.INTEGER);
			} else {
				editUserPs.setInt(i++, modelMaxTokens);
			}
			if (modelMaxResponseTime == null) {
				editUserPs.setNull(i++, java.sql.Types.DOUBLE);
			} else {
				editUserPs.setDouble(i++, modelMaxResponseTime);
			}
			if (modelUsageFrequency == null || (modelUsageFrequency = modelUsageFrequency.trim()).isEmpty()) {
				editUserPs.setNull(i++, java.sql.Types.VARCHAR);
			} else {
				editUserPs.setString(i++, modelUsageFrequency);
			}
			// we have these to update as well for native
			if (updatePassword) {
				editUserPs.setString(i++, newHashPass);
				editUserPs.setString(i++, newSalt);
			}
			// Where
			editUserPs.setString(i++, userId);
			editUserPs.setString(i++, type);
			editUserPs.execute();
			if (!editUserPs.getConnection().getAutoCommit()) {
				editUserPs.getConnection().commit();
			}

		} catch (Exception e) {
			classLogger.error("Failed to update user account information in the security database", e);
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, editUserPs);
		}

		/**
		 * HMM what to do about this one should i update?
		 */
		if (updatePassword) {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			try {
				SecurityNativeUserUtils.storeUserPassword(userId, type, newHashPass, newSalt, timestamp);
			} catch (Exception e) {
				classLogger.error("Failed to update user account information in the security database", e);
			}
		}
		return true;
	}

	/**
	 * Delete a user and all its relationships.
	 * 
	 * @param userIdToDelete
	 * @param userTypeToDelete
	 * @return
	 */
	public boolean deleteUser(String userIdToDelete, String userTypeToDelete) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// TODO: need to start binding on userId + type
		{
			String[] deleteQueries = new String[] { "DELETE FROM ENGINEPERMISSION WHERE USERID=?",
					"DELETE FROM PROJECTPERMISSION WHERE USERID=?", "DELETE FROM USERINSIGHTPERMISSION WHERE USERID=?",
					"DELETE FROM SMSS_USER_ACCESS_KEYS WHERE USERID=?", "DELETE FROM USERMETA WHERE USERID=?",
					"DELETE FROM SMSS_USER WHERE ID=?", };
			for (String query : deleteQueries) {
				PreparedStatement ps = null;
				try {
					ps = securityDb.getPreparedStatement(query);
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userIdToDelete);
					ps.execute();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error("Failed to delete user records from the security database", e);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		}
		{
			String[] deleteQueries = new String[] { "DELETE FROM CUSTOMGROUPASSIGNMENT WHERE USERID=? AND TYPE=?" };
			for (String query : deleteQueries) {
				PreparedStatement ps = null;
				try {
					ps = securityDb.getPreparedStatement(query);
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userIdToDelete);
					ps.setString(parameterIndex++, userTypeToDelete);
					ps.execute();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error("Failed to delete user records from the security database", e);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		}
//		{
//			String[] deleteQueries = new String[] {
//					"DELETE FROM SMSS_USER WHERE ID=? AND TYPE=?",
//					"DELETE FROM SMSS_USER_ACCESS_KEYS WHERE ID=? AND TYPE=?"
//			};
//			for(String query : deleteQueries) {
//				PreparedStatement ps = null;
//				try {
//					ps = securityDb.getPreparedStatement(query);
//					int parameterIndex = 1;
//					ps.setString(parameterIndex++, userToDelete);
//					ps.setString(parameterIndex++, type);
//					ps.execute();
//					if(!ps.getConnection().getAutoCommit()) {
//						ps.getConnection().commit();
//					}
//				} catch (SQLException e) {
//					logger.error("Failed to delete user records from the security database", e);
//				} finally {
//					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
//				}
//			}
//		}
		return true;
	}

	/**
	 * 
	 * @param userId
	 * @param userType
	 * @param newEmail
	 */
	public void updateUserEmail(String userId, String userType, String newEmail) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "UPDATE SMSS_USER SET EMAIL=? WHERE ID=? AND TYPE=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			if (newEmail == null || (newEmail = newEmail.trim()).isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, newEmail);
			}
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, userType);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to update user email", e);
			throw new IllegalArgumentException("An error occurred updating this user's email");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Set the user's publishing rights
	 * 
	 * @param userId
	 * @param isPublisher
	 */
	public void setUserPublisher(String userId, boolean isPublisher) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "UPDATE SMSS_USER SET PUBLISHER=? WHERE ID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, isPublisher);
			ps.setString(parameterIndex++, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to update user publisher privilege", e);
			throw new IllegalArgumentException("An error occurred setting this user as a publisher");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Set the user's exporting rights
	 * 
	 * @param userId
	 * @param isExporter
	 */
	public void setUserExporter(String userId, boolean isExporter) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "UPDATE SMSS_USER SET EXPORTER=? WHERE ID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, isExporter);
			ps.setString(parameterIndex++, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to update user exporter privilege", e);
			throw new IllegalArgumentException("An error occurred setting this user as an exporter");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);

		}
	}

	/**
	 * Set the user locked/unlocked
	 * 
	 * @param userId
	 * @param isExporter
	 */
	public void setUserLock(String userId, String type, boolean isLocked) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = null;
		if (isLocked) {
			query = "UPDATE SMSS_USER SET LOCKED=? WHERE ID=? AND TYPE=?";
		} else {
			query = "UPDATE SMSS_USER SET LOCKED=?, LASTLOGIN=? WHERE ID=? AND TYPE=?";
		}
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, isLocked);
			if (!isLocked) {
				// we reset the counter so lastlogin will be today
				java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
				ps.setTimestamp(parameterIndex++, timestamp);
			}
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, type);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to update user lock status", e);
			throw new IllegalArgumentException("An error occurred setting this user as locked/unlocked");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Update the user metadata. Will delete existing values and then perform a bulk
	 * insert
	 * 
	 * @param userId
	 * @param userType
	 * @param insightId
	 * @param tags
	 */
	public void updateUserMetadata(String userId, AuthProvider userType, Map<String, ?> metadata) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userTypeString = userType.toString();

		// first do a delete
		String deleteQ = "DELETE FROM USERMETA WHERE METAKEY=? AND USERID=? AND TYPE=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for (String field : metadata.keySet()) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, field);
				deletePs.setString(parameterIndex++, userId);
				deletePs.setString(parameterIndex++, userTypeString);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update user metadata", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}

		// now we do the new insert with the order of the tags
		String query = securityDb.getQueryUtil().createInsertPreparedStatementString("USERMETA",
				new String[] { "USERID", "TYPE", "METAKEY", "METAVALUE", "METAORDER" });
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for (String field : metadata.keySet()) {
				Object val = metadata.get(field);
				List<Object> values = new ArrayList<>();
				if (val instanceof Collection) {
					values.addAll((Collection<Object>) val);
				} else {
					values.add(val);
				}

				for (int i = 0; i < values.size(); i++) {
					int parameterIndex = 1;
					Object fieldVal = values.get(i);

					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, userTypeString);
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
			classLogger.error("Failed to update user metadata", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * METHODS FOR DATABASE AUTHORIZATION THAT ARE AT THE ADMIN LEVEL
	 */

	/**
	 * 
	 * @param engineFilter
	 * @param engineTypes
	 * @param engineMetadataFilter
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getAllEngineSettings(List<String> engineFilter, List<String> engineTypes,
			Map<String, Object> engineMetadataFilter, String searchTerm, String limit, String offset) {
		return getAllEngineSettings(engineFilter, engineTypes, engineMetadataFilter, searchTerm, limit, offset, null);
	}

	public List<Map<String, Object>> getAllEngineSettings(List<String> engineFilter, List<String> engineTypes,
			Map<String, Object> engineMetadataFilter, String searchTerm, String limit, String offset,
			Map<String, String> sortFields) {

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "engine_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "engine_display_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "engine_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "engine_subtype"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "engine_cost"));
		qs.addSelector(new QueryColumnSelector("ENGINE__DISCOVERABLE", "engine_discoverable"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "engine_global"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TOOL_APP", "engine_tool_app"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBY", "engine_created_by"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBYTYPE", "engine_created_by_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__DATECREATED", "engine_date_created"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "ENGINE__ENGINENAME",
				"low_engine_name"));

		// legacy alias names
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TOOL_APP", "database_tool_app"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "ENGINE__ENGINENAME",
				"low_database_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "database_global"));
		// legacy alias names
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TOOL_APP", "tool_app"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "app_global"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "app_display_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "engine_display_name"));

		if (engineFilter != null && !engineFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineFilter));
		}
		if (engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
		}
		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter("ENGINE__ENGINENAME", searchTerm));
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter("ENGINE__ENGINEID", searchTerm));
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter("ENGINE__ENGINEDISPLAYNAME", searchTerm));
			qs.addExplicitFilter(searchFilter);
		}
		// filtering by enginemeta key-value pairs (i.e. <tag>:value): for each pair,
		// add in-filter against engineids from subquery
		if (engineMetadataFilter != null && !engineMetadataFilter.isEmpty()) {
			for (String k : engineMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAVALUE", "==",
						engineMetadataFilter.get(k)));
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "==", subQs));
			}
		}

		// add the sort
		if (sortFields == null || sortFields.isEmpty()) {
			qs.addOrderBy(new QueryColumnOrderBySelector("low_engine_name"));
		} else {
			Set<String> sortKeys = sortFields.keySet();
			Set<String> validSorts = new HashSet<>(Arrays.asList("ENGINENAME", "DATECREATED"));
			if (sortFields.containsKey(null) || sortFields.containsValue(null)) {
				throw new SemossPixelException("Sort parameters cannot contain null keys or values");
			}
			if (!validSorts.containsAll(sortKeys)) {
				throw new SemossPixelException(
						"Invalid Sort Parameters passed: Only \"ENGINENAME\" and \"DATECREATED\" are supported");
			}
			for (String s : sortKeys) {
				if ("ENGINENAME".equals(s)) {
					qs.addOrderBy("low_engine_name", sortFields.getOrDefault(s, "ASC"));
				} else {
					qs.addOrderBy("ENGINE__" + s, sortFields.getOrDefault(s, "ASC"));
				}
			}
		}

		Long long_limit = -1L;
		Long long_offset = -1L;
		if (limit != null && !limit.trim().isEmpty()) {
			long_limit = Long.parseLong(limit);
		}
		if (offset != null && !offset.trim().isEmpty()) {
			long_offset = Long.parseLong(offset);
		}
		qs.setLimit(long_limit);
		qs.setOffSet(long_offset);

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * 
	 * @param projectFilter
	 * @param projectMetadataFilter
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getAllProjectSettings(List<String> projectFilter, List<String> typeFilter,
			Map<String, Object> projectMetadataFilter, String searchTerm, String limit, String offset) {
		return getAllProjectSettings(projectFilter, typeFilter, projectMetadataFilter, searchTerm, limit, offset, null);
	}

	public List<Map<String, Object>> getAllProjectSettings(List<String> projectFilter, List<String> typeFilter,
			Map<String, Object> projectMetadataFilter, String searchTerm, String limit, String offset,
			Map<String, String> sortFields) {

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		String projectPrefix = "PROJECT__";
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTDISPLAYNAME", "project_display_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "COST", "project_cost"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "GLOBAL", "project_global"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "DISCOVERABLE", "project_discoverable"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "CATALOGNAME", "project_catalog_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "CREATEDBY", "project_created_by"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "CREATEDBYTYPE", "project_created_by_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "DATECREATED", "project_date_created"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "DATELASTEDITED", "project_date_last_edited"));
		// dont forget reactors/portal information
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHED", "project_portal_published_date"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHEDUSER", "project_published_user"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHEDTYPE", "project_published_user_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "REACTORSCOMPILED", "project_reactors_compiled_date"));
		qs.addSelector(
				new QueryColumnSelector(projectPrefix + "REACTORSCOMPILEDUSER", "project_reactors_compiled_user"));
		qs.addSelector(
				new QueryColumnSelector(projectPrefix + "REACTORSCOMPILEDTYPE", "project_reactors_compiled_user_type"));
		// for sort
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "PROJECT__PROJECTNAME",
				"low_project_name"));

		if (projectFilter != null && !projectFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
		}
		if (typeFilter != null && !typeFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__TYPE", "==", typeFilter));
		}
		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTID", searchTerm));
			searchFilter.addFilter(
					securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTNAME", searchTerm));
			searchFilter.addFilter(
					securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTDISPLAYNAME", searchTerm));
			qs.addExplicitFilter(searchFilter);
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
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "==", subQs));
			}
		}

		if (sortFields == null || sortFields.isEmpty()) {
			qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
		} else {
			for (Map.Entry<String, String> sortEntry : sortFields.entrySet()) {
				String sortKey = sortEntry.getKey();
				String sortDirection = sortEntry.getValue();
				if (sortKey == null || sortDirection == null) {
					throw new SemossPixelException("Sort parameters cannot contain null keys or values");
				}
				String normalizedKey = sortKey.replace("_", "").toUpperCase();
				if ("PROJECTNAME".equals(normalizedKey)) {
					qs.addOrderBy("low_project_name", sortDirection);
				} else if ("DATECREATED".equals(normalizedKey)) {
					qs.addOrderBy(projectPrefix + "DATECREATED", sortDirection);
				} else if ("DATELASTEDITED".equals(normalizedKey)) {
					qs.addOrderBy(projectPrefix + "DATELASTEDITED", sortDirection);
				} else {
					throw new SemossPixelException(
							"Invalid Sort Parameters passed: Only \"PROJECTNAME\", \"DATECREATED\", and \"DATELASTEDITED\" are supported");
				}
			}
		}

		Long long_limit = -1L;
		Long long_offset = -1L;
		if (limit != null && !limit.trim().isEmpty()) {
			long_limit = Long.parseLong(limit);
		}
		if (offset != null && !offset.trim().isEmpty()) {
			long_offset = Long.parseLong(offset);
		}
		qs.setLimit(long_limit);
		qs.setOffSet(long_offset);

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Set if engine should be public or not
	 * 
	 * @param engineId
	 * @param global
	 */
	public boolean setEngineGlobal(String engineId, boolean global) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE ENGINE SET GLOBAL=? WHERE ENGINEID=?");
			ps.setBoolean(1, global);
			ps.setString(2, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine global visibility setting", e);
			throw new IllegalArgumentException("An error occurred setting the engine public");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}

	/**
	 * Set if the engine is discoverable to all users on this instance
	 * 
	 * @param user
	 * @param engineId
	 * @param discoverable
	 * @return
	 * @throws IllegalAccessException
	 */
	public boolean setEngineDiscoverable(String engineId, boolean discoverable) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE ENGINE SET DISCOVERABLE=? WHERE ENGINEID=?");
			ps.setBoolean(1, discoverable);
			ps.setString(2, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine discoverability setting", e);
			throw new IllegalArgumentException("An error occurred setting the engine discoverable flag");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}

	/**
	 * Set if project should be public or not
	 * 
	 * @param projectId
	 * @param isPublic
	 */
	public boolean setProjectGlobal(String projectId, boolean global) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
			throw new IllegalArgumentException("An error occurred setting the project public");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}

	/**
	 * Set if the project is discoverable to all users on this instance
	 * 
	 * @param user
	 * @param projectId
	 * @param discoverable
	 * @return
	 * @throws IllegalAccessException
	 */
	public boolean setProjectDiscoverable(String projectId, boolean discoverable) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
			throw new IllegalArgumentException("An error occurred setting the project discoverable flag");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}

	/**
	 * Get all the users for a databases
	 * 
	 * @param engineId
	 * @param userId
	 * @param permission
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getEngineUsers(String engineId, String searchParam, String permission, long limit,
			long offset) {
		return SecurityUserEngineUtils.getEngineUsers(engineId, searchParam, permission, limit, offset);
	}

	/**
	 * 
	 * @param engineId
	 * @param userId
	 * @param permission
	 * @return
	 */
	public long getEngineUsersCount(String engineId, String searchParam, String permission) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean hasSearchParam = searchParam != null && !(searchParam = searchParam.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission = permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("count");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(fSelector);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		if (hasSearchParam) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchParam));
			qs.addExplicitFilter(or);
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "==",
					AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	/**
	 * Get all the users for a project
	 * 
	 * @param projectId
	 * @return
	 */
	public List<Map<String, Object>> getProjectUsers(String projectId, String searchParam, String permission,
			long limit, long offset) {
		return SecurityUserProjectUtils.getProjectUsers(projectId, searchParam, permission, limit, offset);
	}

	public long getProjectUsersCount(String projectId, String searchParam, String permission) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
	 * @param newUserId
	 * @param engineId
	 * @param permission
	 * @param user
	 * @param endDate
	 * @param usageRestriction
	 * @param usageFrequency
	 * @param maxTokens
	 * @param maxResponseTime
	 */
	public void addEngineUser(String newUserId, String engineId, String permission, User user, String endDate,
			String usageRestriction, String usageFrequency, int maxTokens, double maxResponseTime) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// make sure user doesn't already exist for this database
		if (SecurityUserEngineUtils.getUserEnginePermission(newUserId, engineId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException(
					"This user already has access to this engine. Please edit the existing permission level.");
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		// insert new user permissions in bulk
		String insertQ = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE, USAGERESTRICTION, USAGEFREQUENCY, MAXTOKENS, MAXRESPONSETIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newUserId);
			ps.setString(parameterIndex++, engineId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission));
			ps.setBoolean(parameterIndex++, true);
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			if (usageRestriction == null || (usageRestriction = usageRestriction.trim()).isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, usageRestriction);
			}
			if (usageFrequency == null || (usageFrequency = usageFrequency.trim()).isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, usageFrequency);
			}
			if (maxTokens == 0) {
				ps.setNull(parameterIndex++, java.sql.Types.INTEGER);
			} else {
				ps.setInt(parameterIndex++, maxTokens);
			}
			if (maxResponseTime == 0.0) {
				ps.setNull(parameterIndex++, java.sql.Types.DOUBLE);
			} else {
				ps.setDouble(parameterIndex++, maxResponseTime);
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to add engine user", e);
			throw new IllegalArgumentException(
					"An error occurred adding the user permissions for this engine. Detailed error message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param engineId
	 * @param permission
	 * @param user
	 */
	public void addEngineUserPermissions(String engineId, List<Map<String, Object>> permission, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// first, check to make sure these users do not already have permissions to
		// database
		// get list of userids from permission list map
		List<String> userIds = permission.stream().map(map -> (String) map.get("userid")).collect(Collectors.toList());
		// this returns a list of existing permissions
		Map<String, Integer> existingUserPermission = SecurityUserEngineUtils.getUserEnginePermissions(userIds,
				engineId);
		if (!existingUserPermission.isEmpty()) {
			throw new IllegalArgumentException(
					"The following users already have access to this database. Please edit the existing permission level: "
							+ String.join(",", existingUserPermission.keySet()));
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// insert new user permissions in bulk
		String insertQ = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE, USAGERESTRICTION, USAGEFREQUENCY, MAXTOKENS, MAXRESPONSETIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			for (int i = 0; i < permission.size(); i++) {
				Map<String, Object> thisPermissionMap = permission.get(i);

				int parameterIndex = 1;
				ps.setString(parameterIndex++, (String) thisPermissionMap.get("userid"));
				ps.setString(parameterIndex++, engineId);
				ps.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission((String) thisPermissionMap.get("permission")));
				ps.setBoolean(parameterIndex++, true);
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setTimestamp(parameterIndex++, startDate);
				// end date for this user
				Timestamp verifiedEndDate = null;
				if (thisPermissionMap.get("endDate") != null) {
					verifiedEndDate = AbstractSecurityUtils.calculateEndDate((String) thisPermissionMap.get("endDate"));
					ps.setTimestamp(parameterIndex++, verifiedEndDate);
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
				}

				// engine usage restrictions
				if (thisPermissionMap.get("usageRestriction") != null
						&& !((String) thisPermissionMap.get("usageRestriction")).trim().isEmpty()) {
					ps.setString(parameterIndex++, ((String) thisPermissionMap.get("usageRestriction")).trim());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				}
				if (thisPermissionMap.get("usageFrequency") != null
						&& !((String) thisPermissionMap.get("usageFrequency")).trim().isEmpty()) {
					ps.setString(parameterIndex++, ((String) thisPermissionMap.get("usageFrequency")).trim());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				}
				if (thisPermissionMap.get("maxTokens") != null) {
					ps.setInt(parameterIndex++, ((Number) thisPermissionMap.get("maxTokens")).intValue());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.INTEGER);
				}
				if (thisPermissionMap.get("maxResponseTime") != null) {
					ps.setDouble(parameterIndex++, ((Number) thisPermissionMap.get("maxResponseTime")).doubleValue());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.DOUBLE);
				}

				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to add engine user permissions", e);
			throw new IllegalArgumentException(
					"An error occurred adding the user permissions for this engine. Detailed error message = "
							+ e.getMessage());
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
	public void addProjectUserPermissions(String projectId, List<Map<String, String>> permission, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// first, check to make sure these users do not already have permissions to
		// project
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
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();

		// insert new user permissions in bulk
		String insertQ = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			for (int i = 0; i < permission.size(); i++) {
				Map<String, String> thisPermissionMap = permission.get(i);

				int parameterIndex = 1;
				ps.setString(parameterIndex++, thisPermissionMap.get("userid"));
				ps.setString(parameterIndex++, projectId);
				ps.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission(thisPermissionMap.get("permission")));
				ps.setBoolean(parameterIndex++, true);
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setTimestamp(parameterIndex++, startDate);
				// end date for this user
				Timestamp verifiedEndDate = null;
				if (thisPermissionMap.get("endDate") != null) {
					verifiedEndDate = AbstractSecurityUtils.calculateEndDate(thisPermissionMap.get("endDate"));
					ps.setTimestamp(parameterIndex++, verifiedEndDate);
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
				}
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to add project user permissions", e);
			throw new IllegalArgumentException(
					"An error occurred adding the user permissions for this project. Detailed error message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param newUserId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public void addInsightUserPermissions(String projectId, String insightId, List<Map<String, String>> permission,
			User user, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// first, check to make sure these users do not already have permissions to
		// insight
		// get list of userids from permission list map
		List<String> userIds = permission.stream().map(map -> map.get("userid")).collect(Collectors.toList());
		// this returns a list of existing permissions
		Map<String, Integer> existingUserPermission = SecurityInsightUtils.getUserInsightPermissions(userIds, projectId,
				insightId);
		if (!existingUserPermission.isEmpty()) {
			throw new IllegalArgumentException(
					"The following users already have access to this insight. Please edit the existing permission level: "
							+ String.join(",", existingUserPermission.keySet()));
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// insert new user permissions in bulk
		String insertQ = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			for (int i = 0; i < permission.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, permission.get(i).get("userid"));
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission(permission.get(i).get("permission")));
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
		} catch (Exception e) {
			classLogger.error("Failed to add insight user permissions", e);
			throw new IllegalArgumentException(
					"An error occurred adding the user permissions for this insight. Detailed error message = "
							+ e.getMessage());
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
	public void addProjectUser(String newUserId, String projectId, String permission, User user, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user doesn't already exist for this project
		if (SecurityUserProjectUtils.getUserProjectPermission(newUserId, projectId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException(
					"This user already has access to this project. Please edit the existing permission level.");
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		// insert new user permissions in bulk
		String insertQ = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newUserId);
			ps.setString(parameterIndex++, projectId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission));
			ps.setBoolean(parameterIndex++, true);
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to add project user", e);
			throw new IllegalArgumentException(
					"An error occurred adding the user permissions for this project. Detailed error message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Return the databases the user has explicit access to
	 * 
	 * @param singleUserId
	 * @return
	 */
	public List<String> getProjectsUserHasExplicitAccess(String singleUserId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", singleUserId));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	/**
	 * Return the databases the user has explicit access to
	 * 
	 * @param singleUserId
	 * @return
	 */
	public Map<String, Boolean> getProjectsAndVisibilityUserHasExplicitAccess(String singleUserId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__VISIBILITY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", singleUserId));
		Map<String, Boolean> values = new HashMap<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				values.put((String) row[0], (Boolean) row[1]);
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve projects with explicit user visibility settings", e);
		}

		return values;
	}

	/**
	 * Give user permission for all the projects
	 * 
	 * @param userId     String - The user id we are providing permissions to
	 * @param permission String - The permission level for the access
	 * @param isAddNew   boolean - If false, modifying existing project permissions
	 *                   to the new permission level If true, adding new projects
	 *                   with the permission level specified
	 */
	public void grantAllProjects(String userId, String permission, boolean isAddNew, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		if (isAddNew) {
			List<String> currentProjectAccess = getProjectsUserHasExplicitAccess(userId);
			List<String> projectIds = SecurityProjectUtils.getAllProjectIds();
			String insertQuery = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, VISIBILITY, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED) VALUES(?, ?,?,?,?,?,?)";
			int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
			boolean visible = true;
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(insertQuery);
				// add new permission for projects
				for (String projectId : projectIds) {
					if (currentProjectAccess.contains(projectId)) {
						// only add for new projects, not existing projects
						continue;
					}
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, projectId);
					ps.setBoolean(parameterIndex++, visible);
					ps.setInt(parameterIndex++, permissionLevel);
					ps.setString(parameterIndex++, userDetails.getValue0());
					ps.setString(parameterIndex++, userDetails.getValue1());
					ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
					ps.addBatch();
				}
				ps.executeBatch();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to grant access to all projects", e);
				throw new IllegalArgumentException(
						"An error occurred granting the user permission for all the projects");
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		} else {
			// first grab the projects and visibility
			Map<String, Boolean> currentProjectToVisibilityMap = getProjectsAndVisibilityUserHasExplicitAccess(userId);

			// we will remove all the current permissions
			// and then re-add the ones they used to have but with the new level

			// delete first
			{
				String deleteQuery = "DELETE FROM PROJECTPERMISSION WHERE USERID=?";
				PreparedStatement ps = null;
				try {
					ps = securityDb.getPreparedStatement(deleteQuery);
					ps.setString(1, userId);
					ps.execute();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error("Failed to grant access to all projects", e);
					throw new IllegalArgumentException(
							"An error occurred granting the user permission for all the projects");
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
			// now add
			{
				// now we insert the values
				String insertQuery = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, VISIBILITY, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED) VALUES(?,?,?,?,?,?,?)";
				int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
				PreparedStatement ps = null;

				try {
					ps = securityDb.getPreparedStatement(insertQuery);
					// add new permission for all projects
					for (String projectId : currentProjectToVisibilityMap.keySet()) {
						boolean visible = currentProjectToVisibilityMap.get(projectId);
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, projectId);
						ps.setBoolean(parameterIndex++, visible);
						ps.setInt(parameterIndex++, permissionLevel);
						ps.setString(parameterIndex++, userDetails.getValue0());
						ps.setString(parameterIndex++, userDetails.getValue1());
						ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
						ps.addBatch();
					}
					ps.executeBatch();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error("Failed to grant access to all projects", e);
					throw new IllegalArgumentException(
							"An error occurred granting the user permission for all the projects");
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		}
	}

	/**
	 * Return the databases the user has explicit access to
	 * 
	 * @param singleUserId
	 * @return
	 */
	public List<String> getEnginesUserHasExplicitAccess(String singleUserId, List<String> engineTypes) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", singleUserId));
		if (engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
			qs.addRelation("ENGINEPERMISSION__ENGINEID", "ENGINE__ENGINEID", "inner.join");
		}
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	/**
	 * Return the databases the user has explicit access to
	 * 
	 * @param singleUserId
	 * @return
	 */
	public Map<String, Boolean> getEnginesAndVisibilityUserHasExplicitAccess(String singleUserId,
			List<String> engineTypes) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__VISIBILITY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", singleUserId));
		if (engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
			qs.addRelation("ENGINEPERMISSION__ENGINEID", "ENGINE__ENGINEID", "inner.join");
		}
		Map<String, Boolean> values = new HashMap<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				values.put((String) row[0], (Boolean) row[1]);
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve engines with explicit user visibility settings", e);
		}

		return values;
	}

	/**
	 * Give user permission for all the engines
	 * 
	 * @param userId      String - The user id we are providing permissions to
	 * @param permission  String - The permission level for the access
	 * @param isAddNew    boolean - If false, modifying existing project permissions
	 *                    to the new permission level If true, adding new projects
	 *                    with the permission level specified
	 * @param engineTypes
	 */
	public void grantAllEngines(String userId, String permission, boolean isAddNew, List<String> engineTypes,
			User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String logETypes = (engineTypes == null || engineTypes.isEmpty()) ? "[ALL]"
				: ("[" + String.join(", ", engineTypes) + "]");

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		if (isAddNew) {
			List<String> currentEngineAccess = getEnginesUserHasExplicitAccess(userId, engineTypes);
			List<String> engineIds = SecurityEngineUtils.getAllEngineIds();
			String insertQuery = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED) VALUES(?,?,?,?,?,?,?)";
			int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
			boolean visible = true;
			PreparedStatement ps = null;

			try {
				ps = securityDb.getPreparedStatement(insertQuery);
				// add new permission for databases
				for (String databaseId : engineIds) {
					if (currentEngineAccess.contains(databaseId)) {
						// only add for new databases, not existing databases
						continue;
					}
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, databaseId);
					ps.setBoolean(parameterIndex++, visible);
					ps.setInt(parameterIndex++, permissionLevel);
					ps.setString(parameterIndex++, userDetails.getValue0());
					ps.setString(parameterIndex++, userDetails.getValue1());
					ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
					ps.addBatch();
				}
				ps.executeBatch();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to grant access to all engines", e);
				throw new IllegalArgumentException(
						"An error occurred granting the user permission for all the engines of type " + logETypes);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		} else {
			// first grab the databases and visibility
			Map<String, Boolean> currentEngineToVisibilityMap = getEnginesAndVisibilityUserHasExplicitAccess(userId,
					engineTypes);

			// we will remove all the current permissions
			// and then re-add the ones they used to have but with the new level

			// delete first
			{
				String deleteQuery = "DELETE FROM ENGINEPERMISSION WHERE USERID=?";
				PreparedStatement ps = null;
				try {
					ps = securityDb.getPreparedStatement(deleteQuery);
					ps.setString(1, userId);
					ps.execute();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error("Failed to grant access to all engines", e);
					throw new IllegalArgumentException(
							"An error occurred granting the user permission for all the engines of type " + logETypes);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
			// now add
			{
				// now we insert the values
				String insertQuery = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED) VALUES(?,?,?,?,?,?,?)";
				int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
				PreparedStatement ps = null;

				try {
					ps = securityDb.getPreparedStatement(insertQuery);
					// add new permission for all projects
					for (String databaseId : currentEngineToVisibilityMap.keySet()) {
						boolean visible = currentEngineToVisibilityMap.get(databaseId);
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, databaseId);
						ps.setBoolean(parameterIndex++, visible);
						ps.setInt(parameterIndex++, permissionLevel);
						ps.setString(parameterIndex++, userDetails.getValue0());
						ps.setString(parameterIndex++, userDetails.getValue1());
						ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
						ps.addBatch();
					}
					ps.executeBatch();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error("Failed to grant access to all engines", e);
					throw new IllegalArgumentException(
							"An error occurred granting the user permission for all the engines of type " + logETypes);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		}
	}

	/**
	 * give new users access to a database
	 * 
	 * @param engineId
	 * @param permission
	 * @param user
	 * @param endDate
	 */
	public void grantNewUsersEngineAccess(String engineId, String permission, User user, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			// get users with no access to app
			List<Map<String, Object>> users = getEngineUsersNoCredentials(engineId, null, -1, -1);
			for (Map<String, Object> userMap : users) {
				String userId = (String) userMap.get("id");
				int parameterIndex = 1;
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, engineId);
				ps.setInt(parameterIndex++, permissionLevel);
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
		} catch (SQLException e) {
			classLogger.error("Failed to grant default engine access to new users", e);
			throw new IllegalArgumentException("An error occurred adding user permissions for engine " + engineId
					+ " with permission " + permission);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param projectId
	 * @param permission
	 * @param user
	 * @param endDate
	 */
	public void grantNewUsersProjectAccess(String projectId, String permission, User user, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		String query = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			// get users with no access to project
			List<Map<String, Object>> users = getProjectUsersNoCredentials(projectId, null, -1, -1);
			for (Map<String, Object> userMap : users) {
				String userId = (String) userMap.get("id");
				int parameterIndex = 1;
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, projectId);
				ps.setInt(parameterIndex++, permissionLevel);
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
		} catch (SQLException e) {
			classLogger.error("Failed to grant default project access to new users", e);
			throw new IllegalArgumentException("An error occurred adding user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Give the user permission for all the insights in a project
	 * 
	 * @param projectId
	 * @param userId
	 * @param permission
	 */
	public void grantAllProjectInsights(String projectId, String userId, String permission, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// delete all previous permissions for the user
		String deleteQuery = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID=? AND PROJECTID=?";
		String insertQuery = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED) VALUES(?,?,?,?,?,?,?)";

		PreparedStatement deletePs = null;
		PreparedStatement insertPs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQuery);
			int parameterIndex = 1;
			deletePs.setString(parameterIndex++, userId);
			deletePs.setString(parameterIndex++, projectId);
			deletePs.execute();

			insertPs = securityDb.getPreparedStatement(insertQuery);
			// add new permission for all insights
			List<String> insightIds = getAllProjectInsights(projectId);
			for (String insightId : insightIds) {
				parameterIndex = 1;
				insertPs.setString(parameterIndex++, userId);
				insertPs.setString(parameterIndex++, projectId);
				insertPs.setString(parameterIndex++, insightId);
				insertPs.setInt(parameterIndex++, permissionLevel);
				insertPs.setString(parameterIndex++, userDetails.getValue0());
				insertPs.setString(parameterIndex++, userDetails.getValue1());
				insertPs.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
				insertPs.addBatch();
			}
			insertPs.executeBatch();

			// do commits
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
			if (!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to grant access to all project insights", e);
			throw new IllegalArgumentException("An error occurred granting the user permission for all the projects");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}
	}

	/**
	 * 
	 * @param existingUserId
	 * @param engineId
	 * @param newPermission
	 * @param user
	 * @param endDate
	 * @param maxTokens
	 * @param maxResponseTime
	 * @param usageRestriction
	 * @param usageFrequency
	 */
	public void editEngineUserPermission(String existingUserId, String engineId, String newPermission, User user,
			String endDate, String usageRestriction, String usageFrequency, int maxTokens, double maxResponseTime) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserEngineUtils.getUserEnginePermission(existingUserId, engineId);
		if (existingUserPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for a user who does not currently have access to the engine");
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		String updateQ = "UPDATE ENGINEPERMISSION SET PERMISSION = ?, PERMISSIONGRANTEDBY = ?, PERMISSIONGRANTEDBYTYPE = ?, DATEADDED = ?, ENDDATE = ?, USAGERESTRICTION=?, USAGEFREQUENCY=?, MAXTOKENS=?, MAXRESPONSETIME=? WHERE USERID = ? AND ENGINEID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int parameterIndex = 1;
			// SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			if (usageRestriction == null || (usageRestriction = usageRestriction.trim()).isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, usageRestriction);
			}
			if (usageFrequency == null || (usageFrequency = usageFrequency.trim()).isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, usageFrequency);
			}
			if (maxTokens == 0) {
				ps.setNull(parameterIndex++, java.sql.Types.INTEGER);
			} else {
				ps.setInt(parameterIndex++, maxTokens);
			}
			if (maxResponseTime == 0.0) {
				ps.setNull(parameterIndex++, java.sql.Types.DOUBLE);
			} else {
				ps.setDouble(parameterIndex++, maxResponseTime);
			}
			// WHERE
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine user permission", e);
			throw new IllegalArgumentException(
					"An error occurred updating the user permissions for this engine. Detailed error message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param engineId
	 * @param permission
	 * @param user
	 * @throws IllegalAccessException
	 */
	public void editEngineUserPermissions(String engineId, List<Map<String, Object>> permission, User user)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// get userid of all requests
		List<String> existingUserIds = new ArrayList<String>();
		for (Map<String, Object> i : permission) {
			String userId = Utility.inputSQLSanitizer((String) i.get("userid"));
			existingUserIds.add(userId);
		}

		// get user permissions to edit
		Map<String, Integer> existingUserPermission = SecurityUserEngineUtils.getUserEnginePermissions(existingUserIds,
				engineId);

		// make sure all users to edit currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for the following users who do not currently have access to the engine: "
							+ String.join(",", toRemoveUserIds));
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// update user permissions in bulk
		String updateQ = "UPDATE ENGINEPERMISSION SET PERMISSION = ?, PERMISSIONGRANTEDBY = ?, PERMISSIONGRANTEDBYTYPE = ?, DATEADDED = ?, ENDDATE = ?, USAGERESTRICTION=?, USAGEFREQUENCY=?, MAXTOKENS=?, MAXRESPONSETIME=? WHERE USERID = ? AND ENGINEID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			for (int i = 0; i < permission.size(); i++) {
				Map<String, Object> thisPermissionMap = permission.get(i);

				int parameterIndex = 1;
				// SET
				ps.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission((String) thisPermissionMap.get("permission")));
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setTimestamp(parameterIndex++, startDate);
				// end date for this user
				Timestamp verifiedEndDate = null;
				if (thisPermissionMap.get("endDate") != null) {
					verifiedEndDate = AbstractSecurityUtils.calculateEndDate((String) thisPermissionMap.get("endDate"));
					ps.setTimestamp(parameterIndex++, verifiedEndDate);
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
				}

				// engine usage restrictions
				if (thisPermissionMap.get("usageRestriction") != null
						&& !((String) thisPermissionMap.get("usageRestriction")).trim().isEmpty()) {
					ps.setString(parameterIndex++, ((String) thisPermissionMap.get("usageRestriction")).trim());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				}
				if (thisPermissionMap.get("usageFrequency") != null
						&& !((String) thisPermissionMap.get("usageFrequency")).trim().isEmpty()) {
					ps.setString(parameterIndex++, ((String) thisPermissionMap.get("usageFrequency")).trim());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				}
				if (thisPermissionMap.get("maxTokens") != null) {
					ps.setInt(parameterIndex++, ((Number) thisPermissionMap.get("maxTokens")).intValue());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.INTEGER);
				}
				if (thisPermissionMap.get("maxResponseTime") != null) {
					ps.setDouble(parameterIndex++, ((Number) thisPermissionMap.get("maxResponseTime")).doubleValue());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.DOUBLE);
				}

				// WHERE
				ps.setString(parameterIndex++, (String) thisPermissionMap.get("userid"));
				ps.setString(parameterIndex++, engineId);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine user permissions", e);
			throw new IllegalArgumentException(
					"An error occurred updating the user permissions for this engine. Detailed error message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param existingUserId
	 * @param projectId
	 * @param newPermission
	 * @return
	 */
	public void editProjectUserPermission(String existingUserId, String projectId, String newPermission, User user,
			String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserProjectUtils.getUserProjectPermission(existingUserId, projectId);
		if (existingUserPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for a user who does not currently have access to the project");
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// update user permissions in bulk
		String updateQ = "UPDATE PROJECTPERMISSION SET PERMISSION = ?, PERMISSIONGRANTEDBY = ?, PERMISSIONGRANTEDBYTYPE = ?, DATEADDED = ?, ENDDATE = ? WHERE USERID = ? AND PROJECTID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int parameterIndex = 1;
			// SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
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
		} catch (Exception e) {
			classLogger.error("Failed to update project user permission", e);
			throw new IllegalArgumentException(
					"An error occurred updating the user permissions for this project. Detailed error message = "
							+ e.getMessage());
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
	 * @param user
	 * @param endDate
	 * @return
	 * @throws IllegalAccessException
	 */
	public void editProjectUserPermissions(String projectId, List<Map<String, String>> requests, User user,
			String endDate) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
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

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// update user permissions in bulk
		String updateQ = "UPDATE PROJECTPERMISSION SET PERMISSION = ?, PERMISSIONGRANTEDBY = ?, PERMISSIONGRANTEDBYTYPE = ?, DATEADDED = ?, ENDDATE = ? WHERE USERID = ? AND PROJECTID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			for (int i = 0; i < requests.size(); i++) {
				int parameterIndex = 1;
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
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project user permissions", e);
			throw new IllegalArgumentException(
					"An error occurred updating the user permissions for this project. Detailed error message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Edit insight user permissions in bulk
	 * 
	 * @param projectId
	 * @param insightId
	 * @param requests
	 * @param user
	 * @param endDate
	 * @throws IllegalAccessException
	 */
	public void editInsightUserPermissions(String projectId, String insightId, List<Map<String, String>> requests,
			User user, String endDate) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// get userid of all requests
		List<String> existingUserIds = new ArrayList<String>();
		for (Map<String, String> i : requests) {
			existingUserIds.add(i.get("userid"));
		}

		// get user permissions to edit
		Map<String, Integer> existingUserPermission = SecurityInsightUtils.getUserInsightPermissions(existingUserIds,
				projectId, insightId);

		// make sure all users to edit currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for the following users who do not currently have access to the insight: "
							+ String.join(",", toRemoveUserIds));
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// update user permissions in bulk
		String updateQ = "UPDATE USERINSIGHTPERMISSION SET PERMISSION = ?, PERMISSIONGRANTEDBY = ?, PERMISSIONGRANTEDBYTYPE = ?, DATEADDED = ?, ENDDATE = ? WHERE USERID = ? AND PROJECTID = ? AND INSIGHTID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			for (int i = 0; i < requests.size(); i++) {
				int parameterIndex = 1;
				// SET
				ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setTimestamp(parameterIndex++, startDate);
				ps.setTimestamp(parameterIndex++, verifiedEndDate);
				// WHERE
				ps.setString(parameterIndex++, requests.get(i).get("userid"));
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update insight user permissions", e);
			throw new IllegalArgumentException(
					"An error occurred updating the user permissions for this insight. Detailed error message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param editedUserId
	 * @param engineId
	 * @return
	 */
	public void removeEngineUser(String existingUserId, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserEngineUtils.getUserEnginePermission(existingUserId, engineId);
		if (existingUserPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for a user who does not currently have access to the engine");
		}

		String deleteQ = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to remove engine user", e);
			throw new IllegalArgumentException("An error occurred removing the users access to this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param editedUserId
	 * @param engineId
	 * @return
	 */
	public void removeEngineUsers(List<String> existingUserIds, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, Integer> existingUserPermission = SecurityUserEngineUtils.getUserEnginePermissions(existingUserIds,
				engineId);

		// make sure these users all exist and have access
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for the following users who do not currently have access to the engine: "
							+ String.join(",", toRemoveUserIds));
		}

		String deleteQ = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			for (int i = 0; i < existingUserIds.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, existingUserIds.get(i));
				ps.setString(parameterIndex++, engineId);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to remove engine users", e);
			throw new IllegalArgumentException("An error occurred removing user permissions from this engine");
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
	public void removeProjectUsers(List<String> existingUserIds, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(existingUserIds,
				projectId);

		// make sure these users all exist and have access
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for the following users who do not currently have access to the project: "
							+ String.join(",", toRemoveUserIds));
		}

		String deleteQ = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
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
			throw new IllegalArgumentException("An error occurred removing user permissions from this project");
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
	public void removeInsightUsers(List<String> existingUserIds, String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, Integer> existingUserPermission = SecurityInsightUtils.getUserInsightPermissions(existingUserIds,
				projectId, insightId);
		// make sure these users all exist and have access
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for the following users who do not currently have access to the insight: "
							+ String.join(",", toRemoveUserIds));
		}
		String deleteQ = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID=? AND PROJECTID=? AND INSIGHTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			for (int i = 0; i < existingUserIds.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, existingUserIds.get(i));
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to remove insight users", e);
			throw new IllegalArgumentException("An error occurred removing insight permissions for this project");
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
	public void removeProjectUser(String existingUserId, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserProjectUtils.getUserProjectPermission(existingUserId, projectId);
		if (existingUserPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for a user who does not currently have access to the project");
		}

		String deleteQ = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to remove project user", e);
			throw new IllegalArgumentException("An error occurred removing user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * METHODS FOR INSIGHT AUTHORIZATION THAT ARE AT THE ADMIN LEVEL
	 */

	/**
	 * 
	 * @param projectId
	 * @return
	 */
	public List<Map<String, Object>> getProjectInsights(String projectId) {

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "project_insight_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "exec_count"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		qs.addOrderBy(new QueryColumnOrderBySelector("INSIGHT__INSIGHTNAME"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * @param projectId
	 * @param insightIds
	 * @throws Exception
	 */
	public void deleteProjectInsights(String projectId, List<String> insightIds) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		IProject project = Utility.getProject(projectId);
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());

		// delete from insights database
		admin.dropInsight(insightIds);

		// delete from the security database
		String insightFilters = createFilter(insightIds);
		// TODO:
		// TODO:
		// TODO:
		// TODO:
		String query = "DELETE FROM INSIGHT WHERE INSIGHTID " + insightFilters + " AND PROJECTID='" + projectId + "';";
		query += "DELETE FROM USERINSIGHTPERMISSION  WHERE INSIGHTID " + insightFilters + " AND PROJECTID='" + projectId
				+ "'";
		securityDb.insertData(query);
		securityDb.commit();
	}

	/**
	 * Retrieve the list of users for a given insight
	 * 
	 * @param projectId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public List<Map<String, Object>> getInsightUsers(String projectId, String insightId, String userId,
			String permission, long limit, long offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean hasUserId = userId != null && !(userId = userId.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission = permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		if (hasUserId) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "?like", userId));
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION", "==",
					AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "USERINSIGHTPERMISSION", "inner.join");
		qs.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("PERMISSION__ID"));
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
	 * 
	 * @param projectId
	 * @param insightId
	 * @param userId
	 * @param permission
	 * @return
	 */
	public long getInsightUsersCount(String projectId, String insightId, String userId, String permission) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean hasUserId = userId != null && !(userId = userId.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission = permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("count");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(fSelector);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		if (hasUserId) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "?like", userId));
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION", "==",
					AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "USERINSIGHTPERMISSION", "inner.join");
		qs.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "inner.join");
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	/**
	 * 
	 * @param newUserId
	 * @param projectId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public void addInsightUser(String newUserId, String projectId, String insightId, String permission, User user,
			String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user doesn't already exist for this insight
		if (SecurityInsightUtils.getUserInsightPermission(newUserId, projectId, insightId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException(
					"This user already has access to this insight. Please edit the existing permission level.");
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		String insertQ = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newUserId);
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission));
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to add insight user", e);
			throw new IllegalArgumentException("An error occurred adding user permissions for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Add all users to an insight with permission level
	 * 
	 * @param projectId
	 * @param insightId
	 * @param permission
	 * @param user
	 * @param endDate
	 * @return
	 */
	public void addAllInsightUsers(String projectId, String insightId, String permission, User user, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		String inertQ = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(inertQ);
			if (projectId != null && permission != null) {
				List<Map<String, Object>> users = getInsightUsersNoCredentials(projectId, insightId, null, -1, -1);
				for (Map<String, Object> userMap : users) {
					String userId = (String) userMap.get("id");
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, projectId);
					ps.setString(parameterIndex++, insightId);
					ps.setInt(parameterIndex++, permissionLevel);
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
				// update existing permissions for users
				updateInsightUserPermissions(projectId, insightId, permission, user, null);
			}
		} catch (Exception e) {
			classLogger.error("Failed to add all insight users", e);
			throw new IllegalArgumentException("An error occurred adding all users for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param existingUserId
	 * @param projectId
	 * @param insightId
	 * @param newPermission
	 * @return
	 */
	public void editInsightUserPermission(String existingUserId, String projectId, String insightId,
			String newPermission, User user, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityInsightUtils.getUserInsightPermission(existingUserId, projectId,
				insightId);
		if (existingUserPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for a user who does not currently have access to the insight");
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		// update user permissions in bulk
		String updateQ = "UPDATE USERINSIGHTPERMISSION SET PERMISSION = ?, PERMISSIONGRANTEDBY = ?, PERMISSIONGRANTEDBYTYPE = ?, DATEADDED = ?, ENDDATE = ? WHERE USERID = ? AND PROJECTID = ? AND INSIGHTID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int parameterIndex = 1;
			// SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			// WHERE
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update insight user permission", e);
			throw new IllegalArgumentException("An error occurred adding user permissions for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param editedUserId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public void removeInsightUser(String existingUserId, String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityInsightUtils.getUserInsightPermission(existingUserId, projectId,
				insightId);
		if (existingUserPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for a user who does not currently have access to the insight");
		}

		// update user permissions in bulk
		String deleteQ = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID = ? AND PROJECTID = ? AND INSIGHTID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			int parameterIndex = 1;
			// WHERE
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to remove insight user", e);
			throw new IllegalArgumentException("An error occurred deleting user permissions for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param projectId
	 * @param isPublic
	 */
	public void setInsightGlobalWithinProject(String projectId, String insightId, boolean isPublic) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// update user permissions in bulk
		String updateQ = "UPDATE INSIGHT SET GLOBAL=? WHERE PROJECTID=? AND INSIGHTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int parameterIndex = 1;
			// SET
			ps.setBoolean(parameterIndex++, isPublic);
			// WHERE
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update insight global within project", e);
			throw new IllegalArgumentException("An error occurred setting this insight global");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Returns List of users that have no access credentials to a given engine
	 * 
	 * @param engineId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getEngineUsersNoCredentials(String engineId, String searchTerm, long limit,
			long offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "!=", null,
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
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Returns List of users that have no access credentials to a given project
	 * 
	 * @param projectId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getProjectUsersNoCredentials(String projectId, String searchTerm, long limit,
			long offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Returns List of users that have no access credentials to a given insight
	 * 
	 * @param projectId
	 * @param insightId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getInsightUsersNoCredentials(String projectId, String insightId, String searchTerm,
			long limit, long offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME", "username"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		// Filter for sub-query
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("SMSS_USER__ID", "!=", subQs));
			// Sub-query itself
			subQs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__USERID"));
			subQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
			subQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION", "!=",
					null, PixelDataType.NULL_VALUE));
		}

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * 
	 * @param engineId
	 * @param newPermission
	 * @param user
	 * @param endDate
	 */
	public void updateEngineUserPermissions(String engineId, String newPermission, User user, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		String query = "UPDATE ENGINEPERMISSION SET PERMISSION=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=?, DATEADDED=?, ENDDATE=? WHERE ENGINEID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			// SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			// WHERE
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine user permissions", e);
			throw new IllegalArgumentException("An error occurred editing user permissions for this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param projectId
	 * @param newPermission
	 * @param user
	 * @param endDate
	 */
	public void updateProjectUserPermissions(String projectId, String newPermission, User user, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		String query = "UPDATE PROJECTPERMISSION SET PERMISSION=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=?, DATEADDED=?, ENDDATE=? WHERE PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			// SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			// WHERE
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update project user permissions", e);
			throw new IllegalArgumentException("An error occurred editing user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Add all users to an database with the same permission
	 * 
	 * @param engineId
	 * @param permission
	 * @param user
	 * @param endDate
	 */
	public void addAllEngineUsers(String engineId, String permission, User user, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			if (engineId != null && permission != null) {
				List<Map<String, Object>> users = getEngineUsersNoCredentials(engineId, null, -1, -1);
				for (Map<String, Object> userMap : users) {
					String userId = (String) userMap.get("id");
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, engineId);
					ps.setInt(parameterIndex++, permissionLevel);
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
				// update existing user permissions
				updateEngineUserPermissions(engineId, permission, user, endDate);
			}
		} catch (SQLException e1) {
			classLogger.error("Failed to add all engine users", e1);
			throw new IllegalArgumentException("An error occurred adding all users to this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Add all users to an project with the same permission
	 * 
	 * @param projectId
	 * @param permission
	 * @param user
	 * @param endDate
	 */
	public void addAllProjectUsers(String projectId, String permission, User user, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		String query = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			if (projectId != null && permission != null) {
				List<Map<String, Object>> users = getProjectUsersNoCredentials(projectId, null, -1, -1);
				for (Map<String, Object> userMap : users) {
					String userId = (String) userMap.get("id");
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, projectId);
					ps.setInt(parameterIndex++, permissionLevel);
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
				// update existing user permissions
				updateProjectUserPermissions(projectId, permission, user, endDate);
			}
		} catch (SQLException e1) {
			classLogger.error("Failed to add all project users", e1);
			throw new IllegalArgumentException("An error occurred adding user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param projectId
	 * @param insightId
	 * @param newPermission
	 * @param user
	 * @param endDate
	 */
	public void updateInsightUserPermissions(String projectId, String insightId, String newPermission, User user,
			String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		String updateQ = "UPDATE USERINSIGHTPERMISSION SET PERMISSION=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=?, DATEADDED=?, ENDDATE=? WHERE PROJECTID=? AND INSIGHTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int parameterIndex = 1;
			// SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			// WHERE
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update insight user permissions", e);
			throw new IllegalArgumentException("An error occurred updating the permissions for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param projectId
	 * @return
	 */
	private List<String> getAllProjectInsights(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	/**
	 * 
	 * @param projectId
	 * @param insightId
	 * @param permission
	 * @param user
	 * @param endDate
	 */
	public void grantNewUsersInsightAccess(String projectId, String insightId, String permission, User user,
			String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		List<Map<String, Object>> users = getInsightUsersNoCredentials(projectId, insightId, null, -1, -1);
		String insertQuery = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQuery);
			for (Map<String, Object> userMap : users) {
				String userId = (String) userMap.get("id");
				int parameterIndex = 1;
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.setInt(parameterIndex++, permissionLevel);
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
		} catch (SQLException e) {
			classLogger.error("Failed to grant default insight access to new users", e);
			throw new IllegalArgumentException("An error occurred granting the user permission for all the projects");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Lock accounts
	 * 
	 * @param numDaysSinceLastLogin
	 */
	public int lockAccounts(int numDaysSinceLastLogin) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int numUpdated = 0;

		ZonedDateTime dateToFilter = ZonedDateTime.now(ZoneId.of("UTC"));
		dateToFilter = dateToFilter.minusDays(numDaysSinceLastLogin);

		String query = "UPDATE SMSS_USER SET LOCKED=? WHERE LASTLOGIN<=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, true);
			ps.setTimestamp(parameterIndex++, Utility.getSqlTimestampUTC(dateToFilter));
			numUpdated = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to lock user accounts that exceed the inactivity threshold", e);
			throw new IllegalArgumentException("An error occurred granting the user permission for all the projects");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		classLogger.info("Number of accounts locked = {}", numUpdated);
		return numUpdated;
	}

	/**
	 * Lock accounts
	 * 
	 * @param numDaysSinceLastLogin
	 */
	public List<Object[]> getUserEmailsGettingLocked() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// if we never lock - nothing to worry about
		int daysToLock = -1;
		int daysToLockEmail = 14;
		try {
			PasswordRequirements passReqInstance = PasswordRequirements.getInstance();
			daysToLock = passReqInstance.getDaysToLock();
			daysToLockEmail = passReqInstance.getDaysToLockEmail();
		} catch (Exception e) {
			classLogger.error("Failed to retrieve user emails for lock warning notifications", e);
		}
		if (daysToLock < 0) {
			return new ArrayList<>();
		}
		int daysSinceLastLoginToSendEmail = (daysToLock - daysToLockEmail);
		if (daysSinceLastLoginToSendEmail < 0) {
			classLogger.warn(
					"Days to Lock is less than the Days To Lock Email Warning. Would result in constant emails. Returning empty set until configured properly");
			return new ArrayList<>();
		}

		LocalDateTime now = LocalDateTime.now();

		List<Object[]> emailsToSend = new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__LASTLOGIN"));
		List<Boolean> values = new ArrayList<>();
		values.add(null);
		values.add(false);
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("SMSS_USER__LOCKED", "==", values, PixelDataType.BOOLEAN));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String email = (String) row[0];
				if (email != null) {
					SemossDate lastLogin = null;
					if (row[1] != null) {
						Object potentialDateValue = row[1];
						if (potentialDateValue instanceof SemossDate) {
							lastLogin = (SemossDate) potentialDateValue;
						} else if (potentialDateValue instanceof String) {
							lastLogin = SemossDate.genTimeStampDateObj(potentialDateValue + "");
						}
					}

					long daysSinceLastLogin = Duration.between(lastLogin.getLocalDateTime(), now).toDays();
					if (daysSinceLastLogin >= daysSinceLastLoginToSendEmail) {
						emailsToSend.add(new Object[] { email, daysSinceLastLogin });
					}
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve user emails for lock warning notifications", e);
		}

		/*
		 * Sadly, the below does work with sqlite since it is a dumb db and doesn't
		 * store dates properly as one would expect
		 */

//		AbstractSqlQueryUtil queryUtil = securityDb.getQueryUtil();
//		String dateDiff = queryUtil.getDateDiffFunctionSyntax("day", "SMSS_USER.LASTLOGIN", queryUtil.getCurrentSqlTimestampUTC());
//
//		String query = "SELECT DISTINCT SMSS_USER.EMAIL, (" + dateDiff + ") as DAYS_SINCE_LASTLOGIN FROM SMSS_USER WHERE "
//				+ "(LOCKED IS NULL OR LOCKED='false') AND (" + dateDiff + ") > " + (daysToLock - daysToLockEmail); 
//		
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//			while(wrapper.hasNext()) {
//				Object[] row = wrapper.next().getValues();
//				if(row[0] != null) {
//					emailsToSend.add(row);
//				}
//			}
//		} catch (Exception e) {
//			logger.error("Failed to retrieve user emails for lock warning notifications", e);
//		} finally {
//			if(wrapper != null) {
//				wrapper.cleanUp();
//			}
//		}

		return emailsToSend;
	}

	/**
	 * Lock accounts
	 * 
	 * @param numDaysSinceLastLogin
	 */
	public int setLockAccountsAndRecalculate(int numDaysSinceLastLogin) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int numUpdated = 0;

		ZonedDateTime dateToFilter = ZonedDateTime.now(ZoneId.of("UTC"));
		dateToFilter = dateToFilter.minusDays(numDaysSinceLastLogin);
		java.sql.Timestamp sqlTimestamp = Utility.getSqlTimestampUTC(dateToFilter);

		String[] queries = new String[] { "UPDATE SMSS_USER SET LOCKED=? WHERE LASTLOGIN<=?",
				"UPDATE SMSS_USER SET LOCKED=? WHERE LASTLOGIN>?" };
		boolean[] queryUpdateBool = new boolean[] { true, false };

		for (int i = 0; i < queries.length; i++) {
			String query = queries[i];
			boolean updateBool = queryUpdateBool[i];
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(query);
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, updateBool);
				ps.setTimestamp(parameterIndex++, sqlTimestamp);
				numUpdated = ps.executeUpdate();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to apply account lock settings and recalculate lock status", e);
				throw new IllegalArgumentException(
						"An error occurred granting the user permission for all the projects");
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}

		classLogger.info("Number of accounts locked = {}", numUpdated);
		return numUpdated;
	}

	/**
	 * Return the number of admins
	 * 
	 * @return
	 */
	public int getNumAdmins() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.COUNT);
		fun.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(fun);
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return ((Number) wrapper.next().getValues()[0]).intValue();
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve the number of admin users", e);
		}

		return 0;
	}

	/**
	 * 
	 * @return
	 */
	public Object[] getAdminUserIdAndType() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return wrapper.next().getValues();
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve admin user identifiers and types", e);
		}

		return null;
	}

	/**
	 * Approving user access requests and giving user access in permissions
	 * 
	 * @param userId
	 * @param userType
	 * @param engineId
	 * @param requests
	 * @param endDate
	 */
	public void approveEngineUserAccessRequests(String userId, String userType, String engineId,
			List<Map<String, Object>> requests, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		// bulk delete
		String deleteQ = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for (int i = 0; i < requests.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				deletePs.setString(parameterIndex++, engineId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve engine user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while deleting enginepermission with detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for (int i = 0; i < requests.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, engineId);
				insertPs.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				insertPs.setBoolean(parameterIndex++, true);
				insertPs.setString(parameterIndex++, userId);
				insertPs.setString(parameterIndex++, userType);
				insertPs.setTimestamp(parameterIndex++, startDate);
				insertPs.setTimestamp(parameterIndex++, verifiedEndDate);
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if (!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve engine user access requests", e);
			throw new IllegalArgumentException("An error occurred editing user permissions for this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}

		// now we do the new bulk update to engineaccessrequest table
		String updateQ = "UPDATE ENGINEACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ? AND ENGINEID = ?";
		PreparedStatement updatePs = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			updatePs = securityDb.getPreparedStatement(updateQ);
			for (int i = 0; i < requests.size(); i++) {
				int index = 1;
				// set
				updatePs.setInt(index++,
						AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp);
				// where
				updatePs.setString(index++, (String) requests.get(i).get("requestid"));
				updatePs.setString(index++, engineId);
				updatePs.addBatch();
			}
			updatePs.executeBatch();
			if (!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve engine user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, updatePs);
		}
	}

	/**
	 * Denying user access requests to engine
	 * 
	 * @param userId
	 * @param userType
	 * @param engineId
	 * @param requests
	 */
	public void denyEngineUserAccessRequests(String userId, String userType, String engineId, List<String> requestIds) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// bulk update to databaseaccessrequest table
		String updateQ = "UPDATE ENGINEACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ? AND ENGINEID = ?";
		PreparedStatement ps = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			ps = securityDb.getPreparedStatement(updateQ);
			for (int i = 0; i < requestIds.size(); i++) {
				int index = 1;
				// set
				ps.setString(index++, userId);
				ps.setString(index++, userType);
				ps.setString(index++, "DENIED");
				ps.setTimestamp(index++, timestamp);
				// where
				ps.setString(index++, requestIds.get(i));
				ps.setString(index++, engineId);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to deny engine user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Approving user access requests and giving user access in permissions
	 * 
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param requests
	 * @param endDate
	 */
	public void approveProjectUserAccessRequests(String userId, String userType, String projectId,
			List<Map<String, Object>> requests, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
				deletePs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
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
				insertPs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, projectId);
				insertPs.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				insertPs.setBoolean(parameterIndex++, true);
				insertPs.setString(parameterIndex++, userId);
				insertPs.setString(parameterIndex++, userType);
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
			throw new IllegalArgumentException(
					"An error occurred inserting user project permissions on request approval");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}

		// now we do the new bulk update to projectaccessrequest table
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement updatePs = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			updatePs = securityDb.getPreparedStatement(updateQ);
			for (int i = 0; i < requests.size(); i++) {
				int index = 1;
				// set
				updatePs.setInt(index++,
						AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp);
				// where
				updatePs.setString(index++, (String) requests.get(i).get("requestid"));
				updatePs.addBatch();
			}
			updatePs.executeBatch();
			if (!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
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
	public void denyProjectUserAccessRequests(String userId, String userType, String projectId,
			List<String> RequestIdList) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// bulk update to projectaccessrequest table
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement ps = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			ps = securityDb.getPreparedStatement(updateQ);
			for (int i = 0; i < RequestIdList.size(); i++) {
				int index = 1;
				// set
				ps.setString(index++, userId);
				ps.setString(index++, userType);
				ps.setString(index++, "DENIED");
				ps.setTimestamp(index++, timestamp);
				// where
				ps.setString(index++, RequestIdList.get(i));
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
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
	 * Approving user access requests and giving user access in permissions
	 * 
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param insightId
	 * @param requests
	 * @param endDate
	 */
	public void approveInsightUserAccessRequests(String userId, String userType, String projectId, String insightId,
			List<Map<String, Object>> requests, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		// bulk delete
		String deleteQ = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID=? AND PROJECTID=? AND INSIGHTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for (int i = 0; i < requests.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				deletePs.setString(parameterIndex++, projectId);
				deletePs.setString(parameterIndex++, insightId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve insight user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while deleting projectpermission with detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for (int i = 0; i < requests.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, projectId);
				insertPs.setString(parameterIndex++, insightId);
				insertPs.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				insertPs.setString(parameterIndex++, userId);
				insertPs.setString(parameterIndex++, userType);
				insertPs.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
				insertPs.setTimestamp(parameterIndex++, verifiedEndDate);
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if (!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve insight user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred inserting user insight permissions on request approval");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}

		// now we do the new bulk update to accessrequest table
		String updateQ = "UPDATE INSIGHTACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement updatePs = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			updatePs = securityDb.getPreparedStatement(updateQ);
			for (int i = 0; i < requests.size(); i++) {
				int index = 1;
				// set
				updatePs.setInt(index++,
						AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp);
				// where
				updatePs.setString(index++, (String) requests.get(i).get("requestid"));
				updatePs.addBatch();
			}
			updatePs.executeBatch();
			if (!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve insight user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, updatePs);
		}
	}

	/**
	 * Denying user access requests
	 * 
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param insightId
	 * @param requests
	 */
	public void denyInsightUserAccessRequests(String userId, String userType, String projectId, String insightId,
			List<String> RequestIdList) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// bulk update to accessrequest table
		String updateQ = "UPDATE INSIGHTACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement ps = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			ps = securityDb.getPreparedStatement(updateQ);
			for (int i = 0; i < RequestIdList.size(); i++) {
				int index = 1;
				// set
				ps.setString(index++, userId);
				ps.setString(index++, userType);
				ps.setString(index++, "DENIED");
				ps.setTimestamp(index++, timestamp);
				// where
				ps.setString(index++, RequestIdList.get(i));
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to deny insight user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Get markdown for a given project
	 * 
	 * @param projectId
	 * @return
	 */
	public String getProjectMarkdown(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", Constants.MARKDOWN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__PROJECTID", "==", projectId));
		return QueryExecutionUtility.flushToString(securityDb, qs);
	}

	/**
	 * Get markdown for a given engine
	 * 
	 * @param user
	 * @param engineId
	 * @return
	 */
	public String getEngineMarkdown(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", Constants.MARKDOWN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__ENGINEID", "==", engineId));
		return QueryExecutionUtility.flushToString(securityDb, qs);
	}

	/**
	 * 
	 * @return
	 */
	public Long getNumUsers() {
		return getNumUsers(null);
	}

	/**
	 * 
	 * @param searchTerm
	 * @return
	 * @throws IllegalArgumentException
	 */
	public Long getNumUsers(String searchTerm) throws IllegalArgumentException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		final String SMSS_USER_PREFIX = "SMSS_USER__";
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, SMSS_USER_PREFIX + "ID",
				"num_users"));
		if (hasSearchTerm) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "ID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "NAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "USERNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "EMAIL", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	/**
	 * 
	 * @return
	 */
	public Long getNumAPIUsers() {
		return getNumAPIUsers(null);
	}

	/**
	 * 
	 * @param searchTerm
	 * @return
	 * @throws IllegalArgumentException
	 */
	public Long getNumAPIUsers(String searchTerm) throws IllegalArgumentException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		final String SMSS_USER_PREFIX = "SMSS_USER__";
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, SMSS_USER_PREFIX + "ID",
				"num_users"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "TYPE", "==",
				AuthProvider.API_USER.toString()));
		if (hasSearchTerm) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "ID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "NAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "USERNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_PREFIX + "EMAIL", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

}
