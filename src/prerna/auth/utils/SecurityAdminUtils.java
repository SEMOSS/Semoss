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
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.PasswordRequirements;
import prerna.auth.User;
import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionErrorRules;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionProblem;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
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
		qs.addSelector(
				QueryFunctionSelector.makeCol2ValCoalesceSelector("USER_PREFERENCES__FAVORITE", 0, "insight_favorite"));

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
		// add a join to get the user permission level
		{
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector(userInsightPrefix + "INSIGHTID", "INSIGHTID"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					userInsightPrefix + "PERMISSION", "PERMISSION"));
			qs2.addGroupBy(new QueryColumnSelector(userInsightPrefix + "INSIGHTID", "INSIGHTID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userInsightPrefix + "USERID", "==", userIds));
			IRelation subQuery = null;
			subQuery = new SubqueryRelationship(qs2, "INSIGHT_USER_PERMISSIONS", "left.outer.join",
					new String[] { "INSIGHT_USER_PERMISSIONS__INSIGHTID", insightPrefix + "INSIGHTID", "=" });
			qs.addRelation(subQuery);
		}
		// add a join to get if favorite
		qs.addRelation(SecurityInsightUtils.getUserInsightPreferencesSubquery(securityDb, user));
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

		// remove hidden projects and insights
		qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", "!=",
				getHiddenIdsQs(user, "USERPROJECTPREFERENCE", "PROJECTID")));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery(insightPrefix + "INSIGHTID", "!=",
				getHiddenIdsQs(user, "USERINSIGHTPREFERENCE", "INSIGHTID")));

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
		try {
			Boolean boundAdminValue = adminValue;
			Boolean boundPublisherValue = publisherValue;
			Boolean boundExporterValue = exporterValue;
			String boundPhone = phone;
			Integer boundModelMaxTokens = modelMaxTokens;
			Double boundModelMaxResponseTime = modelMaxResponseTime;
			String boundNewSalt = newSalt;
			String boundNewHashPass = newHashPass;
			QueryExecutionUtility.executeUpdate(securityDb, editUserQuery, editUserPs -> {
				int i = 1;
				editUserPs.setString(i++, email);
				editUserPs.setString(i++, username);
				editUserPs.setString(i++, name);
				editUserPs.setBoolean(i++, boundAdminValue);
				editUserPs.setBoolean(i++, boundPublisherValue);
				editUserPs.setBoolean(i++, boundExporterValue);
				editUserPs.setString(i++, boundPhone);
				editUserPs.setString(i++, phoneExtension);
				editUserPs.setString(i++, countryCode);

				securityDb.getQueryUtil().setStringEmptyAsNullable(editUserPs, i++,
						StringUtils.trim(modelUsageRestriction));
				if (boundModelMaxTokens == null) {
					editUserPs.setNull(i++, java.sql.Types.INTEGER);
				} else {
					editUserPs.setInt(i++, boundModelMaxTokens);
				}
				if (boundModelMaxResponseTime == null) {
					editUserPs.setNull(i++, java.sql.Types.DOUBLE);
				} else {
					editUserPs.setDouble(i++, boundModelMaxResponseTime);
				}
				securityDb.getQueryUtil().setStringEmptyAsNullable(editUserPs, i++,
						StringUtils.trim(modelUsageFrequency));
				// we have these to update as well for native
				if (updatePassword) {
					editUserPs.setString(i++, boundNewHashPass);
					editUserPs.setString(i++, boundNewSalt);
				}
				// Where
				editUserPs.setString(i++, userId);
				editUserPs.setString(i++, type);
			});
		} catch (Exception e) {
			classLogger.error("Failed to update user account information in the security database", e);
			throw new IllegalArgumentException(e.getMessage());
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
				try {
					String boundQuery = query;
					QueryExecutionUtility.executeUpdate(securityDb, boundQuery, ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userIdToDelete);
					});
				} catch (RuntimeException e) {
					throw e;
				} catch (Exception e) {
					classLogger.error("Failed to delete user records from the security database", e);
				}
			}
		}
		{
			String[] deleteQueries = new String[] { "DELETE FROM CUSTOMGROUPASSIGNMENT WHERE USERID=? AND TYPE=?",
					"DELETE FROM GROUPMANAGERS WHERE USERID=? AND TYPE=?",
					"DELETE FROM USERENGINEPREFERENCE WHERE USERID=? AND TYPE=?",
					"DELETE FROM USERPROJECTPREFERENCE WHERE USERID=? AND TYPE=?",
					"DELETE FROM USERINSIGHTPREFERENCE WHERE USERID=? AND TYPE=?",
					"DELETE FROM ENGINEACCESSREQUEST WHERE REQUEST_USERID=? AND REQUEST_TYPE=?",
					"DELETE FROM PROJECTACCESSREQUEST WHERE REQUEST_USERID=? AND REQUEST_TYPE=?", };
			for (String query : deleteQueries) {
				try {
					String boundQuery = query;
					QueryExecutionUtility.executeUpdate(securityDb, boundQuery, ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userIdToDelete);
						ps.setString(parameterIndex++, userTypeToDelete);
					});
				} catch (RuntimeException e) {
					throw e;
				} catch (Exception e) {
					classLogger.error("Failed to delete user records from the security database", e);
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
		try {
			String boundQuery = query;
			QueryExecutionUtility.executeUpdate(securityDb, boundQuery, ps -> {
				int parameterIndex = 1;
				securityDb.getQueryUtil().setStringEmptyAsNullable(ps, parameterIndex++, StringUtils.trim(newEmail));
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, userType);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update user email", e);
			throw new IllegalArgumentException("An error occurred updating this user's email");
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
		try {
			String boundQuery = query;
			QueryExecutionUtility.executeUpdate(securityDb, boundQuery, ps -> {
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, isPublisher);
				ps.setString(parameterIndex++, userId);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update user publisher privilege", e);
			throw new IllegalArgumentException("An error occurred setting this user as a publisher");
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
		try {
			String boundQuery = query;
			QueryExecutionUtility.executeUpdate(securityDb, boundQuery, ps -> {
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, isExporter);
				ps.setString(parameterIndex++, userId);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update user exporter privilege", e);
			throw new IllegalArgumentException("An error occurred setting this user as an exporter");
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
		try {
			String boundQuery = query;
			QueryExecutionUtility.executeUpdate(securityDb, boundQuery, ps -> {
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, isLocked);
				if (!isLocked) {
					// we reset the counter so lastlogin will be today
					java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
					ps.setTimestamp(parameterIndex++, timestamp);
				}
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, type);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update user lock status", e);
			throw new IllegalArgumentException("An error occurred setting this user as locked/unlocked");
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
		try {
			QueryExecutionUtility.write(securityDb, connection -> {
				String userTypeString = userType.toString();

				// first do a delete
				String deleteQ = "DELETE FROM USERMETA WHERE METAKEY=? AND USERID=? AND TYPE=?";

				QueryExecutionUtility.executeBatch(connection, deleteQ, metadata.keySet(), (deletePs, field) -> {
					int parameterIndex = 1;
					deletePs.setString(parameterIndex++, field);
					deletePs.setString(parameterIndex++, userId);
					deletePs.setString(parameterIndex++, userTypeString);
				});

				// now we do the new insert with the order of the tags
				String query = securityDb.getQueryUtil().createInsertPreparedStatementString("USERMETA",
						new String[] { "USERID", "TYPE", "METAKEY", "METAVALUE", "METAORDER" });

				QueryExecutionUtility.executeBatch(connection, query, ps -> {
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
				});
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to update user metadata", e);
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

	/**
	 * 
	 * @param engineFilter
	 * @param engineTypes
	 * @param engineMetadataFilter
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @param sortFields
	 * @return
	 */
	public List<Map<String, Object>> getAllEngineSettings(List<String> engineFilter, List<String> engineTypes,
			Map<String, Object> engineMetadataFilter, String searchTerm, String limit, String offset,
			Map<String, String> sortFields) {
		return getAllEngineSettings(engineFilter, engineTypes, engineMetadataFilter, searchTerm, limit, offset,
				sortFields, null, null, null);
	}

	/**
	 * Get every engine on the server, for an admin.
	 *
	 * @param engineFilter         keep only these engine ids; null or empty for
	 *                             every engine
	 * @param engineTypes          keep only the engines of these types; null or
	 *                             empty for every type
	 * @param engineMetadataFilter keep only the engines whose metadata holds each
	 *                             of these key and value pairs; null or empty for
	 *                             no filter
	 * @param searchTerm           keep only the engines whose id, name, or display
	 *                             name matches; null or blank for no search
	 * @param limit                the most engines to return; null or empty for no
	 *                             limit
	 * @param offset               how many engines to skip; null or empty for none
	 * @param sortFields           sort keys and directions: {@code ENGINENAME} or
	 *                             {@code DATECREATED} mapped to {@code ASC} or
	 *                             {@code DESC}; null or empty to sort by name
	 * @param permissionUser       the id of the user whose grants
	 *                             {@code effectivePermissions} checks; when set,
	 *                             each row also reports that user's direct grant as
	 *                             {@code permission_user_permission} and their
	 *                             custom groups' best grant as
	 *                             {@code permission_user_group_permission}
	 * @param effectivePermissions keep only the engines on which
	 *                             {@code permissionUser} holds one of these
	 *                             {@link prerna.auth.AccessPermissionEnum} ids,
	 *                             counting direct and custom group grants (identity
	 *                             provider groups are known only while that user is
	 *                             signed in), with a global engine counting as read
	 *                             only; null or empty for no filter
	 * @param createdBy            keep only the engines created by one of these
	 *                             (login id, login type) pairs, as
	 *                             {@link User#getUserIdAndType(User)} returns them;
	 *                             null or empty for no filter
	 * @return one map per engine, with its details
	 * @throws IllegalArgumentException when {@code effectivePermissions} is given
	 *                                  without {@code permissionUser}, or holds a
	 *                                  level that is not an
	 *                                  {@link prerna.auth.AccessPermissionEnum} id
	 */
	public List<Map<String, Object>> getAllEngineSettings(List<String> engineFilter, List<String> engineTypes,
			Map<String, Object> engineMetadataFilter, String searchTerm, String limit, String offset,
			Map<String, String> sortFields, String permissionUser, Collection<Integer> effectivePermissions,
			Collection<Pair<String, String>> createdBy) {

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
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "app_global"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "engine_display_name"));

		if (engineFilter != null && !engineFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineFilter));
		}
		if (engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
		}
		addPermissionUserFilters(qs, "ENGINE__ENGINEID", "ENGINE__GLOBAL", "ENGINEPERMISSION", "GROUPENGINEPERMISSION",
				"ENGINEID", permissionUser, effectivePermissions);
		if (createdBy != null && !createdBy.isEmpty()) {
			qs.addExplicitFilter(getCreatedByFilter("ENGINE__CREATEDBY", "ENGINE__CREATEDBYTYPE", createdBy));
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
	/**
	 * Join one user's grants onto an admin resource list and, when levels are
	 * given, keep only the resources on which that user holds one of them. The
	 * user's direct grant and their custom groups' best grant are joined as
	 * {@code permission_user_permission} and
	 * {@code permission_user_group_permission}. Identity provider groups are only
	 * known while that user is signed in, so they are not counted. Nothing is
	 * joined without a permission user.
	 *
	 * @param qs                   the admin list query
	 * @param resourceIdCol        the resource id column, such as
	 *                             {@code PROJECT__PROJECTID}
	 * @param globalCol            the resource's global flag, such as
	 *                             {@code PROJECT__GLOBAL}
	 * @param permissionTable      the user grant table, such as
	 *                             {@code PROJECTPERMISSION}
	 * @param groupPermissionTable the group grant table, such as
	 *                             {@code GROUPPROJECTPERMISSION}
	 * @param idColumn             the resource id column in both grant tables, such
	 *                             as {@code PROJECTID}
	 * @param permissionUser       the user whose grants to check; may be null
	 * @param effectivePermissions the levels to keep; null or empty to only join
	 * @throws IllegalArgumentException when levels are given without a permission
	 *                                  user
	 */
	private static void addPermissionUserFilters(SelectQueryStruct qs, String resourceIdCol, String globalCol,
			String permissionTable, String groupPermissionTable, String idColumn, String permissionUser,
			Collection<Integer> effectivePermissions) {
		boolean hasUser = permissionUser != null && !permissionUser.trim().isEmpty();
		boolean hasLevels = effectivePermissions != null && !effectivePermissions.isEmpty();
		if (!hasUser) {
			if (hasLevels) {
				throw new IllegalArgumentException("A permission filter needs the user whose permission it checks");
			}
			return;
		}
		String userId = Utility.inputSQLSanitizer(permissionUser.trim());
		String userAlias = "PERMISSION_USER_GRANTS";
		String groupAlias = "PERMISSION_USER_GROUP_GRANTS";

		// the user's best direct grant per resource
		{
			SelectQueryStruct userQs = new SelectQueryStruct();
			userQs.addSelector(new QueryColumnSelector(permissionTable + "__" + idColumn, idColumn));
			userQs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					permissionTable + "__PERMISSION", "PERMISSION"));
			userQs.addGroupBy(new QueryColumnSelector(permissionTable + "__" + idColumn, idColumn));
			userQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(permissionTable + "__USERID", "==", userId));
			qs.addRelation(new SubqueryRelationship(userQs, userAlias, "left.outer.join",
					new String[] { userAlias + "__" + idColumn, resourceIdCol, "=" }));
		}

		// the best grant of the custom groups the user belongs to, per resource
		{
			SelectQueryStruct membershipQs = new SelectQueryStruct();
			membershipQs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__GROUPID"));
			membershipQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__USERID", "==", userId));

			SelectQueryStruct groupQs = new SelectQueryStruct();
			groupQs.addSelector(new QueryColumnSelector(groupPermissionTable + "__" + idColumn, idColumn));
			groupQs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					groupPermissionTable + "__PERMISSION", "PERMISSION"));
			groupQs.addGroupBy(new QueryColumnSelector(groupPermissionTable + "__" + idColumn, idColumn));
			groupQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(groupPermissionTable + "__TYPE", "==", "CUSTOM"));
			groupQs.addExplicitFilter(
					SimpleQueryFilter.makeColToSubQuery(groupPermissionTable + "__ID", "==", membershipQs));
			qs.addRelation(new SubqueryRelationship(groupQs, groupAlias, "left.outer.join",
					new String[] { groupAlias + "__" + idColumn, resourceIdCol, "=" }));
		}

		qs.addSelector(new QueryColumnSelector(userAlias + "__PERMISSION", "permission_user_permission"));
		qs.addSelector(new QueryColumnSelector(groupAlias + "__PERMISSION", "permission_user_group_permission"));

		if (hasLevels) {
			qs.addExplicitFilter(getEffectivePermissionFilter(userAlias + "__PERMISSION", groupAlias + "__PERMISSION",
					globalCol, effectivePermissions));
		}
	}

	public List<Map<String, Object>> getAllProjectSettings(List<String> projectFilter, List<String> typeFilter,
			Map<String, Object> projectMetadataFilter, String searchTerm, String limit, String offset) {
		return getAllProjectSettings(projectFilter, typeFilter, projectMetadataFilter, searchTerm, limit, offset, null);
	}

	public List<Map<String, Object>> getAllProjectSettings(List<String> projectFilter, List<String> typeFilter,
			Map<String, Object> projectMetadataFilter, String searchTerm, String limit, String offset,
			Map<String, String> sortFields) {
		return getAllProjectSettings(projectFilter, typeFilter, projectMetadataFilter, searchTerm, limit, offset,
				sortFields, null, null, null);
	}

	/**
	 * Get every project on the server, for an admin.
	 *
	 * @param projectFilter         keep only these project ids; null or empty for
	 *                              every project
	 * @param typeFilter            keep only the projects of these types; null or
	 *                              empty for every type
	 * @param projectMetadataFilter keep only the projects whose metadata holds each
	 *                              of these key and value pairs; null or empty for
	 *                              no filter
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
	 * @param permissionUser        the id of the user whose grants
	 *                              {@code effectivePermissions} checks; when set,
	 *                              each row also reports that user's direct grant
	 *                              as {@code permission_user_permission} and their
	 *                              custom groups' best grant as
	 *                              {@code permission_user_group_permission}
	 * @param effectivePermissions  keep only the projects on which
	 *                              {@code permissionUser} holds one of these
	 *                              {@link prerna.auth.AccessPermissionEnum} ids,
	 *                              counting direct and custom group grants
	 *                              (identity provider groups are known only while
	 *                              that user is signed in), with a global project
	 *                              counting as read only; null or empty for no
	 *                              filter
	 * @param createdBy             keep only the projects created by one of these
	 *                              (login id, login type) pairs, as
	 *                              {@link User#getUserIdAndType(User)} returns
	 *                              them; null or empty for no filter
	 * @return one map per project, with its details
	 * @throws IllegalArgumentException when {@code effectivePermissions} is given
	 *                                  without {@code permissionUser}, or holds a
	 *                                  level that is not an
	 *                                  {@link prerna.auth.AccessPermissionEnum} id
	 */
	public List<Map<String, Object>> getAllProjectSettings(List<String> projectFilter, List<String> typeFilter,
			Map<String, Object> projectMetadataFilter, String searchTerm, String limit, String offset,
			Map<String, String> sortFields, String permissionUser, Collection<Integer> effectivePermissions,
			Collection<Pair<String, String>> createdBy) {

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
		qs.addSelector(new QueryColumnSelector(projectPrefix + "IS_TEMPLATE", "project_is_template"));
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
		addPermissionUserFilters(qs, "PROJECT__PROJECTID", "PROJECT__GLOBAL", "PROJECTPERMISSION",
				"GROUPPROJECTPERMISSION", "PROJECTID", permissionUser, effectivePermissions);
		if (createdBy != null && !createdBy.isEmpty()) {
			qs.addExplicitFilter(getCreatedByFilter("PROJECT__CREATEDBY", "PROJECT__CREATEDBYTYPE", createdBy));
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
		try {
			QueryExecutionUtility.executeUpdate(securityDb, "UPDATE ENGINE SET GLOBAL=? WHERE ENGINEID=?", ps -> {
				ps.setBoolean(1, global);
				ps.setString(2, engineId);
			});
		} catch (Exception e) {
			classLogger.error("Failed to update engine global visibility setting", e);
			throw new IllegalArgumentException("An error occurred setting the engine public");
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
		try {
			QueryExecutionUtility.executeUpdate(securityDb, "UPDATE ENGINE SET DISCOVERABLE=? WHERE ENGINEID=?", ps -> {
				ps.setBoolean(1, discoverable);
				ps.setString(2, engineId);
			});
		} catch (Exception e) {
			classLogger.error("Failed to update engine discoverability setting", e);
			throw new IllegalArgumentException("An error occurred setting the engine discoverable flag");
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
		try {
			QueryExecutionUtility.executeUpdate(securityDb, "UPDATE PROJECT SET GLOBAL=? WHERE PROJECTID=?", ps -> {
				ps.setBoolean(1, global);
				ps.setString(2, projectId);
			});
		} catch (Exception e) {
			classLogger.error("Failed to update project global visibility setting", e);
			throw new IllegalArgumentException("An error occurred setting the project public");
		}
		return true;
	}

	/**
	 * Set whether viewers may clone a project as a template.
	 *
	 * @param projectId  project identifier
	 * @param isTemplate whether the project is a template
	 * @return {@code true} when the flag is updated
	 */
	public boolean setProjectTemplate(String projectId, boolean isTemplate) {
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
		try {
			QueryExecutionUtility.executeUpdate(securityDb, "UPDATE PROJECT SET DISCOVERABLE=? WHERE PROJECTID=?",
					ps -> {
						ps.setBoolean(1, discoverable);
						ps.setString(2, projectId);
					});
		} catch (Exception e) {
			classLogger.error("Failed to update project discoverability setting", e);
			throw new IllegalArgumentException("An error occurred setting the project discoverable flag");
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
	 * Gives a person access to an engine in admin mode, as a permission batch.
	 * Limits of 0 are no limit.
	 *
	 * @param newUserId        the person
	 * @param engineId         the engine
	 * @param permission       the level, by name; anything else is Read-Only
	 * @param user             the admin
	 * @param endDate          when the access ends, or null
	 * @param usageRestriction a model engine's usage restriction, or null
	 * @param usageFrequency   how often a model engine's limits reset, or null
	 * @param maxTokens        a model engine's token limit, or 0 for none
	 * @param maxResponseTime  a model engine's response time limit, or 0 for none
	 */
	public void addEngineUser(String newUserId, String engineId, String permission, User user, String endDate,
			String usageRestriction, String usageFrequency, int maxTokens, double maxResponseTime) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.engine(engineId),
				List.of(PermissionChange.add(PermissionSubject.ofUser(newUserId, null),
						new PermissionGrant(permission, endDate)
								.withUsageLimits(usageRestriction, usageFrequency, maxTokens, maxResponseTime))),
				PermissionBatchOptions.adminMode().zeroLimitsAsNone(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.ALREADY_HAS_ACCESS,
						"This user already has access to this engine. Please edit the existing permission level.")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred adding the user permissions for this engine"));
	}

	/**
	 * Gives many people access to an engine together in admin mode, as an access
	 * batch. Each entry has {@code userid}, {@code permission}, and optionally
	 * {@code type}, {@code endDate} and the model limits.
	 *
	 * @param engineId   the engine
	 * @param permission the people, their levels, end dates and limits
	 * @param user       the admin
	 */
	public void addEngineUserPermissions(String engineId, List<Map<String, Object>> permission, User user) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.engine(engineId),
				PermissionBatchUtils.engineChanges(PermissionChange.Action.ADD, permission),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgumentWithIds(PermissionProblem.Code.ALREADY_HAS_ACCESS,
						"The following users already have access to this database. Please edit the existing permission level: ")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred adding the user permissions for this engine"));
	}

	/**
	 * Gives many people access to a project together in admin mode, as an access
	 * batch. Each entry has {@code userid}, {@code permission}, and optionally
	 * {@code type} and {@code endDate}.
	 *
	 * @param projectId  the project
	 * @param permission the people, their levels and end dates
	 * @param user       the admin
	 */
	public void addProjectUserPermissions(String projectId, List<Map<String, String>> permission, User user) {
		// each row has its own end date
		List<PermissionChange> changes = new ArrayList<>();
		for (Map<String, String> entry : permission) {
			changes.add(PermissionChange.add(PermissionSubject.ofUser(entry.get("userid"), entry.get("type")),
					new PermissionGrant(entry.get("permission"), entry.get("endDate"))));
		}
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.project(projectId), changes,
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgumentWithIds(PermissionProblem.Code.ALREADY_HAS_ACCESS,
						"The following users already have access to this project. Please edit the existing permission level: ")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred adding the user permissions for this project"));
	}

	/**
	 * Gives many people access to an insight together in admin mode, as an access
	 * batch. Each entry has {@code userid} and {@code permission}; every row gets
	 * the end date given here.
	 *
	 * @param projectId  the project the insight belongs to
	 * @param insightId  the insight
	 * @param permission the people and their levels
	 * @param user       the admin
	 * @param endDate    when the access ends, or null
	 */
	public void addInsightUserPermissions(String projectId, String insightId, List<Map<String, String>> permission,
			User user, String endDate) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.insight(projectId, insightId),
				PermissionBatchUtils.peopleChanges(PermissionChange.Action.ADD, permission, endDate),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgumentWithIds(PermissionProblem.Code.ALREADY_HAS_ACCESS,
						"The following users already have access to this insight. Please edit the existing permission level: ")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred adding the user permissions for this insight"));
	}

	/**
	 * Gives a person access to a project in admin mode, as a permission batch.
	 *
	 * @param newUserId  the person
	 * @param projectId  the project
	 * @param permission the level, by name; anything else is Read-Only
	 * @param user       the admin
	 * @param endDate    when the access ends, or null
	 */
	public void addProjectUser(String newUserId, String projectId, String permission, User user, String endDate) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.project(projectId),
				List.of(PermissionChange
						.add(PermissionSubject.ofUser(newUserId, null), new PermissionGrant(permission, endDate))),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.ALREADY_HAS_ACCESS,
						"This user already has access to this project. Please edit the existing permission level.")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred adding the user permissions for this project"));
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
	 * Gives a person one level on every project in admin mode, as permission
	 * batches. With {@code isAddNew}, they get it on every project they have no
	 * access to; otherwise the projects they have access to change to it, and their
	 * end dates are cleared.
	 *
	 * @param userId     the person
	 * @param permission the level, by name; anything else is Read-Only
	 * @param isAddNew   whether to add the projects they cannot use, rather than
	 *                   change the ones they can
	 * @param user       the admin
	 */
	public void grantAllProjects(String userId, String permission, boolean isAddNew, User user) {
		List<PermissionBatch> batches = new ArrayList<>();
		if (isAddNew) {
			// rows with no level are no access, so those projects are added
			Set<String> current = new HashSet<>(getProjectIdsUserCanUse(userId));
			for (String projectId : SecurityProjectUtils.getAllProjectIds()) {
				if (!current.contains(projectId)) {
					batches.add(new PermissionBatch(PermissionTargetResource.project(projectId),
							PermissionChange.forPeople(PermissionChange.Action.ADD, List.of(userId),
									new PermissionGrant(permission, null))));
				}
			}
		} else {
			for (String projectId : getProjectIdsUserCanUse(userId)) {
				batches.add(new PermissionBatch(PermissionTargetResource.project(projectId), PermissionChange.forPeople(
						PermissionChange.Action.CHANGE, List.of(userId), new PermissionGrant(permission, null))));
			}
		}
		PermissionBatchUtils.saveChangesNoCheckedException(user, batches, PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred granting the user permission for all the projects"));
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
	 * Gives a person one level on every engine of the given types in admin mode, as
	 * permission batches. With {@code isAddNew}, they get it on every such engine
	 * they have no access to; otherwise those they have access to change to it, and
	 * their end dates and model limits are cleared.
	 *
	 * @param userId      the person
	 * @param permission  the level, by name; anything else is Read-Only
	 * @param isAddNew    whether to add the engines they cannot use, rather than
	 *                    change the ones they can
	 * @param engineTypes the engine types, or null or empty for every type
	 * @param user        the admin
	 */
	public void grantAllEngines(String userId, String permission, boolean isAddNew, List<String> engineTypes,
			User user) {
		String logETypes = (engineTypes == null || engineTypes.isEmpty()) ? "[ALL]"
				: ("[" + String.join(", ", engineTypes) + "]");
		List<PermissionBatch> batches = new ArrayList<>();
		if (isAddNew) {
			// rows with no level are no access, so those engines are added
			Set<String> current = new HashSet<>(getEngineIdsUserCanUse(userId, engineTypes));
			List<String> engineIds = engineTypes == null || engineTypes.isEmpty()
					? SecurityEngineUtils.getAllEngineIds()
					: SecurityEngineUtils.getAllEngineIds(engineTypes);
			for (String engineId : engineIds) {
				if (!current.contains(engineId)) {
					batches.add(new PermissionBatch(PermissionTargetResource.engine(engineId),
							PermissionChange.forPeople(PermissionChange.Action.ADD, List.of(userId),
									new PermissionGrant(permission, null))));
				}
			}
		} else {
			for (String engineId : getEngineIdsUserCanUse(userId, engineTypes)) {
				batches.add(new PermissionBatch(PermissionTargetResource.engine(engineId), PermissionChange.forPeople(
						PermissionChange.Action.CHANGE, List.of(userId), new PermissionGrant(permission, null))));
			}
		}
		PermissionBatchUtils.saveChangesNoCheckedException(user, batches, PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred granting the user permission for all the engines of type " + logETypes));
	}

	/**
	 * Gives everyone who has no access to an engine one level on it, in admin mode,
	 * as a permission batch. It is a one time grant, not a rule for people who join
	 * later.
	 *
	 * @param engineId   the engine
	 * @param permission the level, by name; anything else is Read-Only
	 * @param user       the admin
	 * @param endDate    when the access ends, or null
	 */
	public void grantNewUsersEngineAccess(String engineId, String permission, User user, String endDate) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.engine(engineId),
				PermissionChange.forPeople(PermissionChange.Action.ADD,
						distinctIds(getEngineUsersNoCredentials(engineId, null, -1, -1)),
						new PermissionGrant(permission, endDate)),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred adding user permissions for engine " + engineId + " with permission "
								+ permission));
	}

	/**
	 * Gives everyone who has no access to a project one level on it, in admin mode,
	 * as a permission batch. It is a one time grant, not a rule for people who join
	 * later.
	 *
	 * @param projectId  the project
	 * @param permission the level, by name; anything else is Read-Only
	 * @param user       the admin
	 * @param endDate    when the access ends, or null
	 */
	public void grantNewUsersProjectAccess(String projectId, String permission, User user, String endDate) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.project(projectId),
				PermissionChange.forPeople(PermissionChange.Action.ADD,
						distinctIds(getProjectUsersNoCredentials(projectId, null, -1, -1)),
						new PermissionGrant(permission, endDate)),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred adding user permissions for this project"));
	}

	/**
	 * Gives a person one level on every insight of a project in admin mode, as
	 * permission batches, with no end date: they get it on the insights they have
	 * no access to, and the ones they have access to change to it.
	 *
	 * @param projectId  the project
	 * @param userId     the person
	 * @param permission the level, by name; anything else is Read-Only
	 * @param user       the admin
	 */
	public void grantAllProjectInsights(String projectId, String userId, String permission, User user) {
		Set<String> current = new HashSet<>(getInsightIdsUserCanUse(userId, projectId));
		List<PermissionBatch> batches = new ArrayList<>();
		for (String insightId : getAllProjectInsights(projectId)) {
			PermissionChange.Action action = current.contains(insightId) ? PermissionChange.Action.CHANGE
					: PermissionChange.Action.ADD;
			batches.add(new PermissionBatch(PermissionTargetResource.insight(projectId, insightId),
					PermissionChange.forPeople(action, List.of(userId), new PermissionGrant(permission, null))));
		}
		PermissionBatchUtils.saveChangesNoCheckedException(user, batches, PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred granting the user permission for all the projects"));
	}

	/**
	 * Gives people access to many engines in admin mode, as permission batches: one
	 * batch for each engine, all saved together in one transaction.
	 *
	 * @param permissionsByEngine for each engine, the people to give access, each
	 *                            with {@code userid}, {@code permission}, and
	 *                            optionally {@code type}, {@code endDate} and the
	 *                            model limits
	 * @param user                the admin
	 */
	public void addUsersToEngines(Map<String, List<Map<String, Object>>> permissionsByEngine, User user) {
		List<PermissionBatch> batches = new ArrayList<>();
		for (Map.Entry<String, List<Map<String, Object>>> engine : permissionsByEngine.entrySet()) {
			batches.add(new PermissionBatch(PermissionTargetResource.engine(engine.getKey()),
					PermissionBatchUtils.engineChanges(PermissionChange.Action.ADD, engine.getValue())));
		}
		PermissionBatchUtils.saveChangesNoCheckedException(user, batches, PermissionBatchOptions.adminMode(),
				new PermissionErrorRules());
	}

	/**
	 * Changes a person's access to an engine in admin mode, as a permission batch.
	 * The limits are all set, and limits of 0 are no limit.
	 *
	 * @param existingUserId   the person
	 * @param engineId         the engine
	 * @param newPermission    the level, by name; anything else is Read-Only
	 * @param user             the admin
	 * @param endDate          when the access ends, or null for never
	 * @param usageRestriction a model engine's usage restriction, or null
	 * @param usageFrequency   how often a model engine's limits reset, or null
	 * @param maxTokens        a model engine's token limit, or 0 for none
	 * @param maxResponseTime  a model engine's response time limit, or 0 for none
	 */
	public void editEngineUserPermission(String existingUserId, String engineId, String newPermission, User user,
			String endDate, String usageRestriction, String usageFrequency, int maxTokens, double maxResponseTime) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.engine(engineId),
				List.of(PermissionChange.change(PermissionSubject.ofUser(existingUserId, null),
						new PermissionGrant(newPermission, endDate)
								.withUsageLimits(usageRestriction, usageFrequency, maxTokens, maxResponseTime))),
				PermissionBatchOptions.adminMode().zeroLimitsAsNone(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for a user who does not currently have access to the engine")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred updating the user permissions for this engine"));
	}

	/**
	 * Changes many people's access to an engine together in admin mode, as an
	 * permission batch. Each entry has {@code userid}, {@code permission}, and
	 * optionally {@code type}, {@code endDate} and the model limits.
	 *
	 * @param engineId   the engine
	 * @param permission the changes
	 * @param user       the admin
	 */
	public void editEngineUserPermissions(String engineId, List<Map<String, Object>> permission, User user) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.engine(engineId),
				PermissionBatchUtils.engineChanges(PermissionChange.Action.CHANGE, permission),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgumentWithIds(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for the following users who do not currently have access to the engine: ")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred updating the user permissions for this engine"));
	}

	/**
	 * Changes a person's access to a project in admin mode, as a permission batch.
	 *
	 * @param existingUserId the person
	 * @param projectId      the project
	 * @param newPermission  the level, by name; anything else is Read-Only
	 * @param user           the admin
	 * @param endDate        when the access ends, or null for never
	 */
	public void editProjectUserPermission(String existingUserId, String projectId, String newPermission, User user,
			String endDate) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.project(projectId),
				List.of(PermissionChange.change(
						PermissionSubject.ofUser(existingUserId, null), new PermissionGrant(newPermission, endDate))),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for a user who does not currently have access to the project")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred updating the user permissions for this project"));
	}

	/**
	 * Changes many people's access to a project together in admin mode, as an
	 * permission batch. Each request has {@code userid} and {@code permission};
	 * every row gets the end date given here.
	 *
	 * @param projectId the project
	 * @param requests  the changes
	 * @param user      the admin
	 * @param endDate   when the access ends, or null for never
	 */
	public void editProjectUserPermissions(String projectId, List<Map<String, String>> requests, User user,
			String endDate) throws IllegalAccessException {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.project(projectId),
				PermissionBatchUtils.peopleChanges(PermissionChange.Action.CHANGE, requests, endDate),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgumentWithIds(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for the following users who do not currently have access to the project: ")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred updating the user permissions for this project"));
	}

	/**
	 * Changes many people's access to an insight together in admin mode, as an
	 * permission batch. Each request has {@code userid} and {@code permission};
	 * every row gets the end date given here.
	 *
	 * @param projectId the project the insight belongs to
	 * @param insightId the insight
	 * @param requests  the changes
	 * @param user      the admin
	 * @param endDate   when the access ends, or null for never
	 */
	public void editInsightUserPermissions(String projectId, String insightId, List<Map<String, String>> requests,
			User user, String endDate) throws IllegalAccessException {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.insight(projectId, insightId),
				PermissionBatchUtils.peopleChanges(PermissionChange.Action.CHANGE, requests, endDate),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgumentWithIds(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for the following users who do not currently have access to the insight: ")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred updating the user permissions for this insight"));
	}

	/**
	 * Takes a person's access to an engine away in admin mode, as a permission
	 * batch.
	 *
	 * @param existingUserId the person
	 * @param engineId       the engine
	 */
	public void removeEngineUser(String existingUserId, String engineId) {
		PermissionBatchUtils.saveChangesNoCheckedException(null, PermissionTargetResource.engine(engineId),
				List.of(PermissionChange.remove(PermissionSubject.ofUser(existingUserId, null))),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgument(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for a user who does not currently have access to the engine")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred removing the users access to this engine"));
	}

	/**
	 * Takes many people's access to an engine away together in admin mode, as an
	 * permission batch.
	 *
	 * @param existingUserIds the people
	 * @param engineId        the engine
	 */
	public void removeEngineUsers(List<String> existingUserIds, String engineId) {
		PermissionBatchUtils.saveChangesNoCheckedException(null, PermissionTargetResource.engine(engineId),
				PermissionChange.forPeople(PermissionChange.Action.REMOVE, existingUserIds, null),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgumentWithIds(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for the following users who do not currently have access to the engine: ")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred removing user permissions from this engine"));
	}

	/**
	 * Takes many people's access to a project away together in admin mode, as an
	 * permission batch.
	 *
	 * @param existingUserIds the people
	 * @param projectId       the project
	 */
	public void removeProjectUsers(List<String> existingUserIds, String projectId) {
		PermissionBatchUtils.saveChangesNoCheckedException(null, PermissionTargetResource.project(projectId),
				PermissionChange.forPeople(PermissionChange.Action.REMOVE, existingUserIds, null),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgumentWithIds(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for the following users who do not currently have access to the project: ")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred removing user permissions from this project"));
	}

	/**
	 * Takes many people's access to an insight away together in admin mode, as an
	 * permission batch.
	 *
	 * @param existingUserIds the people
	 * @param projectId       the project the insight belongs to
	 * @param insightId       the insight
	 */
	public void removeInsightUsers(List<String> existingUserIds, String projectId, String insightId) {
		PermissionBatchUtils.saveChangesNoCheckedException(null, PermissionTargetResource.insight(projectId, insightId),
				PermissionChange.forPeople(PermissionChange.Action.REMOVE, existingUserIds, null),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgumentWithIds(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for the following users who do not currently have access to the insight: ")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred removing insight permissions for this project"));
	}

	/**
	 * Takes a person's access to a project away in admin mode, as a permission
	 * batch. Their permissions on its insights stay.
	 *
	 * @param existingUserId the person
	 * @param projectId      the project
	 */
	public void removeProjectUser(String existingUserId, String projectId) {
		PermissionBatchUtils.saveChangesNoCheckedException(null, PermissionTargetResource.project(projectId),
				List.of(PermissionChange.remove(PermissionSubject.ofUser(existingUserId, null))),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgument(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for a user who does not currently have access to the project")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred removing user permissions for this project"));
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
				+ "';";
		query += "DELETE FROM USERINSIGHTPREFERENCE WHERE INSIGHTID " + insightFilters + " AND PROJECTID='" + projectId
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
	 * Gives a person access to an insight in admin mode, as a permission batch.
	 *
	 * @param newUserId  the person
	 * @param projectId  the project the insight belongs to
	 * @param insightId  the insight
	 * @param permission the level, by name; anything else is Read-Only
	 * @param user       the admin
	 * @param endDate    when the access ends, or null
	 */
	public void addInsightUser(String newUserId, String projectId, String insightId, String permission, User user,
			String endDate) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.insight(projectId, insightId),
				List.of(PermissionChange
						.add(PermissionSubject.ofUser(newUserId, null), new PermissionGrant(permission, endDate))),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.ALREADY_HAS_ACCESS,
						"This user already has access to this insight. Please edit the existing permission level.")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred adding user permissions for this insight"));
	}

	/**
	 * Gives everyone one level on an insight in admin mode, as permission batches:
	 * people with no access get it, and people with access change to it. Every row
	 * ends up with no end date. It does nothing without a project or a level.
	 *
	 * @param projectId  the project the insight belongs to
	 * @param insightId  the insight
	 * @param permission the level, by name; anything else is Read-Only
	 * @param user       the admin
	 * @param endDate    not used: the rows are left with no end date
	 */
	public void addAllInsightUsers(String projectId, String insightId, String permission, User user, String endDate) {
		if (projectId == null || permission == null) {
			return;
		}
		List<PermissionChange> changes = PermissionChange.forPeople(PermissionChange.Action.ADD,
				distinctIds(getInsightUsersNoCredentials(projectId, insightId, null, -1, -1)),
				new PermissionGrant(permission, null));
		changes.addAll(PermissionChange.forPeople(PermissionChange.Action.CHANGE,
				getInsightUserIdsWithAccess(projectId, insightId), new PermissionGrant(permission, null)));
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.insight(projectId, insightId),
				changes, PermissionBatchOptions.adminMode(), new PermissionErrorRules().illegalArgument(
						PermissionProblem.Code.WRITE_FAILED, "An error occurred adding all users for this insight"));
	}

	/**
	 * Changes a person's access to an insight in admin mode, as a permission batch.
	 *
	 * @param existingUserId the person
	 * @param projectId      the project the insight belongs to
	 * @param insightId      the insight
	 * @param newPermission  the level, by name; anything else is Read-Only
	 * @param user           the admin
	 * @param endDate        when the access ends, or null for never
	 */
	public void editInsightUserPermission(String existingUserId, String projectId, String insightId,
			String newPermission, User user, String endDate) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.insight(projectId, insightId),
				List.of(PermissionChange.change(
						PermissionSubject.ofUser(existingUserId, null), new PermissionGrant(newPermission, endDate))),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for a user who does not currently have access to the insight")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred adding user permissions for this insight"));
	}

	/**
	 * Takes a person's access to an insight away in admin mode, as a permission
	 * batch.
	 *
	 * @param existingUserId the person
	 * @param projectId      the project the insight belongs to
	 * @param insightId      the insight
	 */
	public void removeInsightUser(String existingUserId, String projectId, String insightId) {
		PermissionBatchUtils.saveChangesNoCheckedException(null, PermissionTargetResource.insight(projectId, insightId),
				List.of(PermissionChange.remove(PermissionSubject.ofUser(existingUserId, null))),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules().illegalArgument(PermissionProblem.Code.NO_ACCESS,
						"Attempting to modify user permission for a user who does not currently have access to the insight")
						.illegalArgument(PermissionProblem.Code.WRITE_FAILED,
								"An error occurred deleting user permissions for this insight"));
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
		try {
			QueryExecutionUtility.executeUpdate(securityDb, updateQ, ps -> {
				int parameterIndex = 1;
				// SET
				ps.setBoolean(parameterIndex++, isPublic);
				// WHERE
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
			});
		} catch (Exception e) {
			classLogger.error("Failed to update insight global within project", e);
			throw new IllegalArgumentException("An error occurred setting this insight global");
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
	 * Changes everyone with access to an engine to one level in admin mode, as
	 * permission batches, keeping their model limits. Rows with no level are no
	 * access, so they stay as they are.
	 *
	 * @param engineId      the engine
	 * @param newPermission the level, by name; anything else is Read-Only
	 * @param user          the admin
	 * @param endDate       when the access ends, or null for never
	 */
	public void updateEngineUserPermissions(String engineId, String newPermission, User user, String endDate) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.engine(engineId),
				PermissionChange.forPeople(PermissionChange.Action.CHANGE, getEngineUserIdsWithAccess(engineId),
						new PermissionGrant(newPermission, endDate)),
				PermissionBatchOptions.adminMode().keepingModelLimits(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred editing user permissions for this engine"));
	}

	/**
	 * Changes everyone with access to a project to one level in admin mode, as
	 * permission batches. Rows with no level are no access, so they stay as they
	 * are.
	 *
	 * @param projectId     the project
	 * @param newPermission the level, by name; anything else is Read-Only
	 * @param user          the admin
	 * @param endDate       when the access ends, or null for never
	 */
	public void updateProjectUserPermissions(String projectId, String newPermission, User user, String endDate) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.project(projectId),
				PermissionChange.forPeople(PermissionChange.Action.CHANGE, getProjectUserIdsWithAccess(projectId),
						new PermissionGrant(newPermission, endDate)),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred editing user permissions for this project"));
	}

	/**
	 * Gives everyone one level on an engine in admin mode, as permission batches:
	 * people with no access get it, and people with access change to it and keep
	 * their model limits. It does nothing without an engine or a level.
	 *
	 * @param engineId   the engine
	 * @param permission the level, by name; anything else is Read-Only
	 * @param user       the admin
	 * @param endDate    when the access ends, or null for never
	 */
	public void addAllEngineUsers(String engineId, String permission, User user, String endDate) {
		if (engineId == null || permission == null) {
			return;
		}
		List<PermissionChange> changes = PermissionChange.forPeople(PermissionChange.Action.ADD,
				distinctIds(getEngineUsersNoCredentials(engineId, null, -1, -1)),
				new PermissionGrant(permission, endDate));
		changes.addAll(PermissionChange.forPeople(PermissionChange.Action.CHANGE, getEngineUserIdsWithAccess(engineId),
				new PermissionGrant(permission, endDate)));
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.engine(engineId), changes,
				PermissionBatchOptions.adminMode().keepingModelLimits(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred adding all users to this engine"));
	}

	/**
	 * Gives everyone one level on a project in admin mode, as permission batches:
	 * people with no access get it, and people with access change to it. It does
	 * nothing without a project or a level.
	 *
	 * @param projectId  the project
	 * @param permission the level, by name; anything else is Read-Only
	 * @param user       the admin
	 * @param endDate    when the access ends, or null for never
	 */
	public void addAllProjectUsers(String projectId, String permission, User user, String endDate) {
		if (projectId == null || permission == null) {
			return;
		}
		List<PermissionChange> changes = PermissionChange.forPeople(PermissionChange.Action.ADD,
				distinctIds(getProjectUsersNoCredentials(projectId, null, -1, -1)),
				new PermissionGrant(permission, endDate));
		changes.addAll(PermissionChange.forPeople(PermissionChange.Action.CHANGE,
				getProjectUserIdsWithAccess(projectId), new PermissionGrant(permission, endDate)));
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.project(projectId), changes,
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred adding user permissions for this project"));
	}

	/**
	 * Changes everyone with access to an insight to one level in admin mode, as
	 * permission batches. Rows with no level are no access, so they stay as they
	 * are.
	 *
	 * @param projectId     the project the insight belongs to
	 * @param insightId     the insight
	 * @param newPermission the level, by name; anything else is Read-Only
	 * @param user          the admin
	 * @param endDate       when the access ends, or null for never
	 */
	public void updateInsightUserPermissions(String projectId, String insightId, String newPermission, User user,
			String endDate) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.insight(projectId, insightId),
				PermissionChange.forPeople(PermissionChange.Action.CHANGE,
						getInsightUserIdsWithAccess(projectId, insightId), new PermissionGrant(newPermission, endDate)),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred updating the permissions for this insight"));
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
	 * Gives everyone who has no access to an insight one level on it, in admin
	 * mode, as a permission batch. It is a one time grant, not a rule for people
	 * who join later.
	 *
	 * @param projectId  the project the insight belongs to
	 * @param insightId  the insight
	 * @param permission the level, by name; anything else is Read-Only
	 * @param user       the admin
	 * @param endDate    when the access ends, or null
	 */
	public void grantNewUsersInsightAccess(String projectId, String insightId, String permission, User user,
			String endDate) {
		PermissionBatchUtils.saveChangesNoCheckedException(user, PermissionTargetResource.insight(projectId, insightId),
				PermissionChange.forPeople(PermissionChange.Action.ADD,
						distinctIds(getInsightUsersNoCredentials(projectId, insightId, null, -1, -1)),
						new PermissionGrant(permission, endDate)),
				PermissionBatchOptions.adminMode(),
				new PermissionErrorRules(endDate).illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred granting the user permission for all the projects"));
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
		try {
			ZonedDateTime boundDateToFilter = dateToFilter;
			String boundQuery = query;
			numUpdated = QueryExecutionUtility.write(securityDb, connection -> {
				return QueryExecutionUtility.executeUpdate(connection, boundQuery, ps -> {
					int parameterIndex = 1;
					ps.setBoolean(parameterIndex++, true);
					ps.setTimestamp(parameterIndex++, Utility.getSqlTimestampUTC(boundDateToFilter));
				});
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to lock user accounts that exceed the inactivity threshold", e);
			throw new IllegalArgumentException("An error occurred granting the user permission for all the projects");
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
			try {
				String boundQuery = query;
				numUpdated = QueryExecutionUtility.write(securityDb, connection -> {
					return QueryExecutionUtility.executeUpdate(connection, boundQuery, ps -> {
						int parameterIndex = 1;
						ps.setBoolean(parameterIndex++, updateBool);
						ps.setTimestamp(parameterIndex++, sqlTimestamp);
					});
				});
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				classLogger.error("Failed to apply account lock settings and recalculate lock status", e);
				throw new IllegalArgumentException(
						"An error occurred granting the user permission for all the projects");
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
	 * Approves access requests in admin mode, as a permission batch: each person
	 * gets the level they asked for, and the requests are marked approved in the
	 * same save.
	 *
	 * @param userId   the approving admin's user id, who the rows say gave the
	 *                 access
	 * @param userType the approving admin's login type
	 * @param engineId the engine
	 * @param requests the requests, each with {@code requestid}, {@code userid} and
	 *                 {@code permission}
	 * @param endDate  when the access ends, or null
	 */
	public void approveEngineUserAccessRequests(String userId, String userType, String engineId,
			List<Map<String, Object>> requests, String endDate) {
		PermissionTargetResource target = PermissionTargetResource.engine(engineId);
		PermissionBatchUtils.saveChangesNoCheckedException(null, target,
				PermissionBatchUtils.approvalChanges(target, requests, endDate),
				PermissionBatchOptions.adminMode().grantedBy(userId, userType).approving(target, requests),
				PermissionErrorRules.forApprovals(endDate));
	}

	/**
	 * Denies access requests to an engine in admin mode: each is marked denied, by
	 * the admin named.
	 *
	 * @param userId   the denying admin's user id, who the rows say denied them
	 * @param userType the denying admin's login type
	 */
	public void denyEngineUserAccessRequests(String userId, String userType, String engineId, List<String> requestIds) {
		PermissionBatchUtils.denyRequestsNoCheckedException(null, PermissionTargetResource.engine(engineId), requestIds,
				PermissionBatchOptions.adminMode().grantedBy(userId, userType),
				new PermissionErrorRules().illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred while updating user access request"));
	}

	/**
	 * Approves access requests in admin mode, as a permission batch: each person
	 * gets the level they asked for, and the requests are marked approved in the
	 * same save.
	 *
	 * @param userId    the approving admin's user id, who the rows say gave the
	 *                  access
	 * @param userType  the approving admin's login type
	 * @param projectId the project
	 * @param requests  the requests, each with {@code requestid}, {@code userid}
	 *                  and {@code permission}
	 * @param endDate   when the access ends, or null
	 */
	public void approveProjectUserAccessRequests(String userId, String userType, String projectId,
			List<Map<String, Object>> requests, String endDate) {
		PermissionTargetResource target = PermissionTargetResource.project(projectId);
		PermissionBatchUtils.saveChangesNoCheckedException(null, target,
				PermissionBatchUtils.approvalChanges(target, requests, endDate),
				PermissionBatchOptions.adminMode().grantedBy(userId, userType).approving(target, requests),
				PermissionErrorRules.forApprovals(endDate));
	}

	/**
	 * Denies access requests to a project in admin mode: each is marked denied, by
	 * the admin named.
	 *
	 * @param userId   the denying admin's user id, who the rows say denied them
	 * @param userType the denying admin's login type
	 */
	public void denyProjectUserAccessRequests(String userId, String userType, String projectId,
			List<String> RequestIdList) {
		PermissionBatchUtils.denyRequestsNoCheckedException(null, PermissionTargetResource.project(projectId),
				RequestIdList, PermissionBatchOptions.adminMode().grantedBy(userId, userType),
				new PermissionErrorRules().illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred while updating user access request"));
	}

	/**
	 * Approves access requests in admin mode, as a permission batch: each person
	 * gets the level they asked for, and the requests are marked approved in the
	 * same save.
	 *
	 * @param userId    the approving admin's user id, who the rows say gave the
	 *                  access
	 * @param userType  the approving admin's login type
	 * @param projectId the project the insight belongs to
	 * @param insightId the insight
	 * @param requests  the requests, each with {@code requestid}, {@code userid}
	 *                  and {@code permission}
	 * @param endDate   when the access ends, or null
	 */
	public void approveInsightUserAccessRequests(String userId, String userType, String projectId, String insightId,
			List<Map<String, Object>> requests, String endDate) {
		PermissionTargetResource target = PermissionTargetResource.insight(projectId, insightId);
		PermissionBatchUtils.saveChangesNoCheckedException(null, target,
				PermissionBatchUtils.approvalChanges(target, requests, endDate),
				PermissionBatchOptions.adminMode().grantedBy(userId, userType).approving(target, requests),
				PermissionErrorRules.forApprovals(endDate));
	}

	/**
	 * Denies access requests to an insight in admin mode: each is marked denied, by
	 * the admin named.
	 *
	 * @param userId   the denying admin's user id, who the rows say denied them
	 * @param userType the denying admin's login type
	 */
	public void denyInsightUserAccessRequests(String userId, String userType, String projectId, String insightId,
			List<String> RequestIdList) {
		PermissionBatchUtils.denyRequestsNoCheckedException(null,
				PermissionTargetResource.insight(projectId, insightId), RequestIdList,
				PermissionBatchOptions.adminMode().grantedBy(userId, userType),
				new PermissionErrorRules().illegalArgument(PermissionProblem.Code.WRITE_FAILED,
						"An error occurred while updating user access request"));
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

	/**
	 * @param users rows with an {@code id}, one for each login of a person
	 * @return each id once, in order, since a project's, engine's or insight's
	 *         people are keyed by id alone
	 */
	private static List<String> distinctIds(List<Map<String, Object>> users) {
		Set<String> ids = new LinkedHashSet<>();
		for (Map<String, Object> row : users) {
			if (row.get("id") != null) {
				ids.add(String.valueOf(row.get("id")));
			}
		}
		return new ArrayList<>(ids);
	}

	/**
	 * @return the ids of the people with access to the project, leaving out rows
	 *         with no level
	 */
	private static List<String> getProjectUserIdsWithAccess(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "!=", null,
				PixelDataType.NULL_VALUE));
		return QueryExecutionUtility.flushToListString(SystemEngineRegistry.getSecurityDb(), qs);
	}

	/**
	 * @return the ids of the people with access to the engine, leaving out rows
	 *         with no level
	 */
	private static List<String> getEngineUserIdsWithAccess(String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "!=", null,
				PixelDataType.NULL_VALUE));
		return QueryExecutionUtility.flushToListString(SystemEngineRegistry.getSecurityDb(), qs);
	}

	/**
	 * @param engineTypes the engine types, or null or empty for every type
	 * @return the ids of the engines of those types the person has access to,
	 *         leaving out rows with no level
	 */
	private static List<String> getEngineIdsUserCanUse(String userId, List<String> engineTypes) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "!=", null,
				PixelDataType.NULL_VALUE));
		if (engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
			qs.addRelation("ENGINEPERMISSION__ENGINEID", "ENGINE__ENGINEID", "inner.join");
		}
		return QueryExecutionUtility.flushToListString(SystemEngineRegistry.getSecurityDb(), qs);
	}

	/**
	 * @return the ids of the people with access to the insight, leaving out rows
	 *         with no level
	 */
	private static List<String> getInsightUserIdsWithAccess(String projectId, String insightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__USERID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION", "!=", null,
				PixelDataType.NULL_VALUE));
		return QueryExecutionUtility.flushToListString(SystemEngineRegistry.getSecurityDb(), qs);
	}

	/**
	 * @return the ids of the project's insights the person has access to, leaving
	 *         out rows with no level
	 */
	private static List<String> getInsightIdsUserCanUse(String userId, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__INSIGHTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION", "!=", null,
				PixelDataType.NULL_VALUE));
		return QueryExecutionUtility.flushToListString(SystemEngineRegistry.getSecurityDb(), qs);
	}

	/**
	 * @return the ids of the projects the person has access to, leaving out rows
	 *         with no level
	 */
	private static List<String> getProjectIdsUserCanUse(String userId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "!=", null,
				PixelDataType.NULL_VALUE));
		return QueryExecutionUtility.flushToListString(SystemEngineRegistry.getSecurityDb(), qs);
	}
}
