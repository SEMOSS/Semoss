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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class SecurityGroupProjectUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityGroupProjectUtils.class);

	/**
	 * Determine if a group can view a project
	 * 
	 * @param user
	 * @param projectId
	 * @return
	 */
	public static boolean userGroupCanViewProject(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__TYPE"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PERMISSION", "!=", null,
				PixelDataType.CONST_INT));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for (AuthProvider login : logins) {
			AccessToken accessToken = user.getAccessToken(login);
			Collection<String> userGroups = accessToken.getUserGroups();
			String userGroupType = accessToken.getUserGroupType();
			Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
			if (userGroups.isEmpty() && userCustomGroups.isEmpty()) {
				continue;
			} else {
				// one of the logins has a group. set checker to false
				anyUserGroups = true;
			}
			if (!userCustomGroups.isEmpty()) {
				AndQueryFilter customAndFilter = new AndQueryFilter();
				customAndFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", "CUSTOM"));
				customAndFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", userCustomGroups));
				orFilter.addFilter(customAndFilter);
			}
			if (!userGroups.isEmpty()) {
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", userGroupType));
				andFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", userGroups));
				orFilter.addFilter(andFilter);
			}
		}

		if (!anyUserGroups) {
			return false;
		}

		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPPROJECTPERMISSION__PERMISSION"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Object val = values[0];
				SemossDate endDate = (SemossDate) values[1];
				if (AbstractSecurityUtils.endDateIsExpired(endDate)) {
					// Need to delete expired permission here
					String groupId = (String) values[2];
					String groupType = (String) values[3];
					removeExpiredProjectGroupPermission(groupId, groupType, projectId);
					continue;
				}
				if (val != null) {
					// actually do not care what the value is - we have a record so that means we
					// can at least view
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user's group can view project.", e);
			throw new IllegalArgumentException("Failed to retrieve existing group project permissions for user", e);
		}

		return false;
	}

	/**
	 * Determine if the group can modify the project
	 * 
	 * @param projectId
	 * @param userId
	 * @return
	 */
	public static boolean userGroupCanEditProject(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__TYPE"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for (AuthProvider login : logins) {
			AccessToken accessToken = user.getAccessToken(login);
			Collection<String> userGroups = accessToken.getUserGroups();
			String userGroupType = accessToken.getUserGroupType();
			Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
			if (userGroups.isEmpty() && userCustomGroups.isEmpty()) {
				continue;
			} else {
				// one of the logins has a group. set checker to false
				anyUserGroups = true;
			}
			if (!userCustomGroups.isEmpty()) {
				AndQueryFilter customAndFilter = new AndQueryFilter();
				customAndFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", "CUSTOM"));
				customAndFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", userCustomGroups));
				orFilter.addFilter(customAndFilter);
			}
			if (!userGroups.isEmpty()) {
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", userGroupType));
				andFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", userGroups));
				orFilter.addFilter(andFilter);
			}
		}

		if (!anyUserGroups) {
			return false;
		}

		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPPROJECTPERMISSION__PERMISSION"));
		Integer bestGroupDatabasePermission = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Object val = values[0];
				SemossDate endDate = (SemossDate) values[1];
				if (AbstractSecurityUtils.endDateIsExpired(endDate)) {
					// Need to delete expired permission here
					String groupId = (String) values[2];
					String groupType = (String) values[3];
					removeExpiredProjectGroupPermission(groupId, groupType, projectId);
					continue;
				}
				if (val != null) {
					bestGroupDatabasePermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user's group can edit project.", e);
			throw new IllegalArgumentException("Failed to retrieve existing group project permissions for user", e);
		}

		if (bestGroupDatabasePermission != null) {
			return AccessPermissionEnum.isEditor(bestGroupDatabasePermission);
		}

		return false;
	}

	/**
	 * Determine if the group is the owner of a project
	 * 
	 * @param userFilters
	 * @param projectId
	 * @return
	 */
	public static boolean userGroupIsOwner(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for (AuthProvider login : logins) {
			AccessToken accessToken = user.getAccessToken(login);
			Collection<String> userGroups = accessToken.getUserGroups();
			String userGroupType = accessToken.getUserGroupType();
			Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
			if (userGroups.isEmpty() && userCustomGroups.isEmpty()) {
				continue;
			} else {
				// one of the logins has a group. set checker to false
				anyUserGroups = true;
			}
			if (!userCustomGroups.isEmpty()) {
				AndQueryFilter customAndFilter = new AndQueryFilter();
				customAndFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", "CUSTOM"));
				customAndFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", userCustomGroups));
				orFilter.addFilter(customAndFilter);
			}
			if (!userGroups.isEmpty()) {
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", userGroupType));
				andFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", userGroups));
				orFilter.addFilter(andFilter);
			}
		}

		if (!anyUserGroups) {
			return false;
		}

		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPPROJECTPERMISSION__PERMISSION"));
		Integer bestGroupDatabasePermission = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					bestGroupDatabasePermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user group has owner-level access.", e);
			throw new IllegalArgumentException("Failed to retrieve existing group project permissions for user", e);
		}

		if (bestGroupDatabasePermission != null) {
			return AccessPermissionEnum.isOwner(bestGroupDatabasePermission);
		}

		return false;
	}

//	/**
//	 * Determine if a user can view a project including group permissions
//	 * @param user
//	 * @param projectId
//	 * @return
//	 */
//	public static boolean userCanViewProject(User user, String projectId) {
//		Integer bestUserProjectPermission = getBestProjectPermission(user, projectId);
//		return bestUserProjectPermission != null;
//	}
//
//	/**
//	 * Determine if the user can modify the project including group permissions
//	 * @param projectId
//	 * @param userId
//	 * @return
//	 */
//	public static boolean userCanEditProject(User user, String projectId) {
//		Integer bestUserProjectPermission = getBestProjectPermission(user, projectId);
//		return bestUserProjectPermission != null && AccessPermission.isEditor(bestUserProjectPermission);
//	}
//
//	/**
//	 * Determine if the user is the owner of an project including group permissions
//	 * @param userFilters
//	 * @param projectId
//	 * @return
//	 */
//	public static boolean userIsOwner(User user, String projectId) {
//		Integer bestUserProjectPermission = getBestProjectPermission(user, projectId);
//		return bestUserProjectPermission != null && AccessPermission.isOwner(bestUserProjectPermission);
//	}

	/**
	 * Determine the strongest project permission for the user/group
	 * 
	 * @param userId
	 * @param projectId
	 * @return
	 */
	public static Integer getBestProjectPermission(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// get best permission from user
		Integer bestUserProjectPermission = null;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addOrderBy(new QueryColumnOrderBySelector("PROJECTPERMISSION__PERMISSION"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					bestUserProjectPermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to determine the highest group-based project permission.", e);
			throw new IllegalArgumentException("Failed to retrieve existing project permissions for user", e);
		}

		// if they are the owner based on user, then skip the group check
		if (bestUserProjectPermission != null && AccessPermissionEnum.isOwner(bestUserProjectPermission)) {
			return bestUserProjectPermission;
		}

		// get best group permission
		Integer bestGroupProjectPermission = null;

		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		for (AuthProvider login : logins) {
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==",
					user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==",
					user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPPROJECTPERMISSION__PERMISSION"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					bestGroupProjectPermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to determine the highest group-based project permission.", e);
			throw new IllegalArgumentException("Failed to retrieve existing project permissions for user", e);
		}

		if (bestGroupProjectPermission == null && bestUserProjectPermission == null) {
			if (SecurityProjectUtils.projectIsGlobal(projectId)) {
				return AccessPermissionEnum.READ_ONLY.getId();
			}
			return null;
		} else if (bestGroupProjectPermission == null
				|| bestGroupProjectPermission.compareTo(bestUserProjectPermission) >= 0) {
			return bestUserProjectPermission;
		} else {
			return bestGroupProjectPermission;
		}
	}

	/**
	 * Create a project group permission
	 * 
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @param permission
	 * @return
	 * @throws IllegalAccessException
	 */
	public static void addProjectGroupPermission(User user, String groupId, String groupType, String projectId,
			String permission, String endDate) throws IllegalAccessException {
		addProjectGroupPermission(user, groupId, groupType, projectId, permission, endDate, null, null, null, null,
				null, null);
	}

	public static void addProjectGroupPermission(User user, String groupId, String groupType, String projectId,
			String permission, String endDate, String usageRestriction, String usageFrequency, Integer maxTokens,
			Double maxResponseTime, Integer maxInputTokens, Integer maxOutputTokens) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		if (getGroupProjectPermission(groupId, groupType, projectId) != null) {
			throw new IllegalArgumentException(
					"This group already has access to this project. Please edit the existing permission level.");
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		String resolvedUsageRestriction = resolveUsageRestriction(usageRestriction, maxTokens, maxInputTokens,
				maxOutputTokens, maxResponseTime);
		String resolvedUsageFrequency = resolvedUsageRestriction == null ? null : usageFrequency;

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO GROUPPROJECTPERMISSION (ID, TYPE, PROJECTID, PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, USAGERESTRICTION, USAGEFREQUENCY, MAXTOKENS, MAXRESPONSETIME, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission));
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			bindNullableString(ps, parameterIndex++, resolvedUsageRestriction);
			bindNullableString(ps, parameterIndex++, resolvedUsageFrequency);
			bindNullableInteger(ps, parameterIndex++, maxTokens);
			bindNullableDouble(ps, parameterIndex++, maxResponseTime);
			bindNullableInteger(ps, parameterIndex++, maxInputTokens);
			bindNullableInteger(ps, parameterIndex++, maxOutputTokens);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to determine the highest group-based project permission.", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Get the project permission for a specific group
	 * 
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @return
	 */
	public static Integer getGroupProjectPermission(String groupId, String groupType, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", groupType));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null && val instanceof Number) {
					return ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the group-based project permission.", e);
		}

		return null;
	}

	/**
	 * Modify a group project permission
	 * 
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException
	 */
	public static void editProjectGroupPermission(User user, String groupId, String groupType, String projectId,
			String newPermission, String endDate) throws IllegalAccessException {
		editProjectGroupPermission(user, groupId, groupType, projectId, newPermission, endDate, null, null, null,
				null, null, null);
	}

	public static void editProjectGroupPermission(User user, String groupId, String groupType, String projectId,
			String newPermission, String endDate, String usageRestriction, String usageFrequency, Integer maxTokens,
			Double maxResponseTime, Integer maxInputTokens, Integer maxOutputTokens) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user can edit the project
		Integer userPermissionLvl = getBestProjectPermission(user, projectId);
		if (userPermissionLvl == null || !AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupProjectPermission(groupId, groupType, projectId);
		if (existingGroupPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify group project permission for a group who does not currently have access to the project");
		}

		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);

		// if i am not an owner
		// then i need to check if i can edit this group permission
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if (AccessPermissionEnum.OWNER.getId() == existingGroupPermission) {
				throw new IllegalAccessException(
						"The user doesn't have the high enough permissions to modify this group project permission.");
			}

			// also, cannot give some owner permission if i am just an editor
			if (AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException(
						"Cannot give owner level access to this project since you are not currently an owner.");
			}
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		String resolvedUsageRestriction = resolveUsageRestriction(usageRestriction, maxTokens, maxInputTokens,
				maxOutputTokens, maxResponseTime);
		String resolvedUsageFrequency = resolvedUsageRestriction == null ? null : usageFrequency;

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"UPDATE GROUPPROJECTPERMISSION SET PERMISSION=?, DATEADDED=?, ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=?, USAGERESTRICTION=?, USAGEFREQUENCY=?, MAXTOKENS=?, MAXRESPONSETIME=?, MAX_INPUT_TOKENS=?, MAX_OUTPUT_TOKENS=? WHERE ID=? AND TYPE=? AND PROJECTID=?");
			int parameterIndex = 1;
			ps.setInt(parameterIndex++, newPermissionLvl);
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			bindNullableString(ps, parameterIndex++, resolvedUsageRestriction);
			bindNullableString(ps, parameterIndex++, resolvedUsageFrequency);
			bindNullableInteger(ps, parameterIndex++, maxTokens);
			bindNullableDouble(ps, parameterIndex++, maxResponseTime);
			bindNullableInteger(ps, parameterIndex++, maxInputTokens);
			bindNullableInteger(ps, parameterIndex++, maxOutputTokens);
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to retrieve the group-based project permission.", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void setGroupProjectTokenLimit(User user, String groupId, String groupType, String projectId,
			String usageFrequency, String existingUsageFrequency, Integer maxTokens, Double maxResponseTime,
			Integer maxInputTokens, Integer maxOutputTokens) throws IllegalAccessException {
		if (usageFrequency == null || usageFrequency.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a usageFrequency");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Integer userPermissionLvl = getBestProjectPermission(user, projectId);
		if (userPermissionLvl == null || !AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		Integer existingGroupPermission = getGroupProjectPermission(groupId, groupType, projectId);
		if (existingGroupPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify group project permission for a group who does not currently have access to the project");
		}

		String lookupFrequency = existingUsageFrequency != null && !existingUsageFrequency.trim().isEmpty()
				? existingUsageFrequency
				: usageFrequency;
		if (!lookupFrequency.equalsIgnoreCase(usageFrequency)
				&& hasGroupProjectUsageLimitRow(groupId, groupType, projectId, usageFrequency)) {
			throw new IllegalArgumentException("A team limit already exists for usageFrequency " + usageFrequency);
		}

		String resolvedUsageRestriction = resolveUsageRestriction(Constants.MODEL_TOKEN_RESTRICTION_VALUE, maxTokens,
				maxInputTokens, maxOutputTokens, maxResponseTime);
		if (resolvedUsageRestriction == null) {
			throw new IllegalArgumentException("At least one usage limit must be provided");
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		PreparedStatement ps = null;
		try {
			if (hasGroupProjectUsageLimitRow(groupId, groupType, projectId, lookupFrequency)) {
				ps = securityDb.getPreparedStatement(
						"UPDATE GROUPPROJECTPERMISSION SET USAGERESTRICTION=?, USAGEFREQUENCY=?, MAXTOKENS=?, MAXRESPONSETIME=?, MAX_INPUT_TOKENS=?, MAX_OUTPUT_TOKENS=?, DATEADDED=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND TYPE=? AND PROJECTID=? AND USAGEFREQUENCY=?");
				int parameterIndex = 1;
				bindNullableString(ps, parameterIndex++, resolvedUsageRestriction);
				bindNullableString(ps, parameterIndex++, usageFrequency);
				bindNullableInteger(ps, parameterIndex++, maxTokens);
				bindNullableDouble(ps, parameterIndex++, maxResponseTime);
				bindNullableInteger(ps, parameterIndex++, maxInputTokens);
				bindNullableInteger(ps, parameterIndex++, maxOutputTokens);
				ps.setTimestamp(parameterIndex++, startDate);
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setString(parameterIndex++, groupId);
				ps.setString(parameterIndex++, groupType);
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, lookupFrequency);
				ps.execute();
			} else {
				ps = securityDb.getPreparedStatement(
						"INSERT INTO GROUPPROJECTPERMISSION (ID, TYPE, PROJECTID, PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, USAGERESTRICTION, USAGEFREQUENCY, MAXTOKENS, MAXRESPONSETIME, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
				int parameterIndex = 1;
				ps.setString(parameterIndex++, groupId);
				ps.setString(parameterIndex++, groupType);
				ps.setString(parameterIndex++, projectId);
				ps.setInt(parameterIndex++, existingGroupPermission);
				ps.setTimestamp(parameterIndex++, startDate);
				ps.setTimestamp(parameterIndex++, null);
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				bindNullableString(ps, parameterIndex++, resolvedUsageRestriction);
				bindNullableString(ps, parameterIndex++, usageFrequency);
				bindNullableInteger(ps, parameterIndex++, maxTokens);
				bindNullableDouble(ps, parameterIndex++, maxResponseTime);
				bindNullableInteger(ps, parameterIndex++, maxInputTokens);
				bindNullableInteger(ps, parameterIndex++, maxOutputTokens);
				ps.execute();
			}
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to upsert the group-based project token limit.", e);
			throw new IllegalArgumentException("Failed to save group project token limit");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void removeGroupProjectTokenLimit(User user, String groupId, String groupType, String projectId,
			String usageFrequency) throws IllegalAccessException {
		if (usageFrequency == null || usageFrequency.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a usageFrequency");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Integer userPermissionLvl = getBestProjectPermission(user, projectId);
		if (userPermissionLvl == null || !AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		if (!hasGroupProjectUsageLimitRow(groupId, groupType, projectId, usageFrequency)) {
			return;
		}

		PreparedStatement ps = null;
		try {
			if (countGroupProjectPermissionRows(groupId, groupType, projectId) <= 1) {
				ps = securityDb.getPreparedStatement(
						"UPDATE GROUPPROJECTPERMISSION SET USAGERESTRICTION=?, USAGEFREQUENCY=?, MAXTOKENS=?, MAXRESPONSETIME=?, MAX_INPUT_TOKENS=?, MAX_OUTPUT_TOKENS=? WHERE ID=? AND TYPE=? AND PROJECTID=? AND USAGEFREQUENCY=?");
				int parameterIndex = 1;
				bindNullableString(ps, parameterIndex++, null);
				bindNullableString(ps, parameterIndex++, null);
				bindNullableInteger(ps, parameterIndex++, null);
				bindNullableDouble(ps, parameterIndex++, null);
				bindNullableInteger(ps, parameterIndex++, null);
				bindNullableInteger(ps, parameterIndex++, null);
				ps.setString(parameterIndex++, groupId);
				ps.setString(parameterIndex++, groupType);
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, usageFrequency);
				ps.execute();
			} else {
				ps = securityDb.getPreparedStatement(
						"DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=? AND PROJECTID=? AND USAGEFREQUENCY=?");
				ps.setString(1, groupId);
				ps.setString(2, groupType);
				ps.setString(3, projectId);
				ps.setString(4, usageFrequency);
				ps.execute();
			}
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to remove the group-based project token limit.", e);
			throw new IllegalArgumentException("Failed to remove group project token limit");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Delete a group project permission
	 * 
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static void removeProjectGroupPermission(User user, String groupId, String groupType, String projectId)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user can edit the project
		Integer userPermissionLvl = getBestProjectPermission(user, projectId);
		if (userPermissionLvl == null || !AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupProjectPermission(groupId, groupType, projectId);
		if (existingGroupPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify group permission for a user who does not currently have access to the project");
		}

		// if i am not an owner
		// then i need to check if i can remove this group permission
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if (AccessPermissionEnum.OWNER.getId() == existingGroupPermission) {
				throw new IllegalAccessException(
						"The user doesn't have the high enough permissions to modify this group project permission.");
			}
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb
					.getPreparedStatement("DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=? AND PROJECTID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to retrieve the group-based project permission.", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Delete a group project permission Note: Does not check to see if group
	 * permission is expired
	 * 
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static void removeExpiredProjectGroupPermission(String groupId, String groupType, String projectId)
			throws IllegalAccessException {

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupProjectPermission(groupId, groupType, projectId);
		if (existingGroupPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify group permission for a user who does not currently have access to the project");
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb
					.getPreparedStatement("DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=? AND PROJECTID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to retrieve the group-based project permission.", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Determine if a group can view a project
	 * 
	 * @param user
	 * @return
	 */
	public static List<String> getAllUserGroupProjects(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PERMISSION", "!=", null,
				PixelDataType.CONST_INT));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		for (AuthProvider login : logins) {
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==",
					user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==",
					user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilter);
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	public static List<Map<String, Object>> getApplicableGroupProjectUsagePermissions(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (user == null || projectId == null || projectId.trim().isEmpty()) {
			return new ArrayList<>();
		}

		OrQueryFilter orFilter = buildUserGroupAccessFilter(user, "GROUPPROJECTPERMISSION__ID",
				"GROUPPROJECTPERMISSION__TYPE");
		if (orFilter.getFilterList().isEmpty()) {
			return new ArrayList<>();
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ID", "groupId"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__TYPE", "groupType"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__USAGERESTRICTION",
				Constants.PROJECT_USAGE_RESTRICTION_KEY));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__USAGEFREQUENCY",
				Constants.PROJECT_USAGE_FREQUENCY_KEY));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__MAXTOKENS", Constants.PROJECT_MAX_TOKEN_KEY));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__MAXRESPONSETIME",
				Constants.PROJECT_MAX_RESPONSE_TIME_KEY));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__MAX_INPUT_TOKENS",
				Constants.PROJECT_MAX_INPUT_TOKEN_KEY));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__MAX_OUTPUT_TOKENS",
				Constants.PROJECT_MAX_OUTPUT_TOKEN_KEY));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PERMISSION", "!=", null,
				PixelDataType.CONST_INT));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__USAGEFREQUENCY", "!=", null));
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPPROJECTPERMISSION__ID"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	private static boolean hasGroupProjectUsageLimitRow(String groupId, String groupType, String projectId,
			String usageFrequency) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", groupType));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__USAGEFREQUENCY", "==", usageFrequency));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify the group-based project usage row.", e);
			return false;
		}
	}

	private static int countGroupProjectPermissionRows(String groupId, String groupType, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector countSelector = new QueryFunctionSelector();
		countSelector.setFunction(QueryFunctionHelper.COUNT);
		countSelector.setAlias("numRows");
		countSelector.addInnerSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ID"));
		qs.addSelector(countSelector);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", groupType));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object value = wrapper.next().getValues()[0];
				if (value instanceof Number) {
					return ((Number) value).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to count group-based project permission rows.", e);
		}
		return 0;
	}

	/**
	 * Get groups that have access to a project
	 * 
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getGroupsWithAccessToProject(User user, String projectId, long limit,
			long offset) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Input projectId must not be null or blank");
		}
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalAccessException("The user does not have access to view this project");
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__TYPE"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__DATEADDED"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__USAGERESTRICTION"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__USAGEFREQUENCY"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__MAXTOKENS"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__MAXRESPONSETIME"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__MAX_INPUT_TOKENS"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__MAX_OUTPUT_TOKENS"));
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPPROJECTPERMISSION__ID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get number of groups that have access to a project
	 * 
	 * @return
	 * @throws IllegalAccessException
	 */
	public static Long getNumGroupsWithAccessToProject(User user, String projectId) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Input projectId must not be null or blank");
		}
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalAccessException("The user does not have access to view this project");
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				"GROUPPROJECTPERMISSION__ID", "numGroups"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	private static OrQueryFilter buildUserGroupAccessFilter(User user, String idColumn, String typeColumn) {
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		for (AuthProvider login : logins) {
			AccessToken accessToken = user.getAccessToken(login);
			Collection<String> userGroups = accessToken.getUserGroups();
			Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
			if (!userCustomGroups.isEmpty()) {
				AndQueryFilter customAndFilter = new AndQueryFilter();
				customAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter(typeColumn, "==", "CUSTOM"));
				customAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter(idColumn, "==", userCustomGroups));
				orFilter.addFilter(customAndFilter);
			}
			if (!userGroups.isEmpty()) {
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter(typeColumn, "==", accessToken.getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(idColumn, "==", userGroups));
				orFilter.addFilter(andFilter);
			}
		}
		return orFilter;
	}

	private static String resolveUsageRestriction(String usageRestriction, Integer maxTokens, Integer maxInputTokens,
			Integer maxOutputTokens, Double maxResponseTime) {
		if (usageRestriction != null && !(usageRestriction = usageRestriction.trim()).isEmpty()) {
			return usageRestriction;
		}
		if (maxTokens != null || maxInputTokens != null || maxOutputTokens != null) {
			return Constants.MODEL_TOKEN_RESTRICTION_VALUE;
		}
		if (maxResponseTime != null) {
			return Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE;
		}
		return null;
	}

	private static void bindNullableString(PreparedStatement ps, int index, String value) throws SQLException {
		if (value == null || value.trim().isEmpty()) {
			ps.setNull(index, java.sql.Types.VARCHAR);
		} else {
			ps.setString(index, value);
		}
	}

	private static void bindNullableInteger(PreparedStatement ps, int index, Integer value) throws SQLException {
		if (value == null) {
			ps.setNull(index, java.sql.Types.INTEGER);
		} else {
			ps.setInt(index, value.intValue());
		}
	}

	private static void bindNullableDouble(PreparedStatement ps, int index, Double value) throws SQLException {
		if (value == null) {
			ps.setNull(index, java.sql.Types.DOUBLE);
		} else {
			ps.setDouble(index, value.doubleValue());
		}
	}
}
