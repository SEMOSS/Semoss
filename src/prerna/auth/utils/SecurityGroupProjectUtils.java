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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionErrorRules;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionProblem;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
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
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

public class SecurityGroupProjectUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityGroupProjectUtils.class);

	private static final String OWNERS_ONLY = "Only this project's owners can change which teams have access to it.";

	/**
	 * Only a project's owners change which groups have access to it. A group's
	 * managers decide who is in it, so attaching a group trusts them with the
	 * access. Being an admin counts for nothing here, since an admin can also act
	 * as an ordinary user; admins change it in admin mode.
	 *
	 * @param user      the user
	 * @param projectId the project
	 * @return whether the user is one of the project's owners
	 */
	public static boolean userCanManageGroupAccess(User user, String projectId) {
		return SecurityProjectUtils.userIsOwner(user, projectId);
	}

	private static void checkCanManageGroupAccess(User user, String projectId) throws IllegalAccessException {
		if (!userCanManageGroupAccess(user, projectId)) {
			throw new IllegalAccessException(OWNERS_ONLY);
		}
	}

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
	 * Gives a team access to a project, as a permission batch. Only the project's
	 * owners can.
	 *
	 * @param user       the user giving access
	 * @param groupId    the team
	 * @param groupType  the team's type
	 * @param projectId  the project
	 * @param permission the level, by name; anything else is Read-Only
	 * @param endDate    when the access ends, or null
	 * @throws IllegalAccessException when the user is not one of the project's
	 *                                owners
	 */
	public static void addProjectGroupPermission(User user, String groupId, String groupType, String projectId,
			String permission, String endDate) throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.project(projectId),
				List.of(PermissionChange.add(PermissionSubject.ofTeam(groupId, groupType),
						new PermissionGrant(permission, endDate))),
				PermissionBatchOptions.userMode().withoutTeamExistsCheck().keepingTeamType(),
				new PermissionErrorRules(endDate).illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, OWNERS_ONLY)
						.illegalArgument(PermissionProblem.ErrorCode.ALREADY_HAS_ACCESS,
								"This group already has access to this project. Please edit the existing permission level."));
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
		Integer permission = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null && val instanceof Number) {
					permission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the group-based project permission.", e);
		}

		return permission;
	}

	/**
	 * Changes a team's access to a project, as a permission batch. Only the
	 * project's owners can.
	 *
	 * @param user          the user changing the access
	 * @param groupId       the team
	 * @param groupType     the team's type
	 * @param projectId     the project
	 * @param newPermission the level, by name; anything else is Read-Only
	 * @param endDate       when the access ends, or null
	 * @throws IllegalAccessException when the user is not one of the project's
	 *                                owners
	 */
	public static void editProjectGroupPermission(User user, String groupId, String groupType, String projectId,
			String newPermission, String endDate) throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.project(projectId),
				List.of(PermissionChange.change(PermissionSubject.ofTeam(groupId, groupType),
						new PermissionGrant(newPermission, endDate))),
				PermissionBatchOptions.userMode().keepingTeamType(),
				new PermissionErrorRules(endDate).illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, OWNERS_ONLY)
						.illegalArgument(PermissionProblem.ErrorCode.NO_ACCESS,
								"Attempting to modify group project permission for a group who does not currently have access to the project"));
	}

	/**
	 * Takes a team's access to a project away, as a permission batch. Only the
	 * project's owners can.
	 *
	 * @param user      the user taking the access away
	 * @param groupId   the team
	 * @param groupType the team's type
	 * @param projectId the project
	 * @throws IllegalAccessException when the user is not one of the project's
	 *                                owners
	 */
	public static void removeProjectGroupPermission(User user, String groupId, String groupType, String projectId)
			throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.project(projectId),
				List.of(PermissionChange.remove(PermissionSubject.ofTeam(groupId, groupType))),
				PermissionBatchOptions.userMode().keepingTeamType(),
				new PermissionErrorRules().illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, OWNERS_ONLY)
						.illegalArgument(PermissionProblem.ErrorCode.NO_ACCESS,
								"Attempting to modify group permission for a user who does not currently have access to the project"));
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

		try {
			QueryExecutionUtility.executeUpdate(securityDb,
					"DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=? AND PROJECTID=?", ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, groupId);
						ps.setString(parameterIndex++, groupType);
						ps.setString(parameterIndex++, projectId);
					});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the group-based project permission.", e);
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

	/**
	 * Get groups that have access to a project
	 * 
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getGroupsWithAccessToProject(User user, String projectId, long limit,
			long offset) throws IllegalAccessException {
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Input projectId must not be null or blank");
		}
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalAccessException("The user does not have access to view this project");
		}
		return queryGroupsWithAccessToProject(projectId, limit, offset);
	}

	/**
	 * The groups that have access to a project, read without checking who asks, for
	 * callers that already checked, such as admin mode
	 *
	 * @return rows with {@code ID}, {@code TYPE}, {@code PERMISSION},
	 *         {@code DATEADDED} and {@code ENDDATE}
	 */
	static List<Map<String, Object>> queryGroupsWithAccessToProject(String projectId, long limit, long offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__TYPE"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__DATEADDED"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ENDDATE"));
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

	/**
	 * Groups that do not have access to a project yet, for its owners to choose
	 * from.
	 *
	 * @param user       a project owner
	 * @param projectId  the project
	 * @param searchTerm text matched against the group id, or null
	 * @param limit      page size, or 0 or less for all
	 * @param offset     rows to skip
	 * @return the groups, with the keys {@code id}, {@code type} and
	 *         {@code description}
	 * @throws IllegalAccessException when the user is not an owner
	 */
	public static List<Map<String, Object>> getAvailableGroupsForProject(User user, String projectId, String searchTerm,
			long limit, long offset) throws IllegalAccessException {
		checkCanManageGroupAccess(user, projectId);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__DESCRIPTION"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_GROUP__ID"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_GROUP__TYPE"));
		{
			// leave out the groups that already have access
			SelectQueryStruct attachedQs = new SelectQueryStruct();
			attachedQs.addSelector(QueryFunctionSelector.makeConcat2ColumnsFunction("GROUPPROJECTPERMISSION__ID",
					"GROUPPROJECTPERMISSION__TYPE", "GROUPKEY"));
			attachedQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
			qs.addExplicitFilter(SimpleQueryFilter.makeQuerySelectorToSubQuery(
					QueryFunctionSelector.makeConcat2ColumnsFunction("SMSS_GROUP__ID", "SMSS_GROUP__TYPE", "GROUPKEY"),
					"!=", attachedQs));
		}
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "?like", searchTerm));
		}
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return getSimpleQuery(qs);
	}
}
