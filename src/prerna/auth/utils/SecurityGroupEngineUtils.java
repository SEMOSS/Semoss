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
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
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

public class SecurityGroupEngineUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityGroupEngineUtils.class);

	private static final String OWNERS_ONLY = "Only this engine's owners can change which teams have access to it.";

	/**
	 * Only an engine's owners change which groups have access to it. A group's
	 * managers decide who is in it, so attaching a group trusts them with the
	 * access. Being an admin counts for nothing here, since an admin can also act
	 * as an ordinary user; admins change it in admin mode.
	 *
	 * @param user     the user
	 * @param engineId the engine
	 * @return whether the user is one of the engine's owners
	 */
	public static boolean userCanManageGroupAccess(User user, String engineId) {
		return SecurityEngineUtils.userIsOwner(user, engineId);
	}

	private static void checkCanManageGroupAccess(User user, String engineId) throws IllegalAccessException {
		if (!userCanManageGroupAccess(user, engineId)) {
			throw new IllegalAccessException(OWNERS_ONLY);
		}
	}

	/**
	 * Determine if a group can view a database
	 * 
	 * @param user
	 * @param databaseId
	 * @return
	 */
	public static boolean userGroupCanViewEngine(User user, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__PERMISSION", "!=", null,
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
				customAndFilter
						.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", "CUSTOM"));
				customAndFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", userCustomGroups));
				orFilter.addFilter(customAndFilter);
			}
			if (!userGroups.isEmpty()) {
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", userGroupType));
				andFilter
						.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", userGroups));
				orFilter.addFilter(andFilter);
			}
		}

		if (!anyUserGroups) {
			return false;
		}

		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPENGINEPERMISSION__PERMISSION"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Object val = values[0];
				// check if permission is expired
				SemossDate endDate = (SemossDate) values[1];
				if (AbstractSecurityUtils.endDateIsExpired(endDate)) {
					// need to delete expired permission
					String groupId = (String) values[2];
					String groupType = (String) values[3];
					removeExpiredEngineGroupPermission(groupId, groupType, engineId);
					continue;
				}
				if (val != null) {
					// actually do not care what the value is - we have a record so that means we
					// can at least view
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user's group can view engine.", e);
			throw new IllegalArgumentException("Failed to retrieve existing group database permissions for user", e);
		}

		return false;
	}

	/**
	 * Determine if the group can modify the database
	 * 
	 * @param databaseId
	 * @param userId
	 * @return
	 */
	public static boolean userGroupCanEditEngine(User user, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", engineId));
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
				customAndFilter
						.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", "CUSTOM"));
				customAndFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", userCustomGroups));
				orFilter.addFilter(customAndFilter);
			}
			if (!userGroups.isEmpty()) {
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", userGroupType));
				andFilter
						.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", userGroups));
				orFilter.addFilter(andFilter);
			}
		}

		if (!anyUserGroups) {
			return false;
		}

		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPENGINEPERMISSION__PERMISSION"));
		Integer bestGroupDatabasePermission = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Object val = values[0];
				SemossDate endDate = (SemossDate) values[1];
				if (AbstractSecurityUtils.endDateIsExpired(endDate)) {
					// Need to delete expired permission here
					String permissionId = (String) values[2];
					String permissionType = (String) values[3];
					removeExpiredEngineGroupPermission(permissionId, permissionType, engineId);
					continue;
				}
				if (val != null) {
					bestGroupDatabasePermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user's group can edit engine.", e);
			throw new IllegalArgumentException("Failed to retrieve existing group database permissions for user", e);
		}

		if (bestGroupDatabasePermission != null) {
			return AccessPermissionEnum.isEditor(bestGroupDatabasePermission);
		}

		return false;
	}

	/**
	 * Determine if the group is the owner of a database
	 * 
	 * @param userFilters
	 * @param databaseId
	 * @return
	 */
	public static boolean userGroupIsOwner(User user, String databaseId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", databaseId));
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
				customAndFilter
						.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", "CUSTOM"));
				customAndFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", userCustomGroups));
				orFilter.addFilter(customAndFilter);
			}
			if (!userGroups.isEmpty()) {
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", userGroupType));
				andFilter
						.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", userGroups));
				orFilter.addFilter(andFilter);
			}
		}

		if (!anyUserGroups) {
			return false;
		}

		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPENGINEPERMISSION__PERMISSION"));
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
			throw new IllegalArgumentException("Failed to retrieve existing group database permissions for user", e);
		}

		if (bestGroupDatabasePermission != null) {
			return AccessPermissionEnum.isOwner(bestGroupDatabasePermission);
		}

		return false;
	}

	/**
	 * Determine the strongest database permission for the user/group
	 * 
	 * @param userId
	 * @param databaseId
	 * @return
	 */
	public static Integer getBestDatabasePermission(User user, String databaseId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// get best permission from user
		Integer bestUserDatabasePermission = null;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addOrderBy(new QueryColumnOrderBySelector("ENGINEPERMISSION__PERMISSION"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					bestUserDatabasePermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to determine the highest group-based engine permission.", e);
			throw new IllegalArgumentException("Failed to retrieve existing database permissions for user", e);
		}

		// if they are the owner based on user, then skip the group check
		if (bestUserDatabasePermission != null && AccessPermissionEnum.isOwner(bestUserDatabasePermission)) {
			return bestUserDatabasePermission;
		}

		// get best group permission
		Integer bestGroupDatabasePermission = null;

		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", databaseId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		for (AuthProvider login : logins) {
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==",
					user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==",
					user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPENGINEPERMISSION__PERMISSION"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					bestGroupDatabasePermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to determine the highest group-based engine permission.", e);
			throw new IllegalArgumentException("Failed to retrieve existing database permissions for user", e);
		}

		if (bestGroupDatabasePermission == null && bestUserDatabasePermission == null) {
			if (SecurityEngineUtils.engineIsGlobal(databaseId)) {
				return AccessPermissionEnum.READ_ONLY.getId();
			}
			return null;
		} else if (bestGroupDatabasePermission == null
				|| bestGroupDatabasePermission.compareTo(bestUserDatabasePermission) >= 0) {
			return bestUserDatabasePermission;
		} else {
			return bestGroupDatabasePermission;
		}
	}

	/**
	 * Gives a team access to an engine, as a permission batch, with no team limit
	 * defined on a model. Only the engine's owners can.
	 *
	 * @param user       the user giving access
	 * @param groupId    the team
	 * @param groupType  the team's type
	 * @param engineId   the engine
	 * @param permission the level, by name; anything else is Read-Only
	 * @param endDate    when the access ends, or null
	 * @throws IllegalAccessException when the user is not one of the engine's
	 *                                owners
	 */
	public static void addEngineGroupPermission(User user, String groupId, String groupType, String engineId,
			String permission, String endDate) throws IllegalAccessException {
		addEngineGroupPermission(user, groupId, groupType, engineId, permission, endDate, null);
	}

	/**
	 * Gives a team access to an engine, as a permission batch, with a team limit on
	 * a model. Only the engine's owners can, since only they set team limits. The
	 * limit is checked as in a batch: it can be set only on a model, and cost rules
	 * only on a model with known prices.
	 *
	 * @param user       the user giving access
	 * @param groupId    the team
	 * @param groupType  the team's type
	 * @param engineId   the engine
	 * @param permission the level, by name; anything else is Read-Only
	 * @param endDate    when the access ends, or null
	 * @param usageLimit the team limit, with shared rules and rules for each
	 *                   member, or null for no limit defined
	 * @throws IllegalAccessException   when the user is not one of the engine's
	 *                                  owners
	 * @throws IllegalArgumentException when the team has access already, or the
	 *                                  limit cannot be defined there
	 */
	public static void addEngineGroupPermission(User user, String groupId, String groupType, String engineId,
			String permission, String endDate, ModelUsageLimit usageLimit) throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.engine(engineId),
				List.of(PermissionChange.add(PermissionSubject.ofTeam(groupId, groupType),
						new PermissionGrant(permission, endDate).withUsageLimit(usageLimit))),
				PermissionBatchOptions.userMode().withoutTeamExistsCheck().keepingTeamType(),
				new PermissionErrorRules(endDate).illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, OWNERS_ONLY)
						.illegalArgument(PermissionProblem.ErrorCode.ALREADY_HAS_ACCESS,
								"This group already has access to this engine. Please edit the existing permission level."));
	}

	/**
	 * Get the database permission for a specific group
	 * 
	 * @param groupId
	 * @param groupType
	 * @param engineId
	 * @return
	 */
	public static Integer getGroupDatabasePermission(String groupId, String groupType, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", groupType));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null && val instanceof Number) {
					return ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the group-based engine permission.", e);
		}

		return null;
	}

	/**
	 * Changes a team's access to an engine, as a permission batch, keeping its team
	 * limit on a model and the date it was first given access. Only the engine's
	 * owners can.
	 *
	 * @param user          the user changing the access
	 * @param groupId       the team
	 * @param groupType     the team's type
	 * @param engineId      the engine
	 * @param newPermission the level, by name; anything else is Read-Only
	 * @param endDate       when the access ends, or null
	 * @throws IllegalAccessException when the user is not one of the engine's
	 *                                owners
	 */
	public static void editDatabaseGroupPermission(User user, String groupId, String groupType, String engineId,
			String newPermission, String endDate) throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.engine(engineId),
				List.of(PermissionChange.change(PermissionSubject.ofTeam(groupId, groupType),
						new PermissionGrant(newPermission, endDate))),
				PermissionBatchOptions.userMode().keepingTeamType().keepingModelLimits(),
				new PermissionErrorRules(endDate).illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, OWNERS_ONLY)
						.illegalArgument(PermissionProblem.ErrorCode.NO_ACCESS,
								"Attempting to modify database permission for a group who does not currently have access to the database"));
	}

	/**
	 * Takes a team's access to an engine away, as a permission batch. Only the
	 * engine's owners can.
	 *
	 * @param user      the user taking the access away
	 * @param groupId   the team
	 * @param groupType the team's type
	 * @param engineId  the engine
	 * @throws IllegalAccessException when the user is not one of the engine's
	 *                                owners
	 */
	public static void removeDatabaseGroupPermission(User user, String groupId, String groupType, String engineId)
			throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.engine(engineId),
				List.of(PermissionChange.remove(PermissionSubject.ofTeam(groupId, groupType))),
				PermissionBatchOptions.userMode().keepingTeamType(),
				new PermissionErrorRules().illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, OWNERS_ONLY)
						.illegalArgument(PermissionProblem.ErrorCode.NO_ACCESS,
								"Attempting to modify group permission for a user who does not currently have access to the database"));
	}

	/**
	 * Removes a team's grant on an engine that has ended, and its team limit on a
	 * model with it, in one transaction.
	 *
	 * @param groupId   the team
	 * @param groupType the team's type
	 * @param engineId  the engine
	 * @throws IllegalArgumentException when the team has no grant on the engine
	 */
	public static void removeExpiredEngineGroupPermission(String groupId, String groupType, String engineId)
			throws IllegalAccessException {

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupDatabasePermission(groupId, groupType, engineId);
		if (existingGroupPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify group permission for a user who does not currently have access to the database");
		}

		try {
			QueryExecutionUtility.write(securityDb, conn -> {
				QueryExecutionUtility.executeUpdate(conn,
						"DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND TYPE=? AND ENGINEID=?", ps -> {
							int parameterIndex = 1;
							ps.setString(parameterIndex++, groupId);
							ps.setString(parameterIndex++, groupType);
							ps.setString(parameterIndex++, engineId);
						});
				// the team limit belongs to the grant
				SecurityModelUsageLimitUtils.setLimit(conn, ModelUsageLimitKey.team(engineId, groupId, groupType), null,
						null);
				return null;
			});
			SecurityModelUsageLimitUtils.markChanged();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the group-based engine permission.", e);
		}
	}

	/**
	 * Determine if a group can view a database
	 * 
	 * @param user
	 * @return
	 */
	public static List<String> getAllUserGroupDatabases(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__PERMISSION", "!=", null,
				PixelDataType.CONST_INT));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		for (AuthProvider login : logins) {
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==",
					user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==",
					user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilter);
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	/**
	 * Get groups that have access to an engine
	 * 
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getGroupsWithAccessToEngine(User user, String engineId, long limit,
			long offset) throws IllegalAccessException {
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Input engineId must not be null or blank");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalAccessException("The user does not have access to view this engine");
		}
		return queryGroupsWithAccessToEngine(engineId, limit, offset);
	}

	/**
	 * The groups that have access to an engine, read without checking who asks, for
	 * callers that already checked, such as admin mode
	 *
	 * @return rows with {@code ID}, {@code TYPE}, {@code PERMISSION},
	 *         {@code DATEADDED} and {@code ENDDATE}
	 */
	static List<Map<String, Object>> queryGroupsWithAccessToEngine(String engineId, long limit, long offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__TYPE"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__DATEADDED"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ENDDATE"));
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPENGINEPERMISSION__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", engineId));
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get number of groups that have access to an engine
	 * 
	 * @return
	 * @throws IllegalAccessException
	 */
	public static Long getNumGroupsWithAccessToEngine(User user, String engineId) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Input engineId must not be null or blank");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalAccessException("The user does not have access to view this engine");
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				"GROUPENGINEPERMISSION__ID", "numGroups"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", engineId));
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	/**
	 * Groups that do not have access to an engine yet, for its owners to choose
	 * from.
	 *
	 * @param user       an engine owner
	 * @param engineId   the engine
	 * @param searchTerm text matched against the group id, or null
	 * @param limit      page size, or 0 or less for all
	 * @param offset     rows to skip
	 * @return the groups, with the keys {@code id}, {@code type} and
	 *         {@code description}
	 * @throws IllegalAccessException when the user is not an owner
	 */
	public static List<Map<String, Object>> getAvailableGroupsForEngine(User user, String engineId, String searchTerm,
			long limit, long offset) throws IllegalAccessException {
		checkCanManageGroupAccess(user, engineId);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__DESCRIPTION"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_GROUP__ID"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_GROUP__TYPE"));
		{
			// leave out the groups that already have access
			SelectQueryStruct attachedQs = new SelectQueryStruct();
			attachedQs.addSelector(QueryFunctionSelector.makeConcat2ColumnsFunction("GROUPENGINEPERMISSION__ID",
					"GROUPENGINEPERMISSION__TYPE", "GROUPKEY"));
			attachedQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", engineId));
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
