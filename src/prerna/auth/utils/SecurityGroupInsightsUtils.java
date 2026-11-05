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

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
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
import prerna.util.ValueUtils;

public class SecurityGroupInsightsUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityGroupInsightsUtils.class);

	private static final String OWNERS_ONLY = "Only this insight's owners can change which teams have access to it.";

	/**
	 * Only an insight's owners change which groups have access to it. A group's
	 * managers decide who is in it, so attaching a group trusts them with the
	 * access. Being an admin counts for nothing here, since an admin can also act
	 * as an ordinary user.
	 *
	 * @param user      the user
	 * @param projectId the project the insight belongs to
	 * @param insightId the insight
	 * @return whether the user is one of the insight's owners
	 */
	public static boolean userCanManageGroupAccess(User user, String projectId, String insightId) {
		return SecurityInsightUtils.userIsInsightOwner(user, projectId, insightId);
	}

	private static void checkCanManageGroupAccess(User user, String projectId, String insightId)
			throws IllegalAccessException {
		if (!userCanManageGroupAccess(user, projectId, insightId)) {
			throw new IllegalAccessException(OWNERS_ONLY);
		}
	}

	/**
	 * Determine if group can view insight
	 * 
	 * @param user
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userGroupCanViewInsight(User user, String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__TYPE"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PERMISSION", "!=", null,
				PixelDataType.CONST_INT));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for (AuthProvider login : logins) {
			if (user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}

			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__TYPE", "==",
					user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__ID", "==",
					user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}

		if (!anyUserGroups) {
			return false;
		}

		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Object val = values[0];
				SemossDate endDate = (SemossDate) values[1];
				if (AbstractSecurityUtils.endDateIsExpired(endDate)) {
					// Need to delete expired permission here
					String groupId = (String) values[2];
					String groupType = (String) values[3];
					removeExpiredInsightGroupPermission(groupId, groupType, projectId, insightId);
					continue;
				}
				if (val != null) {
					// actually do not care what the value is - we have a record so that means we
					// can at least view
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user's group can view insight.", e);
			throw new IllegalArgumentException("Failed to retrieve existing group project permissions for user", e);
		}

		return false;
	}

	/**
	 * Determine if group can edit insight
	 * 
	 * @param user
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userGroupCanEditInsight(User user, String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__TYPE"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PROJECTID", "==", projectId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for (AuthProvider login : logins) {
			if (user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}

			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__TYPE", "==",
					user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__ID", "==",
					user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}

		if (!anyUserGroups) {
			return false;
		}

		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPINSIGHTPERMISSION__PERMISSION"));
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
					removeExpiredInsightGroupPermission(groupId, groupType, projectId, insightId);
					continue;
				}
				if (val != null) {
					bestGroupDatabasePermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user's group can edit insight.", e);
			throw new IllegalArgumentException("Failed to retrieve existing group project permissions for user", e);
		}

		if (bestGroupDatabasePermission != null) {
			return AccessPermissionEnum.isEditor(bestGroupDatabasePermission);
		}

		return false;
	}

	/**
	 * Determine if group is owner of insight
	 * 
	 * @param user
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userGroupIsOwner(User user, String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PROJECTID", "==", projectId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for (AuthProvider login : logins) {
			if (user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}

			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__TYPE", "==",
					user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__ID", "==",
					user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}

		if (!anyUserGroups) {
			return false;
		}

		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPINSIGHTPERMISSION__PERMISSION"));
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
	 * Determine the strongest insight permission for the user/group
	 * 
	 * @param userId
	 * @param insightId
	 * @return
	 */
	public static Integer getBestInsightPermission(User user, String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// get best permission from user
		Integer bestUserInsightPermission = null;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addOrderBy(new QueryColumnOrderBySelector("USERINSIGHTPERMISSION__PERMISSION"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					bestUserInsightPermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to determine the highest group-based insight permission.", e);
			throw new IllegalArgumentException("Failed to retrieve existing insight permissions for user", e);
		}

		// if they are the owner based on user, then skip the group check
		if (bestUserInsightPermission != null && AccessPermissionEnum.isOwner(bestUserInsightPermission)) {
			return bestUserInsightPermission;
		}

		// get best group permission
		Integer bestGroupInsightPermission = null;

		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		for (AuthProvider login : logins) {
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__TYPE", "==",
					user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__ID", "==",
					user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					bestGroupInsightPermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to determine the highest group-based insight permission.", e);
			throw new IllegalArgumentException("Failed to retrieve existing insight permissions for user", e);
		}

		if (bestGroupInsightPermission == null && bestUserInsightPermission == null) {
			if (SecurityInsightUtils.insightIsGlobal(projectId, insightId)) {
				return AccessPermissionEnum.READ_ONLY.getId();
			}
			return null;
		} else if (bestGroupInsightPermission == null
				|| bestGroupInsightPermission.compareTo(bestUserInsightPermission) >= 0) {
			return bestUserInsightPermission;
		} else {
			return bestGroupInsightPermission;
		}
	}

	/**
	 * Gives a team access to an insight, as a permission batch. Only the insight's
	 * owners can.
	 *
	 * @param user       the user giving access
	 * @param groupId    the team
	 * @param groupType  the team's type
	 * @param projectId  the project the insight belongs to
	 * @param insightId  the insight
	 * @param permission the level, by name; anything else is Read-Only
	 * @param endDate    when the access ends, or null
	 * @throws IllegalAccessException when the user is not one of the insight's
	 *                                owners
	 */
	public static void addInsightGroupPermission(User user, String groupId, String groupType, String projectId,
			String insightId, String permission, String endDate) throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.insight(projectId, insightId),
				List.of(PermissionChange.add(PermissionSubject.ofTeam(groupId, groupType),
						new PermissionGrant(permission, endDate))),
				PermissionBatchOptions.userMode().withoutTeamExistsCheck().keepingTeamType(),
				new PermissionErrorRules(endDate).illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, OWNERS_ONLY)
						.illegalArgument(PermissionProblem.ErrorCode.ALREADY_HAS_ACCESS,
								"This group already has access to this insight. Please edit the existing permission level."));
	}

	/**
	 * Get the insight permission for a specific group
	 * 
	 * @param groupId
	 * @param groupType
	 * @param insightId
	 * @return
	 */
	public static Integer getGroupInsightPermission(String groupId, String groupType, String projectId,
			String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__TYPE", "==", groupType));
		Integer permission = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null && val instanceof Number) {
					permission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to determine the highest group-based insight permission.", e);
		}

		return permission;
	}

	/**
	 * Changes a team's access to an insight, as a permission batch. Only the
	 * insight's owners can.
	 *
	 * @param user          the user changing the access
	 * @param groupId       the team
	 * @param groupType     the team's type
	 * @param projectId     the project the insight belongs to
	 * @param insightId     the insight
	 * @param newPermission the level, by name; anything else is Read-Only
	 * @param endDate       when the access ends, or null
	 * @throws IllegalAccessException when the user is not one of the insight's
	 *                                owners
	 */
	public static void editInsightGroupPermission(User user, String groupId, String groupType, String projectId,
			String insightId, String newPermission, String endDate) throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.insight(projectId, insightId),
				List.of(PermissionChange.change(PermissionSubject.ofTeam(groupId, groupType),
						new PermissionGrant(newPermission, endDate))),
				PermissionBatchOptions.userMode().keepingTeamType(),
				new PermissionErrorRules(endDate).illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, OWNERS_ONLY)
						.illegalArgument(PermissionProblem.ErrorCode.NO_ACCESS,
								"Attempting to modify insight permission for a group who does not currently have access to the insight"));
	}

	/**
	 * Takes a team's access to an insight away, as a permission batch. Only the
	 * insight's owners can.
	 *
	 * @param user      the user taking the access away
	 * @param groupId   the team
	 * @param groupType the team's type
	 * @param projectId the project the insight belongs to
	 * @param insightId the insight
	 * @throws IllegalAccessException when the user is not one of the insight's
	 *                                owners
	 */
	public static void removeInsightGroupPermission(User user, String groupId, String groupType, String projectId,
			String insightId) throws IllegalAccessException {
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.insight(projectId, insightId),
				List.of(PermissionChange.remove(PermissionSubject.ofTeam(groupId, groupType))),
				PermissionBatchOptions.userMode().keepingTeamType(),
				new PermissionErrorRules().illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, OWNERS_ONLY)
						.illegalArgument(PermissionProblem.ErrorCode.NO_ACCESS,
								"Attempting to modify group permission for a user who does not currently have access to the insight"));
	}

	/**
	 * Delete a group insight permission
	 * 
	 * @param groupId
	 * @param groupType
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static void removeExpiredInsightGroupPermission(String groupId, String groupType, String projectId,
			String insightId) throws IllegalAccessException {

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupInsightPermission(groupId, groupType, projectId, insightId);
		if (existingGroupPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify group permission for a user who does not currently have access to the insight");
		}

		try {
			QueryExecutionUtility.executeUpdate(securityDb,
					"DELETE FROM GROUPINSIGHTPERMISSION WHERE ID=? AND TYPE=? AND PROJECTID=? AND INSIGHTID=?", ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, groupId);
						ps.setString(parameterIndex++, groupType);
						ps.setString(parameterIndex++, projectId);
						ps.setString(parameterIndex++, insightId);
					});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to determine the highest group-based insight permission.", e);
		}
	}

	/**
	 * The groups that have access to an insight.
	 *
	 * @param user      a user who can view the insight
	 * @param projectId the project the insight belongs to
	 * @param insightId the insight
	 * @param limit     page size, or 0 or less for all
	 * @param offset    rows to skip
	 * @return the groups, with the keys {@code ID}, {@code TYPE},
	 *         {@code PERMISSION} and {@code DATEADDED}, as the project and engine
	 *         versions return them
	 * @throws IllegalAccessException when the user cannot view the insight
	 */
	public static List<Map<String, Object>> getGroupsWithAccessToInsight(User user, String projectId, String insightId,
			long limit, long offset) throws IllegalAccessException {
		checkCanViewInsight(user, projectId, insightId);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__TYPE"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__DATEADDED"));
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPINSIGHTPERMISSION__ID"));
		addInsightFilters(qs, projectId, insightId);
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * @param user      a user who can view the insight
	 * @param projectId the project the insight belongs to
	 * @param insightId the insight
	 * @return the number of groups that have access to the insight
	 * @throws IllegalAccessException when the user cannot view the insight
	 */
	public static Long getNumGroupsWithAccessToInsight(User user, String projectId, String insightId)
			throws IllegalAccessException {
		checkCanViewInsight(user, projectId, insightId);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				"GROUPINSIGHTPERMISSION__ID", "numGroups"));
		addInsightFilters(qs, projectId, insightId);
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	/**
	 * Groups that do not have access to an insight yet, for its owners to choose
	 * from.
	 *
	 * @param user       an owner of the insight
	 * @param projectId  the project the insight belongs to
	 * @param insightId  the insight
	 * @param searchTerm text matched against the group id, or null
	 * @param limit      page size, or 0 or less for all
	 * @param offset     rows to skip
	 * @return the groups, with the keys {@code id}, {@code type} and
	 *         {@code description}
	 * @throws IllegalAccessException when the user is not an owner
	 */
	public static List<Map<String, Object>> getAvailableGroupsForInsight(User user, String projectId, String insightId,
			String searchTerm, long limit, long offset) throws IllegalAccessException {
		requireInsight(projectId, insightId);
		checkCanManageGroupAccess(user, projectId, insightId);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__DESCRIPTION"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_GROUP__ID"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_GROUP__TYPE"));
		{
			// leave out the groups that already have access
			SelectQueryStruct attachedQs = new SelectQueryStruct();
			attachedQs.addSelector(QueryFunctionSelector.makeConcat2ColumnsFunction("GROUPINSIGHTPERMISSION__ID",
					"GROUPINSIGHTPERMISSION__TYPE", "GROUPKEY"));
			addInsightFilters(attachedQs, projectId, insightId);
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

	private static void checkCanViewInsight(User user, String projectId, String insightId)
			throws IllegalAccessException {
		requireInsight(projectId, insightId);
		if (!SecurityInsightUtils.userCanViewInsight(user, projectId, insightId)) {
			throw new IllegalAccessException("The user does not have access to view this insight");
		}
	}

	private static void requireInsight(String projectId, String insightId) {
		ValueUtils.requireNonBlank(projectId, "Input projectId must not be null or blank");
		ValueUtils.requireNonBlank(insightId, "Input insightId must not be null or blank");
	}

	private static void addInsightFilters(SelectQueryStruct qs, String projectId, String insightId) {
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__INSIGHTID", "==", insightId));
	}
}
