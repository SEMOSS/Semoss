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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionErrorRules;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionProblem;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.ValueUtils;

/**
 * Group managers: the users who manage the members of a custom group.
 *
 * <p>
 * An admin creates a custom group and becomes its first manager. A group's
 * managers add and remove its members, which gives those people whatever access
 * resource owners have given the group, and add and remove its other managers.
 * They can see which projects and engines the group has access to, but only the
 * resources' owners change that. Admins can do everything a manager can, for
 * every custom group, in admin mode: through the methods that take the admin
 * group utilities. Outside admin mode an admin is an ordinary user. Groups of
 * other types take their members from the login, so they have no managers.
 * </p>
 *
 * <p>
 * Every method that reads or changes a group checks the user's permissions
 * itself. A manager is matched on a login's id and type together, since ids
 * alone are not unique across providers. A person picked from the Microsoft
 * directory who is not in the security database yet is added to it, with the
 * details the directory holds for them, only after the change has been
 * validated.
 * </p>
 */
public class SecurityGroupManagerUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityGroupManagerUtils.class);

	/** The type of the groups whose members are managed in SEMOSS */
	public static final String CUSTOM_GROUP_TYPE = "CUSTOM";

	/** The most members or users a group's manager reads at once */
	public static final int MAX_PAGE_SIZE = 200;

	private static final String GROUP_ID_REQUIRED = "Must define the group id";
	private static final String USER_ID_REQUIRED = "Must define the user id";
	private static final String MANAGERS_ONLY = "Only this group's managers and admins can view or change it";

	/**
	 * Inserts a manager: group id, user id, user type, date added, granted by id
	 * and type
	 */
	static final String INSERT_MANAGER_QUERY = "INSERT INTO GROUPMANAGERS (GROUPID, USERID, TYPE, DATEADDED, "
			+ "PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES (?,?,?,?,?,?)";

	private SecurityGroupManagerUtils() {

	}

	/*
	 * Who manages a group
	 */

	/**
	 * @param user    the user
	 * @param groupId the custom group
	 * @return whether one of the user's logins manages the group
	 */
	public static boolean userIsGroupManager(User user, String groupId) {
		IQueryFilter loginFilter = getManagerLoginFilter(user);
		if (groupId == null || loginFilter == null) {
			return false;
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPMANAGERS__GROUPID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPMANAGERS__GROUPID", "==", groupId));
		qs.addExplicitFilter(loginFilter);
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user manages the group.", e);
		}
		return false;
	}

	/**
	 * Being an admin counts for nothing here, since an admin can also act as an
	 * ordinary user; admins manage any custom group in admin mode, through the
	 * methods that take the admin group utilities.
	 *
	 * @param user    the user
	 * @param groupId the group
	 * @return whether the user can manage the group's members: the group is a
	 *         custom group and the user is one of its managers
	 */
	public static boolean userCanManageGroup(User user, String groupId) {
		if (groupId == null || !AdminSecurityGroupUtils.getUncheckedInstance().isCustomGroup(groupId)) {
			return false;
		}
		return userIsGroupManager(user, groupId);
	}

	/**
	 * Whether the user can see who manages a custom group: they can manage it, or
	 * they own the project or engine they are about to give it access to, and are
	 * shown its managers before they do.
	 *
	 * @param user      the user
	 * @param groupId   the custom group
	 * @param projectId the project the user owns, or null
	 * @param engineId  the engine the user owns, or null
	 * @return whether the user can see the group's managers
	 */
	public static boolean userCanViewGroupManagers(User user, String groupId, String projectId, String engineId) {
		if (userCanManageGroup(user, groupId)) {
			return true;
		}
		if (groupId == null || !AdminSecurityGroupUtils.getUncheckedInstance().isCustomGroup(groupId)) {
			return false;
		}
		return (projectId != null && SecurityGroupProjectUtils.userCanManageGroupAccess(user, projectId))
				|| (engineId != null && SecurityGroupEngineUtils.userCanManageGroupAccess(user, engineId))
				|| userCanViewGroupPeople(user, groupId, projectId, engineId);
	}

	/**
	 * Whether the user can see who is in a custom group. With no project or engine,
	 * its managers can. Through a project or engine, its managers can; so can the
	 * owners of that project or engine, who see the people they would give access
	 * to before they give the group access, as they see its managers; and so can
	 * its editors once the group has access to it. Being an admin counts for
	 * nothing, since an admin can also act as an ordinary user.
	 *
	 * @param user      the user
	 * @param groupId   the custom group
	 * @param projectId a project the user owns or edits, or null
	 * @param engineId  an engine the user owns or edits, or null
	 * @return whether the user can see the group's members and managers
	 */
	public static boolean userCanViewGroupPeople(User user, String groupId, String projectId, String engineId) {
		if (projectId == null && engineId == null) {
			return userCanManageGroup(user, groupId);
		}
		if (userIsGroupManager(user, groupId)) {
			return true;
		}
		if (groupId == null || !AdminSecurityGroupUtils.getUncheckedInstance().isCustomGroup(groupId)) {
			return false;
		}
		if (projectId != null) {
			return SecurityGroupProjectUtils.userCanManageGroupAccess(user, projectId)
					|| (SecurityGroupProjectUtils.getGroupProjectPermission(groupId, CUSTOM_GROUP_TYPE,
							projectId) != null && SecurityProjectUtils.userCanEditProject(user, projectId));
		}
		if (engineId != null) {
			return SecurityGroupEngineUtils.userCanManageGroupAccess(user, engineId)
					|| (SecurityGroupEngineUtils.getGroupDatabasePermission(groupId, CUSTOM_GROUP_TYPE,
							engineId) != null && SecurityEngineUtils.userCanEditEngine(user, engineId));
		}
		return false;
	}

	/**
	 * @param limit the page size a caller asked for, or 0 or less for all
	 * @return the page size to read, at most {@link #MAX_PAGE_SIZE}
	 */
	public static long clampPageSize(long limit) {
		return limit <= 0 || limit > MAX_PAGE_SIZE ? MAX_PAGE_SIZE : limit;
	}

	/*
	 * The groups a user manages
	 */

	/**
	 * The custom groups the user manages, in the same shape as the admin group
	 * list, with {@code manager_since}: when the user became a manager, the
	 * earliest of their logins when more than one manages the group.
	 *
	 * @param user       the user
	 * @param searchTerm text matched against the group id, or null
	 * @param limit      page size, or 0 or less for all
	 * @param offset     rows to skip
	 * @return the groups
	 */
	public static List<Map<String, Object>> getManagedGroups(User user, String searchTerm, long limit, long offset) {
		if (getManagerLoginFilter(user) == null) {
			return new ArrayList<>();
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__DESCRIPTION"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__USERID", "CREATED_BY_USERID"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__USERIDTYPE", "CREATED_BY_USERIDTYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__DATEADDED"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_GROUP__ID"));
		addManagedGroupFilters(qs, user, searchTerm);
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		List<Map<String, Object>> groups = getSimpleQuery(qs);
		addManagerSince(groups, user);
		AdminSecurityGroupUtils.addMemberCounts(groups);
		return groups;
	}

	/**
	 * @param user       the user
	 * @param searchTerm text matched against the group id, or null
	 * @return the number of custom groups the user manages
	 */
	public static Long getNumManagedGroups(User user, String searchTerm) {
		if (getManagerLoginFilter(user) == null) {
			return 0L;
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, "SMSS_GROUP__ID", "numGroups"));
		addManagedGroupFilters(qs, user, searchTerm);
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	/**
	 * Limits a group query to the custom groups the user manages. The managers are
	 * matched in a subquery, so a group comes back once however many of the user's
	 * logins manage it.
	 */
	private static void addManagedGroupFilters(SelectQueryStruct qs, User user, String searchTerm) {
		SelectQueryStruct managedQs = new SelectQueryStruct();
		managedQs.addSelector(new QueryColumnSelector("GROUPMANAGERS__GROUPID"));
		managedQs.addExplicitFilter(getManagerLoginFilter(user));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__TYPE", "==", CUSTOM_GROUP_TYPE));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("SMSS_GROUP__ID", "==", managedQs));
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "?like", searchTerm));
		}
	}

	/**
	 * Adds {@code manager_since} to each group: the earliest date one of the user's
	 * logins became its manager.
	 */
	private static void addManagerSince(List<Map<String, Object>> groups, User user) {
		List<String> groupIds = new ArrayList<>();
		for (Map<String, Object> group : groups) {
			if (group.get("id") != null) {
				groupIds.add(group.get("id").toString());
			}
		}
		if (groupIds.isEmpty()) {
			return;
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPMANAGERS__GROUPID", "GROUPID"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, "GROUPMANAGERS__DATEADDED",
				"MANAGER_SINCE"));
		qs.addGroupBy(new QueryColumnSelector("GROUPMANAGERS__GROUPID", "GROUPID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPMANAGERS__GROUPID", "==", groupIds));
		qs.addExplicitFilter(getManagerLoginFilter(user));

		Map<String, Object> managerSince = new HashMap<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				if (values[0] != null) {
					managerSince.put(values[0].toString(), values[1]);
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to read when the user became a manager of their groups.", e);
		}
		for (Map<String, Object> group : groups) {
			if (group.get("id") != null) {
				group.put("manager_since", managerSince.get(group.get("id").toString()));
			}
		}
	}

	/*
	 * A group's managers
	 */

	/**
	 * The managers of a custom group, with their names and emails.
	 *
	 * @param user      the user asking: one of the group's managers, or the owner
	 *                  of the project or engine passed
	 * @param groupId   the custom group
	 * @param projectId a project the user owns and may give the group access to, or
	 *                  null
	 * @param engineId  an engine the user owns and may give the group access to, or
	 *                  null
	 * @return the managers, with the keys {@code userid}, {@code type},
	 *         {@code dateadded}, {@code name}, {@code username} and {@code email}
	 * @throws IllegalAccessException when the user cannot see the group's managers
	 */
	public static List<Map<String, Object>> getGroupManagers(User user, String groupId, String projectId,
			String engineId) throws IllegalAccessException {
		ValueUtils.requireNonBlank(groupId, GROUP_ID_REQUIRED);
		if (!userCanViewGroupManagers(user, groupId, projectId, engineId)) {
			throw new IllegalAccessException(
					"Only this group's managers, and the owners of what it is being given, can see who manages it");
		}
		return queryGroupManagers(groupId);
	}

	/**
	 * {@link #getGroupManagers(User, String, String, String)} for an admin, who
	 * proves it with the admin group utilities, so it is not checked again.
	 *
	 * @param adminGroupUtils the admin group utilities, from
	 *                        {@link AdminSecurityGroupUtils#getInstance(User)}
	 * @param groupId         the custom group
	 * @return the managers
	 * @throws IllegalAccessException when no admin group utilities are passed
	 */
	public static List<Map<String, Object>> getGroupManagers(AdminSecurityGroupUtils adminGroupUtils, String groupId)
			throws IllegalAccessException {
		requireAdmin(adminGroupUtils);
		checkCanManageGroup(adminGroupUtils, null, groupId);
		return queryGroupManagers(groupId);
	}

	/**
	 * @param groupId the custom group
	 * @return its managers, with the keys {@code userid}, {@code type},
	 *         {@code dateadded}, {@code name}, {@code username} and {@code email},
	 *         read without checking who asks
	 */
	static List<Map<String, Object>> queryGroupManagers(String groupId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPMANAGERS__USERID"));
		qs.addSelector(new QueryColumnSelector("GROUPMANAGERS__TYPE"));
		qs.addSelector(new QueryColumnSelector("GROUPMANAGERS__DATEADDED"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__NAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPMANAGERS__GROUPID", "==", groupId));
		qs.addRelation("GROUPMANAGERS__USERID", "SMSS_USER__ID", "inner.join");
		qs.addRelation("GROUPMANAGERS__TYPE", "SMSS_USER__TYPE", "inner.join");
		return getSimpleQuery(qs);
	}

	/**
	 * @param groupId  the custom group
	 * @param userId   the user id
	 * @param userType the user's login type
	 * @return whether the user manages the group
	 */
	public static boolean managerExists(String groupId, String userId, String userType) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPMANAGERS__GROUPID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPMANAGERS__GROUPID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPMANAGERS__USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPMANAGERS__TYPE", "==", userType));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user manages the group.", e);
		}
		return false;
	}

	/**
	 * Makes a user a manager of a custom group, as a permission batch.
	 *
	 * @param grantor  the manager making the change
	 * @param groupId  the custom group
	 * @param userId   the new manager's user id
	 * @param userType the new manager's login type
	 * @throws IllegalAccessException   when the grantor does not manage the group
	 * @throws IllegalArgumentException when the group is not a custom group, the
	 *                                  user does not exist or already manages the
	 *                                  group
	 */
	public static void addGroupManager(User grantor, String groupId, String userId, String userType) throws Exception {
		checkCanManageGroup(null, grantor, groupId);
		changeManager(grantor, false, PermissionChange.Action.ADD, groupId, userId, userType);
	}

	/**
	 * {@link #addGroupManager(User, String, String, String)} for an admin, who
	 * proves it with the admin group utilities, so it is not checked again.
	 *
	 * @param adminGroupUtils the admin group utilities, from
	 *                        {@link AdminSecurityGroupUtils#getInstance(User)}
	 * @throws IllegalAccessException when no admin group utilities are passed
	 */
	public static void addGroupManager(AdminSecurityGroupUtils adminGroupUtils, User grantor, String groupId,
			String userId, String userType) throws Exception {
		requireAdmin(adminGroupUtils);
		checkCanManageGroup(adminGroupUtils, grantor, groupId);
		changeManager(grantor, true, PermissionChange.Action.ADD, groupId, userId, userType);
	}

	/**
	 * Adds or removes a manager as a permission batch, once the caller's
	 * permissions have been checked, with these methods' own messages.
	 */
	private static void changeManager(User user, boolean asAdmin, PermissionChange.Action action, String groupId,
			String userId, String userType) throws IllegalAccessException {
		String type = normalizeUserType(userType);
		ValueUtils.requireNonBlank(userId, USER_ID_REQUIRED);
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.teamManagers(groupId),
				List.of(new PermissionChange(action, PermissionSubject.ofUser(userId, type), null)),
				asAdmin ? PermissionBatchOptions.adminMode() : PermissionBatchOptions.userMode(),
				new PermissionErrorRules().illegalAccess(PermissionProblem.Code.NO_PERMISSION, MANAGERS_ONLY)
						.illegalArgument(PermissionProblem.Code.INVALID_TARGET,
								"Group " + groupId + " is not a custom group")
						.illegalArgument(PermissionProblem.Code.ALREADY_HAS_ACCESS,
								"User " + userId + " already manages group " + groupId)
						.illegalArgument(PermissionProblem.Code.NO_ACCESS,
								"User " + userId + " does not manage group " + groupId));
	}

	/**
	 * Stops a user managing a custom group, as a permission batch.
	 *
	 * @param user     the manager making the change
	 * @param groupId  the custom group
	 * @param userId   the manager's user id
	 * @param userType the manager's login type
	 * @throws IllegalAccessException   when the user does not manage the group
	 * @throws IllegalArgumentException when the person does not manage the group
	 */
	public static void removeGroupManager(User user, String groupId, String userId, String userType) throws Exception {
		checkCanManageGroup(null, user, groupId);
		changeManager(user, false, PermissionChange.Action.REMOVE, groupId, userId, userType);
	}

	/**
	 * {@link #removeGroupManager(User, String, String, String)} for an admin, who
	 * proves it with the admin group utilities, so it is not checked again.
	 *
	 * @param adminGroupUtils the admin group utilities, from
	 *                        {@link AdminSecurityGroupUtils#getInstance(User)}
	 * @throws IllegalAccessException when no admin group utilities are passed
	 */
	public static void removeGroupManager(AdminSecurityGroupUtils adminGroupUtils, String groupId, String userId,
			String userType) throws Exception {
		requireAdmin(adminGroupUtils);
		checkCanManageGroup(adminGroupUtils, null, groupId);
		changeManager(null, true, PermissionChange.Action.REMOVE, groupId, userId, userType);
	}

	/*
	 * Group details and members, for the group's managers
	 */

	/**
	 * @see AdminSecurityGroupUtils#getGroupDetails(String, String)
	 * @throws IllegalAccessException when the user cannot manage the group
	 */
	public static Map<String, Object> getGroupDetails(User user, String groupId) throws IllegalAccessException {
		checkCanManageGroup(user, groupId);
		return AdminSecurityGroupUtils.getUncheckedInstance().getGroupDetails(groupId, CUSTOM_GROUP_TYPE);
	}

	/**
	 * @param limit page size, at most {@link #MAX_PAGE_SIZE}; 0 or less reads that
	 *              many
	 * @see AdminSecurityGroupUtils#getGroupMembers(String, String, long, long)
	 * @throws IllegalAccessException when the user cannot manage the group
	 */
	public static List<Map<String, Object>> getGroupMembers(User user, String groupId, String searchTerm, long limit,
			long offset) throws IllegalAccessException {
		checkCanManageGroup(user, groupId);
		return AdminSecurityGroupUtils.getUncheckedInstance().getGroupMembers(groupId, searchTerm, clampPageSize(limit),
				offset);
	}

	/**
	 * {@link #getGroupMembers(User, String, String, long, long)} for the owners and
	 * editors of a project or engine the group has access to, as well as its
	 * managers and admins.
	 *
	 * @param projectId a project the user owns or edits, or null
	 * @param engineId  an engine the user owns or edits, or null
	 * @throws IllegalAccessException when the user cannot see the group's members
	 * @see #userCanViewGroupPeople(User, String, String, String)
	 */
	public static List<Map<String, Object>> getGroupMembers(User user, String groupId, String projectId,
			String engineId, String searchTerm, long limit, long offset) throws IllegalAccessException {
		checkCanViewGroupPeople(user, groupId, projectId, engineId);
		return AdminSecurityGroupUtils.getUncheckedInstance().getGroupMembers(groupId, searchTerm, clampPageSize(limit),
				offset);
	}

	/**
	 * {@link #getNumMembersInGroup(User, String, String)} for the owners and
	 * editors of a project or engine the group has access to, as well as its
	 * managers and admins.
	 *
	 * @throws IllegalAccessException when the user cannot see the group's members
	 * @see #userCanViewGroupPeople(User, String, String, String)
	 */
	public static Long getNumMembersInGroup(User user, String groupId, String projectId, String engineId,
			String searchTerm) throws IllegalAccessException {
		checkCanViewGroupPeople(user, groupId, projectId, engineId);
		return AdminSecurityGroupUtils.getUncheckedInstance().getNumMembersInGroup(groupId, searchTerm);
	}

	/**
	 * Every member of a custom group, so a directory search can leave them out.
	 *
	 * @see AdminSecurityGroupUtils#getGroupMembers(String, String, long, long)
	 * @throws IllegalAccessException when the user cannot manage the group
	 */
	public static List<Map<String, Object>> getAllGroupMembers(User user, String groupId)
			throws IllegalAccessException {
		checkCanManageGroup(user, groupId);
		return AdminSecurityGroupUtils.getUncheckedInstance().getGroupMembers(groupId, null, -1, -1);
	}

	/**
	 * @see AdminSecurityGroupUtils#getNumMembersInGroup(String, String)
	 * @throws IllegalAccessException when the user cannot manage the group
	 */
	public static Long getNumMembersInGroup(User user, String groupId, String searchTerm)
			throws IllegalAccessException {
		checkCanManageGroup(user, groupId);
		return AdminSecurityGroupUtils.getUncheckedInstance().getNumMembersInGroup(groupId, searchTerm);
	}

	/**
	 * @param limit page size, at most {@link #MAX_PAGE_SIZE}; 0 or less reads that
	 *              many
	 * @see AdminSecurityGroupUtils#getNonGroupMembers(String, String, long, long)
	 * @throws IllegalAccessException when the user cannot manage the group
	 */
	public static List<Map<String, Object>> getNonGroupMembers(User user, String groupId, String searchTerm, long limit,
			long offset) throws IllegalAccessException {
		checkCanManageGroup(user, groupId);
		return AdminSecurityGroupUtils.getUncheckedInstance().getNonGroupMembers(groupId, searchTerm,
				clampPageSize(limit), offset);
	}

	/**
	 * The projects a custom group has access to, so its managers know what the
	 * people they add can use.
	 *
	 * @param limit page size, at most {@link #MAX_PAGE_SIZE}; 0 or less reads that
	 *              many
	 * @see AdminSecurityGroupUtils#getProjectsForGroup(String, String, String,
	 *      long, long, boolean)
	 * @throws IllegalAccessException when the user cannot manage the group
	 */
	public static List<Map<String, Object>> getProjectsForGroup(User user, String groupId, String searchTerm,
			long limit, long offset, boolean onlyApps) throws IllegalAccessException {
		checkCanManageGroup(user, groupId);
		return AdminSecurityGroupUtils.getUncheckedInstance().getProjectsForGroup(groupId, CUSTOM_GROUP_TYPE,
				searchTerm, clampPageSize(limit), offset, onlyApps);
	}

	/**
	 * @see AdminSecurityGroupUtils#getNumProjectsForGroup(String, String, String,
	 *      boolean)
	 * @throws IllegalAccessException when the user cannot manage the group
	 */
	public static Long getNumProjectsForGroup(User user, String groupId, String searchTerm, boolean onlyApps)
			throws IllegalAccessException {
		checkCanManageGroup(user, groupId);
		return AdminSecurityGroupUtils.getUncheckedInstance().getNumProjectsForGroup(groupId, CUSTOM_GROUP_TYPE,
				searchTerm, onlyApps);
	}

	/**
	 * The engines a custom group has access to, so its managers know what the
	 * people they add can use.
	 *
	 * @param limit page size, at most {@link #MAX_PAGE_SIZE}; 0 or less reads that
	 *              many
	 * @see AdminSecurityGroupUtils#getEnginesForGroup(String, String, String, long,
	 *      long)
	 * @throws IllegalAccessException when the user cannot manage the group
	 */
	public static List<Map<String, Object>> getEnginesForGroup(User user, String groupId, String searchTerm, long limit,
			long offset) throws IllegalAccessException {
		checkCanManageGroup(user, groupId);
		return AdminSecurityGroupUtils.getUncheckedInstance().getEnginesForGroup(groupId, CUSTOM_GROUP_TYPE, searchTerm,
				clampPageSize(limit), offset);
	}

	/**
	 * @see AdminSecurityGroupUtils#getNumEnginesForGroup(String, String, String)
	 * @throws IllegalAccessException when the user cannot manage the group
	 */
	public static Long getNumEnginesForGroup(User user, String groupId, String searchTerm)
			throws IllegalAccessException {
		checkCanManageGroup(user, groupId);
		return AdminSecurityGroupUtils.getUncheckedInstance().getNumEnginesForGroup(groupId, CUSTOM_GROUP_TYPE,
				searchTerm);
	}

	/**
	 * Adds a member to a custom group, as a permission batch.
	 *
	 * @param user     the manager making the change
	 * @param groupId  the custom group
	 * @param userId   the new member's user id
	 * @param userType the new member's login type
	 * @param endDate  when the membership ends, or null or blank for never
	 * @throws IllegalAccessException   when the user does not manage the group
	 * @throws IllegalArgumentException when the person is already a member, does
	 *                                  not exist, or the end date cannot be read
	 */
	public static void addUserToGroup(User user, String groupId, String userId, String userType, String endDate)
			throws Exception {
		checkCanManageGroup(null, user, groupId);
		changeMember(user, false, PermissionChange.Action.ADD, groupId, userId, userType, endDate);
	}

	/**
	 * {@link #addUserToGroup(User, String, String, String, String)} for an admin,
	 * who proves it with the admin group utilities, so it is not checked again.
	 *
	 * @param adminGroupUtils the admin group utilities, from
	 *                        {@link AdminSecurityGroupUtils#getInstance(User)}
	 * @throws IllegalAccessException when no admin group utilities are passed
	 */
	public static void addUserToGroup(AdminSecurityGroupUtils adminGroupUtils, User user, String groupId, String userId,
			String userType, String endDate) throws Exception {
		requireAdmin(adminGroupUtils);
		checkCanManageGroup(adminGroupUtils, user, groupId);
		changeMember(user, true, PermissionChange.Action.ADD, groupId, userId, userType, endDate);
	}

	/**
	 * Adds or removes a member as a permission batch, once the caller's permissions
	 * have been checked, with these methods' own messages.
	 */
	private static void changeMember(User user, boolean asAdmin, PermissionChange.Action action, String groupId,
			String userId, String userType, String endDate) throws IllegalAccessException {
		String type;
		if (action == PermissionChange.Action.ADD) {
			type = normalizeUserType(userType);
			ValueUtils.requireNonBlank(userId, USER_ID_REQUIRED);
		} else {
			ValueUtils.requireNonBlank(userId, USER_ID_REQUIRED);
			type = normalizeUserType(userType);
		}
		String end = endDate == null || endDate.trim().isEmpty() ? null : endDate.trim();
		PermissionBatchUtils.saveChanges(user, PermissionTargetResource.team(groupId),
				List.of(new PermissionChange(action, PermissionSubject.ofUser(userId, type),
						new PermissionGrant(null, end))),
				asAdmin ? PermissionBatchOptions.adminMode() : PermissionBatchOptions.userMode(),
				new PermissionErrorRules(end).illegalAccess(PermissionProblem.Code.NO_PERMISSION, MANAGERS_ONLY)
						.illegalArgument(PermissionProblem.Code.INVALID_TARGET,
								"Group " + groupId + " is not a custom group")
						.illegalArgument(PermissionProblem.Code.ALREADY_HAS_ACCESS,
								"User " + userId + " already has access to group " + groupId)
						.illegalArgument(PermissionProblem.Code.NO_ACCESS,
								"User " + userId + " does not have access to group " + groupId));
	}

	/**
	 * Takes a member out of a custom group, as a permission batch.
	 *
	 * @param user     the manager making the change
	 * @param groupId  the custom group
	 * @param userId   the member's user id
	 * @param userType the member's login type
	 * @throws IllegalAccessException   when the user does not manage the group
	 * @throws IllegalArgumentException when the person is not a member
	 */
	public static void removeUserFromGroup(User user, String groupId, String userId, String userType) throws Exception {
		checkCanManageGroup(null, user, groupId);
		changeMember(user, false, PermissionChange.Action.REMOVE, groupId, userId, userType, null);
	}

	/**
	 * {@link #removeUserFromGroup(User, String, String, String)} for an admin, who
	 * proves it with the admin group utilities, so it is not checked again.
	 *
	 * @param adminGroupUtils the admin group utilities, from
	 *                        {@link AdminSecurityGroupUtils#getInstance(User)}
	 * @throws IllegalAccessException when no admin group utilities are passed
	 */
	public static void removeUserFromGroup(AdminSecurityGroupUtils adminGroupUtils, String groupId, String userId,
			String userType) throws Exception {
		requireAdmin(adminGroupUtils);
		checkCanManageGroup(adminGroupUtils, null, groupId);
		changeMember(null, true, PermissionChange.Action.REMOVE, groupId, userId, userType, null);
	}

	/*
	 * Checks
	 */

	private static void checkCanManageGroup(User user, String groupId) throws IllegalAccessException {
		checkCanManageGroup(null, user, groupId);
	}

	/**
	 * Someone who cannot see the group's members is told only that, whether or not
	 * the group exists or is a custom group, so they learn nothing about it. Only a
	 * custom group's members can be seen.
	 *
	 * @throws IllegalArgumentException when the group id is missing
	 * @throws IllegalAccessException   when the user cannot see the group's members
	 */
	static void checkCanViewGroupPeople(User user, String groupId, String projectId, String engineId)
			throws IllegalAccessException {
		ValueUtils.requireNonBlank(groupId, GROUP_ID_REQUIRED);
		if (!userCanViewGroupPeople(user, groupId, projectId, engineId)) {
			throw new IllegalAccessException(
					"Only this group's managers, and the owners and editors of what it can use, can see its members");
		}
	}

	/**
	 * @param adminGroupUtils the admin group utilities when the caller has already
	 *                        proved they are an admin, or null to check the user
	 * @param user            the user, when no admin group utilities are passed
	 * @param groupId         the group
	 * @throws IllegalArgumentException when the group id is missing, or the group
	 *                                  is not a custom group, which only admins and
	 *                                  the group's managers are told
	 * @throws IllegalAccessException   when no admin group utilities are passed and
	 *                                  the user is not one of the group's managers
	 */
	private static void checkCanManageGroup(AdminSecurityGroupUtils adminGroupUtils, User user, String groupId)
			throws IllegalAccessException {
		ValueUtils.requireNonBlank(groupId, GROUP_ID_REQUIRED);
		// someone who does not manage the group is told only that, so they cannot
		// tell a missing group or a login provider group from a custom group
		if (adminGroupUtils == null && !userIsGroupManager(user, groupId)) {
			throw new IllegalAccessException(MANAGERS_ONLY);
		}
		if (!AdminSecurityGroupUtils.getUncheckedInstance().isCustomGroup(groupId)) {
			throw new IllegalArgumentException("Group " + groupId + " is not a custom group");
		}
	}

	/**
	 * {@link AdminSecurityGroupUtils#getInstance(User)} returns the utilities only
	 * for admins, so holding them proves the caller is one.
	 *
	 * @throws IllegalAccessException when none are passed
	 */
	private static void requireAdmin(AdminSecurityGroupUtils adminGroupUtils) throws IllegalAccessException {
		if (adminGroupUtils == null) {
			throw new IllegalAccessException("This functionality is limited to only admins");
		}
	}

	/**
	 * @param userType a login type as a request sends it, in any case
	 * @return the type as the security database stores it
	 * @throws IllegalArgumentException when the type is missing
	 */
	static String normalizeUserType(String userType) {
		return AuthProvider.getProviderLabel(ValueUtils.requireNonBlank(userType, "Must define the user's login type"));
	}

	/**
	 * Matches the manager rows for the user's logins, each on its id and type.
	 *
	 * @return the filter, or null when the user has no logins
	 */
	private static IQueryFilter getManagerLoginFilter(User user) {
		if (user == null) {
			return null;
		}
		OrQueryFilter logins = new OrQueryFilter();
		for (AuthProvider login : user.getLogins()) {
			AccessToken token = user.getAccessToken(login);
			if (token == null || token.getId() == null) {
				continue;
			}
			// query filters escape their values, so the id goes in as the login holds it
			AndQueryFilter thisLogin = new AndQueryFilter();
			thisLogin.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPMANAGERS__USERID", "==", token.getId()));
			thisLogin.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPMANAGERS__TYPE", "==", login.getLabel()));
			logins.addFilter(thisLogin);
		}
		return logins.isEmpty() ? null : logins;
	}
}
