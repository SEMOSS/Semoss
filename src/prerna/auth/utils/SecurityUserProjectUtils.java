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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

class SecurityUserProjectUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityUserProjectUtils.class);

	/**
	 * Get user databases + global databases
	 * 
	 * @param userId
	 * @return
	 */
	public static List<String> getFullUserProjectIds(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		List<String> projectIdList = QueryExecutionUtility.flushToListString(securityDb, qs);
		return projectIdList;
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// String userFilters = getUserFilters(user);
		// String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM
		// ENGINEPERMISSION "
		// + "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters;
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					int permission = ((Number) val).intValue();
					return AccessPermissionEnum.getPermissionValueById(permission);
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the user's effective project permission.", e);
		}

		// see if project is public
		if (SecurityProjectUtils.projectIsGlobal(projectId)) {
			return AccessPermissionEnum.READ_ONLY.getPermission();
		}

		return null;
	}

	public static List<String> getActualGroupUserProjectPermission(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));

		// check if user has groups
		Collection<String> userGroups = getUserGroupFiltersQs(user);
		if (!userGroups.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", userGroups));
		} else {
			// If no groups - return empty list
			return new ArrayList<>();
		}

		List<String> permissions = new ArrayList<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					int permission = ((Number) val).intValue();
					permissions.add(AccessPermissionEnum.getPermissionValueById(permission));
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the user's effective group-based project permission.", e);
			throw new IllegalArgumentException("Error during getting the project permission");
		}

		// see if project is public
		if (SecurityProjectUtils.projectIsGlobal(projectId)) {
			permissions.add(AccessPermissionEnum.READ_ONLY.getPermission());
		}

		return permissions;
	}

	public static String getHighestProjectPermission(String userPermission, List<String> groupUserPermissions) {
		Map<Integer, String> map = new HashMap<>();
		if (userPermission != null) {
			map.put(AccessPermissionEnum.getIdByPermission(userPermission), userPermission);
		}
		if (groupUserPermissions != null) {
			for (String permission : groupUserPermissions) {
				map.put(AccessPermissionEnum.getIdByPermission(permission), permission);
			}
		}
		if (map.isEmpty()) {
			return null;
		}
		Set<Entry<Integer, String>> mpset = map.entrySet();
		int minPermissionId = Integer.MAX_VALUE;
		for (Entry<Integer, String> i : mpset) {
			minPermissionId = Math.min(minPermissionId, i.getKey());
		}
		return map.get(minPermissionId);
	}

	/**
	 * Get the project permissions for a specific user
	 * 
	 * @param singleUserId
	 * @param projectId
	 * @return
	 */
	public static Integer getUserProjectPermission(String singleUserId, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM
		// ENGINEPERMISSION "
		// + "WHERE ENGINEID='" + engineId + "' AND USERID='" + singleUserId + "'";
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", singleUserId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null && val instanceof Number) {
					return ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve user project permission.", e);
		}

		return null;
	}

	/**
	 * Get the project permissions for a specific user
	 * 
	 * @param user
	 * @param projectId
	 * @return
	 */
	public static Integer getUserProjectPermission(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		// TODO: account for different logins with different levels of access
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null && val instanceof Number) {
					return ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve user project permission.", e);
		}

		return null;
	}

	/**
	 * Determine if the user is the owner of a project
	 * 
	 * @param userFilters
	 * @param engineId
	 * @return
	 */
	public static boolean userIsOwner(User user, String projectId) {
		// Check to see if permission has expired
		try {
			boolean isExpired = projectPermissionIsExpired(User.getSingleLogginName(user), projectId);
			// If permission is expired remove permission
			if (isExpired) {
				SecurityProjectUtils.removeExpiredProjectUser(User.getSingleLogginName(user), projectId);
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user has owner-level access.", e);
		}

		return userIsOwner(getUserFiltersQs(user), projectId);
	}

	static boolean userIsOwner(Collection<String> userIds, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val == null) {
					return false;
				}
				int permission = ((Number) val).intValue();
				if (AccessPermissionEnum.isOwner(permission)) {
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user has owner-level access.", e);
		}

		return false;
	}

	/**
	 * Determine if a user can view a project
	 * 
	 * @param user
	 * @param projectId
	 * @return
	 */
	public static boolean userCanViewProject(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// Check to see if permission has expired
		try {
			boolean isExpired = projectPermissionIsExpired(User.getSingleLogginName(user), projectId);
			// If permission is expired remove permission
			if (isExpired) {
				SecurityProjectUtils.removeExpiredProjectUser(User.getSingleLogginName(user), projectId);
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user can view project.", e);
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		orFilter.addFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(orFilter);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				// if you are here, you can view
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user can view project.", e);
		}
		return false;
	}

	/**
	 * Determine if the user can modify the database
	 * 
	 * @param projectId
	 * @param userId
	 * @return
	 */
	public static boolean userCanEditProject(User user, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// Check to see if permission has expired
		try {
			boolean isExpired = projectPermissionIsExpired(User.getSingleLogginName(user), projectId);
			// If permission is expired remove permission
			if (isExpired) {
				SecurityProjectUtils.removeExpiredProjectUser(User.getSingleLogginName(user), projectId);
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user can edit project.", e);
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val == null) {
					return false;
				}
				int permission = ((Number) val).intValue();
				if (AccessPermissionEnum.isEditor(permission)) {
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user can edit project.", e);
		}
		return false;
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
			classLogger.error("Unable to retrieve the user's highest project permission.", e);
		}
		return AccessPermissionEnum.READ_ONLY.getId();
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
		// Check to see if permission has expired
		try {
			boolean isExpired = projectPermissionIsExpired(userId, projectId);
			// If permission is expired remove permission
			if (isExpired) {
				SecurityProjectUtils.removeExpiredProjectUser(userId, projectId);
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user has access to the project.", e);
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user has access to the project.", e);
			throw e;
		}
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
	public static List<Map<String, Object>> getProjectUsers(String projectId, String searchParam, String permission,
			long limit, long offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean hasSearchParam = searchParam != null && !(searchParam = searchParam.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission = permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE", "type"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		// return the end date of the permission
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__ENDDATE", "end_date"));
		// also return who did this and when
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSIONGRANTEDBY", "permission_granted_by"));
		qs.addSelector(
				new QueryColumnSelector("PROJECTPERMISSION__PERMISSIONGRANTEDBYTYPE", "permission_granted_by_type"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__DATEADDED", "date_added"));
		// filter to the project
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
	 * Check if permission to project has expired
	 * 
	 * @param engineId
	 * @param userId
	 */
	public static boolean projectPermissionIsExpired(String userId, String projectId) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		LocalDateTime currentTime = LocalDateTime.now();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__ENDDATE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				SemossDate endDate = (SemossDate) wrapper.next().getValues()[0];
				if (endDate == null) {
					return false;
				}
				LocalDateTime formattedEndDate = endDate.getLocalDateTime();
				return formattedEndDate.isBefore(currentTime);
			} else {
				return false;
			}
		} catch (Exception e) {
			classLogger.error("Unable to determine whether the project permission has expired.", e);
			throw e;
		}
	}
}
