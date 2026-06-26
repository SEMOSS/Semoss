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
package prerna.reactor.appprofile;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class AppProfileUtils {

	private static final Logger classLogger = LogManager.getLogger(AppProfileUtils.class);
	private static final String FEATURE_KEY_PATTERN = "^[a-zA-Z0-9\\-]+$";
	private static final int FEATURE_KEY_MAX_LENGTH = 100;

	private AppProfileUtils() {
	}

	// ─── Permission checks ──────────────────────────────────────────────────

	/**
	 * Returns true if the user has permission to manage profiles for the given app
	 * (admin, owner, or editor).
	 */
	public static boolean canManageProfiles(User user, String appId) {
		if (SecurityAdminUtils.userIsAdmin(user)) return true;
		if (!appExists(appId)) {
			throw new IllegalArgumentException("App not found: " + appId);
		}
		if (SecurityProjectUtils.userIsOwner(user, appId)) return true;
		if (SecurityProjectUtils.userCanEditProject(user, appId)) return true;
		return false;
	}

	/**
	 * Returns true if the user can assign/remove users from profiles (either as a
	 * full manager or as a delegated BU admin with 'assign' permission).
	 */
	public static boolean canAssignProfiles(User user, String appId) {
		if (canManageProfiles(user, appId)) return true;
		String userId = getUserId(user);
		if (userId == null) return false;
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector countFn = new QueryFunctionSelector();
		countFn.addInnerSelector(new QueryColumnSelector("APP_PROFILE_MANAGER__USER_ID"));
		countFn.setFunction(QueryFunctionHelper.COUNT);
		qs.addSelector(countFn);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_MANAGER__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_MANAGER__USER_ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_MANAGER__PERMISSION", "==", "assign"));
		Integer count = QueryExecutionUtility.flushToInteger(securityDb, qs);
		return count != null && count > 0;
	}

	/**
	 * Returns true if the user can evaluate features for the given app (admin,
	 * profile manager, or any project viewer).
	 */
	public static boolean canEvaluateFeatures(User user, String appId) {
		if (SecurityAdminUtils.userIsAdmin(user)) return true;
		if (canAssignProfiles(user, appId)) return true;
		return SecurityProjectUtils.userCanViewProject(user, appId);
	}

	private static boolean appExists(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "projectId"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", appId));
		return QueryExecutionUtility.flushToString(securityDb, qs) != null;
	}

	// ─── Profile CRUD ───────────────────────────────────────────────────────

	/**
	 * Creates a new named profile for an app and returns its metadata map.
	 */
	public static Map<String, Object> createProfile(String appId, String name, String description,
			boolean isDefault, boolean isGroup, User user) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Profile name cannot be blank.");
		}
		if (name.trim().length() > 100) {
			throw new IllegalArgumentException("Profile name cannot exceed 100 characters.");
		}
		String profileName = name.trim();
		String profileId = UUID.randomUUID().toString();
		String actorId = getUserId(user);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		if (isDefault) {
			clearDefaultProfile(securityDb, appId);
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO APP_PROFILE (PROFILE_ID, APP_ID, PROFILE_NAME, DESCRIPTION, IS_DEFAULT, IS_GROUP, CREATED_BY, CREATED_AT) VALUES (?,?,?,?,?,?,?,?)");
			int i = 1;
			ps.setString(i++, profileId);
			ps.setString(i++, appId);
			ps.setString(i++, profileName);
			ps.setString(i++, description);
			ps.setBoolean(i++, isDefault);
			ps.setBoolean(i++, isGroup);
			ps.setString(i++, actorId);
			ps.setTimestamp(i++, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to create app profile", e);
			throw new IllegalArgumentException("An error occurred creating the app profile.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		Map<String, Object> result = new HashMap<>();
		result.put("profileId", profileId);
		result.put("profileName", profileName);
		result.put("description", description);
		result.put("isDefault", isDefault);
		result.put("isGroup", isGroup);
		return result;
	}

	/**
	 * Updates mutable fields on an existing app profile (name, description,
	 * isDefault, isGroup). Null parameters are ignored.
	 */
	public static void updateProfile(String appId, String profileId, String name, String description,
			Boolean isDefault, Boolean isGroup, User user) {
		if (name != null) {
			if (name.trim().isEmpty()) throw new IllegalArgumentException("Profile name cannot be blank.");
			if (name.trim().length() > 100) throw new IllegalArgumentException("Profile name cannot exceed 100 characters.");
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (isDefault != null && isDefault) {
			clearDefaultProfile(securityDb, appId);
		}
		StringBuilder sb = new StringBuilder("UPDATE APP_PROFILE SET");
		List<Object> params = new ArrayList<>();
		if (name != null) { sb.append(" PROFILE_NAME=?,"); params.add(name.trim()); }
		if (description != null) { sb.append(" DESCRIPTION=?,"); params.add(description); }
		if (isDefault != null) { sb.append(" IS_DEFAULT=?,"); params.add(isDefault); }
		if (isGroup != null) { sb.append(" IS_GROUP=?,"); params.add(isGroup); }
		if (params.isEmpty()) return;
		sb.setLength(sb.length() - 1);
		sb.append(" WHERE PROFILE_ID=? AND APP_ID=?");
		params.add(profileId);
		params.add(appId);

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(sb.toString());
			for (int i = 0; i < params.size(); i++) {
				Object val = params.get(i);
				if (val instanceof Boolean) {
					ps.setBoolean(i + 1, (Boolean) val);
				} else {
					ps.setString(i + 1, (String) val);
				}
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to update app profile", e);
			throw new IllegalArgumentException("An error occurred updating the app profile.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Deletes a profile and cascades to its feature mappings, subgroups, and
	 * subgroup assignments. Throws if any users are still assigned.
	 */
	public static void deleteProfile(String appId, String profileId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int count = getAssignedUserCount(securityDb, appId, profileId);
		if (count > 0) {
			throw new IllegalArgumentException(
					"Cannot delete: " + count + " user(s) are assigned to this profile. Reassign them first.");
		}
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_PROFILE WHERE PROFILE_ID=? AND APP_ID=?");
			ps.setString(1, profileId);
			ps.setString(2, appId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to delete app profile {}", profileId, e);
			throw new IllegalArgumentException("An error occurred deleting the app profile.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_PROFILE_FEATURE WHERE PROFILE_ID=? AND APP_ID=?");
			ps.setString(1, profileId);
			ps.setString(2, appId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to cascade delete app profile features for profile {}", profileId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		// cascade delete subgroups
		List<String> subgroupIds = getSubgroupIdsForProfile(securityDb, appId, profileId);
		for (String subgroupId : subgroupIds) {
			deleteSubgroupInternal(securityDb, appId, subgroupId);
		}
		ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_PROFILE_SUBGROUP WHERE PROFILE_ID=? AND APP_ID=?");
			ps.setString(1, profileId);
			ps.setString(2, appId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to cascade delete subgroups for profile {}", profileId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Returns all profiles for an app, each with a live USER_COUNT from
	 * APP_USER_PROFILE via LEFT JOIN + GROUP BY.
	 */
	public static List<Map<String, Object>> getProfiles(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_ID", "profileId"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_NAME", "profileName"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__DESCRIPTION", "description"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__IS_DEFAULT", "isDefault"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__IS_GROUP", "isGroup"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__CREATED_BY", "createdBy"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__CREATED_AT", "createdAt"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, "APP_USER_PROFILE__USER_ID", "userCount"));
		qs.addRelation("APP_PROFILE__PROFILE_ID", "APP_USER_PROFILE__PROFILE_ID", "left.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE__APP_ID", "==", appId));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__PROFILE_ID"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__PROFILE_NAME"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__DESCRIPTION"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__IS_DEFAULT"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__IS_GROUP"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__CREATED_BY"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__CREATED_AT"));
		qs.addOrderBy(new QueryColumnOrderBySelector("APP_PROFILE__PROFILE_NAME"));
		List<Map<String, Object>> profiles = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		profiles.forEach(p -> {
			p.put("isDefault", Boolean.TRUE.equals(p.get("isDefault")));
			p.put("isGroup", Boolean.TRUE.equals(p.get("isGroup")));
		});
		return profiles;
	}

	// ─── Feature CRUD ───────────────────────────────────────────────────────

	/**
	 * Creates a new app-level feature definition and returns its metadata map.
	 */
	public static Map<String, Object> createFeature(String appId, String featureKey,
			String description, User user) {
		validateFeatureKey(featureKey);
		String featureId = UUID.randomUUID().toString();
		String actorId = getUserId(user);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO APP_FEATURE (FEATURE_ID, APP_ID, FEATURE_KEY, DESCRIPTION, CREATED_BY, CREATED_AT) VALUES (?,?,?,?,?,?)");
			int i = 1;
			ps.setString(i++, featureId);
			ps.setString(i++, appId);
			ps.setString(i++, featureKey);
			ps.setString(i++, description);
			ps.setString(i++, actorId);
			ps.setTimestamp(i++, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to create app feature", e);
			throw new IllegalArgumentException("An error occurred creating the feature.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		Map<String, Object> result = new HashMap<>();
		result.put("featureId", featureId);
		result.put("featureKey", featureKey);
		result.put("description", description);
		return result;
	}

	/**
	 * Updates the key and/or description of an existing app feature. Null
	 * parameters are ignored.
	 */
	public static void updateFeature(String appId, String featureId, String featureKey,
			String description, User user) {
		if (featureKey != null) validateFeatureKey(featureKey);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		StringBuilder sb = new StringBuilder("UPDATE APP_FEATURE SET");
		List<String> params = new ArrayList<>();
		if (featureKey != null) { sb.append(" FEATURE_KEY=?,"); params.add(featureKey); }
		if (description != null) { sb.append(" DESCRIPTION=?,"); params.add(description); }
		if (params.isEmpty()) return;
		sb.setLength(sb.length() - 1);
		sb.append(" WHERE FEATURE_ID=? AND APP_ID=?");
		params.add(featureId);
		params.add(appId);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(sb.toString());
			for (int i = 0; i < params.size(); i++) {
				ps.setString(i + 1, params.get(i));
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to update app feature", e);
			throw new IllegalArgumentException("An error occurred updating the feature.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Deletes an app feature and cascades removal from profile and subgroup feature
	 * mapping tables.
	 */
	public static void deleteFeature(String appId, String featureId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_FEATURE WHERE FEATURE_ID=? AND APP_ID=?");
			ps.setString(1, featureId);
			ps.setString(2, appId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to delete app feature", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_PROFILE_FEATURE WHERE FEATURE_ID=? AND APP_ID=?");
			ps.setString(1, featureId);
			ps.setString(2, appId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to cascade delete feature from profile mappings", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_SUBGROUP_FEATURE WHERE FEATURE_ID=? AND APP_ID=?");
			ps.setString(1, featureId);
			ps.setString(2, appId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to cascade delete feature from subgroup mappings", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Returns all feature definitions for an app, ordered by FEATURE_KEY ascending.
	 */
	public static List<Map<String, Object>> getFeatures(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_ID", "featureId"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_KEY", "featureKey"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__DESCRIPTION", "description"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__CREATED_BY", "createdBy"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__CREATED_AT", "createdAt"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE__APP_ID", "==", appId));
		qs.addOrderBy(new QueryColumnOrderBySelector("APP_FEATURE__FEATURE_KEY"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	// ─── Profile-Feature Assignment ─────────────────────────────────────────

	/**
	 * Sets the enabled state of a feature for a profile (upsert via delete+insert).
	 */
	public static void setProfileFeature(String appId, String profileId, String featureId,
			boolean enabled, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"DELETE FROM APP_PROFILE_FEATURE WHERE APP_ID=? AND PROFILE_ID=? AND FEATURE_ID=?");
			ps.setString(1, appId);
			ps.setString(2, profileId);
			ps.setString(3, featureId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to delete existing profile feature row", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO APP_PROFILE_FEATURE (APP_ID, PROFILE_ID, FEATURE_ID, ENABLED) VALUES (?,?,?,?)");
			ps.setString(1, appId);
			ps.setString(2, profileId);
			ps.setString(3, featureId);
			ps.setBoolean(4, enabled);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to insert profile feature", e);
			throw new IllegalArgumentException("An error occurred setting the profile feature.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Returns all features for an app merged with their enabled state for the given
	 * profile. Defaults to false for features without an explicit assignment.
	 */
	public static List<Map<String, Object>> getProfileFeatures(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs1 = new SelectQueryStruct();
		qs1.addSelector(new QueryColumnSelector("APP_PROFILE_FEATURE__FEATURE_ID", "featureId"));
		qs1.addSelector(new QueryColumnSelector("APP_PROFILE_FEATURE__ENABLED", "enabled"));
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_FEATURE__APP_ID", "==", appId));
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_FEATURE__PROFILE_ID", "==", profileId));
		Map<String, Boolean> enabledMap = new HashMap<>();
		for (Map<String, Object> r : QueryExecutionUtility.flushRsToMap(securityDb, qs1)) {
			enabledMap.put((String) r.get("featureId"), Boolean.TRUE.equals(r.get("enabled")));
		}
		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs2.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_ID", "featureId"));
		qs2.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_KEY", "featureKey"));
		qs2.addSelector(new QueryColumnSelector("APP_FEATURE__DESCRIPTION", "description"));
		qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE__APP_ID", "==", appId));
		qs2.addOrderBy(new QueryColumnOrderBySelector("APP_FEATURE__FEATURE_KEY"));
		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(securityDb, qs2);
		results.forEach(r -> r.put("enabled", enabledMap.getOrDefault((String) r.get("featureId"), Boolean.FALSE)));
		return results;
	}

	// ─── User-Profile Assignment (multi-profile) ────────────────────────────

	/**
	 * Assigns a user to a profile. A user can be in multiple profiles simultaneously.
	 * If already assigned to this specific profile, this is a no-op.
	 */
	public static void assignUserProfile(String appId, String userId, String profileId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// check for existing assignment to avoid duplicates
		SelectQueryStruct checkQs = new SelectQueryStruct();
		QueryFunctionSelector countFn = new QueryFunctionSelector();
		countFn.addInnerSelector(new QueryColumnSelector("APP_USER_PROFILE__USER_ID"));
		countFn.setFunction(QueryFunctionHelper.COUNT);
		checkQs.addSelector(countFn);
		checkQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__APP_ID", "==", appId));
		checkQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__USER_ID", "==", userId));
		checkQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__PROFILE_ID", "==", profileId));
		Integer existingCount = QueryExecutionUtility.flushToInteger(securityDb, checkQs);
		if (existingCount != null && existingCount > 0) return;

		String actorId = getUserId(actor);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO APP_USER_PROFILE (APP_ID, USER_ID, PROFILE_ID, ASSIGNED_BY, ASSIGNED_AT) VALUES (?,?,?,?,?)");
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.setString(3, profileId);
			ps.setString(4, actorId);
			ps.setTimestamp(5, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to assign user profile", e);
			throw new IllegalArgumentException("An error occurred assigning the user profile.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Removes ALL profile assignments for a user from an app (used when removing user from app entirely).
	 */
	public static void removeUserProfile(String appId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_USER_PROFILE WHERE APP_ID=? AND USER_ID=?");
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to remove all user profile assignments", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		// Also remove all subgroup assignments
		ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_USER_SUBGROUP WHERE APP_ID=? AND USER_ID=?");
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to remove user subgroup assignments", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Removes a user from a specific profile assignment.
	 */
	public static void removeUserProfile(String appId, String userId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"DELETE FROM APP_USER_PROFILE WHERE APP_ID=? AND USER_ID=? AND PROFILE_ID=?");
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.setString(3, profileId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to remove user profile assignment", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Returns all explicit profile assignments for a user in an app.
	 * Each entry includes profileId, profileName, isGroup.
	 */
	public static List<Map<String, Object>> getUserProfiles(String appId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		return getExplicitUserProfiles(securityDb, appId, userId);
	}

	/**
	 * Returns a structured summary of the calling user's profile memberships:
	 * - "profiles": list of directly-assigned standard profiles
	 * - "groups": map of parent profile name -> list of subgroup names the user is in
	 */
	public static Map<String, Object> getUserAppProfiles(String appId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = getUserId(user);

		Map<String, Object> result = new LinkedHashMap<>();
		List<Map<String, Object>> directProfiles = new ArrayList<>();
		Map<String, List<String>> groupMemberships = new LinkedHashMap<>();

		// Standard profile assignments
		List<Map<String, Object>> explicitProfiles = getExplicitUserProfiles(securityDb, appId, userId);
		boolean hasExplicit = !explicitProfiles.isEmpty();

		for (Map<String, Object> p : explicitProfiles) {
			boolean isGroup = (Boolean) p.getOrDefault("isGroup", Boolean.FALSE);
			if (!isGroup) {
				Map<String, Object> entry = new LinkedHashMap<>();
				entry.put("profileId", p.get("profileId"));
				entry.put("profileName", p.get("profileName"));
				entry.put("isDefault", false);
				directProfiles.add(entry);
			}
		}

		// Fall back to default standard profile if no explicit assignment
		if (!hasExplicit) {
			Map<String, Object> defaultProfile = getDefaultProfile(securityDb, appId);
			if (defaultProfile != null && !(Boolean) defaultProfile.getOrDefault("isGroup", Boolean.FALSE)) {
				Map<String, Object> entry = new LinkedHashMap<>();
				entry.put("profileId", defaultProfile.get("profileId"));
				entry.put("profileName", defaultProfile.get("profileName"));
				entry.put("isDefault", true);
				directProfiles.add(entry);
			}
		}

		// Subgroup memberships
		List<Map<String, Object>> subgroups = getExplicitUserSubgroups(securityDb, appId, userId);
		for (Map<String, Object> sg : subgroups) {
			String parentProfileName = (String) sg.get("profileName");
			String subgroupName = (String) sg.get("subgroupName");
			groupMemberships.computeIfAbsent(parentProfileName, k -> new ArrayList<>()).add(subgroupName);
		}

		result.put("profiles", directProfiles);
		result.put("groups", groupMemberships);
		return result;
	}

	/**
	 * Returns all users assigned to a profile, including display name and email via
	 * JOIN with SMSS_USER.
	 */
	public static List<Map<String, Object>> getProfileUsers(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_USER_PROFILE__USER_ID", "userId"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addSelector(new QueryColumnSelector("APP_USER_PROFILE__ASSIGNED_BY", "assignedBy"));
		qs.addSelector(new QueryColumnSelector("APP_USER_PROFILE__ASSIGNED_AT", "assignedAt"));
		qs.addRelation("APP_USER_PROFILE__USER_ID", "SMSS_USER__ID", "left.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__PROFILE_ID", "==", profileId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	// ─── Subgroup CRUD ───────────────────────────────────────────────────────

	/**
	 * Creates a new named sub-group within a group-style profile and returns its
	 * metadata map.
	 */
	public static Map<String, Object> createSubgroup(String appId, String profileId, String name,
			String description, User user) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Subgroup name cannot be blank.");
		}
		if (name.trim().length() > 100) {
			throw new IllegalArgumentException("Subgroup name cannot exceed 100 characters.");
		}
		// Verify parent profile is group-style
		if (!isGroupProfile(appId, profileId)) {
			throw new IllegalArgumentException("Sub-groups can only be added to group-style profiles.");
		}
		String subgroupId = UUID.randomUUID().toString();
		String actorId = getUserId(user);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO APP_PROFILE_SUBGROUP (SUBGROUP_ID, PROFILE_ID, APP_ID, SUBGROUP_NAME, DESCRIPTION, CREATED_BY, CREATED_AT) VALUES (?,?,?,?,?,?,?)");
			int i = 1;
			ps.setString(i++, subgroupId);
			ps.setString(i++, profileId);
			ps.setString(i++, appId);
			ps.setString(i++, name.trim());
			ps.setString(i++, description);
			ps.setString(i++, actorId);
			ps.setTimestamp(i++, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to create subgroup", e);
			throw new IllegalArgumentException("An error occurred creating the subgroup.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		Map<String, Object> result = new HashMap<>();
		result.put("subgroupId", subgroupId);
		result.put("profileId", profileId);
		result.put("subgroupName", name.trim());
		result.put("description", description);
		return result;
	}

	/**
	 * Updates the name and/or description of an existing sub-group. Null parameters
	 * are ignored.
	 */
	public static void updateSubgroup(String appId, String subgroupId, String name,
			String description, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		StringBuilder sb = new StringBuilder("UPDATE APP_PROFILE_SUBGROUP SET");
		List<String> params = new ArrayList<>();
		if (name != null) {
			if (name.trim().isEmpty()) throw new IllegalArgumentException("Subgroup name cannot be blank.");
			sb.append(" SUBGROUP_NAME=?,"); params.add(name.trim());
		}
		if (description != null) { sb.append(" DESCRIPTION=?,"); params.add(description); }
		if (params.isEmpty()) return;
		sb.setLength(sb.length() - 1);
		sb.append(" WHERE SUBGROUP_ID=? AND APP_ID=?");
		params.add(subgroupId);
		params.add(appId);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(sb.toString());
			for (int i = 0; i < params.size(); i++) {
				ps.setString(i + 1, params.get(i));
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to update subgroup", e);
			throw new IllegalArgumentException("An error occurred updating the subgroup.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Deletes a sub-group and its user and feature assignments.
	 */
	public static void deleteSubgroup(String appId, String subgroupId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		deleteSubgroupInternal(securityDb, appId, subgroupId);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_PROFILE_SUBGROUP WHERE SUBGROUP_ID=? AND APP_ID=?");
			ps.setString(1, subgroupId);
			ps.setString(2, appId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to delete subgroup", e);
			throw new IllegalArgumentException("An error occurred deleting the subgroup.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Returns all sub-groups for a profile, each with a live USER_COUNT from
	 * APP_USER_SUBGROUP via LEFT JOIN + GROUP BY.
	 */
	public static List<Map<String, Object>> getSubgroups(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_SUBGROUP__SUBGROUP_ID", "subgroupId"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_SUBGROUP__SUBGROUP_NAME", "subgroupName"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_SUBGROUP__DESCRIPTION", "description"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_SUBGROUP__CREATED_BY", "createdBy"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_SUBGROUP__CREATED_AT", "createdAt"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, "APP_USER_SUBGROUP__USER_ID", "userCount"));
		qs.addRelation("APP_PROFILE_SUBGROUP__SUBGROUP_ID", "APP_USER_SUBGROUP__SUBGROUP_ID", "left.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_SUBGROUP__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_SUBGROUP__PROFILE_ID", "==", profileId));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE_SUBGROUP__SUBGROUP_ID"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE_SUBGROUP__SUBGROUP_NAME"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE_SUBGROUP__DESCRIPTION"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE_SUBGROUP__CREATED_BY"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE_SUBGROUP__CREATED_AT"));
		qs.addOrderBy(new QueryColumnOrderBySelector("APP_PROFILE_SUBGROUP__SUBGROUP_NAME"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Returns all users assigned to a sub-group, including display name and email
	 * via JOIN with SMSS_USER.
	 */
	public static List<Map<String, Object>> getSubgroupUsers(String appId, String subgroupId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_USER_SUBGROUP__USER_ID", "userId"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addSelector(new QueryColumnSelector("APP_USER_SUBGROUP__ASSIGNED_BY", "assignedBy"));
		qs.addSelector(new QueryColumnSelector("APP_USER_SUBGROUP__ASSIGNED_AT", "assignedAt"));
		qs.addRelation("APP_USER_SUBGROUP__USER_ID", "SMSS_USER__ID", "left.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_SUBGROUP__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_SUBGROUP__SUBGROUP_ID", "==", subgroupId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	// ─── Subgroup-Feature Assignment ────────────────────────────────────────

	/**
	 * Sets the enabled state of a feature for a sub-group (upsert via
	 * delete+insert).
	 */
	public static void setSubgroupFeature(String appId, String subgroupId, String featureId,
			boolean enabled, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"DELETE FROM APP_SUBGROUP_FEATURE WHERE APP_ID=? AND SUBGROUP_ID=? AND FEATURE_ID=?");
			ps.setString(1, appId);
			ps.setString(2, subgroupId);
			ps.setString(3, featureId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to delete existing subgroup feature row", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO APP_SUBGROUP_FEATURE (APP_ID, SUBGROUP_ID, FEATURE_ID, ENABLED) VALUES (?,?,?,?)");
			ps.setString(1, appId);
			ps.setString(2, subgroupId);
			ps.setString(3, featureId);
			ps.setBoolean(4, enabled);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to insert subgroup feature", e);
			throw new IllegalArgumentException("An error occurred setting the subgroup feature.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Returns all features for an app merged with their enabled state for the given
	 * sub-group. Defaults to false for features without an explicit assignment.
	 */
	public static List<Map<String, Object>> getSubgroupFeatures(String appId, String subgroupId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs1 = new SelectQueryStruct();
		qs1.addSelector(new QueryColumnSelector("APP_SUBGROUP_FEATURE__FEATURE_ID", "featureId"));
		qs1.addSelector(new QueryColumnSelector("APP_SUBGROUP_FEATURE__ENABLED", "enabled"));
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_SUBGROUP_FEATURE__APP_ID", "==", appId));
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_SUBGROUP_FEATURE__SUBGROUP_ID", "==", subgroupId));
		Map<String, Boolean> enabledMap = new HashMap<>();
		for (Map<String, Object> r : QueryExecutionUtility.flushRsToMap(securityDb, qs1)) {
			enabledMap.put((String) r.get("featureId"), Boolean.TRUE.equals(r.get("enabled")));
		}
		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs2.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_ID", "featureId"));
		qs2.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_KEY", "featureKey"));
		qs2.addSelector(new QueryColumnSelector("APP_FEATURE__DESCRIPTION", "description"));
		qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE__APP_ID", "==", appId));
		qs2.addOrderBy(new QueryColumnOrderBySelector("APP_FEATURE__FEATURE_KEY"));
		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(securityDb, qs2);
		results.forEach(r -> r.put("enabled", enabledMap.getOrDefault((String) r.get("featureId"), Boolean.FALSE)));
		return results;
	}

	// ─── User-Subgroup Assignment ────────────────────────────────────────────

	/**
	 * Assigns a user to a sub-group. If already assigned, this is a no-op.
	 */
	public static void assignUserSubgroup(String appId, String userId, String subgroupId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// check for existing assignment
		SelectQueryStruct checkQs = new SelectQueryStruct();
		QueryFunctionSelector countFn = new QueryFunctionSelector();
		countFn.addInnerSelector(new QueryColumnSelector("APP_USER_SUBGROUP__USER_ID"));
		countFn.setFunction(QueryFunctionHelper.COUNT);
		checkQs.addSelector(countFn);
		checkQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_SUBGROUP__APP_ID", "==", appId));
		checkQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_SUBGROUP__USER_ID", "==", userId));
		checkQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_SUBGROUP__SUBGROUP_ID", "==", subgroupId));
		Integer existingCount = QueryExecutionUtility.flushToInteger(securityDb, checkQs);
		if (existingCount != null && existingCount > 0) return;

		String actorId = getUserId(actor);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO APP_USER_SUBGROUP (APP_ID, USER_ID, SUBGROUP_ID, ASSIGNED_BY, ASSIGNED_AT) VALUES (?,?,?,?,?)");
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.setString(3, subgroupId);
			ps.setString(4, actorId);
			ps.setTimestamp(5, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to assign user to subgroup", e);
			throw new IllegalArgumentException("An error occurred assigning the user to the subgroup.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Removes a user from a specific sub-group assignment.
	 */
	public static void removeUserSubgroup(String appId, String userId, String subgroupId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"DELETE FROM APP_USER_SUBGROUP WHERE APP_ID=? AND USER_ID=? AND SUBGROUP_ID=?");
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.setString(3, subgroupId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to remove user from subgroup", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	// ─── Profile Manager (delegated BU admin) ────────────────────────────────

	/**
	 * Grants a user delegated 'assign' permission to manage profile assignments for
	 * an app. If already a manager, this is a no-op.
	 */
	public static void addProfileManager(String appId, String userId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// check for existing
		SelectQueryStruct checkQs = new SelectQueryStruct();
		QueryFunctionSelector countFn = new QueryFunctionSelector();
		countFn.addInnerSelector(new QueryColumnSelector("APP_PROFILE_MANAGER__USER_ID"));
		countFn.setFunction(QueryFunctionHelper.COUNT);
		checkQs.addSelector(countFn);
		checkQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_MANAGER__APP_ID", "==", appId));
		checkQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_MANAGER__USER_ID", "==", userId));
		Integer existingCount = QueryExecutionUtility.flushToInteger(securityDb, checkQs);
		if (existingCount != null && existingCount > 0) return;

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO APP_PROFILE_MANAGER (APP_ID, USER_ID, PERMISSION) VALUES (?,?,'assign')");
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to add profile manager", e);
			throw new IllegalArgumentException("An error occurred adding the profile manager.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Revokes delegated profile manager permission from a user for an app.
	 */
	public static void removeProfileManager(String appId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_PROFILE_MANAGER WHERE APP_ID=? AND USER_ID=?");
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to remove profile manager", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Returns all users with delegated profile manager permission for an app, with
	 * display name and email via JOIN with SMSS_USER.
	 */
	public static List<Map<String, Object>> getProfileManagers(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_MANAGER__USER_ID", "userId"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_MANAGER__PERMISSION", "permission"));
		qs.addRelation("APP_PROFILE_MANAGER__USER_ID", "SMSS_USER__ID", "left.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_MANAGER__APP_ID", "==", appId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	// ─── Feature evaluation ──────────────────────────────────────────────────

	/**
	 * Returns true if the given feature key is enabled for the calling user in the
	 * app, evaluated across all assigned profiles, subgroups, and the default
	 * profile fallback.
	 */
	public static boolean checkFeature(String appId, String featureKey, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String featureId = resolveFeatureId(securityDb, appId, featureKey);
		if (featureId == null) return false;
		String userId = getUserId(user);

		// Check across all standard profiles
		List<Map<String, Object>> profiles = getExplicitUserProfiles(securityDb, appId, userId);
		if (profiles.isEmpty()) {
			Map<String, Object> defaultProfile = getDefaultProfile(securityDb, appId);
			if (defaultProfile != null && !(Boolean) defaultProfile.getOrDefault("isGroup", Boolean.FALSE)) {
				if (queryFeatureEnabled(securityDb, appId, (String) defaultProfile.get("profileId"), featureId)) {
					return true;
				}
			}
		} else {
			for (Map<String, Object> p : profiles) {
				if (!(Boolean) p.getOrDefault("isGroup", Boolean.FALSE)) {
					if (queryFeatureEnabled(securityDb, appId, (String) p.get("profileId"), featureId)) {
						return true;
					}
				}
			}
		}

		// Check across all subgroup memberships, plus the parent group profile's base features
		List<Map<String, Object>> subgroups = getExplicitUserSubgroups(securityDb, appId, userId);
		for (Map<String, Object> sg : subgroups) {
			if (querySubgroupFeatureEnabled(securityDb, appId, (String) sg.get("subgroupId"), featureId)) {
				return true;
			}
			// Group profile base features apply to all subgroup members
			String parentProfileId = (String) sg.get("profileId");
			if (parentProfileId != null && queryFeatureEnabled(securityDb, appId, parentProfileId, featureId)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Returns all enabled features for the calling user, across all profiles and
	 * subgroup memberships (union). Falls back to the default profile if unassigned.
	 */
	public static Map<String, Object> getUserFeatures(String appId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = getUserId(user);
		Map<String, Object> result = new HashMap<>();

		// Standard profile features
		List<Map<String, Object>> explicitProfiles = getExplicitUserProfiles(securityDb, appId, userId);
		if (explicitProfiles.isEmpty()) {
			Map<String, Object> defaultProfile = getDefaultProfile(securityDb, appId);
			if (defaultProfile != null && !(Boolean) defaultProfile.getOrDefault("isGroup", Boolean.FALSE)) {
				addProfileFeaturesToResult(securityDb, appId,
						(String) defaultProfile.get("profileId"),
						(String) defaultProfile.get("profileName"),
						true, result);
			}
		} else {
			for (Map<String, Object> p : explicitProfiles) {
				if (!(Boolean) p.getOrDefault("isGroup", Boolean.FALSE)) {
					addProfileFeaturesToResult(securityDb, appId,
							(String) p.get("profileId"),
							(String) p.get("profileName"),
							false, result);
				}
			}
		}

		// Subgroup features + parent group profile base features
		List<Map<String, Object>> subgroups = getExplicitUserSubgroups(securityDb, appId, userId);
		for (Map<String, Object> sg : subgroups) {
			addSubgroupFeaturesToResult(securityDb, appId,
					(String) sg.get("subgroupId"),
					(String) sg.get("subgroupName"),
					(String) sg.get("profileName"),
					result);
			// Group profile base features apply to all subgroup members
			String parentProfileId = (String) sg.get("profileId");
			if (parentProfileId != null) {
				addProfileFeaturesToResult(securityDb, appId,
						parentProfileId,
						(String) sg.get("profileName"),
						false, result);
			}
		}

		return result;
	}

	// ─── Private helpers ─────────────────────────────────────────────────────

	private static void validateFeatureKey(String key) {
		if (key == null || key.isEmpty()) {
			throw new IllegalArgumentException("Feature key cannot be blank.");
		}
		if (key.length() > FEATURE_KEY_MAX_LENGTH) {
			throw new IllegalArgumentException("Feature key cannot exceed " + FEATURE_KEY_MAX_LENGTH + " characters.");
		}
		if (!key.matches(FEATURE_KEY_PATTERN)) {
			throw new IllegalArgumentException(
					"Feature key must contain only alphanumeric characters and hyphens.");
		}
	}

	private static void clearDefaultProfile(IRDBMSEngine securityDb, String appId) {
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE APP_PROFILE SET IS_DEFAULT=? WHERE APP_ID=?");
			ps.setBoolean(1, false);
			ps.setString(2, appId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to clear default profile", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	private static int getAssignedUserCount(IRDBMSEngine securityDb, String appId, String profileId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector countFn = new QueryFunctionSelector();
		countFn.addInnerSelector(new QueryColumnSelector("APP_USER_PROFILE__USER_ID"));
		countFn.setFunction(QueryFunctionHelper.COUNT);
		qs.addSelector(countFn);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__PROFILE_ID", "==", profileId));
		Integer count = QueryExecutionUtility.flushToInteger(securityDb, qs);
		return count != null ? count : 0;
	}


	private static Map<String, Object> getDefaultProfile(IRDBMSEngine securityDb, String appId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_ID", "profileId"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_NAME", "profileName"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__IS_GROUP", "isGroup"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE__IS_DEFAULT", "==", Boolean.TRUE));
		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		if (rows.isEmpty()) return null;
		Map<String, Object> row = rows.get(0);
		row.put("isGroup", Boolean.TRUE.equals(row.get("isGroup")));
		row.put("isExplicitAssignment", false);
		return row;
	}

	/**
	 * Returns all explicit profile assignments for a user (not falling back to default).
	 */
	private static List<Map<String, Object>> getExplicitUserProfiles(IRDBMSEngine securityDb,
			String appId, String userId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_ID", "profileId"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_NAME", "profileName"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__IS_GROUP", "isGroup"));
		qs.addRelation("APP_USER_PROFILE__PROFILE_ID", "APP_PROFILE__PROFILE_ID", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__USER_ID", "==", userId));
		List<Map<String, Object>> profiles = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		profiles.forEach(r -> r.put("isGroup", Boolean.TRUE.equals(r.get("isGroup"))));
		return profiles;
	}

	/**
	 * Returns all subgroup assignments for a user, including parent profile name.
	 */
	private static List<Map<String, Object>> getExplicitUserSubgroups(IRDBMSEngine securityDb,
			String appId, String userId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_USER_SUBGROUP__SUBGROUP_ID", "subgroupId"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_SUBGROUP__SUBGROUP_NAME", "subgroupName"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_ID", "profileId"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_NAME", "profileName"));
		qs.addRelation("APP_USER_SUBGROUP__SUBGROUP_ID", "APP_PROFILE_SUBGROUP__SUBGROUP_ID", "inner.join");
		qs.addRelation("APP_PROFILE_SUBGROUP__PROFILE_ID", "APP_PROFILE__PROFILE_ID", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_SUBGROUP__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_SUBGROUP__USER_ID", "==", userId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	private static void addProfileFeaturesToResult(IRDBMSEngine securityDb, String appId,
			String profileId, String profileName, boolean isDefaultProfile,
			Map<String, Object> result) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_KEY", "featureKey"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_ID", "featureId"));
		qs.addRelation("APP_PROFILE_FEATURE__FEATURE_ID", "APP_FEATURE__FEATURE_ID", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_FEATURE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_FEATURE__PROFILE_ID", "==", profileId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_FEATURE__ENABLED", "==", Boolean.TRUE));
		for (Map<String, Object> row : QueryExecutionUtility.flushRsToMap(securityDb, qs)) {
			String featureKey = (String) row.get("featureKey");
			if (!result.containsKey(featureKey)) {
				Map<String, Object> featureInfo = new HashMap<>();
				featureInfo.put("featureId", row.get("featureId"));
				featureInfo.put("profileName", profileName);
				featureInfo.put("isDefaultProfile", isDefaultProfile);
				result.put(featureKey, featureInfo);
			}
		}
	}

	private static void addSubgroupFeaturesToResult(IRDBMSEngine securityDb, String appId,
			String subgroupId, String subgroupName, String parentProfileName,
			Map<String, Object> result) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_KEY", "featureKey"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_ID", "featureId"));
		qs.addRelation("APP_SUBGROUP_FEATURE__FEATURE_ID", "APP_FEATURE__FEATURE_ID", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_SUBGROUP_FEATURE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_SUBGROUP_FEATURE__SUBGROUP_ID", "==", subgroupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_SUBGROUP_FEATURE__ENABLED", "==", Boolean.TRUE));
		for (Map<String, Object> row : QueryExecutionUtility.flushRsToMap(securityDb, qs)) {
			String featureKey = (String) row.get("featureKey");
			if (!result.containsKey(featureKey)) {
				Map<String, Object> featureInfo = new HashMap<>();
				featureInfo.put("featureId", row.get("featureId"));
				featureInfo.put("profileName", parentProfileName);
				featureInfo.put("subgroupName", subgroupName);
				featureInfo.put("isDefaultProfile", false);
				result.put(featureKey, featureInfo);
			}
		}
	}

	private static String resolveFeatureId(IRDBMSEngine securityDb, String appId, String featureKey) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_ID", "featureId"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE__FEATURE_KEY", "==", featureKey));
		return QueryExecutionUtility.flushToString(securityDb, qs);
	}

	private static boolean queryFeatureEnabled(IRDBMSEngine securityDb, String appId,
			String profileId, String featureId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_FEATURE__ENABLED", "enabled"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_FEATURE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_FEATURE__PROFILE_ID", "==", profileId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_FEATURE__FEATURE_ID", "==", featureId));
		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		return !rows.isEmpty() && Boolean.TRUE.equals(rows.get(0).get("enabled"));
	}

	private static boolean querySubgroupFeatureEnabled(IRDBMSEngine securityDb, String appId,
			String subgroupId, String featureId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_SUBGROUP_FEATURE__ENABLED", "enabled"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_SUBGROUP_FEATURE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_SUBGROUP_FEATURE__SUBGROUP_ID", "==", subgroupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_SUBGROUP_FEATURE__FEATURE_ID", "==", featureId));
		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		return !rows.isEmpty() && Boolean.TRUE.equals(rows.get(0).get("enabled"));
	}

	private static boolean isGroupProfile(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__IS_GROUP", "isGroup"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE__PROFILE_ID", "==", profileId));
		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		return !rows.isEmpty() && Boolean.TRUE.equals(rows.get(0).get("isGroup"));
	}

	private static List<String> getSubgroupIdsForProfile(IRDBMSEngine securityDb, String appId, String profileId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_SUBGROUP__SUBGROUP_ID", "subgroupId"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_SUBGROUP__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_SUBGROUP__PROFILE_ID", "==", profileId));
		List<String> ids = new ArrayList<>();
		for (Map<String, Object> r : QueryExecutionUtility.flushRsToMap(securityDb, qs)) {
			ids.add((String) r.get("subgroupId"));
		}
		return ids;
	}

	private static void deleteSubgroupInternal(IRDBMSEngine securityDb, String appId, String subgroupId) {
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_USER_SUBGROUP WHERE APP_ID=? AND SUBGROUP_ID=?");
			ps.setString(1, appId);
			ps.setString(2, subgroupId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to cascade delete subgroup user assignments", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_SUBGROUP_FEATURE WHERE APP_ID=? AND SUBGROUP_ID=?");
			ps.setString(1, appId);
			ps.setString(2, subgroupId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to cascade delete subgroup feature assignments", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/** Returns the primary user ID from the user's active access token. */
	public static String getUserId(User user) {
		if (user == null) return null;
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		return token != null ? token.getId() : null;
	}
}
