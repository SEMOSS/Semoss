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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
			boolean isDefault, User user) {
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

		SelectQueryStruct dupQs = new SelectQueryStruct();
		dupQs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_ID", "profileId"));
		dupQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE__APP_ID", "==", appId));
		dupQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE__PROFILE_NAME", "==", profileName));
		if (QueryExecutionUtility.flushToString(securityDb, dupQs) != null) {
			throw new IllegalArgumentException("A profile named '" + profileName + "' already exists for this app.");
		}

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
			ps.setBoolean(i++, false);
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
		return result;
	}

	/**
	 * Updates mutable fields on an existing app profile (name, description,
	 * isDefault). Null parameters are ignored.
	 */
	public static void updateProfile(String appId, String profileId, String name, String description,
			Boolean isDefault, User user) {
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
	 * Deletes a profile and cascades to its feature mappings.
	 * Throws if any users are still assigned.
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
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__CREATED_BY", "createdBy"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__CREATED_AT", "createdAt"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, "APP_USER_PROFILE__USER_ID", "userCount"));
		qs.addRelation("APP_PROFILE__PROFILE_ID", "APP_USER_PROFILE__PROFILE_ID", "left.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE__APP_ID", "==", appId));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__PROFILE_ID"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__PROFILE_NAME"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__DESCRIPTION"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__IS_DEFAULT"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__CREATED_BY"));
		qs.addGroupBy(new QueryColumnSelector("APP_PROFILE__CREATED_AT"));
		qs.addOrderBy(new QueryColumnOrderBySelector("APP_PROFILE__PROFILE_NAME"));
		List<Map<String, Object>> profiles = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		profiles.forEach(p -> p.put("isDefault", Boolean.TRUE.equals(p.get("isDefault"))));
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
	 * Deletes an app feature and cascades removal from the profile feature mapping table.
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

	// ─── User-Profile Assignment ────────────────────────────────────────────

	/**
	 * Returns {@code true} if the given userId exists as a row in {@code SMSS_USER}.
	 */
	private static boolean userExists(IRDBMSEngine securityDb, String userId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector countFn = new QueryFunctionSelector();
		countFn.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
		countFn.setFunction(QueryFunctionHelper.COUNT);
		qs.addSelector(countFn);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		Integer count = QueryExecutionUtility.flushToInteger(securityDb, qs);
		return count != null && count > 0;
	}

	/**
	 * Assigns a user to a profile. If already assigned to this specific profile, this is a no-op.
	 */
	public static void assignUserProfile(String appId, String userId, String profileId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
	 * Bulk-assigns a list of users to a profile for an app. Each user is processed independently:
	 * users already in the profile are silently skipped, users not found in {@code SMSS_USER} are
	 * recorded in the errors bucket.
	 *
	 * @return a map with keys: {@code assigned}, {@code skipped}, {@code errors}
	 */
	public static Map<String, Object> assignUsersToProfile(String appId, List<String> userIds, String profileId, User actor) {
		List<String> assigned = new ArrayList<>();
		List<String> skipped = new ArrayList<>();
		Map<String, String> errors = new LinkedHashMap<>();
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String actorId = getUserId(actor);

		for (String userId : userIds) {
			if (!userExists(securityDb, userId)) {
				classLogger.warn("assignUsersToProfile: user '{}' not found in SMSS_USER — adding to errors", userId);
				errors.put(userId, "User not found.");
				continue;
			}

			SelectQueryStruct checkQs = new SelectQueryStruct();
			QueryFunctionSelector countFn = new QueryFunctionSelector();
			countFn.addInnerSelector(new QueryColumnSelector("APP_USER_PROFILE__USER_ID"));
			countFn.setFunction(QueryFunctionHelper.COUNT);
			checkQs.addSelector(countFn);
			checkQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__APP_ID", "==", appId));
			checkQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__USER_ID", "==", userId));
			checkQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__PROFILE_ID", "==", profileId));
			Integer existingCount = QueryExecutionUtility.flushToInteger(securityDb, checkQs);
			if (existingCount != null && existingCount > 0) {
				classLogger.debug("assignUsersToProfile: user '{}' already in profile '{}' for app '{}' — skipping", userId, profileId, appId);
				skipped.add(userId);
				continue;
			}

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
				assigned.add(userId);
			} catch (SQLException e) {
				classLogger.error("assignUsersToProfile: DB error assigning user '{}'", userId, e);
				errors.put(userId, "Database error during assignment.");
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("assigned", assigned);
		result.put("skipped", skipped);
		result.put("errors", errors);
		return result;
	}

	/**
	 * Removes ALL profile assignments for a user from an app.
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
	 */
	public static List<Map<String, Object>> getUserProfiles(String appId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		return getExplicitUserProfiles(securityDb, appId, userId);
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

	// ─── Profile Manager (delegated BU admin) ────────────────────────────────

	/**
	 * Grants a user delegated 'assign' permission to manage profile assignments for
	 * an app. If already a manager, this is a no-op.
	 */
	public static void addProfileManager(String appId, String userId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
	 * Returns all users with delegated profile manager permission for an app.
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
	 * app, evaluated across all assigned profiles with default profile fallback.
	 */
	public static boolean checkFeature(String appId, String featureKey, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String featureId = resolveFeatureId(securityDb, appId, featureKey);
		if (featureId == null) return false;
		String userId = getUserId(user);

		List<Map<String, Object>> profiles = getExplicitUserProfiles(securityDb, appId, userId);
		if (profiles.isEmpty()) {
			Map<String, Object> defaultProfile = getDefaultProfile(securityDb, appId);
			if (defaultProfile != null) {
				return queryFeatureEnabled(securityDb, appId, (String) defaultProfile.get("profileId"), featureId);
			}
			return false;
		}
		for (Map<String, Object> p : profiles) {
			if (queryFeatureEnabled(securityDb, appId, (String) p.get("profileId"), featureId)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns a boolean feature map for the calling user — all app features keyed
	 * by featureKey, true if enabled for this user across any assigned profile.
	 * Falls back to the default profile if the user has no explicit assignment.
	 */
	public static Map<String, Boolean> getUserFeatures(String appId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = getUserId(user);
		Map<String, String> catalog = getFeatureCatalog(securityDb, appId);
		Set<String> enabledIds = new HashSet<>();

		List<Map<String, Object>> explicitProfiles = getExplicitUserProfiles(securityDb, appId, userId);
		if (explicitProfiles.isEmpty()) {
			Map<String, Object> defaultProfile = getDefaultProfile(securityDb, appId);
			if (defaultProfile != null) {
				enabledIds.addAll(getEnabledProfileFeatureIds(securityDb, appId, (String) defaultProfile.get("profileId")));
			}
		} else {
			explicitProfiles.forEach(p -> enabledIds.addAll(getEnabledProfileFeatureIds(securityDb, appId, (String) p.get("profileId"))));
		}

		return buildFeatureMap(catalog, enabledIds);
	}

	/**
	 * Returns the calling user's profile(s) for this app, each with a full feature map.
	 * Falls back to the default profile if the user has no explicit assignment.
	 */
	public static List<Map<String, Object>> getUserAppProfiles(String appId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = getUserId(user);
		Map<String, String> catalog = getFeatureCatalog(securityDb, appId);
		List<Map<String, Object>> result = new ArrayList<>();

		List<Map<String, Object>> explicitProfiles = getExplicitUserProfiles(securityDb, appId, userId);
		if (!explicitProfiles.isEmpty()) {
			for (Map<String, Object> p : explicitProfiles) {
				Map<String, Object> entry = new LinkedHashMap<>();
				entry.put("profileId", p.get("profileId"));
				entry.put("profileName", p.get("profileName"));
				entry.put("isDefault", false);
				entry.put("features", buildFeatureMap(catalog, getEnabledProfileFeatureIds(securityDb, appId, (String) p.get("profileId"))));
				result.add(entry);
			}
		} else {
			Map<String, Object> defaultProfile = getDefaultProfile(securityDb, appId);
			if (defaultProfile != null) {
				Map<String, Object> entry = new LinkedHashMap<>();
				entry.put("profileId", defaultProfile.get("profileId"));
				entry.put("profileName", defaultProfile.get("profileName"));
				entry.put("isDefault", true);
				entry.put("features", buildFeatureMap(catalog, getEnabledProfileFeatureIds(securityDb, appId, (String) defaultProfile.get("profileId"))));
				result.add(entry);
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
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE__IS_DEFAULT", "==", Boolean.TRUE));
		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		return rows.isEmpty() ? null : rows.get(0);
	}

	/**
	 * Returns all explicit profile assignments for a user.
	 * Two single-table queries + in-memory join to avoid unreliable OWL join traversal.
	 */
	private static List<Map<String, Object>> getExplicitUserProfiles(IRDBMSEngine securityDb,
			String appId, String userId) {
		SelectQueryStruct qs1 = new SelectQueryStruct();
		qs1.addSelector(new QueryColumnSelector("APP_USER_PROFILE__PROFILE_ID", "profileId"));
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__APP_ID", "==", appId));
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_PROFILE__USER_ID", "==", userId));
		Set<String> assignedIds = QueryExecutionUtility.flushRsToMap(securityDb, qs1).stream()
				.map(r -> (String) r.get("profileId")).collect(Collectors.toSet());
		if (assignedIds.isEmpty()) return new ArrayList<>();

		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs2.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_ID", "profileId"));
		qs2.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_NAME", "profileName"));
		qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE__APP_ID", "==", appId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs2).stream()
				.filter(p -> assignedIds.contains((String) p.get("profileId")))
				.collect(Collectors.toList());
	}

	/** Returns featureId → featureKey map for all features in this app. */
	private static Map<String, String> getFeatureCatalog(IRDBMSEngine securityDb, String appId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_ID", "featureId"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_KEY", "featureKey"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE__APP_ID", "==", appId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs).stream()
				.collect(Collectors.toMap(r -> (String) r.get("featureId"), r -> (String) r.get("featureKey")));
	}

	/** Returns the set of enabled featureIds for a profile. */
	private static Set<String> getEnabledProfileFeatureIds(IRDBMSEngine securityDb, String appId, String profileId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_FEATURE__FEATURE_ID", "featureId"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_FEATURE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_FEATURE__PROFILE_ID", "==", profileId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_PROFILE_FEATURE__ENABLED", "==", Boolean.TRUE));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs).stream()
				.map(r -> (String) r.get("featureId")).collect(Collectors.toSet());
	}

	/** Builds a {featureKey: true/false} map for all features, true if featureId is in enabledIds. */
	private static Map<String, Boolean> buildFeatureMap(Map<String, String> catalog, Set<String> enabledIds) {
		Map<String, Boolean> map = new LinkedHashMap<>();
		catalog.forEach((featureId, featureKey) -> map.put(featureKey, enabledIds.contains(featureId)));
		return map;
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

	/** Returns the primary user ID from the user's active access token. */
	public static String getUserId(User user) {
		if (user == null) return null;
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		return token != null ? token.getId() : null;
	}
}
