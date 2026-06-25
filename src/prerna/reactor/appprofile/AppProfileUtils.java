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
import java.sql.ResultSet;
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
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
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
		String sql = "SELECT COUNT(*) FROM APP_PROFILE_MANAGER WHERE APP_ID=? AND USER_ID=? AND PERMISSION='assign'";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, userId);
			rs = ps.executeQuery();
			if (rs.next()) return rs.getInt(1) > 0;
		} catch (SQLException e) {
			classLogger.error("Failed to check assign permission", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return false;
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
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", appId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Error checking app existence", e);
			return false;
		}
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
	 * APP_USER_PROFILE. Uses a correlated subquery and must remain as
	 * PreparedStatement.
	 */
	public static List<Map<String, Object>> getProfiles(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> profiles = new ArrayList<>();
		String sql = "SELECT p.PROFILE_ID, p.PROFILE_NAME, p.DESCRIPTION, p.IS_DEFAULT, p.IS_GROUP, p.CREATED_BY, p.CREATED_AT, "
				+ "(SELECT COUNT(DISTINCT up.USER_ID) FROM APP_USER_PROFILE up WHERE up.APP_ID=p.APP_ID AND up.PROFILE_ID=p.PROFILE_ID) AS USER_COUNT "
				+ "FROM APP_PROFILE p WHERE p.APP_ID=? ORDER BY p.PROFILE_NAME ASC";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> profile = new HashMap<>();
				profile.put("profileId", rs.getString("PROFILE_ID"));
				profile.put("profileName", rs.getString("PROFILE_NAME"));
				profile.put("description", rs.getString("DESCRIPTION"));
				profile.put("isDefault", rs.getBoolean("IS_DEFAULT"));
				profile.put("isGroup", rs.getBoolean("IS_GROUP"));
				profile.put("createdBy", rs.getString("CREATED_BY"));
				profile.put("createdAt", rs.getTimestamp("CREATED_AT"));
				profile.put("userCount", rs.getInt("USER_COUNT"));
				profiles.add(profile);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get app profiles", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
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
		List<Map<String, Object>> features = new ArrayList<>();

		SelectQueryStruct sqs = new SelectQueryStruct();
		sqs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_ID"));
		sqs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_KEY"));
		sqs.addSelector(new QueryColumnSelector("APP_FEATURE__DESCRIPTION"));
		sqs.addSelector(new QueryColumnSelector("APP_FEATURE__CREATED_BY"));
		sqs.addSelector(new QueryColumnSelector("APP_FEATURE__CREATED_AT"));
		sqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE__APP_ID", "==", appId));
		sqs.addOrderBy("APP_FEATURE__FEATURE_KEY", "ASC");

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, sqs)) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Map<String, Object> feature = new HashMap<>();
				feature.put("featureId", values[0]);
				feature.put("featureKey", values[1]);
				feature.put("description", values[2]);
				feature.put("createdBy", values[3]);
				feature.put("createdAt", values[4]);
				features.add(feature);
			}
		} catch (Exception e) {
			classLogger.error("Failed to get app features", e);
		}
		return features;
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
	 * profile. Uses two sequential queries joined in code; kept as PreparedStatement.
	 */
	public static List<Map<String, Object>> getProfileFeatures(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> results = new ArrayList<>();
		Map<String, Boolean> enabledMap = new HashMap<>();
		String assignSql = "SELECT FEATURE_ID, ENABLED FROM APP_PROFILE_FEATURE "
				+ "WHERE APP_ID=? AND PROFILE_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(assignSql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			rs = ps.executeQuery();
			while (rs.next()) {
				enabledMap.put(rs.getString("FEATURE_ID"), rs.getBoolean("ENABLED"));
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get profile feature assignments", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}

		String featSql = "SELECT FEATURE_ID, FEATURE_KEY, DESCRIPTION "
				+ "FROM APP_FEATURE WHERE APP_ID=? ORDER BY FEATURE_KEY ASC";
		ps = null;
		rs = null;
		try {
			ps = securityDb.getPreparedStatement(featSql);
			ps.setString(1, appId);
			rs = ps.executeQuery();
			while (rs.next()) {
				String featureId = rs.getString("FEATURE_ID");
				Map<String, Object> row = new HashMap<>();
				row.put("featureId", featureId);
				row.put("featureKey", rs.getString("FEATURE_KEY"));
				row.put("description", rs.getString("DESCRIPTION"));
				row.put("enabled", enabledMap.getOrDefault(featureId, Boolean.FALSE));
				results.add(row);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get profile features", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
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
		String checkSql = "SELECT COUNT(*) FROM APP_USER_PROFILE WHERE APP_ID=? AND USER_ID=? AND PROFILE_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(checkSql);
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.setString(3, profileId);
			rs = ps.executeQuery();
			if (rs.next() && rs.getInt(1) > 0) {
				return; // already assigned
			}
		} catch (SQLException e) {
			classLogger.error("Failed to check existing profile assignment", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}

		String actorId = getUserId(actor);
		ps = null;
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
	 * JOIN with SMSS_USER. Kept as PreparedStatement due to the LEFT JOIN.
	 */
	public static List<Map<String, Object>> getProfileUsers(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> users = new ArrayList<>();
		String sql = "SELECT up.USER_ID, u.NAME, u.EMAIL, up.ASSIGNED_BY, up.ASSIGNED_AT "
				+ "FROM APP_USER_PROFILE up "
				+ "LEFT JOIN SMSS_USER u ON up.USER_ID = u.ID "
				+ "WHERE up.APP_ID = ? AND up.PROFILE_ID = ?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("userId", rs.getString("USER_ID"));
				row.put("name", rs.getString("NAME"));
				row.put("email", rs.getString("EMAIL"));
				row.put("assignedBy", rs.getString("ASSIGNED_BY"));
				row.put("assignedAt", rs.getTimestamp("ASSIGNED_AT"));
				users.add(row);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get profile users", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return users;
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
	 * APP_USER_SUBGROUP. Uses a correlated subquery; kept as PreparedStatement.
	 */
	public static List<Map<String, Object>> getSubgroups(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> result = new ArrayList<>();
		String sql = "SELECT s.SUBGROUP_ID, s.SUBGROUP_NAME, s.DESCRIPTION, s.CREATED_BY, s.CREATED_AT, "
				+ "(SELECT COUNT(*) FROM APP_USER_SUBGROUP us WHERE us.APP_ID=s.APP_ID AND us.SUBGROUP_ID=s.SUBGROUP_ID) AS USER_COUNT "
				+ "FROM APP_PROFILE_SUBGROUP s WHERE s.APP_ID=? AND s.PROFILE_ID=? ORDER BY s.SUBGROUP_NAME ASC";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("subgroupId", rs.getString("SUBGROUP_ID"));
				row.put("subgroupName", rs.getString("SUBGROUP_NAME"));
				row.put("description", rs.getString("DESCRIPTION"));
				row.put("createdBy", rs.getString("CREATED_BY"));
				row.put("createdAt", rs.getTimestamp("CREATED_AT"));
				row.put("userCount", rs.getInt("USER_COUNT"));
				result.add(row);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get subgroups", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return result;
	}

	/**
	 * Returns all users assigned to a sub-group, including display name and email
	 * via JOIN with SMSS_USER. Kept as PreparedStatement due to the LEFT JOIN.
	 */
	public static List<Map<String, Object>> getSubgroupUsers(String appId, String subgroupId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> users = new ArrayList<>();
		String sql = "SELECT us.USER_ID, u.NAME, u.EMAIL, us.ASSIGNED_BY, us.ASSIGNED_AT "
				+ "FROM APP_USER_SUBGROUP us "
				+ "LEFT JOIN SMSS_USER u ON us.USER_ID = u.ID "
				+ "WHERE us.APP_ID=? AND us.SUBGROUP_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, subgroupId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("userId", rs.getString("USER_ID"));
				row.put("name", rs.getString("NAME"));
				row.put("email", rs.getString("EMAIL"));
				row.put("assignedBy", rs.getString("ASSIGNED_BY"));
				row.put("assignedAt", rs.getTimestamp("ASSIGNED_AT"));
				users.add(row);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get subgroup users", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return users;
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
	 * sub-group. Uses two sequential queries joined in code; kept as PreparedStatement.
	 */
	public static List<Map<String, Object>> getSubgroupFeatures(String appId, String subgroupId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> results = new ArrayList<>();
		Map<String, Boolean> enabledMap = new HashMap<>();
		String assignSql = "SELECT FEATURE_ID, ENABLED FROM APP_SUBGROUP_FEATURE WHERE APP_ID=? AND SUBGROUP_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(assignSql);
			ps.setString(1, appId);
			ps.setString(2, subgroupId);
			rs = ps.executeQuery();
			while (rs.next()) {
				enabledMap.put(rs.getString("FEATURE_ID"), rs.getBoolean("ENABLED"));
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get subgroup feature assignments", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}

		String featSql = "SELECT FEATURE_ID, FEATURE_KEY, DESCRIPTION FROM APP_FEATURE WHERE APP_ID=? ORDER BY FEATURE_KEY ASC";
		ps = null;
		rs = null;
		try {
			ps = securityDb.getPreparedStatement(featSql);
			ps.setString(1, appId);
			rs = ps.executeQuery();
			while (rs.next()) {
				String featureId = rs.getString("FEATURE_ID");
				Map<String, Object> row = new HashMap<>();
				row.put("featureId", featureId);
				row.put("featureKey", rs.getString("FEATURE_KEY"));
				row.put("description", rs.getString("DESCRIPTION"));
				row.put("enabled", enabledMap.getOrDefault(featureId, Boolean.FALSE));
				results.add(row);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get subgroup features", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return results;
	}

	// ─── User-Subgroup Assignment ────────────────────────────────────────────

	/**
	 * Assigns a user to a sub-group. If already assigned, this is a no-op.
	 */
	public static void assignUserSubgroup(String appId, String userId, String subgroupId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// check for existing assignment
		String checkSql = "SELECT COUNT(*) FROM APP_USER_SUBGROUP WHERE APP_ID=? AND USER_ID=? AND SUBGROUP_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(checkSql);
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.setString(3, subgroupId);
			rs = ps.executeQuery();
			if (rs.next() && rs.getInt(1) > 0) return;
		} catch (SQLException e) {
			classLogger.error("Failed to check existing subgroup assignment", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}

		String actorId = getUserId(actor);
		ps = null;
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
		String checkSql = "SELECT COUNT(*) FROM APP_PROFILE_MANAGER WHERE APP_ID=? AND USER_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(checkSql);
			ps.setString(1, appId);
			ps.setString(2, userId);
			rs = ps.executeQuery();
			if (rs.next() && rs.getInt(1) > 0) return;
		} catch (SQLException e) {
			classLogger.error("Failed to check existing profile manager", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}

		ps = null;
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
	 * display name and email via JOIN. Kept as PreparedStatement due to LEFT JOIN.
	 */
	public static List<Map<String, Object>> getProfileManagers(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> result = new ArrayList<>();
		String sql = "SELECT pm.USER_ID, u.NAME, u.EMAIL, pm.PERMISSION "
				+ "FROM APP_PROFILE_MANAGER pm "
				+ "LEFT JOIN SMSS_USER u ON pm.USER_ID = u.ID "
				+ "WHERE pm.APP_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("userId", rs.getString("USER_ID"));
				row.put("name", rs.getString("NAME"));
				row.put("email", rs.getString("EMAIL"));
				row.put("permission", rs.getString("PERMISSION"));
				result.add(row);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get profile managers", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return result;
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
		// Count distinct users in APP_USER_PROFILE for this profile
		String sql = "SELECT COUNT(DISTINCT USER_ID) FROM APP_USER_PROFILE WHERE APP_ID=? AND PROFILE_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			rs = ps.executeQuery();
			if (rs.next()) return rs.getInt(1);
		} catch (SQLException e) {
			classLogger.error("Failed to count profile users", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return 0;
	}


	private static Map<String, Object> getDefaultProfile(IRDBMSEngine securityDb, String appId) {
		String sql = "SELECT PROFILE_ID, PROFILE_NAME, IS_GROUP FROM APP_PROFILE WHERE APP_ID=? AND IS_DEFAULT=TRUE";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			rs = ps.executeQuery();
			if (rs.next()) {
				Map<String, Object> result = new HashMap<>();
				result.put("profileId", rs.getString("PROFILE_ID"));
				result.put("profileName", rs.getString("PROFILE_NAME"));
				result.put("isGroup", rs.getBoolean("IS_GROUP"));
				result.put("isExplicitAssignment", false);
				return result;
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get default profile", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return null;
	}

	/**
	 * Returns all explicit profile assignments for a user (not falling back to default).
	 */
	private static List<Map<String, Object>> getExplicitUserProfiles(IRDBMSEngine securityDb,
			String appId, String userId) {
		List<Map<String, Object>> profiles = new ArrayList<>();
		String sql = "SELECT p.PROFILE_ID, p.PROFILE_NAME, p.IS_GROUP "
				+ "FROM APP_USER_PROFILE up "
				+ "JOIN APP_PROFILE p ON up.PROFILE_ID = p.PROFILE_ID AND up.APP_ID = p.APP_ID "
				+ "WHERE up.APP_ID=? AND up.USER_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, userId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("profileId", rs.getString("PROFILE_ID"));
				row.put("profileName", rs.getString("PROFILE_NAME"));
				row.put("isGroup", rs.getBoolean("IS_GROUP"));
				profiles.add(row);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get explicit user profiles", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return profiles;
	}

	/**
	 * Returns all subgroup assignments for a user, including parent profile name.
	 */
	private static List<Map<String, Object>> getExplicitUserSubgroups(IRDBMSEngine securityDb,
			String appId, String userId) {
		List<Map<String, Object>> subgroups = new ArrayList<>();
		String sql = "SELECT us.SUBGROUP_ID, sg.SUBGROUP_NAME, p.PROFILE_ID, p.PROFILE_NAME "
				+ "FROM APP_USER_SUBGROUP us "
				+ "JOIN APP_PROFILE_SUBGROUP sg ON us.SUBGROUP_ID = sg.SUBGROUP_ID "
				+ "JOIN APP_PROFILE p ON sg.PROFILE_ID = p.PROFILE_ID "
				+ "WHERE us.APP_ID=? AND us.USER_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, userId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("subgroupId", rs.getString("SUBGROUP_ID"));
				row.put("subgroupName", rs.getString("SUBGROUP_NAME"));
				row.put("profileId", rs.getString("PROFILE_ID"));
				row.put("profileName", rs.getString("PROFILE_NAME"));
				subgroups.add(row);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get explicit user subgroups", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return subgroups;
	}

	private static void addProfileFeaturesToResult(IRDBMSEngine securityDb, String appId,
			String profileId, String profileName, boolean isDefaultProfile,
			Map<String, Object> result) {
		String sql = "SELECT f.FEATURE_KEY, f.FEATURE_ID "
				+ "FROM APP_PROFILE_FEATURE pf "
				+ "JOIN APP_FEATURE f ON pf.FEATURE_ID = f.FEATURE_ID AND pf.APP_ID = f.APP_ID "
				+ "WHERE pf.APP_ID=? AND pf.PROFILE_ID=? AND pf.ENABLED=TRUE";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			rs = ps.executeQuery();
			while (rs.next()) {
				String featureKey = rs.getString("FEATURE_KEY");
				if (!result.containsKey(featureKey)) {
					Map<String, Object> featureInfo = new HashMap<>();
					featureInfo.put("featureId", rs.getString("FEATURE_ID"));
					featureInfo.put("profileName", profileName);
					featureInfo.put("isDefaultProfile", isDefaultProfile);
					result.put(featureKey, featureInfo);
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to add profile features to result", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
	}

	private static void addSubgroupFeaturesToResult(IRDBMSEngine securityDb, String appId,
			String subgroupId, String subgroupName, String parentProfileName,
			Map<String, Object> result) {
		String sql = "SELECT f.FEATURE_KEY, f.FEATURE_ID "
				+ "FROM APP_SUBGROUP_FEATURE sf "
				+ "JOIN APP_FEATURE f ON sf.FEATURE_ID = f.FEATURE_ID AND sf.APP_ID = f.APP_ID "
				+ "WHERE sf.APP_ID=? AND sf.SUBGROUP_ID=? AND sf.ENABLED=TRUE";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, subgroupId);
			rs = ps.executeQuery();
			while (rs.next()) {
				String featureKey = rs.getString("FEATURE_KEY");
				if (!result.containsKey(featureKey)) {
					Map<String, Object> featureInfo = new HashMap<>();
					featureInfo.put("featureId", rs.getString("FEATURE_ID"));
					featureInfo.put("profileName", parentProfileName);
					featureInfo.put("subgroupName", subgroupName);
					featureInfo.put("isDefaultProfile", false);
					result.put(featureKey, featureInfo);
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to add subgroup features to result", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
	}

	private static String resolveFeatureId(IRDBMSEngine securityDb, String appId, String featureKey) {
		String sql = "SELECT FEATURE_ID FROM APP_FEATURE WHERE APP_ID=? AND FEATURE_KEY=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, featureKey);
			rs = ps.executeQuery();
			if (rs.next()) return rs.getString("FEATURE_ID");
		} catch (SQLException e) {
			classLogger.error("Failed to resolve feature ID", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return null;
	}

	private static boolean queryFeatureEnabled(IRDBMSEngine securityDb, String appId,
			String profileId, String featureId) {
		String sql = "SELECT ENABLED FROM APP_PROFILE_FEATURE WHERE APP_ID=? AND PROFILE_ID=? AND FEATURE_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			ps.setString(3, featureId);
			rs = ps.executeQuery();
			if (rs.next()) return rs.getBoolean("ENABLED");
		} catch (SQLException e) {
			classLogger.error("Failed to check feature enabled", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return false;
	}

	private static boolean querySubgroupFeatureEnabled(IRDBMSEngine securityDb, String appId,
			String subgroupId, String featureId) {
		String sql = "SELECT ENABLED FROM APP_SUBGROUP_FEATURE WHERE APP_ID=? AND SUBGROUP_ID=? AND FEATURE_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, subgroupId);
			ps.setString(3, featureId);
			rs = ps.executeQuery();
			if (rs.next()) return rs.getBoolean("ENABLED");
		} catch (SQLException e) {
			classLogger.error("Failed to check subgroup feature enabled", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return false;
	}

	private static boolean isGroupProfile(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String sql = "SELECT IS_GROUP FROM APP_PROFILE WHERE APP_ID=? AND PROFILE_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			rs = ps.executeQuery();
			if (rs.next()) return rs.getBoolean("IS_GROUP");
		} catch (SQLException e) {
			classLogger.error("Failed to check isGroup on profile", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return false;
	}

	private static List<String> getSubgroupIdsForProfile(IRDBMSEngine securityDb, String appId, String profileId) {
		List<String> ids = new ArrayList<>();
		String sql = "SELECT SUBGROUP_ID FROM APP_PROFILE_SUBGROUP WHERE APP_ID=? AND PROFILE_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			rs = ps.executeQuery();
			while (rs.next()) ids.add(rs.getString("SUBGROUP_ID"));
		} catch (SQLException e) {
			classLogger.error("Failed to get subgroup IDs for profile", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
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
