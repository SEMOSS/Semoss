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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
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

	public static boolean canManageProfiles(User user, String appId) {
		if (!appExists(appId)) {
			throw new IllegalArgumentException("App not found: " + appId);
		}
		if (SecurityAdminUtils.userIsAdmin(user)) return true;
		if (SecurityProjectUtils.userIsOwner(user, appId)) return true;
		if (SecurityProjectUtils.userCanEditProject(user, appId)) return true;
		return false;
	}

	public static boolean canEvaluateFeatures(User user, String appId) {
		return SecurityProjectUtils.userCanViewProject(user, appId);
	}

	private static boolean appExists(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", appId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Error checking app existence", e);
			return false;
		}
	}

	// ─── Profile CRUD ───────────────────────────────────────────────────────

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

		if (isDefault) {
			clearDefaultProfile(securityDb, appId);
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO APP_PROFILE (PROFILE_ID, APP_ID, PROFILE_NAME, DESCRIPTION, IS_DEFAULT, CREATED_BY, CREATED_AT) VALUES (?,?,?,?,?,?,?)");
			int i = 1;
			ps.setString(i++, profileId);
			ps.setString(i++, appId);
			ps.setString(i++, profileName);
			ps.setString(i++, description);
			ps.setBoolean(i++, isDefault);
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
			classLogger.error("Failed to delete app profile", e);
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
			classLogger.error("Failed to cascade delete app profile features", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static List<Map<String, Object>> getProfiles(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> profiles = new ArrayList<>();
		String sql = "SELECT PROFILE_ID, PROFILE_NAME, DESCRIPTION, IS_DEFAULT, CREATED_BY, CREATED_AT "
				+ "FROM APP_PROFILE WHERE APP_ID=? ORDER BY PROFILE_NAME ASC";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			rs = ps.executeQuery();
			while (rs.next()) {
				String profileId = rs.getString("PROFILE_ID");
				Map<String, Object> profile = new HashMap<>();
				profile.put("profileId", profileId);
				profile.put("profileName", rs.getString("PROFILE_NAME"));
				profile.put("description", rs.getString("DESCRIPTION"));
				profile.put("isDefault", rs.getBoolean("IS_DEFAULT"));
				profile.put("createdBy", rs.getString("CREATED_BY"));
				profile.put("createdAt", rs.getTimestamp("CREATED_AT"));
				profile.put("userCount", getAssignedUserCount(securityDb, appId, profileId));
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

	public static List<Map<String, Object>> getFeatures(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> features = new ArrayList<>();
		String sql = "SELECT FEATURE_ID, FEATURE_KEY, DESCRIPTION, CREATED_BY, CREATED_AT "
				+ "FROM APP_FEATURE WHERE APP_ID=? ORDER BY FEATURE_KEY ASC";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> feature = new HashMap<>();
				feature.put("featureId", rs.getString("FEATURE_ID"));
				feature.put("featureKey", rs.getString("FEATURE_KEY"));
				feature.put("description", rs.getString("DESCRIPTION"));
				feature.put("createdBy", rs.getString("CREATED_BY"));
				feature.put("createdAt", rs.getTimestamp("CREATED_AT"));
				features.add(feature);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get app features", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return features;
	}

	// ─── Profile-Feature Assignment ─────────────────────────────────────────

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

	public static List<Map<String, Object>> getProfileFeatures(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> results = new ArrayList<>();
		String sql = "SELECT f.FEATURE_ID, f.FEATURE_KEY, f.DESCRIPTION, pf.ENABLED "
				+ "FROM APP_FEATURE f "
				+ "LEFT JOIN APP_PROFILE_FEATURE pf ON f.FEATURE_ID = pf.FEATURE_ID "
				+ "AND f.APP_ID = pf.APP_ID AND pf.PROFILE_ID = ? "
				+ "WHERE f.APP_ID = ? ORDER BY f.FEATURE_KEY";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, profileId);
			ps.setString(2, appId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("featureId", rs.getString("FEATURE_ID"));
				row.put("featureKey", rs.getString("FEATURE_KEY"));
				row.put("description", rs.getString("DESCRIPTION"));
				Object enabledObj = rs.getObject("ENABLED");
				row.put("enabled", enabledObj != null && rs.getBoolean("ENABLED"));
				results.add(row);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get profile features", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return results;
	}

	// ─── User-Profile Assignment ────────────────────────────────────────────

	public static void assignUserProfile(String appId, String userId, String profileId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String actorId = getUserId(actor);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_USER_PROFILE WHERE APP_ID=? AND USER_ID=?");
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to remove existing user profile assignment", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
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
			classLogger.error("Failed to remove user profile assignment", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static Map<String, Object> getUserProfile(String appId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String sql = "SELECT p.PROFILE_ID, p.PROFILE_NAME FROM APP_USER_PROFILE up "
				+ "JOIN APP_PROFILE p ON up.PROFILE_ID = p.PROFILE_ID "
				+ "WHERE up.APP_ID = ? AND up.USER_ID = ?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, userId);
			rs = ps.executeQuery();
			if (rs.next()) {
				Map<String, Object> result = new HashMap<>();
				result.put("profileId", rs.getString("PROFILE_ID"));
				result.put("profileName", rs.getString("PROFILE_NAME"));
				result.put("isExplicitAssignment", true);
				return result;
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get user profile", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return getDefaultProfile(securityDb, appId);
	}

	public static List<Map<String, Object>> getProfileUsers(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> users = new ArrayList<>();
		String sql = "SELECT up.USER_ID, up.ASSIGNED_BY, up.ASSIGNED_AT "
				+ "FROM APP_USER_PROFILE up "
				+ "INNER JOIN PROJECTPERMISSION pp ON up.USER_ID = pp.USERID AND up.APP_ID = pp.PROJECTID "
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

	// ─── Feature evaluation ──────────────────────────────────────────────────

	public static boolean checkFeature(String appId, String featureKey, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String featureId = resolveFeatureId(securityDb, appId, featureKey);
		if (featureId == null) return false;
		String userId = getUserId(user);
		Map<String, Object> profile = getUserProfile(appId, userId);
		if (profile == null) return false;
		String profileId = (String) profile.get("profileId");
		return queryFeatureEnabled(securityDb, appId, profileId, featureId);
	}

	public static Map<String, Object> getUserFeatures(String appId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = getUserId(user);
		Map<String, Object> profile = getUserProfile(appId, userId);
		if (profile == null) return new HashMap<>();
		String profileId = (String) profile.get("profileId");
		String profileName = (String) profile.get("profileName");
		boolean isDefaultProfile = !(Boolean) profile.getOrDefault("isExplicitAssignment", Boolean.TRUE);

		Map<String, Object> result = new HashMap<>();
		// Return only features with ENABLED=true — callers cannot infer what features exist but are hidden
		String sql = "SELECT f.FEATURE_KEY, f.FEATURE_ID "
				+ "FROM APP_PROFILE_FEATURE pf "
				+ "JOIN APP_FEATURE f ON pf.FEATURE_ID = f.FEATURE_ID AND pf.APP_ID = f.APP_ID "
				+ "WHERE pf.APP_ID = ? AND pf.PROFILE_ID = ? AND pf.ENABLED = true";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> featureInfo = new HashMap<>();
				featureInfo.put("featureId", rs.getString("FEATURE_ID"));
				featureInfo.put("profileName", profileName);
				featureInfo.put("isDefaultProfile", isDefaultProfile);
				result.put(rs.getString("FEATURE_KEY"), featureInfo);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get user features", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
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
			ps = securityDb.getPreparedStatement("UPDATE APP_PROFILE SET IS_DEFAULT=false WHERE APP_ID=?");
			ps.setString(1, appId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to clear default profile", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	private static int getAssignedUserCount(IRDBMSEngine securityDb, String appId, String profileId) {
		String sql = "SELECT COUNT(*) FROM APP_USER_PROFILE WHERE APP_ID=? AND PROFILE_ID=?";
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
		String sql = "SELECT PROFILE_ID, PROFILE_NAME FROM APP_PROFILE WHERE APP_ID=? AND IS_DEFAULT=true";
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

	private static boolean queryFeatureEnabled(IRDBMSEngine securityDb, String appId, String profileId, String featureId) {
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

	static String getUserId(User user) {
		if (user == null) return null;
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		return token != null ? token.getId() : null;
	}
}
