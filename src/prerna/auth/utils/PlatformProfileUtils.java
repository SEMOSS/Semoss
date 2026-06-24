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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class PlatformProfileUtils {

	private static final Logger classLogger = LogManager.getLogger(PlatformProfileUtils.class);

	public static final Set<String> PREDEFINED_FEATURE_KEYS = Collections.unmodifiableSet(
			new HashSet<>(Arrays.asList(
					"nav.app-catalog",
					"nav.build",
					"nav.skills",
					"nav.settings",
					"nav.engine")));

	private PlatformProfileUtils() {
	}

	// ─── Permission check ────────────────────────────────────────────────────

	public static boolean canManage(User user) {
		return SecurityAdminUtils.userIsAdmin(user);
	}

	// ─── Profile CRUD ────────────────────────────────────────────────────────

	public static Map<String, Object> createProfile(String name, String description, User user) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Profile name cannot be blank.");
		}
		String profileId = UUID.randomUUID().toString();
		String actorId = AppProfileUtils.getUserId(user);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO PLATFORM_PROFILE (PROFILE_ID, PROFILE_NAME, DESCRIPTION, CREATED_BY, CREATED_AT) VALUES (?,?,?,?,?)");
			int i = 1;
			ps.setString(i++, profileId);
			ps.setString(i++, name.trim());
			ps.setString(i++, description);
			ps.setString(i++, actorId);
			ps.setTimestamp(i++, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to create platform profile", e);
			throw new IllegalArgumentException("An error occurred creating the platform profile.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		Map<String, Object> result = new HashMap<>();
		result.put("profileId", profileId);
		result.put("profileName", name.trim());
		result.put("description", description);
		return result;
	}

	public static void updateProfile(String profileId, String name, String description, User user) {
		if (name != null && name.trim().isEmpty()) {
			throw new IllegalArgumentException("Profile name cannot be blank.");
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		StringBuilder sb = new StringBuilder("UPDATE PLATFORM_PROFILE SET");
		List<String> params = new ArrayList<>();
		if (name != null) { sb.append(" PROFILE_NAME=?,"); params.add(name.trim()); }
		if (description != null) { sb.append(" DESCRIPTION=?,"); params.add(description); }
		if (params.isEmpty()) return;
		sb.setLength(sb.length() - 1);
		sb.append(" WHERE PROFILE_ID=?");
		params.add(profileId);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(sb.toString());
			for (int i = 0; i < params.size(); i++) {
				ps.setString(i + 1, params.get(i));
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to update platform profile", e);
			throw new IllegalArgumentException("An error occurred updating the platform profile.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void deleteProfile(String profileId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int count = getAssignedUserCount(securityDb, profileId);
		if (count > 0) {
			throw new IllegalArgumentException(
					"Cannot delete: " + count + " user(s) are assigned to this profile. Reassign them first.");
		}
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM PLATFORM_PROFILE WHERE PROFILE_ID=?");
			ps.setString(1, profileId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to delete platform profile", e);
			throw new IllegalArgumentException("An error occurred deleting the platform profile.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM PLATFORM_PROFILE_FEATURE WHERE PROFILE_ID=?");
			ps.setString(1, profileId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to cascade delete platform profile features", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static List<Map<String, Object>> getProfiles(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> profiles = new ArrayList<>();
		String sql = "SELECT PROFILE_ID, PROFILE_NAME, DESCRIPTION, CREATED_BY, CREATED_AT "
				+ "FROM PLATFORM_PROFILE ORDER BY PROFILE_NAME";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> profile = new HashMap<>();
				String pid = rs.getString("PROFILE_ID");
				profile.put("profileId", pid);
				profile.put("profileName", rs.getString("PROFILE_NAME"));
				profile.put("description", rs.getString("DESCRIPTION"));
				profile.put("createdBy", rs.getString("CREATED_BY"));
				profile.put("createdAt", rs.getTimestamp("CREATED_AT"));
				profile.put("userCount", getAssignedUserCount(securityDb, pid));
				profiles.add(profile);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get platform profiles", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return profiles;
	}

	// ─── Profile-Feature Assignment ──────────────────────────────────────────

	public static void setProfileFeature(String profileId, String featureKey, boolean enabled, User user) {
		if (!PREDEFINED_FEATURE_KEYS.contains(featureKey)) {
			throw new IllegalArgumentException(
					"Unknown platform feature key: " + featureKey
							+ ". Valid keys: " + PREDEFINED_FEATURE_KEYS);
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"DELETE FROM PLATFORM_PROFILE_FEATURE WHERE PROFILE_ID=? AND FEATURE_KEY=?");
			ps.setString(1, profileId);
			ps.setString(2, featureKey);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to delete existing platform feature row", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO PLATFORM_PROFILE_FEATURE (PROFILE_ID, FEATURE_KEY, ENABLED) VALUES (?,?,?)");
			ps.setString(1, profileId);
			ps.setString(2, featureKey);
			ps.setBoolean(3, enabled);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to insert platform feature", e);
			throw new IllegalArgumentException("An error occurred setting the platform feature.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static Map<String, Boolean> getProfileFeatures(String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// Start with all keys disabled
		Map<String, Boolean> result = new LinkedHashMap<>();
		for (String key : PREDEFINED_FEATURE_KEYS) {
			result.put(key, Boolean.FALSE);
		}
		String sql = "SELECT FEATURE_KEY, ENABLED FROM PLATFORM_PROFILE_FEATURE WHERE PROFILE_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, profileId);
			rs = ps.executeQuery();
			while (rs.next()) {
				String key = rs.getString("FEATURE_KEY");
				if (PREDEFINED_FEATURE_KEYS.contains(key)) {
					result.put(key, rs.getBoolean("ENABLED"));
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get platform profile features", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return result;
	}

	// ─── User-Profile Assignment ─────────────────────────────────────────────

	public static void assignUserProfile(String userId, String profileId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String actorId = AppProfileUtils.getUserId(actor);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM PLATFORM_USER_PROFILE WHERE USER_ID=?");
			ps.setString(1, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to remove existing platform user profile", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO PLATFORM_USER_PROFILE (USER_ID, PROFILE_ID, ASSIGNED_BY, ASSIGNED_AT) VALUES (?,?,?,?)");
			ps.setString(1, userId);
			ps.setString(2, profileId);
			ps.setString(3, actorId);
			ps.setTimestamp(4, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to assign platform user profile", e);
			throw new IllegalArgumentException("An error occurred assigning the platform profile.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void removeUserProfile(String userId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM PLATFORM_USER_PROFILE WHERE USER_ID=?");
			ps.setString(1, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error("Failed to remove platform user profile", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	// ─── Feature evaluation ──────────────────────────────────────────────────

	/**
	 * Returns all predefined platform feature keys with their enabled status for the
	 * calling user. If the user has no platform profile assigned, all keys are true
	 * (fail-open — user is already authenticated and admin-provisioned).
	 */
	public static Map<String, Boolean> getUserFeatures(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = AppProfileUtils.getUserId(user);
		String profileId = getAssignedProfileId(securityDb, userId);
		if (profileId == null) {
			// No profile assigned → all nav visible
			Map<String, Boolean> all = new LinkedHashMap<>();
			for (String key : PREDEFINED_FEATURE_KEYS) {
				all.put(key, Boolean.TRUE);
			}
			return all;
		}
		return getProfileFeatures(profileId);
	}

	// ─── Private helpers ─────────────────────────────────────────────────────

	private static int getAssignedUserCount(IRDBMSEngine securityDb, String profileId) {
		String sql = "SELECT COUNT(*) FROM PLATFORM_USER_PROFILE WHERE PROFILE_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, profileId);
			rs = ps.executeQuery();
			if (rs.next()) return rs.getInt(1);
		} catch (SQLException e) {
			classLogger.error("Failed to count platform profile users", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return 0;
	}

	private static String getAssignedProfileId(IRDBMSEngine securityDb, String userId) {
		String sql = "SELECT PROFILE_ID FROM PLATFORM_USER_PROFILE WHERE USER_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, userId);
			rs = ps.executeQuery();
			if (rs.next()) return rs.getString("PROFILE_ID");
		} catch (SQLException e) {
			classLogger.error("Failed to get platform user profile", e);
		} finally {
			ConnectionUtils.closeAllConnections(ps, rs);
		}
		return null;
	}
}
