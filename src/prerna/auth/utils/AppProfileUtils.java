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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;

public class AppProfileUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(AppProfileUtils.class);

	private static final String FEATURE_KEY_PATTERN = "^[a-zA-Z0-9\\-]+$";

	private AppProfileUtils() {
	}

	// -----------------------------------------------------------------------
	// Permission checks
	// -----------------------------------------------------------------------

	public static boolean canManageProfiles(User user, String appId) {
		if (SecurityAdminUtils.userIsAdmin(user)) {
			return true;
		}
		verifyAppExists(appId);
		return SecurityProjectUtils.userIsOwner(user, appId)
				|| SecurityProjectUtils.userCanEditProject(user, appId);
	}

	public static boolean canEvaluateFeatures(User user, String appId) {
		return SecurityProjectUtils.userCanViewProject(user, appId);
	}

	private static void verifyAppExists(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"PROJECTPERMISSION__PROJECTID", "==", appId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (!wrapper.hasNext()) {
				throw new IllegalArgumentException("App not found: " + appId);
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error verifying app existence: " + appId);
		}
	}

	// -----------------------------------------------------------------------
	// Profile CRUD
	// -----------------------------------------------------------------------

	public static Map<String, Object> createProfile(String appId, String name,
			String description, boolean isDefault, User user) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Profile name is required.");
		}
		if (name.trim().length() > 100) {
			throw new IllegalArgumentException("Profile name must be 100 characters or fewer.");
		}
		name = name.trim();
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		if (isDefault) {
			clearDefaultProfile(securityDb, appId);
		}

		String profileId = UUID.randomUUID().toString();
		String actorId = getUserId(user);
		Timestamp now = nowUtc();

		String query = "INSERT INTO APP_PROFILE (PROFILE_ID, APP_ID, PROFILE_NAME, DESCRIPTION, IS_DEFAULT, CREATED_BY, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int i = 1;
			ps.setString(i++, profileId);
			ps.setString(i++, appId);
			ps.setString(i++, name);
			ps.setString(i++, description != null ? description : "");
			ps.setBoolean(i++, isDefault);
			ps.setString(i++, actorId);
			ps.setTimestamp(i++, now);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to create profile: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		Map<String, Object> result = new HashMap<>();
		result.put("profileId", profileId);
		result.put("profileName", name);
		result.put("description", description != null ? description : "");
		result.put("isDefault", isDefault);
		return result;
	}

	public static void updateProfile(String appId, String profileId, String name,
			String description, Boolean isDefault, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		if (isDefault != null && isDefault) {
			clearDefaultProfile(securityDb, appId);
		}

		StringBuilder sb = new StringBuilder("UPDATE APP_PROFILE SET");
		List<Object> params = new ArrayList<>();

		if (name != null && !name.trim().isEmpty()) {
			sb.append(" PROFILE_NAME=?,");
			params.add(name.trim());
		}
		if (description != null) {
			sb.append(" DESCRIPTION=?,");
			params.add(description);
		}
		if (isDefault != null) {
			sb.append(" IS_DEFAULT=?,");
			params.add(isDefault);
		}

		if (params.isEmpty()) {
			return;
		}

		String sql = sb.substring(0, sb.length() - 1)
				+ " WHERE PROFILE_ID=? AND APP_ID=?";
		params.add(profileId);
		params.add(appId);

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			for (int i = 0; i < params.size(); i++) {
				Object val = params.get(i);
				if (val instanceof String) {
					ps.setString(i + 1, (String) val);
				} else if (val instanceof Boolean) {
					ps.setBoolean(i + 1, (Boolean) val);
				}
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to update profile: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void deleteProfile(String appId, String profileId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		// Count assigned users
		SelectQueryStruct countQs = new SelectQueryStruct();
		QueryFunctionSelector countSel = new QueryFunctionSelector();
		countSel.setFunction(QueryFunctionHelper.COUNT);
		countSel.addInnerSelector(new QueryColumnSelector("APP_USER_PROFILE__USER_ID"));
		countSel.setAlias("user_count");
		countQs.addSelector(countSel);
		countQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_USER_PROFILE__APP_ID", "==", appId));
		countQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_USER_PROFILE__PROFILE_ID", "==", profileId));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, countQs)) {
			if (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				long count = row[0] != null ? ((Number) row[0]).longValue() : 0;
				if (count > 0) {
					throw new IllegalArgumentException(
							"Cannot delete: " + count + " user(s) are assigned to this profile. Reassign them first.");
				}
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error checking profile assignment count.");
		}

		String[] deletes = {
			"DELETE FROM APP_PROFILE WHERE PROFILE_ID=? AND APP_ID=?",
			"DELETE FROM APP_PROFILE_FEATURE WHERE PROFILE_ID=? AND APP_ID=?"
		};
		for (String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(deleteQuery);
				ps.setString(1, profileId);
				ps.setString(2, appId);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Failed to delete profile: " + e.getMessage());
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}

	public static List<Map<String, Object>> getProfiles(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> profiles = new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_ID"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_NAME"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__DESCRIPTION"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__IS_DEFAULT"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__CREATED_BY"));
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__CREATED_AT"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_PROFILE__APP_ID", "==", appId));
		qs.addOrderBy("APP_PROFILE__PROFILE_NAME", "ASC");

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String profileId = (String) row[0];
				Map<String, Object> profile = new HashMap<>();
				profile.put("profileId", profileId);
				profile.put("profileName", row[1]);
				profile.put("description", row[2]);
				profile.put("isDefault", row[3]);
				profile.put("createdBy", row[4]);
				profile.put("createdAt", row[5] != null ? row[5].toString() : null);
				profile.put("userCount", getProfileUserCount(securityDb, appId, profileId));
				profiles.add(profile);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve profiles: " + e.getMessage());
		}
		return profiles;
	}

	// -----------------------------------------------------------------------
	// Feature CRUD
	// -----------------------------------------------------------------------

	public static Map<String, Object> createFeature(String appId, String featureKey,
			String description, User user) {
		validateFeatureKey(featureKey);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String featureId = UUID.randomUUID().toString();
		String actorId = getUserId(user);
		Timestamp now = nowUtc();

		String query = "INSERT INTO APP_FEATURE (FEATURE_ID, APP_ID, FEATURE_KEY, DESCRIPTION, CREATED_BY, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int i = 1;
			ps.setString(i++, featureId);
			ps.setString(i++, appId);
			ps.setString(i++, featureKey);
			ps.setString(i++, description != null ? description : "");
			ps.setString(i++, actorId);
			ps.setTimestamp(i++, now);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to create feature: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		Map<String, Object> result = new HashMap<>();
		result.put("featureId", featureId);
		result.put("featureKey", featureKey);
		result.put("description", description != null ? description : "");
		return result;
	}

	public static void updateFeature(String appId, String featureId, String featureKey,
			String description, User user) {
		if (featureKey != null) {
			validateFeatureKey(featureKey);
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		StringBuilder sb = new StringBuilder("UPDATE APP_FEATURE SET");
		List<Object> params = new ArrayList<>();

		if (featureKey != null) {
			sb.append(" FEATURE_KEY=?,");
			params.add(featureKey);
		}
		if (description != null) {
			sb.append(" DESCRIPTION=?,");
			params.add(description);
		}
		if (params.isEmpty()) {
			return;
		}
		String sql = sb.substring(0, sb.length() - 1)
				+ " WHERE FEATURE_ID=? AND APP_ID=?";
		params.add(featureId);
		params.add(appId);

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			for (int i = 0; i < params.size(); i++) {
				ps.setString(i + 1, (String) params.get(i));
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to update feature: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void deleteFeature(String appId, String featureId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String[] deletes = {
			"DELETE FROM APP_FEATURE WHERE FEATURE_ID=? AND APP_ID=?",
			"DELETE FROM APP_PROFILE_FEATURE WHERE FEATURE_ID=? AND APP_ID=?"
		};
		for (String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(deleteQuery);
				ps.setString(1, featureId);
				ps.setString(2, appId);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Failed to delete feature: " + e.getMessage());
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}

	public static List<Map<String, Object>> getFeatures(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> features = new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_ID"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_KEY"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__DESCRIPTION"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__CREATED_BY"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__CREATED_AT"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_FEATURE__APP_ID", "==", appId));
		qs.addOrderBy("APP_FEATURE__FEATURE_KEY", "ASC");

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				Map<String, Object> feature = new HashMap<>();
				feature.put("featureId", row[0]);
				feature.put("featureKey", row[1]);
				feature.put("description", row[2]);
				feature.put("createdBy", row[3]);
				feature.put("createdAt", row[4] != null ? row[4].toString() : null);
				features.add(feature);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve features: " + e.getMessage());
		}
		return features;
	}

	// -----------------------------------------------------------------------
	// Profile-Feature Assignment
	// -----------------------------------------------------------------------

	public static void setProfileFeature(String appId, String profileId,
			String featureId, boolean enabled, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		PreparedStatement del = null;
		try {
			del = securityDb.getPreparedStatement(
					"DELETE FROM APP_PROFILE_FEATURE WHERE APP_ID=? AND PROFILE_ID=? AND FEATURE_ID=?");
			del.setString(1, appId);
			del.setString(2, profileId);
			del.setString(3, featureId);
			del.execute();
			if (!del.getConnection().getAutoCommit()) {
				del.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, del);
		}

		PreparedStatement ins = null;
		try {
			ins = securityDb.getPreparedStatement(
					"INSERT INTO APP_PROFILE_FEATURE (APP_ID, PROFILE_ID, FEATURE_ID, ENABLED) VALUES (?, ?, ?, ?)");
			ins.setString(1, appId);
			ins.setString(2, profileId);
			ins.setString(3, featureId);
			ins.setBoolean(4, enabled);
			ins.execute();
			if (!ins.getConnection().getAutoCommit()) {
				ins.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to set profile feature: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ins);
		}
	}

	public static List<Map<String, Object>> getProfileFeatures(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> result = new ArrayList<>();

		// Get all features for the app with their enabled state for this profile
		String sql = "SELECT f.FEATURE_ID, f.FEATURE_KEY, f.DESCRIPTION, pf.ENABLED "
				+ "FROM APP_FEATURE f "
				+ "LEFT JOIN APP_PROFILE_FEATURE pf "
				+ "ON f.FEATURE_ID = pf.FEATURE_ID AND pf.APP_ID=? AND pf.PROFILE_ID=? "
				+ "WHERE f.APP_ID=? "
				+ "ORDER BY f.FEATURE_KEY";

		PreparedStatement ps = null;
		java.sql.ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			ps.setString(3, appId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("featureId", rs.getString(1));
				row.put("featureKey", rs.getString(2));
				row.put("description", rs.getString(3));
				Boolean enabled = rs.getObject(4) != null ? rs.getBoolean(4) : false;
				row.put("enabled", enabled);
				result.add(row);
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve profile features: " + e.getMessage());
		} finally {
			if (rs != null) { try { rs.close(); } catch (Exception ignore) {} }
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return result;
	}

	// -----------------------------------------------------------------------
	// User-Profile Assignment
	// -----------------------------------------------------------------------

	public static void assignUserProfile(String appId, String userId,
			String profileId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String actorId = getUserId(actor);
		Timestamp now = nowUtc();

		PreparedStatement del = null;
		try {
			del = securityDb.getPreparedStatement(
					"DELETE FROM APP_USER_PROFILE WHERE APP_ID=? AND USER_ID=?");
			del.setString(1, appId);
			del.setString(2, userId);
			del.execute();
			if (!del.getConnection().getAutoCommit()) {
				del.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, del);
		}

		PreparedStatement ins = null;
		try {
			ins = securityDb.getPreparedStatement(
					"INSERT INTO APP_USER_PROFILE (APP_ID, USER_ID, PROFILE_ID, ASSIGNED_BY, ASSIGNED_AT) VALUES (?, ?, ?, ?, ?)");
			ins.setString(1, appId);
			ins.setString(2, userId);
			ins.setString(3, profileId);
			ins.setString(4, actorId);
			ins.setTimestamp(5, now);
			ins.execute();
			if (!ins.getConnection().getAutoCommit()) {
				ins.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to assign user profile: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ins);
		}
	}

	public static void removeUserProfile(String appId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"DELETE FROM APP_USER_PROFILE WHERE APP_ID=? AND USER_ID=?");
			ps.setString(1, appId);
			ps.setString(2, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void removeAllUserProfilesForApp(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"DELETE FROM APP_USER_PROFILE WHERE APP_ID=?");
			ps.setString(1, appId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void removeUserProfiles(String appId, List<String> userIds) {
		if (userIds == null || userIds.isEmpty()) {
			return;
		}
		for (String userId : userIds) {
			removeUserProfile(appId, userId);
		}
	}

	public static Map<String, Object> getUserProfile(String appId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		// Explicit assignment
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_USER_PROFILE__PROFILE_ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_USER_PROFILE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_USER_PROFILE__USER_ID", "==", userId));

		String profileId = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				profileId = (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		boolean explicit = profileId != null;
		if (profileId == null) {
			// Fall back to default profile
			profileId = getDefaultProfileId(securityDb, appId);
		}
		if (profileId == null) {
			return null;
		}

		// Get profile name
		SelectQueryStruct nameQs = new SelectQueryStruct();
		nameQs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_NAME"));
		nameQs.addSelector(new QueryColumnSelector("APP_PROFILE__IS_DEFAULT"));
		nameQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_PROFILE__PROFILE_ID", "==", profileId));
		nameQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_PROFILE__APP_ID", "==", appId));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, nameQs)) {
			if (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				Map<String, Object> result = new HashMap<>();
				result.put("profileId", profileId);
				result.put("profileName", row[0]);
				result.put("isDefaultProfile", row[1]);
				result.put("isExplicitAssignment", explicit);
				return result;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	public static List<Map<String, Object>> getProfileUsers(String appId, String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> users = new ArrayList<>();

		String sql = "SELECT up.USER_ID, u.NAME, u.EMAIL, up.ASSIGNED_BY, up.ASSIGNED_AT "
				+ "FROM APP_USER_PROFILE up "
				+ "INNER JOIN SMSS_USER u ON up.USER_ID = u.ID "
				+ "INNER JOIN PROJECTPERMISSION pp ON up.USER_ID = pp.USERID AND up.APP_ID = pp.PROJECTID "
				+ "WHERE up.APP_ID=? AND up.PROFILE_ID=?";

		PreparedStatement ps = null;
		java.sql.ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("userId", rs.getString(1));
				row.put("displayName", rs.getString(2));
				row.put("email", rs.getString(3));
				row.put("assignedBy", rs.getString(4));
				row.put("assignedAt", rs.getTimestamp(5) != null ? rs.getTimestamp(5).toString() : null);
				users.add(row);
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve profile users: " + e.getMessage());
		} finally {
			if (rs != null) { try { rs.close(); } catch (Exception ignore) {} }
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return users;
	}

	// -----------------------------------------------------------------------
	// Evaluation
	// -----------------------------------------------------------------------

	public static boolean checkFeature(String appId, String featureKey, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		String featureId = resolveFeatureId(securityDb, appId, featureKey);
		if (featureId == null) {
			return false;
		}

		String userId = getUserId(user);
		Map<String, Object> profile = getUserProfile(appId, userId);
		if (profile == null) {
			return false;
		}
		String profileId = (String) profile.get("profileId");

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE_FEATURE__ENABLED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_PROFILE_FEATURE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_PROFILE_FEATURE__PROFILE_ID", "==", profileId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_PROFILE_FEATURE__FEATURE_ID", "==", featureId));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				return val != null && (Boolean) val;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return false;
	}

	public static Map<String, Object> getUserFeatures(String appId, User user) {
		Map<String, Object> result = new HashMap<>();
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		String userId = getUserId(user);
		Map<String, Object> profile = getUserProfile(appId, userId);
		if (profile == null) {
			return result;
		}
		String profileId = (String) profile.get("profileId");
		String profileName = (String) profile.get("profileName");
		boolean isDefaultProfile = Boolean.TRUE.equals(profile.get("isDefaultProfile"));

		String sql = "SELECT f.FEATURE_KEY, pf.FEATURE_ID "
				+ "FROM APP_PROFILE_FEATURE pf "
				+ "INNER JOIN APP_FEATURE f ON pf.FEATURE_ID = f.FEATURE_ID AND f.APP_ID=pf.APP_ID "
				+ "WHERE pf.APP_ID=? AND pf.PROFILE_ID=? AND pf.ENABLED=?";

		PreparedStatement ps = null;
		java.sql.ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, appId);
			ps.setString(2, profileId);
			ps.setBoolean(3, true);
			rs = ps.executeQuery();
			while (rs.next()) {
				String featureKey = rs.getString(1);
				String featureId = rs.getString(2);
				Map<String, Object> featureInfo = new HashMap<>();
				featureInfo.put("featureId", featureId);
				featureInfo.put("profileName", profileName);
				featureInfo.put("isDefaultProfile", isDefaultProfile);
				result.put(featureKey, featureInfo);
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve user features: " + e.getMessage());
		} finally {
			if (rs != null) { try { rs.close(); } catch (Exception ignore) {} }
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return result;
	}

	// -----------------------------------------------------------------------
	// Internal helpers
	// -----------------------------------------------------------------------

	private static void clearDefaultProfile(IRDBMSEngine securityDb, String appId) {
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"UPDATE APP_PROFILE SET IS_DEFAULT=? WHERE APP_ID=?");
			ps.setBoolean(1, false);
			ps.setString(2, appId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	private static String getDefaultProfileId(IRDBMSEngine securityDb, String appId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_PROFILE__PROFILE_ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_PROFILE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_PROFILE__IS_DEFAULT", "==", true));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	private static String resolveFeatureId(IRDBMSEngine securityDb, String appId, String featureKey) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE__FEATURE_ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_FEATURE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_FEATURE__FEATURE_KEY", "==", featureKey));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	private static long getProfileUserCount(IRDBMSEngine securityDb, String appId, String profileId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector countSel = new QueryFunctionSelector();
		countSel.setFunction(QueryFunctionHelper.COUNT);
		countSel.addInnerSelector(new QueryColumnSelector("APP_USER_PROFILE__USER_ID"));
		countSel.setAlias("cnt");
		qs.addSelector(countSel);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_USER_PROFILE__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"APP_USER_PROFILE__PROFILE_ID", "==", profileId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				return val != null ? ((Number) val).longValue() : 0;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return 0;
	}

	private static void validateFeatureKey(String featureKey) {
		if (featureKey == null || featureKey.isEmpty()) {
			throw new IllegalArgumentException("Feature key is required.");
		}
		if (featureKey.length() > 100) {
			throw new IllegalArgumentException("Feature key must be 100 characters or fewer.");
		}
		if (!featureKey.matches(FEATURE_KEY_PATTERN)) {
			throw new IllegalArgumentException(
					"Feature key may only contain letters, numbers, and hyphens.");
		}
	}

	private static String getUserId(User user) {
		if (user == null) {
			throw new IllegalArgumentException("User is required.");
		}
		return user.getPrimaryLoginToken().getId();
	}

	private static Timestamp nowUtc() {
		ZonedDateTime zdt = LocalDateTime.now().atZone(ZoneId.of("UTC"));
		return new Timestamp(zdt.toInstant().toEpochMilli());
	}
}
