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
package prerna.reactor.platformprofile;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
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

public class PlatformProfileUtils {

	private static final Logger classLogger = LogManager.getLogger(PlatformProfileUtils.class);

	public static final Set<String> PREDEFINED_FEATURE_KEYS = Collections.unmodifiableSet(
			new HashSet<>(Arrays.asList(
					"nav.app-catalog",
					"nav.skills",
					"nav.settings",
					"nav.engine")));

	private PlatformProfileUtils() {
	}

	// ─── Profile CRUD ────────────────────────────────────────────────────────

	/** Creates a new platform profile with the given name and description and returns its id, name, and description. */
	public static Map<String, Object> createProfile(String name, String description, User user) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Profile name cannot be blank.");
		}
		String profileId = UUID.randomUUID().toString();
		String actorId = getUserId(user);
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

	/** Updates the name and/or description of an existing platform profile identified by {@code profileId}. */
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

	/** Deletes a platform profile and its feature rows; throws if any users are still assigned to it. */
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

	/** Returns all platform profiles ordered by name, each with id, name, description, createdBy, createdAt, and userCount. */
	public static List<Map<String, Object>> getProfiles(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE__PROFILE_ID", "profileId"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE__PROFILE_NAME", "profileName"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE__DESCRIPTION", "description"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE__CREATED_BY", "createdBy"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE__CREATED_AT", "createdAt"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, "PLATFORM_USER_PROFILE__USER_ID", "userCount"));
		qs.addRelation("PLATFORM_PROFILE__PROFILE_ID", "PLATFORM_USER_PROFILE__PROFILE_ID", "left.join");
		qs.addGroupBy(new QueryColumnSelector("PLATFORM_PROFILE__PROFILE_ID"));
		qs.addGroupBy(new QueryColumnSelector("PLATFORM_PROFILE__PROFILE_NAME"));
		qs.addGroupBy(new QueryColumnSelector("PLATFORM_PROFILE__DESCRIPTION"));
		qs.addGroupBy(new QueryColumnSelector("PLATFORM_PROFILE__CREATED_BY"));
		qs.addGroupBy(new QueryColumnSelector("PLATFORM_PROFILE__CREATED_AT"));
		qs.addOrderBy(new QueryColumnOrderBySelector("PLATFORM_PROFILE__PROFILE_NAME"));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	// ─── Profile-Feature Assignment ──────────────────────────────────────────

	/** Sets the enabled state of a predefined platform nav feature for the specified profile; replaces any existing row. */
	public static void setProfileFeature(String profileId, String featureKey, boolean enabled, User user) {
		if (!PREDEFINED_FEATURE_KEYS.contains(featureKey)) {
			throw new IllegalArgumentException(
					"Unknown platform feature key: " + featureKey
							+ ". Valid keys: " + new java.util.TreeSet<>(PREDEFINED_FEATURE_KEYS));
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

	/** Returns all predefined platform feature keys with their enabled status for the given profile; unknown keys default to {@code false}. */
	public static Map<String, Boolean> getProfileFeatures(String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, Boolean> result = new LinkedHashMap<>();
		for (String key : PREDEFINED_FEATURE_KEYS) {
			result.put(key, Boolean.FALSE);
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE_FEATURE__FEATURE_KEY"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE_FEATURE__ENABLED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PLATFORM_PROFILE_FEATURE__PROFILE_ID", "==", profileId));

		for (Object[] row : QueryExecutionUtility.flushRsToListOfObjArray(securityDb, qs)) {
			String key = (String) row[0];
			if (key != null && PREDEFINED_FEATURE_KEYS.contains(key)) {
				Object enabledVal = row[1];
				boolean enabled = enabledVal instanceof Boolean ? (Boolean) enabledVal
						: (enabledVal != null && "true".equalsIgnoreCase(enabledVal.toString()));
				result.put(key, enabled);
			}
		}
		return result;
	}

	// ─── User-Profile Assignment ─────────────────────────────────────────────

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

	/** Assigns a user to a platform profile, replacing any existing assignment for that user. */
	public static void assignUserProfile(String userId, String profileId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String actorId = getUserId(actor);
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

	/**
	 * Bulk-assigns a list of users to a platform profile. Each user is processed independently.
	 * Because a user may be in at most one platform profile at a time, users already in this
	 * profile are silently skipped; users in a different profile are re-assigned (prior assignment
	 * removed first). Users not found in {@code SMSS_USER} are recorded in the errors bucket.
	 *
	 * @param userIds   non-empty list of {@code SMSS_USER.ID} values to assign
	 * @param profileId the platform profile to assign users to
	 * @param actor     the user performing the assignment
	 * @return a map with keys: {@code assigned} ({@code List<String>}), {@code skipped}
	 *         ({@code List<String>}), {@code errors} ({@code Map<String,String>})
	 */
	public static Map<String, Object> assignUsersToProfile(List<String> userIds, String profileId, User actor) {
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

			String currentProfileId = getAssignedProfileId(securityDb, userId);
			if (profileId.equals(currentProfileId)) {
				classLogger.debug("assignUsersToProfile: user '{}' already in platform profile '{}' — skipping", userId, profileId);
				skipped.add(userId);
				continue;
			}

			// Remove any existing platform profile assignment before inserting the new one
			if (currentProfileId != null) {
				PreparedStatement delPs = null;
				try {
					delPs = securityDb.getPreparedStatement("DELETE FROM PLATFORM_USER_PROFILE WHERE USER_ID=?");
					delPs.setString(1, userId);
					delPs.execute();
					if (!delPs.getConnection().getAutoCommit()) delPs.getConnection().commit();
				} catch (SQLException e) {
					classLogger.error("assignUsersToProfile: DB error removing existing assignment for user '{}'", userId, e);
					errors.put(userId, "Database error removing existing assignment.");
					continue;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, delPs);
				}
			}

			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(
						"INSERT INTO PLATFORM_USER_PROFILE (USER_ID, PROFILE_ID, ASSIGNED_BY, ASSIGNED_AT) VALUES (?,?,?,?)");
				ps.setString(1, userId);
				ps.setString(2, profileId);
				ps.setString(3, actorId);
				ps.setTimestamp(4, Utility.getCurrentSqlTimestampUTC());
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

	/** Removes the platform profile assignment for the specified user. */
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
		String userId = getUserId(user);
		String profileId = getAssignedProfileId(securityDb, userId);
		if (profileId == null) {
			Map<String, Boolean> all = new LinkedHashMap<>();
			for (String key : PREDEFINED_FEATURE_KEYS) {
				all.put(key, Boolean.TRUE);
			}
			return all;
		}
		return getProfileFeatures(profileId);
	}

	/** Returns the list of users assigned to the given platform profile with their name, email, and assignment metadata. */
	public static List<Map<String, Object>> getPlatformProfileUsers(String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PLATFORM_USER_PROFILE__USER_ID", "userId"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_USER_PROFILE__ASSIGNED_BY", "assignedBy"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_USER_PROFILE__ASSIGNED_AT", "assignedAt"));
		qs.addRelation("PLATFORM_USER_PROFILE__USER_ID", "SMSS_USER__ID", "left.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PLATFORM_USER_PROFILE__PROFILE_ID", "==", profileId));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	// ─── Private helpers ─────────────────────────────────────────────────────

	private static String getUserId(User user) {
		if (user == null) return null;
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		return token != null ? token.getId() : null;
	}

	private static int getAssignedUserCount(IRDBMSEngine securityDb, String profileId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector countFn = new QueryFunctionSelector();
		countFn.addInnerSelector(new QueryColumnSelector("PLATFORM_USER_PROFILE__USER_ID"));
		countFn.setFunction(QueryFunctionHelper.COUNT);
		qs.addSelector(countFn);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PLATFORM_USER_PROFILE__PROFILE_ID", "==", profileId));
		Integer count = QueryExecutionUtility.flushToInteger(securityDb, qs);
		return count != null ? count : 0;
	}

	private static String getAssignedProfileId(IRDBMSEngine securityDb, String userId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PLATFORM_USER_PROFILE__PROFILE_ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PLATFORM_USER_PROFILE__USER_ID", "==", userId));
		return QueryExecutionUtility.flushToString(securityDb, qs);
	}
}
