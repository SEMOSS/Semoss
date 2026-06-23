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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
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

// Evaluation model: a flag has a MIN_VERSION; a user is enabled if their assigned version >= MIN_VERSION.
// Version 0 means everyone is enabled (no restriction).

public class AppFeatureFlagUtils {

	private static final Logger classLogger = LogManager.getLogger(AppFeatureFlagUtils.class);

	public static final String PLATFORM_APP_ID = "SEMOSS";

	private AppFeatureFlagUtils() {
	}

	/**
	 * Returns true if the user is authorized to manage flags for the given appId.
	 * Platform flags (appId == PLATFORM_APP_ID) require admin; app flags require
	 * owner or admin.
	 */
	public static boolean canManageFlags(User user, String appId) {
		if (PLATFORM_APP_ID.equals(appId)) {
			return SecurityAdminUtils.userIsAdmin(user);
		}
		return SecurityProjectUtils.userIsOwner(user, appId) || SecurityAdminUtils.userIsAdmin(user);
	}

	/**
	 * Returns true if the user can evaluate flags for the given appId.
	 * Platform flags are accessible to any authenticated user; app flags require
	 * view access.
	 */
	public static boolean canEvaluateFlags(User user, String appId) {
		if (PLATFORM_APP_ID.equals(appId)) {
			return user != null;
		}
		return SecurityProjectUtils.userCanViewProject(user, appId);
	}

	// -------------------------------------------------------------------------
	// Evaluation
	// -------------------------------------------------------------------------

	public static boolean evaluate(String appId, String flagId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		// 1. Resolve MIN_VERSION and DEFAULT_VERSION from the flag
		int minVersion = -1;
		int defaultVersion = 0;
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__MIN_VERSION"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__DEFAULT_VERSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE_FLAG__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE_FLAG__FLAG_ID", "==", flagId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				minVersion = row[0] != null ? ((Number) row[0]).intValue() : 0;
				defaultVersion = row[1] != null ? ((Number) row[1]).intValue() : 0;
			}
		} catch (Exception e) {
			classLogger.error("Error resolving flag {}/{}", appId, flagId, e);
			return false;
		}
		if (minVersion < 0) {
			// flag not found
			return false;
		}

		// 2. Get user's assigned version; fall back to flag's DEFAULT_VERSION if
		// unassigned
		String userId = getUserId(user);
		int userVersion = getUserVersion(appId, flagId, userId);
		int effectiveVersion = userVersion >= 0 ? userVersion : defaultVersion;

		return effectiveVersion >= minVersion;
	}

	// -------------------------------------------------------------------------
	// CRUD operations
	// -------------------------------------------------------------------------

	public static String createFlag(String appId, String flagKey, String description, String createdBy) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String flagId = UUID.randomUUID().toString();
		String query = "INSERT INTO APP_FEATURE_FLAG (FLAG_ID, APP_ID, FLAG_KEY, DESCRIPTION, MIN_VERSION, DEFAULT_VERSION, CREATED_BY, CREATED_AT) VALUES (?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int idx = 1;
			ps.setString(idx++, flagId);
			ps.setString(idx++, appId);
			ps.setString(idx++, flagKey);
			ps.setString(idx++, description);
			ps.setInt(idx++, 1); // minVersion: off by default until users are placed in v1+
			ps.setInt(idx++, 0); // defaultVersion: unassigned users are off
			ps.setString(idx++, createdBy);
			ps.setTimestamp(idx++, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Error creating flag {}/{}", appId, flagKey, e);
			throw new IllegalArgumentException("Failed to create feature flag: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		// Automatically create v0 bucket for the new flag
		createVersionBucketRecord(appId, flagId, 0, "Default version - feature disabled for most users");

		return flagId;
	}

	public static void updateFlag(String appId, String flagId, Integer minVersion, Integer defaultVersion,
			String description) {
		if (minVersion == null && defaultVersion == null && description == null) {
			throw new IllegalArgumentException(
					"At least one of 'minVersion', 'defaultVersion', or 'description' is required");
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		StringBuilder sql = new StringBuilder("UPDATE APP_FEATURE_FLAG SET ");
		List<Object> params = new ArrayList<>();
		if (minVersion != null) {
			sql.append("MIN_VERSION=?");
			params.add(minVersion);
		}
		if (defaultVersion != null) {
			if (!params.isEmpty())
				sql.append(", ");
			sql.append("DEFAULT_VERSION=?");
			params.add(defaultVersion);
		}
		if (description != null) {
			if (!params.isEmpty())
				sql.append(", ");
			sql.append("DESCRIPTION=?");
			params.add(description);
		}
		sql.append(" WHERE APP_ID=? AND FLAG_ID=?");
		params.add(appId);
		params.add(flagId);

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(sql.toString());
			for (int i = 0; i < params.size(); i++) {
				Object p = params.get(i);
				if (p instanceof Integer) {
					ps.setInt(i + 1, (Integer) p);
				} else {
					ps.setString(i + 1, (String) p);
				}
			}
			int updated = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			if (updated == 0) {
				throw new IllegalArgumentException("Flag not found: " + appId + "/" + flagId);
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error updating flag {}/{}", appId, flagId, e);
			throw new IllegalArgumentException("Failed to update feature flag: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void deleteFlag(String appId, String flagId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_FEATURE_FLAG WHERE APP_ID=? AND FLAG_ID=?");
			ps.setString(1, appId);
			ps.setString(2, flagId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Error deleting flag {}/{}", appId, flagId, e);
			throw new IllegalArgumentException("Failed to delete feature flag: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		// Clean up associated version buckets and user version assignments
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_VERSION_BUCKET WHERE APP_ID=? AND FLAG_ID=?");
			ps.setString(1, appId);
			ps.setString(2, flagId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Error deleting version buckets for flag {}/{}", appId, flagId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM APP_USER_VERSION WHERE APP_ID=? AND FLAG_ID=?");
			ps.setString(1, appId);
			ps.setString(2, flagId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Error deleting user versions for flag {}/{}", appId, flagId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static List<Map<String, Object>> getFlagsForApp(String appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> results = new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__FLAG_ID"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__FLAG_KEY"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__DESCRIPTION"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__MIN_VERSION"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__DEFAULT_VERSION"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__CREATED_BY"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__CREATED_AT"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE_FLAG__APP_ID", "==", appId));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				Map<String, Object> flag = new HashMap<>();
				flag.put("flagId", row[0]);
				flag.put("flagKey", row[1]);
				flag.put("description", row[2]);
				flag.put("minVersion", row[3] != null ? ((Number) row[3]).intValue() : 0);
				flag.put("defaultVersion", row[4] != null ? ((Number) row[4]).intValue() : 0);
				flag.put("createdBy", row[5]);
				flag.put("createdAt", row[6] != null ? row[6].toString() : null);
				results.add(flag);
			}
		} catch (Exception e) {
			classLogger.error("Error fetching flags for app {}", appId, e);
		}
		return results;
	}

	// -------------------------------------------------------------------------
	// User version management
	// -------------------------------------------------------------------------

	public static void setUserVersions(String appId, String flagId, List<String> userIds, int version) {
		for (String userId : userIds) {
			setUserVersion(appId, flagId, userId, version);
		}
	}

	public static void setUserVersion(String appId, String flagId, String userId, int version) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// Try update first, then insert
		int updated = 0;
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"UPDATE APP_USER_VERSION SET VERSION=? WHERE APP_ID=? AND FLAG_ID=? AND USER_ID=?");
			ps.setInt(1, version);
			ps.setString(2, appId);
			ps.setString(3, flagId);
			ps.setString(4, userId);
			updated = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Error updating user version", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		if (updated == 0) {
			try {
				ps = securityDb
						.getPreparedStatement(
								"INSERT INTO APP_USER_VERSION (APP_ID, FLAG_ID, USER_ID, VERSION) VALUES (?,?,?,?)");
				ps.setString(1, appId);
				ps.setString(2, flagId);
				ps.setString(3, userId);
				ps.setInt(4, version);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (Exception e) {
				classLogger.error("Error inserting user version", e);
				throw new IllegalArgumentException("Failed to set user version: " + e.getMessage());
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}

	public static int getUserVersion(String appId, String flagId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_USER_VERSION__VERSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_VERSION__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_VERSION__FLAG_ID", "==", flagId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_VERSION__USER_ID", "==", userId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					return ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Error getting user version for {}/{}", appId, userId, e);
		}
		return -1;
	}

	// -------------------------------------------------------------------------
	// Bulk evaluation for client-side handling
	// -------------------------------------------------------------------------

	public static Map<String, Boolean> getUserFeatureFlags(String appId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, Boolean> result = new HashMap<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__FLAG_ID"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__FLAG_KEY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE_FLAG__APP_ID", "==", appId));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String flagId = (String) row[0];
				String flagKey = (String) row[1];
				result.put(flagKey, evaluate(appId, flagId, user));
			}
		} catch (Exception e) {
			classLogger.error("Error fetching flags for bulk evaluation, app={}", appId, e);
		}
		return result;
	}

	public static Map<String, Map<String, Object>> getUserFeatureFlagDetails(String appId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, Map<String, Object>> result = new HashMap<>();
		String userId = getUserId(user);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__FLAG_ID"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__FLAG_KEY"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__MIN_VERSION"));
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__DEFAULT_VERSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE_FLAG__APP_ID", "==", appId));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String flagId = (String) row[0];
				String flagKey = (String) row[1];
				int minVersion = row[2] != null ? ((Number) row[2]).intValue() : 0;
				int defaultVersion = row[3] != null ? ((Number) row[3]).intValue() : 0;
				int userVersion = getUserVersion(appId, flagId, userId);
				int effectiveVersion = userVersion >= 0 ? userVersion : defaultVersion;

				Map<String, Object> detail = new HashMap<>();
				detail.put("flagId", flagId);
				detail.put("enabled", effectiveVersion >= minVersion);
				detail.put("userVersion", userVersion);
				detail.put("defaultVersion", defaultVersion);
				detail.put("effectiveVersion", effectiveVersion);
				detail.put("minVersion", minVersion);
				result.put(flagKey, detail);
			}
		} catch (Exception e) {
			classLogger.error("Error fetching flag details for bulk evaluation, app={}", appId, e);
		}
		return result;
	}

	public static Map<Integer, Map<String, Object>> getVersionBucketsWithDetails(String appId, String flagId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<Integer, Map<String, Object>> buckets = new HashMap<>();

		// First load all defined buckets and their descriptions from APP_VERSION_BUCKET
		SelectQueryStruct bucketQs = new SelectQueryStruct();
		bucketQs.addSelector(new QueryColumnSelector("APP_VERSION_BUCKET__VERSION"));
		bucketQs.addSelector(new QueryColumnSelector("APP_VERSION_BUCKET__DESCRIPTION"));
		bucketQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_VERSION_BUCKET__APP_ID", "==", appId));
		bucketQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_VERSION_BUCKET__FLAG_ID", "==", flagId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, bucketQs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				int version = row[0] != null ? ((Number) row[0]).intValue() : 0;
				String description = row[1] != null ? (String) row[1] : "";
				Map<String, Object> bucket = new HashMap<>();
				bucket.put("version", version);
				bucket.put("users", new ArrayList<String>());
				bucket.put("description", description);
				buckets.put(version, bucket);
			}
		} catch (Exception e) {
			classLogger.error("Error fetching version bucket definitions for app {}/{}", appId, flagId, e);
		}

		// Then populate each bucket's users from APP_USER_VERSION
		SelectQueryStruct userQs = new SelectQueryStruct();
		userQs.addSelector(new QueryColumnSelector("APP_USER_VERSION__VERSION"));
		userQs.addSelector(new QueryColumnSelector("APP_USER_VERSION__USER_ID"));
		userQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_VERSION__APP_ID", "==", appId));
		userQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_VERSION__FLAG_ID", "==", flagId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, userQs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				int version = row[0] != null ? ((Number) row[0]).intValue() : 0;
				String userId = (String) row[1];
				if (userId == null) {
					continue;
				}
				// Only populate users into declared buckets for this flag
				if (buckets.containsKey(version)) {
					@SuppressWarnings("unchecked")
					List<String> users = (List<String>) buckets.get(version).get("users");
					users.add(userId);
				}
			}
		} catch (Exception e) {
			classLogger.error("Error fetching version bucket users for app {}", appId, e);
		}
		return buckets;
	}

	/**
	 * Creates a version bucket for a feature flag. Validates the flag exists.
	 * This allows the version to appear in UI even before users are assigned to it.
	 */
	public static void createVersionBucket(String appId, String flagId, int version, String description) {
		if (!flagExists(appId, flagId)) {
			throw new IllegalArgumentException("Feature flag not found: " + appId + "/" + flagId);
		}
		if (versionBucketExists(appId, flagId, version)) {
			throw new IllegalArgumentException(
					"Version bucket already exists: app=" + appId + ", flag=" + flagId + ", version=" + version);
		}
		createVersionBucketRecord(appId, flagId, version, description);
	}

	/**
	 * Inserts a row into APP_VERSION_BUCKET without any flag validation. Used
	 * internally.
	 */
	private static void createVersionBucketRecord(String appId, String flagId, int version, String description) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO APP_VERSION_BUCKET (APP_ID, FLAG_ID, VERSION, DESCRIPTION) VALUES (?,?,?,?)");
			ps.setString(1, appId);
			ps.setString(2, flagId);
			ps.setInt(3, version);
			ps.setString(4, description != null ? description : "");
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Error creating version bucket record {}/{}", appId, version, e);
			throw new IllegalArgumentException("Failed to create version bucket: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Updates only the description for an existing version bucket.
	 */
	public static void updateVersionBucketDescription(String appId, String flagId, int version, String description) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"UPDATE APP_VERSION_BUCKET SET DESCRIPTION=? WHERE APP_ID=? AND FLAG_ID=? AND VERSION=?");
			ps.setString(1, description != null ? description : "");
			ps.setString(2, appId);
			ps.setString(3, flagId);
			ps.setInt(4, version);
			int updated = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			if (updated == 0) {
				throw new IllegalArgumentException(
						"Version bucket not found: app=" + appId + ", flag=" + flagId + ", version=" + version);
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error updating version bucket description {}/{}", appId, flagId, e);
			throw new IllegalArgumentException("Failed to update version bucket description: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Deletes a version bucket definition. Does not affect user assignments.
	 */
	public static void deleteVersionBucket(String appId, String flagId, int version) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"DELETE FROM APP_VERSION_BUCKET WHERE APP_ID=? AND FLAG_ID=? AND VERSION=?");
			ps.setString(1, appId);
			ps.setString(2, flagId);
			ps.setInt(3, version);
			int deleted = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			if (deleted == 0) {
				throw new IllegalArgumentException(
						"Version bucket not found: app=" + appId + ", flag=" + flagId + ", version=" + version);
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error deleting version bucket {}/{}", appId, flagId, e);
			throw new IllegalArgumentException("Failed to delete version bucket: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Removes a user from a flag (deletes the assignment row). User falls back to
	 * flag's defaultVersion.
	 */
	public static void removeUserFromFlag(String appId, String flagId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"DELETE FROM APP_USER_VERSION WHERE APP_ID=? AND FLAG_ID=? AND USER_ID=?");
			ps.setString(1, appId);
			ps.setString(2, flagId);
			ps.setString(3, userId);
			ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Error removing user {} from flag {}/{}", userId, appId, flagId, e);
			throw new IllegalArgumentException("Failed to remove user from flag: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Returns all version numbers assigned for an app (including empty buckets).
	 */
	public static List<Integer> getVersionsForFlag(String appId, String flagId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Integer> versions = new ArrayList<>();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_USER_VERSION__VERSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_VERSION__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_USER_VERSION__FLAG_ID", "==", flagId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					int version = ((Number) val).intValue();
					if (!versions.contains(version)) {
						versions.add(version);
					}
				}
			}
		} catch (Exception e) {
			classLogger.error("Error fetching versions for app {}/{}", appId, flagId, e);
		}
		return versions;
	}

	/**
	 * Checks if a feature flag exists for the app.
	 */
	public static boolean flagExists(String appId, String flagId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_FEATURE_FLAG__FLAG_ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE_FLAG__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_FEATURE_FLAG__FLAG_ID", "==", flagId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Error checking flag existence {}/{}", appId, flagId, e);
			return false;
		}
	}

	/**
	 * Checks if a version bucket already exists for an app and flag.
	 */
	public static boolean versionBucketExists(String appId, String flagId, int version) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("APP_VERSION_BUCKET__VERSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_VERSION_BUCKET__APP_ID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_VERSION_BUCKET__FLAG_ID", "==", flagId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("APP_VERSION_BUCKET__VERSION", "==", version + ""));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Error checking version bucket existence {}/{}", appId, version, e);
			return false;
		}
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	public static String getUserId(User user) {
		if (user == null) {
			return null;
		}
		AuthProvider primary = user.getPrimaryLogin();
		if (primary != null) {
			AccessToken token = user.getAccessToken(primary);
			if (token != null) {
				return token.getId();
			}
		}
		List<AuthProvider> logins = user.getLogins();
		if (logins != null && !logins.isEmpty()) {
			AccessToken token = user.getAccessToken(logins.get(0));
			if (token != null) {
				return token.getId();
			}
		}
		return null;
	}

}
