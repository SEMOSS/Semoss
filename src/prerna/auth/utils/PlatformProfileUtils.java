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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

public class PlatformProfileUtils extends AbstractSecurityUtils {

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

	public static boolean canManage(User user) {
		return SecurityAdminUtils.userIsAdmin(user);
	}

	// -----------------------------------------------------------------------
	// Profile CRUD
	// -----------------------------------------------------------------------

	public static Map<String, Object> createProfile(String name, String description, User user) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Profile name is required.");
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String profileId = UUID.randomUUID().toString();
		String actorId = getUserId(user);
		Timestamp now = nowUtc();

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO PLATFORM_PROFILE (PROFILE_ID, PROFILE_NAME, DESCRIPTION, CREATED_BY, CREATED_AT) VALUES (?, ?, ?, ?, ?)");
			ps.setString(1, profileId);
			ps.setString(2, name.trim());
			ps.setString(3, description != null ? description : "");
			ps.setString(4, actorId);
			ps.setTimestamp(5, now);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to create platform profile: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		Map<String, Object> result = new HashMap<>();
		result.put("profileId", profileId);
		result.put("profileName", name.trim());
		result.put("description", description != null ? description : "");
		return result;
	}

	public static void updateProfile(String profileId, String name, String description, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		StringBuilder sb = new StringBuilder("UPDATE PLATFORM_PROFILE SET");
		List<Object> params = new ArrayList<>();

		if (name != null && !name.trim().isEmpty()) {
			sb.append(" PROFILE_NAME=?,");
			params.add(name.trim());
		}
		if (description != null) {
			sb.append(" DESCRIPTION=?,");
			params.add(description);
		}
		if (params.isEmpty()) {
			return;
		}
		String sql = sb.substring(0, sb.length() - 1) + " WHERE PROFILE_ID=?";
		params.add(profileId);

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
			throw new IllegalArgumentException("Failed to update platform profile: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void deleteProfile(String profileId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		SelectQueryStruct countQs = new SelectQueryStruct();
		QueryFunctionSelector countSel = new QueryFunctionSelector();
		countSel.setFunction(QueryFunctionHelper.COUNT);
		countSel.addInnerSelector(new QueryColumnSelector("PLATFORM_USER_PROFILE__USER_ID"));
		countSel.setAlias("cnt");
		countQs.addSelector(countSel);
		countQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"PLATFORM_USER_PROFILE__PROFILE_ID", "==", profileId));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, countQs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				long count = val != null ? ((Number) val).longValue() : 0;
				if (count > 0) {
					throw new IllegalArgumentException(
							"Cannot delete: " + count + " user(s) are assigned to this profile. Reassign them first.");
				}
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		String[] deletes = {
			"DELETE FROM PLATFORM_PROFILE WHERE PROFILE_ID=?",
			"DELETE FROM PLATFORM_PROFILE_FEATURE WHERE PROFILE_ID=?"
		};
		for (String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(deleteQuery);
				ps.setString(1, profileId);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Failed to delete platform profile: " + e.getMessage());
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}

	public static List<Map<String, Object>> getProfiles(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> profiles = new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE__PROFILE_ID"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE__PROFILE_NAME"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE__DESCRIPTION"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE__CREATED_BY"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE__CREATED_AT"));
		qs.addOrderBy("PLATFORM_PROFILE__PROFILE_NAME", "ASC");

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String pid = (String) row[0];
				Map<String, Object> profile = new HashMap<>();
				profile.put("profileId", pid);
				profile.put("profileName", row[1]);
				profile.put("description", row[2]);
				profile.put("createdBy", row[3]);
				profile.put("createdAt", row[4] != null ? row[4].toString() : null);
				profile.put("userCount", getPlatformProfileUserCount(securityDb, pid));
				profiles.add(profile);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve platform profiles: " + e.getMessage());
		}
		return profiles;
	}

	// -----------------------------------------------------------------------
	// Platform Feature Toggles
	// -----------------------------------------------------------------------

	public static void setProfileFeature(String profileId, String featureKey,
			boolean enabled, User user) {
		if (!PREDEFINED_FEATURE_KEYS.contains(featureKey)) {
			throw new IllegalArgumentException(
					"Unknown platform feature key: " + featureKey
					+ ". Valid keys: " + PREDEFINED_FEATURE_KEYS);
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		PreparedStatement del = null;
		try {
			del = securityDb.getPreparedStatement(
					"DELETE FROM PLATFORM_PROFILE_FEATURE WHERE PROFILE_ID=? AND FEATURE_KEY=?");
			del.setString(1, profileId);
			del.setString(2, featureKey);
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
					"INSERT INTO PLATFORM_PROFILE_FEATURE (PROFILE_ID, FEATURE_KEY, ENABLED) VALUES (?, ?, ?)");
			ins.setString(1, profileId);
			ins.setString(2, featureKey);
			ins.setBoolean(3, enabled);
			ins.execute();
			if (!ins.getConnection().getAutoCommit()) {
				ins.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to set platform feature: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ins);
		}
	}

	public static Map<String, Boolean> getProfileFeatures(String profileId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, Boolean> result = new HashMap<>();

		// Default all predefined keys to false
		for (String key : PREDEFINED_FEATURE_KEYS) {
			result.put(key, false);
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE_FEATURE__FEATURE_KEY"));
		qs.addSelector(new QueryColumnSelector("PLATFORM_PROFILE_FEATURE__ENABLED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"PLATFORM_PROFILE_FEATURE__PROFILE_ID", "==", profileId));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String key = (String) row[0];
				Boolean enabled = row[1] != null ? (Boolean) row[1] : false;
				if (PREDEFINED_FEATURE_KEYS.contains(key)) {
					result.put(key, enabled);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve platform profile features: " + e.getMessage());
		}
		return result;
	}

	// -----------------------------------------------------------------------
	// User-Profile Assignment
	// -----------------------------------------------------------------------

	public static void assignUserProfile(String userId, String profileId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String actorId = getUserId(actor);
		Timestamp now = nowUtc();

		PreparedStatement del = null;
		try {
			del = securityDb.getPreparedStatement(
					"DELETE FROM PLATFORM_USER_PROFILE WHERE USER_ID=?");
			del.setString(1, userId);
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
					"INSERT INTO PLATFORM_USER_PROFILE (USER_ID, PROFILE_ID, ASSIGNED_BY, ASSIGNED_AT) VALUES (?, ?, ?, ?)");
			ins.setString(1, userId);
			ins.setString(2, profileId);
			ins.setString(3, actorId);
			ins.setTimestamp(4, now);
			ins.execute();
			if (!ins.getConnection().getAutoCommit()) {
				ins.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to assign platform profile: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ins);
		}
	}

	public static void removeUserProfile(String userId, User actor) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"DELETE FROM PLATFORM_USER_PROFILE WHERE USER_ID=?");
			ps.setString(1, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to remove platform profile: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	// -----------------------------------------------------------------------
	// Evaluation
	// -----------------------------------------------------------------------

	public static Map<String, Boolean> getUserFeatures(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = getUserId(user);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PLATFORM_USER_PROFILE__PROFILE_ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"PLATFORM_USER_PROFILE__USER_ID", "==", userId));

		String profileId = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				profileId = (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (profileId == null) {
			// No profile assigned — fail open (all nav visible)
			Map<String, Boolean> allOpen = new HashMap<>();
			for (String key : PREDEFINED_FEATURE_KEYS) {
				allOpen.put(key, true);
			}
			return allOpen;
		}

		return getProfileFeatures(profileId);
	}

	// -----------------------------------------------------------------------
	// Internal helpers
	// -----------------------------------------------------------------------

	private static long getPlatformProfileUserCount(IRDBMSEngine securityDb, String profileId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector countSel = new QueryFunctionSelector();
		countSel.setFunction(QueryFunctionHelper.COUNT);
		countSel.addInnerSelector(new QueryColumnSelector("PLATFORM_USER_PROFILE__USER_ID"));
		countSel.setAlias("cnt");
		qs.addSelector(countSel);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				"PLATFORM_USER_PROFILE__PROFILE_ID", "==", profileId));
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
