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
 * 	MERMERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;

/**
 * Stores per-principal token limits outside permission tables.
 */
public class SecurityPrincipalTokenLimitUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityPrincipalTokenLimitUtils.class);

	public static final String ENGINE_USER_TABLE = "ENGINEUSERTOKENLIMIT";
	public static final String ENGINE_TEAM_TABLE = "ENGINETEAMTOKENLIMIT";
	public static final String PROJECT_USER_TABLE = "PROJECTUSERTOKENLIMIT";
	public static final String PROJECT_TEAM_TABLE = "PROJECTTEAMTOKENLIMIT";

	public static final String ALL_ENGINES_SENTINEL = "__ALL__";

	private SecurityPrincipalTokenLimitUtils() {
		// utility class
	}

	public static List<Map<String, Object>> getEngineUserTokenLimits(String engineId, String userId) {
		if (isBlank(engineId)) {
			return new ArrayList<>();
		}
		StringBuilder query = new StringBuilder("SELECT USER_ID, ENGINE_ID, USAGE_RESTRICTION, USAGE_FREQUENCY, "
				+ "MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAX_RESPONSE_TIME, IS_ACTIVE, CREATED_BY, "
				+ "CREATED_BY_TYPE, DATE_CREATED, DATE_MODIFIED FROM " + ENGINE_USER_TABLE + " WHERE ENGINE_ID=?");
		if (!isBlank(userId)) {
			query.append(" AND USER_ID=?");
		}
		query.append(" ORDER BY USER_ID, USAGE_FREQUENCY");
		return getLimits(query.toString(), engineId, userId);
	}

	public static List<Map<String, Object>> getProjectUserTokenLimits(String projectId, String userId, String engineId) {
		if (isBlank(projectId)) {
			return new ArrayList<>();
		}
		String scopedEngineId = normalizeScopedEngineId(engineId);
		StringBuilder query = new StringBuilder("SELECT USER_ID, PROJECT_ID, ENGINE_ID, USAGE_RESTRICTION, "
				+ "USAGE_FREQUENCY, MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAX_RESPONSE_TIME, "
				+ "RESTRICT_PER_MODEL, IS_ACTIVE, CREATED_BY, CREATED_BY_TYPE, DATE_CREATED, DATE_MODIFIED FROM "
				+ PROJECT_USER_TABLE + " WHERE PROJECT_ID=?");
		if (!isBlank(userId)) {
			query.append(" AND USER_ID=?");
		}
		if (!ALL_ENGINES_SENTINEL.equals(scopedEngineId)) {
			query.append(" AND ENGINE_ID=?");
		}
		query.append(" ORDER BY USER_ID, ENGINE_ID, USAGE_FREQUENCY");
		if (!isBlank(userId) && !ALL_ENGINES_SENTINEL.equals(scopedEngineId)) {
			return getLimits(query.toString(), projectId, userId, scopedEngineId);
		} else if (!isBlank(userId)) {
			return getLimits(query.toString(), projectId, userId);
		} else if (!ALL_ENGINES_SENTINEL.equals(scopedEngineId)) {
			return getLimits(query.toString(), projectId, scopedEngineId);
		}
		return getLimits(query.toString(), projectId);
	}

	public static List<Map<String, Object>> getEngineTeamTokenLimits(String engineId, String groupId, String groupType) {
		if (isBlank(engineId)) {
			return new ArrayList<>();
		}
		StringBuilder query = new StringBuilder("SELECT GROUP_ID, GROUP_TYPE, ENGINE_ID, USAGE_RESTRICTION, "
				+ "USAGE_FREQUENCY, MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAX_RESPONSE_TIME, IS_ACTIVE, "
				+ "CREATED_BY, CREATED_BY_TYPE, DATE_CREATED, DATE_MODIFIED FROM " + ENGINE_TEAM_TABLE
				+ " WHERE ENGINE_ID=?");
		if (!isBlank(groupId)) {
			query.append(" AND GROUP_ID=?");
		}
		if (!isBlank(groupType)) {
			query.append(" AND GROUP_TYPE=?");
		}
		query.append(" ORDER BY GROUP_ID, GROUP_TYPE, USAGE_FREQUENCY");
		if (!isBlank(groupId) && !isBlank(groupType)) {
			return getLimits(query.toString(), engineId, groupId, groupType);
		} else if (!isBlank(groupId)) {
			return getLimits(query.toString(), engineId, groupId);
		} else if (!isBlank(groupType)) {
			return getLimits(query.toString(), engineId, groupType);
		}
		return getLimits(query.toString(), engineId);
	}

	public static List<Map<String, Object>> getApplicableEngineUserTokenLimits(User user, String engineId) {
		List<Map<String, Object>> limits = new ArrayList<>();
		if (user == null || isBlank(engineId)) {
			return limits;
		}
		for (AuthProvider login : user.getLogins()) {
			AccessToken token = user.getAccessToken(login);
			if (token != null && !isBlank(token.getId())) {
				limits.addAll(getEngineUserTokenLimits(engineId, token.getId()));
			}
		}
		return limits;
	}

	public static List<Map<String, Object>> getApplicableEngineTeamTokenLimits(User user, String engineId) {
		List<Map<String, Object>> limits = new ArrayList<>();
		if (user == null || isBlank(engineId)) {
			return limits;
		}
		for (AuthProvider login : user.getLogins()) {
			AccessToken token = user.getAccessToken(login);
			if (token == null || isBlank(token.getUserGroupType())) {
				continue;
			}
			Collection<String> groups = token.getUserGroups();
			if (groups == null || groups.isEmpty()) {
				continue;
			}
			for (String groupId : groups) {
				if (!isBlank(groupId)) {
					limits.addAll(getEngineTeamTokenLimits(engineId, groupId, token.getUserGroupType()));
				}
			}
		}
		return limits;
	}

	public static List<Map<String, Object>> getApplicableProjectUserTokenLimits(User user, String projectId,
			String engineId) {
		List<Map<String, Object>> limits = new ArrayList<>();
		if (user == null || isBlank(projectId)) {
			return limits;
		}
		for (AuthProvider login : user.getLogins()) {
			AccessToken token = user.getAccessToken(login);
			if (token != null && !isBlank(token.getId())) {
				limits.addAll(getProjectUserTokenLimits(projectId, token.getId(), engineId));
				if (!ALL_ENGINES_SENTINEL.equals(normalizeScopedEngineId(engineId))) {
					limits.addAll(getProjectUserTokenLimits(projectId, token.getId(), ALL_ENGINES_SENTINEL));
				}
			}
		}
		return limits;
	}

	public static List<Map<String, Object>> getApplicableProjectTeamTokenLimits(User user, String projectId,
			String engineId) {
		List<Map<String, Object>> limits = new ArrayList<>();
		if (user == null || isBlank(projectId)) {
			return limits;
		}
		for (AuthProvider login : user.getLogins()) {
			AccessToken token = user.getAccessToken(login);
			if (token == null || isBlank(token.getUserGroupType())) {
				continue;
			}
			Collection<String> groups = token.getUserGroups();
			if (groups == null || groups.isEmpty()) {
				continue;
			}
			for (String groupId : groups) {
				if (!isBlank(groupId)) {
					limits.addAll(getProjectTeamTokenLimits(projectId, groupId, token.getUserGroupType(), engineId));
					if (!ALL_ENGINES_SENTINEL.equals(normalizeScopedEngineId(engineId))) {
						limits.addAll(getProjectTeamTokenLimits(projectId, groupId, token.getUserGroupType(),
								ALL_ENGINES_SENTINEL));
					}
				}
			}
		}
		return limits;
	}

	public static List<Map<String, Object>> getProjectTeamTokenLimits(String projectId, String groupId, String groupType,
			String engineId) {
		if (isBlank(projectId)) {
			return new ArrayList<>();
		}
		String scopedEngineId = normalizeScopedEngineId(engineId);
		StringBuilder query = new StringBuilder("SELECT GROUP_ID, GROUP_TYPE, PROJECT_ID, ENGINE_ID, "
				+ "USAGE_RESTRICTION, USAGE_FREQUENCY, MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, "
				+ "MAX_RESPONSE_TIME, RESTRICT_PER_MODEL, IS_ACTIVE, CREATED_BY, CREATED_BY_TYPE, DATE_CREATED, "
				+ "DATE_MODIFIED FROM " + PROJECT_TEAM_TABLE + " WHERE PROJECT_ID=?");
		List<String> params = new ArrayList<>();
		params.add(projectId);
		if (!isBlank(groupId)) {
			query.append(" AND GROUP_ID=?");
			params.add(groupId);
		}
		if (!isBlank(groupType)) {
			query.append(" AND GROUP_TYPE=?");
			params.add(groupType);
		}
		if (!ALL_ENGINES_SENTINEL.equals(scopedEngineId)) {
			query.append(" AND ENGINE_ID=?");
			params.add(scopedEngineId);
		}
		query.append(" ORDER BY GROUP_ID, GROUP_TYPE, ENGINE_ID, USAGE_FREQUENCY");
		return getLimits(query.toString(), params.toArray(new String[0]));
	}

	public static void setEngineUserTokenLimit(String userId, String engineId, String usageFrequency,
			String existingUsageFrequency, long maxTokens, long maxInputTokens, long maxOutputTokens,
			Double maxResponseTime, boolean isActive, String createdBy, String createdByType) {
		if (isBlank(userId)) {
			throw new IllegalArgumentException("Must provide a valid userId");
		}
		if (isBlank(engineId)) {
			throw new IllegalArgumentException("Must provide a valid engineId");
		}
		setLimit(ENGINE_USER_TABLE, null, userId, null, null, engineId, null, usageFrequency, existingUsageFrequency,
				maxTokens, maxInputTokens, maxOutputTokens, maxResponseTime, false, isActive, createdBy, createdByType);
	}

	public static void setProjectUserTokenLimit(String userId, String projectId, String engineId, String usageFrequency,
			String existingUsageFrequency, long maxTokens, long maxInputTokens, long maxOutputTokens,
			Double maxResponseTime, boolean restrictPerModel, boolean isActive, String createdBy,
			String createdByType) {
		if (isBlank(userId)) {
			throw new IllegalArgumentException("Must provide a valid userId");
		}
		if (isBlank(projectId)) {
			throw new IllegalArgumentException("Must provide a valid projectId");
		}
		setLimit(PROJECT_USER_TABLE, null, userId, null, null, null, projectId, usageFrequency, existingUsageFrequency,
				maxTokens, maxInputTokens, maxOutputTokens, maxResponseTime, restrictPerModel, isActive, createdBy,
				createdByType, normalizeScopedEngineId(engineId));
	}

	public static void setEngineTeamTokenLimit(String groupId, String groupType, String engineId, String usageFrequency,
			String existingUsageFrequency, long maxTokens, long maxInputTokens, long maxOutputTokens,
			Double maxResponseTime, boolean isActive, String createdBy, String createdByType) {
		if (isBlank(groupId) || isBlank(groupType)) {
			throw new IllegalArgumentException("Must provide a valid groupId and groupType");
		}
		if (isBlank(engineId)) {
			throw new IllegalArgumentException("Must provide a valid engineId");
		}
		setLimit(ENGINE_TEAM_TABLE, null, null, groupId, groupType, engineId, null, usageFrequency,
				existingUsageFrequency, maxTokens, maxInputTokens, maxOutputTokens, maxResponseTime, false, isActive,
				createdBy, createdByType);
	}

	public static void setProjectTeamTokenLimit(String groupId, String groupType, String projectId, String engineId,
			String usageFrequency, String existingUsageFrequency, long maxTokens, long maxInputTokens,
			long maxOutputTokens, Double maxResponseTime, boolean restrictPerModel, boolean isActive, String createdBy,
			String createdByType) {
		if (isBlank(groupId) || isBlank(groupType)) {
			throw new IllegalArgumentException("Must provide a valid groupId and groupType");
		}
		if (isBlank(projectId)) {
			throw new IllegalArgumentException("Must provide a valid projectId");
		}
		setLimit(PROJECT_TEAM_TABLE, null, null, groupId, groupType, null, projectId, usageFrequency,
				existingUsageFrequency, maxTokens, maxInputTokens, maxOutputTokens, maxResponseTime, restrictPerModel,
				isActive, createdBy, createdByType, normalizeScopedEngineId(engineId));
	}

	public static void removeEngineUserTokenLimit(String userId, String engineId, String usageFrequency) {
		removeLimit(ENGINE_USER_TABLE, "USER_ID=? AND ENGINE_ID=? AND USAGE_FREQUENCY=?", userId, engineId,
				validateFrequency(usageFrequency));
	}

	public static void removeProjectUserTokenLimit(String userId, String projectId, String engineId,
			String usageFrequency) {
		removeLimit(PROJECT_USER_TABLE, "USER_ID=? AND PROJECT_ID=? AND ENGINE_ID=? AND USAGE_FREQUENCY=?", userId,
				projectId, normalizeScopedEngineId(engineId), validateFrequency(usageFrequency));
	}

	public static void removeEngineTeamTokenLimit(String groupId, String groupType, String engineId,
			String usageFrequency) {
		removeLimit(ENGINE_TEAM_TABLE, "GROUP_ID=? AND GROUP_TYPE=? AND ENGINE_ID=? AND USAGE_FREQUENCY=?", groupId,
				groupType, engineId, validateFrequency(usageFrequency));
	}

	public static void removeProjectTeamTokenLimit(String groupId, String groupType, String projectId, String engineId,
			String usageFrequency) {
		removeLimit(PROJECT_TEAM_TABLE,
				"GROUP_ID=? AND GROUP_TYPE=? AND PROJECT_ID=? AND ENGINE_ID=? AND USAGE_FREQUENCY=?", groupId,
				groupType, projectId, normalizeScopedEngineId(engineId), validateFrequency(usageFrequency));
	}

	private static void setLimit(String tableName, String ignoredEntityType, String userId, String groupId,
			String groupType, String engineId, String projectId, String usageFrequency, String existingUsageFrequency,
			long maxTokens, long maxInputTokens, long maxOutputTokens, Double maxResponseTime,
			boolean restrictPerModel, boolean isActive, String createdBy, String createdByType) {
		setLimit(tableName, ignoredEntityType, userId, groupId, groupType, engineId, projectId, usageFrequency,
				existingUsageFrequency, maxTokens, maxInputTokens, maxOutputTokens, maxResponseTime, restrictPerModel,
				isActive, createdBy, createdByType, null);
	}

	private static void setLimit(String tableName, String ignoredEntityType, String userId, String groupId,
			String groupType, String engineId, String projectId, String usageFrequency, String existingUsageFrequency,
			long maxTokens, long maxInputTokens, long maxOutputTokens, Double maxResponseTime,
			boolean restrictPerModel, boolean isActive, String createdBy, String createdByType, String scopedEngineId) {
		String normalizedFrequency = validateFrequency(usageFrequency);
		String lookupFrequency = !isBlank(existingUsageFrequency) ? validateFrequency(existingUsageFrequency)
				: normalizedFrequency;
		if (!lookupFrequency.equals(normalizedFrequency)
				&& hasLimit(tableName, userId, groupId, groupType, engineId, projectId, scopedEngineId,
						normalizedFrequency)) {
			throw new IllegalArgumentException("A token limit already exists for usageFrequency " + normalizedFrequency);
		}

		String usageRestriction = resolveUsageRestriction(maxTokens, maxInputTokens, maxOutputTokens, maxResponseTime);
		if (usageRestriction == null) {
			throw new IllegalArgumentException("At least one usage limit must be provided");
		}

		boolean exists = hasLimit(tableName, userId, groupId, groupType, engineId, projectId, scopedEngineId,
				lookupFrequency);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			if (exists) {
				String updateSql = getUpdateSql(tableName);
				ps = securityDb.getPreparedStatement(updateSql);
				int idx = 1;
				bindString(ps, idx++, usageRestriction);
				bindString(ps, idx++, normalizedFrequency);
				bindLong(ps, idx++, maxTokens);
				bindLong(ps, idx++, maxInputTokens);
				bindLong(ps, idx++, maxOutputTokens);
				bindDouble(ps, idx++, maxResponseTime);
				if (isProjectTable(tableName)) {
					ps.setBoolean(idx++, restrictPerModel);
				}
				ps.setBoolean(idx++, isActive);
				bindString(ps, idx++, createdBy);
				bindString(ps, idx++, createdByType);
				idx = bindKey(ps, idx, tableName, userId, groupId, groupType, engineId, projectId, scopedEngineId,
						lookupFrequency);
				ps.execute();
			} else {
				String insertSql = getInsertSql(tableName);
				ps = securityDb.getPreparedStatement(insertSql);
				int idx = bindInsertKey(ps, 1, tableName, userId, groupId, groupType, engineId, projectId,
						scopedEngineId);
				bindString(ps, idx++, usageRestriction);
				bindString(ps, idx++, normalizedFrequency);
				bindLong(ps, idx++, maxTokens);
				bindLong(ps, idx++, maxInputTokens);
				bindLong(ps, idx++, maxOutputTokens);
				bindDouble(ps, idx++, maxResponseTime);
				if (isProjectTable(tableName)) {
					ps.setBoolean(idx++, restrictPerModel);
				}
				ps.setBoolean(idx++, isActive);
				bindString(ps, idx++, createdBy);
				bindString(ps, idx++, createdByType);
				ps.execute();
			}
			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error setting token limit in {}", tableName, e);
			throw new IllegalArgumentException("Failed to set token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	private static String getUpdateSql(String tableName) {
		StringBuilder sql = new StringBuilder("UPDATE ").append(tableName)
				.append(" SET USAGE_RESTRICTION=?, USAGE_FREQUENCY=?, MAX_TOKENS=?, MAX_INPUT_TOKENS=?, ")
				.append("MAX_OUTPUT_TOKENS=?, MAX_RESPONSE_TIME=?, ");
		if (isProjectTable(tableName)) {
			sql.append("RESTRICT_PER_MODEL=?, ");
		}
		sql.append("IS_ACTIVE=?, CREATED_BY=?, CREATED_BY_TYPE=?, DATE_MODIFIED=CURRENT_TIMESTAMP WHERE ");
		sql.append(getKeyWhereClause(tableName));
		return sql.toString();
	}

	private static String getInsertSql(String tableName) {
		if (ENGINE_USER_TABLE.equals(tableName)) {
			return "INSERT INTO " + tableName + " (USER_ID, ENGINE_ID, USAGE_RESTRICTION, USAGE_FREQUENCY, "
					+ "MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAX_RESPONSE_TIME, IS_ACTIVE, CREATED_BY, "
					+ "CREATED_BY_TYPE, DATE_CREATED, DATE_MODIFIED) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
					+ "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
		} else if (ENGINE_TEAM_TABLE.equals(tableName)) {
			return "INSERT INTO " + tableName + " (GROUP_ID, GROUP_TYPE, ENGINE_ID, USAGE_RESTRICTION, "
					+ "USAGE_FREQUENCY, MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAX_RESPONSE_TIME, "
					+ "IS_ACTIVE, CREATED_BY, CREATED_BY_TYPE, DATE_CREATED, DATE_MODIFIED) VALUES (?, ?, ?, ?, ?, ?, "
					+ "?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
		} else if (PROJECT_USER_TABLE.equals(tableName)) {
			return "INSERT INTO " + tableName + " (USER_ID, PROJECT_ID, ENGINE_ID, USAGE_RESTRICTION, "
					+ "USAGE_FREQUENCY, MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAX_RESPONSE_TIME, "
					+ "RESTRICT_PER_MODEL, IS_ACTIVE, CREATED_BY, CREATED_BY_TYPE, DATE_CREATED, DATE_MODIFIED) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
		}
		return "INSERT INTO " + tableName + " (GROUP_ID, GROUP_TYPE, PROJECT_ID, ENGINE_ID, USAGE_RESTRICTION, "
				+ "USAGE_FREQUENCY, MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAX_RESPONSE_TIME, "
				+ "RESTRICT_PER_MODEL, IS_ACTIVE, CREATED_BY, CREATED_BY_TYPE, DATE_CREATED, DATE_MODIFIED) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
	}

	private static boolean hasLimit(String tableName, String userId, String groupId, String groupType, String engineId,
			String projectId, String scopedEngineId, String usageFrequency) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT 1 FROM " + tableName + " WHERE " + getKeyWhereClause(tableName);
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			bindKey(ps, 1, tableName, userId, groupId, groupType, engineId, projectId, scopedEngineId, usageFrequency);
			rs = ps.executeQuery();
			return rs.next();
		} catch (Exception e) {
			classLogger.error("Error checking token limit existence in {}", tableName, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
	}

	private static void removeLimit(String tableName, String whereClause, String... params) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "DELETE FROM " + tableName + " WHERE " + whereClause;
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for (int i = 0; i < params.length; i++) {
				ps.setString(i + 1, params[i]);
			}
			ps.execute();
			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error removing token limit from {}", tableName, e);
			throw new IllegalArgumentException("Failed to remove token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	private static List<Map<String, Object>> getLimits(String query, String... params) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> results = new ArrayList<>();
		try {
			ps = securityDb.getPreparedStatement(query);
			for (int i = 0; i < params.length; i++) {
				ps.setString(i + 1, params[i]);
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				results.add(buildResultMap(rs));
			}
		} catch (Exception e) {
			classLogger.error("Error getting token limits", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return results;
	}

	private static Map<String, Object> buildResultMap(ResultSet rs) throws SQLException {
		Map<String, Object> result = new HashMap<>();
		putIfColumnExists(rs, result, "USER_ID", "userId");
		putIfColumnExists(rs, result, "GROUP_ID", "groupId");
		putIfColumnExists(rs, result, "GROUP_TYPE", "groupType");
		putIfColumnExists(rs, result, "ENGINE_ID", "engineId");
		putIfColumnExists(rs, result, "PROJECT_ID", "projectId");
		result.put("usageRestriction", rs.getString("USAGE_RESTRICTION"));
		result.put("usageFrequency", rs.getString("USAGE_FREQUENCY"));
		result.put("maxTokens", rs.getObject("MAX_TOKENS"));
		result.put("maxInputTokens", rs.getObject("MAX_INPUT_TOKENS"));
		result.put("maxOutputTokens", rs.getObject("MAX_OUTPUT_TOKENS"));
		result.put("maxResponseTime", rs.getObject("MAX_RESPONSE_TIME"));
		putIfColumnExists(rs, result, "RESTRICT_PER_MODEL", "restrictPerModel");
		result.put("isActive", rs.getObject("IS_ACTIVE"));
		result.put("createdBy", rs.getString("CREATED_BY"));
		result.put("createdByType", rs.getString("CREATED_BY_TYPE"));
		result.put("dateCreated", rs.getObject("DATE_CREATED"));
		result.put("dateModified", rs.getObject("DATE_MODIFIED"));
		return result;
	}

	private static void putIfColumnExists(ResultSet rs, Map<String, Object> result, String column, String key)
			throws SQLException {
		try {
			Object value = rs.getObject(column);
			result.put(key, value);
		} catch (SQLException e) {
			if (!isMissingColumnException(e)) {
				throw e;
			}
		}
	}

	private static boolean isMissingColumnException(SQLException e) {
		String message = e.getMessage();
		return message != null && message.toLowerCase().contains("column");
	}

	private static int bindInsertKey(PreparedStatement ps, int idx, String tableName, String userId, String groupId,
			String groupType, String engineId, String projectId, String scopedEngineId) throws SQLException {
		if (ENGINE_USER_TABLE.equals(tableName)) {
			ps.setString(idx++, userId);
			ps.setString(idx++, engineId);
		} else if (ENGINE_TEAM_TABLE.equals(tableName)) {
			ps.setString(idx++, groupId);
			ps.setString(idx++, groupType);
			ps.setString(idx++, engineId);
		} else if (PROJECT_USER_TABLE.equals(tableName)) {
			ps.setString(idx++, userId);
			ps.setString(idx++, projectId);
			ps.setString(idx++, scopedEngineId);
		} else {
			ps.setString(idx++, groupId);
			ps.setString(idx++, groupType);
			ps.setString(idx++, projectId);
			ps.setString(idx++, scopedEngineId);
		}
		return idx;
	}

	private static int bindKey(PreparedStatement ps, int idx, String tableName, String userId, String groupId,
			String groupType, String engineId, String projectId, String scopedEngineId, String usageFrequency)
			throws SQLException {
		if (ENGINE_USER_TABLE.equals(tableName)) {
			ps.setString(idx++, userId);
			ps.setString(idx++, engineId);
		} else if (ENGINE_TEAM_TABLE.equals(tableName)) {
			ps.setString(idx++, groupId);
			ps.setString(idx++, groupType);
			ps.setString(idx++, engineId);
		} else if (PROJECT_USER_TABLE.equals(tableName)) {
			ps.setString(idx++, userId);
			ps.setString(idx++, projectId);
			ps.setString(idx++, scopedEngineId);
		} else {
			ps.setString(idx++, groupId);
			ps.setString(idx++, groupType);
			ps.setString(idx++, projectId);
			ps.setString(idx++, scopedEngineId);
		}
		ps.setString(idx++, usageFrequency);
		return idx;
	}

	private static String getKeyWhereClause(String tableName) {
		if (ENGINE_USER_TABLE.equals(tableName)) {
			return "USER_ID=? AND ENGINE_ID=? AND USAGE_FREQUENCY=?";
		} else if (ENGINE_TEAM_TABLE.equals(tableName)) {
			return "GROUP_ID=? AND GROUP_TYPE=? AND ENGINE_ID=? AND USAGE_FREQUENCY=?";
		} else if (PROJECT_USER_TABLE.equals(tableName)) {
			return "USER_ID=? AND PROJECT_ID=? AND ENGINE_ID=? AND USAGE_FREQUENCY=?";
		}
		return "GROUP_ID=? AND GROUP_TYPE=? AND PROJECT_ID=? AND ENGINE_ID=? AND USAGE_FREQUENCY=?";
	}

	private static boolean isProjectTable(String tableName) {
		return PROJECT_USER_TABLE.equals(tableName) || PROJECT_TEAM_TABLE.equals(tableName);
	}

	private static String normalizeScopedEngineId(String engineId) {
		return isBlank(engineId) ? ALL_ENGINES_SENTINEL : engineId.trim();
	}

	public static String validateFrequency(String frequency) {
		if (isBlank(frequency)) {
			throw new IllegalArgumentException("Must provide a valid usageFrequency");
		}
		String normalized = frequency.trim().toUpperCase();
		if ("HOUR".equals(normalized) || "DAY".equals(normalized) || "WEEK".equals(normalized)
				|| "MONTH".equals(normalized) || "YEAR".equals(normalized) || "ALL_TIME".equals(normalized)) {
			return normalized;
		}
		throw new IllegalArgumentException("Invalid usageFrequency: " + frequency);
	}

	private static String resolveUsageRestriction(long maxTokens, long maxInputTokens, long maxOutputTokens,
			Double maxResponseTime) {
		if (maxTokens >= 0 || maxInputTokens >= 0 || maxOutputTokens >= 0) {
			return Constants.MODEL_TOKEN_RESTRICTION_VALUE;
		}
		if (maxResponseTime != null && maxResponseTime.doubleValue() >= 0) {
			return Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE;
		}
		return null;
	}

	private static void bindString(PreparedStatement ps, int index, String value) throws SQLException {
		if (isBlank(value)) {
			ps.setNull(index, java.sql.Types.VARCHAR);
		} else {
			ps.setString(index, value);
		}
	}

	private static void bindLong(PreparedStatement ps, int index, long value) throws SQLException {
		if (value < 0) {
			ps.setNull(index, java.sql.Types.BIGINT);
		} else {
			ps.setLong(index, value);
		}
	}

	private static void bindDouble(PreparedStatement ps, int index, Double value) throws SQLException {
		if (value == null || value.doubleValue() < 0) {
			ps.setNull(index, java.sql.Types.DOUBLE);
		} else {
			ps.setDouble(index, value.doubleValue());
		}
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}
}
