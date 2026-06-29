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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;

/**
 * Utility class for managing platform-wide user query rate limits with optional
 * per-user overrides.
 */
public class SecurityQueryRateLimitUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityQueryRateLimitUtils.class);
	private static final String TABLE_NAME = "QUERYRATELIMIT";

	private SecurityQueryRateLimitUtils() {
		// utility class
	}

	public static List<Map<String, Object>> getEffectiveQueryRateLimits(String userId) {
		List<Map<String, Object>> userLimits = getUserQueryRateLimits(userId);
		Set<String> activeUserFrequencies = new HashSet<>();
		List<Map<String, Object>> effectiveLimits = new ArrayList<>();

		for (Map<String, Object> userLimit : userLimits) {
			if (!isActive(userLimit.get("isActive")) || !hasRequestLimit(userLimit)) {
				continue;
			}
			String frequency = (String) userLimit.get("usageFrequency");
			if (frequency != null && !frequency.trim().isEmpty()) {
				activeUserFrequencies.add(frequency.toUpperCase());
				effectiveLimits.add(userLimit);
			}
		}

		for (Map<String, Object> defaultLimit : getDefaultQueryRateLimits()) {
			String frequency = (String) defaultLimit.get("usageFrequency");
			if (frequency == null || activeUserFrequencies.contains(frequency.toUpperCase())) {
				continue;
			}
			if (isActive(defaultLimit.get("isActive")) && hasRequestLimit(defaultLimit)) {
				effectiveLimits.add(defaultLimit);
			}
		}

		return effectiveLimits;
	}

	public static List<Map<String, Object>> getDefaultQueryRateLimits() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT USERID, USAGE_FREQUENCY, MAX_REQUESTS, IS_ACTIVE, CREATED_BY, DATE_CREATED, DATE_MODIFIED "
				+ "FROM " + TABLE_NAME + " WHERE USERID IS NULL ORDER BY USAGE_FREQUENCY";
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> results = new ArrayList<>();
		try {
			ps = securityDb.getPreparedStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				results.add(buildResultMap(rs));
			}
		} catch (Exception e) {
			classLogger.error("Error getting default query rate limits", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return results;
	}

	public static List<Map<String, Object>> getUserQueryRateLimits(String userId) {
		List<Map<String, Object>> results = new ArrayList<>();
		if (userId == null || userId.trim().isEmpty()) {
			return results;
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT USERID, USAGE_FREQUENCY, MAX_REQUESTS, IS_ACTIVE, CREATED_BY, DATE_CREATED, DATE_MODIFIED "
				+ "FROM " + TABLE_NAME + " WHERE USERID = ? ORDER BY USAGE_FREQUENCY";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, userId);
			rs = ps.executeQuery();
			while (rs.next()) {
				results.add(buildResultMap(rs));
			}
		} catch (Exception e) {
			classLogger.error("Error getting query rate limits for user " + userId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return results;
	}

	public static void setDefaultQueryRateLimit(String usageFrequency, long maxRequests, boolean isActive,
			String createdBy) {
		setQueryRateLimit(null, usageFrequency, maxRequests, isActive, createdBy);
	}

	public static void setUserQueryRateLimit(String userId, String usageFrequency, long maxRequests, boolean isActive,
			String createdBy) {
		if (userId == null || userId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a userId for user-specific query limits");
		}
		setQueryRateLimit(userId, usageFrequency, maxRequests, isActive, createdBy);
	}

	public static void removeDefaultQueryRateLimit(String usageFrequency) {
		removeQueryRateLimit(null, usageFrequency);
	}

	public static void removeUserQueryRateLimit(String userId, String usageFrequency) {
		if (userId == null || userId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a userId to remove");
		}
		removeQueryRateLimit(userId, usageFrequency);
	}

	public static List<Map<String, Object>> getAllQueryRateLimits() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT q.USERID, q.USAGE_FREQUENCY, q.MAX_REQUESTS, q.IS_ACTIVE, q.CREATED_BY, "
				+ "q.DATE_CREATED, q.DATE_MODIFIED, u.NAME AS USER_NAME, u.EMAIL AS USER_EMAIL "
				+ "FROM " + TABLE_NAME + " q LEFT JOIN SMSS_USER u ON q.USERID = u.ID "
				+ "ORDER BY q.USERID IS NOT NULL, q.USERID, q.USAGE_FREQUENCY";
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> results = new ArrayList<>();
		try {
			ps = securityDb.getPreparedStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = buildResultMap(rs);
				row.put("userName", rs.getString("USER_NAME"));
				row.put("userEmail", rs.getString("USER_EMAIL"));
				results.add(row);
			}
		} catch (Exception e) {
			classLogger.error("Error listing all query rate limits", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return results;
	}

	private static void setQueryRateLimit(String userId, String usageFrequency, long maxRequests, boolean isActive,
			String createdBy) {
		if (usageFrequency == null || usageFrequency.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a usageFrequency");
		}

		String normalizedFrequency = usageFrequency.trim().toUpperCase();
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean exists = hasQueryRateLimit(userId, normalizedFrequency);
		PreparedStatement ps = null;
		try {
			if (exists) {
				String updateSql = "UPDATE " + TABLE_NAME
						+ " SET MAX_REQUESTS=?, IS_ACTIVE=?, DATE_MODIFIED=CURRENT_TIMESTAMP WHERE ";
				updateSql += userId == null ? "USERID IS NULL AND USAGE_FREQUENCY=?"
						: "USERID=? AND USAGE_FREQUENCY=?";
				ps = securityDb.getPreparedStatement(updateSql);
				int idx = 1;
				ps.setLong(idx++, maxRequests);
				ps.setBoolean(idx++, isActive);
				if (userId != null) {
					ps.setString(idx++, userId);
				}
				ps.setString(idx++, normalizedFrequency);
				ps.execute();
			} else {
				String insertSql = "INSERT INTO " + TABLE_NAME
						+ " (USERID, USAGE_FREQUENCY, MAX_REQUESTS, IS_ACTIVE, CREATED_BY, DATE_CREATED, DATE_MODIFIED) "
						+ "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
				ps = securityDb.getPreparedStatement(insertSql);
				int idx = 1;
				if (userId == null) {
					ps.setNull(idx++, Types.VARCHAR);
				} else {
					ps.setString(idx++, userId);
				}
				ps.setString(idx++, normalizedFrequency);
				ps.setLong(idx++, maxRequests);
				ps.setBoolean(idx++, isActive);
				ps.setString(idx++, createdBy);
				ps.execute();
			}

			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error setting query rate limit", e);
			throw new IllegalArgumentException("Failed to set query rate limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	private static void removeQueryRateLimit(String userId, String usageFrequency) {
		if (usageFrequency == null || usageFrequency.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a usageFrequency");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteSql = "DELETE FROM " + TABLE_NAME + " WHERE ";
		deleteSql += userId == null ? "USERID IS NULL AND USAGE_FREQUENCY=?" : "USERID=? AND USAGE_FREQUENCY=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteSql);
			int idx = 1;
			if (userId != null) {
				ps.setString(idx++, userId);
			}
			ps.setString(idx++, usageFrequency.trim().toUpperCase());
			ps.execute();
			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error removing query rate limit", e);
			throw new IllegalArgumentException("Failed to remove query rate limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	private static boolean hasQueryRateLimit(String userId, String usageFrequency) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE ";
		query += userId == null ? "USERID IS NULL AND USAGE_FREQUENCY=?" : "USERID=? AND USAGE_FREQUENCY=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int idx = 1;
			if (userId != null) {
				ps.setString(idx++, userId);
			}
			ps.setString(idx++, usageFrequency);
			rs = ps.executeQuery();
			if (rs.next()) {
				return rs.getInt(1) > 0;
			}
		} catch (Exception e) {
			classLogger.error("Error checking query rate limit existence", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return false;
	}

	private static boolean hasRequestLimit(Map<String, Object> limit) {
		Number maxRequests = (Number) limit.get("maxRequests");
		return maxRequests != null && maxRequests.longValue() >= 0;
	}

	private static boolean isActive(Object isActiveObj) {
		return isActiveObj == null || Boolean.TRUE.equals(isActiveObj);
	}

	private static Map<String, Object> buildResultMap(ResultSet rs) throws java.sql.SQLException {
		Map<String, Object> result = new HashMap<>();
		result.put("userId", rs.getString("USERID"));
		result.put("usageFrequency", rs.getString("USAGE_FREQUENCY"));
		result.put("maxRequests", rs.getObject("MAX_REQUESTS"));
		result.put("isActive", rs.getObject("IS_ACTIVE"));
		result.put("createdBy", rs.getString("CREATED_BY"));
		result.put("dateCreated", rs.getObject("DATE_CREATED"));
		result.put("dateModified", rs.getObject("DATE_MODIFIED"));
		return result;
	}
}
