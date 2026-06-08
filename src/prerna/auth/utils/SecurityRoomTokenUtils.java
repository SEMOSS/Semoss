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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;

/**
 * Utility class for managing the platform-wide room token policy stored in the
 * security database, with optional per-user overrides.
 */
public class SecurityRoomTokenUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityRoomTokenUtils.class);

	private SecurityRoomTokenUtils() {
		// utility class
	}

	/**
	 * Get the effective room token policy for a user.
	 * Returns the active user-specific override when present, otherwise the
	 * platform default.
	 */
	public static Map<String, Object> getEffectiveRoomTokenLimit(String userId) {
		Map<String, Object> userLimit = getRoomTokenLimitForUser(userId);
		if (userLimit != null) {
			Object isActive = userLimit.get("isActive");
			if (isActive == null || Boolean.TRUE.equals(isActive)) {
				return userLimit;
			}
			// User override is inactive — fall through to default
		}
		return getDefaultRoomTokenLimit();
	}

	public static Map<String, Object> getDefaultRoomTokenLimit() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, IS_ACTIVE, CREATED_BY, DATE_CREATED, DATE_MODIFIED "
				+ "FROM ROOMTOKENLIMIT WHERE USERID IS NULL";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			rs = ps.executeQuery();
			if (rs.next()) {
				return buildResultMap(rs, null);
			}
		} catch (Exception e) {
			classLogger.error("Error getting default room token limit", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return null;
	}

	public static Map<String, Object> getRoomTokenLimitForUser(String userId) {
		if (userId == null || userId.trim().isEmpty()) {
			return null;
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, IS_ACTIVE, CREATED_BY, DATE_CREATED, DATE_MODIFIED "
				+ "FROM ROOMTOKENLIMIT WHERE USERID = ?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, userId);
			rs = ps.executeQuery();
			if (rs.next()) {
				return buildResultMap(rs, userId);
			}
		} catch (Exception e) {
			classLogger.error("Error getting room token limit for user " + userId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return null;
	}

	public static void setDefaultRoomTokenLimit(long maxTokens, long maxInputTokens,
			long maxOutputTokens, boolean isActive, String createdBy) {
		setRoomTokenLimit(null, maxTokens, maxInputTokens, maxOutputTokens, isActive, createdBy);
	}

	public static void setUserRoomTokenLimit(String userId, long maxTokens, long maxInputTokens,
			long maxOutputTokens, boolean isActive, String createdBy) {
		if (userId == null || userId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a userId for user-specific limits");
		}
		setRoomTokenLimit(userId, maxTokens, maxInputTokens, maxOutputTokens, isActive, createdBy);
	}

	private static void setRoomTokenLimit(String userId, long maxTokens, long maxInputTokens,
			long maxOutputTokens, boolean isActive, String createdBy) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		boolean exists;
		if (userId == null) {
			exists = getDefaultRoomTokenLimit() != null;
		} else {
			exists = getRoomTokenLimitForUser(userId) != null;
		}

		PreparedStatement ps = null;
		try {
			if (exists) {
				String updateSql;
				if (userId == null) {
					updateSql = "UPDATE ROOMTOKENLIMIT SET MAX_TOKENS=?, MAX_INPUT_TOKENS=?, MAX_OUTPUT_TOKENS=?, "
							+ "IS_ACTIVE=?, DATE_MODIFIED=CURRENT_TIMESTAMP WHERE USERID IS NULL";
				} else {
					updateSql = "UPDATE ROOMTOKENLIMIT SET MAX_TOKENS=?, MAX_INPUT_TOKENS=?, MAX_OUTPUT_TOKENS=?, "
							+ "IS_ACTIVE=?, DATE_MODIFIED=CURRENT_TIMESTAMP WHERE USERID=?";
				}
				ps = securityDb.getPreparedStatement(updateSql);
				int idx = 1;
				ps.setLong(idx++, maxTokens);
				ps.setLong(idx++, maxInputTokens);
				ps.setLong(idx++, maxOutputTokens);
				ps.setBoolean(idx++, isActive);
				if (userId != null) {
					ps.setString(idx++, userId);
				}
				ps.execute();
			} else {
				String insertSql = "INSERT INTO ROOMTOKENLIMIT (USERID, MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, "
						+ "IS_ACTIVE, CREATED_BY, DATE_CREATED, DATE_MODIFIED) "
						+ "VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
				ps = securityDb.getPreparedStatement(insertSql);
				int idx = 1;
				if (userId == null) {
					ps.setNull(idx++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(idx++, userId);
				}
				ps.setLong(idx++, maxTokens);
				ps.setLong(idx++, maxInputTokens);
				ps.setLong(idx++, maxOutputTokens);
				ps.setBoolean(idx++, isActive);
				ps.setString(idx++, createdBy);
				ps.execute();
			}

			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error setting room token limit", e);
			throw new IllegalArgumentException("Failed to set room token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	public static void removeUserRoomTokenLimit(String userId) {
		if (userId == null || userId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a userId to remove");
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteSql = "DELETE FROM ROOMTOKENLIMIT WHERE USERID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteSql);
			ps.setString(1, userId);
			ps.execute();
			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error removing room token limit for user " + userId, e);
			throw new IllegalArgumentException("Failed to remove room token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	public static List<Map<String, Object>> getAllRoomTokenLimits() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT r.USERID, r.MAX_TOKENS, r.MAX_INPUT_TOKENS, r.MAX_OUTPUT_TOKENS, "
				+ "r.IS_ACTIVE, r.CREATED_BY, r.DATE_CREATED, r.DATE_MODIFIED, "
				+ "u.NAME AS USER_NAME, u.EMAIL AS USER_EMAIL "
				+ "FROM ROOMTOKENLIMIT r LEFT JOIN SMSS_USER u ON r.USERID = u.ID "
				+ "ORDER BY r.USERID IS NOT NULL, r.DATE_CREATED";
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> results = new ArrayList<>();
		try {
			ps = securityDb.getPreparedStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("userId", rs.getString("USERID"));
				row.put("maxTokens", rs.getObject("MAX_TOKENS"));
				row.put("maxInputTokens", rs.getObject("MAX_INPUT_TOKENS"));
				row.put("maxOutputTokens", rs.getObject("MAX_OUTPUT_TOKENS"));
				row.put("isActive", rs.getObject("IS_ACTIVE"));
				row.put("createdBy", rs.getString("CREATED_BY"));
				row.put("dateCreated", rs.getObject("DATE_CREATED"));
				row.put("dateModified", rs.getObject("DATE_MODIFIED"));
				row.put("userName", rs.getString("USER_NAME"));
				row.put("userEmail", rs.getString("USER_EMAIL"));
				results.add(row);
			}
		} catch (Exception e) {
			classLogger.error("Error listing all room token limits", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return results;
	}

	private static Map<String, Object> buildResultMap(ResultSet rs, String userId) throws java.sql.SQLException {
		Map<String, Object> result = new HashMap<>();
		result.put("userId", userId);
		result.put("maxTokens", rs.getObject("MAX_TOKENS"));
		result.put("maxInputTokens", rs.getObject("MAX_INPUT_TOKENS"));
		result.put("maxOutputTokens", rs.getObject("MAX_OUTPUT_TOKENS"));
		result.put("isActive", rs.getObject("IS_ACTIVE"));
		result.put("createdBy", rs.getString("CREATED_BY"));
		result.put("dateCreated", rs.getObject("DATE_CREATED"));
		result.put("dateModified", rs.getObject("DATE_MODIFIED"));
		return result;
	}
}
