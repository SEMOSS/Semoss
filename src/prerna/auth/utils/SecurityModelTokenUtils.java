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
 * Stores platform-wide model usage limits by model and frequency.
 */
public class SecurityModelTokenUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityModelTokenUtils.class);

	private static final String TABLE_NAME = "MODELTOKENLIMIT";

	private SecurityModelTokenUtils() {
		// utility class
	}

	public static List<Map<String, Object>> getModelTokenLimits(String engineId) {
		if (engineId == null || engineId.trim().isEmpty()) {
			return new ArrayList<>();
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT ENGINE_ID, USAGE_FREQUENCY, MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, "
				+ "MAX_RESPONSE_TIME, IS_ACTIVE, CREATED_BY, DATE_CREATED, DATE_MODIFIED "
				+ "FROM " + TABLE_NAME + " WHERE ENGINE_ID=? ORDER BY USAGE_FREQUENCY";
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> results = new ArrayList<>();
		try {
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, engineId);
			rs = ps.executeQuery();
			while (rs.next()) {
				results.add(buildResultMap(rs));
			}
		} catch (Exception e) {
			classLogger.error("Error getting model token limits for engine {}", engineId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return results;
	}

	public static void setModelTokenLimit(String engineId, String usageFrequency, long maxTokens, long maxInputTokens,
			long maxOutputTokens, double maxResponseTime, boolean isActive, String createdBy) {
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid engineId");
		}
		if (usageFrequency == null || usageFrequency.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid usageFrequency");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean exists = hasModelTokenLimit(engineId, usageFrequency);
		PreparedStatement ps = null;
		try {
			if (exists) {
				String updateSql = "UPDATE " + TABLE_NAME
						+ " SET MAX_TOKENS=?, MAX_INPUT_TOKENS=?, MAX_OUTPUT_TOKENS=?, MAX_RESPONSE_TIME=?, "
						+ "IS_ACTIVE=?, CREATED_BY=?, DATE_MODIFIED=CURRENT_TIMESTAMP "
						+ "WHERE ENGINE_ID=? AND USAGE_FREQUENCY=?";
				ps = securityDb.getPreparedStatement(updateSql);
				int idx = 1;
				bindLong(ps, idx++, maxTokens);
				bindLong(ps, idx++, maxInputTokens);
				bindLong(ps, idx++, maxOutputTokens);
				bindDouble(ps, idx++, maxResponseTime);
				ps.setBoolean(idx++, isActive);
				bindString(ps, idx++, createdBy);
				ps.setString(idx++, engineId);
				ps.setString(idx++, usageFrequency);
				ps.execute();
			} else {
				String insertSql = "INSERT INTO " + TABLE_NAME
						+ " (ENGINE_ID, USAGE_FREQUENCY, MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, "
						+ "MAX_RESPONSE_TIME, IS_ACTIVE, CREATED_BY, DATE_CREATED, DATE_MODIFIED) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
				ps = securityDb.getPreparedStatement(insertSql);
				int idx = 1;
				ps.setString(idx++, engineId);
				ps.setString(idx++, usageFrequency);
				bindLong(ps, idx++, maxTokens);
				bindLong(ps, idx++, maxInputTokens);
				bindLong(ps, idx++, maxOutputTokens);
				bindDouble(ps, idx++, maxResponseTime);
				ps.setBoolean(idx++, isActive);
				bindString(ps, idx++, createdBy);
				ps.execute();
			}

			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error setting model token limit for engine {} and frequency {}", engineId,
					usageFrequency, e);
			throw new IllegalArgumentException("Failed to set model token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	public static void removeModelTokenLimit(String engineId, String usageFrequency) {
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid engineId");
		}
		if (usageFrequency == null || usageFrequency.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid usageFrequency");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteSql = "DELETE FROM " + TABLE_NAME + " WHERE ENGINE_ID=? AND USAGE_FREQUENCY=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteSql);
			ps.setString(1, engineId);
			ps.setString(2, usageFrequency);
			ps.execute();
			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error removing model token limit for engine {} and frequency {}", engineId,
					usageFrequency, e);
			throw new IllegalArgumentException("Failed to remove model token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	private static boolean hasModelTokenLimit(String engineId, String usageFrequency) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT 1 FROM " + TABLE_NAME + " WHERE ENGINE_ID=? AND USAGE_FREQUENCY=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, engineId);
			ps.setString(2, usageFrequency);
			rs = ps.executeQuery();
			return rs.next();
		} catch (Exception e) {
			classLogger.error("Error checking model token limit for engine {} and frequency {}", engineId,
					usageFrequency, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
	}

	private static Map<String, Object> buildResultMap(ResultSet rs) throws java.sql.SQLException {
		Map<String, Object> result = new HashMap<>();
		result.put("engineId", rs.getString("ENGINE_ID"));
		result.put("usageFrequency", rs.getString("USAGE_FREQUENCY"));
		result.put("maxTokens", rs.getObject("MAX_TOKENS"));
		result.put("maxInputTokens", rs.getObject("MAX_INPUT_TOKENS"));
		result.put("maxOutputTokens", rs.getObject("MAX_OUTPUT_TOKENS"));
		result.put("maxResponseTime", rs.getObject("MAX_RESPONSE_TIME"));
		result.put("isActive", rs.getObject("IS_ACTIVE"));
		result.put("createdBy", rs.getString("CREATED_BY"));
		result.put("dateCreated", rs.getObject("DATE_CREATED"));
		result.put("dateModified", rs.getObject("DATE_MODIFIED"));
		return result;
	}

	private static void bindString(PreparedStatement ps, int index, String value) throws java.sql.SQLException {
		if (value == null || value.trim().isEmpty()) {
			ps.setNull(index, java.sql.Types.VARCHAR);
		} else {
			ps.setString(index, value);
		}
	}

	private static void bindLong(PreparedStatement ps, int index, long value) throws java.sql.SQLException {
		if (value < 0) {
			ps.setNull(index, java.sql.Types.BIGINT);
		} else {
			ps.setLong(index, value);
		}
	}

	private static void bindDouble(PreparedStatement ps, int index, double value) throws java.sql.SQLException {
		if (value < 0) {
			ps.setNull(index, java.sql.Types.DOUBLE);
		} else {
			ps.setDouble(index, value);
		}
	}
}
