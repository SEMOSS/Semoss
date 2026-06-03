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
 * Utility class for model-level platform token limits.
 * Limits are scoped by model engine and usage frequency.
 */
public class SecurityModelTokenUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityModelTokenUtils.class);

	private SecurityModelTokenUtils() {
		// utility class
	}

	public static List<Map<String, Object>> getModelTokenLimits(String engineId) {
		List<Map<String, Object>> results = new ArrayList<>();
		if (engineId == null || engineId.trim().isEmpty()) {
			return results;
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT ENGINEID, USAGE_FREQUENCY, MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, "
				+ "MAX_RESPONSE_TIME, IS_ACTIVE, CREATED_BY, DATE_CREATED, DATE_MODIFIED "
				+ "FROM MODELTOKENLIMIT WHERE ENGINEID = ? ORDER BY USAGE_FREQUENCY";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, engineId);
			rs = ps.executeQuery();
			while (rs.next()) {
				results.add(buildResultMap(rs));
			}
		} catch (Exception e) {
			classLogger.error("Error getting model token limits for engineId {}", engineId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return results;
	}

	public static void setModelTokenLimit(String engineId, String usageFrequency, long maxTokens, long maxInputTokens,
			long maxOutputTokens, double maxResponseTime, boolean isActive, String createdBy) {
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide an engineId");
		}
		if (usageFrequency == null || usageFrequency.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a usageFrequency");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			boolean exists = getModelTokenLimit(engineId, usageFrequency) != null;
			if (exists) {
				String updateSql = "UPDATE MODELTOKENLIMIT SET MAX_TOKENS=?, MAX_INPUT_TOKENS=?, MAX_OUTPUT_TOKENS=?, "
						+ "MAX_RESPONSE_TIME=?, IS_ACTIVE=?, DATE_MODIFIED=CURRENT_TIMESTAMP "
						+ "WHERE ENGINEID=? AND USAGE_FREQUENCY=?";
				ps = securityDb.getPreparedStatement(updateSql);
				int idx = 1;
				ps.setLong(idx++, maxTokens);
				ps.setLong(idx++, maxInputTokens);
				ps.setLong(idx++, maxOutputTokens);
				ps.setDouble(idx++, maxResponseTime);
				ps.setBoolean(idx++, isActive);
				ps.setString(idx++, engineId);
				ps.setString(idx++, usageFrequency);
				ps.execute();
			} else {
				String insertSql = "INSERT INTO MODELTOKENLIMIT (ENGINEID, USAGE_FREQUENCY, MAX_TOKENS, "
						+ "MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAX_RESPONSE_TIME, IS_ACTIVE, CREATED_BY, DATE_CREATED, DATE_MODIFIED) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
				ps = securityDb.getPreparedStatement(insertSql);
				int idx = 1;
				ps.setString(idx++, engineId);
				ps.setString(idx++, usageFrequency);
				ps.setLong(idx++, maxTokens);
				ps.setLong(idx++, maxInputTokens);
				ps.setLong(idx++, maxOutputTokens);
				ps.setDouble(idx++, maxResponseTime);
				ps.setBoolean(idx++, isActive);
				ps.setString(idx++, createdBy);
				ps.execute();
			}

			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error setting model token limit", e);
			throw new IllegalArgumentException("Failed to set model token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	public static void removeModelTokenLimit(String engineId, String usageFrequency) {
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide an engineId");
		}
		if (usageFrequency == null || usageFrequency.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a usageFrequency");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteSql = "DELETE FROM MODELTOKENLIMIT WHERE ENGINEID = ? AND USAGE_FREQUENCY = ?";
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
			classLogger.error("Error removing model token limit", e);
			throw new IllegalArgumentException("Failed to remove model token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	private static Map<String, Object> getModelTokenLimit(String engineId, String usageFrequency) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT ENGINEID, USAGE_FREQUENCY, MAX_TOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, "
				+ "MAX_RESPONSE_TIME, IS_ACTIVE, CREATED_BY, DATE_CREATED, DATE_MODIFIED "
				+ "FROM MODELTOKENLIMIT WHERE ENGINEID = ? AND USAGE_FREQUENCY = ?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, engineId);
			ps.setString(2, usageFrequency);
			rs = ps.executeQuery();
			if (rs.next()) {
				return buildResultMap(rs);
			}
		} catch (Exception e) {
			classLogger.error("Error checking model token limit", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return null;
	}

	private static Map<String, Object> buildResultMap(ResultSet rs) throws java.sql.SQLException {
		Map<String, Object> result = new HashMap<>();
		result.put("engineId", rs.getString("ENGINEID"));
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
}
