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
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;

/**
 * Persists default token limits for entity usage-limit pages.
 * Separate rows are stored for default user limits and default team limits.
 */
public class SecurityEntityDefaultTokenUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityEntityDefaultTokenUtils.class);

	private static final String USER_DEFAULT_TABLE = "ENTITYDEFAULTTOKENLIMIT";
	private static final String TEAM_DEFAULT_TABLE = "ENTITYDEFAULTTEAMTOKENLIMIT";
	private static final String ENGINE_ENTITY_TYPE = "ENGINE";
	private static final String PROJECT_ENTITY_TYPE = "PROJECT";

	private SecurityEntityDefaultTokenUtils() {
		// utility class
	}

	public static List<Map<String, Object>> getEngineDefaultTokenLimits(String engineId) {
		return getDefaultTokenLimits(USER_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId);
	}

	public static List<Map<String, Object>> getProjectDefaultTokenLimits(String projectId) {
		return getDefaultTokenLimits(USER_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId);
	}

	public static List<Map<String, Object>> getEngineDefaultTeamTokenLimits(String engineId) {
		return getDefaultTokenLimits(TEAM_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId);
	}

	public static List<Map<String, Object>> getProjectDefaultTeamTokenLimits(String projectId) {
		return getDefaultTokenLimits(TEAM_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId);
	}

	public static void setEngineDefaultTokenLimit(String engineId, String usageFrequency, long maxTokens,
			long maxInputTokens, long maxOutputTokens, boolean isActive, String createdBy, String createdByType,
			String existingUsageFrequency) {
		setDefaultTokenLimit(USER_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId, usageFrequency, existingUsageFrequency, maxTokens,
				maxInputTokens, maxOutputTokens, null, isActive, createdBy, createdByType, false);
	}

	public static void setProjectDefaultTokenLimit(String projectId, String usageFrequency, long maxTokens,
			long maxInputTokens, long maxOutputTokens, boolean isActive, String createdBy, String createdByType,
			boolean restrictPerModel, String existingUsageFrequency) {
		setDefaultTokenLimit(USER_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId, usageFrequency, existingUsageFrequency, maxTokens,
				maxInputTokens, maxOutputTokens, null, isActive, createdBy, createdByType, restrictPerModel);
	}

	public static void setEngineDefaultTeamTokenLimit(String engineId, String usageFrequency, long maxTokens,
			long maxInputTokens, long maxOutputTokens, boolean isActive, String createdBy, String createdByType,
			String existingUsageFrequency) {
		setDefaultTokenLimit(TEAM_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId, usageFrequency, existingUsageFrequency, maxTokens,
				maxInputTokens, maxOutputTokens, null, isActive, createdBy, createdByType, false);
	}

	public static void setProjectDefaultTeamTokenLimit(String projectId, String usageFrequency, long maxTokens,
			long maxInputTokens, long maxOutputTokens, boolean isActive, String createdBy, String createdByType,
			String existingUsageFrequency) {
		setDefaultTokenLimit(TEAM_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId, usageFrequency, existingUsageFrequency, maxTokens,
				maxInputTokens, maxOutputTokens, null, isActive, createdBy, createdByType, false);
	}

	public static void removeEngineDefaultTokenLimit(String engineId, String usageFrequency) {
		removeDefaultTokenLimit(USER_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId, usageFrequency);
	}

	public static void removeProjectDefaultTokenLimit(String projectId, String usageFrequency) {
		removeDefaultTokenLimit(USER_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId, usageFrequency);
	}

	public static void removeEngineDefaultTeamTokenLimit(String engineId, String usageFrequency) {
		removeDefaultTokenLimit(TEAM_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId, usageFrequency);
	}

	public static void removeProjectDefaultTeamTokenLimit(String projectId, String usageFrequency) {
		removeDefaultTokenLimit(TEAM_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId, usageFrequency);
	}

	private static List<Map<String, Object>> getDefaultTokenLimits(String tableName, String entityType, String entityId) {
		if (entityId == null || entityId.trim().isEmpty()) {
			return new ArrayList<>();
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT ENTITY_TYPE, ENTITY_ID, USAGE_RESTRICTION, USAGE_FREQUENCY, MAX_TOKENS, "
				+ "MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAX_RESPONSE_TIME, IS_ACTIVE, CREATED_BY, CREATED_BY_TYPE, "
				+ "DATE_CREATED, DATE_MODIFIED, RESTRICT_PER_MODEL FROM " + tableName
				+ " WHERE ENTITY_TYPE=? AND ENTITY_ID=? ORDER BY USAGE_FREQUENCY";
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> results = new ArrayList<>();
		try {
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, entityType);
			ps.setString(2, entityId);
			rs = ps.executeQuery();
			while (rs.next()) {
				results.add(buildResultMap(rs));
			}
		} catch (Exception e) {
			classLogger.error("Error getting default token limit from {} for entity {}:{}", tableName, entityType,
					entityId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return results;
	}

	private static void setDefaultTokenLimit(String tableName, String entityType, String entityId, String usageFrequency,
			String existingUsageFrequency, long maxTokens, long maxInputTokens, long maxOutputTokens,
			Double maxResponseTime, boolean isActive, String createdBy, String createdByType, boolean restrictPerModel) {
		if (entityId == null || entityId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid entityId");
		}
		if (usageFrequency == null || usageFrequency.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid usageFrequency");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean exists = hasDefaultTokenLimit(tableName, entityType, entityId, usageFrequency);
		if (!exists && existingUsageFrequency != null && !existingUsageFrequency.trim().isEmpty()) {
			exists = hasDefaultTokenLimit(tableName, entityType, entityId, existingUsageFrequency);
		}
		if (exists && existingUsageFrequency != null && !existingUsageFrequency.trim().isEmpty()
				&& !existingUsageFrequency.equalsIgnoreCase(usageFrequency)
				&& hasDefaultTokenLimit(tableName, entityType, entityId, usageFrequency)) {
			throw new IllegalArgumentException("A default token limit already exists for usageFrequency " + usageFrequency);
		}
		String usageRestriction = resolveUsageRestriction(maxTokens, maxInputTokens, maxOutputTokens, maxResponseTime);
		String persistedFrequency = existingUsageFrequency != null && !existingUsageFrequency.trim().isEmpty()
				? existingUsageFrequency
				: usageFrequency;

		PreparedStatement ps = null;
		try {
			if (exists) {
				String updateSql = "UPDATE " + tableName + " SET USAGE_RESTRICTION=?, USAGE_FREQUENCY=?, MAX_TOKENS=?, "
						+ "MAX_INPUT_TOKENS=?, MAX_OUTPUT_TOKENS=?, MAX_RESPONSE_TIME=?, IS_ACTIVE=?, CREATED_BY=?, "
						+ "CREATED_BY_TYPE=?, DATE_MODIFIED=CURRENT_TIMESTAMP, RESTRICT_PER_MODEL=? "
						+ "WHERE ENTITY_TYPE=? AND ENTITY_ID=? AND USAGE_FREQUENCY=?";
				ps = securityDb.getPreparedStatement(updateSql);
				int idx = 1;
				bindString(ps, idx++, usageRestriction);
				bindString(ps, idx++, usageFrequency);
				bindLong(ps, idx++, maxTokens);
				bindLong(ps, idx++, maxInputTokens);
				bindLong(ps, idx++, maxOutputTokens);
				bindDouble(ps, idx++, maxResponseTime);
				ps.setBoolean(idx++, isActive);
				bindString(ps, idx++, createdBy);
				bindString(ps, idx++, createdByType);
				ps.setBoolean(idx++, restrictPerModel);
				ps.setString(idx++, entityType);
				ps.setString(idx++, entityId);
				ps.setString(idx++, persistedFrequency);
				ps.execute();
			} else {
				String insertSql = "INSERT INTO " + tableName
						+ " (ENTITY_TYPE, ENTITY_ID, USAGE_RESTRICTION, USAGE_FREQUENCY, MAX_TOKENS, "
						+ "MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAX_RESPONSE_TIME, IS_ACTIVE, CREATED_BY, "
						+ "CREATED_BY_TYPE, DATE_CREATED, DATE_MODIFIED, RESTRICT_PER_MODEL) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)";
				ps = securityDb.getPreparedStatement(insertSql);
				int idx = 1;
				ps.setString(idx++, entityType);
				ps.setString(idx++, entityId);
				bindString(ps, idx++, usageRestriction);
				bindString(ps, idx++, usageFrequency);
				bindLong(ps, idx++, maxTokens);
				bindLong(ps, idx++, maxInputTokens);
				bindLong(ps, idx++, maxOutputTokens);
				bindDouble(ps, idx++, maxResponseTime);
				ps.setBoolean(idx++, isActive);
				bindString(ps, idx++, createdBy);
				bindString(ps, idx++, createdByType);
				ps.setBoolean(idx++, restrictPerModel);
				ps.execute();
			}

			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error setting default token limit on {} for entity {}:{}", tableName, entityType,
					entityId, e);
			throw new IllegalArgumentException("Failed to set default token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	private static void removeDefaultTokenLimit(String tableName, String entityType, String entityId) {
		removeDefaultTokenLimit(tableName, entityType, entityId, null);
	}

	private static void removeDefaultTokenLimit(String tableName, String entityType, String entityId, String usageFrequency) {
		if (entityId == null || entityId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid entityId");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteSql = "DELETE FROM " + tableName + " WHERE ENTITY_TYPE=? AND ENTITY_ID=?"
				+ (usageFrequency != null && !usageFrequency.trim().isEmpty() ? " AND USAGE_FREQUENCY=?" : "");
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteSql);
			ps.setString(1, entityType);
			ps.setString(2, entityId);
			if (usageFrequency != null && !usageFrequency.trim().isEmpty()) {
				ps.setString(3, usageFrequency);
			}
			ps.execute();
			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error removing default token limit on {} for entity {}:{}", tableName, entityType,
					entityId, e);
			throw new IllegalArgumentException("Failed to remove default token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	private static String resolveUsageRestriction(long maxTokens, long maxInputTokens, long maxOutputTokens,
			Double maxResponseTime) {
		boolean hasTokenLimits = maxTokens > -1 || maxInputTokens > -1 || maxOutputTokens > -1;
		if (hasTokenLimits) {
			return Constants.MODEL_TOKEN_RESTRICTION_VALUE;
		}
		if (maxResponseTime != null && maxResponseTime.doubleValue() > -1) {
			return Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE;
		}
		return null;
	}

	private static Map<String, Object> buildResultMap(ResultSet rs) throws java.sql.SQLException {
		Map<String, Object> result = new HashMap<>();
		result.put("entityType", rs.getString("ENTITY_TYPE"));
		result.put("entityId", rs.getString("ENTITY_ID"));
		result.put("usageRestriction", rs.getString("USAGE_RESTRICTION"));
		result.put("usageFrequency", rs.getString("USAGE_FREQUENCY"));
		result.put("maxTokens", rs.getObject("MAX_TOKENS"));
		result.put("maxInputTokens", rs.getObject("MAX_INPUT_TOKENS"));
		result.put("maxOutputTokens", rs.getObject("MAX_OUTPUT_TOKENS"));
		result.put("maxResponseTime", rs.getObject("MAX_RESPONSE_TIME"));
		result.put("isActive", rs.getObject("IS_ACTIVE"));
		result.put("createdBy", rs.getString("CREATED_BY"));
		result.put("createdByType", rs.getString("CREATED_BY_TYPE"));
		result.put("dateCreated", rs.getObject("DATE_CREATED"));
		result.put("dateModified", rs.getObject("DATE_MODIFIED"));
		result.put("restrictPerModel", rs.getObject("RESTRICT_PER_MODEL"));
		return result;
	}

	private static boolean hasDefaultTokenLimit(String tableName, String entityType, String entityId, String usageFrequency) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT 1 FROM " + tableName + " WHERE ENTITY_TYPE=? AND ENTITY_ID=? AND USAGE_FREQUENCY=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, entityType);
			ps.setString(2, entityId);
			ps.setString(3, usageFrequency);
			rs = ps.executeQuery();
			return rs.next();
		} catch (Exception e) {
			classLogger.error("Error checking default token limit from {} for entity {}:{} and frequency {}", tableName,
					entityType, entityId, usageFrequency, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
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

	private static void bindDouble(PreparedStatement ps, int index, Double value) throws java.sql.SQLException {
		if (value == null || value.doubleValue() < 0) {
			ps.setNull(index, java.sql.Types.DOUBLE);
		} else {
			ps.setDouble(index, value.doubleValue());
		}
	}
}
