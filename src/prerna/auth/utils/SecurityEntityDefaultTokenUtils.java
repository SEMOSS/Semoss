package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
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

	public static Map<String, Object> getEngineDefaultTokenLimit(String engineId) {
		return getDefaultTokenLimit(USER_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId);
	}

	public static Map<String, Object> getProjectDefaultTokenLimit(String projectId) {
		return getDefaultTokenLimit(USER_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId);
	}

	public static Map<String, Object> getEngineDefaultTeamTokenLimit(String engineId) {
		return getDefaultTokenLimit(TEAM_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId);
	}

	public static Map<String, Object> getProjectDefaultTeamTokenLimit(String projectId) {
		return getDefaultTokenLimit(TEAM_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId);
	}

	public static void setEngineDefaultTokenLimit(String engineId, String usageFrequency, long maxTokens,
			long maxInputTokens, long maxOutputTokens, boolean isActive, String createdBy, String createdByType) {
		setDefaultTokenLimit(USER_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId, usageFrequency, maxTokens,
				maxInputTokens, maxOutputTokens, null, isActive, createdBy, createdByType, false);
	}

	public static void setProjectDefaultTokenLimit(String projectId, String usageFrequency, long maxTokens,
			long maxInputTokens, long maxOutputTokens, boolean isActive, String createdBy, String createdByType,
			boolean restrictPerModel) {
		setDefaultTokenLimit(USER_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId, usageFrequency, maxTokens,
				maxInputTokens, maxOutputTokens, null, isActive, createdBy, createdByType, restrictPerModel);
	}

	public static void setEngineDefaultTeamTokenLimit(String engineId, String usageFrequency, long maxTokens,
			long maxInputTokens, long maxOutputTokens, boolean isActive, String createdBy, String createdByType) {
		setDefaultTokenLimit(TEAM_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId, usageFrequency, maxTokens,
				maxInputTokens, maxOutputTokens, null, isActive, createdBy, createdByType, false);
	}

	public static void setProjectDefaultTeamTokenLimit(String projectId, String usageFrequency, long maxTokens,
			long maxInputTokens, long maxOutputTokens, boolean isActive, String createdBy, String createdByType) {
		setDefaultTokenLimit(TEAM_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId, usageFrequency, maxTokens,
				maxInputTokens, maxOutputTokens, null, isActive, createdBy, createdByType, false);
	}

	public static void removeEngineDefaultTokenLimit(String engineId) {
		removeDefaultTokenLimit(USER_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId);
	}

	public static void removeProjectDefaultTokenLimit(String projectId) {
		removeDefaultTokenLimit(USER_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId);
	}

	public static void removeEngineDefaultTeamTokenLimit(String engineId) {
		removeDefaultTokenLimit(TEAM_DEFAULT_TABLE, ENGINE_ENTITY_TYPE, engineId);
	}

	public static void removeProjectDefaultTeamTokenLimit(String projectId) {
		removeDefaultTokenLimit(TEAM_DEFAULT_TABLE, PROJECT_ENTITY_TYPE, projectId);
	}

	private static Map<String, Object> getDefaultTokenLimit(String tableName, String entityType, String entityId) {
		if (entityId == null || entityId.trim().isEmpty()) {
			return null;
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT ENTITY_TYPE, ENTITY_ID, USAGE_RESTRICTION, USAGE_FREQUENCY, MAX_TOKENS, "
				+ "MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAX_RESPONSE_TIME, IS_ACTIVE, CREATED_BY, CREATED_BY_TYPE, "
				+ "DATE_CREATED, DATE_MODIFIED, RESTRICT_PER_MODEL FROM " + tableName
				+ " WHERE ENTITY_TYPE=? AND ENTITY_ID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, entityType);
			ps.setString(2, entityId);
			rs = ps.executeQuery();
			if (rs.next()) {
				return buildResultMap(rs);
			}
		} catch (Exception e) {
			classLogger.error("Error getting default token limit from {} for entity {}:{}", tableName, entityType,
					entityId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return null;
	}

	private static void setDefaultTokenLimit(String tableName, String entityType, String entityId, String usageFrequency,
			long maxTokens, long maxInputTokens, long maxOutputTokens, Double maxResponseTime, boolean isActive,
			String createdBy, String createdByType, boolean restrictPerModel) {
		if (entityId == null || entityId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid entityId");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, Object> existing = getDefaultTokenLimit(tableName, entityType, entityId);
		String usageRestriction = resolveUsageRestriction(maxTokens, maxInputTokens, maxOutputTokens, maxResponseTime);

		PreparedStatement ps = null;
		try {
			if (existing != null) {
				String updateSql = "UPDATE " + tableName + " SET USAGE_RESTRICTION=?, USAGE_FREQUENCY=?, MAX_TOKENS=?, "
						+ "MAX_INPUT_TOKENS=?, MAX_OUTPUT_TOKENS=?, MAX_RESPONSE_TIME=?, IS_ACTIVE=?, CREATED_BY=?, "
						+ "CREATED_BY_TYPE=?, DATE_MODIFIED=CURRENT_TIMESTAMP, RESTRICT_PER_MODEL=? "
						+ "WHERE ENTITY_TYPE=? AND ENTITY_ID=?";
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
		if (entityId == null || entityId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid entityId");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteSql = "DELETE FROM " + tableName + " WHERE ENTITY_TYPE=? AND ENTITY_ID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteSql);
			ps.setString(1, entityType);
			ps.setString(2, entityId);
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
