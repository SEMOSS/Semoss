package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;

/**
 * Utility for default token limits stored on ENGINEPERMISSION / PROJECTPERMISSION rows
 * with USERID = NULL.
 */
public class SecurityEntityDefaultTokenUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityEntityDefaultTokenUtils.class);

	private SecurityEntityDefaultTokenUtils() {
		// utility class
	}

	public static Map<String, Object> getEngineDefaultTokenLimit(String engineId) {
		if (engineId == null || engineId.trim().isEmpty()) {
			return null;
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT ENGINEID, USAGEFREQUENCY, USAGERESTRICTION, MAXTOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAXRESPONSETIME "
				+ "FROM ENGINEPERMISSION WHERE ENGINEID = ? AND USERID IS NULL";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, engineId);
			rs = ps.executeQuery();
			if (rs.next()) {
				return buildEngineMap(rs);
			}
		} catch (Exception e) {
			classLogger.error("Error getting engine default token limit for engineId {}", engineId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return null;
	}

	public static void setEngineDefaultTokenLimit(String engineId, String usageFrequency, long maxTokens,
			long maxInputTokens, long maxOutputTokens, boolean isActive, String createdBy, String createdByType) {
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide an engineId");
		}

		String normalizedFrequency = usageFrequency == null ? null : usageFrequency.trim();
		if (normalizedFrequency != null && normalizedFrequency.isEmpty()) {
			normalizedFrequency = null;
		}

		String usageRestriction = isActive ? "token" : null;

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			boolean exists = getEngineDefaultTokenLimit(engineId) != null;
			if (exists) {
				String updateSql = "UPDATE ENGINEPERMISSION SET USAGERESTRICTION = ?, USAGEFREQUENCY = ?, MAXTOKENS = ?, "
						+ "MAX_INPUT_TOKENS = ?, MAX_OUTPUT_TOKENS = ?, MAXRESPONSETIME = ?, PERMISSIONGRANTEDBY = ?, "
						+ "PERMISSIONGRANTEDBYTYPE = ?, DATEADDED = CURRENT_TIMESTAMP WHERE ENGINEID = ? AND USERID IS NULL";
				ps = securityDb.getPreparedStatement(updateSql);
				int idx = 1;
				if (usageRestriction == null) {
					ps.setNull(idx++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(idx++, usageRestriction);
				}
				if (normalizedFrequency == null) {
					ps.setNull(idx++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(idx++, normalizedFrequency);
				}
				setLongOrNull(ps, idx++, maxTokens);
				setLongOrNull(ps, idx++, maxInputTokens);
				setLongOrNull(ps, idx++, maxOutputTokens);
				ps.setNull(idx++, java.sql.Types.DOUBLE);
				ps.setString(idx++, createdBy);
				ps.setString(idx++, createdByType);
				ps.setString(idx++, engineId);
				ps.execute();
			} else {
				String insertSql = "INSERT INTO ENGINEPERMISSION (USERID, PERMISSION, ENGINEID, VISIBILITY, FAVORITE, "
						+ "PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE, USAGERESTRICTION, "
						+ "USAGEFREQUENCY, MAXTOKENS, MAXRESPONSETIME, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?)";
				ps = securityDb.getPreparedStatement(insertSql);
				int idx = 1;
				ps.setNull(idx++, java.sql.Types.VARCHAR);
				ps.setNull(idx++, java.sql.Types.INTEGER);
				ps.setString(idx++, engineId);
				ps.setBoolean(idx++, true);
				ps.setBoolean(idx++, false);
				ps.setString(idx++, createdBy);
				ps.setString(idx++, createdByType);
				ps.setNull(idx++, java.sql.Types.TIMESTAMP);
				if (usageRestriction == null) {
					ps.setNull(idx++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(idx++, usageRestriction);
				}
				if (normalizedFrequency == null) {
					ps.setNull(idx++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(idx++, normalizedFrequency);
				}
				setLongOrNull(ps, idx++, maxTokens);
				ps.setNull(idx++, java.sql.Types.DOUBLE);
				setLongOrNull(ps, idx++, maxInputTokens);
				setLongOrNull(ps, idx++, maxOutputTokens);
				ps.execute();
			}
			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error setting engine default token limit for engineId {}", engineId, e);
			throw new IllegalArgumentException("Failed to set engine default token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	public static void removeEngineDefaultTokenLimit(String engineId) {
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide an engineId");
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteSql = "DELETE FROM ENGINEPERMISSION WHERE ENGINEID = ? AND USERID IS NULL";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteSql);
			ps.setString(1, engineId);
			ps.execute();
			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error removing engine default token limit for engineId {}", engineId, e);
			throw new IllegalArgumentException("Failed to remove engine default token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	public static Map<String, Object> getProjectDefaultTokenLimit(String projectId) {
		if (projectId == null || projectId.trim().isEmpty()) {
			return null;
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "SELECT PROJECTID, USAGEFREQUENCY, USAGERESTRICTION, MAXTOKENS, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MAXRESPONSETIME, RESTRICT_PER_MODEL "
				+ "FROM PROJECTPERMISSION WHERE PROJECTID = ? AND USERID IS NULL";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, projectId);
			rs = ps.executeQuery();
			if (rs.next()) {
				return buildProjectMap(rs);
			}
		} catch (Exception e) {
			classLogger.error("Error getting project default token limit for projectId {}", projectId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, rs);
		}
		return null;
	}

	public static void setProjectDefaultTokenLimit(String projectId, String usageFrequency, long maxTokens,
			long maxInputTokens, long maxOutputTokens, boolean isActive, String createdBy, String createdByType,
			boolean restrictPerModel) {
		if (projectId == null || projectId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a projectId");
		}

		String normalizedFrequency = usageFrequency == null ? null : usageFrequency.trim();
		if (normalizedFrequency != null && normalizedFrequency.isEmpty()) {
			normalizedFrequency = null;
		}

		String usageRestriction = isActive ? "token" : null;

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			boolean exists = getProjectDefaultTokenLimit(projectId) != null;
			if (exists) {
				String updateSql = "UPDATE PROJECTPERMISSION SET USAGERESTRICTION = ?, USAGEFREQUENCY = ?, MAXTOKENS = ?, "
						+ "MAX_INPUT_TOKENS = ?, MAX_OUTPUT_TOKENS = ?, MAXRESPONSETIME = ?, RESTRICT_PER_MODEL = ?, "
						+ "PERMISSIONGRANTEDBY = ?, PERMISSIONGRANTEDBYTYPE = ?, DATEADDED = CURRENT_TIMESTAMP "
						+ "WHERE PROJECTID = ? AND USERID IS NULL";
				ps = securityDb.getPreparedStatement(updateSql);
				int idx = 1;
				if (usageRestriction == null) {
					ps.setNull(idx++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(idx++, usageRestriction);
				}
				if (normalizedFrequency == null) {
					ps.setNull(idx++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(idx++, normalizedFrequency);
				}
				setLongOrNull(ps, idx++, maxTokens);
				setLongOrNull(ps, idx++, maxInputTokens);
				setLongOrNull(ps, idx++, maxOutputTokens);
				ps.setNull(idx++, java.sql.Types.DOUBLE);
				ps.setBoolean(idx++, restrictPerModel);
				ps.setString(idx++, createdBy);
				ps.setString(idx++, createdByType);
				ps.setString(idx++, projectId);
				ps.execute();
			} else {
				String insertSql = "INSERT INTO PROJECTPERMISSION (USERID, PERMISSION, PROJECTID, VISIBILITY, FAVORITE, "
						+ "PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE, USAGERESTRICTION, "
						+ "USAGEFREQUENCY, MAXTOKENS, MAXRESPONSETIME, MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, RESTRICT_PER_MODEL) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)";
				ps = securityDb.getPreparedStatement(insertSql);
				int idx = 1;
				ps.setNull(idx++, java.sql.Types.VARCHAR);
				ps.setNull(idx++, java.sql.Types.INTEGER);
				ps.setString(idx++, projectId);
				ps.setBoolean(idx++, true);
				ps.setBoolean(idx++, false);
				ps.setString(idx++, createdBy);
				ps.setString(idx++, createdByType);
				ps.setNull(idx++, java.sql.Types.TIMESTAMP);
				if (usageRestriction == null) {
					ps.setNull(idx++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(idx++, usageRestriction);
				}
				if (normalizedFrequency == null) {
					ps.setNull(idx++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(idx++, normalizedFrequency);
				}
				setLongOrNull(ps, idx++, maxTokens);
				ps.setNull(idx++, java.sql.Types.DOUBLE);
				setLongOrNull(ps, idx++, maxInputTokens);
				setLongOrNull(ps, idx++, maxOutputTokens);
				ps.setBoolean(idx++, restrictPerModel);
				ps.execute();
			}

			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error setting project default token limit for projectId {}", projectId, e);
			throw new IllegalArgumentException("Failed to set project default token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	public static void removeProjectDefaultTokenLimit(String projectId) {
		if (projectId == null || projectId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a projectId");
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteSql = "DELETE FROM PROJECTPERMISSION WHERE PROJECTID = ? AND USERID IS NULL";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteSql);
			ps.setString(1, projectId);
			ps.execute();
			if (!securityDb.isConnectionPooling()) {
				securityDb.commit();
			}
		} catch (Exception e) {
			classLogger.error("Error removing project default token limit for projectId {}", projectId, e);
			throw new IllegalArgumentException("Failed to remove project default token limit: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	private static void setLongOrNull(PreparedStatement ps, int idx, long value) throws Exception {
		if (value <= 0) {
			ps.setNull(idx, java.sql.Types.BIGINT);
		} else {
			ps.setLong(idx, value);
		}
	}

	private static Map<String, Object> buildEngineMap(ResultSet rs) throws Exception {
		Map<String, Object> ret = new HashMap<>();
		ret.put("engineId", rs.getString("ENGINEID"));
		ret.put("usageFrequency", rs.getString("USAGEFREQUENCY"));
		ret.put("usageRestriction", rs.getString("USAGERESTRICTION"));
		ret.put("maxTokens", rs.getObject("MAXTOKENS"));
		ret.put("maxInputTokens", rs.getObject("MAX_INPUT_TOKENS"));
		ret.put("maxOutputTokens", rs.getObject("MAX_OUTPUT_TOKENS"));
		ret.put("maxResponseTime", rs.getObject("MAXRESPONSETIME"));
		ret.put("isActive", rs.getString("USAGERESTRICTION") != null && !rs.getString("USAGERESTRICTION").trim().isEmpty());
		return ret;
	}

	private static Map<String, Object> buildProjectMap(ResultSet rs) throws Exception {
		Map<String, Object> ret = new HashMap<>();
		ret.put("projectId", rs.getString("PROJECTID"));
		ret.put("usageFrequency", rs.getString("USAGEFREQUENCY"));
		ret.put("usageRestriction", rs.getString("USAGERESTRICTION"));
		ret.put("maxTokens", rs.getObject("MAXTOKENS"));
		ret.put("maxInputTokens", rs.getObject("MAX_INPUT_TOKENS"));
		ret.put("maxOutputTokens", rs.getObject("MAX_OUTPUT_TOKENS"));
		ret.put("maxResponseTime", rs.getObject("MAXRESPONSETIME"));
		ret.put("restrictPerModel", rs.getObject("RESTRICT_PER_MODEL"));
		ret.put("isActive", rs.getString("USAGERESTRICTION") != null && !rs.getString("USAGERESTRICTION").trim().isEmpty());
		return ret;
	}
}
