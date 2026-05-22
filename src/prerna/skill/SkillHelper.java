package prerna.skill;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class SkillHelper {

	private static final Logger classLogger = LogManager.getLogger(SkillHelper.class);

	private SkillHelper() {
	}

	public static Map<String, Object> createSkill(User user, String name, String content, String description,
			List<String> tags, String projectId) {
		IRDBMSEngine engine = SystemEngineRegistry.getSkillDb();
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = engine.getConnection();
			Timestamp now = Utility.getCurrentSqlTimestampUTC();
			String skillId = GUID.v7().toUUID().toString();
			String ownerId = resolveUserId(user);
			int version = 1;
			String normalizedDescription = description == null ? "" : description;
			String normalizedContent = content == null ? "" : content;
			String tagsJson = AbstractSkillUtils.skillGson.toJson(tags == null ? List.of() : tags);

			String insertSkill = "INSERT INTO SKILL (SKILL_ID, SKILL_NAME, DESCRIPTION, CONTENT, VERSION, TAGS, OWNER_ID, PROJECT_ID, CREATED_AT, UPDATED_AT, IS_ACTIVE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = conn.prepareStatement(insertSkill);
			ps.setString(1, skillId);
			ps.setString(2, name);
			ps.setString(3, normalizedDescription);
			ps.setString(4, normalizedContent);
			ps.setInt(5, version);
			ps.setString(6, tagsJson);
			ps.setString(7, ownerId);
			ps.setString(8, projectId);
			ps.setTimestamp(9, now);
			ps.setTimestamp(10, now);
			ps.setBoolean(11, true);
			ps.executeUpdate();
			ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, null);

			String insertVersion = "INSERT INTO SKILL_VERSION (SKILL_ID, VERSION, CONTENT, CHANGE_NOTES, CREATED_AT) VALUES (?, ?, ?, ?, ?)";
			ps = conn.prepareStatement(insertVersion);
			ps.setString(1, skillId);
			ps.setInt(2, version);
			ps.setString(3, normalizedContent);
			ps.setString(4, "Initial version");
			ps.setTimestamp(5, now);
			ps.executeUpdate();

			if (!conn.getAutoCommit()) {
				conn.commit();
			}

			return getSkill(skillId);
		} catch (Exception e) {
			classLogger.error("Failed to create skill '{}'", name, e);
			return null;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, ps, null);
		}
	}

	public static Map<String, Object> getSkill(String skillId) {
		IRDBMSEngine engine = SystemEngineRegistry.getSkillDb();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = engine.getConnection();
			ps = conn.prepareStatement(
					"SELECT SKILL_ID, SKILL_NAME, DESCRIPTION, CONTENT, VERSION, TAGS, OWNER_ID, PROJECT_ID, CREATED_AT, UPDATED_AT, IS_ACTIVE FROM SKILL WHERE SKILL_ID = ?");
			ps.setString(1, skillId);
			rs = ps.executeQuery();
			if (!rs.next()) {
				return null;
			}
			Skill skill = new Skill(rs.getString("SKILL_ID"), rs.getString("SKILL_NAME"),
					readString(rs.getObject("DESCRIPTION")), readString(rs.getObject("CONTENT")),
					rs.getInt("VERSION"), parseTags(readString(rs.getObject("TAGS"))), rs.getString("OWNER_ID"),
					rs.getString("PROJECT_ID"), rs.getTimestamp("CREATED_AT"), rs.getTimestamp("UPDATED_AT"),
					rs.getBoolean("IS_ACTIVE"));
			return skill.toMap();
		} catch (Exception e) {
			classLogger.error("Failed to get skill '{}'", skillId, e);
			return null;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, ps, rs);
		}
	}

	public static List<Map<String, Object>> listSkills(String filterWord, List<String> tags, int limit, int offset) {
		IRDBMSEngine engine = SystemEngineRegistry.getSkillDb();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> results = new ArrayList<>();

		try {
			StringBuilder sql = new StringBuilder(
					"SELECT SKILL_ID, SKILL_NAME, DESCRIPTION, CONTENT, VERSION, TAGS, OWNER_ID, PROJECT_ID, CREATED_AT, UPDATED_AT, IS_ACTIVE FROM SKILL WHERE IS_ACTIVE = ?");
			List<Object> params = new ArrayList<>();
			params.add(true);

			if (StringUtils.isNotBlank(filterWord)) {
				sql.append(" AND (LOWER(SKILL_NAME) LIKE ? OR LOWER(DESCRIPTION) LIKE ?)");
				String search = "%" + filterWord.toLowerCase() + "%";
				params.add(search);
				params.add(search);
			}

			if (tags != null && !tags.isEmpty()) {
				for (String tag : tags) {
					if (StringUtils.isBlank(tag)) {
						continue;
					}
					sql.append(" AND LOWER(TAGS) LIKE ?");
					params.add("%\"" + tag.toLowerCase() + "\"%");
				}
			}
			sql.append(" ORDER BY UPDATED_AT DESC");

			conn = engine.getConnection();
			ps = conn.prepareStatement(sql.toString());
			for (int i = 0; i < params.size(); i++) {
				Object val = params.get(i);
				if (val instanceof Boolean) {
					ps.setBoolean(i + 1, (Boolean) val);
				} else {
					ps.setString(i + 1, val.toString());
				}
			}
			rs = ps.executeQuery();

			int idx = 0;
			int start = Math.max(offset, 0);
			int max = limit <= 0 ? Integer.MAX_VALUE : limit;
			while (rs.next()) {
				if (idx++ < start) {
					continue;
				}
				if (results.size() >= max) {
					break;
				}
				Skill skill = new Skill(rs.getString("SKILL_ID"), rs.getString("SKILL_NAME"),
						readString(rs.getObject("DESCRIPTION")), readString(rs.getObject("CONTENT")),
						rs.getInt("VERSION"), parseTags(readString(rs.getObject("TAGS"))), rs.getString("OWNER_ID"),
						rs.getString("PROJECT_ID"), rs.getTimestamp("CREATED_AT"), rs.getTimestamp("UPDATED_AT"),
						rs.getBoolean("IS_ACTIVE"));
				results.add(skill.toMap());
			}
		} catch (Exception e) {
			classLogger.error("Failed to list skills", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, ps, rs);
		}
		return results;
	}

	public static boolean updateSkill(String skillId, String content, String description) {
		IRDBMSEngine engine = SystemEngineRegistry.getSkillDb();
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = engine.getConnection();
			Timestamp now = Utility.getCurrentSqlTimestampUTC();
			ps = conn.prepareStatement(
					"UPDATE SKILL SET CONTENT = ?, DESCRIPTION = ?, UPDATED_AT = ? WHERE SKILL_ID = ?");
			ps.setString(1, content == null ? "" : content);
			ps.setString(2, description == null ? "" : description);
			ps.setTimestamp(3, now);
			ps.setString(4, skillId);
			int updated = ps.executeUpdate();
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return updated > 0;
		} catch (Exception e) {
			classLogger.error("Failed to update skill '{}'", skillId, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, ps, null);
		}
	}

	public static boolean deleteSkillVersion(String skillId, int version) {
		IRDBMSEngine engine = SystemEngineRegistry.getSkillDb();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = engine.getConnection();

			// must keep at least one version
			ps = conn.prepareStatement("SELECT COUNT(*) FROM SKILL_VERSION WHERE SKILL_ID = ?");
			ps.setString(1, skillId);
			rs = ps.executeQuery();
			rs.next();
			int total = rs.getInt(1);
			ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, rs);
			rs = null;
			if (total <= 1) {
				classLogger.warn("Cannot delete the only remaining version for skill '{}'", skillId);
				return false;
			}

			// delete the specific version
			ps = conn.prepareStatement("DELETE FROM SKILL_VERSION WHERE SKILL_ID = ? AND VERSION = ?");
			ps.setString(1, skillId);
			ps.setInt(2, version);
			int deleted = ps.executeUpdate();
			if (deleted == 0) {
				return false;
			}
			ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, null);

			// sync SKILL.VERSION to max remaining version
			ps = conn.prepareStatement(
					"UPDATE SKILL SET VERSION = (SELECT COALESCE(MAX(VERSION), 1) FROM SKILL_VERSION WHERE SKILL_ID = ?) WHERE SKILL_ID = ?");
			ps.setString(1, skillId);
			ps.setString(2, skillId);
			ps.executeUpdate();

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to delete version {} for skill '{}'", version, skillId, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, ps, rs);
		}
	}

	public static boolean deleteSkill(String skillId) {
		IRDBMSEngine engine = SystemEngineRegistry.getSkillDb();
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = engine.getConnection();
			// delete versions first (child rows)
			ps = conn.prepareStatement("DELETE FROM SKILL_VERSION WHERE SKILL_ID = ?");
			ps.setString(1, skillId);
			ps.executeUpdate();
			ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, null);
			// delete the skill
			ps = conn.prepareStatement("DELETE FROM SKILL WHERE SKILL_ID = ?");
			ps.setString(1, skillId);
			int deleted = ps.executeUpdate();
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return deleted > 0;
		} catch (Exception e) {
			classLogger.error("Failed to delete skill '{}'", skillId, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, ps, null);
		}
	}

	public static Map<String, Object> versionSkill(String skillId, String changeNotes) {
		IRDBMSEngine engine = SystemEngineRegistry.getSkillDb();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = engine.getConnection();

			// get current content from SKILL
			ps = conn.prepareStatement("SELECT CONTENT FROM SKILL WHERE SKILL_ID = ?");
			ps.setString(1, skillId);
			rs = ps.executeQuery();
			if (!rs.next()) {
				return null;
			}
			String content = readString(rs.getObject("CONTENT"));
			Timestamp now = Utility.getCurrentSqlTimestampUTC();
			ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, rs);

			// base next version on actual max in SKILL_VERSION, not the SKILL counter
			ps = conn.prepareStatement("SELECT COALESCE(MAX(VERSION), 0) FROM SKILL_VERSION WHERE SKILL_ID = ?");
			ps.setString(1, skillId);
			rs = ps.executeQuery();
			rs.next();
			int newVersion = rs.getInt(1) + 1;
			ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, rs);

			ps = conn.prepareStatement(
					"INSERT INTO SKILL_VERSION (SKILL_ID, VERSION, CONTENT, CHANGE_NOTES, CREATED_AT) VALUES (?, ?, ?, ?, ?)");
			ps.setString(1, skillId);
			ps.setInt(2, newVersion);
			ps.setString(3, content == null ? "" : content);
			ps.setString(4, changeNotes == null ? "" : changeNotes);
			ps.setTimestamp(5, now);
			ps.executeUpdate();
			ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, null);

			ps = conn.prepareStatement("UPDATE SKILL SET VERSION = ?, UPDATED_AT = ? WHERE SKILL_ID = ?");
			ps.setInt(1, newVersion);
			ps.setTimestamp(2, now);
			ps.setString(3, skillId);
			ps.executeUpdate();

			if (!conn.getAutoCommit()) {
				conn.commit();
			}

			Map<String, Object> out = new LinkedHashMap<>();
			out.put("skill_id", skillId);
			out.put("version", newVersion);
			return out;
		} catch (Exception e) {
			classLogger.error("Failed to version skill '{}'", skillId, e);
			return null;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, ps, rs);
		}
	}

	public static List<Map<String, Object>> getSkillVersions(String skillId) {
		IRDBMSEngine engine = SystemEngineRegistry.getSkillDb();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> versions = new ArrayList<>();
		try {
			conn = engine.getConnection();
			ps = conn.prepareStatement(
					"SELECT SKILL_ID, VERSION, CONTENT, CHANGE_NOTES, CREATED_AT FROM SKILL_VERSION WHERE SKILL_ID = ? ORDER BY VERSION DESC");
			ps.setString(1, skillId);
			rs = ps.executeQuery();
			while (rs.next()) {
				SkillVersion version = new SkillVersion(rs.getString("SKILL_ID"), rs.getInt("VERSION"),
						readString(rs.getObject("CONTENT")), readString(rs.getObject("CHANGE_NOTES")),
						rs.getTimestamp("CREATED_AT"));
				versions.add(version.toMap());
			}
		} catch (Exception e) {
			classLogger.error("Failed to get versions for skill '{}'", skillId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, ps, rs);
		}
		return versions;
	}

	public static boolean revertSkill(String skillId, int version) {
		IRDBMSEngine engine = SystemEngineRegistry.getSkillDb();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = engine.getConnection();
			ps = conn.prepareStatement(
					"SELECT CONTENT FROM SKILL_VERSION WHERE SKILL_ID = ? AND VERSION = ?");
			ps.setString(1, skillId);
			ps.setInt(2, version);
			rs = ps.executeQuery();
			if (!rs.next()) {
				return false;
			}
			String targetContent = readString(rs.getObject("CONTENT"));
			ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, rs);

			ps = conn.prepareStatement(
					"UPDATE SKILL SET CONTENT = ?, VERSION = ?, UPDATED_AT = ? WHERE SKILL_ID = ?");
			ps.setString(1, targetContent == null ? "" : targetContent);
			ps.setInt(2, version);
			ps.setTimestamp(3, Utility.getCurrentSqlTimestampUTC());
			ps.setString(4, skillId);
			int updated = ps.executeUpdate();
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return updated > 0;
		} catch (Exception e) {
			classLogger.error("Failed to revert skill '{}' to version {}", skillId, version, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, ps, rs);
		}
	}

	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Utility methods
	 */

	private static String resolveUserId(User user) {
		if (user == null || user.getPrimaryLoginToken() == null) {
			return null;
		}
		return user.getPrimaryLoginToken().getId();
	}

	private static String readString(Object value) throws Exception {
		if (value == null) {
			return null;
		}
		if (value instanceof Clob) {
			Clob clob = (Clob) value;
			return clob.getSubString(1, (int) clob.length());
		}
		return value.toString();
	}

	private static List<String> parseTags(String tagsJson) {
		if (StringUtils.isBlank(tagsJson)) {
			return new ArrayList<>();
		}
		try {
			return AbstractSkillUtils.skillGson.fromJson(tagsJson, new TypeToken<List<String>>() {
			}.getType());
		} catch (Exception e) {
			return new ArrayList<>();
		}
	}

}
