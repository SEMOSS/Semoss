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
package prerna.collaboration;

import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.engine.impl.owl.AbstractOwlCreator.OwlIndex;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

// Loads the Collaboration database and holds the shared JDBC helpers for the *Utils classes
public class CollaborationDbUtils {

	private static final Logger classLogger = LogManager.getLogger(CollaborationDbUtils.class);

	static boolean initialized = false;

	private CollaborationDbUtils() {

	}

	interface RowMapper<T> {
		T map(ResultSet rs) throws SQLException;
	}

	interface TransactionWork {
		void run(Connection conn) throws SQLException;
	}

	static IRDBMSEngine db() {
		return SystemEngineRegistry.getCollaborationDb();
	}

	static Timestamp now() {
		return Utility.getCurrentSqlTimestampUTC();
	}

	// owner id and type for a signed-in user
	static Pair<String, String> ownerOf(User user) {
		if (user == null) {
			throw new IllegalArgumentException("A signed-in user is required");
		}
		return User.getPrimaryUserIdAndTypePair(user);
	}

	// name-UUID of owner plus logical key, so retries land on the same id
	static String deterministicId(String ownerId, String ownerType, String... keyParts) {
		StringBuilder seed = new StringBuilder(ownerId).append('|').append(ownerType);
		for (String part : keyParts) {
			seed.append('|').append(part == null ? "" : part);
		}
		return UUID.nameUUIDFromBytes(seed.toString().getBytes(StandardCharsets.UTF_8)).toString();
	}

	static <T> List<T> query(String sql, RowMapper<T> mapper, Object... params) {
		IRDBMSEngine engine = db();
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<T> rows = new ArrayList<>();
		try {
			ps = engine.getPreparedStatement(sql);
			bind(ps, params);
			rs = ps.executeQuery();
			while (rs.next()) {
				rows.add(mapper.map(rs));
			}
		} catch (SQLException e) {
			classLogger.error("Collaboration query failed [{}]", sql, e);
			throw new IllegalStateException("Collaboration query failed", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, ps, rs);
		}
		return rows;
	}

	static <T> T queryOne(String sql, RowMapper<T> mapper, Object... params) {
		List<T> rows = query(sql, mapper, params);
		return rows.isEmpty() ? null : rows.get(0);
	}

	static int count(String sql, Object... params) {
		Integer value = queryOne(sql, rs -> rs.getInt(1), params);
		return value == null ? 0 : value;
	}

	static boolean exists(String sql, Object... params) {
		return queryOne(sql, rs -> Boolean.TRUE, params) != null;
	}

	// single insert/update/delete; rolls back on failure so a pooled connection goes back clean
	static int update(String sql, Object... params) {
		IRDBMSEngine engine = db();
		PreparedStatement ps = null;
		Connection conn = null;
		try {
			ps = engine.getPreparedStatement(sql);
			conn = ps.getConnection();
			bind(ps, params);
			int count = ps.executeUpdate();
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return count;
		} catch (SQLException e) {
			rollback(conn);
			classLogger.error("Collaboration update failed [{}]", sql, e);
			throw new IllegalStateException("Collaboration update failed", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, ps);
		}
	}

	// several statements on one connection, one commit; needs pooling so the connection is not shared
	static void inTransaction(TransactionWork work) {
		IRDBMSEngine engine = db();
		if (!engine.isConnectionPooling()) {
			classLogger.warn("Collaboration transaction without connection pooling shares the engine connection");
		}
		Connection conn = null;
		boolean autoCommit = true;
		try {
			conn = engine.getConnection();
			autoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			work.run(conn);
			conn.commit();
		} catch (SQLException e) {
			rollback(conn);
			classLogger.error("Collaboration transaction failed", e);
			throw new IllegalStateException("Collaboration transaction failed", e);
		} finally {
			if (conn != null) {
				try {
					conn.setAutoCommit(autoCommit);
				} catch (SQLException e) {
					classLogger.error("Failed to restore auto-commit", e);
				}
			}
			if (conn != null && engine.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close connection", e);
				}
			}
		}
	}

	// for use inside inTransaction
	static int update(Connection conn, String sql, Object... params) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			bind(ps, params);
			return ps.executeUpdate();
		}
	}

	// clob-safe string read, same as NotificationDbUtils
	static String getString(ResultSet rs, String column) throws SQLException {
		Object value = rs.getObject(column);
		if (value == null) {
			return null;
		}
		if (value instanceof Clob clob) {
			return clob.getSubString(1L, (int) clob.length());
		}
		return String.valueOf(value);
	}

	// null when the column is null, unlike rs.getBoolean
	static Boolean getBoolean(ResultSet rs, String column) throws SQLException {
		boolean value = rs.getBoolean(column);
		return rs.wasNull() ? null : value;
	}

	static Integer getInteger(ResultSet rs, String column) throws SQLException {
		int value = rs.getInt(column);
		return rs.wasNull() ? null : value;
	}

	// timestamps are stored as UTC wall time (Utility.getCurrentSqlTimestampUTC), returned as ISO-8601
	static String getTimestamp(ResultSet rs, String column) throws SQLException {
		Timestamp value = rs.getTimestamp(column);
		return value == null ? null : value.toLocalDateTime().atOffset(ZoneOffset.UTC).toInstant().toString();
	}

	private static void bind(PreparedStatement ps, Object... params) throws SQLException {
		for (int i = 0; i < params.length; i++) {
			ps.setObject(i + 1, params[i]);
		}
	}

	private static void rollback(Connection conn) {
		if (conn == null) {
			return;
		}
		try {
			if (!conn.getAutoCommit()) {
				conn.rollback();
			}
		} catch (SQLException e) {
			classLogger.error("Collaboration rollback failed", e);
		}
	}

	public static void loadCollaborationDatabase() throws Exception {
		IRDBMSEngine collaborationDb = SystemEngineRegistry.getCollaborationDb();
		CollaborationOwlCreator owlCreator = new CollaborationOwlCreator(collaborationDb.getQueryUtil());
		if (owlCreator.needsRemake(collaborationDb)) {
			owlCreator.remakeOwl(collaborationDb);
		}
		initialize(owlCreator.getDBSchema());
		initialized = true;
	}

	/**
	 * Determine if the collaboration db is present.
	 *
	 * @return
	 */
	public static boolean isInitalized() {
		return CollaborationDbUtils.initialized;
	}

	private static void initialize(List<Pair<String, List<Pair<String, String>>>> dbSchema) throws Exception {
		IRDBMSEngine collaborationDb = SystemEngineRegistry.getCollaborationDb();
		Connection conn = collaborationDb.getConnection();
		try {
			// create the tables and columns from the OWL creator schema
			AbstractOwlCreator.syncSchema(collaborationDb, conn, dbSchema);

			AbstractOwlCreator.syncIndexes(collaborationDb, conn, List.of(
					// owner and sources
					OwlIndex.of("COLLAB_OWNER_OWNER_INDEX", "COLLAB_OWNER", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("COLLAB_OWNER_MS_USER_INDEX", "COLLAB_OWNER", "MS_USER_ID"),
					OwlIndex.of("SOURCE_CONNECTION_OWNER_INDEX", "SOURCE_CONNECTION", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("SOURCE_CONNECTION_SOURCE_INDEX", "SOURCE_CONNECTION", "OWNER_ID", "OWNER_TYPE",
							"SOURCE"),
					OwlIndex.of("SOURCE_SYNC_STATE_OWNER_INDEX", "SOURCE_SYNC_STATE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("SOURCE_SYNC_STATE_SOURCE_FOLDER_INDEX", "SOURCE_SYNC_STATE", "OWNER_ID", "OWNER_TYPE",
							"SOURCE", "FOLDER"),
					OwlIndex.of("INBOUND_EVENT_OWNER_INDEX", "INBOUND_EVENT", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("INBOUND_EVENT_EVENT_KEY_INDEX", "INBOUND_EVENT", "EVENT_KEY"),

					// brain: you
					OwlIndex.of("BRAIN_PROFILE_OWNER_INDEX", "BRAIN_PROFILE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_SETTINGS_OWNER_INDEX", "BRAIN_SETTINGS", "OWNER_ID", "OWNER_TYPE"),

					// brain: people
					OwlIndex.of("BRAIN_ACCOUNT_OWNER_INDEX", "BRAIN_ACCOUNT", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_ACCOUNT_ACCOUNT_ID_INDEX", "BRAIN_ACCOUNT", "OWNER_ID", "OWNER_TYPE",
							"ACCOUNT_ID"),
					OwlIndex.of("BRAIN_PERSON_OWNER_INDEX", "BRAIN_PERSON", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_PERSON_PERSON_ID_INDEX", "BRAIN_PERSON", "OWNER_ID", "OWNER_TYPE",
							"PERSON_ID"),
					OwlIndex.of("BRAIN_PERSON_EMAIL_NORM_INDEX", "BRAIN_PERSON", "OWNER_ID", "OWNER_TYPE",
							"EMAIL_NORM"),
					OwlIndex.of("BRAIN_PERSON_ADDRESS_OWNER_INDEX", "BRAIN_PERSON_ADDRESS", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_PERSON_ADDRESS_PERSON_ID_INDEX", "BRAIN_PERSON_ADDRESS", "OWNER_ID",
							"OWNER_TYPE", "PERSON_ID"),

					// brain: topics
					OwlIndex.of("BRAIN_TOPIC_OWNER_INDEX", "BRAIN_TOPIC", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_TOPIC_TOPIC_ID_INDEX", "BRAIN_TOPIC", "OWNER_ID", "OWNER_TYPE", "TOPIC_ID"),
					OwlIndex.of("BRAIN_TOPIC_NOTE_OWNER_INDEX", "BRAIN_TOPIC_NOTE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_TOPIC_NOTE_TOPIC_ID_INDEX", "BRAIN_TOPIC_NOTE", "OWNER_ID", "OWNER_TYPE",
							"TOPIC_ID"),
					OwlIndex.of("BRAIN_TOPIC_PERSON_OWNER_INDEX", "BRAIN_TOPIC_PERSON", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_TOPIC_PERSON_TOPIC_ID_INDEX", "BRAIN_TOPIC_PERSON", "OWNER_ID", "OWNER_TYPE",
							"TOPIC_ID"),
					OwlIndex.of("BRAIN_TOPIC_PERSON_PERSON_ID_INDEX", "BRAIN_TOPIC_PERSON", "OWNER_ID", "OWNER_TYPE",
							"PERSON_ID"),

					// brain: threads
					OwlIndex.of("BRAIN_THREAD_OWNER_INDEX", "BRAIN_THREAD", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_THREAD_THREAD_ID_INDEX", "BRAIN_THREAD", "OWNER_ID", "OWNER_TYPE",
							"THREAD_ID"),
					OwlIndex.of("BRAIN_THREAD_THREAD_KEY_INDEX", "BRAIN_THREAD", "OWNER_ID", "OWNER_TYPE",
							"THREAD_KEY"),
					OwlIndex.of("BRAIN_THREAD_TOPIC_OWNER_INDEX", "BRAIN_THREAD_TOPIC", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_THREAD_TOPIC_THREAD_ID_INDEX", "BRAIN_THREAD_TOPIC", "OWNER_ID", "OWNER_TYPE",
							"THREAD_ID"),
					OwlIndex.of("BRAIN_THREAD_TOPIC_TOPIC_ID_INDEX", "BRAIN_THREAD_TOPIC", "OWNER_ID", "OWNER_TYPE",
							"TOPIC_ID"),
					OwlIndex.of("BRAIN_THREAD_PARTICIPANT_OWNER_INDEX", "BRAIN_THREAD_PARTICIPANT", "OWNER_ID",
							"OWNER_TYPE"),
					OwlIndex.of("BRAIN_THREAD_PARTICIPANT_THREAD_ID_INDEX", "BRAIN_THREAD_PARTICIPANT", "OWNER_ID",
							"OWNER_TYPE", "THREAD_ID"),
					OwlIndex.of("BRAIN_THREAD_PARTICIPANT_PERSON_ID_INDEX", "BRAIN_THREAD_PARTICIPANT", "OWNER_ID",
							"OWNER_TYPE", "PERSON_ID"),
					OwlIndex.of("BRAIN_THREAD_LINK_OWNER_INDEX", "BRAIN_THREAD_LINK", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_THREAD_LINK_FROM_THREAD_KEY_INDEX", "BRAIN_THREAD_LINK", "OWNER_ID",
							"OWNER_TYPE", "FROM_THREAD_KEY"),
					OwlIndex.of("BRAIN_THREAD_LINK_TO_THREAD_KEY_INDEX", "BRAIN_THREAD_LINK", "OWNER_ID",
							"OWNER_TYPE", "TO_THREAD_KEY"),
					OwlIndex.of("BRAIN_MESSAGE_OWNER_INDEX", "BRAIN_MESSAGE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_MESSAGE_MESSAGE_KEY_INDEX", "BRAIN_MESSAGE", "OWNER_ID", "OWNER_TYPE",
							"MESSAGE_KEY"),
					OwlIndex.of("BRAIN_MESSAGE_THREAD_ID_INDEX", "BRAIN_MESSAGE", "OWNER_ID", "OWNER_TYPE",
							"THREAD_ID"),

					// brain: control
					OwlIndex.of("BRAIN_RULE_OWNER_INDEX", "BRAIN_RULE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_RULE_RULE_ID_INDEX", "BRAIN_RULE", "OWNER_ID", "OWNER_TYPE", "RULE_ID"),
					OwlIndex.of("BRAIN_REVIEW_OWNER_INDEX", "BRAIN_REVIEW", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_REVIEW_REVIEW_ID_INDEX", "BRAIN_REVIEW", "OWNER_ID", "OWNER_TYPE",
							"REVIEW_ID"),
					OwlIndex.of("BRAIN_CHANGE_OWNER_INDEX", "BRAIN_CHANGE", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("BRAIN_CHANGE_ENTITY_INDEX", "BRAIN_CHANGE", "OWNER_ID", "OWNER_TYPE", "ENTITY_TYPE",
							"ENTITY_ID"),
					OwlIndex.of("BRAIN_EXCLUSION_FINGERPRINT_OWNER_INDEX", "BRAIN_EXCLUSION_FINGERPRINT", "OWNER_ID",
							"OWNER_TYPE"),
					OwlIndex.of("BRAIN_EXCLUSION_FINGERPRINT_THREAD_PERSON_INDEX", "BRAIN_EXCLUSION_FINGERPRINT",
							"OWNER_ID", "OWNER_TYPE", "THREAD_KEY", "PERSON_ID"),

					// work
					OwlIndex.of("WORK_ITEM_OWNER_INDEX", "WORK_ITEM", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("WORK_ITEM_ITEM_ID_INDEX", "WORK_ITEM", "OWNER_ID", "OWNER_TYPE", "ITEM_ID"),
					OwlIndex.of("WORK_ITEM_THREAD_ID_INDEX", "WORK_ITEM", "OWNER_ID", "OWNER_TYPE", "THREAD_ID"),
					OwlIndex.of("WORK_ITEM_DEDUPE_KEY_INDEX", "WORK_ITEM", "OWNER_ID", "OWNER_TYPE", "DEDUPE_KEY"),
					OwlIndex.of("WORK_ITEM_HISTORY_OWNER_INDEX", "WORK_ITEM_HISTORY", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("WORK_ITEM_HISTORY_ITEM_ID_INDEX", "WORK_ITEM_HISTORY", "OWNER_ID", "OWNER_TYPE",
							"ITEM_ID"),
					OwlIndex.of("WORK_OPEN_ROOM_OWNER_INDEX", "WORK_OPEN_ROOM", "OWNER_ID", "OWNER_TYPE"),
					OwlIndex.of("WORK_OPEN_ROOM_THREAD_ID_INDEX", "WORK_OPEN_ROOM", "OWNER_ID", "OWNER_TYPE",
							"THREAD_ID")));

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			if (conn != null && collaborationDb.isConnectionPooling()) {
				conn.close();
			}
		}
	}
}
