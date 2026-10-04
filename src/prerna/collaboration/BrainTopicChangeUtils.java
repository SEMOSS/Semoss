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

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.javatuples.Pair;

import prerna.auth.User;

// Undo for topic delete and merge: each one saves a before/after copy of the rows it touches in
// BRAIN_CHANGE.SNAPSHOT_JSON; undo puts the before rows back if nothing changed them since.
public final class BrainTopicChangeUtils {

	public static final String DELETE = "delete";
	public static final String MERGE = "merge";
	static final String UNDO = "undo";
	private static final String ENTITY_TOPIC = "topic";
	// snapshots back the session Undo button, so they are dropped after a day; the
	// change row stays
	private static final long SNAPSHOT_KEEP_MS = 24L * 60 * 60 * 1000;

	// table -> order by; rows are keyed by topic id, except thread links which are
	// keyed by thread
	private static final Map<String, String> TOPIC_TABLES = new LinkedHashMap<>();
	static {
		TOPIC_TABLES.put("BRAIN_TOPIC", "TOPIC_ID");
		TOPIC_TABLES.put("BRAIN_TOPIC_NOTE", "NOTE_ID");
		TOPIC_TABLES.put("BRAIN_TOPIC_PERSON", "TOPIC_ID, PERSON_ID");
		TOPIC_TABLES.put("BRAIN_THREAD_TOPIC_REJECTION", "TOPIC_ID, THREAD_ID");
		TOPIC_TABLES.put("BRAIN_RULE", "RULE_ID");
	}
	private static final String THREAD_TOPIC = "BRAIN_THREAD_TOPIC";
	// rows outside the topic that point at it by LINK_TOPIC_ID -> their id column
	private static final Map<String, String> LINK_TABLES = Map.of("WORK_ITEM", "ITEM_ID", "WORK_THREAD_STEP",
			"STEP_ID");
	private static final String OWNED = " WHERE OWNER_ID = ? AND OWNER_TYPE = ?";

	private BrainTopicChangeUtils() {
	}

	/**
	 * Rows a delete or merge is about to change, read inside its transaction before
	 * any write.
	 */
	static final class Snapshot {
		final String ownerId;
		final String ownerType;
		final List<String> topicIds;
		final List<String> threadIds;
		final String before;
		// [table, row id] of rows whose LINK_TOPIC_ID was the removed topic
		final List<List<String>> links = new ArrayList<>();
		final List<String> mergeCandidates;
		final List<String> reviews;

		private Snapshot(String ownerId, String ownerType, List<String> topicIds, List<String> threadIds, String before,
				List<String> mergeCandidates, List<String> reviews) {
			this.ownerId = ownerId;
			this.ownerType = ownerType;
			this.topicIds = topicIds;
			this.threadIds = threadIds;
			this.before = before;
			this.mergeCandidates = mergeCandidates;
			this.reviews = reviews;
		}
	}

	// removedTopicId goes away; a merge passes its target, a delete passes null
	static Snapshot capture(Connection conn, String ownerId, String ownerType, String removedTopicId,
			String targetTopicId) throws SQLException {
		List<String> topicIds = targetTopicId == null ? List.of(removedTopicId)
				: List.of(removedTopicId, targetTopicId);
		List<String> threadIds = strings(readRows(conn,
				"SELECT DISTINCT THREAD_ID FROM " + THREAD_TOPIC + OWNED + " AND TOPIC_ID IN ("
						+ CollaborationDbUtils.placeholders(topicIds.size()) + ") ORDER BY THREAD_ID",
				params(ownerId, ownerType, topicIds)), "THREAD_ID");
		List<String> mergeCandidates = strings(readRows(conn,
				"SELECT TOPIC_ID FROM BRAIN_TOPIC" + OWNED + " AND MERGE_CANDIDATE_ID = ? ORDER BY TOPIC_ID", ownerId,
				ownerType, removedTopicId), "TOPIC_ID");
		mergeCandidates.removeAll(topicIds);
		// a delete dismisses open reviews about the topic
		List<String> reviews = targetTopicId != null ? List.of()
				: strings(readRows(conn,
						"SELECT REVIEW_ID FROM BRAIN_REVIEW" + OWNED
								+ " AND REF_ID = ? AND STATUS = ? ORDER BY REVIEW_ID",
						ownerId, ownerType, removedTopicId, "open"), "REVIEW_ID");
		Snapshot snapshot = new Snapshot(ownerId, ownerType, topicIds, threadIds,
				CollaborationDbUtils.toJson(scopeRows(conn, ownerId, ownerType, topicIds, threadIds)), mergeCandidates,
				reviews);
		for (Map.Entry<String, String> table : LINK_TABLES.entrySet()) {
			for (Map<String, Object> row : readRows(conn,
					"SELECT " + table.getValue() + " FROM " + table.getKey() + OWNED + " AND LINK_TOPIC_ID = ?",
					ownerId, ownerType, removedTopicId)) {
				snapshot.links.add(List.of(table.getKey(), (String) row.get(table.getValue())));
			}
		}
		return snapshot;
	}

	// call last in the same transaction; saves the change and returns its id for
	// undo
	static String record(Connection conn, Snapshot snapshot, String field, String removedTopicId, String targetTopicId,
			Timestamp now) throws SQLException {
		Map<String, Object> saved = new LinkedHashMap<>();
		saved.put("topicIds", snapshot.topicIds);
		saved.put("threadIds", snapshot.threadIds);
		saved.put("before", snapshot.before);
		saved.put("after", CollaborationDbUtils
				.toJson(scopeRows(conn, snapshot.ownerId, snapshot.ownerType, snapshot.topicIds, snapshot.threadIds)));
		saved.put("links", snapshot.links);
		saved.put("linkAfter", targetTopicId);
		saved.put("mergeCandidates", snapshot.mergeCandidates);
		saved.put("removedTopicId", removedTopicId);
		saved.put("reviews", snapshot.reviews);

		CollaborationDbUtils.update(conn,
				"UPDATE BRAIN_CHANGE SET SNAPSHOT_JSON = NULL" + OWNED + " AND SNAPSHOT_JSON IS NOT NULL AND AT < ?",
				snapshot.ownerId, snapshot.ownerType, new Timestamp(now.getTime() - SNAPSHOT_KEEP_MS));
		String changeId = UUID.randomUUID().toString();
		insertChange(conn, snapshot.ownerId, snapshot.ownerType, changeId, removedTopicId, field, null, targetTopicId,
				now, CollaborationDbUtils.toJson(saved));
		return changeId;
	}

	// puts back the rows from before a topic delete or merge; refused if any of
	// them changed since
	@SuppressWarnings("unchecked")
	public static Map<String, Object> undo(User user, String changeId) {
		var owner = CollaborationDbUtils.ownerOf(user);
		return BrainTopicReviewProfiles.serialized(owner.getValue0(), owner.getValue1(),
				() -> undoInReview(user, changeId));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> undoInReview(User user, String changeId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Map<String, Object> change = CollaborationDbUtils.queryOne("SELECT ENTITY_ID, FIELD, SNAPSHOT_JSON "
				+ "FROM BRAIN_CHANGE" + OWNED + " AND CHANGE_ID = ? AND ENTITY_TYPE = ?", rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("topicId", CollaborationDbUtils.getString(rs, "ENTITY_ID"));
					row.put("field", CollaborationDbUtils.getString(rs, "FIELD"));
					row.put("snapshot", CollaborationDbUtils.getString(rs, "SNAPSHOT_JSON"));
					return row;
				}, ownerId, ownerType, changeId, ENTITY_TOPIC);
		if (change == null || !List.of(DELETE, MERGE).contains(change.get("field"))) {
			throw new IllegalArgumentException("Topic change not found");
		}
		if (change.get("snapshot") == null) {
			throw new IllegalArgumentException("This change can no longer be undone");
		}
		Map<String, Object> saved = CollaborationDbUtils.parseMap((String) change.get("snapshot"));
		List<String> topicIds = CollaborationDbUtils.toStringList((List<Object>) saved.get("topicIds"));
		List<String> threadIds = CollaborationDbUtils.toStringList((List<Object>) saved.get("threadIds"));
		Map<String, Object> before = CollaborationDbUtils.parseMap((String) saved.get("before"));
		Timestamp now = CollaborationDbUtils.now();
		String undoId = UUID.randomUUID().toString();

		CollaborationDbUtils.inTransaction(conn -> {
			if (!readRows(conn, "SELECT CHANGE_ID FROM BRAIN_CHANGE" + OWNED + " AND FIELD = ? AND OLD_VALUE = ?",
					ownerId, ownerType, UNDO, changeId).isEmpty()) {
				throw new IllegalArgumentException("Change already undone");
			}
			Map<String, List<Map<String, Object>>> currentRows = scopeRows(conn, ownerId, ownerType, topicIds, threadIds);
			// Old snapshots predate rejection storage. They remain undoable only while that scope has no new decisions.
			Map<String, Object> expectedAfter = CollaborationDbUtils.parseMap((String) saved.get("after"));
			if (!expectedAfter.containsKey("BRAIN_THREAD_TOPIC_REJECTION")) {
				if (!currentRows.get("BRAIN_THREAD_TOPIC_REJECTION").isEmpty()) {
					throw new IllegalArgumentException("Topic corrections changed since; undo not applied");
				}
				currentRows.remove("BRAIN_THREAD_TOPIC_REJECTION");
			}
			String current = CollaborationDbUtils.toJson(currentRows);
			if (!current.equals(saved.get("after"))) {
				throw new IllegalArgumentException("The topic changed since; undo not applied");
			}
			for (String table : TOPIC_TABLES.keySet()) {
				CollaborationDbUtils.update(conn,
						"DELETE FROM " + table + OWNED + " AND TOPIC_ID IN ("
								+ CollaborationDbUtils.placeholders(topicIds.size()) + ")",
						params(ownerId, ownerType, topicIds));
			}
			if (!threadIds.isEmpty()) {
				CollaborationDbUtils.update(conn,
						"DELETE FROM " + THREAD_TOPIC + OWNED + " AND THREAD_ID IN ("
								+ CollaborationDbUtils.placeholders(threadIds.size()) + ")",
						params(ownerId, ownerType, threadIds));
			}
			for (Map.Entry<String, Object> table : before.entrySet()) {
				insertRows(conn, table.getKey(), (List<Map<String, Object>>) table.getValue());
			}
			// links on other rows go back only where nothing else changed them
			Object after = saved.get("linkAfter");
			for (Object entry : (List<Object>) saved.get("links")) {
				List<Object> link = (List<Object>) entry;
				String table = (String) link.get(0);
				String idColumn = LINK_TABLES.get(table);
				if (idColumn == null) {
					continue;
				}
				CollaborationDbUtils.update(conn,
						"UPDATE " + table + " SET LINK_TOPIC_ID = ?" + OWNED + " AND " + idColumn + " = ? AND "
								+ (after == null ? "LINK_TOPIC_ID IS NULL" : "LINK_TOPIC_ID = ?"),
						after == null ? new Object[] { saved.get("removedTopicId"), ownerId, ownerType, link.get(1) }
								: new Object[] { saved.get("removedTopicId"), ownerId, ownerType, link.get(1), after });
			}
			for (Object topicId : (List<Object>) saved.get("mergeCandidates")) {
				CollaborationDbUtils.update(conn,
						"UPDATE BRAIN_TOPIC SET MERGE_CANDIDATE_ID = ?" + OWNED
								+ " AND TOPIC_ID = ? AND MERGE_CANDIDATE_ID IS NULL",
						saved.get("removedTopicId"), ownerId, ownerType, topicId);
			}
			for (Object reviewId : (List<Object>) saved.get("reviews")) {
				CollaborationDbUtils.update(conn,
						"UPDATE BRAIN_REVIEW SET STATUS = ?, RESOLVED_AT = NULL" + OWNED
								+ " AND REVIEW_ID = ? AND STATUS = ? AND RESOLVED_BY IS NULL",
						"open", ownerId, ownerType, reviewId, "dismissed");
			}
			insertChange(conn, ownerId, ownerType, undoId, (String) change.get("topicId"), UNDO, changeId, null, now,
					null);
		});

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("changeId", undoId);
		result.put("undoOf", changeId);
		result.put("topicId", change.get("topicId"));
		return result;
	}

	private static void insertChange(Connection conn, String ownerId, String ownerType, String changeId, String topicId,
			String field, String oldValue, String newValue, Timestamp at, String snapshot) throws SQLException {
		CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_CHANGE (OWNER_ID, OWNER_TYPE, CHANGE_ID, ENTITY_TYPE, "
				+ "ENTITY_ID, FIELD, OLD_VALUE, NEW_VALUE, ACTOR, AT, SNAPSHOT_JSON) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				ownerId, ownerType, changeId, ENTITY_TOPIC, topicId, field, oldValue, newValue, BrainProfileUtils.YOU,
				at, snapshot);
	}

	// every row the change can touch, table by table, in a stable order so
	// before/after compare as text
	private static Map<String, List<Map<String, Object>>> scopeRows(Connection conn, String ownerId, String ownerType,
			List<String> topicIds, List<String> threadIds) throws SQLException {
		Map<String, List<Map<String, Object>>> rows = new LinkedHashMap<>();
		for (Map.Entry<String, String> table : TOPIC_TABLES.entrySet()) {
			rows.put(table.getKey(),
					readRows(conn, "SELECT * FROM " + table.getKey() + OWNED + " AND TOPIC_ID IN ("
							+ CollaborationDbUtils.placeholders(topicIds.size()) + ") ORDER BY " + table.getValue(),
							params(ownerId, ownerType, topicIds)));
		}
		rows.put(THREAD_TOPIC, threadIds.isEmpty() ? List.of()
				: readRows(conn, "SELECT * FROM " + THREAD_TOPIC + OWNED + " AND THREAD_ID IN ("
						+ CollaborationDbUtils.placeholders(threadIds.size()) + ") ORDER BY THREAD_ID, TOPIC_ID",
						params(ownerId, ownerType, threadIds)));
		return rows;
	}

	// column names upper-cased; clobs as text and timestamps as Timestamp.toString
	// so rows survive JSON
	private static List<Map<String, Object>> readRows(Connection conn, String sql, Object... params)
			throws SQLException {
		List<Map<String, Object>> rows = new ArrayList<>();
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				ps.setObject(i + 1, params[i]);
			}
			try (ResultSet rs = ps.executeQuery()) {
				ResultSetMetaData meta = rs.getMetaData();
				while (rs.next()) {
					Map<String, Object> row = new LinkedHashMap<>();
					for (int c = 1; c <= meta.getColumnCount(); c++) {
						Object value = rs.getObject(c);
						if (value instanceof Clob clob) {
							value = clob.getSubString(1L, (int) clob.length());
						} else if (value instanceof Timestamp ts) {
							value = ts.toString();
						}
						row.put(meta.getColumnLabel(c).toUpperCase(Locale.ROOT), value);
					}
					rows.add(row);
				}
			}
		}
		return rows;
	}

	// JSON numbers come back as doubles, so each value is converted to its column's
	// type
	private static void insertRows(Connection conn, String table, List<Map<String, Object>> rows) throws SQLException {
		if (rows == null || rows.isEmpty()) {
			return;
		}
		Map<String, Integer> types = new LinkedHashMap<>();
		try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + table + " WHERE 1 = 0");
				ResultSet rs = ps.executeQuery()) {
			ResultSetMetaData meta = rs.getMetaData();
			for (int c = 1; c <= meta.getColumnCount(); c++) {
				types.put(meta.getColumnLabel(c).toUpperCase(Locale.ROOT), meta.getColumnType(c));
			}
		}
		for (Map<String, Object> row : rows) {
			List<String> columns = new ArrayList<>(row.keySet());
			columns.retainAll(types.keySet());
			try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + table + " (" + String.join(", ", columns)
					+ ") VALUES (" + CollaborationDbUtils.placeholders(columns.size()) + ")")) {
				for (int i = 0; i < columns.size(); i++) {
					int type = types.get(columns.get(i));
					Object value = row.get(columns.get(i));
					if (value == null) {
						ps.setNull(i + 1, type);
					} else {
						ps.setObject(i + 1, toColumnType(value, type));
					}
				}
				ps.executeUpdate();
			}
		}
	}

	private static Object toColumnType(Object value, int type) {
		switch (type) {
		case Types.TIMESTAMP:
		case Types.TIMESTAMP_WITH_TIMEZONE:
		case Types.DATE:
			return Timestamp.valueOf(String.valueOf(value));
		case Types.BOOLEAN:
		case Types.BIT:
			return value instanceof Boolean ? value : Boolean.valueOf(String.valueOf(value));
		case Types.INTEGER:
		case Types.SMALLINT:
		case Types.TINYINT:
			return value instanceof Number n ? n.intValue() : Integer.valueOf(String.valueOf(value));
		case Types.BIGINT:
			return value instanceof Number n ? n.longValue() : Long.valueOf(String.valueOf(value));
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.REAL:
			return value instanceof Number n ? n.doubleValue() : Double.valueOf(String.valueOf(value));
		case Types.NUMERIC:
		case Types.DECIMAL:
			return new BigDecimal(String.valueOf(value));
		default:
			return String.valueOf(value);
		}
	}

	private static Object[] params(String ownerId, String ownerType, List<String> ids) {
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		params.addAll(ids);
		return params.toArray();
	}

	private static List<String> strings(List<Map<String, Object>> rows, String column) {
		List<String> values = new ArrayList<>();
		for (Map<String, Object> row : rows) {
			values.add(Objects.toString(row.get(column), null));
		}
		return values;
	}
}
