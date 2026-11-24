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
package prerna.engine.impl.model.inferencetracking;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * The reads of the model inference logs behind the router evidence, the learning trigger and the replay cost. They live in this
 * package, which {@link SystemEngineRegistry} already allows to open the logs database. All are scoped to the given user, except
 * that the owner of a router reads the runs of every user of it ({@code pooled}; the caller decides who is the owner).
 */
public final class RouterEvidenceStore {

	private RouterEvidenceStore() {
	}

	/** The start of a WHERE clause that limits a read to the user's own rows unless pooled; {@link #bindUser} fills its parameters. */
	private static String scope(boolean pooled) {
		return pooled ? "" : "USER_ID = ? AND (USER_TYPE = ? OR USER_TYPE IS NULL) AND ";
	}

	private static int bindUser(PreparedStatement ps, AccessToken login, boolean pooled) throws SQLException {
		if (pooled) {
			return 1;
		}
		ps.setString(1, login.getId());
		ps.setString(2, login.getProvider().getLabel());
		return 3;
	}

	/** Ids of the newest runs of an engine in the interval, newest first: the user's own, or all users' when pooled. */
	public static List<String> runIds(AccessToken login, String engineId, Instant since, int limit, boolean pooled) throws Exception {
		return runIds(login, engineId, since, Instant.now(), limit, 0, pooled);
	}

	/** A stable page of run ids: the upper timestamp bounds a multi-page read and RUN_ID breaks timestamp ties. */
	public static List<String> runIds(AccessToken login, String engineId, Instant since, Instant through, int limit, long offset, boolean pooled)
			throws Exception {
		return runIdsExcludingLearningReplays(login, engineId, since, through, limit, offset, pooled, List.of());
	}

	/**
	 * Run ids for independent evidence, omitting runs sharing an Insight with any exact attempt room supplied by the caller.
	 * An empty list excludes nothing. Runs without room metadata remain eligible.
	 */
	public static List<String> runIdsExcludingLearningReplays(AccessToken login, String engineId, Instant since, Instant through, int limit,
			long offset, boolean pooled, List<String> attemptRoomIds) throws Exception {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		String roomParameters = roomParameters(attemptRoomIds);
		StringBuilder sql = new StringBuilder("SELECT task_run.RUN_ID FROM AGENT_RUN task_run WHERE ")
				.append(pooled ? "" : "task_run.USER_ID = ? AND (task_run.USER_TYPE = ? OR task_run.USER_TYPE IS NULL) AND ")
				.append("task_run.MODEL_ID = ? AND task_run.DATE_CREATED >= ? AND task_run.DATE_CREATED <= ?")
				.append(excludeLearningReplays(roomParameters));
		sql.append(" ORDER BY task_run.DATE_CREATED DESC, task_run.RUN_ID DESC");
		db.getQueryUtil().addLimitOffsetToQuery(sql, limit, offset);
		return QueryExecutionUtility.queryList(db, sql.toString(), ps -> {
			int i = bindUser(ps, login, pooled);
			ps.setString(i++, engineId);
			ps.setTimestamp(i++, Timestamp.valueOf(LocalDateTime.ofInstant(since, ZoneOffset.UTC)));
			ps.setTimestamp(i++, Timestamp.valueOf(LocalDateTime.ofInstant(through, ZoneOffset.UTC)));
			bindAttemptRooms(ps, i, attemptRoomIds);
		}, rs -> rs.getString("RUN_ID"));
	}

	/** Whether any of the supplied exact attempt rooms has a submitted, running or input-required run. */
	public static boolean attemptRunning(List<String> attemptRoomIds) throws Exception {
		if (attemptRoomIds.isEmpty()) {
			return false;
		}
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		return QueryExecutionUtility.queryOne(db,
				"SELECT COUNT(*) AS N FROM AGENT_RUN WHERE ROOM_ID IN (" + roomParameters(attemptRoomIds)
						+ ") AND STATUS IN ('SUBMITTED', 'RUNNING', 'INPUT_REQUIRED')",
				ps -> bindAttemptRooms(ps, 1, attemptRoomIds), rs -> rs.getLong("N")) > 0;
	}

	/**
	 * Runs of an engine created after the newest run in the supplied exact attempt rooms, or since the epoch when there
	 * is no prior attempt. Evaluator replays sharing those rooms' Insights are excluded. Counts the user's own runs,
	 * or all users' when pooled.
	 */
	public static long runsSinceLastAttempt(AccessToken login, String engineId, List<String> attemptRoomIds, boolean pooled)
			throws Exception {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		String roomParameters = roomParameters(attemptRoomIds);
		String cutoff = attemptRoomIds.isEmpty() ? "?"
				: "COALESCE((SELECT MAX(DATE_CREATED) FROM AGENT_RUN WHERE ROOM_ID IN (" + roomParameters + ")), ?)";
		String sql = "SELECT COUNT(*) AS N FROM AGENT_RUN task_run WHERE "
				+ (pooled ? "" : "task_run.USER_ID = ? AND (task_run.USER_TYPE = ? OR task_run.USER_TYPE IS NULL) AND ")
				+ "task_run.MODEL_ID = ? AND task_run.DATE_CREATED > " + cutoff + excludeLearningReplays(roomParameters);
		return QueryExecutionUtility.queryOne(db, sql, ps -> {
			int i = bindUser(ps, login, pooled);
			ps.setString(i++, engineId);
			i = bindAttemptRooms(ps, i, attemptRoomIds);
			ps.setTimestamp(i++, Timestamp.valueOf(LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)));
			bindAttemptRooms(ps, i, attemptRoomIds);
		}, rs -> rs.getLong("N"));
	}

	private static String roomParameters(List<String> attemptRoomIds) {
		return String.join(", ", Collections.nCopies(attemptRoomIds.size(), "?"));
	}

	private static String excludeLearningReplays(String roomParameters) {
		return roomParameters.isEmpty() ? "" : " AND NOT EXISTS (SELECT 1 FROM ROOM replay_room JOIN ROOM learning_room "
				+ "ON learning_room.INSIGHT_ID = replay_room.INSIGHT_ID WHERE replay_room.ROOM_ID = task_run.ROOM_ID "
				+ "AND learning_room.ROOM_ID IN (" + roomParameters + "))";
	}

	private static int bindAttemptRooms(PreparedStatement ps, int i, List<String> attemptRoomIds) throws SQLException {
		for (String roomId : attemptRoomIds) {
			ps.setString(i++, roomId);
		}
		return i;
	}

	/**
	 * The priced MESSAGE rows of a room, the user's own or, when pooled, whoever's ({@code TRANSACTION_ID}, {@code COST_MICROS}). A routed call is
	 * written twice: the router's own {@code route_*} rows carry the transaction id the response message keeps,
	 * the served model's rows another one, and the two share the {@code MESSAGE_ID} of the input row. Input rows
	 * are used for attribution but only response rows are returned for cost accounting.
	 */
	public static List<Map<String, Object>> callRows(String roomId, AccessToken login, boolean pooled) throws Exception {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		String sql = "SELECT TRANSACTION_ID, MESSAGE_ID, MESSAGE_TYPE, MESSAGE_METHOD, COST_MICROS FROM MESSAGE WHERE " + scope(pooled) + "ROOM_ID = ?";
		return attribute(QueryExecutionUtility.queryList(db, sql, ps -> ps.setString(bindUser(ps, login, pooled), roomId), rs -> {
			Map<String, Object> row = new HashMap<>();
			row.put("TRANSACTION_ID", rs.getString("TRANSACTION_ID"));
			row.put("MESSAGE_ID", rs.getString("MESSAGE_ID"));
			row.put("MESSAGE_TYPE", rs.getString("MESSAGE_TYPE"));
			row.put("MESSAGE_METHOD", rs.getString("MESSAGE_METHOD"));
			row.put("COST_MICROS", rs.getObject("COST_MICROS"));
			return row;
		}));
	}

	/** Sums recorded response charges without treating an absent or unknown charge as zero. */
	public static long totalCostMicros(List<Map<String, Object>> rows) {
		if (rows.isEmpty()) {
			throw new IllegalStateException("No model cost rows were recorded for the room");
		}
		long total = 0;
		for (Map<String, Object> row : rows) {
			Object value = row.get("COST_MICROS");
			if (!(value instanceof Number)) {
				throw new IllegalStateException("A model cost row has no numeric COST_MICROS value");
			}
			long cost = new BigDecimal(value.toString()).longValueExact();
			if (cost < 0) {
				throw new IllegalStateException("A model cost row has a negative COST_MICROS value");
			}
			total = Math.addExact(total, cost);
		}
		return total;
	}

	/** Drops unpriced input/router rows and moves each served response row to the transaction id of its router call. */
	static List<Map<String, Object>> attribute(List<Map<String, Object>> rows) {
		Map<String, String> routerTransactionByMessage = new HashMap<>();
		Map<String, List<String>> servedTransactionsByMessage = new HashMap<>();
		for (Map<String, Object> row : rows) {
			String messageId = (String) row.get("MESSAGE_ID");
			if (messageId == null || !"INPUT".equals(row.get("MESSAGE_TYPE"))) {
				continue;
			}
			if (isRouterRow(row)) {
				routerTransactionByMessage.put(messageId, (String) row.get("TRANSACTION_ID"));
			} else {
				servedTransactionsByMessage.computeIfAbsent(messageId, k -> new ArrayList<>()).add((String) row.get("TRANSACTION_ID"));
			}
		}
		Map<String, String> routerTransactionByServed = new HashMap<>();
		servedTransactionsByMessage.forEach((messageId, served) -> {
			String router = routerTransactionByMessage.get(messageId);
			if (router != null) {
				served.forEach(tx -> routerTransactionByServed.put(tx, router));
			}
		});
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> row : rows) {
			if (isRouterRow(row) || !"RESPONSE".equals(row.get("MESSAGE_TYPE"))) {
				continue;
			}
			Map<String, Object> mapped = new HashMap<>();
			String tx = (String) row.get("TRANSACTION_ID");
			mapped.put("TRANSACTION_ID", routerTransactionByServed.getOrDefault(tx, tx));
			mapped.put("COST_MICROS", row.get("COST_MICROS"));
			out.add(mapped);
		}
		return out;
	}

	private static boolean isRouterRow(Map<String, Object> row) {
		return row.get("MESSAGE_METHOD") instanceof String method && method.startsWith("route_");
	}
}
