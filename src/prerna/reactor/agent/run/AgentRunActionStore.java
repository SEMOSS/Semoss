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
 * Unless required by applicable law or agreed to in writing, software
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
package prerna.reactor.agent.run;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Persistence layer for the {@code AGENT_RUN_ACTION} table.
 *
 * <p>Each row represents a single MCP tool call that was paused because its
 * {@code SMSS_MCP_EXECUTION} is {@code "ask"}. The harness creates rows when
 * it transitions a run to {@code INPUT_REQUIRED}; {@code RunMCPToolReactor}
 * updates them when the user decides; {@code GetAgentRunReactor} reads them
 * to surface pending actions to the UI.
 */
public final class AgentRunActionStore {

	private static final Gson GSON = new Gson();

	/**
	 * Insert a batch of pending actions for a single run. Called by the harness
	 * when it pauses on {@code ask} tools.
	 *
	 * @param runId    the agent run id
	 * @param roomId   the room id
	 * @param userId   the user id
	 * @param actions  one map per pending tool call, each carrying at minimum:
	 *                 {@code toolCallId}, {@code toolName}, {@code toolArgs},
	 *                 {@code toolMeta}, {@code parentMessageId}, {@code hasUi},
	 *                 {@code uiUrl}
	 */
	public void insertPendingActions(String runId, String roomId, String userId, List<Map<String, Object>> actions) {
		if (actions == null || actions.isEmpty()) {
			return;
		}
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		try {
			String query = "INSERT INTO AGENT_RUN_ACTION (ACTION_ID, RUN_ID, ROOM_ID, PARENT_MESSAGE_ID, "
					+ "TOOL_CALL_ID, TOOL_NAME, TOOL_ARGS, TOOL_META, HAS_UI, UI_URL, STATUS, "
					+ "DATE_CREATED, USER_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = db.getPreparedStatement(query);
			for (Map<String, Object> action : actions) {
				int idx = 1;
				String actionId = stringValue(action.get("actionId"));
				if (actionId == null) {
					throw new IllegalArgumentException("actionId is required for AGENT_RUN_ACTION rows");
				}
				ps.setString(idx++, actionId);
				ps.setString(idx++, runId);
				ps.setString(idx++, roomId);
				setNullableString(ps, idx++, stringValue(action.get("parentMessageId")));
				setNullableString(ps, idx++, stringValue(action.get("toolCallId")));
				setNullableString(ps, idx++, stringValue(action.get("toolName")));
				setClob(db, ps, idx++, toJson(action.get("toolArgs")));
				setClob(db, ps, idx++, toJson(action.get("toolMeta")));
				ps.setString(idx++, booleanValue(action.get("hasUi")) ? "true" : "false");
				setClob(db, ps, idx++, stringValue(action.get("uiUrl")));
				ps.setString(idx++, "PENDING");
				ps.setTimestamp(idx++, Utility.getCurrentSqlTimestampUTC());
				setNullableString(ps, idx++, userId);
				ps.addBatch();
			}
			ps.executeBatch();
			commitIfNeeded(ps);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to insert AGENT_RUN_ACTION rows for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	/**
	 * Return all action rows for a run id, ordered by creation time.
	 * Used by {@code GetAgentRunReactor} to surface pending actions.
	 */
	public List<Map<String, Object>> getActionsForRun(String runId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT ACTION_ID, RUN_ID, ROOM_ID, PARENT_MESSAGE_ID, TOOL_CALL_ID, TOOL_NAME, "
					+ "TOOL_ARGS, EDITED_ARGS, TOOL_META, HAS_UI, UI_URL, STATUS, "
					+ "RESULT, DATE_CREATED, DECIDED_AT, USER_ID "
					+ "FROM AGENT_RUN_ACTION WHERE RUN_ID = ? ORDER BY DATE_CREATED ASC";
			ps = db.getPreparedStatement(query);
			ps.setString(1, runId);
			rs = ps.executeQuery();
			List<Map<String, Object>> results = new ArrayList<>();
			while (rs.next()) {
				results.add(rowToMap(rs));
			}
			return results;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load AGENT_RUN_ACTION rows for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Return only PENDING actions for a run.
	 */
	public List<Map<String, Object>> getPendingActions(String runId) {
		List<Map<String, Object>> all = getActionsForRun(runId);
		List<Map<String, Object>> pending = new ArrayList<>();
		for (Map<String, Object> a : all) {
			if ("PENDING".equals(a.get("status"))) {
				pending.add(a);
			}
		}
		return pending;
	}

	/**
	 * Return one pending action row by action id and owner. ACTION_ID is a
	 * globally unique v7 UUID (the table PK), so scoping by actionId + userId
	 * is sufficient; runId is derivable from the row.
	 */
	public Map<String, Object> getPendingActionById(String actionId, String userId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT ACTION_ID, RUN_ID, ROOM_ID, PARENT_MESSAGE_ID, TOOL_CALL_ID, TOOL_NAME, "
					+ "TOOL_ARGS, EDITED_ARGS, TOOL_META, HAS_UI, UI_URL, STATUS, "
					+ "RESULT, DATE_CREATED, DECIDED_AT, USER_ID "
					+ "FROM AGENT_RUN_ACTION WHERE ACTION_ID = ? AND USER_ID = ? AND STATUS = 'PENDING'";
			ps = db.getPreparedStatement(query);
			ps.setString(1, actionId);
			ps.setString(2, userId);
			rs = ps.executeQuery();
			if (rs.next()) {
				return rowToMap(rs);
			}
			return null;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load pending AGENT_RUN_ACTION actionId=" + actionId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Mark an action as decided. STATUS is the single lifecycle column: it moves
	 * from PENDING to one of APPROVED / EDITED / REJECTED / RESPONDED, which also
	 * encodes what the user chose (there is no separate DECISION column).
	 *
	 * @param actionId    the action id
	 * @param editedArgs  final args when they differ from what the model proposed, else null
	 * @param result      the tool result (approve/edit), the user's response (respond),
	 *                    or the rejection message (reject)
	 * @param status      the decided status: APPROVED, EDITED, REJECTED, RESPONDED
	 */
	public boolean markDecided(String actionId, String runId, String userId, Object editedArgs, String result,
			String status) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		try {
			StringBuilder query = new StringBuilder("UPDATE AGENT_RUN_ACTION SET STATUS = ?, DECIDED_AT = ?");
			if (editedArgs != null) {
				query.append(", EDITED_ARGS = ?");
			}
			if (result != null) {
				query.append(", RESULT = ?");
			}
			query.append(" WHERE ACTION_ID = ? AND RUN_ID = ? AND USER_ID = ? AND STATUS = 'PENDING'");

			ps = db.getPreparedStatement(query.toString());
			int idx = 1;
			ps.setString(idx++, status);
			ps.setTimestamp(idx++, Utility.getCurrentSqlTimestampUTC());
			if (editedArgs != null) {
				setClob(db, ps, idx++, toJson(editedArgs));
			}
			if (result != null) {
				setClob(db, ps, idx++, result);
			}
			ps.setString(idx++, actionId);
			ps.setString(idx++, runId);
			ps.setString(idx++, userId);
			int updated = ps.executeUpdate();
			commitIfNeeded(ps);
			return updated > 0;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to update AGENT_RUN_ACTION for actionId=" + actionId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	/**
	 * Return true if there is at least one action row for the given run.
	 * Used by the worker as a short-term marker that the run has entered the
	 * HITL resume flow without relying on persisted REQUEST_JSON.resumeMode.
	 */
	public boolean hasAnyActions(String runId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT COUNT(*) FROM AGENT_RUN_ACTION WHERE RUN_ID = ?";
			ps = db.getPreparedStatement(query);
			ps.setString(1, runId);
			rs = ps.executeQuery();
			return rs.next() && rs.getInt(1) > 0;
		} catch (Exception e) {
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Check if all actions for a run have been decided (no PENDING remaining).
	 */
	public boolean allActionsDecided(String runId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT COUNT(*) FROM AGENT_RUN_ACTION WHERE RUN_ID = ? AND STATUS = 'PENDING'";
			ps = db.getPreparedStatement(query);
			ps.setString(1, runId);
			rs = ps.executeQuery();
			if (rs.next()) {
				return rs.getInt(1) == 0;
			}
			return true;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to count pending AGENT_RUN_ACTION rows for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	// --- helpers ---

	private static Map<String, Object> rowToMap(ResultSet rs) throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put("actionId", rs.getString("ACTION_ID"));
		map.put("runId", rs.getString("RUN_ID"));
		map.put("roomId", rs.getString("ROOM_ID"));
		map.put("parentMessageId", rs.getString("PARENT_MESSAGE_ID"));
		map.put("toolCallId", rs.getString("TOOL_CALL_ID"));
		map.put("toolName", rs.getString("TOOL_NAME"));
		map.put("toolArgs", clobToString(rs, "TOOL_ARGS"));
		map.put("editedArgs", clobToString(rs, "EDITED_ARGS"));
		map.put("toolMeta", clobToString(rs, "TOOL_META"));
		map.put("hasUi", "true".equalsIgnoreCase(rs.getString("HAS_UI")));
		map.put("uiUrl", clobToString(rs, "UI_URL"));
		map.put("status", rs.getString("STATUS"));
		map.put("result", clobToString(rs, "RESULT"));
		map.put("dateCreated", stringValue(rs.getTimestamp("DATE_CREATED")));
		map.put("decidedAt", stringValue(rs.getTimestamp("DECIDED_AT")));
		map.put("userId", rs.getString("USER_ID"));
		return map;
	}

	private static String clobToString(ResultSet rs, String colName) throws Exception {
		String val = rs.getString(colName);
		return val == null || val.trim().isEmpty() ? null : val;
	}

	private static String stringValue(Object value) {
		if (value == null) {
			return null;
		}
		String s = String.valueOf(value).trim();
		return s.isEmpty() ? null : s;
	}

	private static boolean booleanValue(Object value) {
		if (value == null) {
			return false;
		}
		return Boolean.TRUE.equals(value) || "true".equalsIgnoreCase(String.valueOf(value));
	}

	private static String toJson(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof String) {
			return (String) value;
		}
		return GSON.toJson(value);
	}

	private static void setNullableString(PreparedStatement ps, int idx, String value) throws Exception {
		if (value == null || value.trim().isEmpty()) {
			ps.setNull(idx, Types.VARCHAR);
		} else {
			ps.setString(idx, value);
		}
	}

	private static void setClob(IRDBMSEngine db, PreparedStatement ps, int idx, String value) throws Exception {
		if (value == null) {
			ps.setNull(idx, Types.CLOB);
		} else {
			db.getQueryUtil().handleInsertionOfClob(ps, value, idx, GSON);
		}
	}

	private static void commitIfNeeded(PreparedStatement ps) throws Exception {
		if (!ps.getConnection().getAutoCommit()) {
			ps.getConnection().commit();
		}
	}
}
