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
package prerna.reactor.agent.run;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;

import com.google.gson.Gson;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public final class AgentRunStore {

	private static final Gson GSON = new Gson();

	public void insertCreated(String runId, RunAgentRequest request, String userId) {
		insert(runId, request, userId, AgentRunStatus.CREATED);
	}

	public void insertQueued(String runId, RunAgentRequest request, String userId) {
		insert(runId, request, userId, AgentRunStatus.QUEUED);
	}

	private void insert(String runId, RunAgentRequest request, String userId, AgentRunStatus status) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		try {
			String query = "INSERT INTO AGENT_RUN (RUN_ID, ROOM_ID, WORKSPACE_ID, MODEL_ID, HARNESS_TYPE, JOB_ID, "
					+ "STATUS, INPUT, INPUT_MESSAGE_ID, FINAL_OUTPUT, FINAL_OUTPUT_MESSAGE_ID, ERROR_MESSAGE, "
					+ "DATE_CREATED, STARTED_AT, COMPLETED_AT, USER_ID) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = db.getPreparedStatement(query);
			int idx = 1;
			ps.setString(idx++, runId);
			setNullableString(ps, idx++, request.getRoomId());
			setNullableString(ps, idx++, request.getWorkspaceId());
			setNullableString(ps, idx++, request.getEngineIdFallback());
			setNullableString(ps, idx++, request.getHarnessType());
			ps.setNull(idx++, Types.VARCHAR);
			ps.setString(idx++, status.name());
			setClob(db, ps, idx++, request.getInput());
			ps.setNull(idx++, Types.VARCHAR);
			ps.setNull(idx++, Types.CLOB);
			ps.setNull(idx++, Types.VARCHAR);
			ps.setNull(idx++, Types.CLOB);
			ps.setTimestamp(idx++, Utility.getCurrentSqlTimestampUTC());
			ps.setNull(idx++, Types.TIMESTAMP);
			ps.setNull(idx++, Types.TIMESTAMP);
			setNullableString(ps, idx++, userId);
			ps.executeUpdate();
			commitIfNeeded(ps);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to create AGENT_RUN row for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	public boolean isOldestQueuedRunForRoom(String runId, String roomId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT RUN_ID FROM AGENT_RUN WHERE ROOM_ID = ? AND STATUS = ? "
					+ "ORDER BY DATE_CREATED ASC, RUN_ID ASC";
			ps = db.getPreparedStatement(query);
			setNullableString(ps, 1, roomId);
			ps.setString(2, AgentRunStatus.QUEUED.name());
			rs = ps.executeQuery();
			if (!rs.next()) {
				return false;
			}
			return runId.equals(rs.getString(1));
		} catch (Exception e) {
			throw new IllegalStateException("Failed to inspect AGENT_RUN queue for roomId=" + roomId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	public void markRunning(String runId, String jobId) {
		updateStatus(runId, AgentRunStatus.RUNNING, jobId, null, null, true, false);
	}

	public void markInputMessage(String runId, String messageId) {
		updateMessageId(runId, "INPUT_MESSAGE_ID", messageId);
	}

	public void markFinalOutputMessage(String runId, String messageId) {
		updateMessageId(runId, "FINAL_OUTPUT_MESSAGE_ID", messageId);
	}

	public void markCompleted(String runId, String jobId, String finalOutput) {
		updateStatus(runId, AgentRunStatus.COMPLETED, jobId, finalOutput, null, false, true);
	}

	public void markFailed(String runId, String jobId, String errorMessage) {
		updateStatus(runId, AgentRunStatus.FAILED, jobId, null, errorMessage, false, true);
	}

	public void markCancelled(String runId, String jobId, String errorMessage) {
		updateStatus(runId, AgentRunStatus.CANCELLED, jobId, null, errorMessage, false, true);
	}

	private void updateStatus(String runId, AgentRunStatus status, String jobId, String finalOutput, String errorMessage,
			boolean setStartedAt, boolean setCompletedAt) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		try {
			StringBuilder query = new StringBuilder("UPDATE AGENT_RUN SET STATUS = ?, JOB_ID = ?");
			if (finalOutput != null) {
				query.append(", FINAL_OUTPUT = ?");
			}
			if (errorMessage != null) {
				query.append(", ERROR_MESSAGE = ?");
			}
			if (setStartedAt) {
				query.append(", STARTED_AT = ?");
			}
			if (setCompletedAt) {
				query.append(", COMPLETED_AT = ?");
			}
			query.append(" WHERE RUN_ID = ?");

			ps = db.getPreparedStatement(query.toString());
			int idx = 1;
			ps.setString(idx++, status.name());
			setNullableString(ps, idx++, jobId);
			if (finalOutput != null) {
				setClob(db, ps, idx++, finalOutput);
			}
			if (errorMessage != null) {
				setClob(db, ps, idx++, errorMessage);
			}
			if (setStartedAt) {
				ps.setTimestamp(idx++, Utility.getCurrentSqlTimestampUTC());
			}
			if (setCompletedAt) {
				ps.setTimestamp(idx++, Utility.getCurrentSqlTimestampUTC());
			}
			ps.setString(idx++, runId);
			ps.executeUpdate();
			commitIfNeeded(ps);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to update AGENT_RUN status for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	private void updateMessageId(String runId, String columnName, String messageId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		try {
			String safeColumnName = safeMessageIdColumn(columnName);
			String query = "UPDATE AGENT_RUN SET " + safeColumnName + " = ? WHERE RUN_ID = ?";
			ps = db.getPreparedStatement(query);
			setNullableString(ps, 1, messageId);
			ps.setString(2, runId);
			ps.executeUpdate();
			commitIfNeeded(ps);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to update AGENT_RUN " + columnName + " for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	private static String safeMessageIdColumn(String columnName) {
		if ("INPUT_MESSAGE_ID".equals(columnName)) {
			return "INPUT_MESSAGE_ID";
		}
		if ("FINAL_OUTPUT_MESSAGE_ID".equals(columnName)) {
			return "FINAL_OUTPUT_MESSAGE_ID";
		}
		throw new IllegalArgumentException("Unsupported AGENT_RUN message-id column: " + columnName);
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
