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
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.agent.AgentRunContext;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.ValueUtils;

/**
 * All SQL against the {@code AGENT_RUN} table: inserts, status transitions,
 * queue queries, and the user-facing reads.
 *
 * <p>
 * The table is the durable record of a run, so some of the concurrency rules
 * are expressed here as SQL rather than as Java.
 * {@link #markRunningIfSubmitted} is the conditional update that decides which
 * node gets to execute a run, and {@link #isOldestSubmittedRunForRoom} is the
 * per-room FIFO order the worker honors before claiming anything.
 *
 * <p>
 * Reads made on a user's behalf are scoped by {@code USER_ID} so one user
 * cannot read another's runs. The two queue queries the worker uses are
 * deliberately unscoped, because the worker serves every user on the node and
 * has no caller identity of its own. So are the {@code ...ForAutomation} reads,
 * made once Automation has authorized the trace, and the {@code ...ForAdmin}
 * reads behind the admin Agent Runs page, made once the reactor has checked the
 * caller is an admin, or by a user closing their own room, which keeps only
 * their own runs.
 */
public final class AgentRunStore {

	private static final Gson GSON = new Gson();
	private static final String ACTIVITY_LOG_COLUMNS = "ar.RUN_ID, ar.PARENT_RUN_ID, ar.ROOM_ID, ar.WORKSPACE_ID, ar.MODEL_ID, "
			+ "ar.HARNESS_TYPE, ar.JOB_ID, ar.STATUS, ar.INPUT, ar.INPUT_MESSAGE_ID, ar.FINAL_OUTPUT, ar.FINAL_OUTPUT_MESSAGE_ID, "
			+ "ar.ERROR_MESSAGE, ar.PROGRESS_JSON, ar.DATE_CREATED, ar.STARTED_AT, ar.COMPLETED_AT, ar.USER_ID, ar.USER_TYPE, "
			+ "r.ROOM_NAME, r.IS_ACTIVE AS ROOM_IS_ACTIVE";
	// Rooms are keyed per user, so the name join must match on both columns.
	private static final String ACTIVITY_LOG_FROM = "FROM AGENT_RUN ar "
			+ "LEFT JOIN ROOM r ON ar.ROOM_ID = r.ROOM_ID AND ar.USER_ID = r.USER_ID";

	/** The statuses of a run that has not finished */
	private static final List<AgentRunStatus> ACTIVE_STATUSES = List.of(AgentRunStatus.SUBMITTED,
			AgentRunStatus.RUNNING, AgentRunStatus.INPUT_REQUIRED);
	/**
	 * When a group of runs was last active: the latest time any of them ended,
	 * started or was made
	 */
	private static final String LAST_ACTIVITY_COLUMN = "MAX(COALESCE(ar.COMPLETED_AT, ar.STARTED_AT, ar.DATE_CREATED)) "
			+ "AS LAST_ACTIVITY_AT";
	/**
	 * The runs that put a room on the admin Agent Runs page: in a room, and not a
	 * subagent run
	 */
	private static final String ROOM_ROOT_RUN_CONDITION = "ar.ROOM_ID IS NOT NULL AND ar.PARENT_RUN_ID IS NULL";

	/**
	 * One row per room and user for a list of rooms with agent runs: its agent, its
	 * user's login type, how many runs it holds, how many are still going or
	 * failed, whether the room is closed, and when it started and was last active.
	 * The login type is the one any of its runs stores, since a run can have none.
	 * The two counts take the active statuses, then FAILED; the room is closed when
	 * its IS_ACTIVE is the false that follows them.
	 */
	private static final String ROOM_LIST_COLUMNS = "ar.ROOM_ID, ar.USER_ID, MAX(ar.USER_TYPE) AS USER_TYPE, "
			+ "MAX(ar.WORKSPACE_ID) AS AGENT_ID, " + "MAX(r.ROOM_NAME) AS ROOM_NAME, COUNT(*) AS RUN_COUNT, "
			+ "SUM(CASE WHEN ar.STATUS IN (?, ?, ?) THEN 1 ELSE 0 END) AS ACTIVE_RUN_COUNT, "
			+ "SUM(CASE WHEN ar.STATUS = ? THEN 1 ELSE 0 END) AS FAILED_RUN_COUNT, "
			+ "MAX(CASE WHEN r.IS_ACTIVE = ? THEN 1 ELSE 0 END) AS ROOM_CLOSED, "
			+ "MIN(ar.DATE_CREATED) AS FIRST_RUN_AT, " + LAST_ACTIVITY_COLUMN;

	/**
	 * Which rooms a list of rooms with agent runs holds: the admin Agent Runs page,
	 * or an agent's Activity tab, for its user's own.
	 *
	 * @param agentId       rooms holding a run of this agent, or null for every
	 *                      agent
	 * @param userId        rooms of this user, or null for every user
	 * @param userType      the login type of {@code userId}, or null to match the
	 *                      id under any type
	 * @param search        rooms whose name or id contains this text, in any case,
	 *                      or null for every room
	 * @param statuses      rooms holding a run in one of these statuses, or empty
	 *                      for any status
	 * @param subagentRooms whether a room holding only subagent runs is listed, as
	 *                      an agent's Activity tab lists the rooms it ran in as
	 *                      another's subagent; the admin page shows those nested in
	 *                      their parent's room instead
	 */
	public record RoomFilter(String agentId, String userId, String userType, String search,
			Set<AgentRunStatus> statuses, boolean subagentRooms) {

		/**
		 * A filter that leaves out rooms holding only subagent runs.
		 *
		 * @param agentId  rooms holding a run of this agent, or null for every agent
		 * @param userId   rooms of this user, or null for every user
		 * @param userType the login type of {@code userId}, or null to match the id
		 *                 under any type
		 * @param search   rooms whose name or id contains this text, or null
		 * @param statuses rooms holding a run in one of these statuses, or empty
		 */
		public RoomFilter(String agentId, String userId, String userType, String search, Set<AgentRunStatus> statuses) {
			this(agentId, userId, userType, search, statuses, false);
		}
	}

	private AgentRunStore() {

	}

	/**
	 * Records a run waiting for a worker to pick it up.
	 *
	 * @param runId    durable id of the run
	 * @param request  the submission the run is created from
	 * @param userId   owner of the run
	 * @param userType login type of the owner's {@code userId}
	 */
	public static void insertSubmitted(String runId, AgentRunRequest request, String userId, String userType) {
		insert(runId, request, userId, userType, AgentRunStatus.SUBMITTED);
	}

	// Human children start waiting; no worker ever picks them up.
	static void insertInputRequired(String runId, AgentRunRequest request, String userId, String userType) {
		insert(runId, request, userId, userType, AgentRunStatus.INPUT_REQUIRED);
	}

	private static void insert(String runId, AgentRunRequest request, String userId, String userType,
			AgentRunStatus status) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		try {
			String query = "INSERT INTO AGENT_RUN (RUN_ID, PARENT_RUN_ID, ROOM_ID, WORKSPACE_ID, MODEL_ID, HARNESS_TYPE, JOB_ID, "
					+ "STATUS, INPUT, REQUEST_JSON, INPUT_MESSAGE_ID, FINAL_OUTPUT, FINAL_OUTPUT_MESSAGE_ID, ERROR_MESSAGE, "
					+ "DATE_CREATED, STARTED_AT, COMPLETED_AT, USER_ID, USER_TYPE) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = db.getPreparedStatement(query);
			int idx = 1;
			ps.setString(idx++, runId);
			setNullableString(ps, idx++, request.getParentRunId());
			setNullableString(ps, idx++, request.getRoomId());
			setNullableString(ps, idx++, request.getWorkspaceId());
			setNullableString(ps, idx++, request.getEngineIdFallback());
			setNullableString(ps, idx++, request.getHarnessType());
			ps.setString(idx++, runId);
			ps.setString(idx++, status.name());
			setClob(db, ps, idx++, request.getInput());
			setClob(db, ps, idx++, GSON.toJson(request.toPersistedMap()));
			ps.setNull(idx++, Types.VARCHAR);
			ps.setNull(idx++, Types.CLOB);
			ps.setNull(idx++, Types.VARCHAR);
			ps.setNull(idx++, Types.CLOB);
			ps.setTimestamp(idx++, Utility.getCurrentSqlTimestampUTC());
			ps.setNull(idx++, Types.TIMESTAMP);
			ps.setNull(idx++, Types.TIMESTAMP);
			setNullableString(ps, idx++, userId);
			setNullableString(ps, idx++, userType);
			ps.executeUpdate();
			commitIfNeeded(ps);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to create AGENT_RUN row for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	public static AgentRunRecord getRun(String runId, Insight insight) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String userId = resolveInsightUserId(insight);
			String query = "SELECT RUN_ID, PARENT_RUN_ID, ROOM_ID, WORKSPACE_ID, MODEL_ID, HARNESS_TYPE, STATUS, INPUT, REQUEST_JSON, "
					+ "USER_ID, USER_TYPE, JOB_ID FROM AGENT_RUN WHERE RUN_ID = ? AND USER_ID = ?";
			ps = db.getPreparedStatement(query);
			ps.setString(1, runId);
			ps.setString(2, userId);
			rs = ps.executeQuery();
			if (!rs.next()) {
				return null;
			}
			AgentRunRequest request = requestFromRow(rs, insight);
			return new AgentRunRecord(rs.getString("RUN_ID"), rs.getString("ROOM_ID"),
					parseRunStatus(rs.getString("STATUS")), request, rs.getString("USER_ID"), rs.getString("USER_TYPE"),
					rs.getString("JOB_ID"));
		} catch (Exception e) {
			if (e instanceof SecurityException) {
				throw (SecurityException) e;
			}
			throw new IllegalStateException("Failed to load AGENT_RUN row for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Loads an agent run without applying the owning-user predicate.
	 *
	 * <p>
	 * This method is reserved for Automation APIs that have already verified
	 * project access and the exact persisted Automation run, node, and agent-run
	 * trace. Generic agent APIs must use {@link #getRun(String, Insight)}.
	 *
	 * @param runId   agent run identifier
	 * @param insight current Automation editor insight used to reconstruct the
	 *                request
	 * @return matching run, or {@code null} when it does not exist
	 */
	public static AgentRunRecord getRunForAutomation(String runId, Insight insight) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		try {
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__RUN_ID", "runId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__PARENT_RUN_ID", "parentRunId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__ROOM_ID", "roomId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__WORKSPACE_ID", "workspaceId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__MODEL_ID", "modelId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__HARNESS_TYPE", "harnessType"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__STATUS", "status"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__INPUT", "input"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__REQUEST_JSON", "requestJson"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__USER_ID", "userId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__USER_TYPE", "userType"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__JOB_ID", "jobId"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("AGENT_RUN__RUN_ID", "==", runId));

			List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(db, qs);
			if (rows.isEmpty()) {
				return null;
			}
			Map<String, Object> row = rows.get(0);
			AgentRunRequest request = requestFromMap(row, insight);
			return new AgentRunRecord(ValueUtils.toStringOrNull(row.get("runId")),
					ValueUtils.toStringOrNull(row.get("roomId")),
					parseRunStatus(ValueUtils.toStringOrNull(row.get("status"))), request,
					ValueUtils.toStringOrNull(row.get("userId")), ValueUtils.toStringOrNull(row.get("userType")),
					ValueUtils.toStringOrNull(row.get("jobId")));
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load Automation AGENT_RUN row for runId=" + runId, e);
		}
	}

	public static Map<String, Object> getRunMap(String runId, Insight insight) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String userId = resolveInsightUserId(insight);
			String query = "SELECT " + ACTIVITY_LOG_COLUMNS + ", ar.REQUEST_JSON " + ACTIVITY_LOG_FROM
					+ " WHERE ar.RUN_ID = ? AND ar.USER_ID = ?";
			ps = db.getPreparedStatement(query);
			ps.setString(1, runId);
			ps.setString(2, userId);
			rs = ps.executeQuery();
			if (!rs.next()) {
				return null;
			}
			return runMapFromRow(rs);
		} catch (Exception e) {
			if (e instanceof SecurityException) {
				throw (SecurityException) e;
			}
			throw new IllegalStateException("Failed to load AGENT_RUN details for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Loads agent-run details without applying the owning-user predicate.
	 *
	 * <p>
	 * This is the detail-map equivalent of
	 * {@link #getRunForAutomation(String, Insight)} and is only valid after
	 * Automation trace authorization succeeds.
	 *
	 * @param runId agent run identifier
	 * @return matching run details, or {@code null} when it does not exist
	 */
	public static Map<String, Object> getRunMapForAutomation(String runId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		try {
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__RUN_ID", "runId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__PARENT_RUN_ID", "parentRunId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__ROOM_ID", "roomId"));
			qs.addSelector(new QueryColumnSelector("ROOM__ROOM_NAME", "roomName"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__WORKSPACE_ID", "workspaceId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__MODEL_ID", "modelId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__HARNESS_TYPE", "harnessType"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__JOB_ID", "jobId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__STATUS", "status"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__INPUT", "input"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__INPUT_MESSAGE_ID", "inputMessageId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__FINAL_OUTPUT", "finalText"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__FINAL_OUTPUT_MESSAGE_ID", "finalOutputMessageId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__ERROR_MESSAGE", "errorMessage"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__DATE_CREATED", "dateCreated"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__STARTED_AT", "startedAt"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__COMPLETED_AT", "completedAt"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__USER_ID", "userId"));
			qs.addSelector(new QueryColumnSelector("AGENT_RUN__USER_TYPE", "userType"));
			qs.addRelation("AGENT_RUN__ROOM_ID", "ROOM__ROOM_ID", "left.outer.join");
			qs.addRelation("AGENT_RUN__USER_ID", "ROOM__USER_ID", "left.outer.join");
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("AGENT_RUN__RUN_ID", "==", runId));

			List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(db, qs);
			if (rows.isEmpty()) {
				return null;
			}
			Map<String, Object> run = rows.get(0);
			run.put("dateCreated", ValueUtils.toStringOrNull(run.get("dateCreated")));
			run.put("startedAt", ValueUtils.toStringOrNull(run.get("startedAt")));
			run.put("completedAt", ValueUtils.toStringOrNull(run.get("completedAt")));
			run.put("artifacts", new ArrayList<>());
			return run;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load Automation AGENT_RUN details for runId=" + runId, e);
		}
	}

	/**
	 * Return one page of runs for an agent owned by the current user, newest first.
	 *
	 * @param insight current request insight, used to scope the query to its user
	 * @param agentId agent workspace identifier used to scope the runs
	 * @param limit   maximum number of runs to return
	 * @param offset  number of matching runs to skip
	 * @return the requested page of agent runs
	 */
	public static List<Map<String, Object>> getActivityLog(Insight insight, String agentId, long limit, long offset) {
		if (agentId == null || agentId.trim().isEmpty()) {
			throw new IllegalArgumentException("agentId is required");
		}
		if (limit <= 0) {
			throw new IllegalArgumentException("limit must be greater than 0");
		}
		if (offset < 0) {
			throw new IllegalArgumentException("offset must be greater than or equal to 0");
		}

		String userId = resolveInsightUserId(insight);
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			StringBuilder query = new StringBuilder("SELECT " + ACTIVITY_LOG_COLUMNS + " " + ACTIVITY_LOG_FROM
					+ " WHERE ar.USER_ID = ? AND ar.WORKSPACE_ID = ? ORDER BY ar.DATE_CREATED DESC, ar.RUN_ID DESC");
			db.getQueryUtil().addLimitOffsetToQuery(query, limit, offset);

			ps = db.getPreparedStatement(query.toString());
			ps.setString(1, userId);
			ps.setString(2, agentId.trim());
			rs = ps.executeQuery();

			List<Map<String, Object>> runs = new ArrayList<>();
			while (rs.next()) {
				runs.add(runMapFromRow(rs));
			}
			return runs;
		} catch (Exception e) {
			if (e instanceof SecurityException) {
				throw (SecurityException) e;
			}
			throw new IllegalStateException("Failed to load AGENT_RUN activity log", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Return every run in a room owned by the current user, newest first.
	 *
	 * @param insight current request insight, used to scope the query to its user
	 * @param roomId  room whose complete run history should be returned
	 * @return all current-user runs for the room
	 */
	public static List<Map<String, Object>> getRunsForRoom(Insight insight, String roomId) {
		if (roomId == null || roomId.trim().isEmpty()) {
			throw new IllegalArgumentException("roomId is required");
		}

		String userId = resolveInsightUserId(insight);
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT " + ACTIVITY_LOG_COLUMNS + " " + ACTIVITY_LOG_FROM
					+ " WHERE ar.USER_ID = ? AND ar.ROOM_ID = ? ORDER BY ar.DATE_CREATED DESC, ar.RUN_ID DESC";
			ps = db.getPreparedStatement(query);
			ps.setString(1, userId);
			ps.setString(2, roomId.trim());
			rs = ps.executeQuery();

			List<Map<String, Object>> runs = new ArrayList<>();
			while (rs.next()) {
				runs.add(runMapFromRow(rs));
			}
			return runs;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load AGENT_RUN rows for roomId=" + roomId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Return every direct subagent run for a parent run owned by the current user,
	 * newest first.
	 *
	 * @param insight     current request insight, used to scope the query to its
	 *                    user
	 * @param parentRunId parent run whose direct subagent runs should be returned
	 * @return all current-user runs with the supplied parent run ID
	 */
	public static List<Map<String, Object>> getSubagentRuns(Insight insight, String parentRunId) {
		if (parentRunId == null || parentRunId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		return querySubagentRuns(parentRunId.trim(), resolveInsightUserId(insight));
	}

	/**
	 * Return the direct subagent runs of any user's run, newest first, for the
	 * admin Agent Runs page.
	 *
	 * @param access      proof that the caller is an admin
	 * @param parentRunId parent run whose direct subagent runs should be returned
	 * @return every run with the supplied parent run ID
	 * @throws SecurityException when {@code access} is missing
	 */
	public static List<Map<String, Object>> getSubagentRunsForAdmin(AdminAccess access, String parentRunId) {
		AdminAccess.require(access);
		return getSubagentRunsOfAnyUser(parentRunId);
	}

	/**
	 * Return the direct subagent runs of any user's run, newest first, for closing
	 * a room, which keeps only the runs of the room's user.
	 *
	 * @param parentRunId parent run whose direct subagent runs should be returned
	 * @return every run with the supplied parent run ID
	 */
	static List<Map<String, Object>> getSubagentRunsOfAnyUser(String parentRunId) {
		if (parentRunId == null || parentRunId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		return querySubagentRuns(parentRunId.trim(), null);
	}

	/**
	 * Reads the direct subagent runs of a run, each marked with whether a person or
	 * an agent ran it.
	 *
	 * @param parentRunId parent run whose direct subagent runs are read
	 * @param userId      the user whose runs are read, or null for every user's
	 * @return the subagent runs, newest first
	 */
	private static List<Map<String, Object>> querySubagentRuns(String parentRunId, String userId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT " + ACTIVITY_LOG_COLUMNS + ", ar.REQUEST_JSON " + ACTIVITY_LOG_FROM
					+ " WHERE ar.PARENT_RUN_ID = ?" + (userId == null ? "" : " AND ar.USER_ID = ?")
					+ " ORDER BY ar.DATE_CREATED DESC, ar.RUN_ID DESC";
			ps = db.getPreparedStatement(query);
			ps.setString(1, parentRunId);
			if (userId != null) {
				ps.setString(2, userId);
			}
			rs = ps.executeQuery();

			List<Map<String, Object>> runs = new ArrayList<>();
			while (rs.next()) {
				Map<String, Object> run = runMapFromRow(rs);
				// Lets child cards tell a person's task from an agent's.
				AgentRunRequest request = requestFromJson(rs.getString("REQUEST_JSON"));
				boolean human = request != null && request.isHumanExecutor();
				run.put("executorType", human ? "HUMAN" : "AGENT");
				run.put("executorLabel", human ? request.getHumanExecutorLabel() : null);
				runs.add(run);
			}
			return runs;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load subagent AGENT_RUN rows for parentRunId=" + parentRunId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	// ------------------------------------------------------------------------
	// rooms and their runs: the caller's own, matched by the id and login type of
	// their primary login, and any user's, for the admin Agent Runs page, which
	// take an AdminAccess so none of them runs without the admin check
	// ------------------------------------------------------------------------

	/**
	 * Return one page of the caller's own rooms with an agent, most recently active
	 * first, for the agent's Activity tab, rooms where it ran only as another
	 * agent's subagent included.
	 *
	 * @param insight  the caller's insight
	 * @param agentId  the agent
	 * @param search   rooms whose name or id contains this text, or null
	 * @param statuses rooms holding a run in one of these statuses, or empty
	 * @param limit    maximum number of rooms to return
	 * @param offset   number of matching rooms to skip
	 * @return each room's id, run counts, whether it is closed, and first and last
	 *         activity
	 * @throws SecurityException when the caller is not signed in
	 */
	public static List<Map<String, Object>> getRoomsForAgent(Insight insight, String agentId, String search,
			Set<AgentRunStatus> statuses, long limit, long offset) {
		return queryRooms(callerRoomFilter(insight, agentId, search, statuses), limit, offset);
	}

	/**
	 * Count the rooms {@link #getRoomsForAgent} lists.
	 *
	 * @param insight  the caller's insight
	 * @param agentId  the agent
	 * @param search   rooms whose name or id contains this text, or null
	 * @param statuses rooms holding a run in one of these statuses, or empty
	 * @return how many rooms match, across every page
	 * @throws SecurityException when the caller is not signed in
	 */
	public static long countRoomsForAgent(Insight insight, String agentId, String search,
			Set<AgentRunStatus> statuses) {
		return queryRoomCount(callerRoomFilter(insight, agentId, search, statuses));
	}

	/**
	 * Return one page of any user's rooms with agent runs, most recently active
	 * first, for the admin Agent Runs page.
	 *
	 * @param access proof that the caller is an admin
	 * @param filter which rooms to list
	 * @param limit  maximum number of rooms to return
	 * @param offset number of matching rooms to skip
	 * @return each room's id, user, agent, run counts, whether it is closed, and
	 *         first and last activity
	 * @throws SecurityException when {@code access} is missing
	 */
	public static List<Map<String, Object>> getRoomsForAdmin(AdminAccess access, RoomFilter filter, long limit,
			long offset) {
		AdminAccess.require(access);
		return queryRooms(filter, limit, offset);
	}

	/**
	 * Count the rooms {@link #getRoomsForAdmin} lists.
	 *
	 * @param access proof that the caller is an admin
	 * @param filter which rooms to count
	 * @return how many rooms match, across every page
	 * @throws SecurityException when {@code access} is missing
	 */
	public static long countRoomsForAdmin(AdminAccess access, RoomFilter filter) {
		AdminAccess.require(access);
		return queryRoomCount(filter);
	}

	/**
	 * The filter for the caller's own rooms with an agent.
	 *
	 * @param insight  the caller's insight
	 * @param agentId  the agent
	 * @param search   rooms whose name or id contains this text, or null
	 * @param statuses rooms holding a run in one of these statuses, or empty
	 * @return the filter, by the id and login type of the caller's primary login
	 */
	private static RoomFilter callerRoomFilter(Insight insight, String agentId, String search,
			Set<AgentRunStatus> statuses) {
		AccessToken login = resolveInsightLogin(insight);
		return new RoomFilter(agentId, login.getId(), login.getProvider().getLabel(), search, statuses, true);
	}

	/**
	 * Read one page of rooms with agent runs, most recently active first. A room is
	 * listed once per user, and, unless the filter asks for subagent rooms, only
	 * when it holds a run that is not a subagent run: a subagent's own room shows
	 * nested in its parent's room.
	 *
	 * @param filter which rooms to list
	 * @param limit  maximum number of rooms to return
	 * @param offset number of matching rooms to skip
	 * @return each room's id, user, agent, run counts, whether it is closed, and
	 *         first and last activity
	 */
	private static List<Map<String, Object>> queryRooms(RoomFilter filter, long limit, long offset) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			List<Object> params = new ArrayList<>();
			StringBuilder query = new StringBuilder(roomsQuery(filter, params))
					.append(" ORDER BY LAST_ACTIVITY_AT DESC, ar.ROOM_ID, ar.USER_ID");
			db.getQueryUtil().addLimitOffsetToQuery(query, limit, offset);
			ps = db.getPreparedStatement(query.toString());
			for (int i = 0; i < params.size(); i++) {
				ps.setObject(i + 1, params.get(i));
			}
			rs = ps.executeQuery();

			List<Map<String, Object>> rooms = new ArrayList<>();
			while (rs.next()) {
				Map<String, Object> room = new HashMap<>();
				room.put("roomId", rs.getString("ROOM_ID"));
				room.put("userId", rs.getString("USER_ID"));
				room.put("userType", rs.getString("USER_TYPE"));
				room.put("agentId", rs.getString("AGENT_ID"));
				room.put("roomName", rs.getString("ROOM_NAME"));
				room.put("runCount", rs.getLong("RUN_COUNT"));
				room.put("activeRunCount", rs.getLong("ACTIVE_RUN_COUNT"));
				room.put("failedRunCount", rs.getLong("FAILED_RUN_COUNT"));
				room.put("isClosed", rs.getInt("ROOM_CLOSED") > 0);
				room.put("firstRunAt", ValueUtils.toStringOrNull(rs.getTimestamp("FIRST_RUN_AT")));
				room.put("lastActivityAt", ValueUtils.toStringOrNull(rs.getTimestamp("LAST_ACTIVITY_AT")));
				rooms.add(room);
			}
			return rooms;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load the rooms with AGENT_RUN rows", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Count the rooms {@link #queryRooms} reads.
	 *
	 * @param filter which rooms to count
	 * @return how many rooms match, across every page
	 */
	private static long queryRoomCount(RoomFilter filter) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			List<Object> params = new ArrayList<>();
			ps = db.getPreparedStatement("SELECT COUNT(*) FROM (" + roomsQuery(filter, params) + ") rooms");
			for (int i = 0; i < params.size(); i++) {
				ps.setObject(i + 1, params.get(i));
			}
			rs = ps.executeQuery();
			return rs.next() ? rs.getLong(1) : 0L;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to count the rooms with AGENT_RUN rows", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Return every run the caller has in one of their rooms, newest first, subagent
	 * runs included: the run list the agent's Activity tab opens.
	 *
	 * @param insight the caller's insight
	 * @param roomId  the caller's room
	 * @return the caller's runs in the room
	 * @throws SecurityException when the caller is not signed in
	 */
	public static List<Map<String, Object>> getRunsForRoomByLogin(Insight insight, String roomId) {
		AccessToken login = resolveInsightLogin(insight);
		return getUserRunsForRoom(roomId, login.getId(), login.getProvider().getLabel());
	}

	/**
	 * Return every run any user has in a room, newest first, for the admin Agent
	 * Runs page: the run list the room's owner sees on the agent's Activity tab.
	 *
	 * @param access   proof that the caller is an admin
	 * @param roomId   room whose runs are returned
	 * @param userId   the user the room belongs to
	 * @param userType the login type of {@code userId}, or null to match the id
	 *                 under any type
	 * @return the user's runs in the room
	 * @throws SecurityException when {@code access} is missing
	 */
	public static List<Map<String, Object>> getRunsForRoomForAdmin(AdminAccess access, String roomId, String userId,
			String userType) {
		AdminAccess.require(access);
		return getUserRunsForRoom(roomId, userId, userType);
	}

	/**
	 * Return every run a user has in a room, newest first, for the reads above and
	 * for closing a room.
	 *
	 * @param roomId   room whose runs are returned
	 * @param userId   the user the room belongs to
	 * @param userType the login type of {@code userId}, or null to match the id
	 *                 under any type
	 * @return the user's runs in the room
	 */
	static List<Map<String, Object>> getUserRunsForRoom(String roomId, String userId, String userType) {
		if (roomId == null || roomId.trim().isEmpty()) {
			throw new IllegalArgumentException("roomId is required");
		}
		if (userId == null || userId.trim().isEmpty()) {
			throw new IllegalArgumentException("userId is required");
		}

		String type = ValueUtils.trimToNull(userType);
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT " + ACTIVITY_LOG_COLUMNS + " " + ACTIVITY_LOG_FROM + " WHERE ar.ROOM_ID = ?"
					+ ownerCondition(type) + " ORDER BY ar.DATE_CREATED DESC, ar.RUN_ID DESC";
			ps = db.getPreparedStatement(query);
			ps.setString(1, roomId.trim());
			ps.setString(2, userId.trim());
			if (type != null) {
				ps.setString(3, type);
			}
			rs = ps.executeQuery();

			List<Map<String, Object>> runs = new ArrayList<>();
			while (rs.next()) {
				runs.add(runMapFromRow(rs));
			}
			return runs;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load AGENT_RUN rows for roomId=" + roomId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Load any user's agent-run details, for the admin Agent Runs page.
	 *
	 * @param access proof that the caller is an admin
	 * @param runId  agent run identifier
	 * @return matching run details with the run's user, or {@code null} when it
	 *         does not exist
	 * @throws SecurityException when {@code access} is missing
	 */
	public static Map<String, Object> getRunMapForAdmin(AdminAccess access, String runId) {
		AdminAccess.require(access);
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT " + ACTIVITY_LOG_COLUMNS + ", ar.REQUEST_JSON " + ACTIVITY_LOG_FROM
					+ " WHERE ar.RUN_ID = ?";
			ps = db.getPreparedStatement(query);
			ps.setString(1, runId);
			rs = ps.executeQuery();
			return rs.next() ? runMapFromRow(rs) : null;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load AGENT_RUN details for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Builds the grouped rooms query the room lists read and count, adding its
	 * parameters in the order its placeholders appear.
	 *
	 * @param filter which rooms to match
	 * @param params receives the query's parameters
	 * @return the query, without an order or a page
	 */
	private static String roomsQuery(RoomFilter filter, List<Object> params) {
		StringBuilder query = new StringBuilder("SELECT ").append(ROOM_LIST_COLUMNS).append(' ')
				.append(ACTIVITY_LOG_FROM).append(" WHERE ar.ROOM_ID IS NOT NULL");
		for (AgentRunStatus status : ACTIVE_STATUSES) {
			params.add(status.name());
		}
		params.add(AgentRunStatus.FAILED.name());
		params.add(Boolean.FALSE);
		if (filter.userId() != null) {
			String type = ValueUtils.trimToNull(filter.userType());
			query.append(ownerCondition(type));
			params.add(filter.userId());
			if (type != null) {
				params.add(type);
			}
		}
		String search = ValueUtils.trimToNull(filter.search());
		if (search != null) {
			query.append(" AND (LOWER(r.ROOM_NAME) LIKE ? ESCAPE '!' OR LOWER(ar.ROOM_ID) LIKE ? ESCAPE '!')");
			params.add(containsPattern(search));
			params.add(containsPattern(search));
		}
		query.append(" GROUP BY ar.ROOM_ID, ar.USER_ID");
		List<String> having = new ArrayList<>();
		if (!filter.subagentRooms()) {
			having.add("SUM(CASE WHEN ar.PARENT_RUN_ID IS NULL THEN 1 ELSE 0 END) > 0");
		}
		if (filter.agentId() != null) {
			having.add("SUM(CASE WHEN ar.WORKSPACE_ID = ? THEN 1 ELSE 0 END) > 0");
			params.add(filter.agentId());
		}
		if (!filter.statuses().isEmpty()) {
			having.add("SUM(CASE WHEN ar.STATUS IN ("
					+ String.join(", ", Collections.nCopies(filter.statuses().size(), "?"))
					+ ") THEN 1 ELSE 0 END) > 0");
			for (AgentRunStatus status : filter.statuses()) {
				params.add(status.name());
			}
		}
		if (!having.isEmpty()) {
			query.append(" HAVING ").append(String.join(" AND ", having));
		}
		return query.toString();
	}

	/**
	 * The agents of the rooms the admin Agent Runs page lists, most recently active
	 * first, for its agent filter: an agent is offered once a room holds a run of
	 * it that is not a subagent run.
	 *
	 * @param access   proof that the caller is an admin
	 * @param search   agents whose id contains this text, in any case, or null for
	 *                 every agent
	 * @param agentIds more agents to match with the search, such as those whose
	 *                 name contains it
	 * @param limit    maximum number of agents to return
	 * @return each agent's id and when its rooms were last active
	 * @throws SecurityException when {@code access} is missing
	 */
	public static List<Map<String, Object>> getRoomAgentsForAdmin(AdminAccess access, String search,
			Collection<String> agentIds, long limit) {
		AdminAccess.require(access);
		List<Object> params = new ArrayList<>();
		StringBuilder query = new StringBuilder("SELECT ar.WORKSPACE_ID AS AGENT_ID, ").append(LAST_ACTIVITY_COLUMN)
				.append(" FROM AGENT_RUN ar WHERE ").append(ROOM_ROOT_RUN_CONDITION)
				.append(" AND ar.WORKSPACE_ID IS NOT NULL");
		String text = ValueUtils.trimToNull(search);
		if (text != null) {
			List<String> matches = new ArrayList<>();
			matches.add("LOWER(ar.WORKSPACE_ID) LIKE ? ESCAPE '!'");
			params.add(containsPattern(text));
			if (!agentIds.isEmpty()) {
				matches.add(
						"ar.WORKSPACE_ID IN (" + String.join(", ", Collections.nCopies(agentIds.size(), "?")) + ")");
				params.addAll(agentIds);
			}
			query.append(" AND (").append(String.join(" OR ", matches)).append(')');
		}
		query.append(" GROUP BY ar.WORKSPACE_ID ORDER BY LAST_ACTIVITY_AT DESC, ar.WORKSPACE_ID");
		return queryOptions(query, params, limit, rs -> {
			Map<String, Object> agent = new HashMap<>();
			agent.put("agentId", rs.getString("AGENT_ID"));
			agent.put("lastActivityAt", ValueUtils.toStringOrNull(rs.getTimestamp("LAST_ACTIVITY_AT")));
			return agent;
		});
	}

	/**
	 * The users of the rooms the admin Agent Runs page lists, most recently active
	 * first, for its user filter: a user is offered once a room of theirs holds a
	 * run that is not a subagent run. A user is an id and the login type their runs
	 * store, so runs that store none give a row of their own.
	 *
	 * @param access proof that the caller is an admin
	 * @param search users whose id contains this text, in any case, or null for
	 *               every user
	 * @param users  more users to match with the search, by id and login type, such
	 *               as those whose name contains it; runs that store no type match
	 *               the id alone
	 * @param limit  maximum number of users to return
	 * @return each user's id and login type and when their rooms were last active
	 * @throws SecurityException when {@code access} is missing
	 */
	public static List<Map<String, Object>> getRoomUsersForAdmin(AdminAccess access, String search,
			Collection<Pair<String, String>> users, long limit) {
		AdminAccess.require(access);
		List<Object> params = new ArrayList<>();
		StringBuilder query = new StringBuilder("SELECT ar.USER_ID, ar.USER_TYPE, ").append(LAST_ACTIVITY_COLUMN)
				.append(" FROM AGENT_RUN ar WHERE ").append(ROOM_ROOT_RUN_CONDITION)
				.append(" AND ar.USER_ID IS NOT NULL");
		String text = ValueUtils.trimToNull(search);
		if (text != null) {
			List<String> matches = new ArrayList<>();
			matches.add("LOWER(ar.USER_ID) LIKE ? ESCAPE '!'");
			params.add(containsPattern(text));
			for (Pair<String, String> user : users) {
				matches.add("(ar.USER_ID = ? AND (ar.USER_TYPE = ? OR ar.USER_TYPE IS NULL))");
				params.add(user.getValue0());
				params.add(user.getValue1());
			}
			query.append(" AND (").append(String.join(" OR ", matches)).append(')');
		}
		query.append(" GROUP BY ar.USER_ID, ar.USER_TYPE ORDER BY LAST_ACTIVITY_AT DESC, ar.USER_ID");
		return queryOptions(query, params, limit, rs -> {
			Map<String, Object> user = new HashMap<>();
			user.put("userId", rs.getString("USER_ID"));
			user.put("userType", rs.getString("USER_TYPE"));
			user.put("lastActivityAt", ValueUtils.toStringOrNull(rs.getTimestamp("LAST_ACTIVITY_AT")));
			return user;
		});
	}

	/**
	 * Reads one page of filter options.
	 *
	 * @param query  the options query, without a page
	 * @param params its parameters, in the order its placeholders appear
	 * @param limit  maximum number of options to return
	 * @param mapper reads one option from its row
	 * @return the options
	 */
	private static List<Map<String, Object>> queryOptions(StringBuilder query, List<Object> params, long limit,
			QueryExecutionUtility.RowMapper<Map<String, Object>> mapper) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			db.getQueryUtil().addLimitOffsetToQuery(query, limit, 0);
			ps = db.getPreparedStatement(query.toString());
			for (int i = 0; i < params.size(); i++) {
				ps.setObject(i + 1, params.get(i));
			}
			rs = ps.executeQuery();
			List<Map<String, Object>> options = new ArrayList<>();
			while (rs.next()) {
				options.add(mapper.map(rs));
			}
			return options;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load the agent run filter options", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * A LIKE pattern, escaped with {@code !}, that matches text containing the
	 * given text in any case. Use it with {@code LOWER(column) LIKE ? ESCAPE '!'}.
	 *
	 * @param text the text to find
	 * @return the pattern
	 */
	private static String containsPattern(String text) {
		return "%" + text.trim().toLowerCase(Locale.ROOT).replace("!", "!!").replace("%", "!%").replace("_", "!_")
				+ "%";
	}

	/**
	 * The condition a run's owner meets: the id, and the login type when one is
	 * given. A run that stores no type matches the id alone, since its login type
	 * is not known.
	 *
	 * @param userType the owner's login type, or null to match the id under any
	 *                 type
	 * @return the condition to append to a WHERE clause; it takes the id, then the
	 *         type when one is given
	 */
	private static String ownerCondition(String userType) {
		return userType == null ? " AND ar.USER_ID = ?"
				: " AND ar.USER_ID = ? AND (ar.USER_TYPE = ? OR ar.USER_TYPE IS NULL)";
	}

	/** Internal durable inputs for detached-child delivery and repair. */
	static List<Map<String, Object>> getTerminalChildCompletions(String childRunId, String parentRunId,
			String parentRoomId, String userId, long limit) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			StringBuilder query = new StringBuilder(
					"SELECT child.RUN_ID AS CHILD_RUN_ID, child.PARENT_RUN_ID, child.STATUS, child.FINAL_OUTPUT, "
							+ "child.ERROR_MESSAGE, child.REQUEST_JSON, child.USER_ID, parent.ROOM_ID AS PARENT_ROOM_ID, "
							+ "parent.REQUEST_JSON AS PARENT_REQUEST_JSON "
							+ "FROM AGENT_RUN child JOIN AGENT_RUN parent ON child.PARENT_RUN_ID = parent.RUN_ID "
							+ "AND child.USER_ID = parent.USER_ID WHERE child.PARENT_RUN_ID IS NOT NULL "
							+ "AND child.STATUS IN (?, ?, ?)");
			if (childRunId != null && !childRunId.isBlank()) {
				query.append(" AND child.RUN_ID = ?");
			}
			if (parentRunId != null && !parentRunId.isBlank()) {
				query.append(" AND child.PARENT_RUN_ID = ?");
			}
			if (parentRoomId != null && !parentRoomId.isBlank()) {
				query.append(" AND parent.ROOM_ID = ?");
			}
			if (userId != null && !userId.isBlank()) {
				query.append(" AND child.USER_ID = ?");
			}
			query.append(" ORDER BY child.COMPLETED_AT DESC, child.RUN_ID DESC");
			if (limit > 0) {
				db.getQueryUtil().addLimitOffsetToQuery(query, limit, 0);
			}

			ps = db.getPreparedStatement(query.toString());
			int idx = 1;
			ps.setString(idx++, AgentRunStatus.COMPLETED.name());
			ps.setString(idx++, AgentRunStatus.FAILED.name());
			ps.setString(idx++, AgentRunStatus.CANCELLED.name());
			if (childRunId != null && !childRunId.isBlank()) {
				ps.setString(idx++, childRunId.trim());
			}
			if (parentRunId != null && !parentRunId.isBlank()) {
				ps.setString(idx++, parentRunId.trim());
			}
			if (parentRoomId != null && !parentRoomId.isBlank()) {
				ps.setString(idx++, parentRoomId.trim());
			}
			if (userId != null && !userId.isBlank()) {
				ps.setString(idx++, userId.trim());
			}

			rs = ps.executeQuery();
			List<Map<String, Object>> completions = new ArrayList<>();
			while (rs.next()) {
				Map<String, Object> completion = new HashMap<>();
				completion.put("childRunId", rs.getString("CHILD_RUN_ID"));
				completion.put("parentRunId", rs.getString("PARENT_RUN_ID"));
				completion.put("parentRoomId", rs.getString("PARENT_ROOM_ID"));
				completion.put("status", rs.getString("STATUS"));
				completion.put("finalText", rs.getString("FINAL_OUTPUT"));
				completion.put("errorMessage", rs.getString("ERROR_MESSAGE"));
				completion.put("requestJson", rs.getString("REQUEST_JSON"));
				completion.put("parentRequestJson", rs.getString("PARENT_REQUEST_JSON"));
				completion.put("userId", rs.getString("USER_ID"));
				completions.add(completion);
			}
			return completions;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load terminal child completions", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	/**
	 * Keeps the final text a cancelled run's thread still reached, without
	 * touching its status, error or completion time, which the cancel set.
	 *
	 * @param runId       the run
	 * @param finalOutput its final text
	 */
	static void recordFinalOutputAfterCancel(String runId, String finalOutput) {
		if (finalOutput == null) {
			return;
		}
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		try {
			ps = db.getPreparedStatement("UPDATE AGENT_RUN SET FINAL_OUTPUT = ? WHERE RUN_ID = ? AND STATUS = ?");
			setClob(db, ps, 1, finalOutput);
			ps.setString(2, runId);
			ps.setString(3, AgentRunStatus.CANCELLED.name());
			ps.executeUpdate();
			commitIfNeeded(ps);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to record the final output of cancelled runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	/**
	 * The error a run's row holds, such as why it was cancelled.
	 *
	 * @param runId the run
	 * @return its error message, or null when it has none or no row
	 */
	static String getErrorMessage(String runId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = db.getPreparedStatement("SELECT ERROR_MESSAGE FROM AGENT_RUN WHERE RUN_ID = ?");
			ps.setString(1, runId);
			rs = ps.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load the AGENT_RUN error for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	// Unscoped: the assignee answering a delegation is not the owner of either run.
	static String getParentRoomId(String childRunId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = db.getPreparedStatement("SELECT parent.ROOM_ID FROM AGENT_RUN child JOIN AGENT_RUN parent "
					+ "ON child.PARENT_RUN_ID = parent.RUN_ID AND child.USER_ID = parent.USER_ID WHERE child.RUN_ID = ?");
			ps.setString(1, childRunId);
			rs = ps.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to load parent room for runId=" + childRunId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	static boolean runExists(String runId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT 1 FROM AGENT_RUN WHERE RUN_ID = ?";
			ps = db.getPreparedStatement(query);
			ps.setString(1, runId);
			rs = ps.executeQuery();
			return rs.next();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to inspect AGENT_RUN existence for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	public static List<AgentRunRecord> getSubmittedRuns(int limit, Insight insight) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT RUN_ID, PARENT_RUN_ID, ROOM_ID, WORKSPACE_ID, MODEL_ID, HARNESS_TYPE, STATUS, INPUT, REQUEST_JSON, "
					+ "USER_ID, USER_TYPE, JOB_ID FROM AGENT_RUN WHERE STATUS = ? ORDER BY DATE_CREATED ASC, RUN_ID ASC";
			ps = db.getPreparedStatement(query);
			ps.setString(1, AgentRunStatus.SUBMITTED.name());
			rs = ps.executeQuery();
			List<AgentRunRecord> records = new ArrayList<>();
			while (rs.next() && (limit <= 0 || records.size() < limit)) {
				AgentRunRequest request = requestFromRow(rs, insight);
				records.add(new AgentRunRecord(rs.getString("RUN_ID"), rs.getString("ROOM_ID"),
						parseRunStatus(rs.getString("STATUS")), request, rs.getString("USER_ID"),
						rs.getString("USER_TYPE"), rs.getString("JOB_ID")));
			}
			return records;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to inspect submitted AGENT_RUN rows", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
	}

	public static boolean isOldestSubmittedRunForRoom(String runId, String roomId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String query = "SELECT RUN_ID FROM AGENT_RUN WHERE ROOM_ID = ? AND STATUS = ? "
					+ "ORDER BY DATE_CREATED ASC, RUN_ID ASC";
			ps = db.getPreparedStatement(query);
			setNullableString(ps, 1, roomId);
			ps.setString(2, AgentRunStatus.SUBMITTED.name());
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

	public static void markRunning(String runId, String jobId) {
		updateStatus(runId, AgentRunStatus.RUNNING, jobId, null, null, true, false);
	}

	public static boolean markRunningIfSubmitted(String runId, String jobId) {
		return updateStatusIfCurrent(runId, AgentRunStatus.SUBMITTED, AgentRunStatus.RUNNING, jobId, null, null, true,
				false);
	}

	public static void markInputMessage(String runId, String messageId) {
		updateMessageId(runId, "INPUT_MESSAGE_ID", messageId);
	}

	public static void markFinalOutputMessage(String runId, String messageId) {
		updateMessageId(runId, "FINAL_OUTPUT_MESSAGE_ID", messageId);
	}

	/**
	 * Marks a run completed with its final output.
	 *
	 * @param runId       the run
	 * @param jobId       the job that ran it
	 * @param finalOutput its final text
	 * @return whether the run is now completed; false when it was cancelled first,
	 *         and stays cancelled
	 */
	public static boolean markCompleted(String runId, String jobId, String finalOutput) {
		return updateStatus(runId, AgentRunStatus.COMPLETED, jobId, finalOutput, null, false, true);
	}

	/**
	 * Marks a run failed that still produced final output, such as one that ran out
	 * of turns.
	 *
	 * @param runId        the run
	 * @param jobId        the job that ran it
	 * @param finalOutput  its final text
	 * @param errorMessage why it did not complete
	 * @return whether the run is now failed; false when it was cancelled first, and
	 *         stays cancelled
	 */
	public static boolean markIncomplete(String runId, String jobId, String finalOutput, String errorMessage) {
		return updateStatus(runId, AgentRunStatus.FAILED, jobId, finalOutput, errorMessage, false, true);
	}

	/**
	 * Marks a run failed.
	 *
	 * @param runId        the run
	 * @param jobId        the job that ran it
	 * @param errorMessage why it failed
	 * @return whether the run is now failed; false when it was cancelled first, and
	 *         stays cancelled
	 */
	public static boolean markFailed(String runId, String jobId, String errorMessage) {
		return updateStatus(runId, AgentRunStatus.FAILED, jobId, null, errorMessage, false, true);
	}

	public static boolean markFailedIfSubmitted(String runId, String jobId, String errorMessage) {
		return updateStatusIfCurrent(runId, AgentRunStatus.SUBMITTED, AgentRunStatus.FAILED, jobId, null, errorMessage,
				false, true);
	}

	/**
	 * Transition a run from RUNNING to INPUT_REQUIRED. Called by the worker when
	 * the harness pauses on {@code SMSS_MCP_EXECUTION=ask} tools.
	 */
	public static void markInputRequired(String runId, String jobId) {
		updateStatusIfCurrent(runId, AgentRunStatus.RUNNING, AgentRunStatus.INPUT_REQUIRED, jobId, null, null, false,
				false);
	}

	/**
	 * Transition a run from INPUT_REQUIRED back to SUBMITTED so the worker picks it
	 * up and resumes. Called by {@code RunMCPToolReactor} after the user's decision
	 * has been written to the room.
	 */
	public static boolean markResumed(String runId, String jobId) {
		return updateStatusIfCurrent(runId, AgentRunStatus.INPUT_REQUIRED, AgentRunStatus.SUBMITTED, jobId, null, null,
				false, false);
	}

	static boolean markCompletedIfInputRequired(String runId, String finalOutput) {
		return updateStatusIfCurrent(runId, AgentRunStatus.INPUT_REQUIRED, AgentRunStatus.COMPLETED, runId, finalOutput,
				null, false, true);
	}

	static boolean markFailedIfInputRequired(String runId, String errorMessage) {
		return updateStatusIfCurrent(runId, AgentRunStatus.INPUT_REQUIRED, AgentRunStatus.FAILED, runId, null,
				errorMessage, false, true);
	}

	public static boolean markCancelledIfNotTerminal(String runId, String jobId, String errorMessage) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		try {
			String query = "UPDATE AGENT_RUN SET STATUS = ?, JOB_ID = ?, ERROR_MESSAGE = ?, COMPLETED_AT = ? "
					+ "WHERE RUN_ID = ? AND STATUS IN (?, ?, ?)";
			ps = db.getPreparedStatement(query);
			int idx = 1;
			ps.setString(idx++, AgentRunStatus.CANCELLED.name());
			setNullableString(ps, idx++, jobId);
			setClob(db, ps, idx++, errorMessage);
			ps.setTimestamp(idx++, Utility.getCurrentSqlTimestampUTC());
			ps.setString(idx++, runId);
			ps.setString(idx++, AgentRunStatus.SUBMITTED.name());
			ps.setString(idx++, AgentRunStatus.RUNNING.name());
			ps.setString(idx++, AgentRunStatus.INPUT_REQUIRED.name());
			int updated = ps.executeUpdate();
			commitIfNeeded(ps);
			return updated > 0;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to cancel AGENT_RUN runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	/**
	 * Sets a run's status, unless it was cancelled: a cancel is final, so a run
	 * that fails or ends after it was stopped, or after its room closed under it,
	 * keeps the reason and time it was cancelled with.
	 */
	/**
	 * Sets a run's status, unless it is cancelled: a cancel is final.
	 *
	 * @return whether the row changed; false when the run is cancelled or has no
	 *         row
	 */
	private static boolean updateStatus(String runId, AgentRunStatus status, String jobId, String finalOutput,
			String errorMessage, boolean setStartedAt, boolean setCompletedAt) {
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
			query.append(" WHERE RUN_ID = ? AND (STATUS IS NULL OR STATUS <> ?)");

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
			ps.setString(idx++, AgentRunStatus.CANCELLED.name());
			int changed = ps.executeUpdate();
			commitIfNeeded(ps);
			return changed > 0;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to update AGENT_RUN status for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	private static boolean updateStatusIfCurrent(String runId, AgentRunStatus currentStatus, AgentRunStatus nextStatus,
			String jobId, String finalOutput, String errorMessage, boolean setStartedAt, boolean setCompletedAt) {
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
			query.append(" WHERE RUN_ID = ? AND STATUS = ?");

			ps = db.getPreparedStatement(query.toString());
			int idx = 1;
			ps.setString(idx++, nextStatus.name());
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
			ps.setString(idx++, currentStatus.name());
			int updated = ps.executeUpdate();
			commitIfNeeded(ps);
			return updated > 0;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to update AGENT_RUN status for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	/**
	 * Store progress independently of the request and final outcome, including
	 * failed runs.
	 */
	public static void updateProgress(String runId, Map<String, Object> progress) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement ps = null;
		try {
			ps = db.getPreparedStatement("UPDATE AGENT_RUN SET PROGRESS_JSON = ? WHERE RUN_ID = ?");
			setClob(db, ps, 1, GSON.toJson(progress));
			ps.setString(2, runId);
			ps.executeUpdate();
			commitIfNeeded(ps);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to persist progress for runId=" + runId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	private static void updateMessageId(String runId, String columnName, String messageId) {
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

	@SuppressWarnings("unchecked")
	private static AgentRunRequest requestFromRow(ResultSet rs, Insight insight) throws Exception {
		String requestJson = rs.getString("REQUEST_JSON");
		if (requestJson != null && !requestJson.trim().isEmpty()) {
			Map<String, Object> persisted = GSON.fromJson(requestJson, Map.class);
			AgentRunRequest request = AgentRunRequest.fromPersistedMap(persisted, insight);
			if (request != null) {
				String parentRunId = rs.getString("PARENT_RUN_ID");
				return request.getParentRunId() == null && parentRunId != null ? request.withParentRunId(parentRunId)
						: request;
			}
		}
		return new AgentRunRequest(rs.getString("ROOM_ID"), rs.getString("INPUT"), rs.getString("MODEL_ID"),
				rs.getString("HARNESS_TYPE"), rs.getString("WORKSPACE_ID"), AgentRunContext.DEFAULT_MAX_TURNS,
				AgentRunContext.DEFAULT_MAX_REFLECTIONS, null, null, null, null, insight)
				.withParentRunId(rs.getString("PARENT_RUN_ID"));
	}

	@SuppressWarnings("unchecked")
	private static AgentRunRequest requestFromMap(Map<String, Object> row, Insight insight) {
		String requestJson = ValueUtils.toStringOrNull(row.get("requestJson"));
		if (requestJson != null) {
			Map<String, Object> persisted = GSON.fromJson(requestJson, Map.class);
			AgentRunRequest request = AgentRunRequest.fromPersistedMap(persisted, insight);
			if (request != null) {
				String parentRunId = ValueUtils.toStringOrNull(row.get("parentRunId"));
				return request.getParentRunId() == null && parentRunId != null ? request.withParentRunId(parentRunId)
						: request;
			}
		}
		return new AgentRunRequest(ValueUtils.toStringOrNull(row.get("roomId")),
				ValueUtils.toStringOrNull(row.get("input")), ValueUtils.toStringOrNull(row.get("modelId")),
				ValueUtils.toStringOrNull(row.get("harnessType")), ValueUtils.toStringOrNull(row.get("workspaceId")),
				AgentRunContext.DEFAULT_MAX_TURNS, AgentRunContext.DEFAULT_MAX_REFLECTIONS, null, null, null, null,
				insight).withParentRunId(ValueUtils.toStringOrNull(row.get("parentRunId")));
	}

	@SuppressWarnings("unchecked")
	private static AgentRunRequest requestFromJson(String json) {
		try {
			return json == null ? null : AgentRunRequest.fromPersistedMap(GSON.fromJson(json, Map.class), null);
		} catch (RuntimeException e) {
			return null;
		}
	}

	private static AgentRunStatus parseRunStatus(String value) {
		if (value == null || value.trim().isEmpty()) {
			return null;
		}
		return AgentRunStatus.valueOf(value.trim());
	}

	/**
	 * The caller's primary login, whose id and login type their runs store.
	 *
	 * @param insight the caller's insight
	 * @return the login
	 * @throws SecurityException when the caller is not signed in
	 */
	private static AccessToken resolveInsightLogin(Insight insight) {
		User user = insight == null ? null : insight.getUser();
		AccessToken login = user == null ? null : user.getPrimaryLoginToken();
		if (login == null || login.getId() == null || login.getId().isBlank() || login.getProvider() == null) {
			throw new SecurityException("AGENT_RUN access requires an authenticated user");
		}
		return login;
	}

	private static String resolveInsightUserId(Insight insight) {
		if (insight == null) {
			throw new SecurityException("AGENT_RUN access requires an authenticated user");
		}
		String insightUserId = insight.getUserId();
		if (insightUserId == null || insightUserId.trim().isEmpty() || "-1".equals(insightUserId)) {
			throw new SecurityException("AGENT_RUN access requires an authenticated user");
		}
		return insightUserId;
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

	private static Map<String, Object> runMapFromRow(ResultSet rs) throws SQLException {
		Map<String, Object> map = new HashMap<>();
		map.put("runId", rs.getString("RUN_ID"));
		map.put("parentRunId", rs.getString("PARENT_RUN_ID"));
		map.put("roomId", rs.getString("ROOM_ID"));
		map.put("roomName", rs.getString("ROOM_NAME"));
		// closed once its row is marked inactive; a room with no row, or no value, is
		// open
		boolean roomActive = rs.getBoolean("ROOM_IS_ACTIVE");
		map.put("isRoomClosed", !roomActive && !rs.wasNull());
		map.put("workspaceId", rs.getString("WORKSPACE_ID"));
		map.put("modelId", rs.getString("MODEL_ID"));
		map.put("harnessType", rs.getString("HARNESS_TYPE"));
		map.put("jobId", rs.getString("JOB_ID"));
		map.put("status", rs.getString("STATUS"));
		map.put("input", rs.getString("INPUT"));
		map.put("inputMessageId", rs.getString("INPUT_MESSAGE_ID"));
		map.put("finalText", rs.getString("FINAL_OUTPUT"));
		map.put("finalOutputMessageId", rs.getString("FINAL_OUTPUT_MESSAGE_ID"));
		map.put("errorMessage", rs.getString("ERROR_MESSAGE"));
		String progressJson = rs.getString("PROGRESS_JSON");
		if (progressJson != null && !progressJson.isBlank()) {
			map.put("progress", GSON.fromJson(progressJson, Map.class));
		}
		Map<String, Object> liveProgress = prerna.reactor.agent.runtime.AgentRunProgress
				.activeSnapshot(rs.getString("RUN_ID"));
		if (liveProgress != null) {
			map.put("progress", liveProgress);
		}
		map.put("dateCreated", ValueUtils.toStringOrNull(rs.getTimestamp("DATE_CREATED")));
		map.put("startedAt", ValueUtils.toStringOrNull(rs.getTimestamp("STARTED_AT")));
		map.put("completedAt", ValueUtils.toStringOrNull(rs.getTimestamp("COMPLETED_AT")));
		map.put("userId", rs.getString("USER_ID"));
		map.put("userType", rs.getString("USER_TYPE"));
		map.put("artifacts", new ArrayList<>());
		addDeliveryMetadata(map);
		return map;
	}

	/**
	 * Project code-owned delivery evidence independently of the run's execution
	 * status.
	 */
	static void addDeliveryMetadata(Map<String, Object> run) {
		if (!(run.get("progress") instanceof Map<?, ?> progress)
				|| !(progress.get("workflow") instanceof Map<?, ?> workflow)) {
			return;
		}
		if (workflow.get("reviewOutcome") instanceof Map<?, ?> review) {
			run.put("reviewOutcome", new HashMap<>(review));
		}
		if (workflow.get("warning") instanceof String warning && !warning.isBlank()) {
			run.put("warnings", List.of(warning));
		}
		if (workflow.get("artifact") instanceof Map<?, ?> artifact && "available".equals(artifact.get("status"))) {
			run.put("artifacts", List.of(new HashMap<>(artifact)));
		}
	}
}
