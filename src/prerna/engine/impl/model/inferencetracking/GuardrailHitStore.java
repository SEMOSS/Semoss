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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * Stores the guardrail hits an engine's hit limits count
 * ({@code GUARDRAIL_HIT}) and the users locked out of an engine after their
 * hits reached a limit ({@code GUARDRAIL_LOCK}), in the model inference
 * database. A user is always their id and login type together. Every method
 * throws when the database cannot be read or written, so a caller can refuse
 * the call rather than let it through unchecked.
 */
public final class GuardrailHitStore {

	/**
	 * One guardrail check hit to count.
	 *
	 * @param engineId          the guarded engine
	 * @param checkId           the check in the engine's pipeline file
	 * @param guardrailEngineId the guardrail engine the check ran
	 * @param methodName        the engine method called
	 * @param phase             INPUT or OUTPUT
	 * @param action            what the check did: BLOCK, MASK or RESPOND
	 * @param roomId            the room the call ran in, or null
	 * @param insightId         the insight the call ran in, or null
	 * @param user              the caller's id and login type, or null when no user
	 *                          is signed in
	 */
	public record Hit(String engineId, String checkId, String guardrailEngineId, String methodName, String phase,
			String action, String roomId, String insightId, Pair<String, String> user) {
	}

	/**
	 * A user's lock on an engine.
	 *
	 * @param engineId    the engine
	 * @param limitId     the hit limit that locked the user
	 * @param userId      the user's id
	 * @param userType    the user's login type
	 * @param lockedAt    when the lock started, in UTC
	 * @param lockedUntil when the lock ends, in UTC
	 */
	public record Lock(String engineId, String limitId, String userId, String userType, Timestamp lockedAt,
			Timestamp lockedUntil) {
	}

	/**
	 * When a user's hit of a check happened.
	 *
	 * @param checkId the check
	 * @param at      when it happened, in UTC
	 */
	public record HitTime(String checkId, Timestamp at) {
	}

	private static final String LOCK_COLUMNS = "ENGINE_ID, LIMIT_ID, USER_ID, USER_TYPE, LOCKED_AT, LOCKED_UNTIL";

	private GuardrailHitStore() {
	}

	/**
	 * Records a hit.
	 *
	 * @param hit the hit
	 * @param at  when it happened, in UTC
	 * @throws Exception when the hit cannot be written
	 */
	public static void recordHit(Hit hit, Timestamp at) throws Exception {
		QueryExecutionUtility.executeUpdate(db(),
				"INSERT INTO GUARDRAIL_HIT (HIT_ID, ENGINE_ID, CHECK_ID, GUARDRAIL_ENGINE_ID, METHOD_NAME, PHASE, "
						+ "ACTION, ROOM_ID, INSIGHT_ID, USER_ID, USER_TYPE, DATE_CREATED) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				ps -> {
					int index = 1;
					ps.setString(index++, GUID.v7().toUUID().toString());
					ps.setString(index++, hit.engineId());
					ps.setString(index++, hit.checkId());
					ps.setString(index++, hit.guardrailEngineId());
					ps.setString(index++, hit.methodName());
					ps.setString(index++, hit.phase());
					ps.setString(index++, hit.action());
					ps.setString(index++, hit.roomId());
					ps.setString(index++, hit.insightId());
					ps.setString(index++, hit.user() == null ? null : hit.user().getValue0());
					ps.setString(index++, hit.user() == null ? null : hit.user().getValue1());
					ps.setTimestamp(index, at);
				});
	}

	/**
	 * Counts a room's hits of an engine's checks, check by check, in one query.
	 *
	 * @param engineId the engine
	 * @param roomId   the room
	 * @param checkIds the checks to count
	 * @return each check's hits; a check with none is left out
	 * @throws Exception when the hits cannot be read
	 */
	public static Map<String, Integer> countRoomHitsByCheck(String engineId, String roomId, Collection<String> checkIds)
			throws Exception {
		List<String> checks = new ArrayList<>(checkIds);
		Map<String, Integer> counts = new HashMap<>();
		for (Object[] row : QueryExecutionUtility.queryList(db(),
				"SELECT CHECK_ID, COUNT(*) FROM GUARDRAIL_HIT WHERE ENGINE_ID = ? AND ROOM_ID = ? AND CHECK_ID IN ("
						+ QueryExecutionUtility.placeholders(checks.size()) + ") GROUP BY CHECK_ID",
				ps -> {
					int index = 1;
					ps.setString(index++, engineId);
					ps.setString(index++, roomId);
					QueryExecutionUtility.bindAll(ps, index, checks);
				}, rs -> new Object[] { rs.getString(1), rs.getObject(2) })) {
			counts.put((String) row[0], ((Number) row[1]).intValue());
		}
		return counts;
	}

	/**
	 * A user's hits of an engine's checks since a time, each with its check and
	 * when it happened, in one query, so each limit can count the ones in its own
	 * window.
	 *
	 * @param engineId the engine
	 * @param user     the user's id and login type
	 * @param checkIds the checks to read
	 * @param since    the earliest hit to read, in UTC
	 * @return the hits
	 * @throws Exception when the hits cannot be read
	 */
	public static List<HitTime> userHitsSince(String engineId, Pair<String, String> user, Collection<String> checkIds,
			Timestamp since) throws Exception {
		List<String> checks = new ArrayList<>(checkIds);
		return QueryExecutionUtility.queryList(db(),
				"SELECT CHECK_ID, DATE_CREATED FROM GUARDRAIL_HIT WHERE ENGINE_ID = ? AND USER_ID = ? AND USER_TYPE = ? "
						+ "AND DATE_CREATED >= ? AND CHECK_ID IN (" + QueryExecutionUtility.placeholders(checks.size())
						+ ")",
				ps -> {
					int index = 1;
					ps.setString(index++, engineId);
					ps.setString(index++, user.getValue0());
					ps.setString(index++, user.getValue1());
					ps.setTimestamp(index++, since);
					QueryExecutionUtility.bindAll(ps, index, checks);
				}, rs -> new HitTime(rs.getString(1), rs.getTimestamp(2)));
	}

	/**
	 * When the user's latest lock on an engine started, whether it ran out or was
	 * cleared.
	 *
	 * @param engineId the engine
	 * @param user     the user's id and login type
	 * @return the start, in UTC, or null when the user was never locked out of it
	 * @throws Exception when the locks cannot be read
	 */
	public static Timestamp latestLockStart(String engineId, Pair<String, String> user) throws Exception {
		return QueryExecutionUtility.queryOne(db(),
				"SELECT MAX(LOCKED_AT) FROM GUARDRAIL_LOCK WHERE ENGINE_ID = ? AND USER_ID = ? AND USER_TYPE = ?",
				ps -> {
					ps.setString(1, engineId);
					ps.setString(2, user.getValue0());
					ps.setString(3, user.getValue1());
				}, rs -> rs.getTimestamp(1));
	}

	/**
	 * The user's lock on an engine that is in force: not ended and not cleared.
	 *
	 * @param engineId the engine
	 * @param user     the user's id and login type
	 * @param now      the current time, in UTC
	 * @return the lock ending last, or null when the user is not locked out
	 * @throws Exception when the locks cannot be read
	 */
	public static Lock activeLock(String engineId, Pair<String, String> user, Timestamp now) throws Exception {
		return QueryExecutionUtility.queryOne(db(),
				"SELECT " + LOCK_COLUMNS + " FROM GUARDRAIL_LOCK WHERE ENGINE_ID = ? AND USER_ID = ? "
						+ "AND USER_TYPE = ? AND LOCKED_UNTIL > ? AND CLEARED_AT IS NULL ORDER BY LOCKED_UNTIL DESC",
				ps -> {
					ps.setString(1, engineId);
					ps.setString(2, user.getValue0());
					ps.setString(3, user.getValue1());
					ps.setTimestamp(4, now);
				}, GuardrailHitStore::lock);
	}

	/**
	 * Every lock on an engine that is in force, ending soonest first.
	 *
	 * @param engineId the engine
	 * @param now      the current time, in UTC
	 * @return the locks
	 * @throws Exception when the locks cannot be read
	 */
	public static List<Lock> activeLocks(String engineId, Timestamp now) throws Exception {
		return QueryExecutionUtility.queryList(db(),
				"SELECT " + LOCK_COLUMNS + " FROM GUARDRAIL_LOCK WHERE ENGINE_ID = ? AND LOCKED_UNTIL > ? "
						+ "AND CLEARED_AT IS NULL ORDER BY LOCKED_UNTIL ASC",
				ps -> {
					ps.setString(1, engineId);
					ps.setTimestamp(2, now);
				}, GuardrailHitStore::lock);
	}

	/**
	 * Locks a user out of an engine.
	 *
	 * @param lock the lock
	 * @throws Exception when the lock cannot be written
	 */
	public static void insertLock(Lock lock) throws Exception {
		QueryExecutionUtility.executeUpdate(db(),
				"INSERT INTO GUARDRAIL_LOCK (LOCK_ID, " + LOCK_COLUMNS + ") VALUES (?, ?, ?, ?, ?, ?, ?)", ps -> {
					ps.setString(1, GUID.v7().toUUID().toString());
					ps.setString(2, lock.engineId());
					ps.setString(3, lock.limitId());
					ps.setString(4, lock.userId());
					ps.setString(5, lock.userType());
					ps.setTimestamp(6, lock.lockedAt());
					ps.setTimestamp(7, lock.lockedUntil());
				});
	}

	/**
	 * Ends a user's locks on an engine that are in force.
	 *
	 * @param engineId  the engine
	 * @param user      the locked user's id and login type
	 * @param clearedBy the id and login type of the user who cleared them
	 * @param now       the current time, in UTC
	 * @return true when a lock was in force and is now cleared
	 * @throws Exception when the locks cannot be written
	 */
	public static boolean clearLock(String engineId, Pair<String, String> user, Pair<String, String> clearedBy,
			Timestamp now) throws Exception {
		return QueryExecutionUtility.executeUpdate(db(),
				"UPDATE GUARDRAIL_LOCK SET CLEARED_AT = ?, CLEARED_BY_ID = ?, CLEARED_BY_TYPE = ? WHERE ENGINE_ID = ? "
						+ "AND USER_ID = ? AND USER_TYPE = ? AND LOCKED_UNTIL > ? AND CLEARED_AT IS NULL",
				ps -> {
					ps.setTimestamp(1, now);
					ps.setString(2, clearedBy.getValue0());
					ps.setString(3, clearedBy.getValue1());
					ps.setString(4, engineId);
					ps.setString(5, user.getValue0());
					ps.setString(6, user.getValue1());
					ps.setTimestamp(7, now);
				}) > 0;
	}

	/**
	 * Ends an engine's locks in force whose limit is not one of the given ones, as
	 * when a save removes a user limit, so no user stays locked out by a limit that
	 * is gone. With no model inference database there are no locks to end.
	 *
	 * @param engineId     the engine
	 * @param keptLimitIds the ids of the user limits the engine still has
	 * @param clearedBy    the id and login type of the user who saved
	 * @param now          the current time, in UTC
	 * @return the locks ended
	 * @throws Exception when the locks cannot be read or written
	 */
	public static List<Lock> clearLocksOfRemovedLimits(String engineId, Collection<String> keptLimitIds,
			Pair<String, String> clearedBy, Timestamp now) throws Exception {
		if (!SystemEngineRegistry.isModelInferenceLogsDbLoaded()) {
			return List.of();
		}
		List<String> kept = new ArrayList<>(keptLimitIds);
		String inForce = " WHERE ENGINE_ID = ? AND LOCKED_UNTIL > ? AND CLEARED_AT IS NULL" + (kept.isEmpty() ? ""
				: " AND LIMIT_ID NOT IN (" + QueryExecutionUtility.placeholders(kept.size()) + ")");
		return QueryExecutionUtility.write(db(), connection -> {
			List<Lock> locks = QueryExecutionUtility.queryList(connection,
					"SELECT " + LOCK_COLUMNS + " FROM GUARDRAIL_LOCK" + inForce, ps -> {
						ps.setString(1, engineId);
						ps.setTimestamp(2, now);
						QueryExecutionUtility.bindAll(ps, 3, kept);
					}, GuardrailHitStore::lock);
			if (!locks.isEmpty()) {
				QueryExecutionUtility.executeUpdate(connection,
						"UPDATE GUARDRAIL_LOCK SET CLEARED_AT = ?, CLEARED_BY_ID = ?, CLEARED_BY_TYPE = ?" + inForce,
						ps -> {
							ps.setTimestamp(1, now);
							ps.setString(2, clearedBy.getValue0());
							ps.setString(3, clearedBy.getValue1());
							ps.setString(4, engineId);
							ps.setTimestamp(5, now);
							QueryExecutionUtility.bindAll(ps, 6, kept);
						});
			}
			return locks;
		});
	}

	private static Lock lock(ResultSet rs) throws SQLException {
		return new Lock(rs.getString("ENGINE_ID"), rs.getString("LIMIT_ID"), rs.getString("USER_ID"),
				rs.getString("USER_TYPE"), rs.getTimestamp("LOCKED_AT"), rs.getTimestamp("LOCKED_UNTIL"));
	}

	private static IRDBMSEngine db() {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			throw new IllegalStateException("The model inference database is not available for guardrail hit limits");
		}
		return db;
	}
}
