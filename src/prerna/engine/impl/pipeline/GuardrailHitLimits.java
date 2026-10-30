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
package prerna.engine.impl.pipeline;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.inferencetracking.GuardrailHitStore;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.usertracking.GuardrailAudit;
import prerna.util.Utility;

/**
 * Enforces an engine's guardrail hit limits. Before any check runs, every call
 * to the engine is refused when the user is locked out of it, or its room is
 * closed or has reached a room limit, so no guardrail runs for it. When a check
 * flags a call and a limit counts that check, the hit is stored and counted;
 * each limit it reaches closes the room, or locks the user out and closes the
 * room, and the call is refused. When the hits or locks cannot be read or
 * written, the call is refused rather than let through unchecked.
 */
final class GuardrailHitLimits {

	private static final Logger classLogger = LogManager.getLogger(GuardrailHitLimits.class);

	static final GuardrailHitLimits NONE = new GuardrailHitLimits(List.of());

	static final String CLOSED_ROOM_MESSAGE = "The room being referenced has been permanently closed. Please open a new room";
	static final String ROOM_LIMIT_MESSAGE = "This room was closed by a guardrail. Please open a new room.";
	static final String UNCHECKED_MESSAGE = "This engine's guardrail limits could not be checked, so the call was refused.";
	private static final DateTimeFormatter LOCK_END_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm 'UTC'");

	private final List<GuardrailHitLimit> limits;

	GuardrailHitLimits(List<GuardrailHitLimit> limits) {
		this.limits = List.copyOf(limits);
	}

	boolean isEmpty() {
		return this.limits.isEmpty();
	}

	/** Whether some limit counts a check's hits */
	boolean counts(String checkId) {
		return this.limits.stream().anyMatch(limit -> limit.checks().contains(checkId));
	}

	List<GuardrailHitLimit> limits() {
		return this.limits;
	}

	/**
	 * Refuses a call before any check runs when the user is locked out of the
	 * engine, or the call's room is closed or has reached a room limit. A room that
	 * reached its limit is closed again, in case closing it failed before. At most
	 * three queries: the lock, the room, and the room's hits, check by check.
	 *
	 * @param call    the guarded call
	 * @param insight the insight the call runs in, or null
	 * @param user    the caller's id and login type, or null with no signed in
	 *                user, who can be neither locked out nor own a room
	 * @throws SemossPixelException refusing the call
	 */
	void refuseIfLockedOrRoomClosed(GuardrailAudit.Call call, Insight insight, Pair<String, String> user) {
		if (user == null) {
			return;
		}
		try {
			if (!ofScope(GuardrailHitLimit.Scope.USER, this.limits).isEmpty()) {
				GuardrailHitStore.Lock lock = GuardrailHitStore.activeLock(call.engineId(), user,
						Utility.getCurrentSqlTimestampUTC());
				if (lock != null) {
					closeRoom(call.roomId(), user);
					GuardrailAudit.recordLockedRequest(insight, call, lock.limitId(), lock.lockedUntil());
					throw new SemossPixelException(lockMessage(limit(lock.limitId()), call, lock.lockedUntil()));
				}
			}
			List<GuardrailHitLimit> roomLimits = ofScope(GuardrailHitLimit.Scope.ROOM, this.limits);
			if (call.roomId() == null || roomLimits.isEmpty()) {
				return;
			}
			if (ModelInferenceLogsUtils.isRoomInActive(user.getValue0(), call.roomId())) {
				throw new SemossPixelException(CLOSED_ROOM_MESSAGE);
			}
			List<Reached> reached = reachedRoomLimits(call, roomLimits);
			if (!reached.isEmpty()) {
				closeRoom(call.roomId(), user);
				throw new SemossPixelException(roomMessage(reached.get(0).limit()));
			}
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to check the guardrail hit limits of engine {}", call.engineId(), e);
			throw new SemossPixelException(UNCHECKED_MESSAGE, e);
		}
	}

	/**
	 * Stores a check's hit and counts it toward each limit that names the check.
	 * Every limit the hit reaches applies: a user limit locks the user out, and any
	 * limit reached closes the room, so a hit that reaches a room limit and a user
	 * limit at once does both. Then the call is refused, saying when the lock ends
	 * when there is one. At most four queries however many limits count the check:
	 * the hit, the room's hits, the user's latest lock, and the user's hits.
	 *
	 * @param call              the guarded call
	 * @param insight           the insight the call runs in, or null
	 * @param user              the caller's id and login type, or null with no
	 *                          signed in user
	 * @param checkId           the check that flagged the call
	 * @param guardrailEngineId the guardrail engine the check ran
	 * @param phase             INPUT or OUTPUT
	 * @param action            what the check did: BLOCK, MASK or RESPOND
	 * @throws SemossPixelException refusing the call when it reaches a limit
	 */
	void countHit(GuardrailAudit.Call call, Insight insight, Pair<String, String> user, String checkId,
			String guardrailEngineId, String phase, String action) {
		List<GuardrailHitLimit> counting = this.limits.stream().filter(limit -> limit.checks().contains(checkId))
				.toList();
		if (counting.isEmpty()) {
			return;
		}
		try {
			Timestamp now = Utility.getCurrentSqlTimestampUTC();
			GuardrailHitStore
					.recordHit(new GuardrailHitStore.Hit(call.engineId(), checkId, guardrailEngineId, call.methodName(),
							phase, action, call.roomId(), insight == null ? null : insight.getInsightId(), user), now);
			List<Reached> reachedRooms = call.roomId() == null ? List.of()
					: reachedRoomLimits(call, ofScope(GuardrailHitLimit.Scope.ROOM, counting));
			List<Reached> reachedUsers = user == null ? List.of()
					: reachedUserLimits(call, user, ofScope(GuardrailHitLimit.Scope.USER, counting), now);
			if (reachedRooms.isEmpty() && reachedUsers.isEmpty()) {
				return;
			}

			GuardrailHitLimit lockingLimit = null;
			Timestamp lockedUntil = null;
			if (user != null) {
				for (Reached reached : reachedUsers) {
					GuardrailHitLimit limit = reached.limit();
					Timestamp until = new Timestamp(now.getTime() + limit.lockMinutes() * 60_000L);
					GuardrailHitStore.insertLock(new GuardrailHitStore.Lock(call.engineId(), limit.limitId(),
							user.getValue0(), user.getValue1(), now, until));
					GuardrailAudit.recordUserLock(insight, call, limit.limitId(), reached.hits(), limit.hits(),
							limit.windowMinutes(), limit.lockMinutes(), until);
					if (lockedUntil == null || until.after(lockedUntil)) {
						lockingLimit = limit;
						lockedUntil = until;
					}
				}
			}
			closeRoom(call.roomId(), user);
			for (Reached reached : reachedRooms) {
				GuardrailAudit.recordRoomClose(insight, call, reached.limit().limitId(), reached.hits(),
						reached.limit().hits());
			}
			throw new SemossPixelException(lockingLimit != null ? lockMessage(lockingLimit, call, lockedUntil)
					: roomMessage(reachedRooms.get(0).limit()));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to count a guardrail hit for engine {}", call.engineId(), e);
			throw new SemossPixelException(UNCHECKED_MESSAGE, e);
		}
	}

	/** A limit a room's or a user's hits have reached, with the hits it counted */
	private record Reached(GuardrailHitLimit limit, int hits) {
	}

	/**
	 * The room limits the call's room has reached, from one query of its hits,
	 * check by check.
	 */
	private static List<Reached> reachedRoomLimits(GuardrailAudit.Call call, List<GuardrailHitLimit> roomLimits)
			throws Exception {
		if (roomLimits.isEmpty()) {
			return List.of();
		}
		Map<String, Integer> hitsByCheck = GuardrailHitStore.countRoomHitsByCheck(call.engineId(), call.roomId(),
				checksOf(roomLimits));
		List<Reached> reached = new ArrayList<>();
		for (GuardrailHitLimit limit : roomLimits) {
			int hits = limit.checks().stream().mapToInt(check -> hitsByCheck.getOrDefault(check, 0)).sum();
			if (hits >= limit.hits()) {
				reached.add(new Reached(limit, hits));
			}
		}
		return reached;
	}

	/**
	 * The user limits the user's hits have reached, each counting the hits in its
	 * own window, from one query of the hits since the earliest window. Hits from
	 * before the user's latest lock on the engine are left out, since a lock clears
	 * the count.
	 */
	private static List<Reached> reachedUserLimits(GuardrailAudit.Call call, Pair<String, String> user,
			List<GuardrailHitLimit> userLimits, Timestamp now) throws Exception {
		if (userLimits.isEmpty()) {
			return List.of();
		}
		Timestamp latestLock = GuardrailHitStore.latestLockStart(call.engineId(), user);
		Map<GuardrailHitLimit, Timestamp> sinceByLimit = new LinkedHashMap<>();
		Timestamp earliest = null;
		for (GuardrailHitLimit limit : userLimits) {
			Timestamp since = new Timestamp(now.getTime() - limit.windowMinutes() * 60_000L);
			if (latestLock != null && !latestLock.before(since)) {
				// the hits that reached that lock were stored at its start
				since = new Timestamp(latestLock.getTime() + 1);
			}
			sinceByLimit.put(limit, since);
			if (earliest == null || since.before(earliest)) {
				earliest = since;
			}
		}
		List<GuardrailHitStore.HitTime> hits = GuardrailHitStore.userHitsSince(call.engineId(), user,
				checksOf(userLimits), earliest);
		List<Reached> reached = new ArrayList<>();
		for (Map.Entry<GuardrailHitLimit, Timestamp> entry : sinceByLimit.entrySet()) {
			GuardrailHitLimit limit = entry.getKey();
			Timestamp since = entry.getValue();
			int count = (int) hits.stream()
					.filter(hit -> limit.checks().contains(hit.checkId()) && !hit.at().before(since)).count();
			if (count >= limit.hits()) {
				reached.add(new Reached(limit, count));
			}
		}
		return reached;
	}

	private static List<GuardrailHitLimit> ofScope(GuardrailHitLimit.Scope scope, List<GuardrailHitLimit> limits) {
		return limits.stream().filter(limit -> limit.scope() == scope).toList();
	}

	private static Set<String> checksOf(List<GuardrailHitLimit> limits) {
		Set<String> checks = new LinkedHashSet<>();
		for (GuardrailHitLimit limit : limits) {
			checks.addAll(limit.checks());
		}
		return checks;
	}

	private GuardrailHitLimit limit(String limitId) {
		return this.limits.stream().filter(limit -> limit.limitId().equals(limitId)).findFirst().orElse(null);
	}

	/** Closes a room its owner, the caller, has open */
	private static void closeRoom(String roomId, Pair<String, String> user) {
		if (roomId == null || user == null) {
			return;
		}
		if (ModelInferenceLogsUtils.doSetRoomToInactive(user.getValue0(), roomId)) {
			RoomMessageStore.evictRedisProjection(roomId);
			classLogger.warn("Guardrail hit limits closed room {}", roomId);
		}
	}

	private static String roomMessage(GuardrailHitLimit limit) {
		return limit.message() != null ? limit.message() : ROOM_LIMIT_MESSAGE;
	}

	static String lockMessage(GuardrailHitLimit limit, GuardrailAudit.Call call, Timestamp lockedUntil) {
		if (limit != null && limit.message() != null) {
			return limit.message();
		}
		return "You can't use " + call.engineName() + " until " + LOCK_END_FORMAT.format(lockedUntil.toLocalDateTime())
				+ " after repeated guardrail violations.";
	}
}
