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
package prerna.usertracking;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.util.Utility;

/**
 * Records what guardrails do to engine calls in {@code USER_AUDIT_EVENTS}: each
 * check that flags a call, whether it blocks, masks or answers in the engine's
 * place, and what the engine's hit limits do: closing a room, locking a user
 * out, refusing a locked out user's call, and an editor clearing a lock. The
 * event is on the guarded engine, so its owners see it on the engine's Audit
 * tab. It holds the values the check screened only when the engine keeps its
 * input and output ({@link IEngine#keepInputOutput()}). Recording never throws,
 * so a failed audit write leaves the call's outcome as the guardrail decided
 * it.
 */
public final class GuardrailAudit {

	private static final Logger classLogger = LogManager.getLogger(GuardrailAudit.class);

	/** What a guardrail check did with a call it flagged */
	public enum Action {
		/** The call was stopped */
		BLOCK,
		/** The call's input was masked and the call ran */
		MASK,
		/** The guardrail's message was returned in place of the call */
		RESPOND
	}

	/**
	 * The guarded engine call an event is about.
	 *
	 * @param engineId        the guarded engine
	 * @param engineName      the guarded engine's name
	 * @param engineType      the guarded engine's catalog type
	 * @param methodName      the engine method called, such as askRoom
	 * @param roomId          the room the call ran in, or null
	 * @param keepInputOutput whether the engine keeps its input and output, which
	 *                        lets an event hold what a check screened
	 */
	public record Call(String engineId, String engineName, IEngine.CATALOG_TYPE engineType, String methodName,
			String roomId, boolean keepInputOutput) {
	}

	/**
	 * One guardrail check that flagged a call.
	 *
	 * @param checkId             the check's id in the engine's pipeline, or null
	 *                            for a check saved before checks had ids
	 * @param guardrailEngineId   the guardrail engine the check ran
	 * @param guardrailEngineName the guardrail engine's name
	 * @param phase               INPUT for a request check, OUTPUT for a response
	 *                            check
	 * @param action              what the check did
	 * @param screened            the values the check handed its guardrail engine,
	 *                            kept only when the engine keeps its input and
	 *                            output
	 */
	public record Hit(String checkId, String guardrailEngineId, String guardrailEngineName, String phase, Action action,
			Object screened) {
	}

	private GuardrailAudit() {
	}

	/**
	 * Records a guardrail check flagging a call. Never throws.
	 *
	 * @param insight the insight the call ran in, which gives the actor, or null
	 * @param call    the guarded call
	 * @param hit     the check and what it did
	 */
	public static void recordHit(Insight insight, Call call, Hit hit) {
		if (call == null || hit == null) {
			return;
		}
		Map<String, Object> details = new LinkedHashMap<>();
		putText(details, "checkId", hit.checkId());
		putText(details, "guardrailEngineId", hit.guardrailEngineId());
		putText(details, "guardrailEngineName", hit.guardrailEngineName());
		putText(details, "methodName", call.methodName());
		putText(details, "phase", hit.phase());
		details.put("action", hit.action().name());
		if (call.keepInputOutput() && hit.screened() != null) {
			details.put("screened", hit.screened());
		}
		// DENIED is kept for access refusals, which the audit trail names Access Denied
		record(insight, call, eventType(hit.action()),
				hit.action() == Action.BLOCK ? AuditStatus.FAILURE : AuditStatus.SUCCESS, details);
	}

	/**
	 * Records a room closed after its hits reached a limit. Never throws.
	 *
	 * @param insight   the insight the call ran in, or null
	 * @param call      the call whose hit reached the limit
	 * @param limitId   the limit
	 * @param hits      the room's hits that the limit counts
	 * @param threshold the hits that reach the limit
	 */
	public static void recordRoomClose(Insight insight, Call call, String limitId, int hits, int threshold) {
		Map<String, Object> details = limitDetails(call, limitId, "ROOM", hits, threshold);
		record(insight, call, AuditEventType.GUARDRAIL_ROOM_CLOSE, AuditStatus.SUCCESS, details);
	}

	/**
	 * Records a user locked out of an engine after their hits reached a limit.
	 * Never throws.
	 *
	 * @param insight       the insight the call ran in, which gives the user
	 * @param call          the call whose hit reached the limit
	 * @param limitId       the limit
	 * @param hits          the user's hits in the window that the limit counts
	 * @param threshold     the hits that reach the limit
	 * @param windowMinutes how far back the limit counts hits
	 * @param lockMinutes   how long the user is locked out
	 * @param lockedUntil   when the lock ends, in UTC
	 */
	public static void recordUserLock(Insight insight, Call call, String limitId, int hits, int threshold,
			int windowMinutes, int lockMinutes, Timestamp lockedUntil) {
		Map<String, Object> details = limitDetails(call, limitId, "USER", hits, threshold);
		details.put("windowMinutes", windowMinutes);
		details.put("lockMinutes", lockMinutes);
		details.put("lockedUntil", Utility.getSqlTimestampToString(lockedUntil));
		record(insight, call, AuditEventType.GUARDRAIL_USER_LOCK, AuditStatus.SUCCESS, details);
	}

	/**
	 * Records a call refused because the user is locked out of the engine. Never
	 * throws.
	 *
	 * @param insight     the insight the call ran in, which gives the user
	 * @param call        the refused call
	 * @param limitId     the limit that locked the user out
	 * @param lockedUntil when the lock ends, in UTC
	 */
	public static void recordLockedRequest(Insight insight, Call call, String limitId, Timestamp lockedUntil) {
		Map<String, Object> details = new LinkedHashMap<>();
		putText(details, "methodName", call.methodName());
		putText(details, "limitId", limitId);
		details.put("lockedUntil", Utility.getSqlTimestampToString(lockedUntil));
		record(insight, call, AuditEventType.GUARDRAIL_LOCKED_REQUEST, AuditStatus.FAILURE, details);
	}

	/**
	 * Records an engine's editor ending a user's lock early. Never throws.
	 *
	 * @param editor     the user who cleared the lock
	 * @param engineId   the engine
	 * @param engineName the engine's name
	 * @param engineType the engine's catalog type
	 * @param lockedUser the locked user's id and login type
	 */
	public static void recordLockClear(User editor, String engineId, String engineName, IEngine.CATALOG_TYPE engineType,
			Pair<String, String> lockedUser) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		try {
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent()
					.eventType(AuditEventType.GUARDRAIL_LOCK_CLEAR).status(AuditStatus.SUCCESS).actorUser(editor)
					.subject(lockedUser.getValue0(), lockedUser.getValue1(), null).subjectNameLookup(true)
					.target(AuditTargetType.ofEngine(engineType), engineId, engineName)
					.context(null, engineId, null, null));
		} catch (Exception e) {
			classLogger.warn("Unable to record the {} audit event for engine {}", AuditEventType.GUARDRAIL_LOCK_CLEAR,
					engineId, e);
		}
	}

	private static Map<String, Object> limitDetails(Call call, String limitId, String scope, int hits, int threshold) {
		Map<String, Object> details = new LinkedHashMap<>();
		putText(details, "methodName", call.methodName());
		putText(details, "limitId", limitId);
		details.put("scope", scope);
		details.put("hits", hits);
		details.put("threshold", threshold);
		return details;
	}

	private static AuditEventType eventType(Action action) {
		return switch (action) {
		case BLOCK -> AuditEventType.GUARDRAIL_BLOCK;
		case MASK -> AuditEventType.GUARDRAIL_MASK;
		case RESPOND -> AuditEventType.GUARDRAIL_RESPOND;
		};
	}

	private static void record(Insight insight, Call call, AuditEventType eventType, AuditStatus status,
			Map<String, Object> details) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		try {
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType(eventType).status(status)
					.actorUser(insight == null ? null : insight.getUser())
					.target(AuditTargetType.ofEngine(call.engineType()), call.engineId(), call.engineName())
					.context(insight == null ? null : insight.getContextProjectId(), call.engineId(),
							insight == null ? null : insight.getInsightId(), call.roomId())
					.details(details));
		} catch (Exception e) {
			classLogger.warn("Unable to record the {} audit event for engine {}", eventType, call.engineId(), e);
		}
	}

	private static void putText(Map<String, Object> details, String key, String value) {
		if (value != null && !value.isBlank()) {
			details.put(key, value);
		}
	}
}
