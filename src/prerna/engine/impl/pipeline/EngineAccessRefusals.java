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

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import prerna.auth.User;
import prerna.om.ThreadStore;
import prerna.usertracking.AuditErrorCode;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditStatus;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;

/**
 * Counts the calls the access check refuses each person on each engine or
 * project in a session, so the audit trail is not flooded by retries. The first
 * refusal in a session records an {@code AUTHORIZATION_DENIED} audit event with
 * what was sent, which the audit trail keeps sanitized like any event's
 * details, and which only admins see; every refusal, that one included, is a
 * row on the Activity Log with its attempt number. A call made with no session
 * is counted per day.
 */
final class EngineAccessRefusals {

	private static final Logger classLogger = LogManager.getLogger(EngineAccessRefusals.class);

	/** How long a count is kept after its last refusal */
	private static final Duration KEPT_FOR = Duration.ofDays(1);
	/** The most counts kept; the least recently refused are let go first */
	private static final long MAX_COUNTS = 100_000;

	private static final Cache<String, AtomicInteger> ATTEMPTS = Caffeine.newBuilder().expireAfterAccess(KEPT_FOR)
			.maximumSize(MAX_COUNTS).build();

	private EngineAccessRefusals() {
	}

	/**
	 * Counts a refused call, and records the first of the person's session on the
	 * engine or project on the audit trail.
	 *
	 * @param user     the person the call was made for, or null when it was made
	 *                 for none
	 * @param targetId the engine or project
	 * @param project  whether it is a project
	 * @param method   the method called
	 * @param args     the call's arguments, or null
	 * @param refusal  the refusal
	 * @return how many times the person was refused on the engine or project in
	 *         this session, this call included
	 */
	static int record(User user, String targetId, boolean project, Method method, Object[] args,
			EngineAccessRefusedException refusal) {
		int attempt = ATTEMPTS.get(key(user, targetId, project), key -> new AtomicInteger()).incrementAndGet();
		if (attempt == 1) {
			recordAuditEvent(user, targetId, project, method, args, refusal);
		}
		return attempt;
	}

	/**
	 * The person, the engine or project, and the session or day a count is for.
	 *
	 * @param user     the person, or null
	 * @param targetId the engine or project
	 * @param project  whether it is a project
	 * @return the key
	 */
	static String key(User user, String targetId, boolean project) {
		String person = "no user";
		if (user != null && user.getPrimaryLogin() != null) {
			Pair<String, String> idAndType = User.getPrimaryUserIdAndTypePair(user);
			person = idAndType.getValue0() + "|" + idAndType.getValue1();
		}
		String session = ThreadStore.getSessionId();
		String period = session == null || session.isBlank() ? "day:" + LocalDate.now(ZoneOffset.UTC)
				: "session:" + session;
		return person + "|" + (project ? "P:" : "E:") + targetId + "|" + period;
	}

	/**
	 * Records the refusal on the audit trail: the method, the level it needed, who
	 * made the call, and what was sent.
	 */
	private static void recordAuditEvent(User user, String targetId, boolean project, Method method, Object[] args,
			EngineAccessRefusedException refusal) {
		try {
			Map<String, Object> details = new LinkedHashMap<>(refusal.getDetails());
			details.put("method", method.getName());
			String request = requestOf(method, args);
			if (request != null) {
				details.put("requestLength", request.length());
				details.put("request", request);
			}
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent()
					.eventType(AuditEventType.AUTHORIZATION_DENIED).action(method.getName()).status(AuditStatus.DENIED)
					.actorUser(user).target(project ? AuditTargetType.PROJECT : AuditTargetType.ENGINE, targetId, null)
					.context(project ? targetId : null, project ? null : targetId, ThreadStore.getInsightId(), null)
					.error(AuditErrorCode.PERMISSION_DENIED, refusal.getMessage()).details(details));
		} catch (RuntimeException e) {
			classLogger.warn("Could not record the refused call to {} on {} on the audit trail", method.getName(),
					targetId, e);
		}
	}

	/**
	 * What a refused call sent, as the Activity Log writes a call's request.
	 *
	 * @param method the method called
	 * @param args   the call's arguments, or null
	 * @return the request as JSON, or null when it cannot be written out
	 */
	private static String requestOf(Method method, Object[] args) {
		try {
			return PipelineInvocationHandler.serializeAuditPayload(method, args == null ? new Object[0] : args, null,
					false);
		} catch (RuntimeException e) {
			classLogger.warn("Could not write out the request of the refused call to {}", method.getName(), e);
			return null;
		}
	}

}
