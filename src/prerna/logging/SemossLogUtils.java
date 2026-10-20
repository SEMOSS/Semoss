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
package prerna.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.LocationAware;

public class SemossLogUtils {

	public static final String LOG_ID = "logId";
	public static final String REQUEST_ID = "requestId";

	public static final String USER_ID = "userId";
	public static final String USER_TYPE = "userType";
	public static final String USER_NAME = "userName";
	public static final String SESSION_ID = "sessionId";
	public static final String CLIENT_IP = "clientIP";

	public static final String SERVICE_NAME = "serviceName";
	public static final String METHOD = "method";
	public static final String ENDPOINT = "endpoint";
	public static final String HOST = "host";
	public static final String USER_AGENT = "userAgent";

	public static final String IS_SUCCESS = "isSuccess";
	public static final String FILTER_NAME = "filterName";
	public static final String ENGINE_ID = "engineId";
	public static final String ENGINE_NAME = "engineName";
	public static final String ENGINE_TYPE = "engineType";
	public static final String ENGINE_SUBTYPE = "engineSubtype";

	public static final String PROJECT_ID = "projectId";
	public static final String PROJECT_NAME = "projectName";
	public static final String INSIGHT_ID = "insightId";
	public static final String ROOM_ID = "roomId";

	public static final String MESSAGE_ID = "messageId";
	public static final String MESSAGE_TYPE = "messageType";
	public static final String NUMBER_OF_TOKENS_IN_PROMPT = "numberOfTokensInPrompt";
	public static final String NUMBER_OF_TOKENS_IN_RESPONSE = "numberOfTokensInResponse";
	// cached-token breakdown (subsets of prompt tokens) for cost tracking
	public static final String NUMBER_OF_CACHE_READ_TOKENS = "numberOfCacheReadTokens";
	public static final String NUMBER_OF_CACHE_CREATION_TOKENS = "numberOfCacheCreationTokens";
	public static final String REQUEST = "request";
	public static final String RESPONSE = "response";
	public static final String METHOD_NAME = "methodName";
	public static final String TIMESTAMP = "timestamp";
	public static final String LEVEL = "logLevel";
	public static final String MESSAGE = "logMessage";
	public static final String REACTOR_SPAN_ID = "reactorSpanId";
	public static final String REACTOR_NAME = "reactorName";
	public static final String SPAN_ID = "spanId";
	public static final String INPUT_REACTOR_NAME = "inputReactorName";
	public static final String OUTPUT_REACTOR_NAME = "outputReactorName";
	// notable action a guardrail took on a row: MASK / BLOCK
	// (null when it ran clean)
	public static final String GUARDRAIL_ACTION = "guardrailAction";
	// the usage limit source a model call was billed to, as the inference logs
	// record it: USER, TEAM, PUBLIC, ALL_MODELS or PLATFORM, with the person's
	// login or the team billed; null when no limit was billed
	public static final String BILLED_SOURCE_TYPE = "billedSourceType";
	public static final String BILLED_SUBJECT_ID = "billedSubjectId";
	public static final String BILLED_SUBJECT_TYPE = "billedSubjectType";
	public static final String REQUEST_START_TIME = "requestStartTime";
	public static final String RESPONSE_END_TIME = "responseEndTime";

	// filters for AuditLogsReport
	public static final String DATE_RANGE_TYPE = "dateRangeType";
	public static final String START_DATE = "startDate";
	public static final String END_DATE = "endDate";
	public static final String DATE_RANGE_VALUE = "dateRangeValue";
	public static final String FILTER_USER_ID = "filterUserId";

	// SecurityAuditLogsJDBCAppender fields - logins, permission changes, admin
	// actions, not the engine-invocation fields above
	public static final String EVENT_TYPE = "eventType";
	public static final String ACTOR_USER_ID = "actorUserId";
	public static final String ACTOR_USER_NAME = "actorUserName";
	public static final String ACTOR_IP = "actorIp";
	public static final String TARGET_TYPE = "targetType";
	public static final String TARGET_ID = "targetId";
	public static final String TARGET_NAME = "targetName";
	public static final String DETAILS = "details";

	public static final String SECURITY_AUDIT_LOGGER_NAME = "SecurityAuditLogger";

	/**
	 * Get the engine level logger
	 *
	 * @return
	 */
	public static Logger getEngineLevelLogger() {
		return LogManager.getLogger("EngineLogger");
	}

	/**
	 * Get the security audit logger - for logins, permission/role/group changes
	 * and admin actions. Kept separate from {@link #getEngineLevelLogger()},
	 * whose AUDIT_LOGS table is shaped for LLM pipeline invocations (engine id,
	 * token counts, guardrail actions), not security events.
	 *
	 * @return
	 */
	public static Logger getSecurityAuditLogger() {
		return LogManager.getLogger(SECURITY_AUDIT_LOGGER_NAME);
	}

	/**
	 * Log a security-relevant event (login, permission/role/group change, admin
	 * action) to the security audit trail. Routes through log4j2's MDC, the same
	 * mechanism {@link prerna.engine.impl.pipeline.PipelineInvocationHandler}
	 * uses, so {@link SecurityAuditLogsJDBCAppender} can read the fields back off
	 * the event's context data.
	 *
	 * @param eventType    a short, stable event name, e.g. "LOGIN_SUCCESS",
	 *                     "LOGIN_FAILURE", "PERMISSION_GRANTED",
	 *                     "PERMISSION_REVOKED", "ADMIN_ACTION"
	 * @param actorUserId  the id of the user who performed the action (null if
	 *                     unauthenticated, e.g. a failed login)
	 * @param actorUserName the actor's display/login name, for a human-readable
	 *                      record even if the id later becomes unresolvable
	 * @param actorIp      the actor's source IP, or null if unavailable
	 * @param targetType   what kind of thing was acted on, e.g. "USER", "GROUP",
	 *                     "PROJECT", "ENGINE", "PERMISSION_BATCH" (null if n/a,
	 *                     e.g. a login event)
	 * @param targetId     the id of the thing acted on (null if n/a)
	 * @param targetName   a human-readable name for the target (null if n/a)
	 * @param success      whether the action succeeded
	 * @param message      a short human-readable description, e.g.
	 *                     {@code "add user jdoe to group Sales"}
	 * @param detailsJson  optional extra detail as a JSON string (e.g. the
	 *                     before/after permission values), or null
	 */
	public static void logSecurityAuditEvent(String eventType, String actorUserId, String actorUserName,
			String actorIp, String targetType, String targetId, String targetName, boolean success, String message,
			String detailsJson) {
		java.util.Map<String, String> mdc = new java.util.HashMap<>(
				org.apache.logging.log4j.ThreadContext.getImmutableContext());
		mdc.put(EVENT_TYPE, eventType);
		putIfNotNull(mdc, ACTOR_USER_ID, actorUserId);
		putIfNotNull(mdc, ACTOR_USER_NAME, actorUserName);
		putIfNotNull(mdc, ACTOR_IP, actorIp);
		putIfNotNull(mdc, TARGET_TYPE, targetType);
		putIfNotNull(mdc, TARGET_ID, targetId);
		putIfNotNull(mdc, TARGET_NAME, targetName);
		mdc.put(IS_SUCCESS, Boolean.toString(success));
		putIfNotNull(mdc, DETAILS, detailsJson);

		try (org.apache.logging.log4j.CloseableThreadContext.Instance ctc = org.apache.logging.log4j.CloseableThreadContext
				.putAll(mdc)) {
			Logger logger = getSecurityAuditLogger();
			if (success) {
				logger.info(message);
			} else {
				logger.warn(message);
			}
		}
	}

	private static void putIfNotNull(java.util.Map<String, String> mdc, String key, String value) {
		if (value != null) {
			mdc.put(key, value);
		}
	}

	/**
	 * Safely extracts source information from a LogEvent and appends it to a
	 * StringBuilder. Handles cases where includeLocation="false" is set on the
	 * appender.
	 * 
	 * @param event  The LogEvent to extract source information from
	 * @param buffer
	 * @return
	 */
	public static String appendSourceInfo(LogEvent event) {
		StringBuilder builder = new StringBuilder();
		try {
			// Check if the event implements LocationAware and has location info
			if (event instanceof LocationAware) {
				LocationAware locationAware = (LocationAware) event;
				if (!locationAware.requiresLocation()) {
					builder.append("location:unavailable");
					return builder.toString();
				}
			}

			// Attempt to get the source location
			StackTraceElement source = event.getSource();

			if (source != null) {
				// Extract and append source information
				String className = source.getClassName();
				String methodName = source.getMethodName();
				int lineNumber = source.getLineNumber();

				// Append class name (simple name only)
				if (className != null) {
					builder.append(className);
				} else {
					builder.append("unknown");
				}

				// Append method name
				if (methodName != null) {
					builder.append(".").append(methodName);
				} else {
					builder.append(".unknown");
				}

				if (lineNumber > 0) {
					builder.append(":" + lineNumber);
				} else {
					builder.append(":unknown");
				}
			} else {
				// Source is null - location information not available
				builder.append("location:unavailable");
			}

		} catch (UnsupportedOperationException e) {
			// This can happen when location is disabled
			builder.append("location:disabled");
		} catch (Exception e) {
			// Handle any other unexpected exceptions
			builder.append("location:error - ").append(e.getMessage());
		}

		return builder.toString();
	}

}
