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

/**
 * Timeline projection of audit log activity.
 * <p>
 * The raw values are written to {@code AUDIT_LOGS} by
 * {@link prerna.logging.AuditLogsJDBCAppender}, using MDC/message data
 * populated by {@link prerna.engine.impl.pipeline.PipelineInvocationHandler}.
 * When records are read, {@link prerna.engine.logging.AuditLogsDbUtils}
 * enriches each row with request-level timing aggregates (min start, max end,
 * and duration).
 *
 * @param requestId    Correlation id ({@code requestId}) used to group all log
 *                     rows for a request.
 * @param startTime    Earliest {@code requestStartTime} found for
 *                     {@code requestId} (request-level aggregate from
 *                     {@code MIN(REQUEST_START_TIME)}).
 * @param endTime      Latest {@code responseEndTime} found for
 *                     {@code requestId} (request-level aggregate from
 *                     {@code MAX(RESPONSE_END_TIME)}).
 * @param request      Serialized request payload captured for this row. May be
 *                     {@code REQUEST NOT TRACKED} when input/output tracking is
 *                     disabled.
 * @param response     Serialized response payload captured for this row. May be
 *                     {@code RESPONSE NOT TRACKED} when input/output tracking
 *                     is disabled.
 * @param tokens       Sum of
 *                     {@code numberOfTokensInPrompt + numberOfTokensInResponse}
 *                     for this row.
 * @param latency      Request-level latency in seconds for {@code requestId},
 *                     computed from
 *                     {@code DATEDIFF(second, MIN(REQUEST_START_TIME), MAX(RESPONSE_END_TIME))}.
 * @param status       Row-level success flag from {@code isSuccess}.
 * @param engineName   Engine display name ({@code engineName}) from logging
 *                     context.
 * @param engineType   Engine catalog type ({@code engineType}) from logging
 *                     context.
 * @param methodName   Invoked engine method name ({@code methodName}).
 * @param userId       User identifier ({@code userId}) associated with the
 *                     event.
 * @param sessionId    Session identifier ({@code sessionId}) associated with
 *                     the event.
 * @param spanId       Span identifier ({@code spanId}) for one proxied engine
 *                     invocation, used to correlate guardrail/input/output log
 *                     rows.
 * @param logTimestamp Log event timestamp written by the appender
 *                     ({@code LOG_TIMESTAMP}), which can differ from request
 *                     start/end timestamps.
 */
public record LogActivityRecord(String requestId, java.sql.Timestamp startTime, java.sql.Timestamp endTime,
		String request, String response, int tokens, long latency, boolean status, String engineName, String engineType,
		String methodName, String userId, String sessionId, String spanId, java.sql.Timestamp logTimestamp) {

}
