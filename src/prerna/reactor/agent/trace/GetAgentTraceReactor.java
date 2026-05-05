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
package prerna.reactor.agent.trace;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.AgentTraceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Retrieves a single agent trace by ID.
 *
 * <pre>
 * GetTrace(traceId=["&lt;id&gt;"])
 * </pre>
 *
 * Returns: MAP with UI-compatible field names (same shape as ListTraces items).
 */
public class GetAgentTraceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAgentTraceReactor.class);

	private static final String KEY_TRACE_ID = "traceId";

	public GetAgentTraceReactor() {
		this.keysToGet = new String[] { KEY_TRACE_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User must be authenticated to retrieve agent traces");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String traceId = this.keyValue.get(KEY_TRACE_ID);
		if (traceId == null || traceId.isEmpty()) {
			throw new IllegalArgumentException("traceId is required for GetTrace");
		}

		Map<String, Object> trace;
		try {
			trace = AgentTraceLogsUtils.getTrace(traceId);
		} catch (Exception e) {
			classLogger.warn("GetAgentTraceReactor: error fetching trace '{}'.", traceId, e);
			return NounMetadata.getErrorNounMessage("Error retrieving trace: " + traceId);
		}

		if (trace == null) {
			return NounMetadata.getErrorNounMessage("Trace not found: " + traceId);
		}

		// Verify the requesting user owns this trace
		String traceOwner = (String) trace.get("USER_ID");
		if (traceOwner != null && !traceOwner.equals(userId)) {
			classLogger.warn("GetAgentTraceReactor: user '{}' attempted to access trace owned by '{}'.", userId, traceOwner);
			return NounMetadata.getErrorNounMessage("Access denied: trace not found");
		}

		// Transform to UI field names
		Map<String, Object> transformed = ListAgentTracesReactor.transformForUI(trace);

		// Enrich with token recovery from MESSAGE table if metrics are empty
		enrichWithMessageTokens(transformed, trace);

		return new NounMetadata(transformed, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	/**
	 * If METRICS_JSON was null (trace logged before async worker finished),
	 * recover tokens from the MESSAGE table at read-time.
	 */
	private void enrichWithMessageTokens(Map<String, Object> transformed, Map<String, Object> rawTrace) {
		int inputTokens = (Integer) transformed.getOrDefault("TOTAL_INPUT_TOKENS", 0);
		int outputTokens = (Integer) transformed.getOrDefault("TOTAL_OUTPUT_TOKENS", 0);
		if (inputTokens > 0 || outputTokens > 0) {
			return;
		}

		String roomId = (String) rawTrace.get("ROOM_ID");
		Object startTimeObj = rawTrace.get("START_TIME");
		if (roomId == null || startTimeObj == null) {
			return;
		}

		try {
			String startStr = String.valueOf(startTimeObj).replace(" ", "T");
			if (!startStr.endsWith("Z") && !startStr.contains("+")) startStr += "Z";
			java.time.Instant since = java.time.Instant.parse(startStr);
			int[] tokens = AgentTraceLogsUtils.sumTokensForRoom(roomId, since);
			if (tokens[0] > 0 || tokens[1] > 0) {
				transformed.put("TOTAL_INPUT_TOKENS", tokens[0]);
				transformed.put("TOTAL_OUTPUT_TOKENS", tokens[1]);
			}
		} catch (Exception e) {
			classLogger.debug("GetAgentTraceReactor: could not recover tokens from MESSAGE.", e);
		}
	}
}
