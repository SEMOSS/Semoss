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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
 * Synthesizes OTel-style spans from AGENT_TRACE + AGENT_TRACE_STEP tables.
 *
 * <pre>
 * ListSpans(traceId=["&lt;id&gt;"])
 * </pre>
 *
 * Returns: LIST of span objects, each with:
 * SPAN_ID, TRACE_ID, PARENT_SPAN_ID, KIND, NAME,
 * STARTED_AT, ENDED_AT, DURATION_MS, STATUS, INPUT_TOKENS, OUTPUT_TOKENS.
 *
 * The root span is synthesized from AGENT_TRACE; child spans from AGENT_TRACE_STEP.
 */
public class ListSpansReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListSpansReactor.class);

	private static final String KEY_TRACE_ID = "traceId";

	public ListSpansReactor() {
		this.keysToGet = new String[] { KEY_TRACE_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User must be authenticated to list spans");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String traceId = this.keyValue.get(KEY_TRACE_ID);
		if (traceId == null || traceId.isEmpty()) {
			throw new IllegalArgumentException("traceId is required for ListSpans");
		}

		// Fetch the trace for root span + ownership check
		Map<String, Object> trace;
		try {
			trace = AgentTraceLogsUtils.getTrace(traceId);
		} catch (Exception e) {
			classLogger.warn("ListSpansReactor: error fetching trace '{}'.", traceId, e);
			return new NounMetadata(new ArrayList<>(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}

		if (trace == null) {
			return new NounMetadata(new ArrayList<>(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}

		// Ownership check
		String traceOwner = (String) trace.get("USER_ID");
		if (traceOwner != null && !traceOwner.equals(userId)) {
			classLogger.warn("ListSpansReactor: user '{}' attempted to access spans for trace owned by '{}'.", userId, traceOwner);
			return new NounMetadata(new ArrayList<>(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}

		List<Map<String, Object>> spans = new ArrayList<>();

		// Root span from the AGENT_TRACE row
		String rootSpanId = traceId; // reuse traceId as root span ID
		Map<String, Object> rootSpan = new LinkedHashMap<>();
		rootSpan.put("SPAN_ID", rootSpanId);
		rootSpan.put("TRACE_ID", traceId);
		rootSpan.put("KIND", "agent.run");
		rootSpan.put("NAME", "agent.run");
		rootSpan.put("STARTED_AT", trace.get("START_TIME"));
		rootSpan.put("ENDED_AT", trace.get("END_TIME"));
		rootSpan.put("DURATION_MS", computeDurationMs(trace.get("START_TIME"), trace.get("END_TIME")));
		rootSpan.put("STATUS", normalizeStatus(trace.get("TERMINATION_REASON")));
		rootSpan.put("INPUT_TOKENS", 0);
		rootSpan.put("OUTPUT_TOKENS", 0);
		spans.add(rootSpan);

		// Child spans from AGENT_TRACE_STEP
		try {
			List<Map<String, Object>> steps = AgentTraceLogsUtils.listTraceSteps(traceId, userId);
			if (steps != null) {
				for (Map<String, Object> step : steps) {
					Map<String, Object> childSpan = new LinkedHashMap<>();
					childSpan.put("SPAN_ID", step.get("STEP_ID"));
					childSpan.put("TRACE_ID", traceId);
					childSpan.put("PARENT_SPAN_ID", rootSpanId);
					childSpan.put("KIND", mapStepTypeToKind(step.get("STEP_TYPE")));
					childSpan.put("NAME", buildSpanName(step));
					childSpan.put("STARTED_AT", step.get("START_TIME"));
					childSpan.put("ENDED_AT", step.get("END_TIME"));
					childSpan.put("DURATION_MS", computeDurationMs(step.get("START_TIME"), step.get("END_TIME")));
					childSpan.put("STATUS", step.get("ERROR_MESSAGE") != null ? "ERROR" : "OK");
					childSpan.put("INPUT_TOKENS", 0);
					childSpan.put("OUTPUT_TOKENS", 0);
					spans.add(childSpan);
				}
			}
		} catch (Exception e) {
			classLogger.debug("ListSpansReactor: error fetching steps for trace '{}'.", traceId, e);
		}

		// If no child spans, synthesize a single model.call span from harness type
		if (spans.size() == 1) {
			String harnessType = (String) trace.get("HARNESS_TYPE");
			if (harnessType != null) {
				Map<String, Object> modelSpan = new LinkedHashMap<>();
				modelSpan.put("SPAN_ID", java.util.UUID.randomUUID().toString());
				modelSpan.put("TRACE_ID", traceId);
				modelSpan.put("PARENT_SPAN_ID", rootSpanId);
				modelSpan.put("KIND", "model.call");
				modelSpan.put("NAME", harnessType + ".run");
				modelSpan.put("STARTED_AT", trace.get("START_TIME"));
				modelSpan.put("ENDED_AT", trace.get("END_TIME"));
				modelSpan.put("DURATION_MS", computeDurationMs(trace.get("START_TIME"), trace.get("END_TIME")));
				modelSpan.put("STATUS", normalizeStatus(trace.get("TERMINATION_REASON")));
				modelSpan.put("INPUT_TOKENS", 0);
				modelSpan.put("OUTPUT_TOKENS", 0);
				spans.add(modelSpan);
			}
		}

		return new NounMetadata(spans, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private static String normalizeStatus(Object terminationReason) {
		if (terminationReason == null) return "OK";
		String reason = String.valueOf(terminationReason);
		if ("SUCCESS".equalsIgnoreCase(reason) || "DONE".equalsIgnoreCase(reason)
				|| "RESPONSE_TEXT".equalsIgnoreCase(reason) || "RESPONSE_TOOL".equalsIgnoreCase(reason)) return "OK";
		if (reason.startsWith("ERROR")) return "ERROR";
		return reason;
	}

	private static long computeDurationMs(Object startTime, Object endTime) {
		if (startTime == null || endTime == null) return 0;
		try {
			String startStr = String.valueOf(startTime).replace(" ", "T");
			String endStr = String.valueOf(endTime).replace(" ", "T");
			if (!startStr.endsWith("Z") && !startStr.contains("+")) startStr += "Z";
			if (!endStr.endsWith("Z") && !endStr.contains("+")) endStr += "Z";
			Instant s = Instant.parse(startStr);
			Instant e = Instant.parse(endStr);
			return Duration.between(s, e).toMillis();
		} catch (Exception ex) {
			return 0;
		}
	}

	private static String mapStepTypeToKind(Object stepType) {
		if (stepType == null) return "tool.call";
		String type = String.valueOf(stepType).toLowerCase();
		if (type.contains("model") || type.contains("llm")) return "model.call";
		if (type.contains("tool")) return "tool.call";
		return "tool.call";
	}

	private static String buildSpanName(Map<String, Object> step) {
		Object toolName = step.get("TOOL_NAME");
		if (toolName != null && !String.valueOf(toolName).isEmpty()) {
			return String.valueOf(toolName);
		}
		Object stepType = step.get("STEP_TYPE");
		if (stepType != null) return String.valueOf(stepType);
		return "step";
	}
}
