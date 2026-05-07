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
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

/**
 * Shared utility methods for agent trace view reactors. Provides common helpers
 * for extracting, normalizing, and computing display-ready values from raw trace
 * data returned by {@link prerna.engine.impl.model.inferencetracking.AgentTraceLogsUtils}.
 */
public final class AgentTraceViewHelper {

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	private AgentTraceViewHelper() {
		// utility class
	}

	/**
	 * Extracts a String value from a trace row map, checking both raw column name
	 * and the AGENT_TRACE__-prefixed variant returned by SelectQueryStruct.
	 */
	public static String extractString(Map<String, Object> row, String key) {
		Object val = row.get(key);
		if (val == null) {
			val = row.get("AGENT_TRACE__" + key);
		}
		return val != null ? String.valueOf(val) : null;
	}

	/**
	 * Normalizes a raw TERMINATION_REASON into a UI-friendly status label.
	 * Returns "OK" for successful completions, "ERROR" for failures, or the raw value.
	 */
	public static String normalizeStatus(String terminationReason) {
		if (terminationReason == null) return "OK";
		if ("SUCCESS".equalsIgnoreCase(terminationReason) || "DONE".equalsIgnoreCase(terminationReason)
				|| "RESPONSE_TEXT".equalsIgnoreCase(terminationReason)
				|| "RESPONSE_TOOL".equalsIgnoreCase(terminationReason)) {
			return "OK";
		}
		if (terminationReason.startsWith("ERROR")) return "ERROR";
		return terminationReason;
	}

	/**
	 * Computes duration in milliseconds between two timestamp objects (from DB).
	 * Handles string timestamps with or without timezone suffixes.
	 *
	 * @return duration in ms, or 0 if either timestamp is null or unparseable
	 */
	public static long computeDurationMs(Object startTime, Object endTime) {
		if (startTime == null || endTime == null) return 0;
		try {
			String startStr = String.valueOf(startTime).replace(" ", "T");
			String endStr = String.valueOf(endTime).replace(" ", "T");
			if (!startStr.endsWith("Z") && !startStr.contains("+")) startStr += "Z";
			if (!endStr.endsWith("Z") && !endStr.contains("+")) endStr += "Z";
			return Duration.between(Instant.parse(startStr), Instant.parse(endStr)).toMillis();
		} catch (Exception ex) {
			return 0;
		}
	}

	/**
	 * Extracts inputTokens and outputTokens from a METRICS_JSON object stored on the trace.
	 *
	 * @return int[2] — {inputTokens, outputTokens}, defaulting to 0 on failure
	 */
	public static int[] extractTokensFromMetrics(Object metricsJson) {
		if (metricsJson == null) return new int[] {0, 0};
		try {
			JsonObject json = GSON.fromJson(String.valueOf(metricsJson), JsonObject.class);
			int input = json.has("inputTokens") ? json.get("inputTokens").getAsInt() : 0;
			int output = json.has("outputTokens") ? json.get("outputTokens").getAsInt() : 0;
			return new int[] {input, output};
		} catch (Exception e) {
			return new int[] {0, 0};
		}
	}

	/**
	 * Safely parses an integer from a trace row map value.
	 *
	 * @return the int value, or defaultValue if null or unparseable
	 */
	public static int extractInt(Map<String, Object> row, String key, int defaultValue) {
		Object val = row.get(key);
		if (val == null) val = row.get("AGENT_TRACE__" + key);
		if (val == null) return defaultValue;
		if (val instanceof Number) return ((Number) val).intValue();
		try {
			return Integer.parseInt(String.valueOf(val));
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}
}
