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
package prerna.reactor.agent.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.reactor.agent.run.AgentRuntimeManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Scores agent effectiveness for a room or a single run. Metrics are computed
 * from the harness-normalized run activity (tool failure rates, improper tool
 * use, identical-retry waste, skill loading, turn counts, token usage) plus
 * room-level inference stats from the model inference logs. Works for both the
 * semoss and claude_code harnesses because it consumes the projected message
 * contract from AgentRuntimeManager rather than harness-native records.
 */
public class GetAgentEffectivenessReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAgentEffectivenessReactor.class);

	private static final String ROOM_ID_KEY = "roomId";
	private static final String RUN_ID_KEY = "runId";
	private static final String INCLUDE_RUNS_KEY = "includeRuns";
	private static final String LIMIT_KEY = "limit";

	private static final int DEFAULT_RUN_LIMIT = 50;

	public GetAgentEffectivenessReactor() {
		this.keysToGet = new String[] { ROOM_ID_KEY, RUN_ID_KEY, INCLUDE_RUNS_KEY, LIMIT_KEY };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = StringUtils.trimToNull(this.keyValue.get(ROOM_ID_KEY));
		String runId = StringUtils.trimToNull(this.keyValue.get(RUN_ID_KEY));
		if (roomId == null && runId == null) {
			throw new IllegalArgumentException("Provide roomId to score a room or runId to score a single run");
		}
		boolean includeRuns = this.keyValue.get(INCLUDE_RUNS_KEY) == null
				|| Boolean.parseBoolean(this.keyValue.get(INCLUDE_RUNS_KEY));
		int limit = parseLimit(this.keyValue.get(LIMIT_KEY));

		AgentRuntimeManager runtime = AgentRuntimeManager.get();
		List<Map<String, Object>> runMetrics = new ArrayList<>();

		if (runId != null) {
			// getRun enforces ownership - it only resolves the current user's runs
			Map<String, Object> run = runtime.getRun(runId, this.insight, true);
			if (roomId == null) {
				roomId = StringUtils.trimToNull(String.valueOf(run.get("roomId")));
			}
			runMetrics.add(AgentEffectivenessCalculator.computeRunMetrics(run));
		} else {
			List<Map<String, Object>> runs = new AgentRunStore().getRunsForRoom(this.insight, roomId);
			// store order is newest-first; score the most recent runs and report
			// them chronologically
			if (runs.size() > limit) {
				runs = new ArrayList<>(runs.subList(0, limit));
			}
			Collections.reverse(runs);
			for (Map<String, Object> run : runs) {
				String currentRunId = StringUtils.trimToNull(String.valueOf(run.get("runId")));
				if (currentRunId == null) {
					continue;
				}
				try {
					Map<String, Object> fullRun = runtime.getRun(currentRunId, this.insight, true);
					runMetrics.add(AgentEffectivenessCalculator.computeRunMetrics(fullRun));
				} catch (Exception e) {
					classLogger.warn("Skipping unreadable run '{}' while scoring roomId '{}'.", currentRunId,
							roomId, e);
				}
			}
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("roomId", roomId);
		result.put("rollup", AgentEffectivenessCalculator.aggregate(runMetrics));
		result.put("roomInference", AgentEffectivenessCalculator.queryRoomInferenceStats(roomId));
		if (includeRuns) {
			result.put("runs", runMetrics);
		} else {
			List<Map<String, Object>> summaries = new ArrayList<>(runMetrics.size());
			for (Map<String, Object> metrics : runMetrics) {
				Map<String, Object> summary = new LinkedHashMap<>();
				summary.put("runId", metrics.get("runId"));
				summary.put("status", metrics.get("status"));
				summary.put("harnessType", metrics.get("harnessType"));
				summary.put("score", metrics.get("score"));
				summaries.add(summary);
			}
			result.put("runs", summaries);
		}
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static int parseLimit(String value) {
		if (value == null || value.trim().isEmpty()) {
			return DEFAULT_RUN_LIMIT;
		}
		try {
			int parsed = Integer.parseInt(value.trim());
			return parsed > 0 ? parsed : DEFAULT_RUN_LIMIT;
		} catch (NumberFormatException e) {
			return DEFAULT_RUN_LIMIT;
		}
	}

	@Override
	public String getReactorDescription() {
		return "Scores agent effectiveness for a room or single run: tool failure rates, improper tool use "
				+ "(unknown tools, malformed arguments, identical retries, unanswered calls), skill loading, "
				+ "turn counts, token usage, and a weighted 0-100 effectiveness score per run.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ROOM_ID_KEY.equals(key)) {
			return "Room whose agent runs should be scored. Required unless runId is provided.";
		}
		if (RUN_ID_KEY.equals(key)) {
			return "Single agent run to score. Required unless roomId is provided.";
		}
		if (INCLUDE_RUNS_KEY.equals(key)) {
			return "When false, per-run detail is reduced to runId/status/score summaries. Defaults to true.";
		}
		if (LIMIT_KEY.equals(key)) {
			return "Maximum number of most-recent runs to score in room mode. Defaults to 50.";
		}
		return super.getDescriptionForKey(key);
	}
}
