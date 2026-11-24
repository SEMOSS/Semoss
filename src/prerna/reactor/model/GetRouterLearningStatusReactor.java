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
package prerna.reactor.model;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.api.IModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ValueUtils;

/**
 * Read-only status of a self-improving router for a caller who can edit the engine: the switch, optimizer model, the quality it may
 * trade, how often it learns, the committed config versions (what changed in each) and the cost per config version over the last 90 days.
 */
public class GetRouterLearningStatusReactor extends AbstractReactor {

	private static final int MAX_VERSIONS = 50;
	private static final int COST_DAYS = 90;
	/** Optional, default true; false skips the slow scan of the run history (cost_by_version). */
	private static final String EVALUATION_ID = "evaluationId";
	private static final String WITH_COSTS = "withCosts";

	public GetRouterLearningStatusReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), WITH_COSTS, EVALUATION_ID };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = ValueUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
		IModelRouterEngine model = UpdateModelRouterConfigReactor.editableRouter(this.insight, engineId);
		String liveJson = UpdateModelRouterConfigReactor.readConfig(model);
		JsonObject live = JsonParser.parseString(liveJson).getAsJsonObject();
		ModelRouterEngine.Improvement improvement = ModelRouterEngine.parseAndValidateConfig(liveJson, "router", engineId).improvement();
		ModelRouterEngine.Improvement shown = improvement == null ? new ModelRouterEngine.Improvement() : improvement;
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("engine", engineId);
		out.put("enabled", Boolean.TRUE.equals(shown.enabled));
		out.put("paused", Boolean.TRUE.equals(shown.paused));
		out.put("optimizer_model", shown.optimizer_model);
		out.put("min_new_tasks", shown.min_new_tasks);
		out.put("max_quality_loss", shown.max_quality_loss);
		out.put("replay", shown.replaySettings());
		out.put("mode", live.has("mode") ? live.get("mode").getAsString() : null);
		out.put("current_version", live.has("version") ? live.get("version").getAsString() : null);
		try {
			out.put("versions", RouterVersions.describe(RouterVersions.history(UpdateModelRouterConfigReactor.versionFolder(engineId),
					UpdateModelRouterConfigReactor.configPath(model)), MAX_VERSIONS));
			String evaluationId = ValueUtils.trimToNull(this.keyValue.get(EVALUATION_ID));
			if (evaluationId != null) {
				Map<String, Object> record = EvaluateRouterCandidateReactor.load(ApplyRouterCandidateReactor.promotionDir(engineId), evaluationId);
				if (record == null) {
					throw new IllegalArgumentException("No retained promotion with that evaluation id");
				}
				record.remove("userId");
				record.remove("userType");
				record.remove("candidate_task_scores");
				record.remove("live_task_scores");
				out.put("promotion", record);
			}
			var job = SetRouterLearningReactor.findBoundJob(engineId, shown.scheduler_job_id);
			out.put("scheduler_job_id", shown.scheduler_job_id);
			out.put("scheduler_job_binding", shown.scheduler_job_id == null ? "unbound"
					: job == null ? "missing" : job.verified() ? "bound" : "invalid");
			out.put("frequency", frequency(job, Boolean.TRUE.equals(shown.enabled)));
			out.put("new_tasks_since_last_attempt", GetRouterEvidenceReactor.newTasksSinceLastAttempt(this.insight, engineId));
			if (!"false".equalsIgnoreCase(this.keyValue.get(WITH_COSTS))) {
				out.put("cost_by_version", GetRouterEvidenceReactor.collect(this.insight, engineId, COST_DAYS, 0).get("by_version"));
			}
		} catch (Exception e) {
			throw new IllegalStateException("Could not read the router learning status: " + e.getMessage(), e);
		}
		return new NounMetadata(out, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/** off, hourly, daily, weekly or custom, from the cron of the learning job; while learning is off, what turning it on gives. */
	private static String frequency(SetRouterLearningReactor.BoundJob job, boolean enabled) {
		if (job == null || !job.verified()) {
			return enabled ? "off" : SetRouterLearningReactor.DEFAULT_FREQUENCY;
		}
		return SetRouterLearningReactor.NEVER_CRON.equals(job.cron()) ? "off" : SetRouterLearningReactor.FREQUENCY_CRONS.entrySet().stream()
				.filter(e -> e.getValue().equals(job.cron())).map(Map.Entry::getKey).findFirst().orElse("custom");
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Reads router learning status, version history, and optionally a retained promotion decision")
				.key(ReactorKeysEnum.ENGINE, "The id of the model router engine")
				.key(WITH_COSTS, "Set false to skip the run-history cost scan")
				.key(EVALUATION_ID, "Optional evaluation id from the version history")
				.output(Map.class, "Learning state, versions, and requested evidence summary");
	}
}
