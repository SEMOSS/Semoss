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
package prerna.engine.impl.model;

import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.ds.py.PyUtils;
import prerna.engine.api.IOpenAIDecisionsEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.OpenAIDecisionsModelEngineResponse;
import prerna.engine.impl.model.usage.ModelUsageLimitException;
import prerna.engine.impl.model.usage.ModelUsageRequest;
import prerna.engine.impl.model.usage.ModelUsageTicket;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;
import prerna.om.ThreadStore;

public class OpenAiEngine extends AbstractPythonModelEngine implements IOpenAIDecisionsEngine {

	private static final Gson GSON = new GsonBuilder().serializeNulls().create();

	@Override
	public OpenAIDecisionsModelEngineResponse decisions(Object input, List<Map<String, Object>> questions,
			Insight insight, Map<String, Object> parameters) {
		if (!(input instanceof String || input instanceof List<?> items && !items.isEmpty()
				&& items.stream().allMatch(OpenAiEngine::isJsonObject))) {
			throw new IllegalArgumentException("input must be text or a list of native OpenAI input items");
		}
		if (questions == null || questions.isEmpty()
				|| !questions.stream().allMatch(OpenAiEngine::isJsonObject)) {
			throw new IllegalArgumentException("questions must be a nonempty list of OpenAI question maps");
		}
		if (insight == null || insight.getUser() == null) {
			throw new IllegalArgumentException("An insight with a user is required for OpenAI Decisions");
		}
		if (parameters != null) {
			if (!isJsonObject(parameters)) {
				throw new IllegalArgumentException("paramValues must be a map with string keys");
			}
			for (String key : List.of("model", "input", "questions", ModelUsageTicket.USAGE_CEILING)) {
				if (parameters.containsKey(key) || parameters.get("extra_body") instanceof Map<?, ?> extra
						&& extra.containsKey(key)) {
					throw new IllegalArgumentException("paramValues cannot override " + key);
				}
			}
		}
		Map<String, Object> request = new LinkedHashMap<>();
		if (parameters != null) {
			request.putAll(parameters);
		}
		request.put("input", input);
		request.put("questions", questions);
		ModelUsageTicket usageTicket = orNoLimits(ModelUsageRestrictionUtility.admit(insight.getUser(), this,
				usageRequestFor(ModelUsageRequest.ask(() -> ModelUsageRequest.estimateTokens(request), getMaxTokens()))));
		ZonedDateTime inputTime = ZonedDateTime.now();
		OpenAIDecisionsModelEngineResponse response;
		try {
			Map<String, Object> ceiling = usageTicket.pythonCeiling();
			if (ceiling != null) {
				request.put(ModelUsageTicket.USAGE_CEILING, ceiling);
			}
			// All caller input crosses the Python bridge as JSON data, never executable fragments.
			String call = this.varName + ".decisions(**__import__('json').loads("
					+ PyUtils.determineStringType(GSON.toJson(request)) + "))";
			checkSocketStatus();
			Object output = this.pyTranslator.runDirectPyNoCancelTrace(insight, call);
			if (ModelUsageTicket.callDoesNotFit(output)) {
				throw usageTicket.doesNotFit(
						ModelUsageLimitException.doesNotFit(ModelUsageTicket.doesNotFitMessage(output)));
			}
			response = OpenAIDecisionsModelEngineResponse.fromObject(output);
		} catch (RuntimeException e) {
			usageTicket.release();
			throw e;
		}
		ZonedDateTime outputTime = ZonedDateTime.now();
		Long costMicros = ModelUsageRestrictionUtility.bill(usageTicket, response, inputTime, outputTime);
		if (this.inferenceLogsEnbaled) {
			String messageId = UUID.randomUUID().toString();
			Thread recorder = new Thread(
					new ModelEngineInferenceLogsWorker(messageId, messageId, inferenceLogMessageMethod("decisions"),
							this, insight.getInsightId(), insight.getContextProjectId(), insight.getProjectId(),
							insight.getUser(), ThreadStore.getSessionId(), insight.getInsightId(), null, null, request,
							response.getNumberOfTokensInPrompt(), inputTime, GSON.toJson(response.getResponse()),
							response.getNumberOfTokensInResponse(), outputTime, response.getNumberOfTokensInPrompt(),
							response.getNumberOfTokensInResponse(), response.getNumberOfCacheReadTokens(),
							response.getNumberOfCacheCreationTokens(), response.getNumberOfThinkingTokens())
							.withBilling(usageTicket.billedTo(), costMicros));
			recorder.start();
		}
		return response;
	}

	private static boolean isJsonObject(Object value) {
		return value instanceof Map<?, ?> map && map.keySet().stream().allMatch(key -> key instanceof String);
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.OPEN_AI;
	}
}
