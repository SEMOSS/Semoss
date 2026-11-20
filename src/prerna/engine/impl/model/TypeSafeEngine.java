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
import prerna.engine.api.ITypeSafeEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.MultiModalEmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;
import prerna.om.ThreadStore;

/** A Python-backed System One engine. Evaluations do not use chat messages. */
public class TypeSafeEngine extends AbstractPythonModelEngine implements ITypeSafeEngine {

	private static final Gson GSON = new GsonBuilder().serializeNulls().create();

	@Override
	public TypeSafeModelEngineResponse evaluate(Object state, Map<String, Object> questions, Insight insight,
			Map<String, Object> parameters) {
		if (!(state instanceof String || state instanceof Map || state instanceof List)) {
			throw new IllegalArgumentException("state must be text, a map, or a list");
		}
		if (questions == null || questions.isEmpty()) {
			throw new IllegalArgumentException("questions must be a nonempty map of named questions");
		}
		if (insight == null || insight.getUser() == null) {
			throw new IllegalArgumentException("An insight with a user is required for TypeSafe evaluations");
		}

		Map<String, Object> restrictions = ModelUsageRestrictionUtility.getModelUsageRestriction(insight.getUser(),
				this.engineId);
		Map<String, Object> request = new LinkedHashMap<>();
		request.put("state", state);
		request.put("questions", questions);
		request.put("parameters", parameters == null ? Map.of() : parameters);
		// Encode all caller input as JSON data, including null criteria descriptions.
		// In particular, parameter names must never be interpolated as Python code.
		String call = this.varName + ".ask(**__import__('json').loads("
				+ PyUtils.determineStringType(GSON.toJson(request)) + "))";
		checkSocketStatus();
		ZonedDateTime inputTime = ZonedDateTime.now();
		Object output = this.pyTranslator.runDirectPyNoCancelTrace(insight, call);
		TypeSafeModelEngineResponse response = TypeSafeModelEngineResponse.fromObject(output);
		ZonedDateTime outputTime = ZonedDateTime.now();

		if (this.inferenceLogsEnbaled) {
			String messageId = UUID.randomUUID().toString();
			Thread recorder = new Thread(new ModelEngineInferenceLogsWorker(messageId, messageId,
					inferenceLogMessageMethod("system_one"), this, insight.getInsightId(), insight.getContextProjectId(),
					insight.getProjectId(), insight.getUser(), ThreadStore.getSessionId(), insight.getInsightId(), null,
					null, request, response.getNumberOfTokensInPrompt(), inputTime, GSON.toJson(response.getResponse()),
					response.getNumberOfTokensInResponse(), outputTime, response.getNumberOfTokensInPrompt(),
					response.getNumberOfTokensInResponse(), null, null, null));
			recorder.start();
		}
		ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(restrictions, response, inputTime, outputTime);
		return response;
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.TYPESAFE;
	}

	@Override
	public AskModelEngineResponse askCall(InputMessage inputMessage, Insight insight, String roomId,
			Map<String, Object> parameters) {
		throw unsupportedOperation();
	}

	@Override
	public AskModelEngineResponse ask(String question, String context, Insight insight, Map<String, Object> parameters) {
		throw unsupportedOperation();
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight,
			Map<String, Object> parameters) {
		throw unsupportedOperation();
	}

	@Override
	public MultiModalEmbeddingsModelEngineResponse multiModalEmbeddings(List<String> text, List<String> image,
			List<String> video, Insight insight, Map<String, Object> parameters) {
		throw unsupportedOperation();
	}

	private UnsupportedOperationException unsupportedOperation() {
		return new UnsupportedOperationException("TypeSafe models require the TypeSafe reactor with state and questions");
	}
}
