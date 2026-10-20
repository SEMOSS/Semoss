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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IOpenAIDecisionsEngine;
import prerna.engine.impl.model.responses.OpenAIDecisionsModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/** Calls the native OpenAI Decisions API without a conversation. */
public class OpenAIDecisionsReactor extends AbstractReactor {

	public OpenAIDecisionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), "input", "questions",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}
		IModelEngine model = Utility.getModel(engineId);
		if (!(model instanceof IOpenAIDecisionsEngine engine)) {
			throw new IllegalArgumentException("The OpenAIDecisions reactor requires an OpenAI Decisions model engine");
		}
		Object input = getDecisionInput();
		List<Map<String, Object>> questions = getQuestions();
		Map<String, Object> parameters = getParameters();
		OpenAIDecisionsModelEngineResponse response = engine.decisions(input, questions, this.insight, parameters);
		return new NounMetadata(response.toMap(), PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private Object getDecisionInput() {
		List<?> values = getValues(1);
		if (values.size() == 1 && values.get(0) instanceof String) {
			return values.get(0);
		}
		if (values.isEmpty() || values.stream().anyMatch(value -> !isStringKeyMap(value)
				|| !(((Map<?, ?>) value).get("role") instanceof String
						|| ((Map<?, ?>) value).get("type") instanceof String))) {
			throw new IllegalArgumentException("input must be text or a list of native OpenAI input items");
		}
		return values;
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getQuestions() {
		List<?> values = getValues(2);
		if (values.isEmpty() || values.stream().anyMatch(value -> !isStringKeyMap(value)
				|| ((Map<?, ?>) value).isEmpty())) {
			throw new IllegalArgumentException("questions must be a nonempty list of OpenAI question maps");
		}
		return (List<Map<String, Object>>) values;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getParameters() {
		List<?> values = getValues(3);
		if (values.isEmpty()) {
			return Map.of();
		}
		if (values.size() != 1 || !isStringKeyMap(values.get(0))) {
			throw new IllegalArgumentException("paramValues must contain one map with string keys");
		}
		return (Map<String, Object>) values.get(0);
	}

	private boolean isStringKeyMap(Object value) {
		return value instanceof Map<?, ?> map && map.keySet().stream().allMatch(key -> key instanceof String);
	}

	private List<?> getValues(int index) {
		GenRowStruct row = this.store.getGenRowStruct(this.keysToGet[index]);
		if (row != null && !row.isEmpty()) {
			if (row.size() == 1 && row.get(0) instanceof List<?> list) {
				return list;
			}
			List<Object> values = new ArrayList<>();
			for (int i = 0; i < row.size(); i++) {
				values.add(row.get(i));
			}
			return values;
		}
		// Mirror organizeKeys' positional ordering while retaining the typed values.
		int position = 0;
		for (int i = 0; i <= index; i++) {
			GenRowStruct named = this.store.getGenRowStruct(this.keysToGet[i]);
			if (named == null || named.isEmpty()) {
				if (i == index && this.curRow != null && position < this.curRow.size()) {
					Object value = this.curRow.get(position);
					return value instanceof List<?> list ? list : Collections.singletonList(value);
				}
				position++;
			}
		}
		return List.of();
	}

	@Override
	public String getReactorDescription() {
		return "Evaluate native OpenAI predicate, choice, and score questions using an OpenAI model engine.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if ("input".equals(key)) {
			return "Text or a list of native OpenAI input items, including messages with role and content.";
		}
		if ("questions".equals(key)) {
			return "List of native OpenAI question objects with type (predicate, choice, score), instructions, and optional name.";
		}
		if (ReactorKeysEnum.PARAM_VALUES_MAP.getKey().equals(key)) {
			return "Optional OpenAI Decisions request options; model selection comes from the engine.";
		}
		return super.getDescriptionForKey(key);
	}
}
