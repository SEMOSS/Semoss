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
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ITypeSafeEngine;
import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/** Evaluates typed Jev questions against shared state, without a conversation. */
public class TypeSafeReactor extends AbstractReactor {

	public TypeSafeReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), "state", "questions",
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
		if (!(model instanceof ITypeSafeEngine engine)) {
			throw new IllegalArgumentException("The TypeSafe reactor requires a TYPESAFE model engine");
		}
		Object state = getInput(1);
		Map<String, Object> questions = getMapInput(2, true);
		Map<String, Object> parameters = getMapInput(3, false);
		TypeSafeModelEngineResponse response = engine.evaluate(state, questions, this.insight, parameters);
		return new NounMetadata(response.toMap(), PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private Object getInput(int index) {
		GenRowStruct row = this.store.getGenRowStruct(this.keysToGet[index]);
		if (row != null && !row.isEmpty()) {
			if (index == 1 && row.size() > 1) {
				List<Object> state = new ArrayList<>();
				for (int i = 0; i < row.size(); i++) {
					state.add(row.get(i));
				}
				return state;
			}
			if (row.size() != 1) {
				throw new IllegalArgumentException(this.keysToGet[index] + " must contain one map");
			}
			return row.get(0);
		}
		// Mirror organizeKeys' positional ordering while retaining the typed values.
		int position = 0;
		for (int i = 0; i <= index; i++) {
			GenRowStruct named = this.store.getGenRowStruct(this.keysToGet[i]);
			if (named == null || named.isEmpty()) {
				if (i == index) {
					return this.curRow != null && position < this.curRow.size() ? this.curRow.get(position) : null;
				}
				position++;
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getMapInput(int index, boolean required) {
		Object value = getInput(index);
		if (value == null && !required) {
			return Map.of();
		}
		if (!(value instanceof Map<?, ?> map) || (required && map.isEmpty())
				|| map.keySet().stream().anyMatch(key -> !(key instanceof String))) {
			throw new IllegalArgumentException(this.keysToGet[index] + " must be "
					+ (required ? "a nonempty" : "a") + " map with string keys");
		}
		return (Map<String, Object>) value;
	}

	@Override
	public String getReactorDescription() {
		return "Evaluate named Choice, Score, and Noul questions against shared state using a TypeSafe/Jev model.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if ("state".equals(key)) {
			return "Text, a map, or a list to evaluate. List state can be nested in a map to preserve its shape.";
		}
		if ("questions".equals(key)) {
			return "Map of question names to objects with type (choice, score, noul), instructions, and criteria.";
		}
		if (ReactorKeysEnum.PARAM_VALUES_MAP.getKey().equals(key)) {
			return "Optional timeout (seconds) and max_retries; model selection comes from the engine.";
		}
		return super.getDescriptionForKey(key);
	}
}
