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
package prerna.reactor.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ExecuteFunctionEngineReactor extends AbstractReactor {

	public ExecuteFunctionEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.MAP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Function Engine " + engineId + " does not exist or user does not have access to this function");
		}

		Map<String, Object> parameterValues = getMap();
		parameterValues.put(Constants.INSIGHT, this.insight);

		IFunctionEngine engine = Utility.getFunctionEngine(engineId);
		Object execValue = engine.execute(parameterValues);
		return new NounMetadata(execValue, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	/**
	 * Collect the parameters to call the function with.
	 *
	 * <p>
	 * The parameters may be supplied three ways, in this order of precedence: as
	 * the named {@code map} key, as an unnamed map argument, or as named arguments
	 * alongside {@code engine}. The last form is what lets a generated MCP tool
	 * call a function the way its own definition describes it -
	 * {@code ExecuteFunctionEngine(engine="...", query="...")} - so a model reading
	 * the tool schema does not also have to know to nest the arguments under a map.
	 *
	 * @return the parameters to pass to the function engine
	 */
	private Map<String, Object> getMap() {
		Map<String, Object> parameterValues = new HashMap<>();

		GenRowStruct mapGrs = this.store.getGenRowStruct(this.keysToGet[1]);
		if (mapGrs != null && !mapGrs.isEmpty()) {
			for (int i = 0; i < mapGrs.size(); i++) {
				NounMetadata noun = mapGrs.getNoun(i);
				parameterValues.putAll((Map<String, Object>) noun.getValue());
			}
			return parameterValues;
		}

		List<Object> mapValues = curRow.getValuesOfType(PixelDataType.MAP);
		if (mapValues != null && !mapValues.isEmpty()) {
			for (int i = 0; i < mapValues.size(); i++) {
				parameterValues.putAll((Map<String, Object>) mapValues.get(i));
			}
			return parameterValues;
		}

		return getNamedArguments();
	}

	/**
	 * Treat every named argument other than the reactor's own keys as a function
	 * parameter. Only reached when no map was supplied, so a caller passing a map
	 * keeps exactly the behavior it always had.
	 *
	 * @return the named arguments as a parameter map, empty when there were none
	 */
	private Map<String, Object> getNamedArguments() {
		Map<String, Object> parameterValues = new HashMap<>();
		if (this.store == null) {
			return parameterValues;
		}

		for (String nounKey : this.store.getNounKeys()) {
			// engine and map are this reactor's own inputs, and the positional
			// bucket holds unnamed arguments that carry no parameter name to use
			if (nounKey.equals(this.keysToGet[0]) || nounKey.equals(this.keysToGet[1])
					|| nounKey.equals(ALL_NOUN_STORE)) {
				continue;
			}
			GenRowStruct grs = this.store.getGenRowStruct(nounKey);
			if (grs == null || grs.isEmpty()) {
				continue;
			}
			// a parameter given more than once is a list to the function rather
			// than a last-one-wins overwrite
			if (grs.size() == 1) {
				parameterValues.put(nounKey, grs.get(0));
			} else {
				List<Object> values = new ArrayList<>();
				for (int i = 0; i < grs.size(); i++) {
					values.add(grs.get(i));
				}
				parameterValues.put(nounKey, values);
			}
		}

		return parameterValues;
	}

	@Override
	public String getReactorDescription() {
		return """
				Runs a function engine - a server side tool an admin has registered on the platform, \
				such as OCR or document extraction, audio transcription, image description, a wrapped \
				enterprise REST API, a web search, or a registered python function. \
				Call GetFunctionEngineDefinition first when the function's parameters are not already known.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The id of the function engine to run";
		} else if (key.equals(ReactorKeysEnum.MAP.getKey())) {
			return """
					The function's parameters, as a map of parameter name to value. The parameters a given \
					function accepts come back from GetFunctionEngineDefinition\
					""";
		}
		return super.getDescriptionForKey(key);
	}

}
