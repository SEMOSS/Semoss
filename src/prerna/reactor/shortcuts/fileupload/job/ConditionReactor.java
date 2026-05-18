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
package prerna.reactor.shortcuts.fileupload.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.reactor.AbstractReactor;
import prerna.reactor.shortcuts.conductor.oss.ConditionEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ConditionReactor extends AbstractReactor {

	public ConditionReactor() {
		// No keysToGet needed as we use ReactorInputHelper
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.CONFIG.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub

		Map<String, Object> config = getConfigMap();
		// Map<String, Object> input = getInputMap();

		/*
		 * Map<String, Object> resultMap = (Map<String, Object>) input.get("result");
		 * Map<String, Object> output = new HashMap<String, Object>(); for (String key :
		 * resultMap.keySet()) { System.out.println("Key: " + key); Map<String, Object>
		 * actionOutput = (Map<String, Object>) resultMap.get(key); // NounMetadata
		 * result = planner.getVariable(key); // Map<String, Object> actionOutput =
		 * (Map<String, Object>) result.getValue(); output =
		 * ConditionEngine.execute(actionOutput, config); }
		 */

		// return Map.of("route", config.get("default"));
		String filePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		System.out.println(filePath);
		Map<String, Object> output = new HashMap<String, Object>();
		List<String> inputs = (List<String>) config.get("inputs");
		for (String key : inputs) {
			NounMetadata result = planner.getVariable(key);
			Map<String, Object> inputMap = (Map<String, Object>) result.getValue();
			System.out.println(inputMap);
			output = ConditionEngine.execute(inputMap, config);

		}

		/*
		 * for (String key : inputs) { NounMetadata result = planner.getVariable(key);
		 * Map<String, Object> map = (Map<String, Object>) result.getValue(); Object
		 * object = map.get(resultFrom); }
		 */

		/*
		 * List<String> inputs = (List<String>) config.get("inputs");
		 * 
		 * for (String key : inputs) { NounMetadata result = planner.getVariable(key);
		 * Map<String, Object> map = (Map<String, Object>) result.getValue(); Object
		 * object = map.get(resultFrom); ; Object nextNode =
		 * DecisionEvaluator.evaluate(config, object); resultMap.put("nextNode",
		 * nextNode); }
		 */
		// String resultFrom = (String) config.get("resultFrom");

		return new NounMetadata(output, PixelDataType.MAP);

	}

	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> getConfigMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.CONFIG.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getInputMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.INPUT.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {

			ObjectMapper mapper = new ObjectMapper();
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.CONST_STRING);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				String result = (String) mapInputs.get(0).getValue();

				// 1) keys: result, action1, fileName, fileType, mimeType - "key":
				String json = result.replaceAll("([\\{,]\\s*)([A-Za-z0-9_]+)=", "$1\"$2\":");

				// 2) values: =value - :"value"
				json = json.replaceAll(":([^\",\\{\\}\\[\\]]+)", ":\"$1\"");

				// Now it's valid JSON:
				System.out.println(json);
				// {"result":{"action1":{"fileName":"workflow.txt","fileType":"TXT","mimeType":"text/plain"}}}

				// 3) Parse
				Map<String, Object> map = new HashMap<String, Object>();
				try {
					map = mapper.readValue(json, Map.class);
				} catch (JsonMappingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return map;
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}
}
