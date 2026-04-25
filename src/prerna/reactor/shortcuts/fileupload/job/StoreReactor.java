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

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class StoreReactor extends AbstractReactor {

	private String storage; // INSIGHT | DB | FILE | S3 (future)
	private String keyPrefix;

	public StoreReactor() {
		// No keysToGet needed as we use ReactorInputHelper
		this.keysToGet = new String[] { ReactorKeysEnum.INPUT.getKey(), ReactorKeysEnum.CONFIG.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {

		organizeKeys();
		NounMetadata nounMetadata = null;
		// Object output = null;
		// Map<String, Object> inputMap = getResultMap();
		Map<String, Object> result = new HashMap<String, Object>();
		// WorkflowActionResult workflowActionResult = new WorkflowActionResult();
		// Map<String, Object> meta = new HashMap<String, Object>();

		// ReactorInputHelper helper = new ReactorInputHelper(this.getNounStore());

		Map<String, Object> config = getConfigMap();
		Map<String, Object> input = getInputMap();

		Map<String, Object> resultMap = (Map<String, Object>) input.get("result");
		Map<String, Object> output = new HashMap<String, Object>();
		for (String key : resultMap.keySet()) {
			System.out.println("Key: " + key);
			output = (Map<String, Object>) resultMap.get(key);

		}

		this.storage = (String) config.get("storage");
		this.keyPrefix = (String) config.get("keyPrefix");
		System.out.println("Storage : " + storage);
		System.out.println("Prefix  : " + keyPrefix);

		/*
		 * Map<String, Object> varStore = (Map<String, Object>) config.get("varStore");
		 * 
		 * String actionName = (String) varStore.get("name");
		 * 
		 * System.out.println("Action  : " + actionName);
		 */
		// Process input
		nounMetadata = processVarStoreInput(nounMetadata, result, config, output);

		// Mark store success
		// meta.put("stored", true);
		// meta.put("storeKey", storeKey);

		// Store in context
		// result.put("meta", meta);

		return nounMetadata;
	}

	private NounMetadata processVarStoreInput(NounMetadata nounMetadata, Map<String, Object> result,
			Map<String, Object> config, Map<String, Object> output) {

		List<String> inputs = (List<String>) config.get("inputs");
		output = store(output);
		/*
		 * for (String key : inputs) { nounMetadata = planner.getVariable(key);
		 * Map<String, Object> inputMap = (Map<String, Object>) nounMetadata.getValue();
		 * FileExtractionResult fileExtractionResult = (FileExtractionResult)
		 * inputMap.get(key);
		 * 
		 * 
		 * System.out.println("Input Key   : " + key); }
		 */

		// Process output

		output = processVarStoreOutput(nounMetadata, result, config, output);

		/*
		 * Map<String, String> input = (Map<String, String>) varStore.get("input");
		 * 
		 * if (input != null) { for (Map.Entry<String, String> entry : input.entrySet())
		 * {
		 * 
		 * // Process input String key = entry.getKey(); // inputData String value =
		 * entry.getValue().replace("${", "").replace("}", ""); // ${results.action1}
		 * 
		 * nounMetadata = planner.getVariable(value); Map<String, Object> inputMap =
		 * (Map<String, Object>) nounMetadata.getValue(); FileExtractionResult
		 * fileExtractionResult = (FileExtractionResult) inputMap.get(value);
		 * 
		 * output = store(fileExtractionResult); System.out.println("Input Key   : " +
		 * key); System.out.println("Input Value : " + value);
		 * 
		 * // Process output
		 * 
		 * nounMetadata = processVarStoreOutput(nounMetadata, result, varStore, output);
		 * } }
		 */
		return new NounMetadata(output, PixelDataType.MAP);
	}

	private Map<String, Object> store(Map<String, Object> output) {
		return switch (storage) {
		case "INSIGHT" -> storeToInsight(output, this.insight);

		case "DB" -> storeToDatabase(output, this.insight);

		case "FILE" -> storeToFile(output, this.insight);

		default -> throw new IllegalArgumentException("Unsupported storage type: " + storage);
		};
	}

	/* ---------- Storage Implementations ---------- */

	private Map<String, Object> storeToInsight(Map<String, Object> output1, Insight insight) {
		// Replace with real Insight / NoSQL persistence

		// System.out.println(" [INSIGHT] key=" + fileExtractionResult.extractedText);
		// System.out.println(fileExtractionResult.extractedText);

		Map<String, Object> output = new HashMap<>();
		output.put("FileExtractionResult", output1);
		output.put("message", "storeToInsight successfully");

		return output1;

		// return workflowActionResult;
	}

	private Map<String, Object> processVarStoreOutput(NounMetadata nounMetadata, Map<String, Object> result,
			Map<String, Object> config, Object output) {

		String resultKey = (String) config.get("resultKey");

		result.put(resultKey, output);
		// nounMetadata = new NounMetadata(result, PixelDataType.MAP);
		// planner.addVariable(resultKey, nounMetadata);
		System.out.println("Output Key   : " + resultKey);
		System.out.println("Output Value : " + output);

		/*
		 * Map<String, String> outputVarStore = (Map<String, String>)
		 * varStore.get("output");
		 * 
		 * if (outputVarStore != null) { for (Map.Entry<String, String> entry :
		 * outputVarStore.entrySet()) {
		 * 
		 * String key = entry.getKey(); // results.action3 // String value =
		 * entry.getValue(); // ${result} Object value =
		 * resolveExpression(entry.getValue(), output); result.put(key, value);
		 * nounMetadata = new NounMetadata(result, PixelDataType.MAP);
		 * planner.addVariable(key, nounMetadata); System.out.println("Output Key   : "
		 * + key); System.out.println("Output Value : " + value); } }
		 */

		return result;
	}

	public Object resolveExpression(String expression, Object workflowResult) {

		if ("${result}".equals(expression)) {

			return workflowResult;
		}

		return expression;
	}

	private Map<String, Object> storeToDatabase(Map<String, Object> output1, Insight insight) {
		// JDBC / JPA integration goes here
		// System.out.println(" [INSIGHT] key=" + fileExtractionResult.extractedText);
		// System.out.println(fileExtractionResult.extractedText);

		Map<String, Object> output = new HashMap<>();
		output.put("FileExtractionResult", output1);
		output.put("message", "storeToDatabase successfully");
		return output;
	}

	private Map<String, Object> storeToFile(Map<String, Object> output1, Insight insight) {
		// JDBC / JPA integration goes here
		// System.out.println(" [INSIGHT] key=" + fileExtractionResult.extractedText);
		// System.out.println(fileExtractionResult.extractedText);

		Map<String, Object> output = new HashMap<>();
		output.put("FileExtractionResult", output1);
		output.put("message", "storeToFile successfully");
		return output;
	}

	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> getResultMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.RESULT.getKey());
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
