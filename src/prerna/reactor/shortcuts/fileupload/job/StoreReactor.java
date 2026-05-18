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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.vector.CreateEmbeddingsFromDocumentsReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.web.services.util.WebUtility;

public class StoreReactor extends AbstractReactor {

	private String storage; // INSIGHT | DB | FILE | S3 (future)
	private String keyPrefix;

	public StoreReactor() {
		// No keysToGet needed as we use ReactorInputHelper
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.CONFIG.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {

		organizeKeys();
		NounMetadata nounMetadata = null;
		// Object output = null;
		// Map<String, Object> inputMap = getResultMap();

		// WorkflowActionResult workflowActionResult = new WorkflowActionResult();
		// Map<String, Object> meta = new HashMap<String, Object>();

		// ReactorInputHelper helper = new ReactorInputHelper(this.getNounStore());

		Map<String, Object> config = getConfigMap();
		// Map<String, Object> input = getInputMap();

		/*
		 * Map<String, Object> resultMap = (Map<String, Object>) input.get("result");
		 * Map<String, Object> output = new HashMap<String, Object>(); for (String key :
		 * resultMap.keySet()) { System.out.println("Key: " + key); output =
		 * (Map<String, Object>) resultMap.get(key);
		 * 
		 * }
		 */
		String filePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

		Map<String, Object> output = new HashMap<String, Object>();
		List<String> inputs = (List<String>) config.get("inputs");
		for (String key : inputs) {
			NounMetadata inputNounMetadata = planner.getVariable(key);
			Map<String, Object> actionInput = (Map<String, Object>) inputNounMetadata.getValue();
			System.out.println(actionInput);
			Map<String, Object> input = (Map<String, Object>) actionInput.get(key);
			nounMetadata = processInput(nounMetadata, config, input, filePath, engineId);

		}
		/*
		 * for (String key : inputs) { nounMetadata = planner.getVariable(key);
		 * Map<String, Object> inputMap = (Map<String, Object>) nounMetadata.getValue();
		 * FileExtractionResult fileExtractionResult = (FileExtractionResult)
		 * inputMap.get(key);
		 * 
		 * 
		 * System.out.println("Input Key   : " + key); }
		 */

		/*
		 * Map<String, Object> varStore = (Map<String, Object>) config.get("varStore");
		 * 
		 * String actionName = (String) varStore.get("name");
		 * 
		 * System.out.println("Action  : " + actionName);
		 */
		// Process input

		// Mark store success
		// meta.put("stored", true);
		// meta.put("storeKey", storeKey);

		// Store in context
		// result.put("meta", meta);

		return nounMetadata;
	}

	private NounMetadata processInput(NounMetadata nounMetadata, Map<String, Object> config, Map<String, Object> input,
			String filePath, String engineId) {

		this.storage = (String) config.get("storage");
		this.keyPrefix = (String) config.get("keyPrefix");
		System.out.println("Storage : " + storage);
		System.out.println("Prefix  : " + keyPrefix);

		Map<String, Object> output = store(input, filePath, config, engineId);
		/*
		 * for (String key : inputs) { nounMetadata = planner.getVariable(key);
		 * Map<String, Object> inputMap = (Map<String, Object>) nounMetadata.getValue();
		 * FileExtractionResult fileExtractionResult file= (FileExtractionResult)
		 * inputMap.get(key);
		 * 
		 * 
		 * System.out.println("Input Key   : " + key); }
		 */

		// Process output

		Map<String, Object> result = processOutput(nounMetadata, output, config);

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
		return new NounMetadata(result, PixelDataType.MAP);
	}

	private Map<String, Object> store(Map<String, Object> input, String filePath, Map<String, Object> config,
			String engineId) {
		return switch (storage) {
		case "INSIGHT" -> storeToInsight(input, this.insight, filePath, config, engineId);

		case "DB" -> storeToDatabase(input, this.insight, filePath, config, engineId);

		case "VECTOR" -> storeToVectorDatabase(input, this.insight, filePath, config, engineId);

		case "FILE" -> storeToFile(input, this.insight, filePath, config, engineId);

		default -> throw new IllegalArgumentException("Unsupported storage type: " + storage);
		};
	}

	private Map<String, Object> storeToVectorDatabase(Map<String, Object> input, Insight insight, String filePath,
			Map<String, Object> config, String engineId) {
		// JDBC / JPA integration goes here
		// System.out.println(" [INSIGHT] key=" + fileExtractionResult.extractedText);
		// System.out.println(fileExtractionResult.extractedText);
		// Initializing and calling LLM Reactor
		storeToInsight(input, insight, filePath, config, engineId);
		CreateEmbeddingsFromDocumentsReactor createEmbeddingsFromDocumentsReactor = new CreateEmbeddingsFromDocumentsReactor();
		NounStore outputNouns = new NounStore("Create Embeddings From Documents");

		GenRowStruct grs = new GenRowStruct();
		grs.add(new NounMetadata(engineId, PixelDataType.CONST_STRING));
		outputNouns.addNoun(ReactorKeysEnum.ENGINE.getKey(), grs);

		grs = new GenRowStruct();
		grs.add(new NounMetadata(filePath, PixelDataType.CONST_STRING));
		outputNouns.addNoun("filePaths", grs);

		createEmbeddingsFromDocumentsReactor.setNounStore(outputNouns);

		createEmbeddingsFromDocumentsReactor.setInsight(this.insight);

		// Executing LLM Reactor for predicting meta model by using LLM for CSV's
		NounMetadata resultNoun = createEmbeddingsFromDocumentsReactor.execute();

		// Response from the LLM Model
		Map<String, Object> response = (Map<String, Object>) resultNoun.getValue();

		Map<String, Object> output = new HashMap<>();
		// output.put("FileExtractionResult", output1);
		output.put("message", "storeToDatabase successfully");
		return output;
	}

	/* ---------- Storage Implementations ---------- */

	private Map<String, Object> storeToInsight(Map<String, Object> input, Insight insight, String filePath,
			Map<String, Object> config, String engineId) {
		// Replace with real Insight / NoSQL persistence

		// System.out.println(" [INSIGHT] key=" + fileExtractionResult.extractedText);
		// System.out.println(fileExtractionResult.extractedText);

		String insightId = WebUtility.inputSanitizer(insight.getInsightId());

		String url = "http://localhost:5173/Monolith/api/uploadFile/baseUpload?insightId=" + insightId;

		Path path = Path.of(filePath);

		String boundary = "----JavaBoundary" + UUID.randomUUID();

		byte[] fileBytes = null;
		try {
			fileBytes = Files.readAllBytes(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String fileName = path.getFileName().toString();

		String mimeType = null;
		try {
			mimeType = Files.probeContentType(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (mimeType == null) {
			mimeType = "application/octet-stream";
		}

		String multipartBody = "--" + boundary + "\r\n" + "Content-Disposition: form-data; name=\"file\"; filename=\""
				+ fileName + "\"\r\n" + "Content-Type: " + mimeType + "\r\n\r\n";

		String endBoundary = "\r\n--" + boundary + "--\r\n";

		byte[] bodyBytes = concat(multipartBody.getBytes(), fileBytes, endBoundary.getBytes());

		HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url))
				.header("Content-Type", "multipart/form-data; boundary=" + boundary)
				.POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes)).build();

		HttpClient client = HttpClient.newHttpClient();

		HttpResponse<String> response = null;
		try {
			response = client.send(request, HttpResponse.BodyHandlers.ofString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Status : " + response.statusCode());
		System.out.println("Response : " + response.body());

		System.out.println(input.get("fileName"));
		System.out.println(input.get("data"));
		System.out.println(input.get("fileType"));
		Map<String, Object> output = new HashMap<>();
		// output.put("output", input);
		output.put("fileName", input.get("fileName"));
		output.put("data", input.get("data"));
		output.put("fileType", input.get("fileType"));
		output.put("message", "storeToInsight successfully");

		return output;

		// return workflowActionResult;
	}

	private static byte[] concat(byte[]... arrays) {

		int totalLength = 0;

		for (byte[] array : arrays) {
			totalLength += array.length;
		}

		byte[] result = new byte[totalLength];

		int currentIndex = 0;

		for (byte[] array : arrays) {
			System.arraycopy(array, 0, result, currentIndex, array.length);
			currentIndex += array.length;
		}

		return result;
	}

	private Map<String, Object> processOutput(NounMetadata nounMetadata, Map<String, Object> output,
			Map<String, Object> config) {
		Map<String, Object> result = new HashMap<String, Object>();
		String resultKey = (String) config.get("resultKey");

		result.put(resultKey, output);
		// nounMetadata = new NounMetadata(result, PixelDataType.MAP);
		// planner.addVariable(resultKey, nounMetadata);
		System.out.println("Output Key   : " + resultKey);

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

	private Map<String, Object> storeToDatabase(Map<String, Object> input, Insight insight, String filePath,
			Map<String, Object> config, String engineId) {
		// JDBC / JPA integration goes here
		// System.out.println(" [INSIGHT] key=" + fileExtractionResult.extractedText);
		// System.out.println(fileExtractionResult.extractedText);

		Map<String, Object> output = new HashMap<>();
		// output.put("FileExtractionResult", output1);
		output.put("message", "storeToDatabase successfully");
		return output;
	}

	private Map<String, Object> storeToFile(Map<String, Object> input, Insight insight, String filePath,
			Map<String, Object> config, String engineId) {
		// JDBC / JPA integration goes here
		// System.out.println(" [INSIGHT] key=" + fileExtractionResult.extractedText);
		// System.out.println(fileExtractionResult.extractedText);

		Map<String, Object> output = new HashMap<>();
		// output.put("FileExtractionResult", output1);
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
