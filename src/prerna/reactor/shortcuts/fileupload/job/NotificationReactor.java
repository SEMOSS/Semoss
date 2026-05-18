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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class NotificationReactor extends AbstractReactor {
	private String channel; // EMAIL | SLACK | WEBHOOK
	private String to;
	private String subject;
	private String template;

	/*
	 * public NotificationReactor(Map<String, Object> config) { this.channel =
	 * String.valueOf(config.getOrDefault("channel", "EMAIL")); this.to =
	 * String.valueOf(config.get("to")); this.subject =
	 * String.valueOf(config.getOrDefault("subject", "Workflow Notification"));
	 * this.template = String.valueOf(config.getOrDefault("template", "DEFAULT")); }
	 */

	public NotificationReactor() {
		// No keysToGet needed as we use ReactorInputHelper
		this.keysToGet = new String[] {ReactorKeysEnum.CONFIG.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {

		organizeKeys();
		NounMetadata nounMetadata = null;
		// Object output = null;
		// Map<String, Object> inputMap = getResultMap();

		// WorkflowActionResult workflowActionResult = new WorkflowActionResult();
		Map<String, Object> config = getConfigMap();
		Map<String, Object> result = new HashMap<String, Object>();
		Map<String, Object> output = new HashMap<String, Object>();
		List<String> inputs = (List<String>) config.get("inputs");
		for (String key : inputs) {
			NounMetadata inputNounMetadata = planner.getVariable(key);
			Map<String, Object> input = (Map<String, Object>) inputNounMetadata.getValue();
			System.out.println(input);
			output = processInput(output, config, input);

		}

		// List<Map<String, Object>> listInputMap = getInputMap();

		// String message = "Sample Message";// buildMessage(ctx);

		/*
		 * Map<String, Object> varStore = (Map<String, Object>) config.get("varStore");
		 * 
		 * String actionName = (String) varStore.get("name");
		 * 
		 * System.out.println("Action  : " + actionName);
		 */

		nounMetadata = processOutput(nounMetadata, result, config, output);
		System.out.println(" NOTIFY completed via " + channel);
		return nounMetadata;
	}

	private Map<String, Object> notify(Map<String, Object> inputMap) {
		return switch (channel) {
		case "EMAIL" -> sendEmail(inputMap, this.insight);

		case "SLACK" -> sendSlack(inputMap, this.insight);

		case "WEBHOOK" -> sendWebhook(inputMap, this.insight);

		default -> throw new IllegalArgumentException("Unsupported notification channel: " + channel);
		};
	}

	private Map<String, Object> processInput(Map<String, Object> result, Map<String, Object> config,
			Map<String, Object> inputMap) {
		// Map<String, String> input = (Map<String, String>) varStore.get("input");

		/*
		 * Map<String, Object> inputMap = new HashMap<>();
		 * 
		 * for (Map<String, Object> map : listInputMap) {
		 * 
		 * if (map == null) { continue; }
		 * 
		 * Object resultObj = map.get("result");
		 * 
		 * if (!(resultObj instanceof Map)) { continue; }
		 * 
		 * Map<String, Object> resultMap = (Map<String, Object>) resultObj;
		 * 
		 * for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
		 * 
		 * String key = entry.getKey(); Object value = entry.getValue();
		 * 
		 * System.out.println("Key: " + key);
		 * 
		 * if (value instanceof Map) { inputMap.put(key, value); } } }
		 */
		this.channel = (String) config.get("channel");
		this.to = (String) config.get("to");
		this.subject = (String) config.get("subject");
		this.template = (String) config.get("template");
		Map<String, Object> output = notify(inputMap);
		/*
		 * List<String> inputs = (List<String>) config.get("inputs");
		 * 
		 * for (String key : inputs) { nounMetadata = planner.getVariable(key);
		 * Map<String, Object> inputMap = (Map<String, Object>) nounMetadata.getValue();
		 * FileExtractionResult fileExtractionResult = (FileExtractionResult)
		 * inputMap.get(key);
		 * 
		 * output = nofify(fileExtractionResult); System.out.println("Input Key   : " +
		 * key); }
		 */
		/*
		 * if (input != null) { for (Map.Entry<String, String> entry : input.entrySet())
		 * {
		 * 
		 * String key = entry.getKey(); // inputData String value =
		 * entry.getValue().replace("${", "").replace("}", ""); // ${results.action1}
		 * 
		 * nounMetadata = planner.getVariable(value); Map<String, Object>
		 * nounMetadataMap = (Map<String, Object>) nounMetadata.getValue(); Map<String,
		 * Object> inputMap = (Map<String, Object>) nounMetadataMap.get(value);
		 * FileExtractionResult fileExtractionResult = (FileExtractionResult)
		 * inputMap.get("FileExtractionResult");
		 * 
		 * output = nofify(fileExtractionResult); System.out.println("Input Key   : " +
		 * key); System.out.println("Input Value : " + value);
		 * 
		 * } }
		 */

		return output;
	}

	private NounMetadata processOutput(NounMetadata nounMetadata, Map<String, Object> result,
			Map<String, Object> config, Object output) {

		String resultKey = (String) config.get("resultKey");

		result.put(resultKey, output);
		nounMetadata = new NounMetadata(result, PixelDataType.MAP);
		planner.addVariable(resultKey, nounMetadata);
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

		return nounMetadata;
	}

	public Object resolveExpression(String expression, Object workflowResult) {

		if ("${result}".equals(expression)) {

			return workflowResult;
		}

		return expression;
	}

	/* ---------- Message Builders ---------- */

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getTo() {
		return to;
	}

	public void setTo(String to) {
		this.to = to;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getTemplate() {
		return template;
	}

	public void setTemplate(String template) {
		this.template = template;
	}

	private String buildMessage(ExecutionContext ctx) {

		StringBuilder sb = new StringBuilder();

		sb.append("Execution ID: ").append(ctx.executionId).append("\n");

		/*
		 * if (ctx.meta.containsKey("fileType")) {
		 * sb.append("File Type: ").append(ctx.meta.get("fileType")).append("\n"); }
		 * 
		 * if (ctx.meta.containsKey("storeKey")) {
		 * sb.append("Stored As: ").append(ctx.meta.get("storeKey")).append("\n"); }
		 * 
		 * if (Boolean.TRUE.equals(ctx.failed)) { sb.append("Status: FAILED\n");
		 * sb.append("Reason: ").append(ctx.failureReason).append("\n"); } else {
		 * sb.append("Status: SUCCESS\n"); }
		 */

		return sb.toString();
	}

	/* ---------- Channel Implementations ---------- */

	private Map<String, Object> sendEmail(Map<String, Object> inputMap, Insight insight) {
		// Integrate JavaMailSender / SES here
		// String extractedText = (String) inputMap.get("extractedText");
		System.out.println(" EMAIL");
		System.out.println("To      : " + this.to);
		System.out.println("Subject : " + this.subject);
		// System.out.println(this.body);

		System.out.println(" [INSIGHT] key=" + inputMap);
		System.out.println(inputMap);

		Map<String, Object> output = new HashMap<>();
		output.put("result", inputMap);
		output.put("To", to);
		output.put("Subject", subject);
		output.put("body", inputMap);
		output.put("message", "sendEmail successfully");

		return output;
	}

	private Map<String, Object> sendSlack(Map<String, Object> inputMap, Insight insight) {
		// Slack webhook integration
		System.out.println(" SLACK");
		// System.out.println("Webhook : " + webhookUrl);
		Map<String, Object> output = new HashMap<>();
		output.put("result", inputMap);
		// output.put("To", to);
		// output.put("Subject", subject);
		output.put("body", inputMap);
		output.put("message", "sendSlack successfully");

		return output;
	}

	private Map<String, Object> sendWebhook(Map<String, Object> inputMap, Insight insight) {
		// HTTP POST integration
		System.out.println(" WEBHOOK");
		Map<String, Object> output = new HashMap<>();
		output.put("FileExtractionResult", inputMap);
		// output.put("To", to);
		// output.put("Subject", subject);
		output.put("body", inputMap);
		output.put("message", "sendWebhook successfully");

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
	private List<Map<String, Object>> getInputMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.INPUT.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			ObjectMapper mapper = new ObjectMapper();

			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.CONST_STRING);

			List<Map<String, Object>> results = new ArrayList<>();

			if (mapInputs != null && !mapInputs.isEmpty()) {

				for (int i = 0; i < mapInputs.size(); i++) {

					NounMetadata noun = mapInputs.get(i);

					// Skip null object
					if (noun == null) {
						continue;
					}

					Object value = noun.getValue();

					// Skip null value
					if (value == null) {
						continue;
					}

					try {

						Map<String, Object> parsedMap = null;

						// Case 1: Already a Map (BEST CASE)
						if (value instanceof Map) {
							parsedMap = (Map<String, Object>) value;
						}

						// Case 2: String - convert to JSON - Map
						else if (value instanceof String) {

							String result = (String) value;

							String json = result.replaceAll("([\\{,]\\s*)([A-Za-z0-9_]+)=", "$1\"$2\":")
									.replaceAll(":([^\",\\{\\}\\[\\]]+)", ":\"$1\"");

							System.out.println("Converted JSON: " + json);

							parsedMap = mapper.readValue(json, Map.class);
						}

						// Add only valid parsed map
						if (parsedMap != null && !parsedMap.isEmpty()) {
							results.add(parsedMap);
						}

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			return results;

		}
		return null;

	}
}
