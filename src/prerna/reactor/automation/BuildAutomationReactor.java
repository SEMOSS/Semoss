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
package prerna.reactor.automation;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRuntimeManager;
import prerna.reactor.agent.run.RunAgentRequest;
import prerna.reactor.agent.run.RunAgentResult;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Agentic automation builder. Uses the platform RunAgent harness to generate or
 * modify an automation graph JSON document from a plain-English description.
 *
 * <p>All user-accessible engines are registered as MCP tools on the Room so the
 * model can query database schema, inspect engine capabilities, etc. before
 * producing the final document. The model's last response must be the raw
 * automation JSON document.
 *
 * <p>Pixel: {@code BuildAutomation(project=["appId"], description=["base64desc"], engine=["modelId"], currentDoc=["base64json"])}
 *
 * <p>Returns the generated document JSON as a string. Does NOT persist  - caller saves via SaveAutomation.
 */
public class BuildAutomationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(BuildAutomationReactor.class);

	private static final int DESCRIPTION_MAX_CHARS = 2000;
	private static final int MAX_TURNS = 10;
	private static final long BUILD_TIMEOUT_MS = 180_000L;
	private static final String HARNESS_TYPE = "semoss";

	private static final String CURRENT_DOC_KEY = "currentDoc";

	private static final String SYSTEM_PROMPT =
		"You are a workflow automation builder. Your job is to create (or modify) an automation graph JSON document "
		+ "from a plain-English description.\n\n"
		+ "Use the available tools to gather context before building (for example, query a database engine to "
		+ "understand its schema before writing SQL). When you are done, your FINAL response must be ONLY the raw "
		+ "automation JSON document  - no prose, no code fences, no explanation. Just: {\"version\":1,...}\n\n"
		+ "## Document format\n\n"
		+ "The document must be:\n"
		+ "{\"version\":1,\"description\":\"<one sentence>\",\"graph\":{\"nodes\":[...],\"edges\":[]}}\n\n"
		+ "Each node: {\"id\":\"<type>-<n>\",\"type\":\"<type>\",\"label\":\"<action label>\","
		+ "\"position\":{\"x\":0,\"y\":0},\"outputVar\":\"<unique_var>\",\"config\":{...}}\n\n"
		+ "Node types and config templates:\n"
		+ "  trigger         {\"mode\":\"manual\"}                                                                                                              outputVar: trigger_out\n"
		+ "  database-engine {\"engineId\":\"<id>\",\"operation\":\"query\",\"expression\":\"<SQL SELECT...>\",\"nlPrompt\":\"<one-sentence plain-English description of what this query fetches>\",\"limit\":50,\"commit\":false}  outputVar: db_out\n"
		+ "  model-engine    {\"engineId\":\"<id>\",\"operation\":\"llm\",\"command\":\"<instruction>\",\"context\":\"\",\"paramValues\":\"\",\"values\":\"\",\"image\":\"\",\"prompt\":\"\",\"entities\":\"\"}  outputVar: model_out\n"
		+ "  vector-engine   {\"engineId\":\"<id>\",\"operation\":\"search\",\"command\":\"<query>\",\"limit\":5,\"filters\":\"\",\"metaFilters\":\"\",\"filePath\":\"\",\"source\":\"\",\"space\":\"\",\"filePaths\":\"\",\"paramValues\":\"\",\"fileNames\":\"\"}  outputVar: vector_out\n"
		+ "  storage-engine  {\"engineId\":\"<id>\",\"operation\":\"list\",\"storagePath\":\"/\",\"filePath\":\"\",\"metadata\":\"\"}                                    outputVar: storage_out\n"
		+ "  function-engine {\"engineId\":\"<id>\",\"operation\":\"execute\",\"params\":\"\"}                                                                        outputVar: fn_out\n"
		+ "  app             {\"pixel\":\"<ReactorName(param=${var})>\",\"appId\":\"\"}                                                                            outputVar: pixel_out\n"
		+ "  wait            {\"seconds\":\"5\"}                                                                                                                 outputVar: wait_out\n\n"
		+ "## Building rules\n"
		+ "1. First node MUST be trigger: id=\"trigger-1\", type=\"trigger\".\n"
		+ "2. Use only node types from the list above.\n"
		+ "3. Set engineId from the available engines listed in the user message. Use \"\" if no match.\n"
		+ "4. outputVar must be unique across all nodes.\n"
		+ "5. Label: action-oriented verb phrase (e.g. \"Search claims database\", \"Draft summary email\").\n"
		+ "6. Variable substitution: reference upstream outputVars with ${varName}. In SQL wrap in single quotes: WHERE col = '${db_out}'.\n"
		+ "7. NEVER use SQL parameterized syntax ($1, ?, :param)  - unsupported.\n"
		+ "8. model-engine \"command\": plain instruction only. Set \"context\" to the upstream var (e.g. \"${db_out}\")  - the model receives real data at runtime.\n"
		+ "9. For database-engine nodes, call the get_schema tool (passing the engineId as database_id) to look up the exact table and column names before writing SQL. Never guess column names.\n"
		+ "10. For app nodes, use the reactor/pixel information in the user message to write valid pixel expressions.\n"
		+ "11. Edit mode: when an existing document is provided, preserve nodes not affected by the user's request.\n"
		+ "12. Format SQL with newlines and indentation: keywords (SELECT, FROM, WHERE, JOIN, ORDER BY, LIMIT, etc.) each on their own line, with clause contents indented.\n"
		+ "13. For every database-engine node, always include \"nlPrompt\"  - a one-sentence plain-English description of what data the query fetches.\n";

	public BuildAutomationReactor() {
		this.keysToGet = new String[] {
			ReactorKeysEnum.PROJECT.getKey(),
			AutomationConstants.DOC_DESCRIPTION,
			ReactorKeysEnum.ENGINE.getKey(),
			CURRENT_DOC_KEY
		};
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in.");
		}

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String description = this.keyValue.get(AutomationConstants.DOC_DESCRIPTION);
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access.");
		}

		if (description == null || description.trim().isEmpty()) {
			throw new IllegalArgumentException("A description of what the automation should do is required.");
		}
		try {
			description = new String(
				Base64.getDecoder().decode(description.trim()),
				StandardCharsets.UTF_8);
		} catch (IllegalArgumentException e) {
			// Not base64-encoded  - use as-is
		}
		if (description.length() > DESCRIPTION_MAX_CHARS) {
			description = description.substring(0, DESCRIPTION_MAX_CHARS);
		}

		if (engineId == null || engineId.trim().isEmpty()) {
			engineId = AutomationExecutionUtils.findFirstModelEngine(user);
		}
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException(
					"No AI model engine is available. Add a model engine connection to build an automation.");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model engine " + engineId + " does not exist or user does not have access.");
		}
		IModelEngine modelEngine = Utility.getModel(engineId);
		if (modelEngine == null) {
			throw new IllegalArgumentException(
					"Model engine " + engineId + " could not be loaded. It may no longer exist.");
		}

		String currentDocRaw = this.keyValue.get(CURRENT_DOC_KEY);
		String currentDoc = null;
		if (currentDocRaw != null && !currentDocRaw.trim().isEmpty()) {
			try {
				currentDoc = new String(
					Base64.getDecoder().decode(currentDocRaw.trim()),
					StandardCharsets.UTF_8);
			} catch (IllegalArgumentException e) {
				currentDoc = currentDocRaw;
			}
			if (currentDoc != null && currentDoc.length() > 50_000) {
				currentDoc = currentDoc.substring(0, 50_000);
			}
		}

		StringBuilder initialMsg = new StringBuilder();
		initialMsg.append(AutomationExecutionUtils.buildAvailableEnginesSection(user)).append("\n");
		if (currentDoc != null) {
			classLogger.info("BuildAutomation edit mode: project={}, docLength={}", projectId, currentDoc.length());
			initialMsg.append("## Existing automation to modify\n").append(currentDoc).append("\n\n");
			initialMsg.append("## User modification request\n").append(description.trim());
		} else {
			initialMsg.append("## User request\n").append(description.trim());
		}

		// Fresh room per build request  - isolated tool-call context, no history bleed
		String pidClean = projectId.replace("-", "");
		String roomId = "automationbuild"
				+ pidClean.substring(0, Math.min(8, pidClean.length()))
				+ Long.toString(System.currentTimeMillis(), 36);

		Map<String, Object> options = AutomationExecutionUtils.buildEngineMcpOptions(user, SYSTEM_PROMPT);

		RoomUtils.createRoomIfNotExists(roomId, this.insight, modelEngine, initialMsg.toString(),
				null, options, null, projectId, null);

		RunAgentRequest request = new RunAgentRequest(
				roomId, initialMsg.toString(), engineId, HARNESS_TYPE, null,
				MAX_TURNS, 0, null, null, null, null, this.insight);

		classLogger.info("BuildAutomation starting RunAgent: project={} roomId={}", projectId, roomId);

		RunAgentResult handle = AgentRuntimeManager.get().run(request);
		Map<String, Object> result;
		try {
			result = AgentRuntimeManager.get().waitForRun(handle.getRunId(), this.insight, BUILD_TIMEOUT_MS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Build interrupted.", e);
		}

		String status = (String) result.get("status");
		if ("FAILED".equals(status)) {
			String errMsg = (String) result.get("errorMessage");
			classLogger.error("BuildAutomation run failed: project={} error={}", projectId, errMsg);
			throw new RuntimeException("AI generation failed: " + (errMsg != null ? errMsg : "unknown error"));
		}

		String finalText = (String) result.get("finalText");
		if (finalText == null || finalText.isBlank()) {
			throw new IllegalStateException(
					"The AI model did not return a response. Try again or start with a blank automation.");
		}

		String docJson = AutomationExecutionUtils.stripCodeFences(finalText.trim());
		AutomationExecutionUtils.validateGeneratedDoc(docJson);

		classLogger.info("BuildAutomation finished: project={}", projectId);
		return new NounMetadata(docJson, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Agentic automation builder using the platform RunAgent harness. Generates or edits an automation "
				+ "graph document from a plain-English description. All user engines are registered as MCP tools "
				+ "so the model can query schema and capabilities before building. Does NOT persist  - "
				+ "caller saves via SaveAutomation.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "project" -> "Project ID that will own this automation.";
			case "description" -> "Base64-encoded plain-English description of what the automation should do.";
			case "engine" -> "Optional model engine ID to use. Defaults to the first available MODEL engine.";
			case CURRENT_DOC_KEY -> "Optional base64-encoded JSON of an existing automation document. When provided, the model modifies the existing document rather than generating from scratch.";
			default -> super.getDescriptionForKey(key);
		};
	}
}
