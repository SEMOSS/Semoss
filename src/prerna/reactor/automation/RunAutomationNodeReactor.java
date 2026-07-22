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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Executes a single automation node for testing purposes.
 *
 * <p>Pixel: {@code RunAutomationNode(project=["appId"], nodeId=["node-id"], runId=["optional-context-run"])}
 *
 * <p>Loads the automation definition, finds the target node, optionally loads scope from a
 * prior run's outputs, and executes just that one node. The result is NOT persisted to
 * any run - this is a test/preview operation.
 *
 * <p>When {@code runId} is provided, prior node outputs from that run are loaded into
 * the scope so that {@code ${varName}} references resolve correctly.
 */
public class RunAutomationNodeReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RunAutomationNodeReactor.class);
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	public RunAutomationNodeReactor() {
		this.keysToGet = new String[]{ "project", "nodeId", "runId" };
		this.keyRequired = new int[]{ 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String nodeId = this.keyValue.get(this.keysToGet[1]);
		String contextRunId = this.keyValue.get(this.keysToGet[2]);

		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must provide a project id");
		}
		if (nodeId == null || nodeId.isEmpty()) {
			throw new IllegalArgumentException("Must provide a node id");
		}

		// Auth check
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access");
		}

		// Load automation and find the target node
		Map<String, Object> node = findNode(projectId, nodeId);
		if (node == null) {
			throw new IllegalArgumentException("Node not found in automation: " + nodeId);
		}

		// Build scope from context run (if provided)
		Map<String, String> scope = buildScope(contextRunId);
		Map<String, String> configMap = AutomationExecutionUtils.loadConfig(projectId);

		// Execute the node
		long startMs = System.currentTimeMillis();
		try {
			Object rawOutput = executeNodePixel(node, scope, configMap);
			@SuppressWarnings("unchecked")
			Map<String, Object> transformConfig = (Map<String, Object>) node.get("outputTransform");
			String transformed = AutomationExecutionUtils.applyOutputTransform(rawOutput, transformConfig);
			long durationMs = System.currentTimeMillis() - startMs;

			Map<String, Object> result = new HashMap<>();
			result.put(AutomationConstants.NODE_ID, nodeId);
			result.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);
			result.put(AutomationConstants.DURATION_MS, durationMs);
			result.put(AutomationConstants.OUTPUT_PREVIEW, PixelExecutionUtils.generatePreview(transformed));
			result.put(AutomationConstants.OUTPUT_VALUE, transformed);
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);

		} catch (Exception e) {
			long durationMs = System.currentTimeMillis() - startMs;
			classLogger.error("Test run of node {} failed: {}", nodeId, e.getMessage(), e);

			Map<String, Object> result = new HashMap<>();
			result.put(AutomationConstants.NODE_ID, nodeId);
			result.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_FAILED);
			result.put(AutomationConstants.DURATION_MS, durationMs);
			result.put(AutomationConstants.ERROR_MESSAGE, e.getMessage());
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		}
	}

	// -- Helpers -------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private Map<String, Object> findNode(String projectId, String nodeId) {
		String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
		File f = new File(portalsFolder + "/" + AutomationConstants.AUTOMATION_FILE_NAME);
		if (!f.exists()) {
			throw new IllegalArgumentException("No automation.json found for this project");
		}
		try {
			String json = Files.readString(f.toPath(), StandardCharsets.UTF_8);
			Map<String, Object> doc = GSON.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
			Map<String, Object> graph = (Map<String, Object>) doc.get("graph");
			List<Map<String, Object>> nodes = (List<Map<String, Object>>) graph.get("nodes");
			if (nodes != null) {
				for (Map<String, Object> node : nodes) {
					if (nodeId.equals(node.get("id"))) {
						return node;
					}
				}
			}
		} catch (IOException e) {
			throw new IllegalStateException("Failed to read automation.json: " + e.getMessage(), e);
		}
		return null;
	}

	private Map<String, String> buildScope(String contextRunId) {
		Map<String, String> scope = new HashMap<>();
		scope.put("date", Instant.now().toString().substring(0, 10));
		scope.put("triggered_at", Instant.now().toString());

		if (contextRunId != null && !contextRunId.isEmpty()) {
			List<Map<String, Object>> nodeOutputs = AutomationDatabaseUtility.getNodeOutputsForRun(contextRunId);
			for (Map<String, Object> output : nodeOutputs) {
				String status = (String) output.get(AutomationConstants.STATUS);
				if (AutomationConstants.NODE_STATUS_SUCCESS.equals(status)) {
					String outputVar = (String) output.get(AutomationConstants.OUTPUT_VAR);
					if (outputVar != null && !outputVar.isEmpty()) {
						Object value = output.get(AutomationConstants.OUTPUT_VALUE);
						scope.put(outputVar, value != null ? value.toString() : "");
					}
				}
			}
		}
		return scope;
	}

	private Object executeNodePixel(Map<String, Object> node, Map<String, String> scope,
			Map<String, String> configMap) {
		String type = (String) node.get("type");
		if (AutomationConstants.NODE_TRIGGER.equals(type)) {
			return scope.get("triggered_at");
		}

		String builtPixel = (String) node.get("builtPixel");
		if (builtPixel == null || builtPixel.isBlank() || builtPixel.startsWith("//")) {
			throw new IllegalStateException("Node has no compiled pixel - save the automation first");
		}

		String resolved = AutomationExecutionUtils.resolve(builtPixel, scope, configMap);
		return PixelExecutionUtils.runAndCollect(this.insight, resolved,
				AutomationConstants.DEFAULT_TIMEOUT_SECONDS);
	}
}
