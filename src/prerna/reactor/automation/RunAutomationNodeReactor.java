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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.nodes.AutomationNodeContext;
import prerna.reactor.automation.nodes.AutomationNodeExecutors;
import prerna.reactor.automation.nodes.IAutomationNodeExecutor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Executes a single automation node for testing/preview purposes.
 * Result is NOT persisted to any run record.
 *
 * <p>Pixel: {@code RunAutomationNode(project=["appId"], nodeId=["node-id"], runId=["optional-context-run"])}
 */
public class RunAutomationNodeReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RunAutomationNodeReactor.class);

	public RunAutomationNodeReactor() {
		this.keysToGet = new String[] { "project", "nodeId", "runId" };
		this.keyRequired = new int[] { 1, 1, 0 };
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

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access");
		}

		Map<String, Object> node = findNode(projectId, nodeId);
		if (node == null) {
			throw new IllegalArgumentException("Node not found in automation: " + nodeId);
		}

		Map<String, String> scope = buildScope(contextRunId);
		Map<String, String> configMap = AutomationExecutionUtils.loadConfig(projectId);

		long startMs = System.currentTimeMillis();
		try {
			String type = (String) node.get("type");
			Object rawOutput;

			if (AutomationConstants.NODE_TRIGGER.equals(type)) {
				rawOutput = scope.get("triggered_at");
			} else {
				IAutomationNodeExecutor executor = AutomationNodeExecutors.EXECUTORS.get(type);
				if (executor == null) {
					throw new IllegalArgumentException("Unsupported node type: " + type);
				}
				AutomationNodeContext ctx = new AutomationNodeContext(
						"test", projectId, node, scope, configMap,
						this.insight, new AtomicBoolean(false));
				rawOutput = executor.execute(ctx);
			}

			@SuppressWarnings("unchecked")
			Map<String, Object> transformConfig = (Map<String, Object>) node.get("outputTransform");
			String transformed = AutomationExecutionUtils.applyOutputTransform(rawOutput, transformConfig);
			long durationMs = System.currentTimeMillis() - startMs;
			String preview = AutomationExecutionUtils.generatePreview(transformed);

			Map<String, Object> result = new HashMap<>();
			result.put(AutomationConstants.NODE_ID, nodeId);
			result.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);
			result.put(AutomationConstants.DURATION_MS, durationMs);
			result.put(AutomationConstants.OUTPUT_PREVIEW, preview);
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

	@SuppressWarnings("unchecked")
	private static Map<String, Object> findNode(String projectId, String nodeId) {
		Map<String, Object> doc = AutomationExecutionUtils.loadAutomationDoc(projectId);
		Map<String, Object> graph = (Map<String, Object>) doc.get("graph");
		List<Map<String, Object>> nodes = (List<Map<String, Object>>) graph.get("nodes");
		if (nodes != null) {
			for (Map<String, Object> node : nodes) {
				if (nodeId.equals(node.get("id"))) return node;
			}
		}
		return null;
	}

	private Map<String, String> buildScope(String contextRunId) {
		Map<String, String> scope = AutomationExecutionUtils.buildInitialScope(null);

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

	@Override
	public String getReactorDescription() {
		return "Executes a single automation node in isolation for testing — result is not persisted.";
	}
}
