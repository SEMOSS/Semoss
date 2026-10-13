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
package prerna.reactor.automation.run;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationRuntime;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns one immutable source snapshot after project authorization.
 */
public class GetAutomationRunSourceReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";
	private static final String NODE_ID_KEY = "nodeId";

	public GetAutomationRunSourceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), RUN_ID_KEY, NODE_ID_KEY };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getString(ReactorKeysEnum.PROJECT.getKey());
		String runId = getString(RUN_ID_KEY);
		String nodeId = getString(NODE_ID_KEY);
		projectId = AutomationProjectService.getViewableAutomationProject(this.insight.getUser(), projectId)
				.getProjectId();
		Map<String, Object> run = AutomationRunStore.getRunDetail(runId);
		if (run == null || !projectId.equals(run.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Automation run not found: " + runId);
		}
		Object snapshotValue = run.get(AutomationConstants.DEFINITION_SNAPSHOT);
		if (!(snapshotValue instanceof String snapshot) || snapshot.isBlank()) {
			throw new IllegalStateException("Automation run has no immutable definition snapshot.");
		}
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidateLegacyRunSnapshot(snapshot);
		Map<String, Object> node = findNode(definition, nodeId);
		AutomationExecutionContract contract = AutomationExecutionContract.forExecution(
				AutomationRunStore.getRunExecutionContract(runId), definition);
		AutomationExecutionContract.Selection selection = contract.selection(nodeId);
		String source = AutomationRunStore.getRunNodeSources(runId).get(nodeId);
		Map<String, Object> response = new LinkedHashMap<>();
		response.put(AutomationConstants.RUN_ID, runId);
		response.put(AutomationConstants.NODE_ID, nodeId);
		if (AutomationExecutionContract.IMPLEMENTATION_PYTHON_TABLE_V2.equals(selection.implementation())) {
			if (source == null) {
				throw new IllegalStateException("Automation Python source snapshot not found for node: " + nodeId);
			}
			response.putAll(pythonSourceIdentity(nodeId, source));
		} else {
			response.put("viewKind", "IMPLEMENTATION");
			response.put("legacyPython", false);
			response.put("nodeType", node.get(AutomationConstants.NODE_FIELD_TYPE));
			response.put("config", node.getOrDefault(AutomationConstants.NODE_FIELD_CONFIG, Map.of()));
			response.put("implementationId", selection.implementation());
			response.put("definitionHash", AutomationExecutionContract.nodeDefinitionHash(node));
		}
		return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	static Map<String, Object> pythonSourceIdentity(String nodeId, String source) {
		Map<String, Object> response = new LinkedHashMap<>();
		response.put("viewKind", "PYTHON");
		response.put("legacyPython", false);
		response.put("nodeType", AutomationConstants.NODE_DEVELOPER_PYTHON);
		response.put("sourceIdentity", AutomationConstants.NODE_DEVELOPER_PYTHON);
		response.put("sourceNodeId", nodeId);
		response.put(AutomationConstants.SOURCE_CODE, source);
		return response;
	}

	private static Map<String, Object> findNode(AutomationDefinitionValidator.ValidatedDefinition definition,
			String nodeId) {
		List<Map<String, Object>> nodes = prerna.reactor.automation.AutomationRuntime.allNodes(definition);
		return nodes.stream()
				.filter(node -> nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID)))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException(
						"Automation run definition does not contain node: " + nodeId));
	}
}
