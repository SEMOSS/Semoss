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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.automation.utils.AutomationExecutionUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Replaces source for an existing custom Python automation step when the caller proves the
 * source has not changed since it was read or created.
 */
public class UpdateAutomationCustomStepReactor extends AbstractReactor {

	private static final String NODE_ID_KEY = "nodeId";
	private static final String SOURCE_KEY = "source";
	private static final String EXPECTED_SOURCE_HASH_KEY = "expectedSourceHash";
	private static final int MAX_SOURCE_LENGTH = 1_000_000;

	public UpdateAutomationCustomStepReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), NODE_ID_KEY, SOURCE_KEY,
				EXPECTED_SOURCE_HASH_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("You must be signed in to update an automation step.");
		}

		String projectId = requireNonblank(this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()), "project");
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		validateProject(user, projectId);

		String nodeId = requireNonblank(this.keyValue.get(NODE_ID_KEY), NODE_ID_KEY);
		if (!AutomationStepTemplateRegistry.isSafeStepNodeId(nodeId)) {
			throw new IllegalArgumentException("nodeId must be a safe automation step identifier.");
		}
		String source = requireNonblank(this.keyValue.get(SOURCE_KEY), SOURCE_KEY);
		if (source.length() > MAX_SOURCE_LENGTH) {
			throw new IllegalArgumentException("Automation step source exceeds the maximum size.");
		}
		String expectedSourceHash = requireNonblank(this.keyValue.get(EXPECTED_SOURCE_HASH_KEY),
				EXPECTED_SOURCE_HASH_KEY);

		String stepRef = AutomationConstants.AUTOMATION_STEPS_FOLDER + "/" + nodeId + ".py";
		validateCustomStep(projectId, nodeId, stepRef);
		String currentSource = AutomationStepSourceService.readSource(projectId, stepRef);
		String currentHash = AutomationStepGenerationService.sha256(currentSource);
		if (!currentHash.equals(expectedSourceHash)) {
			throw new IllegalArgumentException(
					"Automation step source changed since it was read. Refresh the source before updating it.");
		}

		AutomationStepSourceService.saveSource(this.insight, projectId, stepRef, source,
				"Update custom automation Python step");
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("stepRef", stepRef);
		result.put("sourceHash", AutomationStepGenerationService.sha256(source));
		result.put("changed", !source.equals(currentSource));
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static void validateProject(User user, String projectId) {
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have edit access.");
		}
		IProject project = Utility.getProject(projectId);
		if (project == null || project.getProjectType() != IProject.PROJECT_TYPE.AUTOMATION) {
			throw new IllegalArgumentException("Project is not an automation project: " + projectId);
		}
	}

	@SuppressWarnings("unchecked")
	private static void validateCustomStep(String projectId, String nodeId, String stepRef) {
		Map<String, Object> document = AutomationExecutionUtils.loadAutomationDoc(projectId);
		Object graphValue = document.get(AutomationConstants.DOC_GRAPH);
		if (!(graphValue instanceof Map<?, ?> graph)) {
			throw new IllegalArgumentException("Automation definition graph is invalid.");
		}
		Object nodesValue = graph.get(AutomationConstants.DOC_NODES);
		if (!(nodesValue instanceof List<?> nodes)) {
			throw new IllegalArgumentException("Automation definition nodes are invalid.");
		}
		for (Object candidate : nodes) {
			if (!(candidate instanceof Map<?, ?> node)
					|| !nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				continue;
			}
			if (!AutomationConstants.NODE_PYTHON_STEP.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				throw new IllegalArgumentException("Only custom python-step nodes may receive custom source updates.");
			}
			Object configValue = node.get(AutomationConstants.NODE_FIELD_CONFIG);
			if (!(configValue instanceof Map<?, ?> config)
					|| !stepRef.equals(config.get(AutomationConstants.CONFIG_STEP_REF))) {
				throw new IllegalArgumentException("Automation step source does not match its graph definition.");
			}
			return;
		}
		throw new IllegalArgumentException("Automation definition does not contain custom step: " + nodeId + ".");
	}

	private static String requireNonblank(String value, String key) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must provide " + key + ".");
		}
		return value;
	}

	@Override
	public String getReactorDescription() {
		return "Updates an existing custom python-step source after verifying its current SHA-256 hash.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "project" -> "The Automation project that owns the custom Python step.";
			case NODE_ID_KEY -> "The existing python-step node identifier.";
			case SOURCE_KEY -> "Complete Python source for the custom step.";
			case EXPECTED_SOURCE_HASH_KEY -> "SHA-256 hash of the source being replaced.";
			default -> super.getDescriptionForKey(key);
		};
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		return Map.of(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.AUTO.getValue());
	}
}
