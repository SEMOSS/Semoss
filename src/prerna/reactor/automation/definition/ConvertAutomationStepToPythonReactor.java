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
package prerna.reactor.automation.definition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Atomically converts one renderer-owned typed node to explicit Python source.
 */
public class ConvertAutomationStepToPythonReactor extends AbstractReactor {

	private static final String NODE_ID_KEY = "nodeId";
	private static final String EXPECTED_REVISION_KEY = "expectedRevision";
	private static final String DEFAULT_SOURCE = """
			# This node was explicitly converted to Python.
			# Implement run(scope) and return the value published under this node's output alias.
			def run(scope):
			    return {}
			""";

	public ConvertAutomationStepToPythonReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PROJECT.getKey(), NODE_ID_KEY, EXPECTED_REVISION_KEY };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = AutomationProjectService.getEditableAutomationProject(this.insight.getUser(),
				required(ReactorKeysEnum.PROJECT.getKey())).getProjectId();
		String nodeId = required(NODE_ID_KEY);
		String expectedRevision = required(EXPECTED_REVISION_KEY);
		return AutomationProjectService.withLockedDefinition(projectId,
				files -> convert(projectId, files, nodeId, expectedRevision));
	}

	private NounMetadata convert(String projectId, AutomationDefinitionService.DefinitionFiles files,
			String nodeId, String expectedRevision) {
		String currentRevision =
				AutomationDefinitionService.calculateRevision(files.definition(), files.nodeSources());
		if (!currentRevision.equals(expectedRevision)) {
			throw new IllegalArgumentException("Automation changed; refresh before converting the node.");
		}

		@SuppressWarnings("unchecked")
		Map<String, Object> document = AutomationRuntimeUtils.GSON.fromJson(files.definition(),
				AutomationRuntimeUtils.MAP_TYPE);
		@SuppressWarnings("unchecked")
		Map<String, Object> graph = (Map<String, Object>) document.get(AutomationConstants.DOC_GRAPH);
		Object rawNodes = graph.get(AutomationConstants.DOC_NODES);
		if (!(rawNodes instanceof List<?> nodes)) {
			throw new IllegalArgumentException("Automation graph is invalid.");
		}

		Map<String, Object> selected = null;
		for (Object value : nodes) {
			if (value instanceof Map<?, ?> node && nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				@SuppressWarnings("unchecked")
				Map<String, Object> mutable = (Map<String, Object>) node;
				selected = mutable;
				break;
			}
		}
		if (selected == null) {
			throw new IllegalArgumentException("Automation does not contain node: " + nodeId);
		}
		String type = String.valueOf(selected.get(AutomationConstants.NODE_FIELD_TYPE));
		if (AutomationConstants.NODE_START.equals(type)
				|| AutomationConstants.NODE_CONTROL_IF.equals(type)
				|| AutomationConstants.NODE_CONTROL_JEV.equals(type)
				|| AutomationConstants.NODE_CONTROL_LOOP.equals(type)) {
			throw new IllegalArgumentException("Control-flow node '" + nodeId + "' cannot be converted to Python.");
		}
		if (AutomationConstants.NODE_DEVELOPER_PYTHON.equals(type)) {
			throw new IllegalArgumentException("Automation node '" + nodeId + "' is already developer.python.");
		}

		String source = files.nodeSources().get(nodeId);
		if (source == null) {
			source = DEFAULT_SOURCE;
		}
		Object originalConfig = selected.getOrDefault(AutomationConstants.NODE_FIELD_CONFIG, Map.of());
		selected.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DEVELOPER_PYTHON);
		selected.put(AutomationConstants.NODE_FIELD_CONFIG,
				Map.of("convertedFromType", type, "convertedFromConfig", originalConfig));
		selected.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_CUSTOM);
		Map<String, String> sources = new LinkedHashMap<>(files.nodeSources());
		sources.put(nodeId, source);
		AutomationDefinitionService.DefinitionFiles saved = AutomationProjectService.saveDefinition(projectId,
				AutomationRuntimeUtils.GSON.toJson(document), sources, this.insight.getUser());

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("nodeId", nodeId);
		result.put("nodeType", AutomationConstants.NODE_DEVELOPER_PYTHON);
		result.put("codeMode", AutomationConstants.NODE_CODE_MODE_CUSTOM);
		result.put("source", saved.nodeSources().get(nodeId));
		result.put("sourceHash", AutomationDefinitionService.calculateSourceHash(saved.nodeSources().get(nodeId)));
		result.put(AutomationConstants.RESULT_REVISION,
				AutomationDefinitionService.calculateRevision(saved.definition(), saved.nodeSources()));
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private String required(String key) {
		String value = this.keyValue.get(key);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must provide " + key + ".");
		}
		return value;
	}

	@Override
	public String getReactorDescription() {
		return "Replaces one typed Automation node with an explicit developer.python node.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "The Automation project ID or alias.";
		}
		if (NODE_ID_KEY.equals(key)) {
			return "The generated typed node to convert.";
		}
		if (EXPECTED_REVISION_KEY.equals(key)) {
			return "The current Automation revision, used to reject stale conversions.";
		}
		return super.getDescriptionForKey(key);
	}
}
