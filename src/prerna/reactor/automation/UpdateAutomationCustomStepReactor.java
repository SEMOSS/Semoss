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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.reactor.automation;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Updates source only for an explicitly custom node after an optimistic-lock check. */
public class UpdateAutomationCustomStepReactor extends AbstractReactor {

	private static final int MAX_SOURCE_LENGTH = 100_000;
	private static final String NODE_ID_KEY = "nodeId";
	private static final String SOURCE_KEY = "source";
	private static final String EXPECTED_SOURCE_HASH_KEY = "expectedSourceHash";

	public UpdateAutomationCustomStepReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PROJECT.getKey(), NODE_ID_KEY, SOURCE_KEY, EXPECTED_SOURCE_HASH_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = editableProjectId();
		String nodeId = required(NODE_ID_KEY);
		String source = required(SOURCE_KEY);
		if (source.length() > MAX_SOURCE_LENGTH) {
			throw new IllegalArgumentException("source exceeds the maximum length.");
		}
		String expectedHash = required(EXPECTED_SOURCE_HASH_KEY);
		return AutomationProjectUtils.withLockedDefinition(projectId,
				files -> updateSource(projectId, files, nodeId, source, expectedHash));
	}

	private NounMetadata updateSource(String projectId, AutomationDefinitionService.DefinitionFiles files,
			String nodeId, String source, String expectedHash) {
		validateCustomNode(files.definition(), nodeId);
		String currentSource = files.nodeSources().get(nodeId);
		if (!AutomationDefinitionService.calculateSourceHash(currentSource).equals(expectedHash)) {
			throw new IllegalArgumentException("Automation node source changed; refresh before updating it.");
		}

		Map<String, String> updatedSources = new LinkedHashMap<>(files.nodeSources());
		updatedSources.put(nodeId, source);
		AutomationDefinitionService.DefinitionFiles saved = AutomationProjectUtils.saveDefinition(projectId,
				files.definition(), updatedSources, this.insight.getUser());
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("nodeId", nodeId);
		result.put("sourceHash", AutomationDefinitionService.calculateSourceHash(saved.nodeSources().get(nodeId)));
		result.put(AutomationConstants.RESULT_REVISION,
				AutomationDefinitionService.calculateRevision(saved.definition(), saved.nodeSources()));
		return new NounMetadata(result,
				PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private String editableProjectId() {
		return AutomationProjectUtils.getEditableAutomationProject(this.insight.getUser(),
				required(ReactorKeysEnum.PROJECT.getKey())).getProjectId();
	}

	private String required(String key) {
		String value = this.keyValue.get(key);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must provide " + key + ".");
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	private static void validateCustomNode(String definition, String nodeId) {
		Map<String, Object> document = AutomationRuntimeUtils.GSON.fromJson(definition,
				AutomationRuntimeUtils.MAP_TYPE);
		Map<String, Object> graph = (Map<String, Object>) document.get(AutomationConstants.DOC_GRAPH);
		Object rawNodes = graph.get(AutomationConstants.DOC_NODES);
		if (!(rawNodes instanceof java.util.List<?> nodes)) {
			throw new IllegalArgumentException("Automation graph is invalid.");
		}
		for (Object value : nodes) {
			if (value instanceof Map<?, ?> node
					&& nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				if (!AutomationConstants.NODE_CODE_MODE_CUSTOM.equals(
						node.get(AutomationConstants.NODE_FIELD_CODE_MODE))) {
					throw new IllegalArgumentException("Only custom automation nodes may receive source updates.");
				}
				return;
			}
		}
		throw new IllegalArgumentException("Automation does not contain node: " + nodeId);
	}

	@Override
	public String getReactorDescription() {
		return "Updates one custom automation Python node after verifying its current source hash.";
	}
}
