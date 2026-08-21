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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Reconfigures a generated node and regenerates only that node's managed source. */
public class UpdateAutomationStepReactor extends AbstractReactor {

	private static final String NODE_ID_KEY = "nodeId";
	private static final String CONFIG_KEY = "config";
	private static final String LABEL_KEY = "label";

	public UpdateAutomationStepReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PROJECT.getKey(), NODE_ID_KEY, CONFIG_KEY, LABEL_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = editableProjectId();
		String nodeId = required(NODE_ID_KEY);
		Map<String, Object> config = parseConfig(required(CONFIG_KEY));
		String label = this.keyValue.get(LABEL_KEY);
		return AutomationProjectUtils.withLockedDefinition(projectId,
				files -> updateStep(projectId, files, nodeId, config, label));
	}

	private NounMetadata updateStep(String projectId, AutomationDefinitionService.DefinitionFiles files,
			String nodeId, Map<String, Object> config, String label) {
		@SuppressWarnings("unchecked")
		Map<String, Object> document = AutomationRuntimeUtils.GSON.fromJson(files.definition(),
				AutomationRuntimeUtils.MAP_TYPE);
		@SuppressWarnings("unchecked")
		Map<String, Object> graph = (Map<String, Object>) document.get(AutomationConstants.DOC_GRAPH);
		Object rawNodes = graph.get(AutomationConstants.DOC_NODES);
		if (!(rawNodes instanceof java.util.List<?> nodes)) {
			throw new IllegalArgumentException("Automation graph is invalid.");
		}

		Map<String, Object> updatedNode = null;
		for (Object value : nodes) {
			if (!(value instanceof Map<?, ?> node)
					|| !nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				continue;
			}
			if (AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				throw new IllegalArgumentException("The trigger node cannot be reconfigured through this tool.");
			}
			if (AutomationConstants.NODE_CODE_MODE_CUSTOM.equals(node.get(AutomationConstants.NODE_FIELD_CODE_MODE))) {
				throw new IllegalArgumentException("Custom nodes must be updated with UpdateAutomationCustomStep.");
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> mutable = (Map<String, Object>) node;
			mutable.put(AutomationConstants.NODE_FIELD_CONFIG, config);
			mutable.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
			if (label != null && !label.isBlank()) {
				mutable.put(AutomationConstants.NODE_FIELD_LABEL, label);
			}
			updatedNode = mutable;
			break;
		}
		if (updatedNode == null) {
			throw new IllegalArgumentException("Automation does not contain node: " + nodeId);
		}

		Map<String, String> sources = new LinkedHashMap<>(files.nodeSources());
		sources.remove(nodeId);
		AutomationDefinitionService.DefinitionFiles saved = AutomationProjectUtils.saveDefinition(projectId,
				AutomationRuntimeUtils.GSON.toJson(document), sources, this.insight.getUser());
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("node", updatedNode);
		result.put("source", saved.nodeSources().get(nodeId));
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
	private static Map<String, Object> parseConfig(String rawOrBase64) {
		String raw;
		try {
			raw = new String(Base64.getDecoder().decode(rawOrBase64), StandardCharsets.UTF_8);
		} catch (IllegalArgumentException ignored) {
			raw = rawOrBase64;
		}
		Object parsed = AutomationRuntimeUtils.GSON.fromJson(raw, Object.class);
		if (!(parsed instanceof Map<?, ?>)) {
			throw new IllegalArgumentException("config must be a JSON object.");
		}
		return new LinkedHashMap<>((Map<String, Object>) parsed);
	}

	@Override
	public String getReactorDescription() {
		return "Updates configuration for one generated automation node.";
	}
}
