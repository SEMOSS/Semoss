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
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Validates and saves an automation graph definition and its per-node Python sources.
 *
 * <p>Pixel: {@code SaveAutomation(project=["appId"], json=["<base64-json>"],
 * nodeSources=["<optional-base64-json-map>"])}
 */
public class SaveAutomationReactor extends AbstractReactor {

	private static final String NODE_SOURCES_KEY = AutomationConstants.DOC_NODE_SOURCES;

	public SaveAutomationReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.JSON.getKey(),
				NODE_SOURCES_KEY,
				AutomationConstants.EXPECTED_REVISION_KEY };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String definition = decodeRequired(this.keyValue.get(ReactorKeysEnum.JSON.getKey()),
				"Must provide a nonblank automation definition.");
		Map<String, String> nodeSources = decodeNodeSources(this.keyValue.get(NODE_SOURCES_KEY));
		if (projectId == null || projectId.isBlank()) {
			throw new IllegalArgumentException("Must provide a project id.");
		}

		projectId = AutomationProjectUtils.getEditableAutomationProject(this.insight.getUser(), projectId)
				.getProjectId();

		AutomationDefinitionService.DefinitionFiles files = AutomationProjectUtils.saveDefinition(projectId,
				definition, nodeSources, this.keyValue.get(AutomationConstants.EXPECTED_REVISION_KEY),
				this.insight.getUser());

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("saved", true);
		result.put(AutomationConstants.DOC_NODE_SOURCES, files.nodeSources());
		result.put(AutomationConstants.RESULT_REVISION,
				AutomationDefinitionService.calculateRevision(files.definition(), files.nodeSources()));
		result.put(AutomationConstants.DOC_GLOBALS, AutomationRuntime.declaredGlobals(
				AutomationDefinitionValidator.parseAndValidate(files.definition()), files.nodeSources()));
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static String decodeRequired(String value, String error) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(error);
		}
		return decode(value);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, String> decodeNodeSources(String value) {
		if (value == null || value.isBlank()) {
			return Map.of();
		}
		Object parsed = AutomationRuntimeUtils.GSON.fromJson(decode(value), Object.class);
		if (!(parsed instanceof Map<?, ?> raw)) {
			throw new IllegalArgumentException("nodeSources must be a JSON object keyed by node id.");
		}
		Map<String, String> sources = new LinkedHashMap<>();
		for (Map.Entry<?, ?> entry : raw.entrySet()) {
			if (!(entry.getKey() instanceof String nodeId) || !(entry.getValue() instanceof String source)) {
				throw new IllegalArgumentException("nodeSources must map node ids to Python source strings.");
			}
			sources.put(nodeId, source);
		}
		return sources;
	}

	private static String decode(String value) {
		try {
			return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
		} catch (IllegalArgumentException ignored) {
			return value;
		}
	}

	@Override
	public String getReactorDescription() {
		return "Validates and saves an automation graph with Python source for each non-start node.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "The project ID or alias of the automation to save.";
		}
		if (ReactorKeysEnum.JSON.getKey().equals(key)) {
			return "Base64-encoded workflow graph JSON.";
		}
		if (NODE_SOURCES_KEY.equals(key)) {
			return "Optional raw or Base64-encoded JSON object: { nodeId: Python source }. "
					+ "The trigger source may declare non-private globals; omitted entries receive generated source.";
		}
		if (AutomationConstants.EXPECTED_REVISION_KEY.equals(key)) {
			return "Optional revision returned by GetAutomation. The save is rejected if the automation changed.";
		}
		return super.getDescriptionForKey(key);
	}
}
