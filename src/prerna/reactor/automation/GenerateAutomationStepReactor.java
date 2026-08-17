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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Generates and persists the initial managed Python implementation for one automation node.
 *
 * <p>Pixel: {@code GenerateAutomationStep(project=["appId"], nodeId=["summarize"],
 * nodeType=["model-engine"], config=["base64json"])}
 *
 * <p>Initial creation never replaces an existing step. Use {@code PreviewAutomationStepUpdate}
 * followed by {@code ApplyAutomationStepUpdate} to explicitly replace an existing source file.
 */
public class GenerateAutomationStepReactor extends AbstractAutomationStepSourceReactor {

	private static final Logger classLogger = LogManager.getLogger(GenerateAutomationStepReactor.class);

	private static final String RESULT_SOURCE = "source";
	private static final String RESULT_STEP_REF = "stepRef";
	private static final String RESULT_ACTION_ID = "actionId";
	private static final String RESULT_DESCRIPTION = "description";
	private static final String RESULT_USAGE = "usage";
	private static final String RESULT_SOURCE_HASH = "sourceHash";
	private static final String RESULT_SETUP_HASH = "setupHash";
	private static final String RESULT_TEMPLATE_VERSION = "templateVersion";

	public GenerateAutomationStepReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), NODE_ID_KEY, NODE_TYPE_KEY, CONFIG_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		PreparedStep step = prepareStep();
		if (sourceExists(step)) {
			throw new IllegalArgumentException("Automation step source already exists: " + step.getStepRef()
					+ ". Preview and apply an explicit update instead.");
		}

		AutomationStepGenerationService.GeneratedStep generated = step.getGenerated();
		saveSource(step, generated.getSource(), "Generate automation Python step");
		classLogger.info("Generated automation step: project={}, stepRef={}, action={}",
				step.getProjectId(), step.getStepRef(), generated.getActionId());
		return new NounMetadata(result(step), PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static Map<String, Object> result(PreparedStep step) {
		AutomationStepGenerationService.GeneratedStep generated = step.getGenerated();
		Map<String, Object> result = new LinkedHashMap<>();
		result.put(RESULT_SOURCE, generated.getSource());
		result.put(RESULT_STEP_REF, step.getStepRef());
		result.put(RESULT_ACTION_ID, generated.getActionId());
		result.put(RESULT_DESCRIPTION, generated.getDescription());
		result.put(RESULT_USAGE, generated.getUsage());
		result.put(RESULT_SOURCE_HASH, generated.getSourceHash());
		result.put(RESULT_SETUP_HASH, generated.getSetupHash());
		result.put(RESULT_TEMPLATE_VERSION, generated.getTemplateVersion());
		return result;
	}

	@Override
	public String getReactorDescription() {
		return "Generates a validated, managed Python action step only when its project asset does not already "
				+ "exist. Returns source, action metadata, deterministic hashes, and template version.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "project" -> "The automation project ID that will own the generated Python step.";
			case NODE_ID_KEY -> "A safe node identifier used to create automation/steps/<nodeId>.py.";
			case NODE_TYPE_KEY -> "Canvas node type: model-engine, database-engine, vector-engine, "
					+ "storage-engine, function-engine, app, or python-step.";
			case CONFIG_KEY -> "JSON node configuration, optionally base64-encoded. "
					+ "Supported actions and runtime input mappings are returned in the usage text.";
			default -> super.getDescriptionForKey(key);
		};
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.AUTO.getValue());
		return meta;
	}
}
