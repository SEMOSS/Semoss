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
 * Applies an approved automation Python step update if its previewed source is still current.
 *
 * <p>Pixel: {@code ApplyAutomationStepUpdate(project=["appId"], nodeId=["summarize"],
 * nodeType=["model-engine"], config=["base64json"], expectedSourceHash=["sha256"])}
 */
public class ApplyAutomationStepUpdateReactor extends AbstractAutomationStepSourceReactor {

	private static final Logger classLogger = LogManager.getLogger(ApplyAutomationStepUpdateReactor.class);

	private static final String EXPECTED_SOURCE_HASH_KEY = "expectedSourceHash";
	private static final String RESULT_STEP_REF = "stepRef";
	private static final String RESULT_SOURCE_HASH = "sourceHash";
	private static final String RESULT_SETUP_HASH = "setupHash";
	private static final String RESULT_TEMPLATE_VERSION = "templateVersion";
	private static final String RESULT_CHANGED = "changed";

	public ApplyAutomationStepUpdateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), NODE_ID_KEY, NODE_TYPE_KEY, CONFIG_KEY,
				EXPECTED_SOURCE_HASH_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		PreparedStep step = prepareStep();
		String expectedSourceHash = requireNonblank(this.keyValue.get(EXPECTED_SOURCE_HASH_KEY),
				EXPECTED_SOURCE_HASH_KEY);
		String currentSource = readCurrentSource(step);
		String currentSourceHash = AutomationStepGenerationService.sha256(currentSource);
		if (!currentSourceHash.equals(expectedSourceHash)) {
			throw new IllegalArgumentException("Automation step source changed after preview. Preview the update again.");
		}

		AutomationStepGenerationService.GeneratedStep generated = step.getGenerated();
		boolean changed = !currentSource.equals(generated.getSource());
		if (changed) {
			saveSource(step, generated.getSource(), "Apply automation Python step update");
		}
		classLogger.info("Applied automation step update: project={}, stepRef={}, changed={}",
				step.getProjectId(), step.getStepRef(), changed);
		return new NounMetadata(result(step, changed), PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static Map<String, Object> result(PreparedStep step, boolean changed) {
		AutomationStepGenerationService.GeneratedStep generated = step.getGenerated();
		Map<String, Object> result = new LinkedHashMap<>();
		result.put(RESULT_STEP_REF, step.getStepRef());
		result.put(RESULT_SOURCE_HASH, generated.getSourceHash());
		result.put(RESULT_SETUP_HASH, generated.getSetupHash());
		result.put(RESULT_TEMPLATE_VERSION, generated.getTemplateVersion());
		result.put(RESULT_CHANGED, changed);
		return result;
	}

	@Override
	public String getReactorDescription() {
		return "Applies an explicitly approved managed Python step update only when the current source SHA-256 "
				+ "matches expectedSourceHash from its preview.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "project" -> "The automation project ID that owns the Python step.";
			case NODE_ID_KEY -> "A safe node identifier for the existing automation/steps/<nodeId>.py source.";
			case NODE_TYPE_KEY -> "Canvas node type used to validate and generate the proposed source.";
			case CONFIG_KEY -> "JSON node configuration, optionally base64-encoded, for the proposed source.";
			case EXPECTED_SOURCE_HASH_KEY -> "SHA-256 of currentSource returned by PreviewAutomationStepUpdate.";
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
