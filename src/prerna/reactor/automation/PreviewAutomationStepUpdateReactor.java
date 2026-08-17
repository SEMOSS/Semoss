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
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Generates an in-memory replacement for an existing automation Python step without writing it.
 *
 * <p>Pixel: {@code PreviewAutomationStepUpdate(project=["appId"], nodeId=["summarize"],
 * nodeType=["model-engine"], config=["base64json"])}
 */
public class PreviewAutomationStepUpdateReactor extends AbstractAutomationStepSourceReactor {

	private static final Logger classLogger = LogManager.getLogger(PreviewAutomationStepUpdateReactor.class);

	private static final String RESULT_STEP_REF = "stepRef";
	private static final String RESULT_CURRENT_SOURCE = "currentSource";
	private static final String RESULT_CURRENT_SOURCE_HASH = "currentSourceHash";
	private static final String RESULT_PROPOSED_SOURCE = "proposedSource";
	private static final String RESULT_PROPOSED_SOURCE_HASH = "proposedSourceHash";
	private static final String RESULT_SETUP_HASH = "setupHash";
	private static final String RESULT_TEMPLATE_VERSION = "templateVersion";
	private static final String RESULT_CHANGED = "changed";

	public PreviewAutomationStepUpdateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), NODE_ID_KEY, NODE_TYPE_KEY, CONFIG_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		PreparedStep step = prepareStep();
		AutomationStepGenerationService.Preview preview =
				AutomationStepGenerationService.preview(readCurrentSource(step), step.getGenerated());
		classLogger.info("Previewed automation step update: project={}, stepRef={}, changed={}",
				step.getProjectId(), step.getStepRef(), preview.isChanged());
		return new NounMetadata(result(step, preview), PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static Map<String, Object> result(PreparedStep step, AutomationStepGenerationService.Preview preview) {
		AutomationStepGenerationService.GeneratedStep proposed = preview.getProposed();
		Map<String, Object> result = new LinkedHashMap<>();
		result.put(RESULT_STEP_REF, step.getStepRef());
		result.put(RESULT_CURRENT_SOURCE, preview.getCurrentSource());
		result.put(RESULT_CURRENT_SOURCE_HASH, preview.getCurrentSourceHash());
		result.put(RESULT_PROPOSED_SOURCE, proposed.getSource());
		result.put(RESULT_PROPOSED_SOURCE_HASH, proposed.getSourceHash());
		result.put(RESULT_SETUP_HASH, proposed.getSetupHash());
		result.put(RESULT_TEMPLATE_VERSION, proposed.getTemplateVersion());
		result.put(RESULT_CHANGED, preview.isChanged());
		return result;
	}

	@Override
	public String getReactorDescription() {
		return "Validates a proposed managed Python step update and returns an in-memory source diff payload "
				+ "without modifying the project.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "project" -> "The automation project ID that owns the existing Python step.";
			case NODE_ID_KEY -> "A safe node identifier for the existing automation/steps/<nodeId>.py source.";
			case NODE_TYPE_KEY -> "Canvas node type used to validate and generate the proposed source.";
			case CONFIG_KEY -> "JSON node configuration, optionally base64-encoded, for the proposed source.";
			default -> super.getDescriptionForKey(key);
		};
	}
}
