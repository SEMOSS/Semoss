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
 * Sends one API Request node's definition from the editor so its author can see
 * the response before running the Automation.
 *
 * <p>
 * Requires edit access to the Automation project. Applies the same validation,
 * destination checks, and Connection permission as a real run. References to
 * other nodes' outputs must be supplied as sample values in {@code inputs}; an
 * unresolved reference is an error.
 *
 * <p>
 * Pixel: {@code TestAutomationRequest(project=["appId"], config=["base64Json"], inputs=["base64Json"])}
 */
public class TestAutomationRequestReactor extends AbstractReactor {

	private static final String CONFIG_KEY = "config";

	public TestAutomationRequestReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), CONFIG_KEY,
				AutomationConstants.AUTOMATION_INPUTS_KEY };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		AutomationProjectService.getEditableAutomationProject(this.insight.getUser(),
				this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()));
		Map<String, Object> config = decodeObject(this.keyValue.get(CONFIG_KEY), "config");
		if (config == null) {
			throw new IllegalArgumentException("config is required.");
		}
		Map<String, Object> samples = decodeObject(this.keyValue.get(AutomationConstants.AUTOMATION_INPUTS_KEY),
				AutomationConstants.AUTOMATION_INPUTS_KEY);
		Map<String, Object> response = AutomationApiRequestExecutor.execute(config,
				samples == null ? Map.of() : samples, this.insight.getUser(), "test");
		return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/** Reads a raw or Base64-encoded JSON object argument, as SaveAutomation does. */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> decodeObject(String value, String name) {
		if (value == null || value.isBlank()) {
			return null;
		}
		Object parsed = AutomationRuntimeUtils.GSON.fromJson(AutomationRuntimeUtils.decodeBase64OrRaw(value),
				Object.class);
		if (!(parsed instanceof Map<?, ?>)) {
			throw new IllegalArgumentException(name + " must be a JSON object.");
		}
		return (Map<String, Object>) parsed;
	}

	@Override
	public String getReactorDescription() {
		return "Sends one API Request node's definition once, using sample values for scope references.";
	}
}
