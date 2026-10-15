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
import java.util.Map;

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/**
 * Classifies and bounds executor-neutral node results before persistence.
 */
final class AutomationNodeFinalizationService {

	private AutomationNodeFinalizationService() {
	}

	static ClassifiedResult classify(Map<String, Object> node, Object value) {
		String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
		boolean internalResultType = AutomationConstants.NODE_MODEL_CHAT.equals(type)
				|| AutomationConstants.NODE_MODEL_VISION.equals(type)
				|| AutomationConstants.NODE_AGENT_RUN.equals(type);
		boolean generated = !AutomationConstants.NODE_CODE_MODE_CUSTOM
				.equals(node.get(AutomationConstants.NODE_FIELD_CODE_MODE));
		if (!internalResultType || !generated || !(value instanceof Map<?, ?> map)) {
			return new ClassifiedResult(value, value);
		}
		boolean hasValue = map.containsKey(AutomationConstants.INTERNAL_RESULT_VALUE);
		boolean hasMetadata = map.containsKey(AutomationConstants.INTERNAL_RESULT_METADATA);
		if (!hasValue && !hasMetadata) {
			return new ClassifiedResult(value, value);
		}
		if (!hasValue || !hasMetadata) {
			throw new IllegalStateException("Generated automation node returned an incomplete internal result.");
		}
		return new ClassifiedResult(map.get(AutomationConstants.INTERNAL_RESULT_VALUE),
				map.get(AutomationConstants.INTERNAL_RESULT_METADATA));
	}

	static PreparedValue prepare(String nodeId, String outputVariable, Object value, Map<String, Object> scope,
			boolean validateScope) {
		String output = AutomationRuntimeUtils.toBoundedRuntimeJson(value,
				AutomationConstants.NODE_OUTPUT_MAX_BYTES, "Automation node '" + nodeId + "' output");
		if (validateScope && outputVariable != null) {
			Map<String, Object> prospectiveScope = new LinkedHashMap<>();
			for (Map.Entry<String, Object> entry : scope.entrySet()) {
				Object scopeValue = entry.getValue();
				prospectiveScope.put(entry.getKey(), scopeValue instanceof AutomationFrameOutput.RegisteredFrame frame
						? frame.summary()
						: scopeValue);
			}
			prospectiveScope.put(outputVariable, value);
			AutomationRuntimeUtils.toBoundedRuntimeJson(prospectiveScope, AutomationConstants.RUN_SCOPE_MAX_BYTES,
					"Automation run scope");
		}
		return new PreparedValue(output, AutomationRuntimeUtils.generatePreview(output));
	}

	record ClassifiedResult(Object value, Object metadata) {
	}

	record PreparedValue(String output, String preview) {
	}
}
