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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.automation.AutomationConstants;

/**
 * Indexes the scalar variables a run sets so an operator can find a run, and the
 * loop iteration within it, from a business identifier such as a tracking number.
 *
 * <p>
 * Only short scalar values are kept, flattened to dotted names such as
 * {@code item.trackingNumber}. Lists, frames, and long values are never indexed;
 * the node output remains the record of the full payload. Indexing is best
 * effort: a failure is logged and never fails the run.
 */
final class AutomationRunVariables {

	private static final Logger classLogger = LogManager.getLogger(AutomationRunVariables.class);

	private AutomationRunVariables() {
	}

	/**
	 * Flattens variables into at most
	 * {@link AutomationConstants#VARIABLES_PER_NODE_MAX} dotted names with bounded
	 * string values.
	 *
	 * @param variables variable names and values set by one node execution
	 * @return indexable name and value pairs in encounter order
	 */
	static Map<String, String> flatten(Map<String, Object> variables) {
		Map<String, String> flattened = new LinkedHashMap<>();
		if (variables != null) {
			for (Map.Entry<String, Object> variable : variables.entrySet()) {
				flattenValue(variable.getKey(), variable.getValue(), 0, flattened);
			}
		}
		return flattened;
	}

	private static void flattenValue(String name, Object value, int depth, Map<String, String> flattened) {
		if (name == null || name.isBlank() || value == null
				|| flattened.size() >= AutomationConstants.VARIABLES_PER_NODE_MAX) {
			return;
		}
		if (value instanceof String || value instanceof Number || value instanceof Boolean) {
			String text = String.valueOf(value);
			if (text.length() > AutomationConstants.VARIABLE_VALUE_MAX_LENGTH) {
				text = text.substring(0, AutomationConstants.VARIABLE_VALUE_MAX_LENGTH);
			}
			flattened.put(name.length() > 255 ? name.substring(0, 255) : name, text);
		} else if (value instanceof Map<?, ?> map && depth < AutomationConstants.VARIABLE_FLATTEN_MAX_DEPTH) {
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				flattenValue(name + "." + entry.getKey(), entry.getValue(), depth + 1, flattened);
			}
		}
	}

	/**
	 * Indexes the variables one node execution set.
	 *
	 * @param runId          owning run
	 * @param nodeId         node execution row that set the variables
	 * @param iterationIndex zero-based loop iteration, or {@code null} outside a loop
	 * @param variables      variable names and values the node set
	 */
	static void record(String runId, String nodeId, Integer iterationIndex, Map<String, Object> variables) {
		try {
			AutomationRunStore.insertVariables(runId, nodeId, iterationIndex, flatten(variables));
		} catch (Exception e) {
			classLogger.warn("Unable to index variables for automation run '{}', node '{}'", runId, nodeId, e);
		}
	}
}
