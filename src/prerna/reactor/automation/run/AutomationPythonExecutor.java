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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationRuntime;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/**
 * Owns lazy Python acquisition and source invocation for one Automation run.
 */
final class AutomationPythonExecutor {

	private final Insight insight;
	private final String runId;
	/**
	 * Java object identities already copied into the execution Insight's Python
	 * namespace. The map lives only for this executor/run and avoids retransmitting
	 * unchanged JSON values to later developer nodes.
	 */
	private final Map<String, Object> retainedValueReferences = new LinkedHashMap<>();
	private PyTranslator translator;

	AutomationPythonExecutor(Insight insight, String runId) {
		if (insight == null || runId == null || runId.isBlank()) {
			throw new IllegalArgumentException("Automation Python execution requires an Insight and run identifier.");
		}
		this.insight = insight;
		this.runId = runId;
	}

	AutomationRuntime.NodeResult executeNode(Map<String, Object> executionDescriptor, String source,
			Map<String, Object> scope, String outputVariable, String assetsFolder, String pyFolder) {
		String nodeType = String.valueOf(executionDescriptor.get(AutomationConstants.NODE_FIELD_TYPE));
		if (!AutomationConstants.NODE_DEVELOPER_PYTHON.equals(nodeType)) {
			throw new IllegalStateException(
					"Python execution requires an explicit developer.python descriptor; received '" + nodeType + "'.");
		}
		if (source == null || source.isBlank()) {
			throw new IllegalArgumentException("Automation node Python source must not be blank.");
		}
		Map<String, Object> pythonScope = new LinkedHashMap<>();
		Map<String, String> frameBindings = new LinkedHashMap<>();
		Set<String> valueBindings = new LinkedHashSet<>();
		for (Map.Entry<String, Object> entry : scope.entrySet()) {
			Object value = entry.getValue();
			if (value instanceof AutomationFrameOutput.RegisteredFrame frame) {
				pythonScope.put(entry.getKey(), frame.summary());
				frameBindings.put(entry.getKey(), frame.backend());
			} else if (retainedValueReferences.containsKey(entry.getKey())
					&& retainedValueReferences.get(entry.getKey()) == value) {
				pythonScope.put(entry.getKey(), null);
				valueBindings.add(entry.getKey());
			} else {
				pythonScope.put(entry.getKey(), value);
			}
		}
		// The Java execution service validates the complete logical scope at run
		// start, node and loop finalization, and resume. This boundary serializes only
		// changed values and retained aliases; revalidating unchanged collections here
		// makes loop cost grow with the accumulated scope rather than the current item.
		Object raw = requireTranslator().runScriptWithExplicitAssetPaths(insight,
				AutomationRuntime.buildNodeInvocationScript(source, pythonScope, outputVariable, frameBindings,
						valueBindings),
				assetsFolder, new String[] { pyFolder });
		AutomationRuntime.NodeResult result = AutomationRuntime.decodeNodeResult(raw);
		if (result.frame()) {
			retainedValueReferences.remove(outputVariable);
			try {
				return new AutomationRuntime.NodeResult(
						AutomationFrameOutput.registerPythonVariable(insight, outputVariable), true);
			} catch (RuntimeException registrationError) {
				requireTranslator().runScript(
						"globals().pop(" + AutomationRuntimeUtils.GSON.toJson(outputVariable) + ", None)");
				throw registrationError;
			}
		}
		for (Map.Entry<String, Object> entry : scope.entrySet()) {
			if (!(entry.getValue() instanceof AutomationFrameOutput.RegisteredFrame)) {
				retainedValueReferences.put(entry.getKey(), entry.getValue());
			}
		}
		if (outputVariable != null && !outputVariable.isBlank()) {
			retainedValueReferences.put(outputVariable, result.value());
		}
		return result;
	}

	private synchronized PyTranslator requireTranslator() {
		if (translator == null) {
			PyTranslator acquiredTranslator = insight.getPyTranslator();
			if (acquiredTranslator == null) {
				throw new IllegalStateException("Python runtime is not available for this insight.");
			}
			AutomationRunRegistry.attachPythonTranslator(runId, acquiredTranslator);
			translator = acquiredTranslator;
		}
		return translator;
	}
}
