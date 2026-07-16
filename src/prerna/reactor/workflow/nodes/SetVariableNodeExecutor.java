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
package prerna.reactor.workflow.nodes;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


import prerna.reactor.workflow.WorkflowConditionEvaluator;
import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "set-variable" node: resolves each configured variable's value
 * template against the current scope and writes the result back into scope.
 *
 * <p>Config: {@code {variables: {varName: "value template"}}}.
 * Returns a JSON object of the resolved values.
 */
public final class SetVariableNodeExecutor implements IWorkflowNodeExecutor {

	/** Matches a resolved value that is a pure numeric arithmetic expression safe to eval. */
	private static final Pattern NUMERIC_EXPR_PATTERN =
			Pattern.compile("^[\\d\\s+\\-*/%.()]+$");

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> node = ctx.node();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		Map<String, Object> config = (Map<String, Object>) node.get("config");
		Object variablesRaw = config.get("variables");

		if (!(variablesRaw instanceof Map)) {
			return "{}";
		}

		Map<String, Object> variables = (Map<String, Object>) variablesRaw;
		Map<String, String> resolved = new HashMap<>();

		for (Map.Entry<String, Object> entry : variables.entrySet()) {
			String varName = entry.getKey();
			if (varName == null || varName.isBlank()) continue;
			String template = entry.getValue() != null ? entry.getValue().toString() : "";
			String value = WorkflowExecutionUtils.resolve(template, scope, configMap);

			// If the resolved value is a pure arithmetic expression (e.g. "5 - 1"),
			// evaluate it so variable math like "${counter} - 1" works as expected.
			value = tryEvalNumeric(value);

			scope.put(varName, value);
			resolved.put(varName, value);
		}

		return WorkflowExecutionUtils.GSON.toJson(resolved);
	}

	/**
	 * If {@code value} consists only of digits, arithmetic operators, spaces, and
	 * parentheses, evaluates it as a JS expression and returns the numeric result
	 * (as an integer string when the result is whole). Returns {@code value}
	 * unchanged if it does not match the pattern or evaluation fails.
	 */
	private static String tryEvalNumeric(String value) {
		if (value == null || !NUMERIC_EXPR_PATTERN.matcher(value.trim()).matches()) return value;
		Double result = WorkflowConditionEvaluator.toNumber(value);
		if (result == null) return value;
		double d = result;
		if (Double.isNaN(d) || Double.isInfinite(d)) return value;
		// Return as integer string when there is no fractional part
		return d == Math.floor(d) ? String.valueOf((long) d) : String.valueOf(d);
	}
}
