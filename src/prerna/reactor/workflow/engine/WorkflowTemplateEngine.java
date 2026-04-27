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
package prerna.reactor.workflow.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Resolves {{stepId.output}} references in step config values at runtime.
 * 
 * This handles step-to-step data passing. Static values like engine IDs,
 * prompts, and tool names are baked into the workflow.json by the frontend
 * at design time and do not need resolution.
 * 
 * Supported patterns:
 *   {{stepId.output}}                  → full output of a prior step
 *   {{stepId.output.fieldName}}        → nested field access (dot-notation)
 *   {{stepId.metadata.fieldName}}      → metadata field access
 */
public final class WorkflowTemplateEngine {

	private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\{\\{(.+?)\\}\\}");
	private static final Gson GSON = new GsonBuilder().create();

	private WorkflowTemplateEngine() {}

	/**
	 * Resolve all {{...}} expressions in a single string.
	 * If the entire string is one template expression and the resolved value
	 * is not a String, the raw object is returned (preserves types like Map, List, Number).
	 */
	public static Object resolve(String template, WorkflowContext context) {
		if (template == null) return null;

		Matcher matcher = TEMPLATE_PATTERN.matcher(template);
		if (!matcher.find()) {
			return template;
		}

		// If the entire string is a single template expression, return the raw object
		if (matcher.start() == 0 && matcher.end() == template.length()) {
			return resolveExpression(matcher.group(1).trim(), context);
		}

		// Otherwise, do string interpolation
		matcher.reset();
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			Object value = resolveExpression(matcher.group(1).trim(), context);
			String replacement = (value != null) ? stringify(value) : "";
			matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	/**
	 * Resolve all string values in a config map. Non-string values are passed through unchanged.
	 * Returns a new map (does not mutate the original).
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> resolveMap(Map<String, Object> configMap, WorkflowContext context) {
		if (configMap == null) return null;

		Map<String, Object> resolved = new HashMap<>();
		for (Map.Entry<String, Object> entry : configMap.entrySet()) {
			Object value = entry.getValue();
			if (value instanceof String) {
				resolved.put(entry.getKey(), resolve((String) value, context));
			} else if (value instanceof Map) {
				resolved.put(entry.getKey(), resolveMap((Map<String, Object>) value, context));
			} else if (value instanceof List) {
				resolved.put(entry.getKey(), resolveList((List<Object>) value, context));
			} else {
				resolved.put(entry.getKey(), value);
			}
		}
		return resolved;
	}

	/**
	 * Resolve all string values in a list. Returns a new list.
	 */
	@SuppressWarnings("unchecked")
	private static List<Object> resolveList(List<Object> list, WorkflowContext context) {
		List<Object> resolved = new java.util.ArrayList<>();
		for (Object item : list) {
			if (item instanceof String) {
				resolved.add(resolve((String) item, context));
			} else if (item instanceof Map) {
				resolved.add(resolveMap((Map<String, Object>) item, context));
			} else if (item instanceof List) {
				resolved.add(resolveList((List<Object>) item, context));
			} else {
				resolved.add(item);
			}
		}
		return resolved;
	}

	/**
	 * Resolve a single expression like "stepId.output" or "stepId.output.field.nested".
	 */
	private static Object resolveExpression(String expression, WorkflowContext context) {
		String[] parts = expression.split("\\.", 2);
		if (parts.length < 2) return null;

		String stepId = parts[0];
		String path = parts[1];

		StepResult result = context.getResult(stepId);
		if (result == null) return null;

		// "stepId.output" or "stepId.output.nested.field"
		if (path.equals("output")) {
			return result.getOutput();
		}
		if (path.startsWith("output.")) {
			return traversePath(result.getOutput(), path.substring("output.".length()));
		}

		// "stepId.metadata.fieldName"
		if (path.equals("metadata")) {
			return result.getMetadata();
		}
		if (path.startsWith("metadata.")) {
			return traversePath(result.getMetadata(), path.substring("metadata.".length()));
		}

		// "stepId.status", "stepId.error", "stepId.durationMs"
		if (path.equals("status")) return result.getStatus() != null ? result.getStatus().name() : null;
		if (path.equals("error")) return result.getError();
		if (path.equals("durationMs")) return result.getDurationMs();

		return null;
	}

	/**
	 * Traverse a dot-separated path into an object (supports Map and nested Maps).
	 */
	@SuppressWarnings("unchecked")
	private static Object traversePath(Object obj, String path) {
		if (obj == null || path == null || path.isEmpty()) return obj;

		String[] segments = path.split("\\.");
		Object current = obj;
		for (String segment : segments) {
			if (current instanceof Map) {
				current = ((Map<String, Object>) current).get(segment);
			} else {
				return null;
			}
			if (current == null) return null;
		}
		return current;
	}

	/**
	 * Convert a value to a string for interpolation.
	 */
	private static String stringify(Object value) {
		if (value instanceof String) return (String) value;
		if (value instanceof Number || value instanceof Boolean) return value.toString();
		return GSON.toJson(value);
	}
}
