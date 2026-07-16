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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "transform" node: applies a lightweight, config-driven transform
 * ({@code convert-to-objects}, {@code extract-field}, {@code map}, {@code filter},
 * {@code flatten}) to a prior node's raw output.
 */
public final class TransformNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> node = ctx.node();
		Map<String, String> scope = ctx.scope();
		Map<String, Object> config = (Map<String, Object>) node.get("config");
		if (config == null) return "{}";

		String inputVar = WorkflowExecutionUtils.strCfg(config.get("inputVar"));
		String raw = inputVar != null && !inputVar.isBlank() ? scope.getOrDefault(inputVar, "") : "";
		String operation = WorkflowExecutionUtils.strCfg(config.get("operation"));
		String expression = WorkflowExecutionUtils.strCfg(config.get("expression"));

		if (operation == null) return raw;

		switch (operation) {
			case "convert-to-objects":
				return WorkflowExecutionUtils.applyOutputTransform(raw,
						Collections.singletonMap("mode", "rows-as-objects"));
			case "extract-field": {
				// expression is a dot-notation path like "[0].name" or "data.items"
				String path = expression != null ? expression.replaceAll("^\\[\\d+\\]\\.", "$0") : "";
				return WorkflowExecutionUtils.applyOutputTransform(raw,
						Map.of("mode", "jsonpath", "path", path));
			}
			case "map": {
				// expression like "item.fieldName" - extract named field from each array element
				if (expression == null || !expression.startsWith("item.")) return raw;
				String field = expression.substring(5).trim();
				try {
					JsonElement el = JsonParser.parseString(raw);
					if (!el.isJsonArray()) return raw;
					List<Object> out = new ArrayList<>();
					for (JsonElement item : el.getAsJsonArray()) {
						if (item.isJsonObject() && item.getAsJsonObject().has(field)) {
							JsonElement val = item.getAsJsonObject().get(field);
							out.add(val.isJsonPrimitive() ? val.getAsString() : val.toString());
						} else {
							out.add(null);
						}
					}
					return WorkflowExecutionUtils.GSON.toJson(out);
				} catch (Exception e) { return raw; }
			}
			case "filter": {
				// expression like "item.field === \"value\"" - simple equality filter
				if (expression == null || !expression.startsWith("item.")) return raw;
				Matcher m = Pattern
						.compile("item\\.([\\w]+)\\s*===?\\s*[\"']?([^\"']+)[\"']?")
						.matcher(expression);
				if (!m.find()) return raw;
				String field = m.group(1);
				String expected = m.group(2).trim();
				try {
					JsonElement el = JsonParser.parseString(raw);
					if (!el.isJsonArray()) return raw;
					List<Object> out = new ArrayList<>();
					for (JsonElement item : el.getAsJsonArray()) {
						if (item.isJsonObject() && item.getAsJsonObject().has(field)) {
							String actual = item.getAsJsonObject().get(field).getAsString();
							if (expected.equals(actual)) out.add(WorkflowExecutionUtils.GSON.fromJson(item, Object.class));
						}
					}
					return WorkflowExecutionUtils.GSON.toJson(out);
				} catch (Exception e) { return raw; }
			}
			case "flatten": {
				try {
					JsonElement el = JsonParser.parseString(raw);
					if (!el.isJsonArray()) return raw;
					List<Object> out = new ArrayList<>();
					for (JsonElement item : el.getAsJsonArray()) {
						if (item.isJsonArray()) {
							for (JsonElement inner : item.getAsJsonArray()) {
								out.add(WorkflowExecutionUtils.GSON.fromJson(inner, Object.class));
							}
						} else {
							out.add(WorkflowExecutionUtils.GSON.fromJson(item, Object.class));
						}
					}
					return WorkflowExecutionUtils.GSON.toJson(out);
				} catch (Exception e) { return raw; }
			}
			default:
				return raw;
		}
	}
}
