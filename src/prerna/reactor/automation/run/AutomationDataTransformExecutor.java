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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.automation.AutomationConstants;

/**
 * Builds {@code data.transform} and {@code data.declare-variable} node output
 * directly in Java, resolving {@code ${name.path}} references through
 * {@link AutomationScopeResolver} instead of generating and executing Python.
 */
final class AutomationDataTransformExecutor {

	private AutomationDataTransformExecutor() {
	}

	/**
	 * Builds a brand-new JSON object from the node's configured field tree,
	 * recursively resolving nested objects and arrays to any depth. The result
	 * never carries any value besides what the configured fields produce - it is
	 * never merged with a prior scope value.
	 */
	static Map<String, Object> transform(Map<String, Object> config, Map<String, Object> scope, String nodeId) {
		List<?> fields = asList(config.get(AutomationConstants.CONFIG_FIELDS));
		return resolveObject(fields, scope, "Transform node '" + nodeId + "'");
	}

	private static Map<String, Object> resolveObject(List<?> fields, Map<String, Object> scope, String valueName) {
		Map<String, Object> result = new LinkedHashMap<>();
		for (Object fieldObj : fields) {
			if (!(fieldObj instanceof Map<?, ?> field)) {
				continue;
			}
			Object nameValue = field.get("name");
			String name = nameValue == null ? "" : nameValue.toString().trim();
			if (name.isEmpty()) {
				continue;
			}
			result.put(name, resolveNode(field.get("node"), scope, valueName + "." + name));
		}
		return result;
	}

	private static List<Object> resolveArray(List<?> items, Map<String, Object> scope, String valueName) {
		List<Object> result = new ArrayList<>(items.size());
		int index = 0;
		for (Object item : items) {
			result.add(resolveNode(item, scope, valueName + "[" + index + "]"));
			index++;
		}
		return result;
	}

	/**
	 * Resolves one {@code {kind, ...}} field-tree node. {@code object} and
	 * {@code array} nodes recurse; {@code boolean} and {@code null} nodes are
	 * literals; any other kind (plain {@code value} nodes) resolves its
	 * {@code value} as an exact {@code ${name.path}} reference or a literal.
	 */
	private static Object resolveNode(Object nodeObj, Map<String, Object> scope, String valueName) {
		if (!(nodeObj instanceof Map<?, ?> node)) {
			return null;
		}
		Object kindValue = node.get("kind");
		String kind = kindValue == null ? "" : kindValue.toString();
		return switch (kind) {
		case "object" -> resolveObject(asList(node.get("fields")), scope, valueName);
		case "array" -> resolveArray(asList(node.get("items")), scope, valueName);
		case "boolean" -> Boolean.TRUE.equals(node.get("boolValue"));
		case "null" -> null;
		default -> AutomationScopeResolver.resolveExactOrLiteral(node.get(AutomationConstants.CONFIG_VALUE), scope,
				valueName);
		};
	}

	private static List<?> asList(Object value) {
		return value instanceof List<?> list ? list : List.of();
	}
}
