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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

/**
 * Resolves typed Automation configuration references against live run values.
 */
final class AutomationScopeResolver {

	private static final Pattern EXACT_SCOPE_REFERENCE = Pattern.compile("^\\$\\{([^}]+)}$");
	private static final Pattern LIST_INDEX = Pattern.compile("^[0-9]+$");

	private AutomationScopeResolver() {
	}

	/**
	 * Returns the path from an exact scope reference, or {@code null} for a literal
	 * value. Embedded references deliberately remain the owning field's concern.
	 */
	static String exactReferencePath(Object configuredValue) {
		if (!(configuredValue instanceof String value)) {
			return null;
		}
		Matcher reference = EXACT_SCOPE_REFERENCE.matcher(value.trim());
		return reference.matches() ? reference.group(1) : null;
	}

	/**
	 * Resolves a required dotted path while preserving the referenced Java type.
	 * Existing null values are distinct from missing values.
	 */
	static Object resolveRequired(String path, Map<String, Object> scope, String valueName) {
		if (path == null || path.isBlank()) {
			throw unavailable(valueName, path);
		}
		String[] parts = path.split("\\.", -1);
		String root = parts[0];
		if (root.isBlank() || !scope.containsKey(root)) {
			throw unavailable(valueName, path);
		}
		Object current = scope.get(root);
		for (int index = 1; index < parts.length; index++) {
			String part = parts[index];
			if (part.isBlank()) {
				throw unresolved(valueName, path);
			}
			if (current instanceof Map<?, ?> map && map.containsKey(part)) {
				current = map.get(part);
				continue;
			}
			Integer itemIndex = listIndex(part);
			if (itemIndex != null && current instanceof List<?> list && itemIndex < list.size()) {
				current = list.get(itemIndex);
				continue;
			}
			if (itemIndex != null && current != null && current.getClass().isArray()
					&& itemIndex < Array.getLength(current)) {
				current = Array.get(current, itemIndex);
				continue;
			}
			throw unresolved(valueName, path);
		}
		return current;
	}

	/** Resolves an exact reference and otherwise returns the configured literal. */
	static Object resolveExactOrLiteral(Object configuredValue, Map<String, Object> scope, String valueName) {
		String path = exactReferencePath(configuredValue);
		return path == null ? configuredValue : resolveRequired(path, scope, valueName);
	}

	/**
	 * Resolves exact references recursively in a typed configuration tree.
	 */
	static Object resolveConfig(Object configuredValue, Map<String, Object> scope, String valueName) {
		if (configuredValue instanceof Map<?, ?> map) {
			Map<String, Object> resolved = new LinkedHashMap<>();
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				if (!(entry.getKey() instanceof String key)) {
					throw new IllegalArgumentException(valueName + " contains a non-string map key.");
				}
				resolved.put(key, resolveConfig(entry.getValue(), scope, valueName + "." + key));
			}
			return resolved;
		}
		if (configuredValue instanceof List<?> list) {
			List<Object> resolved = new ArrayList<>(list.size());
			for (int index = 0; index < list.size(); index++) {
				resolved.add(resolveConfig(list.get(index), scope, valueName + "[" + index + "]"));
			}
			return resolved;
		}
		if (configuredValue != null && configuredValue.getClass().isArray()) {
			int length = Array.getLength(configuredValue);
			List<Object> resolved = new ArrayList<>(length);
			for (int index = 0; index < length; index++) {
				resolved.add(resolveConfig(Array.get(configuredValue, index), scope, valueName + "[" + index + "]"));
			}
			return resolved;
		}
		return resolveExactOrLiteral(configuredValue, scope, valueName);
	}

	private static Integer listIndex(String part) {
		if (!LIST_INDEX.matcher(part).matches()) {
			return null;
		}
		try {
			return Integer.valueOf(part);
		} catch (NumberFormatException ignored) {
			return null;
		}
	}

	private static IllegalArgumentException unavailable(String valueName, String path) {
		return new IllegalArgumentException(valueName + " references unavailable scope value: " + path);
	}

	private static IllegalArgumentException unresolved(String valueName, String path) {
		return new IllegalArgumentException(valueName + " cannot resolve scope value: " + path);
	}
}
