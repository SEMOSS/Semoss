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
package prerna.reactor.automation.nodes;

import java.util.Map;

/**
 * Shared config-extraction helpers used by all automation node executors.
 *
 * <p>Each executor previously contained private copies of {@code required()},
 * {@code optional()}, and {@code optionalInt()} with identical logic. This class
 * centralises them so changes (e.g. to error message format) propagate everywhere.
 */
final class NodeConfigHelper {

	private NodeConfigHelper() {
		// utility class
	}

	/**
	 * Returns the string value of {@code key} from {@code config}, throwing if absent or blank.
	 *
	 * @param config    node config map
	 * @param key       config key to look up
	 * @param nodeLabel display label of the containing node (used in the error message)
	 */
	static String required(Map<String, Object> config, String key, String nodeLabel) {
		Object v = config.get(key);
		if (v == null || v.toString().isBlank()) {
			throw new IllegalArgumentException(
					"Node \"" + nodeLabel + "\": '" + key + "' is required");
		}
		return v.toString();
	}

	/**
	 * Returns the string value of {@code key} from {@code config}, or {@code def} if absent or blank.
	 *
	 * @param config node config map
	 * @param key    config key to look up
	 * @param def    value to return when key is absent or blank
	 */
	static String optional(Map<String, Object> config, String key, String def) {
		Object v = config.get(key);
		return (v == null || v.toString().isBlank()) ? def : v.toString();
	}

	/**
	 * Returns the string value of {@code key} from {@code config}, or {@code null} if absent or blank.
	 *
	 * @param config node config map
	 * @param key    config key to look up
	 */
	static String optional(Map<String, Object> config, String key) {
		return optional(config, key, null);
	}

	/**
	 * Returns the integer value of {@code key} from {@code config}, or {@code def} if absent, blank,
	 * or not parseable as an integer.
	 *
	 * @param config node config map
	 * @param key    config key to look up
	 * @param def    default value when key is absent, blank, or unparseable
	 */
	static int optionalInt(Map<String, Object> config, String key, int def) {
		Object v = config.get(key);
		if (v == null) return def;
		try {
			return Integer.parseInt(v.toString().trim());
		} catch (NumberFormatException e) {
			return def;
		}
	}
}
