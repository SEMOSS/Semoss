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

import prerna.reactor.automation.utils.AutomationExecutionUtils;

/**
 * Delegates to {@link AutomationExecutionUtils} for config-extraction helpers.
 *
 * <p>The canonical implementations now live in {@code AutomationExecutionUtils}. This class
 * is kept as a thin delegation shim so existing callers in the {@code nodes} sub-package
 * continue to compile without modification. It will be removed in a later cleanup pass once
 * those callers are updated to reference {@code AutomationExecutionUtils} directly.
 *
 * @deprecated Use {@link AutomationExecutionUtils#required}, {@link AutomationExecutionUtils#optional},
 *             and {@link AutomationExecutionUtils#optionalInt} directly.
 */
@Deprecated
final class NodeConfigHelper {

	private NodeConfigHelper() {
		// utility class
	}

	/** @see AutomationExecutionUtils#required(Map, String, String) */
	static String required(Map<String, Object> config, String key, String nodeLabel) {
		return AutomationExecutionUtils.required(config, key, nodeLabel);
	}

	/** @see AutomationExecutionUtils#optional(Map, String, String) */
	static String optional(Map<String, Object> config, String key, String def) {
		return AutomationExecutionUtils.optional(config, key, def);
	}

	/** @see AutomationExecutionUtils#optional(Map, String) */
	static String optional(Map<String, Object> config, String key) {
		return AutomationExecutionUtils.optional(config, key);
	}

	/** @see AutomationExecutionUtils#optionalInt(Map, String, int) */
	static int optionalInt(Map<String, Object> config, String key, int def) {
		return AutomationExecutionUtils.optionalInt(config, key, def);
	}
}
