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
 * -----------------------------------------------------------------------------
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
 ******************************************************************************/
package prerna.reactor.automation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Opaque pointer to a value retained by one Automation execution Insight.
 *
 * <p>
 * The reference is only a locator. Callers must authorize the Automation
 * project and run before resolving it inside that run's execution Insight.
 *
 * @param schemaVersion serialized contract version
 * @param referenceId run-local identifier
 */
public record AutomationDataReference(int schemaVersion, String referenceId) {

	public static final int CURRENT_SCHEMA_VERSION = 1;
	public static final String MARKER = "__automationDataReference";
	public static final String CLIENT_PREVIEW = "Large output is retained in this run workspace.";

	public AutomationDataReference {
		if (schemaVersion != CURRENT_SCHEMA_VERSION) {
			throw new IllegalArgumentException("Unsupported Automation data-reference schema: " + schemaVersion);
		}
		if (referenceId == null || referenceId.isBlank()) {
			throw new IllegalArgumentException("Automation data reference ID is required.");
		}
	}

	/** @return JSON-shaped value stored in run scope and node history */
	public Map<String, Object> toMap() {
		return Map.of(MARKER, Map.of("schemaVersion", this.schemaVersion, "referenceId", this.referenceId));
	}

	/** Returns a reference when the supplied value is a valid reference marker. */
	public static AutomationDataReference fromValue(Object value) {
		if (!(value instanceof Map<?, ?> outer) || !(outer.get(MARKER) instanceof Map<?, ?> metadata)) {
			return null;
		}
		Object schema = metadata.get("schemaVersion");
		Object reference = metadata.get("referenceId");
		if (!(schema instanceof Number) || !(reference instanceof String)) {
			return null;
		}
		try {
			return new AutomationDataReference(((Number) schema).intValue(), (String) reference);
		} catch (IllegalArgumentException ignored) {
			return null;
		}
	}

	/** Returns the first retained-data reference found at any JSON depth. */
	public static AutomationDataReference find(Object value) {
		AutomationDataReference reference = fromValue(value);
		if (reference != null) {
			return reference;
		}
		if (value instanceof Map<?, ?> map) {
			for (Object item : map.values()) {
				reference = find(item);
				if (reference != null) {
					return reference;
				}
			}
		} else if (value instanceof List<?> list) {
			for (Object item : list) {
				reference = find(item);
				if (reference != null) {
					return reference;
				}
			}
		}
		return null;
	}

	/** Replaces private references with a bounded message for client responses. */
	public static Object forClient(Object value) {
		if (fromValue(value) != null) {
			return CLIENT_PREVIEW;
		}
		if (value instanceof Map<?, ?> map) {
			Map<String, Object> result = new LinkedHashMap<>();
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				if (entry.getKey() instanceof String key) {
					result.put(key, forClient(entry.getValue()));
				}
			}
			return result;
		}
		if (value instanceof List<?> list) {
			List<Object> result = new ArrayList<>(list.size());
			for (Object item : list) {
				result.add(forClient(item));
			}
			return result;
		}
		return value;
	}
}
