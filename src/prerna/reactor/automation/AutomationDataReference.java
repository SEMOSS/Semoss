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
 *******************************************************************************/
package prerna.reactor.automation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Opaque pointer to a value owned by an Automation execution Insight.
 *
 * <p>
 * The serialized reference deliberately contains no engine identifier, query,
 * frame name, filesystem path, or runtime representation. Resolution is
 * possible only inside the authenticated execution Insight that created it. The
 * reference is not an authorization token: every resolver must independently
 * enforce the owning Automation project's permissions and immutable run owner.
 *
 * @param schemaVersion serialized contract version
 * @param referenceId   opaque run-local identifier
 * @param valueType     provider-independent compatibility category
 */
public record AutomationDataReference(int schemaVersion, String referenceId, AutomationValueType valueType) {

	/** Current serialized reference schema. */
	public static final int CURRENT_SCHEMA_VERSION = 1;
	/** Marker wrapping a reference inside an ordinary JSON scope value. */
	public static final String MARKER = "__automationDataReference";
	/** Bounded text shown to clients instead of the private reference payload. */
	public static final String CLIENT_PREVIEW = "Output is available from this run.";

	public AutomationDataReference {
		if (schemaVersion != CURRENT_SCHEMA_VERSION) {
			throw new IllegalArgumentException("Unsupported Automation data-reference schema: " + schemaVersion);
		}
		if (referenceId == null || referenceId.isBlank()) {
			throw new IllegalArgumentException("Automation data reference ID is required.");
		}
		if (valueType == null || valueType == AutomationValueType.UNKNOWN) {
			throw new IllegalArgumentException("Automation data reference value type is required.");
		}
	}

	/**
	 * @return JSON-shaped value stored in run scope and node history
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> metadata = new LinkedHashMap<>();
		metadata.put("schemaVersion", this.schemaVersion);
		metadata.put("referenceId", this.referenceId);
		metadata.put("valueType", this.valueType.getValue());
		return Map.of(MARKER, metadata);
	}

	/**
	 * Reads a reference from a runtime value. Invalid or unrelated maps are not
	 * treated as references.
	 *
	 * @param value possible serialized reference
	 * @return parsed reference, or {@code null}
	 */
	public static AutomationDataReference fromValue(Object value) {
		if (!(value instanceof Map<?, ?> outer) || !(outer.get(MARKER) instanceof Map<?, ?> metadata)) {
			return null;
		}
		Object schema = metadata.get("schemaVersion");
		Object reference = metadata.get("referenceId");
		Object type = metadata.get("valueType");
		if (!(schema instanceof Number) || !(reference instanceof String) || !(type instanceof String)) {
			return null;
		}
		try {
			return new AutomationDataReference(((Number) schema).intValue(), (String) reference,
					AutomationValueType.fromValue((String) type));
		} catch (IllegalArgumentException ignored) {
			return null;
		}
	}

	/**
	 * Returns whether a JSON-shaped value contains a private data reference at any
	 * depth.
	 *
	 * @param value possible runtime value
	 * @return {@code true} when the value contains a reference
	 */
	public static boolean containsReference(Object value) {
		if (fromValue(value) != null) {
			return true;
		}
		if (value instanceof Map<?, ?> map) {
			for (Object item : map.values()) {
				if (containsReference(item)) {
					return true;
				}
			}
		} else if (value instanceof List<?> list) {
			for (Object item : list) {
				if (containsReference(item)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Replaces private reference payloads with a bounded client-facing message.
	 * Ordinary JSON values retain their existing shape.
	 *
	 * @param value runtime value
	 * @return client-safe JSON-shaped value
	 */
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
