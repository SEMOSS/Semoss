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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Canonical provider-neutral page returned for retained Automation data.
 *
 * <p>
 * Providers may hold values as tasks, frames, files, Python objects, or another
 * internal representation. Every provider must normalize its client response to
 * this contract so the UI never branches on storage implementation details.
 */
record AutomationDataPage(Kind kind, AutomationValueType valueType, int offset, int limit, int count, long total,
		boolean hasMore, List<String> headers, List<List<Object>> rows, Object value) {

	enum Kind {
		TABLE, JSON, TEXT
	}

	AutomationDataPage {
		if (kind == null) {
			throw new IllegalArgumentException("Automation data page kind is required.");
		}
		if (valueType == null || valueType == AutomationValueType.UNKNOWN) {
			throw new IllegalArgumentException("Automation data page value type is required.");
		}
		if (offset < 0 || limit < 1 || count < 0 || total < 0 || (long) offset + count > total) {
			throw new IllegalArgumentException("Automation data page contains invalid bounds.");
		}
		if (kind == Kind.TABLE && (headers == null || rows == null)) {
			throw new IllegalArgumentException("Automation table data requires headers and rows.");
		}
		headers = headers == null ? null : List.copyOf(headers);
		if (rows != null) {
			List<List<Object>> immutableRows = new ArrayList<>(rows.size());
			for (List<Object> row : rows) {
				immutableRows.add(Collections.unmodifiableList(new ArrayList<>(row)));
			}
			rows = Collections.unmodifiableList(immutableRows);
		}
	}

	/**
	 * Parses and validates a provider response before it crosses the reactor
	 * boundary.
	 *
	 * @param raw provider page
	 * @return canonical page
	 */
	static AutomationDataPage fromValue(Object raw) {
		if (!(raw instanceof Map<?, ?> map) || !Boolean.TRUE.equals(map.get("available"))) {
			throw new IllegalStateException("Automation run data returned an invalid page.");
		}
		Kind kind;
		try {
			kind = Kind.valueOf(requiredString(map, "kind").toUpperCase(Locale.ROOT));
		} catch (IllegalArgumentException e) {
			throw new IllegalStateException("Automation run data returned an unsupported page kind.", e);
		}
		AutomationValueType valueType = AutomationValueType.fromValue(requiredString(map, "valueType"));
		if (valueType == AutomationValueType.UNKNOWN) {
			throw new IllegalStateException("Automation run data returned an unsupported value type.");
		}

		List<String> headers = kind == Kind.TABLE ? stringList(map.get("headers"), "headers") : null;
		List<List<Object>> rows = kind == Kind.TABLE ? rows(map.get("rows")) : null;
		try {
			return new AutomationDataPage(kind, valueType, requiredInt(map, "offset"), requiredInt(map, "limit"),
					requiredInt(map, "count"), requiredLong(map, "total"), requiredBoolean(map, "hasMore"), headers,
					rows, map.get("value"));
		} catch (IllegalArgumentException e) {
			throw new IllegalStateException("Automation run data returned invalid page bounds.", e);
		}
	}

	/** @return stable JSON-shaped response consumed by Automation clients */
	Map<String, Object> toMap() {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("available", true);
		result.put("kind", this.kind.name().toLowerCase(Locale.ROOT));
		result.put("valueType", this.valueType.getValue());
		result.put("offset", this.offset);
		result.put("limit", this.limit);
		result.put("count", this.count);
		result.put("total", this.total);
		result.put("hasMore", this.hasMore);
		if (this.kind == Kind.TABLE) {
			result.put("headers", this.headers);
			result.put("rows", this.rows);
		} else {
			result.put("value", this.value);
		}
		return result;
	}

	private static String requiredString(Map<?, ?> map, String key) {
		Object value = map.get(key);
		if (!(value instanceof String string) || string.isBlank()) {
			throw new IllegalStateException("Automation data page requires " + key + ".");
		}
		return string;
	}

	private static int requiredInt(Map<?, ?> map, String key) {
		Object value = map.get(key);
		if (!(value instanceof Number number)) {
			throw new IllegalStateException("Automation data page requires numeric " + key + ".");
		}
		double raw = number.doubleValue();
		int parsed = number.intValue();
		if (!Double.isFinite(raw) || raw != parsed) {
			throw new IllegalStateException("Automation data page requires integer " + key + ".");
		}
		return parsed;
	}

	private static long requiredLong(Map<?, ?> map, String key) {
		Object value = map.get(key);
		if (!(value instanceof Number number)) {
			throw new IllegalStateException("Automation data page requires numeric " + key + ".");
		}
		double raw = number.doubleValue();
		long parsed = number.longValue();
		if (!Double.isFinite(raw) || raw != parsed) {
			throw new IllegalStateException("Automation data page requires integer " + key + ".");
		}
		return parsed;
	}

	private static boolean requiredBoolean(Map<?, ?> map, String key) {
		Object value = map.get(key);
		if (!(value instanceof Boolean bool)) {
			throw new IllegalStateException("Automation data page requires boolean " + key + ".");
		}
		return bool;
	}

	private static List<String> stringList(Object raw, String key) {
		if (!(raw instanceof List<?> list)) {
			throw new IllegalStateException("Automation table data requires " + key + ".");
		}
		List<String> result = new ArrayList<>(list.size());
		for (Object item : list) {
			if (!(item instanceof String string)) {
				throw new IllegalStateException("Automation table headers must be strings.");
			}
			result.add(string);
		}
		return result;
	}

	private static List<List<Object>> rows(Object raw) {
		if (!(raw instanceof List<?> list)) {
			throw new IllegalStateException("Automation table data requires rows.");
		}
		List<List<Object>> result = new ArrayList<>(list.size());
		for (Object item : list) {
			if (!(item instanceof List<?> row)) {
				throw new IllegalStateException("Automation table rows must be arrays.");
			}
			result.add(new ArrayList<>(row));
		}
		return result;
	}
}
