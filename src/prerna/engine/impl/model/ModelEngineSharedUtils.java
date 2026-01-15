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
package prerna.engine.impl.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

/**
 * Shared helpers for in-process Java model engines.
 *
 * This intentionally focuses on SEMOSS-side concerns (parameter parsing, tool
 * arg normalization, and schema cleanup), not provider SDK specifics.
 */
final class ModelEngineSharedUtils {

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private ModelEngineSharedUtils() {
	}

	static String asString(Object o) {
		return o == null ? null : o.toString();
	}

	static String firstNonBlank(String... values) {
		if (values == null) {
			return null;
		}
		for (String v : values) {
			if (v != null && !v.isBlank()) {
				return v;
			}
		}
		return null;
	}

	static boolean parseBoolean(Object value, boolean defaultValue) {
		if (value == null) {
			return defaultValue;
		}
		if (value instanceof Boolean) {
			return (Boolean) value;
		}
		String s = value.toString().trim().toLowerCase();
		if (s.isEmpty()) {
			return defaultValue;
		}
		if ("true".equals(s) || "1".equals(s) || "yes".equals(s) || "y".equals(s)) {
			return true;
		}
		if ("false".equals(s) || "0".equals(s) || "no".equals(s) || "n".equals(s)) {
			return false;
		}
		return defaultValue;
	}

	static Long firstLong(Map<String, Object> parameters, String... keys) {
		if (parameters == null) {
			return null;
		}
		for (String key : keys) {
			Object v = parameters.get(key);
			Long parsed = parseLong(v);
			if (parsed != null) {
				return parsed;
			}
		}
		return null;
	}

	static Double firstDouble(Map<String, Object> parameters, String... keys) {
		if (parameters == null) {
			return null;
		}
		for (String key : keys) {
			Object v = parameters.get(key);
			Double parsed = parseDouble(v);
			if (parsed != null) {
				return parsed;
			}
		}
		return null;
	}

	static Map<String, Object> normalizeToolArgs(Object argsObj) {
		if (argsObj == null) {
			return new HashMap<>();
		}
		if (argsObj instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> args = (Map<String, Object>) argsObj;
			return args;
		}
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> args = GSON.fromJson(argsObj.toString(), Map.class);
			return args != null ? args : new HashMap<>();
		} catch (Exception e) {
			return new HashMap<>();
		}
	}

	static Object stripSchemaTitles(Object value) {
		if (!(value instanceof Map)) {
			if (value instanceof List<?>) {
				List<Object> cleaned = new ArrayList<>();
				for (Object v : (List<?>) value) {
					cleaned.add(stripSchemaTitles(v));
				}
				return cleaned;
			}
			return value;
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> map = (Map<String, Object>) value;
		Map<String, Object> cleaned = new LinkedHashMap<>();
		for (Map.Entry<String, Object> e : map.entrySet()) {
			if ("title".equals(e.getKey())) {
				continue;
			}
			cleaned.put(e.getKey(), stripSchemaTitles(e.getValue()));
		}
		return cleaned;
	}

	static String stackTraceToString(Throwable t) {
		try (java.io.StringWriter sw = new java.io.StringWriter(); java.io.PrintWriter pw = new java.io.PrintWriter(sw)) {
			t.printStackTrace(pw);
			return sw.toString();
		} catch (Exception e) {
			return t.toString();
		}
	}

	static Long parseLong(Object v) {
		if (v instanceof Number) {
			return ((Number) v).longValue();
		}
		if (v == null) {
			return null;
		}
		try {
			String s = v.toString().trim();
			if (s.isEmpty()) {
				return null;
			}
			return Long.parseLong(s);
		} catch (Exception e) {
			return null;
		}
	}

	static Double parseDouble(Object v) {
		if (v instanceof Number) {
			return ((Number) v).doubleValue();
		}
		if (v == null) {
			return null;
		}
		try {
			String s = v.toString().trim();
			if (s.isEmpty()) {
				return null;
			}
			return Double.parseDouble(s);
		} catch (Exception e) {
			return null;
		}
	}
}
