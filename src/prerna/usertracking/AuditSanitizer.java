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
package prerna.usertracking;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import prerna.security.SensitiveKeys;

/**
 * Redacts secrets and bounds the size of everything written to the audit trail,
 * following the OWASP logging guidance that audit records must never contain
 * passwords, tokens, keys, connection strings, or stack traces.
 */
public final class AuditSanitizer {

	public static final String REDACTED = "[REDACTED]";

	static final int MAX_STRING_LENGTH = 2000;
	static final int MAX_MESSAGE_LENGTH = 1000;
	static final int MAX_ARRAY_ITEMS = 200;
	static final int MAX_DEPTH = 8;

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();

	private static final Pattern BEARER = Pattern.compile("(?i)(bearer\\s+)[A-Za-z0-9._~+/=-]+");
	private static final Pattern KEY_VALUE_SECRET = Pattern.compile("(?i)([\"']?)([a-z][a-z0-9_.-]*)\\1(\\s*[=:]\\s*)"
			+ "(\"(?:\\\\.|[^\"\\\\])*\"|'(?:\\\\.|[^'\\\\])*'|[^\\s,;&\"'}]+)");
	private static final Pattern ESCAPED_KEY = Pattern.compile("\\\\+[\"']([a-zA-Z][a-zA-Z0-9_.-]*)\\\\+[\"']\\s*:");
	private static final Pattern ESCAPED_KEY_VALUE = Pattern
			.compile("(\\\\+[\"'])([a-zA-Z][a-zA-Z0-9_.-]*)(\\\\+[\"']\\s*:\\s*)"
					+ "(?:(\\\\+[\"'])(.*?)(\\\\+[\"'])|([^\\s,;&\\\\}\\]]+))");
	private static final Pattern JDBC_URL = Pattern.compile("(?i)jdbc:[^\\s\"'<>]+");
	private static final Pattern URL_USER_INFO = Pattern
			.compile("(?i)\\b([a-z][a-z0-9+.-]*://)[^\\s/@:\"']+:[^\\s/@\"']+@");
	private static final Pattern PRIVATE_KEY = Pattern
			.compile("-----BEGIN [A-Z ]*PRIVATE KEY-----[\\s\\S]*?-----END [A-Z ]*PRIVATE KEY-----");
	private static final Pattern STACK_FRAME = Pattern.compile("\\r?\\n\\s*at\\s");

	private AuditSanitizer() {

	}

	/**
	 * Whether values stored under a key must never be written, decided by
	 * {@link SensitiveKeys#isSensitiveKey(String)}.
	 *
	 * @param key a JSON/map key
	 * @return true if values stored under the key must never be written
	 */
	public static boolean isSensitiveKey(String key) {
		return SensitiveKeys.isSensitiveKey(key);
	}

	/**
	 * Serialize a value to JSON after redacting sensitive keys, scrubbing secrets
	 * out of free text, and bounding the size of strings and arrays.
	 *
	 * @param value any value (map, list, bean, string)
	 * @return sanitized JSON, or null if the value is null
	 */
	public static String toSanitizedJson(Object value) {
		if (value == null) {
			return null;
		}
		JsonElement element;
		if (value instanceof JsonElement json) {
			element = json;
		} else if (value instanceof String text) {
			element = parseJsonOrText(text);
		} else {
			try {
				element = GSON.toJsonTree(value);
			} catch (Exception e) {
				element = new JsonPrimitive(String.valueOf(value));
			}
		}
		JsonElement sanitized = sanitize(element, 0);
		if (sanitized == null || sanitized.isJsonNull()) {
			return null;
		}
		return GSON.toJson(sanitized);
	}

	/**
	 * Reduce an error message to a single sanitized summary line with no stack
	 * trace and no secrets.
	 *
	 * @param message raw error message
	 * @return sanitized summary or null
	 */
	public static String sanitizeMessage(String message) {
		if (message == null) {
			return null;
		}
		String summary = message;
		Matcher frame = STACK_FRAME.matcher(summary);
		if (frame.find()) {
			summary = summary.substring(0, frame.start());
		}
		summary = scrubText(summary).replaceAll("\\s+", " ").trim();
		if (summary.isEmpty()) {
			return null;
		}
		return truncate(summary, MAX_MESSAGE_LENGTH);
	}

	/**
	 * Remove secrets that appear inside free text (bearer tokens, key=value
	 * secrets, JDBC URLs, URL credentials, PEM private keys).
	 *
	 * @param text raw text
	 * @return text with secrets replaced by {@link #REDACTED}
	 */
	public static String scrubText(String text) {
		return scrub(text, true);
	}

	/**
	 * Removes secrets from error text the way {@link #scrubText(String)} does,
	 * except that a secret key inside nested escaped JSON (such as
	 * {@code \"token\":\"abc\"}) has only its value masked, so the rest of the
	 * error stays readable.
	 *
	 * @param text raw error text
	 * @return text with secrets replaced by {@link #REDACTED}
	 */
	public static String scrubErrorText(String text) {
		return scrub(text, false);
	}

	private static String scrub(String text, boolean omitNestedSecretJson) {
		if (text == null || text.isEmpty()) {
			return text;
		}
		String scrubbed = PRIVATE_KEY.matcher(text).replaceAll(Matcher.quoteReplacement(REDACTED));
		scrubbed = BEARER.matcher(scrubbed).replaceAll("$1" + Matcher.quoteReplacement(REDACTED));
		if (omitNestedSecretJson) {
			// Nested encoded JSON has ambiguous quoting: omit it rather than risk partial
			// redaction.
			Matcher escaped = ESCAPED_KEY.matcher(scrubbed);
			while (escaped.find()) {
				if (isSensitiveKey(escaped.group(1))) {
					return REDACTED;
				}
			}
		} else {
			scrubbed = ESCAPED_KEY_VALUE.matcher(scrubbed)
					.replaceAll(m -> Matcher.quoteReplacement(!isSensitiveKey(m.group(2)) ? m.group()
							: m.group(1) + m.group(2) + m.group(3)
									+ (m.group(4) != null ? m.group(4) + REDACTED + m.group(6) : REDACTED)));
		}
		Matcher assignments = KEY_VALUE_SECRET.matcher(scrubbed);
		StringBuffer safe = new StringBuffer();
		while (assignments.find()) {
			String replacement = isSensitiveKey(assignments.group(2)) ? assignments.group(1) + assignments.group(2)
					+ assignments.group(1) + assignments.group(3) + REDACTED : assignments.group();
			assignments.appendReplacement(safe, Matcher.quoteReplacement(replacement));
		}
		assignments.appendTail(safe);
		scrubbed = safe.toString();
		scrubbed = JDBC_URL.matcher(scrubbed).replaceAll(Matcher.quoteReplacement("jdbc:" + REDACTED));
		scrubbed = URL_USER_INFO.matcher(scrubbed).replaceAll("$1" + Matcher.quoteReplacement(REDACTED) + "@");
		return scrubbed;
	}

	static String truncate(String value, int maxLength) {
		if (value == null || value.length() <= maxLength) {
			return value;
		}
		String marker = "...[TRUNCATED]";
		if (maxLength <= marker.length()) {
			return marker.substring(0, Math.max(0, maxLength));
		}
		return value.substring(0, maxLength - marker.length()) + marker;
	}

	private static JsonElement parseJsonOrText(String text) {
		String trimmed = text.trim();
		if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
			try {
				return com.google.gson.JsonParser.parseString(trimmed);
			} catch (Exception e) {
				// not JSON - treat as plain text
			}
		}
		return new JsonPrimitive(text);
	}

	private static JsonElement sanitize(JsonElement element, int depth) {
		if (element == null || element.isJsonNull()) {
			return JsonNull.INSTANCE;
		}
		if (depth > MAX_DEPTH) {
			return new JsonPrimitive("[TRUNCATED]");
		}
		if (element.isJsonObject()) {
			JsonObject sanitized = new JsonObject();
			for (Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
				if (isSensitiveKey(entry.getKey())) {
					sanitized.addProperty(entry.getKey(), REDACTED);
				} else {
					sanitized.add(entry.getKey(), sanitize(entry.getValue(), depth + 1));
				}
			}
			return sanitized;
		}
		if (element.isJsonArray()) {
			JsonArray source = element.getAsJsonArray();
			JsonArray sanitized = new JsonArray();
			int limit = Math.min(source.size(), MAX_ARRAY_ITEMS);
			for (int i = 0; i < limit; i++) {
				sanitized.add(sanitize(source.get(i), depth + 1));
			}
			if (source.size() > limit) {
				sanitized.add("[" + (source.size() - limit) + " MORE ITEMS TRUNCATED]");
			}
			return sanitized;
		}
		JsonPrimitive primitive = element.getAsJsonPrimitive();
		if (primitive.isString()) {
			return new JsonPrimitive(truncate(scrubText(primitive.getAsString()), MAX_STRING_LENGTH));
		}
		return primitive;
	}
}
