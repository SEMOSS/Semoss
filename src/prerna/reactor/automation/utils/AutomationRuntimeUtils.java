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
package prerna.reactor.automation.utils;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.reactor.automation.AutomationConstants;

/**
 * Shared static utilities for the automation execution engine.
 *
 * <p>Centralizes logic shared by the graph runtime and its engine executors.
 */
public final class AutomationRuntimeUtils {

	/** Reusable {@link TypeToken} type for {@code Map<String, Object>} deserialization. */
	public static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

	/**
	 * Shared Gson instance for the Automation persistence and execution boundaries. Keeping one
	 * configured instance ensures temporal values, explicit nulls, and errors have the same JSON
	 * representation across graph, scope, and run-history operations.
	 */
	public static final Gson GSON = new GsonBuilder()
			.disableHtmlEscaping()
			.serializeNulls()
			.registerTypeHierarchyAdapter(ZoneId.class,
					(JsonSerializer<ZoneId>) (src, t, ctx) -> new JsonPrimitive(src.getId()))
			.registerTypeAdapter(ZonedDateTime.class,
					(JsonSerializer<ZonedDateTime>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.registerTypeAdapter(OffsetDateTime.class,
					(JsonSerializer<OffsetDateTime>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.registerTypeAdapter(LocalDateTime.class,
					(JsonSerializer<LocalDateTime>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.registerTypeAdapter(LocalDate.class,
					(JsonSerializer<LocalDate>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.registerTypeAdapter(Instant.class,
					(JsonSerializer<Instant>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.registerTypeHierarchyAdapter(Throwable.class,
					(JsonSerializer<Throwable>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.create();

	private AutomationRuntimeUtils() {
	}

	// -- Scope building ------------------------------------------------------------

	/**
	 * Builds the initial variable scope for an automation run, seeded with {@code date},
	 * {@code triggered_at}, and {@code run_id} (when non-blank).
	 *
	 * @param runId the run ID to seed into scope, or {@code null} for test runs
	 * @param user  the triggering user  - used to localise {@code date} and {@code triggered_at}
	 *              to the user's configured timezone; falls back to UTC when {@code null} or
	 *              when no zone has been set on the user
	 * @return mutable scope used only by the current run
	 */
	public static Map<String, Object> buildInitialScope(String runId, User user) {
		Map<String, Object> scope = new HashMap<>();
		ZoneId zone = (user != null && user.getZoneId() != null) ? user.getZoneId() : ZoneId.of("UTC");
		ZonedDateTime now = ZonedDateTime.now(zone);
		scope.put(AutomationConstants.SCOPE_DATE, now.format(DateTimeFormatter.ISO_LOCAL_DATE));
		scope.put(AutomationConstants.SCOPE_TRIGGERED_AT, now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
		if (runId != null && !runId.isBlank()) scope.put(AutomationConstants.SCOPE_RUN_ID, runId);
		return scope;
	}

	/** Serializes JSON-compatible runtime values without dropping explicit null map entries. */
	public static String toRuntimeJson(Object value) {
		return GSON.toJson(value);
	}

	/**
	 * Validates container depth before serialization, then limits the encoded UTF-8 payload.
	 * Runtime values originate as JSON, so repeated container identity has no defined meaning and
	 * is rejected with cycles before Gson can recurse indefinitely.
	 *
	 * @param value runtime value to serialize
	 * @param maxBytes maximum UTF-8 payload size
	 * @param valueName safe value name used in validation errors
	 * @return serialized JSON
	 */
	public static String toBoundedRuntimeJson(Object value, int maxBytes, String valueName) {
		validateRuntimeDepth(value, valueName);
		String json = toRuntimeJson(value);
		int byteCount = json.getBytes(StandardCharsets.UTF_8).length;
		if (byteCount > maxBytes) {
			throw new IllegalArgumentException(valueName + " exceeds the maximum of " + maxBytes
					+ " UTF-8 bytes.");
		}
		return json;
	}

	private static void validateRuntimeDepth(Object value, String valueName) {
		ArrayDeque<RuntimeValueDepth> pending = new ArrayDeque<>();
		Set<Object> visitedContainers = Collections.newSetFromMap(new IdentityHashMap<>());
		pending.push(new RuntimeValueDepth(value, 0));
		while (!pending.isEmpty()) {
			RuntimeValueDepth current = pending.pop();
			Object currentValue = current.value();
			if (!(currentValue instanceof Map<?, ?>)
					&& !(currentValue instanceof Iterable<?>)
					&& (currentValue == null || !currentValue.getClass().isArray())) {
				continue;
			}
			if (current.depth() >= AutomationConstants.RUNTIME_JSON_MAX_DEPTH) {
				throw new IllegalArgumentException(valueName + " exceeds the maximum JSON depth of "
						+ AutomationConstants.RUNTIME_JSON_MAX_DEPTH + ".");
			}
			if (!visitedContainers.add(currentValue)) {
				throw new IllegalArgumentException(valueName
						+ " contains a repeated or cyclic JSON container.");
			}
			int childDepth = current.depth() + 1;
			if (currentValue instanceof Map<?, ?> map) {
				for (Object child : map.values()) {
					pending.push(new RuntimeValueDepth(child, childDepth));
				}
			} else if (currentValue instanceof Iterable<?> iterable) {
				for (Object child : iterable) {
					pending.push(new RuntimeValueDepth(child, childDepth));
				}
			} else {
				for (int index = 0; index < Array.getLength(currentValue); index++) {
					pending.push(new RuntimeValueDepth(Array.get(currentValue, index), childDepth));
				}
			}
		}
	}

	/**
	 * Truncates a value for run-history display.
	 *
	 * @param s value to preview
	 * @return bounded preview, or {@code null} when the input is null
	 */
	public static String generatePreview(String s) {
		if (s == null) return null;
		return s.length() <= AutomationConstants.OUTPUT_PREVIEW_MAX_LENGTH
				? s : s.substring(0, AutomationConstants.OUTPUT_PREVIEW_MAX_LENGTH);
	}

	private record RuntimeValueDepth(Object value, int depth) {}
}
