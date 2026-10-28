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
package prerna.util.k8s;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Null-safe navigation of Kubernetes API objects held as Gson trees. Every
 * getter takes a path of field names and returns null (or an empty list) when
 * any step is missing or of another type, which is the normal case for optional
 * status fields.
 */
public final class KubeJson {

	private KubeJson() {
	}

	public static JsonElement element(JsonObject root, String... path) {
		JsonElement current = root;
		for (String field : path) {
			if (current == null || !current.isJsonObject()) {
				return null;
			}
			current = current.getAsJsonObject().get(field);
		}
		return current == null || current.isJsonNull() ? null : current;
	}

	public static JsonObject obj(JsonObject root, String... path) {
		JsonElement element = element(root, path);
		return element != null && element.isJsonObject() ? element.getAsJsonObject() : null;
	}

	public static String str(JsonObject root, String... path) {
		JsonElement element = element(root, path);
		return element != null && element.isJsonPrimitive() ? element.getAsString() : null;
	}

	public static Integer integer(JsonObject root, String... path) {
		JsonElement element = element(root, path);
		if (element == null || !element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
			return null;
		}
		return element.getAsInt();
	}

	public static boolean isTrue(JsonObject root, String... path) {
		JsonElement element = element(root, path);
		return element != null && element.isJsonPrimitive() && element.getAsJsonPrimitive().isBoolean()
				&& element.getAsBoolean();
	}

	/**
	 * @return the objects of the array at the path, skipping anything that is not
	 *         an object; never null
	 */
	public static List<JsonObject> objects(JsonObject root, String... path) {
		JsonElement element = element(root, path);
		if (element == null || !element.isJsonArray()) {
			return Collections.emptyList();
		}
		return objects(element.getAsJsonArray());
	}

	public static List<JsonObject> objects(JsonArray array) {
		List<JsonObject> result = new ArrayList<>();
		if (array != null) {
			for (JsonElement item : array) {
				if (item != null && item.isJsonObject()) {
					result.add(item.getAsJsonObject());
				}
			}
		}
		return result;
	}

	/**
	 * @return the strings of the array at the path; never null
	 */
	public static List<String> strings(JsonObject root, String... path) {
		JsonElement element = element(root, path);
		List<String> result = new ArrayList<>();
		if (element != null && element.isJsonArray()) {
			for (JsonElement item : element.getAsJsonArray()) {
				if (item != null && item.isJsonPrimitive()) {
					result.add(item.getAsString());
				}
			}
		}
		return result;
	}

	/**
	 * Kubernetes timestamps are RFC 3339 (`2026-10-26T23:57:46Z`, and with
	 * fractions and offsets in events and container logs).
	 *
	 * @return the instant, or null when missing or unparseable
	 */
	public static Instant time(JsonObject root, String... path) {
		return parseTime(str(root, path));
	}

	public static Instant parseTime(String value) {
		if (value == null || value.isBlank()) {
			return null;
		}
		try {
			return OffsetDateTime.parse(value.strip()).toInstant();
		} catch (DateTimeParseException e) {
			return null;
		}
	}
}
