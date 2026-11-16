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
package prerna.security;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import prerna.usertracking.AuditSanitizer;
import prerna.util.Constants;

/**
 * Keeps secrets out of the error text sent to users. Error text is built from
 * internal state, such as a provider's error body, a Python traceback or a
 * failed HTTP call that names its URL, so it can carry secrets the user never
 * asked for and has no right to see. Normal results are never passed through
 * here.
 * <p>
 * Two checks run on error text. First, every engine and project records the
 * values of its secret settings when it opens ({@link #recordSecrets}), and any
 * recorded value in the text is replaced. These are the exact strings the
 * engine loaded, from its smss or the secret store, so nothing is guessed.
 * Second, the shapes {@link AuditSanitizer#scrubText(String)} knows are
 * replaced: bearer tokens, {@code key=value} pairs with a secret key, JDBC
 * URLs, credentials in a URL, and private keys.
 */
public final class SecretRedactor {

	/** Shorter values match ordinary words too often to be replaced on sight. */
	private static final int MIN_SECRET_LENGTH = 8;
	private static final String BEARER_PREFIX = "Bearer ";
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	private static final Map<String, Set<String>> SECRETS_BY_OWNER = new ConcurrentHashMap<>();
	private static volatile List<String> secretsLongestFirst = List.of();

	private SecretRedactor() {

	}

	/**
	 * Records the secret values an engine or project loaded, so error text that
	 * contains them can be redacted. A value is secret when its key is
	 * ({@link SensitiveKeys#isSensitiveKey(String)}). Besides each value as is,
	 * this records its JSON escaped and URL encoded forms, the token after
	 * {@code Bearer }, and, for a JSON value, the values under its secret keys.
	 * Values are added to what the owner already recorded, so the connections of
	 * one engine, or several engines that share a key, never drop each other's
	 * values.
	 *
	 * @param ownerId    the engine or project id
	 * @param properties its smss values, including those from the secret store
	 */
	public static void recordSecrets(String ownerId, Properties properties) {
		if (properties == null || properties.isEmpty()) {
			return;
		}
		Map<Object, Object> entries;
		synchronized (properties) {
			entries = new HashMap<>(properties);
		}
		Set<String> found = new HashSet<>();
		for (Map.Entry<Object, Object> entry : entries.entrySet()) {
			if (entry.getKey() != null && SensitiveKeys.isSensitiveKey(entry.getKey().toString())) {
				addSecret(found, entry.getValue());
			}
		}
		if (found.isEmpty()) {
			return;
		}
		synchronized (SECRETS_BY_OWNER) {
			SECRETS_BY_OWNER.computeIfAbsent(ownerKey(ownerId), key -> ConcurrentHashMap.newKeySet()).addAll(found);
			rebuildLongestFirst();
		}
	}

	/**
	 * Drops the values recorded for an engine or project that was deleted. A value
	 * another owner also recorded stays redacted.
	 *
	 * @param ownerId the engine or project id
	 */
	public static void forgetSecrets(String ownerId) {
		synchronized (SECRETS_BY_OWNER) {
			if (SECRETS_BY_OWNER.remove(ownerKey(ownerId)) != null) {
				rebuildLongestFirst();
			}
		}
	}

	/**
	 * Removes secrets from error text: every recorded secret value, then the secret
	 * shapes {@link AuditSanitizer#scrubErrorText(String)} finds. Each is replaced
	 * with {@link AuditSanitizer#REDACTED}.
	 *
	 * @param text the error text
	 * @return the text without secrets; the same text when it holds none
	 */
	public static String redactError(String text) {
		if (text == null || text.isEmpty()) {
			return text;
		}
		String redacted = text;
		for (String secret : secretsLongestFirst) {
			if (redacted.contains(secret)) {
				redacted = redacted.replace(secret, AuditSanitizer.REDACTED);
			}
		}
		redacted = AuditSanitizer.scrubErrorText(redacted);
		return redacted.equals(text) ? text : redacted;
	}

	/**
	 * Removes secrets from an error value: text is passed to
	 * {@link #redactError(String)}, and maps, collections, arrays and Gson JSON are
	 * copied with each value redacted. A text value under a secret key is replaced
	 * whole; a list or map under one is redacted item by item. Other values, such
	 * as numbers, are left alone.
	 *
	 * @param value the error message or error details
	 * @return the value without secrets; the same object when it holds none
	 */
	public static Object redactErrorValue(Object value) {
		if (value instanceof String text) {
			return redactError(text);
		}
		if (value instanceof Map<?, ?> map) {
			Map<Object, Object> copy = new LinkedHashMap<>();
			boolean changed = false;
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				Object entryValue = entry.getValue();
				Object redacted = entryValue instanceof String && entry.getKey() instanceof String key
						&& SensitiveKeys.isSensitiveKey(key) ? AuditSanitizer.REDACTED : redactErrorValue(entryValue);
				changed |= redacted != entryValue;
				copy.put(entry.getKey(), redacted);
			}
			return changed ? copy : value;
		}
		if (value instanceof Collection<?> collection) {
			return redactItems(collection, value);
		}
		if (value instanceof Object[] array) {
			return redactItems(Arrays.asList(array), value);
		}
		if (value instanceof JsonElement json) {
			return redactJson(json);
		}
		return value;
	}

	private static Object redactItems(Collection<?> items, Object original) {
		List<Object> copy = new ArrayList<>(items.size());
		boolean changed = false;
		for (Object item : items) {
			Object redacted = redactErrorValue(item);
			changed |= redacted != item;
			copy.add(redacted);
		}
		return changed ? copy : original;
	}

	/**
	 * Redacts Gson JSON the way {@link #redactErrorValue(Object)} redacts maps and
	 * lists.
	 *
	 * @param json the JSON
	 * @return the JSON without secrets; the same object when it holds none
	 */
	private static JsonElement redactJson(JsonElement json) {
		if (json.isJsonPrimitive() && json.getAsJsonPrimitive().isString()) {
			String text = json.getAsString();
			String redacted = redactError(text);
			return redacted == text ? json : new JsonPrimitive(redacted);
		}
		if (json.isJsonArray()) {
			JsonArray copy = new JsonArray();
			boolean changed = false;
			for (JsonElement item : json.getAsJsonArray()) {
				JsonElement redacted = redactJson(item);
				changed |= redacted != item;
				copy.add(redacted);
			}
			return changed ? copy : json;
		}
		if (json.isJsonObject()) {
			JsonObject copy = new JsonObject();
			boolean changed = false;
			for (Map.Entry<String, JsonElement> entry : json.getAsJsonObject().entrySet()) {
				JsonElement member = entry.getValue();
				JsonElement redacted = member.isJsonPrimitive() && member.getAsJsonPrimitive().isString()
						&& SensitiveKeys.isSensitiveKey(entry.getKey()) ? new JsonPrimitive(AuditSanitizer.REDACTED)
								: redactJson(member);
				changed |= redacted != member;
				copy.add(entry.getKey(), redacted);
			}
			return changed ? copy : json;
		}
		return json;
	}

	private static void addSecret(Set<String> found, Object rawValue) {
		if (rawValue == null) {
			return;
		}
		String value = rawValue.toString().trim();
		if (value.length() < MIN_SECRET_LENGTH || Constants.SENSITIVE_INFO_MASK.equals(value)) {
			return;
		}
		addWithEncodings(found, value);
		if (value.regionMatches(true, 0, BEARER_PREFIX, 0, BEARER_PREFIX.length())) {
			addWithEncodings(found, value.substring(BEARER_PREFIX.length()).trim());
		}
		if (value.startsWith("{") || value.startsWith("[")) {
			addJsonSecrets(found, parseJson(value), false);
		}
	}

	/**
	 * Parses a value that looks like JSON.
	 *
	 * @param value the value
	 * @return the parsed JSON, or null when the value is not JSON after all
	 */
	private static JsonElement parseJson(String value) {
		try {
			return JsonParser.parseString(value);
		} catch (JsonParseException e) {
			return null;
		}
	}

	/**
	 * Records the string values in a JSON setting that sit under a secret key, or
	 * under a key ending in {@code key} (such as
	 * {@code Ocp-Apim-Subscription-Key}), at any depth.
	 *
	 * @param found          the values found so far
	 * @param element        the JSON value
	 * @param underSecretKey whether a key above this value is secret
	 */
	private static void addJsonSecrets(Set<String> found, JsonElement element, boolean underSecretKey) {
		if (element == null || element.isJsonNull()) {
			return;
		}
		if (element.isJsonPrimitive()) {
			if (underSecretKey) {
				addSecret(found, element.getAsString());
			}
			return;
		}
		if (element.isJsonArray()) {
			for (JsonElement item : element.getAsJsonArray()) {
				addJsonSecrets(found, item, underSecretKey);
			}
			return;
		}
		for (Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
			String key = entry.getKey();
			boolean secretKey = SensitiveKeys.isSensitiveKey(key) || SensitiveKeys.normalize(key).endsWith("key");
			addJsonSecrets(found, entry.getValue(), underSecretKey || secretKey);
		}
	}

	private static void addWithEncodings(Set<String> found, String value) {
		if (value.length() < MIN_SECRET_LENGTH) {
			return;
		}
		found.add(value);
		String json = GSON.toJson(value);
		found.add(json.substring(1, json.length() - 1));
		found.add(URLEncoder.encode(value, StandardCharsets.UTF_8));
	}

	private static void rebuildLongestFirst() {
		Set<String> all = new LinkedHashSet<>();
		for (Set<String> secrets : SECRETS_BY_OWNER.values()) {
			all.addAll(secrets);
		}
		List<String> sorted = new ArrayList<>(all);
		sorted.sort(Comparator.comparingInt(String::length).reversed());
		secretsLongestFirst = List.copyOf(sorted);
	}

	private static String ownerKey(String ownerId) {
		return ownerId == null ? "" : ownerId;
	}
}
