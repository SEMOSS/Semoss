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
package prerna.reactor.automation;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.automation.utils.AutomationExecutionUtils;
import prerna.util.Utility;

/**
 * Generates deterministic managed Python source and its authoring metadata.
 *
 * <p>This is the common source-generation path for initial step creation, previews, and explicit
 * updates. It resolves engine aliases and validates engine access before delegating source
 * rendering to {@link AutomationStepTemplateRegistry}.
 */
public final class AutomationStepGenerationService {

	public static final int TEMPLATE_VERSION = 1;

	private AutomationStepGenerationService() {
	}

	/**
	 * Parses, authorizes, and renders a managed automation step.
	 *
	 * @param user authenticated requesting user
	 * @param nodeType canvas node type
	 * @param rawOrBase64Config JSON configuration, optionally Base64 encoded
	 * @return generated source and deterministic authoring metadata
	 */
	public static GeneratedStep generate(User user, String nodeType, String rawOrBase64Config) {
		Map<String, Object> config = parseConfig(rawOrBase64Config);
		AutomationStepTemplateRegistry.ActionDefinition action =
				AutomationStepTemplateRegistry.selectAction(nodeType, config);
		return generate(nodeType, resolveEngineConfig(user, action, config));
	}

	/**
	 * Parses, authorizes, and renders a step selected by its stable action identifier.
	 * The action catalog owns the internal canvas node type and operation so MCP callers
	 * cannot submit an invalid combination.
	 *
	 * @param user authenticated requesting user
	 * @param actionId approved action identifier, such as {@code model.llm}
	 * @param rawOrBase64Config JSON configuration, optionally Base64 encoded
	 * @return generated source and deterministic authoring metadata
	 */
	public static GeneratedStep generateForAction(User user, String actionId, String rawOrBase64Config) {
		AutomationStepTemplateRegistry.ActionDefinition action =
				AutomationStepTemplateRegistry.getAction(actionId);
		Map<String, Object> config = parseConfig(rawOrBase64Config);
		config.put(AutomationConstants.CONFIG_OPERATION, action.getOperation());
		return generate(action.getNodeType(), resolveEngineConfig(user, action, config));
	}

	/**
	 * Renders an already-authorized configuration. Intended for internal callers and focused tests.
	 *
	 * @param nodeType canvas node type
	 * @param config validated configuration with resolved engine ID when applicable
	 * @return generated source and deterministic authoring metadata
	 */
	static GeneratedStep generate(String nodeType, Map<String, Object> config) {
		AutomationStepTemplateRegistry.GeneratedStep rendered =
				AutomationStepTemplateRegistry.generate(nodeType, config);
		String setupHash = sha256(canonicalSetupJson(nodeType, config));
		return new GeneratedStep(rendered.getActionId(), rendered.getDescription(), rendered.getUsage(),
				rendered.getSource(), sha256(rendered.getSource()), setupHash, TEMPLATE_VERSION, config);
	}

	/**
	 * Compares a saved source with generated source without persisting either value.
	 *
	 * @param currentSource existing project-owned source
	 * @param proposed generated source metadata
	 * @return preview data suitable for explicit author approval
	 */
	public static Preview preview(String currentSource, GeneratedStep proposed) {
		if (currentSource == null) {
			throw new IllegalArgumentException("Current automation step source is required.");
		}
		return new Preview(currentSource, sha256(currentSource), proposed,
				!currentSource.equals(proposed.getSource()));
	}

	/**
	 * Computes a SHA-256 digest as lowercase hexadecimal.
	 *
	 * @param value UTF-8 text to hash
	 * @return digest
	 */
	public static String sha256(String value) {
		try {
			byte[] digest = MessageDigest.getInstance("SHA-256")
					.digest(value.getBytes(StandardCharsets.UTF_8));
			StringBuilder hex = new StringBuilder(digest.length * 2);
			for (byte valueByte : digest) {
				hex.append(String.format("%02x", valueByte));
			}
			return hex.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable.", e);
		}
	}

	private static Map<String, Object> parseConfig(String rawOrBase64Config) {
		if (rawOrBase64Config == null || rawOrBase64Config.isBlank()) {
			throw new IllegalArgumentException("Must provide config.");
		}
		Object parsed;
		try {
			parsed = AutomationExecutionUtils.GSON.fromJson(decodeBase64OrRaw(rawOrBase64Config), Object.class);
		} catch (JsonSyntaxException e) {
			throw new IllegalArgumentException("Automation step config must be valid JSON.", e);
		}
		if (!(parsed instanceof Map<?, ?> map)) {
			throw new IllegalArgumentException("Automation step config must be a JSON object.");
		}

		Map<String, Object> config = new LinkedHashMap<>();
		for (Map.Entry<?, ?> entry : map.entrySet()) {
			if (!(entry.getKey() instanceof String key)) {
				throw new IllegalArgumentException("Automation step config must use string keys.");
			}
			config.put(key, entry.getValue());
		}
		return config;
	}

	private static String decodeBase64OrRaw(String value) {
		try {
			return new String(Base64.getDecoder().decode(value.trim()), StandardCharsets.UTF_8);
		} catch (IllegalArgumentException e) {
			return value;
		}
	}

	private static Map<String, Object> resolveEngineConfig(User user,
			AutomationStepTemplateRegistry.ActionDefinition action, Map<String, Object> config) {
		if (action.getExpectedCatalogType() == null) {
			return new LinkedHashMap<>(config);
		}

		Object configuredEngine = config.get(AutomationConstants.CONFIG_ENGINE_ID);
		if (!(configuredEngine instanceof String engineId) || engineId.isBlank()) {
			if ("model.llm".equals(action.getActionId())) {
				return new LinkedHashMap<>(config);
			}
			throw new IllegalArgumentException("Automation step config field \"engineId\" must be a nonblank string.");
		}
		String resolvedEngineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);
		validateEngineAccess(user, action, resolvedEngineId);

		IEngine engine = Utility.getEngine(resolvedEngineId);
		if (engine == null) {
			throw new IllegalArgumentException("Engine could not be loaded: " + resolvedEngineId);
		}
		AutomationStepTemplateRegistry.validateEngineCatalog(action, engine.getCatalogType());

		Map<String, Object> resolvedConfig = new LinkedHashMap<>(config);
		resolvedConfig.put(AutomationConstants.CONFIG_ENGINE_ID, resolvedEngineId);
		return resolvedConfig;
	}

	private static void validateEngineAccess(User user, AutomationStepTemplateRegistry.ActionDefinition action,
			String engineId) {
		boolean authorized = action.requiresEditAccess()
				? SecurityEngineUtils.userCanEditEngine(user, engineId)
				: SecurityEngineUtils.userCanViewEngine(user, engineId);
		if (!authorized) {
			throw new IllegalArgumentException("Engine does not exist or user does not have "
					+ (action.requiresEditAccess() ? "edit" : "view") + " access: " + engineId);
		}
	}

	private static String canonicalSetupJson(String nodeType, Map<String, Object> config) {
		Map<String, Object> setup = new LinkedHashMap<>();
		setup.put("config", config);
		setup.put("nodeType", nodeType);
		return AutomationExecutionUtils.GSON.toJson(canonicalize(AutomationExecutionUtils.GSON.toJsonTree(setup)));
	}

	private static JsonElement canonicalize(JsonElement value) {
		if (value == null || value.isJsonNull()) {
			return JsonNull.INSTANCE;
		}
		if (value.isJsonArray()) {
			JsonArray canonical = new JsonArray();
			for (JsonElement entry : value.getAsJsonArray()) {
				canonical.add(canonicalize(entry));
			}
			return canonical;
		}
		if (!value.isJsonObject()) {
			return JsonParser.parseString(value.toString());
		}

		JsonObject canonical = new JsonObject();
		TreeMap<String, JsonElement> sorted = new TreeMap<>();
		for (Map.Entry<String, JsonElement> entry : value.getAsJsonObject().entrySet()) {
			sorted.put(entry.getKey(), entry.getValue());
		}
		for (Map.Entry<String, JsonElement> entry : sorted.entrySet()) {
			canonical.add(entry.getKey(), canonicalize(entry.getValue()));
		}
		return canonical;
	}

	/**
	 * Generated source, template details, and deterministic authoring hashes.
	 */
	public static final class GeneratedStep {
		private final String actionId;
		private final String description;
		private final String usage;
		private final String source;
		private final String sourceHash;
		private final String setupHash;
		private final int templateVersion;
		private final Map<String, Object> resolvedConfig;

		private GeneratedStep(String actionId, String description, String usage, String source, String sourceHash,
				String setupHash, int templateVersion, Map<String, Object> resolvedConfig) {
			this.actionId = actionId;
			this.description = description;
			this.usage = usage;
			this.source = source;
			this.sourceHash = sourceHash;
			this.setupHash = setupHash;
			this.templateVersion = templateVersion;
			this.resolvedConfig = Collections.unmodifiableMap(new LinkedHashMap<>(resolvedConfig));
		}

		public String getActionId() {
			return actionId;
		}

		public String getDescription() {
			return description;
		}

		public String getUsage() {
			return usage;
		}

		public String getSource() {
			return source;
		}

		public String getSourceHash() {
			return sourceHash;
		}

		public String getSetupHash() {
			return setupHash;
		}

		public int getTemplateVersion() {
			return templateVersion;
		}

		/**
		 * Returns the validated configuration used to render this source, including a resolved
		 * canonical engine ID where the action requires an engine.
		 */
		public Map<String, Object> getResolvedConfig() {
			return resolvedConfig;
		}
	}

	/**
	 * In-memory comparison of a saved step and a proposed generated replacement.
	 */
	public static final class Preview {
		private final String currentSource;
		private final String currentSourceHash;
		private final GeneratedStep proposed;
		private final boolean changed;

		private Preview(String currentSource, String currentSourceHash, GeneratedStep proposed, boolean changed) {
			this.currentSource = currentSource;
			this.currentSourceHash = currentSourceHash;
			this.proposed = proposed;
			this.changed = changed;
		}

		public String getCurrentSource() {
			return currentSource;
		}

		public String getCurrentSourceHash() {
			return currentSourceHash;
		}

		public GeneratedStep getProposed() {
			return proposed;
		}

		public boolean isChanged() {
			return changed;
		}
	}
}
