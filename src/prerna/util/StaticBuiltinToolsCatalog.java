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
package prerna.util;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

/**
 * Read-only view over the curated provider built-in tool catalog stored in
 * meta/builtin-tools.json.
 * <p>
 * The catalog is keyed by serving provider. A serving provider node holds
 * either tool definitions directly (openai, anthropic, google - each value
 * carries an "alias") or, for aggregators that host other vendors' models
 * (bedrock), a second level keyed by model provider. Mixing the two shapes
 * within one node is not supported.
 * <p>
 * A serving provider whose value is a string instead names another provider
 * key, as in "azure": "openai" - a host that exposes another provider's tool
 * surface unchanged does not have to duplicate its definitions. The alias is
 * resolved before anything else, so the resolved key is what the direct-tools
 * rule below compares the model provider against.
 * <p>
 * A tool definition may carry a "constraints" object whose "models" list
 * restricts the tool to specific model ids. The catalog is hand-maintained
 * and optional, so a missing file or an unknown provider yields no tools
 * rather than an error.
 */
public final class StaticBuiltinToolsCatalog {

	private static final Logger classLogger = LogManager.getLogger(StaticBuiltinToolsCatalog.class);
	private static final Gson GSON = new Gson();

	private static final String META_DIRECTORY = "meta";
	private static final String BUILTIN_TOOLS_FILE = "builtin-tools.json";
	private static final String TOOL_ALIAS = "alias";
	private static final Type TOOL_DEFINITION_TYPE = new TypeToken<LinkedHashMap<String, Object>>() {
	}.getType();
	private static final Object CACHE_LOCK = new Object();

	private static volatile CatalogCache catalogCache;

	private StaticBuiltinToolsCatalog() {
	}

	/**
	 * Location of the catalog file within the SEMOSS base folder.
	 */
	public static Path getCatalogFile() {
		String baseFolder = Utility.getBaseFolder();
		if (baseFolder == null || baseFolder.trim().isEmpty()) {
			throw new IllegalStateException("SEMOSS base folder is not configured");
		}
		return Paths.get(baseFolder, META_DIRECTORY, BUILTIN_TOOLS_FILE);
	}

	/**
	 * The built-in tools a model can use, keyed by canonical tool name. Returns
	 * an empty map when the install has no catalog file, the providers are
	 * unknown to the catalog, or every tool is ruled out by its model
	 * constraints.
	 *
	 * @param servingProvider lowercase catalog key for who hosts the model
	 *                        (openai, azure, anthropic, google, bedrock, ...)
	 * @param modelProvider   lowercase catalog key for who made the model
	 * @param modelId         provider model id used to evaluate per-tool model
	 *                        constraints; null skips that filter
	 */
	public static Map<String, Object> getTools(Path catalogFile, String servingProvider, String modelProvider,
			String modelId) {
		Map<String, Object> tools = new LinkedHashMap<>();
		if (!Files.isRegularFile(catalogFile)) {
			// the catalog is optional - installs without one simply offer no tools
			return tools;
		}

		// a direct provider serves its own models, so either provider can stand
		// in for a missing counterpart
		String serving = servingProvider != null ? servingProvider : modelProvider;
		String model = modelProvider != null ? modelProvider : servingProvider;
		if (serving == null) {
			return tools;
		}

		JsonObject catalog = loadCatalog(catalogFile);
		String resolvedServing = resolveProviderKey(catalog, serving);
		if (resolvedServing == null) {
			return tools;
		}
		JsonObject providerNode = catalog.getAsJsonObject(resolvedServing);

		JsonObject toolDefinitions;
		if (isToolMap(providerNode)) {
			// tools listed directly on the serving provider only apply to that
			// provider's own models - an anthropic model served through vertex
			// cannot use the google tools
			if (!resolvedServing.equals(model)) {
				return tools;
			}
			toolDefinitions = providerNode;
		} else {
			JsonElement nested = model == null ? null : providerNode.get(model);
			if (nested == null || !nested.isJsonObject()) {
				return tools;
			}
			toolDefinitions = nested.getAsJsonObject();
		}

		for (Map.Entry<String, JsonElement> entry : toolDefinitions.entrySet()) {
			if (!entry.getValue().isJsonObject()) {
				continue;
			}
			JsonObject definition = entry.getValue().getAsJsonObject();
			if (!modelSatisfiesConstraints(definition, modelId)) {
				continue;
			}
			tools.put(entry.getKey(), GSON.fromJson(definition, TOOL_DEFINITION_TYPE));
		}
		return tools;
	}

	/**
	 * Reduce a provider name from any of the shapes the platform records - the
	 * MODELPROVIDER / SERVINGPROVIDER columns (OPENAI, BEDROCK), the SMSS
	 * MODEL_TYPE (OPEN_AI, AZURE_OPEN_AI, VERTEX), or the SMSS PROVIDER
	 * variable (google, bedrock) - to the lowercase key the catalog uses.
	 * Returns null for a blank input.
	 */
	public static String normalizeProviderKey(String provider) {
		if (provider == null || provider.trim().isEmpty()) {
			return null;
		}
		String value = provider.trim().toLowerCase(Locale.ROOT).replaceAll("[\\s-]+", "_");
		return switch (value) {
		case "open_ai", "openai" -> "openai";
		case "azure_open_ai", "azure_openai" -> "azure";
		case "vertex", "vertex_ai", "google_vertex" -> "google";
		case "aws_bedrock", "amazon_bedrock" -> "bedrock";
		default -> value;
		};
	}

	/**
	 * The catalog key that actually holds the serving provider's tools, following
	 * any string-valued provider aliases along the way. Returns null when the
	 * chain leaves the catalog - an unknown provider offers no tools - and when a
	 * mis-edited file points the aliases in a circle.
	 */
	private static String resolveProviderKey(JsonObject catalog, String provider) {
		String key = provider;
		Set<String> visited = new LinkedHashSet<>();
		while (visited.add(key)) {
			JsonElement providerElement = catalog.get(key);
			if (providerElement == null) {
				return null;
			}
			if (providerElement.isJsonObject()) {
				return key;
			}
			if (!providerElement.isJsonPrimitive() || !providerElement.getAsJsonPrimitive().isString()) {
				return null;
			}
			key = providerElement.getAsString().trim().toLowerCase(Locale.ROOT);
		}
		classLogger.warn("The built-in tools catalog has a circular provider alias reached from '{}'",
				Utility.cleanLogString(provider));
		return null;
	}

	/**
	 * A node holds tool definitions when its values carry an alias; otherwise
	 * it is a second level keyed by model provider. An empty node reads as
	 * nested, which yields no tools either way.
	 */
	private static boolean isToolMap(JsonObject providerNode) {
		for (Map.Entry<String, JsonElement> entry : providerNode.entrySet()) {
			if (entry.getValue().isJsonObject() && entry.getValue().getAsJsonObject().has(TOOL_ALIAS)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Whether the model id passes the tool's constraints.models list. A tool
	 * without model constraints - or a call without a model id to check -
	 * passes. Both sides are compared through the same qualifier peeling the
	 * model catalog uses, so "us.openai.gpt-5.4" matches a constraint entry of
	 * "openai.gpt-5.4".
	 */
	private static boolean modelSatisfiesConstraints(JsonObject definition, String modelId) {
		JsonElement constraintsElement = definition.get("constraints");
		if (constraintsElement == null || !constraintsElement.isJsonObject()) {
			return true;
		}
		JsonElement modelsElement = constraintsElement.getAsJsonObject().get("models");
		if (modelsElement == null || !modelsElement.isJsonArray() || modelsElement.getAsJsonArray().size() == 0) {
			return true;
		}
		if (modelId == null || modelId.trim().isEmpty()) {
			return true;
		}

		Set<String> lookupIds = StaticModelMetadataCatalog.getLookupIds(modelId.trim().toLowerCase(Locale.ROOT));
		JsonArray models = modelsElement.getAsJsonArray();
		for (JsonElement model : models) {
			if (model == null || !model.isJsonPrimitive()) {
				continue;
			}
			String constraintModel = model.getAsString().trim().toLowerCase(Locale.ROOT);
			if (constraintModel.isEmpty()) {
				continue;
			}
			for (String constraintLookupId : StaticModelMetadataCatalog.getLookupIds(constraintModel)) {
				if (lookupIds.contains(constraintLookupId)) {
					return true;
				}
			}
		}
		return false;
	}

	private static JsonObject loadCatalog(Path catalogFile) {
		Path normalizedPath = catalogFile.toAbsolutePath().normalize();
		try {
			BasicFileAttributes attributes = Files.readAttributes(normalizedPath, BasicFileAttributes.class);
			if (!attributes.isRegularFile()) {
				throw new IllegalStateException("Built-in tools catalog path is not a file: " + normalizedPath);
			}

			CatalogCache currentCache = catalogCache;
			if (currentCache != null && currentCache.matches(normalizedPath, attributes)) {
				return currentCache.catalog();
			}

			synchronized (CACHE_LOCK) {
				attributes = Files.readAttributes(normalizedPath, BasicFileAttributes.class);
				currentCache = catalogCache;
				if (currentCache != null && currentCache.matches(normalizedPath, attributes)) {
					return currentCache.catalog();
				}

				JsonElement parsed;
				try (Reader reader = Files.newBufferedReader(normalizedPath, StandardCharsets.UTF_8)) {
					parsed = JsonParser.parseReader(reader);
				}
				if (!parsed.isJsonObject()) {
					throw new IllegalStateException(
							"Built-in tools catalog file must contain a JSON object: " + normalizedPath);
				}

				JsonObject catalog = parsed.getAsJsonObject();
				catalogCache = new CatalogCache(normalizedPath, attributes.lastModifiedTime(), attributes.size(),
						catalog);
				return catalog;
			}
		} catch (IOException | JsonParseException e) {
			classLogger.error("Unable to read the built-in tools catalog from {}", normalizedPath, e);
			throw new IllegalStateException("Unable to read the built-in tools catalog from " + normalizedPath, e);
		}
	}

	private record CatalogCache(Path path, FileTime lastModifiedTime, long size, JsonObject catalog) {

		private boolean matches(Path catalogPath, BasicFileAttributes attributes) {
			return this.path.equals(catalogPath) && this.lastModifiedTime.equals(attributes.lastModifiedTime())
					&& this.size == attributes.size();
		}
	}
}
