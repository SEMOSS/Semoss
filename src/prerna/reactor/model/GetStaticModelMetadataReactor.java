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
package prerna.reactor.model;

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
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetStaticModelMetadataReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetStaticModelMetadataReactor.class);

	static final String MODEL_ID_KEY = "modelId";
	private static final String META_DIRECTORY = "meta";
	private static final String MODEL_METADATA_FILE = "model.json";
	private static final Type MODEL_METADATA_TYPE = new TypeToken<LinkedHashMap<String, Object>>() {
	}.getType();
	private static final Object CACHE_LOCK = new Object();

	private static volatile MetadataCache metadataCache;

	public GetStaticModelMetadataReactor() {
		this.keysToGet = new String[] { MODEL_ID_KEY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String modelId = requireValue(this.keyValue.get(MODEL_ID_KEY), MODEL_ID_KEY);

		Map<String, Object> metadata = getModelMetadata(getMetadataFile(), modelId);
		return new NounMetadata(metadata, PixelDataType.MAP);
	}

	static Map<String, Object> getModelMetadata(Path metadataFile, String modelId) {
		JsonObject allMetadata = loadMetadata(metadataFile);
		JsonElement metadata = findMetadata(allMetadata, modelId);
		if (metadata == null || metadata.isJsonNull()) {
			return new LinkedHashMap<>();
		}
		if (!metadata.isJsonObject()) {
			throw new IllegalStateException("Static model metadata entry '" + modelId + "' must be a JSON object");
		}
		return flattenMetadata(metadata.getAsJsonObject());
	}

	private static JsonElement findMetadata(JsonObject allMetadata, String modelId) {
		Set<String> lookupIds = getLookupIds(modelId);
		for (String lookupId : lookupIds) {
			JsonElement metadata = allMetadata.get(lookupId);
			if (metadata != null) {
				return metadata;
			}
		}

		for (Map.Entry<String, JsonElement> entry : allMetadata.entrySet()) {
			JsonElement candidate = entry.getValue();
			if (!candidate.isJsonObject()) {
				continue;
			}
			JsonElement candidateId = candidate.getAsJsonObject().get("id");
			if (candidateId != null && candidateId.isJsonPrimitive()
					&& lookupIds.contains(candidateId.getAsString())) {
				return candidate;
			}
		}
		return null;
	}

	private static Set<String> getLookupIds(String modelId) {
		LinkedHashSet<String> lookupIds = new LinkedHashSet<>();
		lookupIds.add(modelId);

		int providerSeparator = modelId.indexOf('/');
		if (providerSeparator >= 0 && providerSeparator < modelId.length() - 1) {
			lookupIds.add(modelId.substring(providerSeparator + 1));
		}

		// Bedrock Anthropic IDs use a dot prefix and optional deployment version,
		// while the catalog file is keyed by Anthropic's model ID.
		if (modelId.startsWith("anthropic.") && modelId.length() > "anthropic.".length()) {
			lookupIds.add(modelId.substring("anthropic.".length()));
		}

		for (String lookupId : new LinkedHashSet<>(lookupIds)) {
			lookupIds.add(lookupId.replace('@', '-'));
			lookupIds.add(lookupId.replaceFirst("-v\\d+(?::\\d+)?$", ""));
		}

		for (String lookupId : new LinkedHashSet<>(lookupIds)) {
			lookupIds.add(lookupId.replaceFirst("-v\\d+(?::\\d+)?$", ""));
		}
		return lookupIds;
	}

	private static Map<String, Object> flattenMetadata(JsonObject metadata) {
		JsonObject flattened = metadata.deepCopy();

		JsonElement modalities = metadata.get("modalities");
		if (modalities != null && modalities.isJsonObject()) {
			JsonObject modalityValues = modalities.getAsJsonObject();
			copyJsonProperty(modalityValues, "input", flattened, "input_modalities");
			copyJsonProperty(modalityValues, "output", flattened, "output_modalities");
		}

		JsonElement limit = metadata.get("limit");
		if (limit != null && limit.isJsonObject()) {
			JsonObject limitValues = limit.getAsJsonObject();
			copyJsonProperty(limitValues, "context", flattened, "context_length");
			copyJsonProperty(limitValues, "input", flattened, "max_input_tokens");
			copyJsonProperty(limitValues, "output", flattened, "max_output_tokens");
		}

		copyJsonProperty(metadata, "knowledge", flattened, "knowledge_cutoff");
		JsonElement openRouter = metadata.get("openrouter");
		if (openRouter != null && openRouter.isJsonObject()) {
			JsonObject openRouterValues = openRouter.getAsJsonObject();
			copyJsonProperty(openRouterValues, "supported_parameters", flattened, "supported_parameters");
			copyJsonProperty(openRouterValues, "reasoning", flattened, "reasoning_config");
		}
		return GSON.fromJson(flattened, MODEL_METADATA_TYPE);
	}

	private static void copyJsonProperty(JsonObject source, String sourceKey, JsonObject target, String targetKey) {
		JsonElement value = source.get(sourceKey);
		if (value != null && !value.isJsonNull()) {
			target.add(targetKey, value.deepCopy());
		}
	}

	Path getMetadataFile() {
		String baseFolder = Utility.getBaseFolder();
		if (baseFolder == null || baseFolder.trim().isEmpty()) {
			throw new IllegalStateException("SEMOSS base folder is not configured");
		}
		return Paths.get(baseFolder, META_DIRECTORY, MODEL_METADATA_FILE);
	}

	private static JsonObject loadMetadata(Path metadataFile) {
		Path normalizedPath = metadataFile.toAbsolutePath().normalize();
		try {
			BasicFileAttributes attributes = Files.readAttributes(normalizedPath, BasicFileAttributes.class);
			if (!attributes.isRegularFile()) {
				throw new IllegalStateException("Static model metadata path is not a file: " + normalizedPath);
			}

			MetadataCache currentCache = metadataCache;
			if (currentCache != null && currentCache.matches(normalizedPath, attributes)) {
				return currentCache.metadata();
			}

			synchronized (CACHE_LOCK) {
				attributes = Files.readAttributes(normalizedPath, BasicFileAttributes.class);
				currentCache = metadataCache;
				if (currentCache != null && currentCache.matches(normalizedPath, attributes)) {
					return currentCache.metadata();
				}

				JsonElement parsed;
				try (Reader reader = Files.newBufferedReader(normalizedPath, StandardCharsets.UTF_8)) {
					parsed = JsonParser.parseReader(reader);
				}
				if (!parsed.isJsonObject()) {
					throw new IllegalStateException("Static model metadata file must contain a JSON object: "
							+ normalizedPath);
				}

				JsonObject metadata = parsed.getAsJsonObject();
				metadataCache = new MetadataCache(normalizedPath, attributes.lastModifiedTime(), attributes.size(),
						metadata);
				return metadata;
			}
		} catch (IOException | JsonParseException e) {
			classLogger.error("Unable to read static model metadata from {}", normalizedPath, e);
			throw new IllegalStateException("Unable to read static model metadata from " + normalizedPath, e);
		}
	}

	private static String requireValue(String value, String key) {
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException("Must input a " + key);
		}
		return value.trim();
	}

	@Override
	public String getReactorDescription() {
		return "Returns static metadata for a model from meta/model.json";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(MODEL_ID_KEY)) {
			return "The catalog model key or fully qualified provider model ID in meta/model.json";
		}
		return super.getDescriptionForKey(key);
	}

	private record MetadataCache(Path path, FileTime lastModifiedTime, long size, JsonObject metadata) {

		private boolean matches(Path metadataPath, BasicFileAttributes attributes) {
			return this.path.equals(metadataPath) && this.lastModifiedTime.equals(attributes.lastModifiedTime())
					&& this.size == attributes.size();
		}
	}
}
