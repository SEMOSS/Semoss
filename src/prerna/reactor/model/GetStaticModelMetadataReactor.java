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
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
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
		this.keysToGet = new String[] { MODEL_ID_KEY, ReactorKeysEnum.PROVIDER.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String modelId = requireValue(this.keyValue.get(MODEL_ID_KEY), MODEL_ID_KEY);
		String provider = requireValue(this.keyValue.get(ReactorKeysEnum.PROVIDER.getKey()),
				ReactorKeysEnum.PROVIDER.getKey());

		Map<String, Object> metadata = getModelMetadata(getMetadataFile(), provider, modelId);
		return new NounMetadata(metadata, PixelDataType.MAP);
	}

	static Map<String, Object> getModelMetadata(Path metadataFile, String provider, String modelId) {
		String metadataKey = provider + "/" + modelId;
		JsonElement metadata = loadMetadata(metadataFile).get(metadataKey);
		if (metadata == null || metadata.isJsonNull()) {
			return new LinkedHashMap<>();
		}
		if (!metadata.isJsonObject()) {
			throw new IllegalStateException("Static model metadata entry '" + metadataKey + "' must be a JSON object");
		}
		return GSON.fromJson(metadata, MODEL_METADATA_TYPE);
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
		return "Returns static metadata for a provider model from meta/model.json";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(MODEL_ID_KEY)) {
			return "The provider-specific model ID";
		}
		if (key.equals(ReactorKeysEnum.PROVIDER.getKey())) {
			return "The model provider used to build the provider/modelId lookup key";
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
