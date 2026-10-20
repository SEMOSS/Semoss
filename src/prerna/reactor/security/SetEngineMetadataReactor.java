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
package prerna.reactor.security;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;

public class SetEngineMetadataReactor extends AbstractSetMetadataReactor {
	private static final String CAPABILITIES_KEY = "capabilities";

	public SetEngineMetadataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), META, ReactorKeysEnum.JSON_CLEANUP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = UploadInputUtility.getEngineNameOrId(this.store,
				this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Engine does not exist or user does not have access to edit");
		}

		Map<String, Object> metadata = new LinkedHashMap<>(getMetaMap());
		Map<String, Object> capabilityUpdates = getCapabilityUpdates(engineId, metadata);
		// check for invalid metakeys
		List<String> validMetakeys = SecurityEngineUtils.getAllMetakeys();
		if (!validMetakeys.containsAll(metadata.keySet())) {
			throw new IllegalArgumentException("Unallowed metakeys. Can only use: " + String.join(", ", validMetakeys));
		}

		SecurityEngineUtils.updateEngineMetadata(engineId, metadata);
		if (capabilityUpdates != null && !capabilityUpdates.isEmpty()) {
			SecurityModelMetadataUtils.updateModelMetadata(engineId, capabilityUpdates);
		}
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(
				NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the engine"));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Defines metadata on an engine, including model capabilities when supplied";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(META)) {
			return "Map containing engine metadata values. Model engines may also include editable capability, modality, token, and built-in tool values in a capabilities map.";
		} else if (key.equals(ReactorKeysEnum.JSON_CLEANUP.getKey())) {
			return "Legacy compatibility flag for older clients that sent escaped JSON strings. Modern clients should not set this.";
		}
		return super.getDescriptionForKey(key);
	}

	private Map<String, Object> getCapabilityUpdates(String engineId, Map<String, Object> metadata) {
		if (!metadata.containsKey(CAPABILITIES_KEY)) {
			return null;
		}

		Object value = metadata.remove(CAPABILITIES_KEY);
		if (!(value instanceof Map<?, ?> capabilities)) {
			throw new IllegalArgumentException("Capabilities must be a map");
		}
		if (SecurityEngineUtils.getEngineType(engineId) != IEngine.CATALOG_TYPE.MODEL) {
			throw new IllegalArgumentException("Capabilities can only be set on a model engine");
		}

		Map<String, Object> updates = new LinkedHashMap<>();
		for (Map.Entry<?, ?> entry : capabilities.entrySet()) {
			if (!(entry.getKey() instanceof String capabilityKey)) {
				throw new IllegalArgumentException("Capability field names must be strings");
			}

			String modelMetadataKey = switch (capabilityKey) {
			case "capability" -> Constants.MODEL_CAPABILITY;
			case "inputModalities" -> Constants.INPUT_MODALITIES;
			case "outputModalities" -> Constants.OUTPUT_MODALITIES;
			case "contextWindow" -> Constants.CONTEXT_WINDOW;
			case "maxOutputTokens" -> Constants.MAX_TOKENS;
			case "builtinTools" -> Constants.BUILTIN_TOOLS;
			default -> throw new IllegalArgumentException("Unallowed capability field " + capabilityKey);
			};
			updates.put(modelMetadataKey, entry.getValue());
		}

		return SecurityModelMetadataUtils.normalizeModelDetails(updates);
	}

}
