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

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.StaticBuiltinToolsCatalog;
import prerna.util.StaticModelMetadataCatalog;
import prerna.util.Utility;

/**
 * Offer the provider-hosted built-in tools a model engine can use, resolved
 * from meta/builtin-tools.json by the engine's serving provider and model
 * provider. The same vendor's models do not share capabilities across hosts -
 * an OpenAI model on Bedrock is not offered the OpenAI-hosted tools - which is
 * why both providers participate in the lookup.
 */
public class GetModelBuiltinToolsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetModelBuiltinToolsReactor.class);

	/**
	 * A dotted qualifier ahead of the model name, as in "us.anthropic.claude-*".
	 * Digits end the qualifiers so version dots ("gpt-5.4") are left alone.
	 */
	private static final Pattern QUALIFIER_PREFIX_PATTERN = Pattern.compile("^([a-z][a-z-]*)\\.(?=.)");

	/**
	 * The SMSS variable some engines use to name who actually hosts the model,
	 * as in the Anthropic-on-Vertex engines. There is no shared constant for it.
	 */
	private static final String SMSS_PROVIDER = "PROVIDER";

	static final String SERVING_PROVIDER_KEY = "servingProvider";
	static final String MODEL_PROVIDER_KEY = "modelProvider";
	static final String MODEL_ID_KEY = "modelId";

	public GetModelBuiltinToolsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), SERVING_PROVIDER_KEY, MODEL_PROVIDER_KEY,
				MODEL_ID_KEY };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = trimToNull(this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
		String servingProviderInput = trimToNull(this.keyValue.get(SERVING_PROVIDER_KEY));
		String modelProviderInput = trimToNull(this.keyValue.get(MODEL_PROVIDER_KEY));
		String modelIdInput = trimToNull(this.keyValue.get(MODEL_ID_KEY));

		// an existing engine resolves everything itself; the explicit inputs
		// serve the import flow, where the engine does not exist yet
		Map<String, Object> metadata = null;
		Properties smssProp = null;
		if (engineId != null) {
			User user = this.insight.getUser();
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);
			if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
				throw new IllegalArgumentException(
						"Model engine does not exist or user does not have access to view it");
			}
			if (SecurityEngineUtils.getEngineType(engineId) != IEngine.CATALOG_TYPE.MODEL) {
				throw new IllegalArgumentException("Engine is not a model engine");
			}
			metadata = SecurityModelMetadataUtils.getModelMetadata(engineId);
			smssProp = loadSmssProperties(engineId);
		} else if (servingProviderInput == null && modelProviderInput == null && modelIdInput == null) {
			throw new IllegalArgumentException(
					"Must input a model engine id, or a serving provider, model provider, or model id");
		}

		String modelId = modelIdInput != null ? modelIdInput : resolveModelId(metadata, smssProp);
		String servingProvider = servingProviderInput != null
				? StaticBuiltinToolsCatalog.normalizeProviderKey(servingProviderInput)
				: resolveServingProvider(metadata, smssProp);
		String modelProvider = modelProviderInput != null
				? StaticBuiltinToolsCatalog.normalizeProviderKey(modelProviderInput)
				: resolveModelProvider(metadata, modelId);

		Map<String, Object> tools = getTools(servingProvider, modelProvider, modelId);

		Map<String, Object> response = new LinkedHashMap<>();
		response.put("engineId", engineId == null ? "" : engineId);
		response.put("modelId", modelId == null ? "" : modelId);
		response.put("modelProvider", modelProvider == null ? "" : modelProvider);
		response.put("servingProvider", servingProvider == null ? "" : servingProvider);
		response.put("tools", tools);
		Object selected = metadata == null ? null : metadata.get("builtinTools");
		response.put("selected", selected == null ? new LinkedHashMap<>() : selected);
		return new NounMetadata(response, PixelDataType.MAP);
	}

	Map<String, Object> getTools(String servingProvider, String modelProvider, String modelId) {
		return StaticBuiltinToolsCatalog.getTools(StaticBuiltinToolsCatalog.getCatalogFile(), servingProvider,
				modelProvider, modelId);
	}

	/**
	 * The provider model id, preferring the saved metadata row over the smss.
	 */
	private static String resolveModelId(Map<String, Object> metadata, Properties smssProp) {
		String modelId = metadata == null ? null : trimToNull(metadata.get("modelId"));
		if (modelId == null && smssProp != null) {
			modelId = trimToNull(smssProp.getProperty(Constants.MODEL));
		}
		return modelId;
	}

	/**
	 * Who hosts the model, as a lowercase catalog key. The saved metadata wins
	 * when set; otherwise the smss PROVIDER variable, then the engine's smss
	 * MODEL_TYPE, which names the client implementation and so tracks the host.
	 */
	private static String resolveServingProvider(Map<String, Object> metadata, Properties smssProp) {
		String servingProvider = metadata == null ? null
				: StaticBuiltinToolsCatalog.normalizeProviderKey(trimToNull(metadata.get("servingProvider")));
		if (servingProvider == null && smssProp != null) {
			servingProvider = StaticBuiltinToolsCatalog
					.normalizeProviderKey(trimToNull(smssProp.getProperty(SMSS_PROVIDER)));
		}
		if (servingProvider == null && smssProp != null) {
			servingProvider = StaticBuiltinToolsCatalog
					.normalizeProviderKey(trimToNull(smssProp.getProperty(IModelEngine.MODEL_TYPE)));
		}
		return servingProvider;
	}

	/**
	 * Who made the model, as a lowercase catalog key. The saved metadata wins
	 * when set; otherwise the model catalog's provider field, then the vendor
	 * qualifier that aggregator hosts prefix onto their model ids.
	 */
	private static String resolveModelProvider(Map<String, Object> metadata, String modelId) {
		String modelProvider = metadata == null ? null
				: StaticBuiltinToolsCatalog.normalizeProviderKey(trimToNull(metadata.get("modelProvider")));
		if (modelProvider == null && modelId != null) {
			try {
				Map<String, Object> catalogEntry = StaticModelMetadataCatalog
						.getFlattenedMetadata(StaticModelMetadataCatalog.getMetadataFile(), modelId);
				modelProvider = StaticBuiltinToolsCatalog.normalizeProviderKey(trimToNull(catalogEntry.get("provider")));
			} catch (RuntimeException e) {
				classLogger.warn("Unable to read the static model catalog while resolving the provider for model {}",
						Utility.cleanLogString(modelId), e);
			}
		}
		if (modelProvider == null) {
			modelProvider = parseProviderQualifier(modelId);
		}
		return modelProvider;
	}

	/**
	 * The last dotted qualifier ahead of the model name - "us.anthropic.claude-*"
	 * carries its maker behind the region. Returns null when there is none.
	 */
	static String parseProviderQualifier(String modelId) {
		if (modelId == null || modelId.trim().isEmpty()) {
			return null;
		}
		String remainder = modelId.trim().toLowerCase(Locale.ROOT);
		int providerSeparator = remainder.indexOf('/');
		if (providerSeparator >= 0 && providerSeparator < remainder.length() - 1) {
			remainder = remainder.substring(providerSeparator + 1);
		}
		String qualifier = null;
		Matcher matcher = QUALIFIER_PREFIX_PATTERN.matcher(remainder);
		while (matcher.find()) {
			qualifier = matcher.group(1);
			remainder = remainder.substring(matcher.end());
			matcher = QUALIFIER_PREFIX_PATTERN.matcher(remainder);
		}
		return qualifier;
	}

	/**
	 * The engine's smss properties, or null when they cannot be read - the
	 * saved metadata row still drives the lookup in that case.
	 */
	private static Properties loadSmssProperties(String engineId) {
		Object smssFile = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);
		if (smssFile == null) {
			return null;
		}
		try {
			return Utility.loadProperties(smssFile.toString());
		} catch (Exception e) {
			classLogger.warn("Unable to read the smss file for engine {}", engineId, e);
			return null;
		}
	}

	private static String trimToNull(Object value) {
		if (value == null) {
			return null;
		}
		String stringValue = value.toString().trim();
		return stringValue.isEmpty() ? null : stringValue;
	}

	@Override
	public String getReactorDescription() {
		return "Returns the provider-hosted built-in tools available to a model from meta/builtin-tools.json, resolved by serving provider and model provider. Pass an existing engine to resolve both from it (alongside its saved tool selection), or pass the providers and model id directly when the engine does not exist yet";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The id or name of an existing model engine; optional when the providers are passed directly";
		}
		if (key.equals(SERVING_PROVIDER_KEY)) {
			return "Who hosts the model (openai, anthropic, google, bedrock, azure); the SMSS MODEL_TYPE values are also accepted";
		}
		if (key.equals(MODEL_PROVIDER_KEY)) {
			return "Who made the model (openai, anthropic, google); inferred from the model id when omitted";
		}
		if (key.equals(MODEL_ID_KEY)) {
			return "The provider model id, used to infer the model provider and evaluate per-tool model constraints";
		}
		return super.getDescriptionForKey(key);
	}
}
