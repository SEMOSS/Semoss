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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.google.gson.GsonBuilder;

import org.json.JSONException;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.interceptor.GenericGuardrailInputOutputReactor;
import prerna.reactor.interceptor.GenericGuardrailInputReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * Validates and writes a model engine's guardrail pipeline configuration (the
 * file the PIPELINE smss key points at). Because the guardrail proxy re-reads
 * the pipeline file every time the model is retrieved, writing the file is
 * enough for the change to apply immediately - no engine reload is required.
 * The PIPELINE smss key is added when missing. An empty pipelines map removes
 * all guardrails. Requires edit access to the engine.
 *
 * Note pixel map literals deliver numeric values as doubles (1 arrives as
 * 1.0); directParameters values are written as provided.
 */
public class UpdateModelGuardrailConfigReactor extends AbstractReactor {

	private static final String PIPELINES_KEY = "pipelines";
	private static final String INPUT_KEY = "input";
	private static final String OUTPUT_KEY = "output";
	private static final String REACTOR_CLASS_KEY = "reactorClass";
	private static final String PARAMS_KEY = "params";
	private static final String GUARDRAIL_ENGINE_ID_KEY = "guardrailEngineId";
	private static final String BLOCK_ON_FAILURE_KEY = "blockOnGuardrailFailure";
	private static final String MASK_ON_FAILURE_KEY = "maskOnGuardrailFailure";
	private static final String MASK_TARGET_PARAM_KEY = "maskTargetParam";
	private static final String INPUT_MAPPING_KEY = "inputMapping";
	private static final String DIRECT_PARAMETERS_KEY = "directParameters";
	private static final String DEFAULT_MASK_TARGET_PARAM = "prompt";
	private static final String DEFAULT_PIPELINE_FILE = "pipeline.json";

	// the output slot is instantiated as IOutputReactor by the pipeline
	// handler - an input-only class there breaks every call to the engine at
	// proxy creation, so the whitelists are enforced per slot
	private static final Set<String> INPUT_REACTOR_WHITELIST = new HashSet<>(Arrays.asList(
			GenericGuardrailInputReactor.class.getName(),
			GenericGuardrailInputOutputReactor.class.getName()));
	private static final Set<String> OUTPUT_REACTOR_WHITELIST = new HashSet<>(Arrays.asList(
			GenericGuardrailInputOutputReactor.class.getName()));

	private static final Set<String> ALLOWED_PARAM_KEYS = new HashSet<>(Arrays.asList(
			GUARDRAIL_ENGINE_ID_KEY, BLOCK_ON_FAILURE_KEY, MASK_ON_FAILURE_KEY,
			MASK_TARGET_PARAM_KEY, INPUT_MAPPING_KEY, DIRECT_PARAMETERS_KEY));

	public UpdateModelGuardrailConfigReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.MAP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have edit access to it");
		}
		Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
		if (typeAndSubtype[0] != IEngine.CATALOG_TYPE.MODEL) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a model engine");
		}

		Map<String, Object> config = this.<String, Object>getGenericMap(ReactorKeysEnum.MAP.getKey(), null);
		if (config == null) {
			throw new IllegalArgumentException("Must provide the guardrail configuration map");
		}
		Map<String, Object> pipelines = validateConfigStructure(config);
		validateGuardrailEngines(user, pipelines);

		// round-trip through the same parser the runtime pipeline handler
		// uses so we never write a file it cannot read back
		String json = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create().toJson(config);
		try {
			new JSONObject(json).getJSONObject(PIPELINES_KEY);
		} catch (JSONException e) {
			throw new IllegalArgumentException("The guardrail configuration does not serialize to valid pipeline JSON: " + e.getMessage(), e);
		}

		IModelEngine model = Utility.getModel(engineId);
		if (model == null) {
			throw new IllegalArgumentException("Could not load model engine " + engineId);
		}

		ReentrantLock lock = EngineSyncUtility.getEngineLock(engineId);
		lock.lock();
		try {
			String pipelineValue = model.getSmssProp().getProperty(IEngine.PIPELINE);
			boolean addSmssKey = pipelineValue == null || pipelineValue.trim().isEmpty();
			String pipelineFileName = addSmssKey ? DEFAULT_PIPELINE_FILE : pipelineValue.trim();

			String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(model.getCatalogType(),
					model.getEngineId(), model.getEngineName());
			File pipelineFile = new File((assetsFolder + "/" + pipelineFileName).replace("\\", "/"));
			File parentDir = pipelineFile.getParentFile();
			if (parentDir != null && !parentDir.exists() && !parentDir.mkdirs()) {
				throw new IllegalStateException("Unable to create the engine assets folder for the guardrail configuration");
			}
			try (Writer writer = new OutputStreamWriter(new FileOutputStream(pipelineFile), StandardCharsets.UTF_8)) {
				writer.write(json);
			} catch (IOException e) {
				throw new IllegalStateException("Unable to write the guardrail configuration: " + e.getMessage(), e);
			}

			if (addSmssKey) {
				try {
					Utility.changePropertiesFileValue(model.getSmssFilePath(), IEngine.PIPELINE, DEFAULT_PIPELINE_FILE);
				} catch (IOException e) {
					throw new IllegalStateException("Wrote the guardrail configuration but was unable to add the PIPELINE key to the engine smss file: " + e.getMessage(), e);
				}
				// the in-memory props are what the guardrail proxy reads, so
				// the running engine picks the key up without a reload
				model.getSmssProp().setProperty(IEngine.PIPELINE, DEFAULT_PIPELINE_FILE);
				ClusterUtil.pushEngineSmss(engineId);
			}
		} finally {
			lock.unlock();
		}

		if (ClusterUtil.IS_CLUSTER) {
			ClusterUtil.pushEngine(engineId);
		}

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully updated the guardrail configuration"));
		return noun;
	}

	/**
	 * Structural validation with no database access - package visible so unit
	 * tests can exercise it directly. Returns the pipelines map on success.
	 * An empty pipelines map is valid and means all guardrails are removed.
	 *
	 * @param config
	 * @return
	 */
	static Map<String, Object> validateConfigStructure(Map<String, Object> config) {
		for (String key : config.keySet()) {
			if (!PIPELINES_KEY.equals(key)) {
				throw new IllegalArgumentException("Unknown key '" + key + "' in the guardrail configuration - only '" + PIPELINES_KEY + "' is allowed");
			}
		}
		Object pipelinesObj = config.get(PIPELINES_KEY);
		if (!(pipelinesObj instanceof Map)) {
			throw new IllegalArgumentException("The guardrail configuration must contain a '" + PIPELINES_KEY + "' map");
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> pipelines = (Map<String, Object>) pipelinesObj;
		for (Map.Entry<String, Object> pipelineEntry : pipelines.entrySet()) {
			String method = pipelineEntry.getKey();
			if (method == null || method.trim().isEmpty()) {
				throw new IllegalArgumentException("Pipeline method names cannot be blank");
			}
			String path = PIPELINES_KEY + "." + method;
			if (!(pipelineEntry.getValue() instanceof Map)) {
				throw new IllegalArgumentException(path + " must be a map with '" + INPUT_KEY + "' and/or '" + OUTPUT_KEY + "' lists");
			}
			Map<?, ?> pipeline = (Map<?, ?>) pipelineEntry.getValue();
			for (Object pipelineKey : pipeline.keySet()) {
				if (!INPUT_KEY.equals(pipelineKey) && !OUTPUT_KEY.equals(pipelineKey)) {
					throw new IllegalArgumentException(path + " contains unknown key '" + pipelineKey + "' - only '" + INPUT_KEY + "' and '" + OUTPUT_KEY + "' are allowed");
				}
			}
			int entryCount = validateSlot(path, INPUT_KEY, pipeline.get(INPUT_KEY), INPUT_REACTOR_WHITELIST)
					+ validateSlot(path, OUTPUT_KEY, pipeline.get(OUTPUT_KEY), OUTPUT_REACTOR_WHITELIST);
			if (entryCount == 0) {
				throw new IllegalArgumentException(path + " must define at least one guardrail in '" + INPUT_KEY + "' or '" + OUTPUT_KEY + "'");
			}
		}
		return pipelines;
	}

	private static int validateSlot(String path, String slotName, Object slot, Set<String> reactorWhitelist) {
		if (slot == null) {
			return 0;
		}
		if (!(slot instanceof List)) {
			throw new IllegalArgumentException(path + "." + slotName + " must be a list");
		}
		List<?> entries = (List<?>) slot;
		for (int i = 0; i < entries.size(); i++) {
			String entryPath = path + "." + slotName + "[" + i + "]";
			Object entry = entries.get(i);
			if (!(entry instanceof Map)) {
				throw new IllegalArgumentException(entryPath + " must be a map");
			}
			Map<?, ?> entryMap = (Map<?, ?>) entry;
			for (Object entryKey : entryMap.keySet()) {
				if (!REACTOR_CLASS_KEY.equals(entryKey) && !PARAMS_KEY.equals(entryKey)) {
					throw new IllegalArgumentException(entryPath + " contains unknown key '" + entryKey + "' - only '" + REACTOR_CLASS_KEY + "' and '" + PARAMS_KEY + "' are allowed");
				}
			}
			Object reactorClass = entryMap.get(REACTOR_CLASS_KEY);
			if (!(reactorClass instanceof String) || !reactorWhitelist.contains(reactorClass)) {
				throw new IllegalArgumentException(entryPath + "." + REACTOR_CLASS_KEY + " must be one of " + reactorWhitelist + " for the '" + slotName + "' slot");
			}
			Object params = entryMap.get(PARAMS_KEY);
			if (!(params instanceof Map)) {
				throw new IllegalArgumentException(entryPath + "." + PARAMS_KEY + " must be a map");
			}
			validateParams(entryPath + "." + PARAMS_KEY, (Map<?, ?>) params);
		}
		return entries.size();
	}

	private static void validateParams(String path, Map<?, ?> params) {
		for (Object paramKey : params.keySet()) {
			if (!ALLOWED_PARAM_KEYS.contains(paramKey)) {
				throw new IllegalArgumentException(path + " contains unknown key '" + paramKey + "' - allowed keys are " + ALLOWED_PARAM_KEYS);
			}
		}

		Object guardrailEngineId = params.get(GUARDRAIL_ENGINE_ID_KEY);
		if (!(guardrailEngineId instanceof String) || ((String) guardrailEngineId).trim().isEmpty()) {
			throw new IllegalArgumentException(path + "." + GUARDRAIL_ENGINE_ID_KEY + " must be a non-empty string");
		}

		Object blockOnFailure = params.get(BLOCK_ON_FAILURE_KEY);
		if (blockOnFailure != null && !(blockOnFailure instanceof Boolean)) {
			throw new IllegalArgumentException(path + "." + BLOCK_ON_FAILURE_KEY + " must be a boolean");
		}
		Object maskOnFailure = params.get(MASK_ON_FAILURE_KEY);
		if (maskOnFailure != null && !(maskOnFailure instanceof Boolean)) {
			throw new IllegalArgumentException(path + "." + MASK_ON_FAILURE_KEY + " must be a boolean");
		}

		Object maskTargetParam = params.get(MASK_TARGET_PARAM_KEY);
		if (maskTargetParam != null && (!(maskTargetParam instanceof String) || ((String) maskTargetParam).trim().isEmpty())) {
			throw new IllegalArgumentException(path + "." + MASK_TARGET_PARAM_KEY + " must be a non-empty string");
		}

		// without a mapping the interceptor calls the guardrail engine with no
		// parameters at all, which fails inside the engine at request time
		Object inputMapping = params.get(INPUT_MAPPING_KEY);
		if (!(inputMapping instanceof Map) || ((Map<?, ?>) inputMapping).isEmpty()) {
			throw new IllegalArgumentException(path + "." + INPUT_MAPPING_KEY
					+ " must map at least one guardrail parameter to a method argument (e.g. prompt to arg0) - the guardrail receives no content to check without it");
		}
		for (Map.Entry<?, ?> mappingEntry : ((Map<?, ?>) inputMapping).entrySet()) {
			Object mappingValue = mappingEntry.getValue();
			String mappingPath = path + "." + INPUT_MAPPING_KEY + "." + mappingEntry.getKey();
			if (mappingValue instanceof String) {
				if (((String) mappingValue).trim().isEmpty()) {
					throw new IllegalArgumentException(mappingPath + " cannot be an empty string");
				}
			} else if (mappingValue instanceof List) {
				List<?> mappingList = (List<?>) mappingValue;
				if (mappingList.isEmpty()) {
					throw new IllegalArgumentException(mappingPath + " cannot be an empty list");
				}
				for (Object mappingItem : mappingList) {
					if (!(mappingItem instanceof String) || ((String) mappingItem).trim().isEmpty()) {
						throw new IllegalArgumentException(mappingPath + " must only contain non-empty strings");
					}
				}
			} else {
				throw new IllegalArgumentException(mappingPath + " must be a string or a list of strings");
			}
		}

		Object directParameters = params.get(DIRECT_PARAMETERS_KEY);
		if (directParameters != null && !(directParameters instanceof Map)) {
			throw new IllegalArgumentException(path + "." + DIRECT_PARAMETERS_KEY + " must be a map");
		}

		// the runtime silently downgrades mask to block when the mask target
		// maps to multiple arguments - reject the combination up front
		if (Boolean.TRUE.equals(maskOnFailure)) {
			String maskTarget = maskTargetParam == null ? DEFAULT_MASK_TARGET_PARAM : ((String) maskTargetParam).trim();
			Object maskMapping = ((Map<?, ?>) inputMapping).get(maskTarget);
			if (!(maskMapping instanceof String)) {
				throw new IllegalArgumentException(path + ": " + MASK_ON_FAILURE_KEY + " requires " + INPUT_MAPPING_KEY
						+ " to map '" + maskTarget + "' to a single argument name - otherwise the runtime falls back to blocking");
			}
		}
	}

	/**
	 * Database-backed validation that every referenced guardrail engine
	 * exists, is a guardrail engine, and is visible to the user.
	 *
	 * @param user
	 * @param pipelines
	 */
	private static void validateGuardrailEngines(User user, Map<String, Object> pipelines) {
		for (Map.Entry<String, Object> pipelineEntry : pipelines.entrySet()) {
			Map<?, ?> pipeline = (Map<?, ?>) pipelineEntry.getValue();
			for (String slotName : new String[] { INPUT_KEY, OUTPUT_KEY }) {
				Object slot = pipeline.get(slotName);
				if (!(slot instanceof List)) {
					continue;
				}
				for (Object entry : (List<?>) slot) {
					Map<?, ?> params = (Map<?, ?>) ((Map<?, ?>) entry).get(PARAMS_KEY);
					String guardrailEngineId = ((String) params.get(GUARDRAIL_ENGINE_ID_KEY)).trim();
					Object[] typeAndSubtype;
					try {
						typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(guardrailEngineId);
					} catch (IllegalArgumentException e) {
						throw new IllegalArgumentException("Guardrail engine " + guardrailEngineId + " does not exist");
					}
					if (typeAndSubtype[0] != IEngine.CATALOG_TYPE.GUARDRAIL) {
						throw new IllegalArgumentException("Engine " + guardrailEngineId + " is not a guardrail engine");
					}
					if (!SecurityEngineUtils.userCanViewEngine(user, guardrailEngineId)) {
						throw new IllegalArgumentException("User does not have access to guardrail engine " + guardrailEngineId);
					}
				}
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return "Validates and saves the guardrail pipeline configuration (pipeline.json) for a model engine, applying it to the running engine immediately. An empty pipelines map removes all guardrails. Requires edit access to the engine.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The id of the model engine";
		}
		if (key.equals(ReactorKeysEnum.MAP.getKey())) {
			return "The guardrail configuration as a map matching the pipeline.json schema";
		}
		return super.getDescriptionForKey(key);
	}
}
