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
package prerna.reactor.model.serving;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.impl.model.serving.ServedModelGuard;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.engine.impl.model.serving.ServedModelStatus;
import prerna.engine.impl.model.serving.ServingCatalog;
import prerna.om.Insight;
import prerna.reactor.model.upload.CreateModelEngineReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * SEMOSS model engines for served models, created exactly as an admin would in
 * the UI's "Self Hosted" (OpenAI-compatible) form, and through the same
 * CreateModelEngine reactor: its permission checks, model metadata, secret
 * store, cluster push and audit all apply. SEMOSS only creates such engines; it
 * never rewrites an existing engine's properties, which live in the secret store
 * when one is configured.
 */
final class ServedModelEngines {

	/** The UI's Self Hosted init script, verbatim (model-import.constants.ts) */
	static final String INIT_SCRIPT = "import genai_client;${VAR_NAME} = genai_client.OpenAiClient("
			+ "endpoint = '${ENDPOINT}', model_name = '${MODEL}', api_key = '${OPEN_AI_KEY}')";
	/** vLLM runs without --api-key; the OpenAI client needs some value */
	static final String API_KEY = "kserve";
	static final String KSERVE_NAMESPACE = ServedModelGuard.NAMESPACE_PROPERTY;
	static final String KSERVE_SERVED_MODEL = ServedModelGuard.SERVED_MODEL_PROPERTY;

	/** What CreateModelEngine is asked for */
	record Request(String engineName, Map<String, Object> details) {
	}

	private ServedModelEngines() {
	}

	/**
	 * The model details of the Self Hosted form. TEXT_GENERATION, as the form sends,
	 * keeps the engine off OpenAI's pricing and built-in tools.
	 */
	static Map<String, Object> modelDetails(String servedModel, String namespace, String endpoint,
			Integer contextWindow, Integer maxTokens) {
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("MODEL_TYPE", "TEXT_GENERATION");
		details.put("MODEL_BRAND", "OPEN_AI");
		details.put("MODEL", servedModel);
		details.put("VAR_NAME", "myModel");
		details.put("ENDPOINT", endpoint);
		details.put("OPEN_AI_KEY", API_KEY);
		if (maxTokens != null) {
			details.put("MAX_TOKENS", maxTokens);
		}
		if (contextWindow != null) {
			details.put("CONTEXT_WINDOW", contextWindow);
		}
		details.put("KEEP_INPUT_OUTPUT", "true");
		details.put("KEEP_CONVERSATION_HISTORY", "true");
		details.put("INIT_MODEL_ENGINE", INIT_SCRIPT);
		// which served model the engine talks to, for ServedModelGuard to explain a failed ask
		details.put(KSERVE_NAMESPACE, namespace);
		details.put(KSERVE_SERVED_MODEL, servedModel);
		return details;
	}

	/**
	 * Create the engine of a served model; see {@link #request}.
	 *
	 * @return the served model, its endpoint (and a note when that endpoint may not
	 *         last, such as KServe's predictor address) and CreateModelEngine's
	 *         engine data
	 */
	static Map<String, Object> create(Insight insight, ServedModelService service, ServedModelStatus status,
			String engineName, boolean global) {
		Request request = request(service, status, engineName);
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("servedModel", status.getServedModel());
		result.put("endpoint", status.getEndpoint());
		if (status.getEndpointNote() != null) {
			result.put("endpointNote", status.getEndpointNote());
		}
		result.put("engine", createEngine(insight, request.engineName(), request.details(), global));
		return result;
	}

	/**
	 * The engine a served model gets, at the endpoint this SEMOSS reaches it by.
	 * The context window and completion length come from the model's catalog
	 * entry, else the context window from vLLM itself when the model has run.
	 *
	 * @param engineName null for the model's display name, or its name when SEMOSS
	 *                   does not accept the display name as an engine name
	 */
	static Request request(ServedModelService service, ServedModelStatus status, String engineName) {
		String servedModel = status.getServedModel();
		if (status.getEndpoint() == null) {
			String note = status.getEndpointNote();
			throw new IllegalArgumentException("SEMOSS has no endpoint for " + servedModel
					+ ", so it cannot create its engine" + (note == null ? "" : ": " + note));
		}
		String name;
		if (engineName != null && !engineName.isBlank()) {
			name = engineName.strip();
		} else if (status.getDisplayName() != null && Utility.validateName(status.getDisplayName())) {
			name = status.getDisplayName();
		} else {
			// an InferenceService name always is a valid engine name
			name = servedModel;
		}
		if (!Utility.validateName(name)) {
			throw new IllegalArgumentException(
					"Invalid engine name " + name + ": start with a letter, then letters, digits, spaces, '_' or '-'");
		}
		if (AbstractSecurityUtils.containsEngineName(name)) {
			throw new IllegalArgumentException("An engine named " + name + " already exists; give another engineName");
		}
		Integer contextWindow = status.getMaxModelLen();
		Integer maxTokens = null;
		if (status.getCatalogEntry() != null) {
			for (ServingCatalog.Entry entry : service.catalog().getEntries()) {
				if (entry.id().equals(status.getCatalogEntry())) {
					contextWindow = entry.contextWindow() != null ? entry.contextWindow() : contextWindow;
					maxTokens = entry.maxTokens();
				}
			}
		}
		return new Request(name,
				modelDetails(servedModel, service.getNamespace(), status.getEndpoint(), contextWindow, maxTokens));
	}

	/** Run CreateModelEngine with these inputs, as the user of the insight. */
	@SuppressWarnings("unchecked")
	static Map<String, Object> createEngine(Insight insight, String engineName, Map<String, Object> details,
			boolean global) {
		CreateModelEngineReactor reactor = new CreateModelEngineReactor();
		reactor.In();
		reactor.setInsight(insight);
		input(reactor, ReactorKeysEnum.MODEL.getKey(), new NounMetadata(engineName, PixelDataType.CONST_STRING));
		input(reactor, ReactorKeysEnum.MODEL_DETAILS.getKey(), new NounMetadata(details, PixelDataType.MAP));
		input(reactor, ReactorKeysEnum.GLOBAL.getKey(), new NounMetadata(global, PixelDataType.BOOLEAN));
		return (Map<String, Object>) reactor.execute().getValue();
	}

	private static void input(CreateModelEngineReactor reactor, String key, NounMetadata value) {
		GenRowStruct row = new GenRowStruct();
		row.add(value);
		reactor.getNounStore().addNoun(key, row);
	}
}
