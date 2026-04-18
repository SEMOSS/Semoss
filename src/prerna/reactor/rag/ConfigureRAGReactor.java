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
package prerna.reactor.rag;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.rag.RAGConfig;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Configure RAG pipeline settings for a vector database engine.
 * Settings are persisted to the engine's SMSS properties.
 * 
 * Pixel syntax:
 * ConfigureRAG(engine=["vecDbId"], hybridSearch=[true], rerankEngine=["funcId"],
 *              model=["llmId"], systemPrompt=["..."], confidenceThreshold=[0.5],
 *              hallucinationCheck=[false])
 */
public class ConfigureRAGReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ConfigureRAGReactor.class);

	private static final String HYBRID_SEARCH_KEY = "hybridSearch";
	private static final String RERANK_ENGINE_KEY = "rerankEngine";
	private static final String MODEL_KEY = "model";
	private static final String SYSTEM_PROMPT_KEY = "systemPrompt";
	private static final String CONFIDENCE_THRESHOLD_KEY = "confidenceThreshold";
	private static final String HALLUCINATION_CHECK_KEY = "hallucinationCheck";

	public ConfigureRAGReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				HYBRID_SEARCH_KEY,
				RERANK_ENGINE_KEY,
				MODEL_KEY,
				SYSTEM_PROMPT_KEY,
				CONFIDENCE_THRESHOLD_KEY,
				HALLUCINATION_CHECK_KEY
		};
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have edit access.");
		}

		IVectorDatabaseEngine vectorEngine = Utility.getVectorDatabase(engineId);
		if (vectorEngine == null) {
			throw new IllegalArgumentException("Unable to find vector database engine: " + engineId);
		}

		Properties smssProp = vectorEngine.getSmssProp();

		// apply each setting if provided
		String hybridSearch = this.keyValue.get(HYBRID_SEARCH_KEY);
		if (hybridSearch != null) {
			smssProp.setProperty(RAGConfig.SMSS_HYBRID_SEARCH_ENABLED, hybridSearch);
		}

		String rerankEngine = this.keyValue.get(RERANK_ENGINE_KEY);
		if (rerankEngine != null) {
			smssProp.setProperty(RAGConfig.SMSS_RERANK_FUNCTION_ID, rerankEngine);
		}

		String model = this.keyValue.get(MODEL_KEY);
		if (model != null) {
			smssProp.setProperty(RAGConfig.SMSS_RAG_MODEL_ID, model);
		}

		String systemPrompt = this.keyValue.get(SYSTEM_PROMPT_KEY);
		if (systemPrompt != null) {
			smssProp.setProperty(RAGConfig.SMSS_RAG_SYSTEM_PROMPT, Utility.decodeURIComponent(systemPrompt));
		}

		String confidenceThreshold = this.keyValue.get(CONFIDENCE_THRESHOLD_KEY);
		if (confidenceThreshold != null) {
			smssProp.setProperty(RAGConfig.SMSS_RAG_CONFIDENCE_THRESHOLD, confidenceThreshold);
		}

		String hallucinationCheck = this.keyValue.get(HALLUCINATION_CHECK_KEY);
		if (hallucinationCheck != null) {
			smssProp.setProperty(RAGConfig.SMSS_RAG_HALLUCINATION_CHECK, hallucinationCheck);
		}

		// return current config
		RAGConfig config = RAGConfig.fromSmssProperties(smssProp);
		Map<String, Object> result = new HashMap<>();
		result.put("engineId", engineId);
		result.put("hybridSearchEnabled", config.isHybridSearchEnabled());
		result.put("supportsNativeHybrid", vectorEngine.supportsHybridSearch());
		result.put("rerankFunctionEngineId", config.getRerankFunctionEngineId());
		result.put("ragModelEngineId", config.getRagModelEngineId());
		result.put("systemPrompt", config.getSystemPrompt());
		result.put("confidenceThreshold", config.getConfidenceThreshold());
		result.put("hallucinationCheckEnabled", config.isHallucinationCheckEnabled());

		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return """
				Configure RAG pipeline settings for a vector database engine. \
				Settings include hybrid search toggle, reranking model, LLM for generation, \
				system prompt, confidence threshold, and hallucination detection.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The vector database engine ID to configure.";
		} else if (key.equals(HYBRID_SEARCH_KEY)) {
			return "Enable/disable hybrid search (true/false).";
		} else if (key.equals(RERANK_ENGINE_KEY)) {
			return "The function engine ID for reranking results.";
		} else if (key.equals(MODEL_KEY)) {
			return "The LLM model engine ID for RAG answer generation.";
		} else if (key.equals(SYSTEM_PROMPT_KEY)) {
			return "Custom system prompt for the RAG pipeline.";
		} else if (key.equals(CONFIDENCE_THRESHOLD_KEY)) {
			return "Minimum confidence threshold (0.0-1.0) for answers.";
		} else if (key.equals(HALLUCINATION_CHECK_KEY)) {
			return "Enable/disable post-generation hallucination detection (true/false).";
		}
		return super.getDescriptionForKey(key);
	}

}
