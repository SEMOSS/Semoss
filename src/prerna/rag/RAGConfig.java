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
package prerna.rag;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for a RAG pipeline attached to a vector database engine.
 * Stored as properties in the engine's SMSS file.
 */
public class RAGConfig {

	public static final String SMSS_HYBRID_SEARCH_ENABLED = "HYBRID_SEARCH_ENABLED";
	public static final String SMSS_RERANK_FUNCTION_ID = "RERANK_FUNCTION_ENGINE_ID";
	public static final String SMSS_RAG_MODEL_ID = "RAG_MODEL_ENGINE_ID";
	public static final String SMSS_RAG_SYSTEM_PROMPT = "RAG_SYSTEM_PROMPT";
	public static final String SMSS_RAG_CONFIDENCE_THRESHOLD = "RAG_CONFIDENCE_THRESHOLD";
	public static final String SMSS_RAG_HALLUCINATION_CHECK = "RAG_HALLUCINATION_CHECK";

	public static final String DEFAULT_SYSTEM_PROMPT = "You are a helpful assistant. "
			+ "Answer the user's question based ONLY on the provided context. "
			+ "If the context does not contain enough information to answer, say so clearly. "
			+ "Cite the source document for each claim you make.";

	private boolean hybridSearchEnabled = true;
	private String rerankFunctionEngineId;
	private String ragModelEngineId;
	private String systemPrompt = DEFAULT_SYSTEM_PROMPT;
	private double confidenceThreshold = 0.5;
	private boolean hallucinationCheckEnabled = false;

	public RAGConfig() {
	}

	public boolean isHybridSearchEnabled() {
		return hybridSearchEnabled;
	}

	public void setHybridSearchEnabled(boolean hybridSearchEnabled) {
		this.hybridSearchEnabled = hybridSearchEnabled;
	}

	public String getRerankFunctionEngineId() {
		return rerankFunctionEngineId;
	}

	public void setRerankFunctionEngineId(String rerankFunctionEngineId) {
		this.rerankFunctionEngineId = rerankFunctionEngineId;
	}

	public String getRagModelEngineId() {
		return ragModelEngineId;
	}

	public void setRagModelEngineId(String ragModelEngineId) {
		this.ragModelEngineId = ragModelEngineId;
	}

	public String getSystemPrompt() {
		return systemPrompt;
	}

	public void setSystemPrompt(String systemPrompt) {
		this.systemPrompt = systemPrompt;
	}

	public double getConfidenceThreshold() {
		return confidenceThreshold;
	}

	public void setConfidenceThreshold(double confidenceThreshold) {
		this.confidenceThreshold = confidenceThreshold;
	}

	public boolean isHallucinationCheckEnabled() {
		return hallucinationCheckEnabled;
	}

	public void setHallucinationCheckEnabled(boolean hallucinationCheckEnabled) {
		this.hallucinationCheckEnabled = hallucinationCheckEnabled;
	}

	/**
	 * Load config from SMSS properties.
	 */
	public static RAGConfig fromSmssProperties(java.util.Properties smssProp) {
		RAGConfig config = new RAGConfig();

		String hybridEnabled = smssProp.getProperty(SMSS_HYBRID_SEARCH_ENABLED);
		if (hybridEnabled != null) {
			config.setHybridSearchEnabled(Boolean.parseBoolean(hybridEnabled));
		}

		String rerankId = smssProp.getProperty(SMSS_RERANK_FUNCTION_ID);
		if (rerankId != null && !rerankId.trim().isEmpty()) {
			config.setRerankFunctionEngineId(rerankId.trim());
		}

		String modelId = smssProp.getProperty(SMSS_RAG_MODEL_ID);
		if (modelId != null && !modelId.trim().isEmpty()) {
			config.setRagModelEngineId(modelId.trim());
		}

		String prompt = smssProp.getProperty(SMSS_RAG_SYSTEM_PROMPT);
		if (prompt != null && !prompt.trim().isEmpty()) {
			config.setSystemPrompt(prompt.trim());
		}

		String threshold = smssProp.getProperty(SMSS_RAG_CONFIDENCE_THRESHOLD);
		if (threshold != null) {
			try {
				config.setConfidenceThreshold(Double.parseDouble(threshold));
			} catch (NumberFormatException e) {
				// use default
			}
		}

		String hallucinationCheck = smssProp.getProperty(SMSS_RAG_HALLUCINATION_CHECK);
		if (hallucinationCheck != null) {
			config.setHallucinationCheckEnabled(Boolean.parseBoolean(hallucinationCheck));
		}

		return config;
	}

	/**
	 * Convert to a map for SMSS property storage.
	 */
	public Map<String, String> toSmssProperties() {
		Map<String, String> props = new HashMap<>();
		props.put(SMSS_HYBRID_SEARCH_ENABLED, String.valueOf(this.hybridSearchEnabled));
		if (this.rerankFunctionEngineId != null) {
			props.put(SMSS_RERANK_FUNCTION_ID, this.rerankFunctionEngineId);
		}
		if (this.ragModelEngineId != null) {
			props.put(SMSS_RAG_MODEL_ID, this.ragModelEngineId);
		}
		if (this.systemPrompt != null) {
			props.put(SMSS_RAG_SYSTEM_PROMPT, this.systemPrompt);
		}
		props.put(SMSS_RAG_CONFIDENCE_THRESHOLD, String.valueOf(this.confidenceThreshold));
		props.put(SMSS_RAG_HALLUCINATION_CHECK, String.valueOf(this.hallucinationCheckEnabled));
		return props;
	}

}
