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
package prerna.rag.generation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;

/**
 * Validates that a RAG-generated answer is grounded in the retrieved source chunks.
 * Uses an LLM to evaluate whether each claim in the answer can be attributed to the sources.
 * Accepts source chunks as List of Maps (same format returned by vector engines).
 */
public class HallucinationDetector {

	private static final Logger classLogger = LogManager.getLogger(HallucinationDetector.class);

	private static final String VERIFICATION_PROMPT = """
			You are a fact-checking assistant. Your job is to determine whether the given ANSWER \
			is fully supported by the provided SOURCE DOCUMENTS.

			Evaluate the answer against the sources and respond with ONLY a JSON object:
			{
			  "grounded": true/false,
			  "confidence": 0.0-1.0,
			  "unsupported_claims": ["claim1", "claim2"]
			}

			Rules:
			- "grounded" is true if ALL factual claims in the answer are supported by the sources
			- "confidence" is your confidence that the answer is grounded (1.0 = fully grounded)
			- "unsupported_claims" lists any claims NOT found in the sources (empty if grounded)
			- If the answer says "I don't know" or similar, it is grounded with confidence 1.0
			- Do NOT evaluate style or completeness, only factual accuracy against sources

			SOURCES:
			%s

			ANSWER:
			%s

			Respond with ONLY the JSON object, nothing else.
			""";

	private final IModelEngine modelEngine;

	public HallucinationDetector(IModelEngine modelEngine) {
		this.modelEngine = modelEngine;
	}

	/**
	 * Verify an answer against retrieved source chunks.
	 * 
	 * @param answer  the generated answer to verify
	 * @param sources the source chunks as Maps with "Content" and "Source" keys
	 * @param insight the current insight context
	 * @return verification result with confidence and hallucination flag
	 */
	public VerificationResult verify(String answer, List<Map<String, Object>> sources, Insight insight) {
		if (answer == null || answer.trim().isEmpty()) {
			return new VerificationResult(true, 1.0, List.of());
		}
		if (sources == null || sources.isEmpty()) {
			return new VerificationResult(false, 0.0, List.of("No sources available for verification"));
		}

		String sourcesText = buildSourcesText(sources);
		String prompt = String.format(VERIFICATION_PROMPT, sourcesText, answer);

		try {
			Map<String, Object> params = new HashMap<>();
			params.put("temperature", 0.0);
			params.put("max_new_tokens", 500);

			AskModelEngineResponse response = modelEngine.ask(prompt, null, insight, params);
			String responseText = String.valueOf(response.getResponse());

			return parseVerificationResponse(responseText);
		} catch (Exception e) {
			classLogger.warn("Hallucination detection failed, defaulting to unverified", e);
			return new VerificationResult(false, 0.0, List.of("Verification failed: " + e.getMessage()));
		}
	}

	private String buildSourcesText(List<Map<String, Object>> sources) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < sources.size(); i++) {
			Map<String, Object> chunk = sources.get(i);
			sb.append("[Source ").append(i + 1).append("] ");
			Object source = chunk.get("Source");
			if (source != null) {
				sb.append("(").append(source).append(") ");
			}
			Object content = chunk.get("Content");
			if (content != null) {
				sb.append(content);
			}
			sb.append("\n\n");
		}
		return sb.toString();
	}

	private VerificationResult parseVerificationResponse(String responseText) {
		try {
			// extract JSON from response (handle cases where LLM adds extra text)
			String json = responseText.trim();
			int startIdx = json.indexOf('{');
			int endIdx = json.lastIndexOf('}');
			if (startIdx >= 0 && endIdx > startIdx) {
				json = json.substring(startIdx, endIdx + 1);
			}

			com.google.gson.JsonObject parsed = com.google.gson.JsonParser.parseString(json).getAsJsonObject();

			boolean grounded = parsed.has("grounded") && parsed.get("grounded").getAsBoolean();
			double confidence = parsed.has("confidence") ? parsed.get("confidence").getAsDouble() : 0.0;

			List<String> unsupportedClaims = new java.util.ArrayList<>();
			if (parsed.has("unsupported_claims") && parsed.get("unsupported_claims").isJsonArray()) {
				for (com.google.gson.JsonElement elem : parsed.getAsJsonArray("unsupported_claims")) {
					unsupportedClaims.add(elem.getAsString());
				}
			}

			return new VerificationResult(grounded, confidence, unsupportedClaims);
		} catch (Exception e) {
			classLogger.warn("Failed to parse hallucination detection response: " + responseText, e);
			return new VerificationResult(false, 0.0, List.of("Failed to parse verification response"));
		}
	}

	/**
	 * Result of hallucination verification.
	 */
	public static class VerificationResult {

		private final boolean grounded;
		private final double confidence;
		private final List<String> unsupportedClaims;

		public VerificationResult(boolean grounded, double confidence, List<String> unsupportedClaims) {
			this.grounded = grounded;
			this.confidence = confidence;
			this.unsupportedClaims = unsupportedClaims;
		}

		public boolean isGrounded() {
			return grounded;
		}

		public double getConfidence() {
			return confidence;
		}

		public List<String> getUnsupportedClaims() {
			return unsupportedClaims;
		}

		public Map<String, Object> toMap() {
			Map<String, Object> map = new HashMap<>();
			map.put("grounded", grounded);
			map.put("confidence", confidence);
			map.put("unsupportedClaims", unsupportedClaims);
			return map;
		}

	}

}
