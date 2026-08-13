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
package prerna.engine.impl.guardrail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.function.FunctionParameter;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Guardrail that checks whether a prompt is on-topic by performing a similarity
 * search against a user-configured vector database. The vector DB should be
 * pre-loaded with example on-topic prompts/documents. Whether the returned
 * Score is a distance or a similarity
 * depends on the configured vector engine, so it is declared via
 * SCORE_IS_DISTANCE rather than assumed.
 *
 */
public class OnTopicGuardrailEngine extends AbstractGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(OnTopicGuardrailEngine.class);

	public static final String VECTOR_ENGINE_ID_KEY = "VECTOR_ENGINE_ID";
	public static final String DEFAULT_THRESHOLD_KEY = "DEFAULT_THRESHOLD";
	public static final String LIMIT_KEY = "LIMIT";
	public static final String SCORE_IS_DISTANCE_KEY = "SCORE_IS_DISTANCE";

	/** Key name used by all vector DB backends for the score. */
	private static final String SCORE_KEY = "Score";

	private String vectorEngineId = null;
	// Provisional - re-derive per deployment against real on-topic query scores.
	private double defaultThreshold = 1.6;
	private int defaultLimit = 5;
	private boolean scoreIsDistance;

	public OnTopicGuardrailEngine() {
		this.keysToGet = new String[] { "prompt", "threshold", "limit" };
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.vectorEngineId = this.smssProp.getProperty(VECTOR_ENGINE_ID_KEY);
		if (this.vectorEngineId == null || this.vectorEngineId.trim().isEmpty()) {
			throw new IllegalArgumentException(VECTOR_ENGINE_ID_KEY + " is required for OnTopicGuardrailEngine");
		}
		this.vectorEngineId = this.vectorEngineId.trim();

		String thresholdStr = this.smssProp.getProperty(DEFAULT_THRESHOLD_KEY);
		if (thresholdStr != null && !(thresholdStr = thresholdStr.trim()).isEmpty()) {
			try {
				this.defaultThreshold = Double.parseDouble(thresholdStr);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid {} value '{}'. Using default {}", DEFAULT_THRESHOLD_KEY, thresholdStr,
						this.defaultThreshold);
			}
		}

		String limitStr = this.smssProp.getProperty(LIMIT_KEY);
		if (limitStr != null && !(limitStr = limitStr.trim()).isEmpty()) {
			try {
				this.defaultLimit = Integer.parseInt(limitStr);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid {} value '{}'. Using default {}", LIMIT_KEY, limitStr, this.defaultLimit);
			}
		}

		String scoreIsDistanceStr = this.smssProp.getProperty(SCORE_IS_DISTANCE_KEY);
		if (scoreIsDistanceStr == null || (scoreIsDistanceStr = scoreIsDistanceStr.trim()).isEmpty()) {
			throw new IllegalArgumentException(SCORE_IS_DISTANCE_KEY + " is required for OnTopicGuardrailEngine - "
					+ "declare whether the configured vector engine's Score is a distance (lower = more similar) "
					+ "or a similarity (higher = more similar); this varies by backend and cannot be assumed");
		}
		if ("true".equalsIgnoreCase(scoreIsDistanceStr)) {
			this.scoreIsDistance = true;
		} else if ("false".equalsIgnoreCase(scoreIsDistanceStr)) {
			this.scoreIsDistance = false;
		} else {
			throw new IllegalArgumentException(
					SCORE_IS_DISTANCE_KEY + " must be 'true' or 'false', got '" + scoreIsDistanceStr + "'");
		}

		this.functionDescription = "Checks whether the prompt is on-topic by comparing it against a vector database "
				+ "of known on-topic examples, using either a distance or similarity score depending on "
				+ "SCORE_IS_DISTANCE.";
		this.parameters = new ArrayList<>();
		this.parameters.add(new FunctionParameter("prompt", "String", "The prompt to evaluate"));
		this.parameters.add(new FunctionParameter("threshold", "Double",
				"Compared against the best match's Score - pass if Score <= threshold when SCORE_IS_DISTANCE is "
						+ "true, or Score >= threshold when false. Default is " + this.defaultThreshold));
		this.parameters.add(new FunctionParameter("limit", "Integer",
				"Number of nearest neighbours to retrieve from the vector database. Default is " + this.defaultLimit));
		this.requiredParameters = new ArrayList<>(Arrays.asList("prompt"));
	}

	@Override
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow) {
		Map<String, String> keyValue = organizeKeys(ns, curRow);

		String prompt = keyValue.get("prompt");
		double threshold = this.defaultThreshold;
		if (keyValue.containsKey("threshold")) {
			try {
				threshold = Double.parseDouble(keyValue.get("threshold"));
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid threshold value '{}'. Using default {}", keyValue.get("threshold"),
						threshold);
			}
		}

		int limit = this.defaultLimit;
		if (keyValue.containsKey("limit")) {
			try {
				limit = Integer.parseInt(keyValue.get("limit"));
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid limit value '{}'. Using default {}", keyValue.get("limit"), limit);
			}
		}

		classLogger.info("OnTopicGuardrail: evaluating prompt (length={}) against vectorDb={}, threshold={}, limit={}",
				prompt.length(), this.vectorEngineId, threshold, limit);

		IVectorDatabaseEngine vectorDb = Utility.getVectorDatabase(this.vectorEngineId);
		if (vectorDb == null) {
			classLogger.error("OnTopicGuardrail: vector database engine not found: {}", this.vectorEngineId);
			throw new IllegalStateException(
					"Could not find vector database engine with id: " + this.vectorEngineId);
		}

		// Vector DB nearestNeighbor requires an Insight for its embedding model calls.
		Insight realInsight = getRealInsight(ns);
		boolean usingEphemeralInsight = (realInsight == null);
		Insight queryInsight = usingEphemeralInsight ? new Insight() : realInsight;
		if (usingEphemeralInsight) {
			InsightStore.getInstance().put(queryInsight);
		}

		List<Map<String, Object>> results;
		try {
			Object raw = vectorDb.nearestNeighbor(queryInsight, prompt, limit, new HashMap<>());
			if (raw instanceof List) {
				@SuppressWarnings("unchecked")
				List<Map<String, Object>> castResults = (List<Map<String, Object>>) raw;
				results = castResults;
			} else {
				results = new ArrayList<>();
			}
		} finally {
			if (usingEphemeralInsight) {
				InsightStore.getInstance().remove(queryInsight.getInsightId());
			}
		}

		// Results are returned sorted by the vector DB — first result is the best match.
		double bestScore = Double.MAX_VALUE;
		if (!results.isEmpty()) {
			Object scoreObj = results.get(0).get(SCORE_KEY);
			if (scoreObj instanceof Number) {
				bestScore = ((Number) scoreObj).doubleValue();
			}
			classLogger.info("OnTopicGuardrail: top match — score={}, content={}",
					bestScore,
					results.get(0).getOrDefault("Content", results.get(0).getOrDefault("content", "<unknown>")));
		} else {
			classLogger.info("OnTopicGuardrail: no results returned from vector DB");
		}

		boolean pass = !results.isEmpty()
				&& (scoreIsDistance ? bestScore <= threshold : bestScore >= threshold);

		Map<String, Object> details = new HashMap<>();
		details.put("threshold", threshold);
		details.put("bestScore", bestScore);
		details.put("limit", limit);
		details.put("results", results);

		classLogger.info("OnTopicGuardrail: bestScore={}, threshold={}, pass={}, resultCount={}",
				bestScore, threshold, pass, results.size());

		return new GuardrailNounMetadata(pass, prompt, details);
	}

	private Insight getRealInsight(NounStore ns) {
		if (ns != null) {
			GenRowStruct grs = ns.getGenRowStruct(Constants.INSIGHT);
			if (grs != null && !grs.isEmpty() && grs.get(0) instanceof Insight) {
				return (Insight) grs.get(0);
			}
		}
		String insightId = ThreadStore.getInsightId();
		if (insightId != null) {
			Insight threadInsight = InsightStore.getInstance().get(insightId);
			if (threadInsight != null) {
				return threadInsight;
			}
		}
		return null;
	}

	@Override
	public GuardrailTypeEnum getGuardrailType() {
		return GuardrailTypeEnum.EMBEDDED_ON_TOPIC;
	}
}
