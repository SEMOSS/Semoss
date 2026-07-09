/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *******************************************************************************/
package prerna.reactor.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.graphrag.JenaGraphRAGEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class JenaHybridRetrieveReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JenaHybridRetrieveReactor.class);

	private static final String QUESTION_KEY = "question";
	private static final String HOPS_KEY = "hops";
	private static final String SECTION_FILTER_KEY = "sectionFilter";
	private static final String MIN_SCORE_KEY = "minScore";

	public JenaHybridRetrieveReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				QUESTION_KEY,
				ReactorKeysEnum.LIMIT.getKey(),
				HOPS_KEY,
				SECTION_FILTER_KEY,
				MIN_SCORE_KEY
		};
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		String engineId = getString(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have access.");
		}
		IVectorDatabaseEngine wrapped = Utility.getVectorDatabase(engineId);
		if (wrapped == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		Object raw = DIHelper.getInstance().getEngineProperty(engineId);
		if (!(raw instanceof JenaGraphRAGEngine)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a Jena GraphRAG database (actual: "
					+ (raw == null ? "null" : raw.getClass().getName()) + ")");
		}
		JenaGraphRAGEngine eng = (JenaGraphRAGEngine) raw;

		String question = getString(QUESTION_KEY);
		if (question == null || question.trim().isEmpty()) {
			throw new IllegalArgumentException("question is required");
		}
		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), 6);

		Map<String, Object> paramMap = new HashMap<>();
		String hopsStr = getString(HOPS_KEY);
		if (hopsStr != null && !hopsStr.trim().isEmpty()) {
			try {
				paramMap.put("hops", Integer.parseInt(hopsStr));
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid hops '{}'; ignoring", hopsStr);
			}
		}
		String sectionFilter = getString(SECTION_FILTER_KEY);
		if (sectionFilter != null && !sectionFilter.trim().isEmpty()) {
			paramMap.put("sectionFilter", sectionFilter);
		}
		String minScore = getString(MIN_SCORE_KEY);
		if (minScore != null && !minScore.trim().isEmpty()) {
			try {
				paramMap.put("minScore", Double.parseDouble(minScore));
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid minScore '{}'; ignoring", minScore);
			}
		}

		Map<String, Object> result = eng.hybridRetrieve(this.insight, question, limit, paramMap);

		// Merge passages from the paired vector engine (its own PyTranslator
		// scope). Jena's Python module can't see the paired searcher variable
		// directly, so we invoke the paired engine's Python from Java here.
		if (eng.isPairedVectorMode() && result != null) {
			List<Map<String, Object>> hits = eng.callPairedSearch(this.insight, question, limit);
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> passages = (List<Map<String, Object>>) result
					.getOrDefault("passages", new ArrayList<Map<String, Object>>());
			for (Map<String, Object> hit : hits) {
				Map<String, Object> p = new HashMap<>();
				Object text = firstNonNull(hit.get("Content"), hit.get("text"));
				p.put("text", text != null ? String.valueOf(text) : "");
				Object source = firstNonNull(hit.get("Source"), hit.get("source"));
				p.put("source", source);
				Object score = firstNonNull(hit.get("Score"), hit.get("score"));
				try {
					p.put("score", score == null ? 0.0 : Double.parseDouble(String.valueOf(score)));
				} catch (NumberFormatException e) {
					p.put("score", 0.0);
				}
				Object uri = firstNonNull(hit.get("uri"), hit.get("id"));
				if (uri != null) {
					p.put("uri", uri);
				}
				passages.add(p);
			}
			result.put("passages", passages);
			@SuppressWarnings("unchecked")
			Map<String, Object> diag = (Map<String, Object>) result.getOrDefault("diagnostics", new HashMap<>());
			diag.put("passages_returned", passages.size());
			result.put("diagnostics", diag);
		}

		classLogger.info("JenaHybridRetrieve diagnostics: {}",
				result != null ? result.get("diagnostics") : "null");
		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private static Object firstNonNull(Object a, Object b) {
		return a != null ? a : b;
	}

	@Override
	public String getReactorDescription() {
		return """
				The full hybrid retrieval pass over the Jena GraphRAG store: extract \
				candidate entities from the question, link each to graph URIs, expand \
				the anchor set 1-2 hops for structural context, and (if a paired vector \
				engine is configured) fold in chunked-prose retrieval for narrative \
				passages. Returns anchors + subgraph + passages + diagnostics — \
				everything an agent needs to synthesize an answer with citations.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The Jena GraphRAG engine ID.";
		} else if (key.equals(QUESTION_KEY)) {
			return "The natural-language question the agent is trying to answer.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Max passages / max anchor entities to return. Defaults to 6.";
		} else if (key.equals(HOPS_KEY)) {
			return "How many hops to expand around each anchor. Defaults to 1, capped at 3.";
		} else if (key.equals(SECTION_FILTER_KEY)) {
			return "Optional filter restricting vector search to chunks tagged with this section name.";
		} else if (key.equals(MIN_SCORE_KEY)) {
			return "Optional minimum entity-match score threshold.";
		}
		return super.getDescriptionForKey(key);
	}
}
