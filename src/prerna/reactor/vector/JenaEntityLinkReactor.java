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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.graphrag.JenaGraphRAGEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class JenaEntityLinkReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JenaEntityLinkReactor.class);

	private static final String TEXT_KEY = "text";
	private static final String NODE_TYPES_KEY = "nodeTypes";
	private static final String SCORE_THRESHOLD_KEY = "scoreThreshold";

	public JenaEntityLinkReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				TEXT_KEY,
				ReactorKeysEnum.LIMIT.getKey(),
				NODE_TYPES_KEY,
				SCORE_THRESHOLD_KEY
		};
		this.keyRequired = new int[] { 1, 1, 0, 0, 0 };
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

		String text = getString(TEXT_KEY);
		if (text == null || text.trim().isEmpty()) {
			throw new IllegalArgumentException("text is required");
		}
		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), 5);

		Map<String, Object> paramMap = new HashMap<>();
		List<String> nodeTypes = getStringList(NODE_TYPES_KEY);
		if (nodeTypes != null && !nodeTypes.isEmpty()) {
			paramMap.put("nodeTypes", nodeTypes);
		}
		String scoreThreshold = getString(SCORE_THRESHOLD_KEY);
		if (scoreThreshold != null && !scoreThreshold.trim().isEmpty()) {
			try {
				paramMap.put("scoreThreshold", Double.parseDouble(scoreThreshold));
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid scoreThreshold '{}'; ignoring", scoreThreshold);
			}
		}

		List<Map<String, Object>> matches = ((JenaGraphRAGEngine) eng).entityLink(this.insight, text, limit, paramMap);
		classLogger.info("JenaEntityLink returned {} matches", matches != null ? matches.size() : 0);
		return new NounMetadata(matches, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private List<String> getStringList(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		java.util.List<String> out = new java.util.ArrayList<>();
		for (int i = 0; i < grs.size(); i++) {
			Object v = grs.get(i);
			if (v == null) {
				continue;
			}
			if (v instanceof List) {
				for (Object inner : (List<?>) v) {
					if (inner != null) {
						out.add(inner.toString());
					}
				}
			} else {
				out.add(v.toString());
			}
		}
		return out;
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals(NODE_TYPES_KEY)) {
			return MCP_KEY_TYPE.ARRAY;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return """
				Match a natural-language phrase against entities in the Jena GraphRAG \
				knowledge graph and return the top-k candidates ranked by label-match \
				score (with an optional boost from a paired vector engine). This is the \
				"linking" stage — take a phrase like "atorvastatin" and get back a small \
				ranked list of candidate URIs to anchor further queries.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The Jena GraphRAG engine ID.";
		} else if (key.equals(TEXT_KEY)) {
			return "The phrase to link (typically an entity name extracted from the user's question).";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Max candidates to return. Defaults to 5.";
		} else if (key.equals(NODE_TYPES_KEY)) {
			return "Optional list of RDF class URIs to restrict matches to.";
		} else if (key.equals(SCORE_THRESHOLD_KEY)) {
			return "Optional minimum match score (0.0 - 3.0) to include a candidate.";
		}
		return super.getDescriptionForKey(key);
	}
}
