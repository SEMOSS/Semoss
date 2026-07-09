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
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class JenaGraphExpandReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JenaGraphExpandReactor.class);

	private static final String NODE_URI_KEY = "nodeUri";
	private static final String HOPS_KEY = "hops";
	private static final String EDGE_TYPES_KEY = "edgeTypes";
	private static final String MAX_EDGES_KEY = "maxEdges";
	private static final String INCLUDE_LABELS_KEY = "includeLabels";

	public JenaGraphExpandReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				NODE_URI_KEY,
				HOPS_KEY,
				EDGE_TYPES_KEY,
				MAX_EDGES_KEY,
				INCLUDE_LABELS_KEY
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

		String nodeUri = getString(NODE_URI_KEY);
		if (nodeUri == null || nodeUri.trim().isEmpty()) {
			throw new IllegalArgumentException("nodeUri is required");
		}
		int hops = getInt(HOPS_KEY, 1);

		Map<String, Object> paramMap = new HashMap<>();
		List<String> edgeTypes = getStringList(EDGE_TYPES_KEY);
		if (edgeTypes != null && !edgeTypes.isEmpty()) {
			paramMap.put("edgeTypes", edgeTypes);
		}
		String maxEdges = getString(MAX_EDGES_KEY);
		if (maxEdges != null && !maxEdges.trim().isEmpty()) {
			try {
				paramMap.put("maxEdges", Integer.parseInt(maxEdges));
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid maxEdges '{}'; ignoring", maxEdges);
			}
		}
		String includeLabels = getString(INCLUDE_LABELS_KEY);
		if (includeLabels != null && !includeLabels.trim().isEmpty()) {
			paramMap.put("includeLabels", Boolean.parseBoolean(includeLabels));
		}

		List<Map<String, Object>> triples = ((JenaGraphRAGEngine) eng).graphExpand(this.insight, nodeUri, hops,
				paramMap);
		classLogger.info("JenaGraphExpand returned {} triples", triples != null ? triples.size() : 0);
		return new NounMetadata(triples, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private List<String> getStringList(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		List<String> out = new ArrayList<>();
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
		if (key.equals(EDGE_TYPES_KEY)) {
			return MCP_KEY_TYPE.ARRAY;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return """
				Return the local subgraph around a given node — every triple reachable \
				within N typed edges. Used by agents to explore what a node is connected \
				to, and by the UI to render "what the agent traversed". Hops are capped \
				at 3 to prevent full-graph walks. Direction-agnostic — captures both \
				outgoing and incoming edges from the anchor.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The Jena GraphRAG engine ID.";
		} else if (key.equals(NODE_URI_KEY)) {
			return "The anchor node URI (typically from a prior JenaEntityLink match).";
		} else if (key.equals(HOPS_KEY)) {
			return "Traversal depth. Defaults to 1. Capped at 3.";
		} else if (key.equals(EDGE_TYPES_KEY)) {
			return "Optional list of predicate URIs to follow. Omit for all edges.";
		} else if (key.equals(MAX_EDGES_KEY)) {
			return "Optional cap on returned triples. Defaults to 200.";
		} else if (key.equals(INCLUDE_LABELS_KEY)) {
			return "Whether to include rdfs:label on subject/object where available. Defaults to true.";
		}
		return super.getDescriptionForKey(key);
	}
}
