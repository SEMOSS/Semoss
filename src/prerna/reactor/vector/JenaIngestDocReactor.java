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

public class JenaIngestDocReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JenaIngestDocReactor.class);

	private static final String BODY_KEY = "body";
	private static final String METADATA_KEY = "metadata";
	private static final String EXTRACTION_MODE_KEY = "extractionMode";

	public JenaIngestDocReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				BODY_KEY,
				METADATA_KEY,
				EXTRACTION_MODE_KEY
		};
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		String engineId = getString(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have edit access.");
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

		String body = getString(BODY_KEY);
		if (body == null || body.trim().isEmpty()) {
			throw new IllegalArgumentException("body is required");
		}

		@SuppressWarnings("unchecked")
		Map<String, Object> metadata = (Map<String, Object>) getMap(METADATA_KEY);
		if (metadata == null) {
			metadata = new HashMap<>();
		}

		Map<String, Object> paramMap = new HashMap<>();
		String extractionMode = getString(EXTRACTION_MODE_KEY);
		if (extractionMode != null && !extractionMode.trim().isEmpty()) {
			paramMap.put("extractionMode", extractionMode);
		}

		Map<String, Object> result = ((JenaGraphRAGEngine) eng).ingestDoc(this.insight, body, metadata, paramMap);
		classLogger.info("JenaIngestDoc result: {}", result);
		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals(METADATA_KEY)) {
			return MCP_KEY_TYPE.OBJECT;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return """
				Ingest a document into the Jena GraphRAG store. Two modes:

				* extractionMode="structured" (default) — body is a JSON string with \
				  shape {"nodes": [...], "edges": [...], "chunks": [...]}. Nodes/edges \
				  get RDFized into TTL and inserted. Chunks (if any) become smss:Chunk \
				  nodes and, when a paired vector engine is configured, get embedded there.

				* extractionMode="triples" — body is raw Turtle POSTed directly to Fuseki \
				  via the Graph Store Protocol. Skip RDF generation.

				URIs are derived deterministically from source + facts (UUID5), so \
				re-ingesting the same document is idempotent.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The Jena GraphRAG engine ID.";
		} else if (key.equals(BODY_KEY)) {
			return "The document body — a JSON string (structured mode) or Turtle text (triples mode).";
		} else if (key.equals(METADATA_KEY)) {
			return "Per-document metadata: source, title, timestamp, tags — attached to the derived nodes.";
		} else if (key.equals(EXTRACTION_MODE_KEY)) {
			return "'structured' (default) for JSON body, 'triples' for raw Turtle.";
		}
		return super.getDescriptionForKey(key);
	}
}
