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
import prerna.util.Utility;

public class JenaSparqlQueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JenaSparqlQueryReactor.class);

	private static final String SPARQL_KEY = "sparql";
	private static final String ROW_CAP_KEY = "rowCap";

	public JenaSparqlQueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), SPARQL_KEY, ROW_CAP_KEY };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		String engineId = getString(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have access.");
		}

		IVectorDatabaseEngine eng = Utility.getVectorDatabase(engineId);
		if (eng == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		if (!(eng instanceof JenaGraphRAGEngine)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a Jena GraphRAG database");
		}

		String sparql = getString(SPARQL_KEY);
		if (sparql == null || sparql.trim().isEmpty()) {
			throw new IllegalArgumentException("A SPARQL query is required");
		}

		Map<String, Object> paramMap = new HashMap<>();
		String rowCapStr = getString(ROW_CAP_KEY);
		if (rowCapStr != null && !rowCapStr.trim().isEmpty()) {
			try {
				paramMap.put("rowCap", Integer.parseInt(rowCapStr));
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid rowCap value '{}'; ignoring", rowCapStr);
			}
		}

		Map<String, Object> result = ((JenaGraphRAGEngine) eng).guardedQuery(this.insight, sparql, paramMap);
		classLogger.info("JenaSparqlQuery returned {} rows", result != null ? result.get("rowCount") : 0);
		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return """
				Run a read-only SPARQL query against the Jena GraphRAG store. \
				The query is guarded server-side: only SELECT / CONSTRUCT / ASK / DESCRIBE \
				are allowed (INSERT, DELETE, DROP, LOAD, etc. are rejected), workspace \
				ontology PREFIX declarations are auto-injected, and unbounded SELECT / \
				CONSTRUCT queries get a LIMIT appended. Returns the sanitized query as \
				actually executed plus the result rows.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The Jena GraphRAG engine ID.";
		} else if (key.equals(SPARQL_KEY)) {
			return "The SPARQL query text. PREFIX declarations are optional — the workspace ontology's prefixes are auto-injected.";
		} else if (key.equals(ROW_CAP_KEY)) {
			return "Optional: override the default LIMIT applied to unbounded SELECT queries.";
		}
		return super.getDescriptionForKey(key);
	}
}
