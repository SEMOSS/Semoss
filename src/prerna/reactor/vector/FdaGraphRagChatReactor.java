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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Portal bridge for the FDA GraphRAG demo app.
 *
 * Runs the LangGraph GraphRAG agent (py/fda_graphrag_agent.chat_json) inside
 * the calling insight's Python context — where the SEMOSS ModelEngine (Opus
 * 4.7) resolves — and returns its base64-encoded JSON result
 * ({@code {answer, evidence, steps}}) straight back to the portal.
 *
 * Registered by ClassGraph package scan as the pixel {@code FdaGraphRagChat}.
 * The agent module must be deployed to the server Python path
 * ({@code /opt/semosshome/py/fda_graphrag_agent.py}).
 */
public class FdaGraphRagChatReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(FdaGraphRagChatReactor.class);
	private static final String QUESTION_B64 = "question_b64";

	public FdaGraphRagChatReactor() {
		this.keysToGet = new String[] { QUESTION_B64 };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String qb64 = getString(QUESTION_B64);
		if (qb64 == null || qb64.trim().isEmpty()) {
			throw new IllegalArgumentException("question_b64 is required");
		}

		PyTranslator pt = this.insight.getPyTranslator();
		if (pt == null) {
			throw new SemossPixelException("No Python translator is available on this insight");
		}

		String call = "fda_graphrag_agent.chat_json(" + PyUtils.determineStringType(qb64) + ")";
		classLogger.info("Running FDA GraphRAG agent >>> {}", call);
		Object result;
		try {
			result = pt.runScript("import fda_graphrag_agent", call);
		} catch (Exception e) {
			classLogger.error("FDA GraphRAG agent invocation failed", e);
			throw new SemossPixelException("FDA GraphRAG agent failed: " + e.getMessage());
		}

		return new NounMetadata(result == null ? "" : result.toString(),
				PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Run the FDA GraphRAG LangGraph agent for one question. Input is a "
				+ "base64-encoded question; output is base64-encoded JSON "
				+ "{answer, evidence:{anchors, subgraph, passages}, steps}.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (QUESTION_B64.equals(key)) {
			return "The user's question, base64-encoded.";
		}
		return super.getDescriptionForKey(key);
	}
}
