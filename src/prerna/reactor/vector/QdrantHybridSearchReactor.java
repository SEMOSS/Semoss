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
import prerna.engine.impl.vector.QdrantVectorDatabaseEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class QdrantHybridSearchReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(QdrantHybridSearchReactor.class);

	public QdrantHybridSearchReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.FILTERS.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		String engineId = getString(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have access to it.");
		}

		IVectorDatabaseEngine eng = Utility.getVectorDatabase(engineId);
		if (eng == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		if (!(eng instanceof QdrantVectorDatabaseEngine)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a Qdrant vector database");
		}

		String question = getString(ReactorKeysEnum.COMMAND.getKey());
		if (question == null || question.trim().isEmpty()) {
			throw new IllegalArgumentException("A search query is required");
		}
		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), 5);

		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}
		List<IQueryFilter> filters = getFilters(ReactorKeysEnum.FILTERS.getKey());
		if (filters != null) {
			paramMap.put(QdrantVectorDatabaseEngine.FILTERS_KEY, filters);
		}

		List<Map<String, Object>> output = ((QdrantVectorDatabaseEngine) eng).hybridSearch(this.insight, question, limit,
				paramMap);
		classLogger.info("QdrantHybridSearch returned {} results", output != null ? output.size() : 0);
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private List<IQueryFilter> getFilters(String key) {
		GenRowStruct filterGrs = store.getGenRowStruct(key);
		if (filterGrs != null && !filterGrs.isEmpty()) {
			List<NounMetadata> filterInputs = filterGrs.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if (filterInputs != null && !filterInputs.isEmpty()) {
				AbstractQueryStruct qs = (AbstractQueryStruct) filterInputs.get(0).getValue();
				return qs.getCombinedFilters().getFilters();
			}
		}
		return null;
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals(ReactorKeysEnum.FILTERS.getKey())) {
			return MCP_KEY_TYPE.OBJECT;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return """
				Hybrid nearest-neighbor search over a Qdrant collection: dense embeddings + BM25 sparse \
				vectors combined via Prefetch and RRF (or DBSF) fusion in a single query_points call. \
				Only works when the collection was created with hybrid search enabled. Supports server-side \
				filtering, score thresholds, and column projection via the param map.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The Qdrant vector database engine ID.";
		} else if (key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "Natural-language query to search for.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Maximum number of results to return. Defaults to 5.";
		} else if (key.equals(ReactorKeysEnum.FILTERS.getKey())) {
			return "Optional server-side filters applied before ranking.";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "Optional map: indexClass, scoreThreshold, hybrid_prefetch_limit, columns_to_return, qdrantFilter.";
		}
		return super.getDescriptionForKey(key);
	}
}
