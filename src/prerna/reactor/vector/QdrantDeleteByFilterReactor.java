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

public class QdrantDeleteByFilterReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(QdrantDeleteByFilterReactor.class);

	public QdrantDeleteByFilterReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.FILTERS.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		String engineId = getString(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have edit access.");
		}

		IVectorDatabaseEngine eng = Utility.getVectorDatabase(engineId);
		if (eng == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		if (!(eng instanceof QdrantVectorDatabaseEngine)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a Qdrant vector database");
		}

		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}
		List<IQueryFilter> filters = getFilters(ReactorKeysEnum.FILTERS.getKey());
		if (filters != null) {
			paramMap.put(QdrantVectorDatabaseEngine.FILTERS_KEY, filters);
		}
		if (!paramMap.containsKey(QdrantVectorDatabaseEngine.FILTERS_KEY)
				&& !paramMap.containsKey(QdrantVectorDatabaseEngine.QDRANT_FILTER_KEY)) {
			throw new IllegalArgumentException("A filter is required — refusing to delete every point.");
		}

		int op = ((QdrantVectorDatabaseEngine) eng).deleteByFilter(this.insight, paramMap);
		classLogger.info("QdrantDeleteByFilter operation_id = {}", op);
		Map<String, Object> result = new HashMap<>();
		result.put("operation_id", op);
		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
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
				Delete every point in a Qdrant collection whose payload matches the supplied filter. \
				This is a scalpel, not a hammer — a filter is required. Common use: purge every point \
				where Source == 'stale-doc.pdf', or remove points older than a Timestamp. Filters use \
				the same DSL as QdrantSearch and QdrantHybridSearch.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The Qdrant vector database engine ID.";
		} else if (key.equals(ReactorKeysEnum.FILTERS.getKey())) {
			return "Server-side filter identifying which points to delete. Required.";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "Optional map: indexClass, qdrantFilter (native filter dict as an alternative to filters).";
		}
		return super.getDescriptionForKey(key);
	}
}
