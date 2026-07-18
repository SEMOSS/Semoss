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

public class QdrantListPointsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(QdrantListPointsReactor.class);

	public QdrantListPointsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), ReactorKeysEnum.FILTERS.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
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

		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), 100);
		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}
		paramMap.put("limit", limit);
		List<IQueryFilter> filters = getFilters(ReactorKeysEnum.FILTERS.getKey());
		if (filters != null) {
			paramMap.put(QdrantVectorDatabaseEngine.FILTERS_KEY, filters);
		}

		Map<String, Object> result = ((QdrantVectorDatabaseEngine) eng).listPoints(this.insight, paramMap);
		classLogger.info("QdrantListPoints returned {} points",
				result != null && result.get("points") != null ? ((List<?>) result.get("points")).size() : 0);
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
				Scroll through the raw points in a Qdrant collection, optionally narrowed by a native filter. \
				Useful for browsing what's actually ingested, checking payload shapes, or paging over a subset \
				(all points with Source == 'X'). Returns {"points": [...], "next_offset": ...}; feed \
				next_offset back via param map to page through.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The Qdrant vector database engine ID.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Maximum points to return per page. Defaults to 100.";
		} else if (key.equals(ReactorKeysEnum.FILTERS.getKey())) {
			return "Optional server-side filter narrowing the scroll.";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "Optional map: indexClass, offset (opaque pagination token from a prior call), qdrantFilter.";
		}
		return super.getDescriptionForKey(key);
	}
}
