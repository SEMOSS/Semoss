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
package prerna.reactor.vector;

import java.util.ArrayList;
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
import prerna.util.Constants;
import prerna.util.Utility;

public class QdrantRecommendReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(QdrantRecommendReactor.class);

	private static final String POSITIVE_IDS_KEY = "positiveIds";
	private static final String NEGATIVE_IDS_KEY = "negativeIds";

	public QdrantRecommendReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), POSITIVE_IDS_KEY, NEGATIVE_IDS_KEY,
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.FILTERS.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0 };
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

		String embeddingsEngineId = eng.getSmssProp().getProperty(Constants.EMBEDDER_ENGINE_ID);
		if (embeddingsEngineId == null
				|| !SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), embeddingsEngineId)) {
			throw new IllegalArgumentException("Embeddings model " + embeddingsEngineId
					+ " does not exist or user does not have access to this model");
		}

		List<String> positiveIds = getStringList(POSITIVE_IDS_KEY);
		if (positiveIds == null || positiveIds.isEmpty()) {
			throw new IllegalArgumentException("At least one positive id must be supplied");
		}
		List<String> negativeIds = getStringList(NEGATIVE_IDS_KEY);
		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), 5);

		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}

		List<IQueryFilter> filters = getFilters(ReactorKeysEnum.FILTERS.getKey());
		if (filters != null) {
			paramMap.put(QdrantVectorDatabaseEngine.FILTERS_KEY, filters);
		}

		QdrantVectorDatabaseEngine qdrant = (QdrantVectorDatabaseEngine) eng;
		List<Map<String, Object>> output = qdrant.recommend(this.insight, positiveIds, negativeIds, limit, paramMap);
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
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

	private List<IQueryFilter> getFilters(String key) {
		AbstractQueryStruct qs;
		GenRowStruct filterGrs = store.getGenRowStruct(key);
		if (filterGrs != null && !filterGrs.isEmpty()) {
			List<NounMetadata> filterInputs = filterGrs.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if (filterInputs != null && !filterInputs.isEmpty()) {
				qs = (AbstractQueryStruct) filterInputs.get(0).getValue();
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
		if (key.equals(POSITIVE_IDS_KEY) || key.equals(NEGATIVE_IDS_KEY)) {
			return MCP_KEY_TYPE.ARRAY;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return """
				Performs a Qdrant recommend query: finds points similar to the supplied positive point ids \
				while steering away from the supplied negative point ids. Optionally accepts filters and a \
				score threshold via the param map.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The Qdrant vector database engine ID.";
		} else if (key.equals(POSITIVE_IDS_KEY)) {
			return "List of point ids the result should look like.";
		} else if (key.equals(NEGATIVE_IDS_KEY)) {
			return "Optional list of point ids the result should look unlike.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The maximum number of recommendations to return. Defaults to 5.";
		} else if (key.equals(ReactorKeysEnum.FILTERS.getKey())) {
			return "Optional filters applied server-side before recommendation.";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "Optional map with keys such as scoreThreshold, indexClass, qdrantFilter.";
		}
		return super.getDescriptionForKey(key);
	}
}
