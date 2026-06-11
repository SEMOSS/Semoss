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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.reactor.AbstractReactor;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum.VectorQueryParamOptions;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class VectorDatabaseQueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(VectorDatabaseQueryReactor.class);

	public VectorDatabaseQueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.FILTERS.getKey(), ReactorKeysEnum.META_FILTERS.getKey() };
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

		String embeddingsEngineId = eng.getSmssProp().getProperty(Constants.EMBEDDER_ENGINE_ID);
		if (embeddingsEngineId == null
				|| !SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), embeddingsEngineId)) {
			throw new IllegalArgumentException("Embeddings model " + embeddingsEngineId
					+ " does not exist or user does not have access to this model");
		}

		String searchStatement = getString(ReactorKeysEnum.COMMAND.getKey());
		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), 5);
		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}

		// add the insightId so Model Engine calls can be made for python
		List<IQueryFilter> filters = getFilters(ReactorKeysEnum.FILTERS.getKey());
		if (filters != null) {
			paramMap.put(AbstractVectorDatabaseEngine.FILTERS_KEY, filters);
		}
		filters = getFilters(ReactorKeysEnum.META_FILTERS.getKey());
		if (filters != null) {
			paramMap.put(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY, filters);
		}

		Object output = eng.nearestNeighbor(this.insight, searchStatement, limit, paramMap);
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	/**
	 * 
	 * @param key
	 * @return
	 */
	private List<IQueryFilter> getFilters(String key) {
		AbstractQueryStruct qs;
		GenRowStruct filterGrs = store.getGenRowStruct(key);
		if (filterGrs != null && !filterGrs.isEmpty()) {
			List<NounMetadata> filterInputs = filterGrs.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if (filterInputs != null && !filterInputs.isEmpty()) {
				qs = (AbstractQueryStruct) filterInputs.get(0).getValue();
				List<IQueryFilter> filters = qs.getCombinedFilters().getFilters();
				return filters;
			}
		}
		return null;
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals(ReactorKeysEnum.FILTERS.getKey()) || key.equals(ReactorKeysEnum.META_FILTERS.getKey())) {
			return MCP_KEY_TYPE.OBJECT;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return """
				Performs a nearest neighbor search in a vector database. \
				Takes a search query, converts it to an embedding, and returns the most similar documents \
				from the vector database based on vector similarity. \
				Supports optional filtering on document content and metadata.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The vector database engine ID to query.";
		} else if (key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "The search query text to find similar documents for";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The maximum number of similar documents to return. Defaults to 5 if not specified";
		} else if (key.equals(ReactorKeysEnum.FILTERS.getKey())) {
			return "Optional filters to apply on the document content before returning results";
		} else if (key.equals(ReactorKeysEnum.META_FILTERS.getKey())) {
			return "Optional filters to apply on the document metadata before returning results";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			StringBuilder finalDescription = new StringBuilder("Param Options depend on the engine implementation");

			for (VectorQueryParamOptions entry : VectorQueryParamOptions.values()) {
				finalDescription.append("\n").append("\t\t\t\t\t")
						.append(entry.getVectorDbType().getVectorDatabaseName()).append(":");

				for (String paramKey : entry.getParamOptionsKeys()) {
					finalDescription.append("\n").append("\t\t\t\t\t\t").append(paramKey).append("\t").append("-")
							.append("\t").append("(").append(entry.getRequirementStatus(paramKey)).append(")")
							.append(" ").append(VectorDatabaseParamOptionsEnum.getDescriptionFromKey(paramKey));
				}
			}
			return finalDescription.toString();
		}

		return super.getDescriptionForKey(key);
	}
}
