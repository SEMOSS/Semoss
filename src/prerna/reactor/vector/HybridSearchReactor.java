/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *******************************************************************************/
package prerna.reactor.vector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
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

public class HybridSearchReactor extends AbstractReactor {

	private static final String SEARCH_FIELDS_KEY = "searchFields";

	public HybridSearchReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				SEARCH_FIELDS_KEY, ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.FILTERS.getKey(),
				AbstractVectorDatabaseEngine.SEARCH_MODE_KEY, AbstractVectorDatabaseEngine.SELECT_KEY };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		String engineId = getString(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have access to it.");
		}

		IVectorDatabaseEngine engine = Utility.getVectorDatabase(engineId);
		if (engine == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		if (engine.getVectorDatabaseType() != VectorDatabaseTypeEnum.AZURE_AI_SEARCH) {
			throw new IllegalArgumentException("Hybrid search is only supported for Azure AI Search engines");
		}

		String embeddingsEngineId = engine.getSmssProp().getProperty(Constants.EMBEDDER_ENGINE_ID);
		if (embeddingsEngineId == null
				|| !SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), embeddingsEngineId)) {
			throw new IllegalArgumentException("Embeddings model " + embeddingsEngineId
					+ " does not exist or user does not have access to this model");
		}

		String searchStatement = getString(ReactorKeysEnum.COMMAND.getKey());
		if (searchStatement == null || searchStatement.trim().isEmpty()) {
			throw new IllegalArgumentException("Search command must not be blank");
		}

		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), 5);
		Map<String, Object> parameters = new HashMap<>();
		List<IQueryFilter> filters = getFilters(ReactorKeysEnum.FILTERS.getKey());
		if (filters != null) {
			parameters.put(AbstractVectorDatabaseEngine.FILTERS_KEY, filters);
		}
		String searchMode = getString(AbstractVectorDatabaseEngine.SEARCH_MODE_KEY);
		if (searchMode != null) {
			parameters.put(AbstractVectorDatabaseEngine.SEARCH_MODE_KEY, searchMode);
		}
		List<String> selectFields = getFields(AbstractVectorDatabaseEngine.SELECT_KEY);
		if (selectFields != null) {
			parameters.put(AbstractVectorDatabaseEngine.SELECT_KEY, selectFields);
		}

		List<Map<String, Object>> output = engine.hybridSearch(this.insight, searchStatement,
				getFields(SEARCH_FIELDS_KEY), limit, parameters);
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private List<String> getFields(String key) {
		GenRowStruct fieldsGrs = this.store.getGenRowStruct(key);
		if (fieldsGrs == null || fieldsGrs.isEmpty()) {
			return null;
		}
		List<String> fields = new ArrayList<>();
		for (int i = 0; i < fieldsGrs.size(); i++) {
			Object value = fieldsGrs.get(i);
			if (value instanceof Collection<?>) {
				for (Object nestedValue : (Collection<?>) value) {
					fields.add(nestedValue == null ? null : nestedValue.toString());
				}
			} else {
				fields.add(value == null ? null : value.toString());
			}
		}
		return fields;
	}

	private List<IQueryFilter> getFilters(String key) {
		GenRowStruct filterGrs = store.getGenRowStruct(key);
		if (filterGrs != null && !filterGrs.isEmpty()) {
			List<NounMetadata> filterInputs = filterGrs.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if (filterInputs != null && !filterInputs.isEmpty()) {
				AbstractQueryStruct queryStruct = (AbstractQueryStruct) filterInputs.get(0).getValue();
				return queryStruct.getCombinedFilters().getFilters();
			}
		}
		return null;
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (SEARCH_FIELDS_KEY.equals(key) || AbstractVectorDatabaseEngine.SELECT_KEY.equals(key)) {
			return MCP_KEY_TYPE.ARRAY;
		}
		if (ReactorKeysEnum.FILTERS.getKey().equals(key)) {
			return MCP_KEY_TYPE.OBJECT;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ENGINE.getKey().equals(key)) {
			return "The Azure AI Search vector database engine ID to query";
		} else if (ReactorKeysEnum.COMMAND.getKey().equals(key)) {
			return "The nonblank search query text";
		} else if (SEARCH_FIELDS_KEY.equals(key)) {
			return "Optional list of searchable string fields. Defaults to every searchable string field in the index";
		} else if (ReactorKeysEnum.LIMIT.getKey().equals(key)) {
			return "The maximum number of documents to return. Defaults to 5";
		} else if (ReactorKeysEnum.FILTERS.getKey().equals(key)) {
			return "Optional filters to apply to indexed document fields";
		} else if (AbstractVectorDatabaseEngine.SEARCH_MODE_KEY.equals(key)) {
			return "Optional search matching mode: 'any' (default) or 'all'";
		} else if (AbstractVectorDatabaseEngine.SELECT_KEY.equals(key)) {
			return "Optional list of selectable index fields to return. Defaults to the standard document fields";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public String getReactorDescription() {
		return "Performs a combined keyword and vector search in an Azure AI Search vector database. "
				+ "The query is embedded with the database's configured embedding model and supports optional content filters.";
	}
}
