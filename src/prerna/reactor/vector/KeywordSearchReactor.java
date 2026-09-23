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
import prerna.util.Utility;

public class KeywordSearchReactor extends AbstractReactor {

	private static final String SEARCH_FIELDS_KEY = "searchFields";

	public KeywordSearchReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				SEARCH_FIELDS_KEY, ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.FILTERS.getKey(), ReactorKeysEnum.META_FILTERS.getKey() };
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
			throw new IllegalArgumentException("Keyword search is only supported for Azure AI Search engines");
		}

		String searchStatement = getString(ReactorKeysEnum.COMMAND.getKey());
		if (searchStatement == null || searchStatement.trim().isEmpty()) {
			throw new IllegalArgumentException("Search command must not be blank");
		}

		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), 5);
		Map<String, Object> parameters = getStringKeyedParameters();
		List<IQueryFilter> filters = getFilters(ReactorKeysEnum.FILTERS.getKey());
		if (filters != null) {
			parameters.put(AbstractVectorDatabaseEngine.FILTERS_KEY, filters);
		}
		List<IQueryFilter> metaFilters = getFilters(ReactorKeysEnum.META_FILTERS.getKey());
		if (metaFilters != null) {
			parameters.put(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY, metaFilters);
		}

		List<Map<String, Object>> output = engine.keywordSearch(this.insight, searchStatement, getSearchFields(), limit,
				parameters);
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private List<String> getSearchFields() {
		GenRowStruct fieldsGrs = this.store.getGenRowStruct(SEARCH_FIELDS_KEY);
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

	@SuppressWarnings("rawtypes")
	private Map<String, Object> getStringKeyedParameters() {
		Map rawParameters = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		Map<String, Object> parameters = new HashMap<>();
		if (rawParameters == null) {
			return parameters;
		}
		for (Object entryObject : rawParameters.entrySet()) {
			Map.Entry entry = (Map.Entry) entryObject;
			if (!(entry.getKey() instanceof String)) {
				throw new IllegalArgumentException("Parameter map keys must be strings");
			}
			parameters.put((String) entry.getKey(), entry.getValue());
		}
		return parameters;
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
		if (SEARCH_FIELDS_KEY.equals(key)) {
			return MCP_KEY_TYPE.ARRAY;
		}
		if (ReactorKeysEnum.FILTERS.getKey().equals(key) || ReactorKeysEnum.META_FILTERS.getKey().equals(key)) {
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
		} else if (ReactorKeysEnum.META_FILTERS.getKey().equals(key)) {
			return "Optional metadata filters retained for compatibility; Azure AI Search does not apply them";
		} else if (ReactorKeysEnum.PARAM_VALUES_MAP.getKey().equals(key)) {
			return "Optional string-keyed parameter map";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public String getReactorDescription() {
		return "Performs a keyword search over searchable text fields in an Azure AI Search vector database. "
				+ "Supports optional content filters and returns normalized document matches.";
	}
}
