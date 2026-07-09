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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.vector.QdrantVectorDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class QdrantAddPointsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(QdrantAddPointsReactor.class);

	private static final String POINTS_KEY = "points";
	private static final Gson GSON_LOCAL = new Gson();

	public QdrantAddPointsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), POINTS_KEY,
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		String engineId = getString(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have edit access.");
		}

		// Utility.getVectorDatabase returns a Proxy exposing only the
		// IVectorDatabaseEngine interface; Qdrant-specific methods and the
		// instanceof check need the raw engine from DIHelper (force-load first).
		Utility.getVectorDatabase(engineId);
		Object rawEng = DIHelper.getInstance().getEngineProperty(engineId);
		IVectorDatabaseEngine eng = (rawEng instanceof IVectorDatabaseEngine)
				? (IVectorDatabaseEngine) rawEng : null;
		if (eng == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		if (!(eng instanceof QdrantVectorDatabaseEngine)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a Qdrant vector database");
		}

		List<Map<String, Object>> points = getPoints();
		if (points == null || points.isEmpty()) {
			throw new IllegalArgumentException("At least one point must be supplied");
		}
		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}

		Map<String, Object> result = ((QdrantVectorDatabaseEngine) eng).addPoints(this.insight, points, paramMap);
		classLogger.info("QdrantAddPoints result: {}", result);
		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getPoints() {
		GenRowStruct grs = this.store.getNoun(POINTS_KEY);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		List<Map<String, Object>> out = new ArrayList<>();
		for (int i = 0; i < grs.size(); i++) {
			Object v = grs.get(i);
			if (v == null) {
				continue;
			}
			if (v instanceof List) {
				for (Object inner : (List<?>) v) {
					out.add(coerceMap(inner));
				}
			} else {
				out.add(coerceMap(v));
			}
		}
		return out;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> coerceMap(Object raw) {
		if (raw instanceof Map) {
			return (Map<String, Object>) raw;
		}
		if (raw instanceof String) {
			try {
				Object parsed = GSON_LOCAL.fromJson((String) raw, Object.class);
				if (parsed instanceof Map) {
					return (Map<String, Object>) parsed;
				}
			} catch (Exception ignored) {
			}
		}
		throw new IllegalArgumentException(
				"QdrantAddPoints entries must be JSON objects (received "
						+ raw.getClass().getSimpleName()
						+ "). Each entry must be {text|vector, payload?, source?, id?, ...}.");
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals(POINTS_KEY)) {
			return MCP_KEY_TYPE.ARRAY;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return """
				Directly ingest points into a Qdrant collection, bypassing the CSV/document pipeline. \
				Each entry is one point. Provide either {"text": "..."} to have the collection's embedder \
				embed it, or {"vector": [...]} to upsert a pre-embedded vector as-is. Optional per-entry: \
				"payload" (arbitrary key/value metadata, filterable server-side), "source", "id", "divider", \
				"part". Payload["Source"] is what ListDocuments and RemoveDocument key off of.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The Qdrant vector database engine ID.";
		} else if (key.equals(POINTS_KEY)) {
			return "Array of point objects: [{text|vector, payload?, source?, id?, divider?, part?}, ...].";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "Optional map with keys such as indexClass, batch_size.";
		}
		return super.getDescriptionForKey(key);
	}
}
