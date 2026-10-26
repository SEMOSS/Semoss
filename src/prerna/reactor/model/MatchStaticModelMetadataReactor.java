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
package prerna.reactor.model;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.StaticModelMetadataCatalog;

/**
 * Resolve a model id against the meta/model.json catalog, and when it does not
 * resolve, hand back what the user needs to pick an entry themselves.
 * <p>
 * The returned map holds:
 * <ul>
 * <li><b>modelId</b> - the id that was looked up, trimmed</li>
 * <li><b>exactMatch</b> - the catalog key the id resolved to, present only when
 * it resolved at all</li>
 * <li><b>matches</b> - the closest catalog entries, best first, each with its
 * key, id, name, provider, family, and match score; empty when nothing in the
 * catalog resembles the id. A score of 1 without an exactMatch means the entry
 * is the same id once casing and punctuation are set aside, as with "gpt5"
 * against the "gpt-5" key</li>
 * <li><b>allKeys</b> - every catalog key, sorted, to fall back on when the
 * suggestions do not include what the user wanted</li>
 * </ul>
 */
public class MatchStaticModelMetadataReactor extends AbstractReactor {

	static final String MODEL_ID_KEY = "modelId";
	static final String LIMIT_KEY = "limit";

	static final String MODEL_ID_OUTPUT = "modelId";
	static final String EXACT_MATCH_OUTPUT = "exactMatch";
	static final String MATCHES_OUTPUT = "matches";
	static final String ALL_KEYS_OUTPUT = "allKeys";

	static final int DEFAULT_LIMIT = 10;
	static final int MAXIMUM_LIMIT = 50;

	public MatchStaticModelMetadataReactor() {
		this.keysToGet = new String[] { MODEL_ID_KEY, LIMIT_KEY };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String modelId = requireValue(this.keyValue.get(MODEL_ID_KEY), MODEL_ID_KEY);
		int limit = getLimit();
		Path metadataFile = getMetadataFile();

		Map<String, Object> result = new LinkedHashMap<>();
		result.put(MODEL_ID_OUTPUT, modelId);

		if (Files.isRegularFile(metadataFile)) {
			String exactMatch = StaticModelMetadataCatalog.findModelKey(metadataFile, modelId);
			if (exactMatch != null) {
				result.put(EXACT_MATCH_OUTPUT, exactMatch);
			}
		}

		List<Map<String, Object>> matches = StaticModelMetadataCatalog.findClosestMatches(metadataFile, modelId, limit);
		result.put(MATCHES_OUTPUT, matches);
		result.put(ALL_KEYS_OUTPUT, StaticModelMetadataCatalog.getCatalogKeys(metadataFile));

		return new NounMetadata(result, PixelDataType.MAP);
	}

	private int getLimit() {
		String value = this.keyValue.get(LIMIT_KEY);
		if (value == null || value.trim().isEmpty()) {
			return DEFAULT_LIMIT;
		}

		int limit;
		try {
			limit = Integer.parseInt(value.trim());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("The " + LIMIT_KEY + " must be a whole number", e);
		}
		if (limit < 1) {
			throw new IllegalArgumentException("The " + LIMIT_KEY + " must be at least 1");
		}
		return Math.min(limit, MAXIMUM_LIMIT);
	}

	Path getMetadataFile() {
		return StaticModelMetadataCatalog.getMetadataFile();
	}

	private static String requireValue(String value, String key) {
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException("Must input a " + key);
		}
		return value.trim();
	}

	@Override
	public String getReactorDescription() {
		return "Matches a model ID against meta/model.json, returning the exact catalog key when there is one, "
				+ "the closest catalog entries, and every catalog key to choose from";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(MODEL_ID_KEY)) {
			return "The model ID to match against the keys and provider IDs in meta/model.json";
		}
		if (key.equals(LIMIT_KEY)) {
			return "How many suggested matches to return, defaulting to " + DEFAULT_LIMIT + " and capped at "
					+ MAXIMUM_LIMIT;
		}
		return super.getDescriptionForKey(key);
	}
}
