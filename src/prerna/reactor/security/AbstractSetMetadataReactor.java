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
package prerna.reactor.security;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Base reactor for metadata update operations.
 * <p>
 * The {@code jsonCleanup} input is a legacy compatibility path intended for
 * older clients that previously sent double-escaped metadata strings. Modern
 * clients should rely on parser-level decoding and avoid enabling this flag.
 */
public abstract class AbstractSetMetadataReactor extends AbstractReactor {

	protected static final String META = "meta";

	/**
	 * Get the metadata map while handling encoded input paths.
	 * <p>
	 * Note: {@code jsonCleanup} is legacy compatibility for older escaped payloads
	 * and should not be used by modern clients.
	 * 
	 * @return parsed metadata map
	 */
	protected Map<String, Object> getMetaMap() {
		Boolean jsonCleanup = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.JSON_CLEANUP.getKey()) + "");
		GenRowStruct metaGrs = this.store.getGenRowStruct(META);
		return parseMetaMap(metaGrs, this.curRow, jsonCleanup);
	}

	/**
	 * Shared metadata-map parsing logic for metadata reactors.
	 * <p>
	 * This exists so reactors that cannot extend this class (because they already
	 * extend another abstract base) can still use a single implementation.
	 *
	 * @param metaGrs     metadata noun row from the noun store
	 * @param curRow      fallback current row
	 * @param jsonCleanup legacy cleanup compatibility flag
	 * @return parsed metadata map
	 */
	protected static Map<String, Object> parseMetaMap(GenRowStruct metaGrs, GenRowStruct curRow, boolean jsonCleanup) {
		Map<String, Object> mapValue = parseMapValue(metaGrs);
		if (mapValue != null) {
			if (jsonCleanup) {
				applyLegacyJsonCleanup(mapValue);
			}
			return mapValue;
		}
		mapValue = parseMapValue(curRow);
		if (mapValue != null) {
			if (jsonCleanup) {
				applyLegacyJsonCleanup(mapValue);
			}
			return mapValue;
		}

		throw new IllegalArgumentException("Must define a metadata map");
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> parseMapValue(GenRowStruct grs) {
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		List<NounMetadata> mapInputs = grs.getNounsOfType(PixelDataType.MAP);
		if (mapInputs == null || mapInputs.isEmpty()) {
			return null;
		}
		return (Map<String, Object>) mapInputs.get(0).getValue();
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> parseEncodedMetaMap(GenRowStruct grs) {
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		List<NounMetadata> encodedStrInputs = grs.getNounsOfType(PixelDataType.CONST_STRING);
		if (encodedStrInputs == null || encodedStrInputs.isEmpty()) {
			return null;
		}
		String str = (String) encodedStrInputs.get(0).getValue();
		return GSON.fromJson(str, Map.class);
	}

	/**
	 * Recursively applies legacy escaped-string cleanup to map values.
	 * <p>
	 * This method exists only for {@code jsonCleanup} compatibility mode.
	 */
	private static void applyLegacyJsonCleanup(Map<String, Object> map) {
		Map<String, Object> replacements = new HashMap<>();
		for (String key : map.keySet()) {
			replacements.put(key, cleanupValue(map.get(key)));
		}

		map.putAll(replacements);
	}

	/**
	 * Normalizes nested values during legacy {@code jsonCleanup} processing.
	 */
	@SuppressWarnings("unchecked")
	private static Object cleanupValue(Object value) {
		if (value instanceof Map) {
			applyLegacyJsonCleanup((Map<String, Object>) value);
			return value;
		}
		if (value instanceof Collection) {
			List<Object> newList = new ArrayList<>();
			for (Object o : (Collection<?>) value) {
				newList.add(cleanupValue(o));
			}
			return newList;
		}
		if (value instanceof String) {
			return PixelUtility.decodeEscapedString((String) value);
		}
		return value;
	}
}
