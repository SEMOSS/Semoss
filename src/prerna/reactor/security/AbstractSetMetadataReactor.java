/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.reactor.security;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public abstract class AbstractSetMetadataReactor extends AbstractReactor {

	protected static final String META = "meta";

	/**
	 * Get the meta map and account for encoded input or not
	 *
	 * @return
	 */
	protected Map<String, Object> getMetaMap() {
		Boolean encoded = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.ENCODED.getKey()) + "");
		GenRowStruct metaGrs = this.store.getNoun(META);
		if (encoded) {
			if (metaGrs != null && !metaGrs.isEmpty()) {
				List<NounMetadata> encodedStrInputs = metaGrs.getNounsOfType(PixelDataType.CONST_STRING);
				if (encodedStrInputs != null && !encodedStrInputs.isEmpty()) {
					String encodedStr = (String) encodedStrInputs.get(0).getValue();
					String decodedStr = Utility.decodeURIComponent(encodedStr);
					return new Gson().fromJson(decodedStr, Map.class);
				}
			}

			List<NounMetadata> encodedStrInputs = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
			if (encodedStrInputs != null && !encodedStrInputs.isEmpty()) {
				String encodedStr = (String) encodedStrInputs.get(0).getValue();
				String decodedStr = Utility.decodeURIComponent(encodedStr);
				return new Gson().fromJson(decodedStr, Map.class);
			}
		} else {
			Boolean jsonCleanup = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.JSON_CLEANUP.getKey()) + "");
			if (metaGrs != null && !metaGrs.isEmpty()) {
				List<NounMetadata> mapInputs = metaGrs.getNounsOfType(PixelDataType.MAP);
				if (mapInputs != null && !mapInputs.isEmpty()) {
					Map<String, Object> retMap = (Map<String, Object>) mapInputs.get(0).getValue();
					if (jsonCleanup) {
						cleanupMap(retMap);
					}
					return retMap;
				}
			}

			List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				Map<String, Object> retMap = (Map<String, Object>) mapInputs.get(0).getValue();
				if (jsonCleanup) {
					cleanupMap(retMap);
				}
				return retMap;
			}
		}

		throw new IllegalArgumentException("Must define a metadata map");
	}

	protected void cleanupMap(Map<String, Object> map) {
		Map<String, Object> replacements = new HashMap<>();
		for (String key : map.keySet()) {
			Object value = map.get(key);
			if (value instanceof Map) {
				cleanupMap((Map<String, Object>) value);
			}
			if (value instanceof Collection) {
				List<Object> newList = new ArrayList<>();
				for (Object o : (Collection) value) {
					if (o instanceof String) {
						newList.add(((String) o).replaceAll("\\\\n", "\n").replaceAll("\\\\t", "\t"));
					} else {
						newList.add(o);
					}
				}
				replacements.put(key, value);
			} else if (value instanceof String) {
				value = ((String) value).replaceAll("\\\\n", "\n").replaceAll("\\\\t", "\t");
				replacements.put(key, value);
			}
		}

		map.putAll(replacements);
	}
}
