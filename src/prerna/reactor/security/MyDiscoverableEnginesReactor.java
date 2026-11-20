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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MyDiscoverableEnginesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MyDiscoverableEnginesReactor.class);

	public MyDiscoverableEnginesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILTER_WORD.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.ENGINE_TYPE.getKey(), ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.META_KEYS.getKey(), ReactorKeysEnum.META_FILTERS.getKey(),
				ReactorKeysEnum.NO_META.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String searchTerm = this.keyValue.get(this.keysToGet[0]);
		String limit = this.keyValue.get(this.keysToGet[1]);
		String offset = this.keyValue.get(this.keysToGet[2]);
		List<String> engineTypes = getListString(ReactorKeysEnum.ENGINE_TYPE.getKey());
		if (engineTypes != null && engineTypes.isEmpty()) {
			engineTypes = null;
		}
		List<String> engineIdFilters = getListString(ReactorKeysEnum.ENGINE.getKey());
		if (engineIdFilters != null && engineIdFilters.isEmpty()) {
			engineIdFilters = null;
		}
		Boolean noMeta = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.NO_META.getKey()));
		List<String> metaKeys = getListString(ReactorKeysEnum.META_KEYS.getKey());
		if (metaKeys != null && metaKeys.isEmpty()) {
			metaKeys = null;
		}

		Map<String, Object> engineMetadataFilter = this.<String, Object>getGenericMap(
				ReactorKeysEnum.META_FILTERS.getKey(), null);
		List<Map<String, Object>> engineInfo = SecurityEngineUtils.getUserDiscoverableEngineList(this.insight.getUser(),
				engineTypes, engineIdFilters, engineMetadataFilter, searchTerm, limit, offset);

		if (!engineInfo.isEmpty() && !noMeta) {
			Map<String, Integer> index = new HashMap<>(engineInfo.size());
			int size = engineInfo.size();
			// now we want to add most executed insights
			for (int i = 0; i < size; i++) {
				Map<String, Object> engine = engineInfo.get(i);
				String engineId = engine.get("engine_id").toString();
				// keep list of engine ids to get the index
				index.put(engineId, Integer.valueOf(i));
			}

			IRawSelectWrapper wrapper = null;
			try {
				wrapper = SecurityEngineUtils.getEngineMetadataWrapper(index.keySet(), metaKeys, true);
				while (wrapper.hasNext()) {
					Object[] data = wrapper.next().getValues();
					String engineId = (String) data[0];
					String metaKey = (String) data[1];
					String metaValue = (String) data[2];
					if (metaValue == null) {
						continue;
					}

					int indexToFind = index.get(engineId);
					Map<String, Object> res = engineInfo.get(indexToFind);
					// whatever it is, if it is single send a single value, if it is multi send as
					// array
					if (res.containsKey(metaKey)) {
						Object obj = res.get(metaKey);
						if (obj instanceof List) {
							((List) obj).add(metaValue);
						} else {
							List<Object> newList = new ArrayList<>();
							newList.add(obj);
							newList.add(metaValue);
							res.put(metaKey, newList);
						}
					} else {
						res.put(metaKey, metaValue);
					}
				}
			} catch (Exception e) {
				classLogger.error("Failed to attach metadata values to discoverable engine list response", e);
			} finally {
				if (wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						classLogger.error(
								"Failed to close metadata wrapper while building discoverable engine list response", e);
					}
				}
			}
		}

		return new NounMetadata(engineInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.SORT.getKey())) {
			return "The sort is a string value containing either 'name' or 'date' for how to sort";
		} else if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "This is an optional database filter";
		}
		return super.getDescriptionForKey(key);
	}
}
