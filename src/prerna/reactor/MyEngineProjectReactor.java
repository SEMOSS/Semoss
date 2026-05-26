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
package prerna.reactor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MyEngineProjectReactor extends AbstractReactor {

	public MyEngineProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILTER_WORD.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.ONLY_FAVORITES.getKey(), ReactorKeysEnum.TYPE.getKey(),
				ReactorKeysEnum.SUB_TYPE.getKey(), ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.META_KEYS.getKey(),
				ReactorKeysEnum.META_FILTERS.getKey(), ReactorKeysEnum.PERMISSION_FILTERS.getKey(),
				ReactorKeysEnum.NO_META.getKey(), ReactorKeysEnum.ONLY_PORTALS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// Parse input params
		String searchTerm = this.keyValue.get(ReactorKeysEnum.FILTER_WORD.getKey());
		String limitStr = this.keyValue.getOrDefault(ReactorKeysEnum.LIMIT.getKey(), "20");
		String offset = this.keyValue.getOrDefault(ReactorKeysEnum.OFFSET.getKey(), "0");
		int totalLimit = Math.min(Integer.parseInt(limitStr), 20); // Cap at 20
		// Determine types to process
		List<String> type = this.getEngineTypes();
		List<String> typesToGet = (type == null || type.isEmpty())
				? Arrays.asList("PROJECT", "DATABASE", "STORAGE", "FUNCTION", "MODEL", "VECTOR")
				: new ArrayList<>(type);

		// Common filters
		Boolean noMeta = Boolean.parseBoolean(String.valueOf(this.keyValue.get(ReactorKeysEnum.NO_META.getKey())));
		Boolean portalsOnly = Boolean
				.parseBoolean(String.valueOf(this.keyValue.get(ReactorKeysEnum.ONLY_PORTALS.getKey())));
		Boolean favoritesOnly = Boolean
				.parseBoolean(String.valueOf(this.keyValue.get(ReactorKeysEnum.ONLY_FAVORITES.getKey())));
		List<Integer> permissionFilters = getPermissionFilters();
		Map<String, Object> engineProjectMetadataFilter = getMetaMap();

		// Distribute limit per type
		int typeCount = typesToGet.size();
		int baseLimit = totalLimit / typeCount;
		int remainder = totalLimit % typeCount;
		// Fetch data for each type
		List<Map<String, Object>> combinedInfo = new ArrayList<>();

		for (int i = 0; i < typesToGet.size(); i++) {
			String currentType = typesToGet.get(i);
			int limitForType = baseLimit + (i < remainder ? 1 : 0); // even distribution

			List<Map<String, Object>> result;
			if ("PROJECT".equalsIgnoreCase(currentType)) {
				result = getProjects(
						// List.of("CODE", "BLOCKS"),
						// for right now, do not apply filter on project type since it is not properly
						// in some smss files
						null,
						favoritesOnly,
						portalsOnly,
						engineProjectMetadataFilter,
						permissionFilters,
						searchTerm,
						String.valueOf(limitForType),
						offset);
			} else {
				result = getEngines(
						Collections.singletonList(currentType),
						noMeta,
						engineProjectMetadataFilter,
						permissionFilters,
						searchTerm,
						String.valueOf(limitForType),
						offset);

			}
			combinedInfo.addAll(result);
			if (combinedInfo.size() >= totalLimit) {
				break; // Optional safety to ensure total results do not exceed limit
			}
		}
		return new NounMetadata(combinedInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}

	/**
	 * 
	 * @param projectTypes
	 * @param favoritesOnly
	 * @param portalsOnly
	 * @param engineProjectMetadataFilter
	 * @param permissionFilters
	 * @param searchTerm
	 * @param limit
	 * @param offSet
	 * @return
	 */
	private List<Map<String, Object>> getProjects(List<String> projectTypes, boolean favoritesOnly, boolean portalsOnly,
			Map<String, Object> engineProjectMetadataFilter, List<Integer> permissionFilters, String searchTerm,
			String limit, String offSet) {
		List<String> projectIdFilters = getProjectIdFilters();
		List<Map<String, Object>> projectInfo = SecurityProjectUtils.getUserProjectList(this.insight.getUser(),
				projectTypes,
				projectIdFilters, favoritesOnly, portalsOnly, engineProjectMetadataFilter, permissionFilters,
				searchTerm, limit, offSet);
		return projectInfo;
	}

	/**
	 * 
	 * @param engineTypes
	 * @param noMeta
	 * @param engineProjectMetadataFilter
	 * @param permissionFilters
	 * @param searchTerm
	 * @param limit
	 * @param offSet
	 * @return
	 */
	private List<Map<String, Object>> getEngines(List<String> engineTypes, Boolean noMeta,
			Map<String, Object> engineProjectMetadataFilter, List<Integer> permissionFilters, String searchTerm,
			String limit, String offSet) {
		List<String> engineIdFilters = getEngineIdFilters();
		List<Map<String, Object>> engineInfo = SecurityEngineUtils.getUserEngineList(this.insight.getUser(),
				engineTypes, engineIdFilters, noMeta, engineProjectMetadataFilter, permissionFilters, searchTerm,
				limit, offSet);
		return engineInfo;
	}

	/**
	 *
	 * @return
	 */
	private List<String> getEngineIdFilters() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.ENGINE.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		return null;
	}

	/**
	 * 
	 * @return
	 */
	private List<String> getEngineTypes() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.TYPE.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		return null;
	}

	/**
	 *
	 * @return
	 */
	private List<String> getProjectIdFilters() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.PROJECT.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		return null;
	}

	/**
	 *
	 * @return
	 */
	private List<Integer> getPermissionFilters() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.PERMISSION_FILTERS.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllNumericColumnsAsInteger();
		}
		return null;
	}

	/**
	 *
	 * @return
	 */
	private Map<String, Object> getMetaMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.META_FILTERS.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.SORT.getKey())) {
			return "The sort is a string value containing either 'name' or 'date' for how to sort";
		} else if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "This is an optional engine filter";
		}
		return super.getDescriptionForKey(key);
	}
}