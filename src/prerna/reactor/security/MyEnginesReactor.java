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
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserCatalogVoteUtils;
import prerna.util.Utility;

public class MyEnginesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MyEnginesReactor.class);

	public MyEnginesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILTER_WORD.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.ONLY_FAVORITES.getKey(),
				ReactorKeysEnum.ENGINE_TYPE.getKey(), ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.PERMISSION_FILTERS.getKey(), ReactorKeysEnum.META_KEYS.getKey(),
				ReactorKeysEnum.META_FILTERS.getKey(), ReactorKeysEnum.NO_META.getKey(),
				ReactorKeysEnum.INCLUDE_USERTRACKING_KEY.getKey(), ReactorKeysEnum.SORT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		String searchTerm = getString(ReactorKeysEnum.FILTER_WORD.getKey());
		String limit = getString(ReactorKeysEnum.LIMIT.getKey());
		String offset = getString(ReactorKeysEnum.OFFSET.getKey());
		boolean favoritesOnly = getBoolean(ReactorKeysEnum.ONLY_FAVORITES.getKey(), false);
		List<String> engineTypes = getListString(ReactorKeysEnum.ENGINE_TYPE.getKey());
		List<String> engineIdFilters = getListString(ReactorKeysEnum.ENGINE.getKey());
		Map<String, String> sortFields = getMap(ReactorKeysEnum.SORT.getKey());
		boolean noMeta = getBoolean(ReactorKeysEnum.NO_META.getKey(), false);
		List<Integer> permissionFilters = getListInteger(ReactorKeysEnum.PERMISSION_FILTERS.getKey());
		boolean includeUserT = getBoolean(ReactorKeysEnum.INCLUDE_USERTRACKING_KEY.getKey(), false);
		Map<String, Object> engineMetadataFilter = getMap(ReactorKeysEnum.META_FILTERS.getKey());

		List<Map<String, Object>> engineInfo = SecurityEngineUtils.getUserEngineList(this.insight.getUser(),
				engineTypes, engineIdFilters, favoritesOnly, engineMetadataFilter, permissionFilters, searchTerm, limit,
				offset, sortFields);

		if (!engineInfo.isEmpty() && (!noMeta || includeUserT)) {
			Map<String, Integer> index = new HashMap<>(engineInfo.size());
			int size = engineInfo.size();
			for (int i = 0; i < size; i++) {
				Map<String, Object> engine = engineInfo.get(i);
				String engineId = engine.get("database_id").toString();
				// keep list of database ids to get the index
				index.put(engineId, Integer.valueOf(i));
			}

			if (!noMeta) {
				IRawSelectWrapper wrapper = null;
				try {
					wrapper = SecurityEngineUtils.getEngineMetadataWrapper(index.keySet(),
							getListString(ReactorKeysEnum.META_KEYS.getKey()), true);
					while (wrapper.hasNext()) {
						Object[] data = wrapper.next().getValues();
						String databaseId = (String) data[0];

						String metaKey = (String) data[1];
						String metaValue = (String) data[2];
						if (metaValue == null) {
							continue;
						}

						int indexToFind = index.get(databaseId);
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
					classLogger.error("Error retrieving engine metadata in MyEnginesReactor", e);
				} finally {
					if (wrapper != null) {
						try {
							wrapper.close();
						} catch (IOException e) {
							classLogger.error("Error closing engine metadata wrapper in MyEnginesReactor", e);
						}
					}
				}
			}
			if (includeUserT && Utility.isUserTrackingEnabled()) {
				try (IRawSelectWrapper wrapper = UserCatalogVoteUtils.getAllVotesWrapper(index.keySet())) {
					while (wrapper.hasNext()) {
						Object[] data = wrapper.next().getValues();
						String databaseId = (String) data[0];
						int upvotes = ((Number) data[1]).intValue();

						int indexToFind = index.get(databaseId);
						Map<String, Object> res = engineInfo.get(indexToFind);
						res.put("upvotes", upvotes);
					}
				} catch (Exception e) {
					classLogger.error("Error retrieving engine vote totals in MyEnginesReactor", e);
				}

				Map<String, Boolean> voted = UserCatalogVoteUtils
						.userEngineVotes(User.getUserIdAndType(this.insight.getUser()), index.keySet());
				for (String ks : index.keySet()) {
					int indexToFind = index.get(ks);
					Boolean hasUpvoted = voted.get(ks);
					if (hasUpvoted == null) {
						hasUpvoted = false;
					}
					engineInfo.get(indexToFind).put("hasUpvoted", hasUpvoted);
				}
			}
		}

		return new NounMetadata(engineInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}

	@Override
	public String getReactorDescription() {
		return """
				Returns a list of engines that the user has access to.

				Inputs: filterWord, limit, offset, onlyFavorites, engineType, engine, permissionFilters, metaKeys, metaFilters, noMeta, includeUserTracking, sort.
				Response keys: prefer engine_* fields (engine_id, engine_name, engine_display_name, engine_type, engine_subtype, engine_cost, engine_discoverable, engine_global, engine_tool_app, engine_created_by, engine_created_by_type, engine_date_created, low_engine_name, engine_user_permission, engine_group_permission, engine_favorite).
				Any response key prefixed with app_* or database_* is legacy and should not be used.
				""";
	}

	@Override
	public JSONObject getResponseSchema() {
		JSONObject schema = new JSONObject();
		schema.put("type", "array");
		schema.put("description", "List of engine objects the user has access to");

		JSONObject itemProperties = new JSONObject();
		itemProperties.put("engine_id",
				new JSONObject().put("type", "string").put("description", "Unique engine identifier (UUID)"));
		itemProperties.put("engine_name", new JSONObject().put("type", "string").put("description", "Engine name"));
		itemProperties.put("engine_display_name",
				new JSONObject().put("type", "string").put("description", "Friendly display name for the engine"));
		itemProperties.put("engine_type",
				new JSONObject().put("type", "string").put("description", "Catalog type for the engine"));
		itemProperties.put("engine_subtype",
				new JSONObject().put("type", "string").put("description", "Catalog subtype for the engine"));
		itemProperties.put("engine_cost", new JSONObject().put("type", "string").put("description", "Cost metadata"));
		itemProperties.put("engine_discoverable",
				new JSONObject().put("type", "boolean").put("description", "Whether the engine is discoverable"));
		itemProperties.put("engine_global", new JSONObject().put("type", "boolean").put("description",
				"Whether the engine is globally accessible"));
		itemProperties.put("engine_tool_app",
				new JSONObject().put("type", "string").put("description", "Linked tool app project id (if set)"));
		itemProperties.put("engine_created_by",
				new JSONObject().put("type", "string").put("description", "Creator user id"));
		itemProperties.put("engine_created_by_type",
				new JSONObject().put("type", "string").put("description", "Creator auth type"));
		itemProperties.put("engine_date_created", new JSONObject().put("type", "string").put("format", "datetime")
				.put("description", "UTC timestamp when the engine was created"));
		itemProperties.put("low_engine_name",
				new JSONObject().put("type", "string").put("description", "Lowercase engine name used for sorting"));
		itemProperties.put("engine_user_permission", new JSONObject().put("type", "integer").put("description",
				"Direct user permission level for this engine"));
		itemProperties.put("engine_group_permission", new JSONObject().put("type", "integer").put("description",
				"Group-derived permission level for this engine"));
		itemProperties.put("engine_favorite", new JSONObject().put("type", "integer")
				.put("enum", new JSONArray().put(0).put(1)).put("description", "1 if favorited, otherwise 0"));
		itemProperties.put("permission", new JSONObject().put("type", "integer").put("description",
				"Effective permission level for this engine"));
		itemProperties.put("upvotes",
				new JSONObject().put("type", "integer").put("description", "Catalog upvote count when requested"));
		itemProperties.put("hasUpvoted", new JSONObject().put("type", "boolean").put("description",
				"Whether the current user has upvoted when requested"));

		// legacy aliases
		itemProperties.put("app_id",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_id"));
		itemProperties.put("app_name",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_name"));
		itemProperties.put("app_display_name",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_display_name"));
		itemProperties.put("app_type",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_type"));
		itemProperties.put("app_subtype",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_subtype"));
		itemProperties.put("app_cost",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_cost"));
		itemProperties.put("app_favorite",
				new JSONObject().put("type", "integer").put("description", "Legacy alias of engine_favorite"));
		itemProperties.put("database_id",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_id"));
		itemProperties.put("database_name",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_name"));
		itemProperties.put("database_type",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_type"));
		itemProperties.put("database_subtype",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_subtype"));
		itemProperties.put("database_cost",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_cost"));
		itemProperties.put("database_discoverable",
				new JSONObject().put("type", "boolean").put("description", "Legacy alias of engine_discoverable"));
		itemProperties.put("database_global",
				new JSONObject().put("type", "boolean").put("description", "Legacy alias of engine_global"));
		itemProperties.put("database_created_by",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_created_by"));
		itemProperties.put("database_created_by_type",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_created_by_type"));
		itemProperties.put("database_date_created",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_date_created"));
		itemProperties.put("low_database_name",
				new JSONObject().put("type", "string").put("description", "Legacy alias of low_engine_name"));
		itemProperties.put("database_favorite",
				new JSONObject().put("type", "integer").put("description", "Legacy alias of engine_favorite"));
		itemProperties.put("user_permission",
				new JSONObject().put("type", "integer").put("description", "Legacy alias of engine_user_permission"));
		itemProperties.put("group_permission",
				new JSONObject().put("type", "integer").put("description", "Legacy alias of engine_group_permission"));
		itemProperties.put("tool_app",
				new JSONObject().put("type", "string").put("description", "Legacy alias of engine_tool_app"));

		JSONObject items = new JSONObject();
		items.put("type", "object");
		items.put("properties", itemProperties);
		schema.put("items", items);
		return schema;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.SORT.getKey())) {
			return "The sort is a map with key and direction. Supported keys are 'ENGINENAME' and 'DATECREATED'. Use values like 'ASC' or 'DESC'. 'ENGINENAME' sorting is case-insensitive.";
		} else if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "This is an optional engine filter";
		}
		return super.getDescriptionForKey(key);
	}

}
