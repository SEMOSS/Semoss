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
package prerna.reactor.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MyProjectsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MyProjectsReactor.class);

	public MyProjectsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILTER_WORD.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.ONLY_FAVORITES.getKey(),
				ReactorKeysEnum.META_KEYS.getKey(), ReactorKeysEnum.META_FILTERS.getKey(),
				ReactorKeysEnum.PERMISSION_FILTERS.getKey(), ReactorKeysEnum.NO_META.getKey(),
				ReactorKeysEnum.ONLY_PORTALS.getKey(), ReactorKeysEnum.INCLUDE_USERTRACKING_KEY.getKey(),
				ReactorKeysEnum.SORT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		String searchTerm = getString(ReactorKeysEnum.FILTER_WORD.getKey());
		String limit = getString(ReactorKeysEnum.LIMIT.getKey());
		String offset = getString(ReactorKeysEnum.OFFSET.getKey());
		List<String> projectTypeFilters = getListString(ReactorKeysEnum.TYPE.getKey());
		List<String> projectIdFilters = getListString(ReactorKeysEnum.PROJECT.getKey());
		boolean favoritesOnly = getBoolean(ReactorKeysEnum.ONLY_FAVORITES.getKey(), false);
		boolean noMeta = getBoolean(ReactorKeysEnum.NO_META.getKey(), false);
		boolean portalsOnly = getBoolean(ReactorKeysEnum.ONLY_PORTALS.getKey(), false);
		List<Integer> permissionFilters = getListInteger(ReactorKeysEnum.PERMISSION_FILTERS.getKey());
		boolean includeUserT = getBoolean(ReactorKeysEnum.INCLUDE_USERTRACKING_KEY.getKey(), false);
		Map<String, Object> projectMetadataFilter = getMap(ReactorKeysEnum.META_FILTERS.getKey());
		Map<String, String> sortFields = getMap(ReactorKeysEnum.SORT.getKey());

		// for right now, do not apply filter on project type since it is not properly
		// in some smss files
		List<Map<String, Object>> projectInfo = SecurityProjectUtils.getUserProjectList(this.insight.getUser(),
				projectTypeFilters, projectIdFilters, favoritesOnly, portalsOnly, projectMetadataFilter,
				permissionFilters, searchTerm, limit, offset, sortFields);

		if (!projectInfo.isEmpty() && (!noMeta || includeUserT)) {
			Map<String, Integer> index = new HashMap<>(projectInfo.size());
			int size = projectInfo.size();
			// now we want to add most executed insights
			for (int i = 0; i < size; i++) {
				Map<String, Object> project = projectInfo.get(i);
				String projectId = project.get("project_id").toString();
				// keep list of project ids to get the index
				index.put(projectId, Integer.valueOf(i));
			}

			if (!noMeta) {
				IRawSelectWrapper wrapper = null;
				try {
					wrapper = SecurityProjectUtils.getProjectMetadataWrapper(index.keySet(),
							getListString(ReactorKeysEnum.META_KEYS.getKey()), true);
					while (wrapper.hasNext()) {
						Object[] data = wrapper.next().getValues();
						String projectId = (String) data[0];

						String metaKey = (String) data[1];
						String metaValue = (String) data[2];
						if (metaValue == null) {
							continue;
						}

						int indexToFind = index.get(projectId);
						Map<String, Object> res = projectInfo.get(indexToFind);
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
					classLogger.error("Failed to attach project metadata values to the project list response", e);
				} finally {
					if (wrapper != null) {
						try {
							wrapper.close();
						} catch (IOException e) {
							classLogger.error("Failed to close metadata wrapper while building project list response",
									e);
						}
					}
				}
			}
//			if(includeUserT) {
//				IRawSelectWrapper wrapper = null;
//				try {
//					wrapper = UserCatalogVoteUtils.getAllVotesWrapper(index.keySet());
//					while(wrapper.hasNext()) {
//						Object[] data = wrapper.next().getValues();
//						String databaseId = (String) data[0];
//						int upvotes = ((Number) data[1]).intValue();
//		
//						int indexToFind = index.get(databaseId);
//						Map<String, Object> res = projectInfo.get(indexToFind);
//						res.put("upvotes", upvotes);
//					}
//				} catch (Exception e) {
//					classLogger.error("Failed to attach vote totals to the project list response", e);
//				} finally {
//					if(wrapper!=null) {
//						try {
//							wrapper.close();
//						} catch (IOException e) {
//							classLogger.error("Failed to close vote wrapper while building project list response", e);
//						}
//					}
//				}
//				
//				Map<String, Boolean> voted = UserCatalogVoteUtils.userEngineVotes(User.getUserIdAndType(this.insight.getUser()), index.keySet());
//				for (String ks : index.keySet()) {
//					int indexToFind = index.get(ks);
//					Boolean hasUpvoted = voted.get(ks);
//					if (hasUpvoted == null) {
//						hasUpvoted = false;
//					}
//					engineInfo.get(indexToFind).put("hasUpvoted", hasUpvoted);
//				}
//			}
		}

		return new NounMetadata(projectInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}

	@Override
	public String getReactorDescription() {
		return "Returns a list of projects that the user has access to.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.SORT.getKey())) {
			return "The sort is a map with key and direction. Supported keys are 'PROJECTNAME', 'DATECREATED', and 'DATELASTEDITED' (or 'DATE_LAST_EDITED').";
		} else if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "This is an optional project filter";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public JSONObject getResponseSchema() {
		JSONObject schema = new JSONObject();
		schema.put("type", "array");
		schema.put("description", "List of project objects the user has access to");

		JSONObject itemProperties = new JSONObject();

		itemProperties.put("project_id",
				new JSONObject().put("type", "string").put("description", "Unique project identifier (UUID)"));

		itemProperties.put("project_name",
				new JSONObject().put("type", "string").put("description", "Display name of the project"));

		itemProperties.put("project_type", new JSONObject().put("type", "string")
				.put("enum", new JSONArray().put("CODE").put("INSIGHTS")).put("description", "The type of project"));

		itemProperties.put("project_date_created", new JSONObject().put("type", "string").put("format", "datetime")
				.put("description", "ISO datetime when project was created"));

		itemProperties.put("project_date_last_edited", new JSONObject().put("type", "string").put("format", "datetime")
				.put("description", "ISO datetime when project was last modified"));

		itemProperties.put("project_created_by",
				new JSONObject().put("type", "string").put("description", "Username of the project creator"));

		itemProperties.put("project_created_by_type",
				new JSONObject().put("type", "string").put("description", "Auth type of creator, e.g. NATIVE"));

		itemProperties.put("permission",
				new JSONObject().put("type", "integer").put("enum", new JSONArray().put(1).put(2).put(3))
						.put("description", "User's permission level: 1=Owner, 2=Editor, 3=ReadOnly"));

		itemProperties.put("user_permission",
				new JSONObject().put("type", "integer").put("description", "Same as permission"));

		itemProperties.put("project_has_portal", new JSONObject().put("type", "boolean").put("description",
				"Whether the project has a portal attached"));

		itemProperties.put("project_portal_name",
				new JSONObject().put("type", "string").put("description", "Name of the portal, empty string if none"));

		itemProperties.put("project_discoverable", new JSONObject().put("type", "boolean").put("description",
				"Whether the project is discoverable by other users"));

		itemProperties.put("project_global", new JSONObject().put("type", "boolean").put("description",
				"Whether the project is globally accessible"));

		itemProperties.put("project_favorite",
				new JSONObject().put("type", "integer").put("enum", new JSONArray().put(0).put(1)).put("description",
						"1 if user has favorited this project, 0 otherwise"));

		itemProperties.put("project_cost",
				new JSONObject().put("type", "string").put("description", "Cost metadata, may be empty string"));

		itemProperties.put("tag",
				new JSONObject().put("type", "string").put("description", "Metadata tag associated with the project"));

		itemProperties.put("low_project_name",
				new JSONObject().put("type", "string").put("description", "Lowercase version of project_name"));

		JSONObject items = new JSONObject();
		items.put("type", "object");
		items.put("properties", itemProperties);

		schema.put("items", items);
		return schema;
	}
}
