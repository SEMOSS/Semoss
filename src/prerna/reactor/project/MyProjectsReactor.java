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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.skill.PlatformSkills;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MyProjectsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MyProjectsReactor.class);

	/**
	 * Value of the {@code type} input (PROJECT.TYPE) that identifies a skill listing,
	 * and the discriminator written onto project-backed skill entries in the response.
	 */
	private static final String SKILL_TYPE = "SKILL";

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

		boolean skillListing = projectTypeFilters != null && projectTypeFilters.contains(SKILL_TYPE);

		List<Map<String, Object>> projectInfo;
		if (skillListing) {
			projectInfo = buildSkillListing(searchTerm, limit, offset, projectTypeFilters, projectIdFilters,
					favoritesOnly, portalsOnly, projectMetadataFilter, permissionFilters, noMeta, includeUserT,
					sortFields);
		} else {
			// for right now, do not apply filter on project type since it is not properly
			// in some smss files
			projectInfo = SecurityProjectUtils.getUserProjectList(this.insight.getUser(), projectTypeFilters,
					projectIdFilters, favoritesOnly, portalsOnly, projectMetadataFilter, permissionFilters, searchTerm,
					limit, offset, sortFields);
			attachProjectMetadata(projectInfo, noMeta, includeUserT);
		}

		return new NounMetadata(projectInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}

	/**
	 * Attaches the requested {@code metaKeys} metadata to each project-backed entry.
	 * Keyed strictly by {@code project_id}, so entries that lack one (platform skills)
	 * must never be passed in here - the caller merges them only after this runs.
	 */
	private void attachProjectMetadata(List<Map<String, Object>> projectInfo, boolean noMeta, boolean includeUserT) {
		if (projectInfo.isEmpty() || (noMeta && !includeUserT)) {
			return;
		}
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
						classLogger.error("Failed to close metadata wrapper while building project list response", e);
					}
				}
			}
		}
	}

	/**
	 * Builds a SKILL listing: the project-backed (registry) skills the user can view,
	 * merged with the read-only platform skills, then sorted and paged in Java.
	 *
	 * <p>Registry skills are fetched unpaged (limit/offset/sort passed as null) so the
	 * merge with platform skills can be paged as one set. Every entry is tagged with a
	 * {@code type} discriminator: {@code SKILL} for project-backed skills,
	 * {@link PlatformSkills#PLATFORM_SKILL_TYPE} for platform skills. Platform skills
	 * have no {@code project_id}, permissions, favorites, or project metadata, so they
	 * are skipped whenever a filter that only they cannot satisfy is set.
	 */
	private List<Map<String, Object>> buildSkillListing(String searchTerm, String limit, String offset,
			List<String> projectTypeFilters, List<String> projectIdFilters, boolean favoritesOnly, boolean portalsOnly,
			Map<String, Object> projectMetadataFilter, List<Integer> permissionFilters, boolean noMeta,
			boolean includeUserT, Map<String, String> sortFields) {
		// registry skills, unpaged - Java applies sort/limit/offset over the union below
		List<Map<String, Object>> registry = SecurityProjectUtils.getUserProjectList(this.insight.getUser(),
				projectTypeFilters, projectIdFilters, favoritesOnly, portalsOnly, projectMetadataFilter,
				permissionFilters, searchTerm, null, null, null);
		attachProjectMetadata(registry, noMeta, includeUserT);
		for (Map<String, Object> row : registry) {
			row.put("type", SKILL_TYPE);
		}

		List<Map<String, Object>> merged = new ArrayList<>(registry);

		boolean skipPlatform = favoritesOnly || (permissionFilters != null && !permissionFilters.isEmpty())
				|| (projectMetadataFilter != null && !projectMetadataFilter.isEmpty());
		if (skipPlatform) {
			classLogger.debug(
					"MyProjects: omitting platform skills from SKILL listing because a favorites/permission/metadata filter is set - platform skills have none of these.");
		} else {
			String term = (searchTerm == null) ? null : searchTerm.trim().toLowerCase();
			for (Map<String, Object> ps : PlatformSkills.list()) {
				String name = stringOf(ps.get("name"));
				String description = stringOf(ps.get("description"));
				// replicate the SQL filterWord LIKE match in Java (name + description)
				if (term != null && !term.isEmpty()) {
					String hay = ((name == null ? "" : name) + " " + (description == null ? "" : description))
							.toLowerCase();
					if (!hay.contains(term)) {
						continue;
					}
				}
				Map<String, Object> entry = new LinkedHashMap<>();
				entry.put("type", PlatformSkills.PLATFORM_SKILL_TYPE);
				entry.put("slug", ps.get("slug"));
				entry.put("project_name", name);
				entry.put("project_description", description);
				entry.put("origin", ps.get("origin"));
				merged.add(entry);
			}
		}

		sortListing(merged, sortFields);
		return page(merged, limit, offset);
	}

	/**
	 * Sorts the merged listing in place by the first entry of {@code sortFields}
	 * (defaulting to {@code project_name} ASC). Missing values (e.g. platform skills
	 * have no dates) sort last regardless of direction.
	 */
	private static void sortListing(List<Map<String, Object>> rows, Map<String, String> sortFields) {
		String field = "project_name";
		boolean asc = true;
		if (sortFields != null && !sortFields.isEmpty()) {
			Map.Entry<String, String> e = sortFields.entrySet().iterator().next();
			field = mapSortKey(e.getKey());
			asc = !"DESC".equalsIgnoreCase(e.getValue());
		}
		final String sortField = field;
		final int dir = asc ? 1 : -1;
		rows.sort((a, b) -> {
			String sa = stringOf(a.get(sortField));
			String sb = stringOf(b.get(sortField));
			if (sa == null && sb == null) {
				return 0;
			}
			if (sa == null) {
				return 1;
			}
			if (sb == null) {
				return -1;
			}
			return dir * sa.compareToIgnoreCase(sb);
		});
	}

	/** Maps a sort key (as accepted by getUserProjectList) to its response-map field. */
	private static String mapSortKey(String key) {
		if (key == null) {
			return "project_name";
		}
		switch (key.toUpperCase()) {
			case "DATECREATED":
				return "project_date_created";
			case "DATELASTEDITED":
			case "DATE_LAST_EDITED":
				return "project_date_last_edited";
			case "PROJECTNAME":
			default:
				return "project_name";
		}
	}

	/** Applies offset/limit in Java over the merged listing. */
	private static List<Map<String, Object>> page(List<Map<String, Object>> rows, String limit, String offset) {
		int off = parseInt(offset, 0);
		if (off < 0) {
			off = 0;
		}
		if (off >= rows.size()) {
			return new ArrayList<>();
		}
		int lim = parseInt(limit, -1);
		int end = (lim < 0) ? rows.size() : Math.min(rows.size(), off + lim);
		return new ArrayList<>(rows.subList(off, end));
	}

	private static int parseInt(String s, int def) {
		if (s == null || s.trim().isEmpty()) {
			return def;
		}
		try {
			return Integer.parseInt(s.trim());
		} catch (NumberFormatException e) {
			return def;
		}
	}

	private static String stringOf(Object o) {
		return o == null ? null : o.toString();
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
				.put("enum", new JSONArray().put("CODE").put("INSIGHTS").put("SKILL"))
				.put("description", "The type of project"));

		itemProperties.put("type", new JSONObject().put("type", "string")
				.put("enum", new JSONArray().put("SKILL").put("PLATFORM_SKILL"))
				.put("description",
						"Only present in a SKILL listing (type=\"SKILL\"). 'SKILL' is a project-backed registry skill (has project_id); "
								+ "'PLATFORM_SKILL' is a read-only disk-backed platform skill (no project_id; carries slug and origin=PLATFORM)."));

		itemProperties.put("slug", new JSONObject().put("type", "string").put("description",
				"Platform-skill folder name. Present only on PLATFORM_SKILL entries."));

		itemProperties.put("origin", new JSONObject().put("type", "string").put("description",
				"Skill origin, e.g. PLATFORM. Present on platform-skill entries in a SKILL listing."));

		itemProperties.put("project_description", new JSONObject().put("type", "string")
				.put("description", "Project/skill description from metadata; may be empty string"));

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
