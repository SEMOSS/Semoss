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
package prerna.reactor.agent.skill;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists skills the caller can see, with an optional scope filter.
 *
 * <p>Skills are Projects of type {@code SKILL}. Access is determined entirely by
 * project permissions: the {@code accessible} filter returns every skill-project
 * the user can view via {@code PROJECTUSER} / {@code GROUPPROJECTPERMISSION} (the
 * same machinery that gates workspaces).
 *
 * <p>Inputs (all optional):
 * <ul>
 *   <li>{@code filter} - one of {@code mine | platform | accessible}.
 *       Default {@code accessible}.
 *     <ul>
 *       <li>{@code mine}       - skills where {@code CREATED_BY = me}</li>
 *       <li>{@code platform}   - built-in platform skills (disk-backed folders under
 *           {@code <BASE_FOLDER>/skills/}; keyed by slug, no {@code skill_id})</li>
 *       <li>{@code accessible} - every skill-project the user can view, plus all
 *           platform skills (this is the default when no {@code filter} is given)</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>Returns a list of skill maps; each map has the same shape as
 * {@code ModelInferenceLogsUtils.getSkillEntry}. Rows are sorted by
 * {@code DATE_UPDATED} desc.
 */
public class GetSkillsReactor extends AbstractReactor {

	private static final String FILTER = "filter";

	private static final String FILTER_MINE       = "mine";
	private static final String FILTER_PLATFORM   = "platform";
	private static final String FILTER_ACCESSIBLE = "accessible";

	public GetSkillsReactor() {
		this.keysToGet = new String[] { FILTER };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String filterInput = this.keyValue.get(FILTER);
		String filter = (filterInput == null || filterInput.isEmpty())
				? FILTER_ACCESSIBLE
				: filterInput.trim().toLowerCase();

		User user = this.insight.getUser();
		String userId = resolveUserId(user);

		List<Map<String, Object>> rows;
		switch (filter) {
			case FILTER_MINE:
				if (userId == null) {
					throw new IllegalArgumentException("filter='mine' requires an authenticated user");
				}
				rows = ModelInferenceLogsUtils.listSkills(null, userId);
				break;
			case FILTER_PLATFORM:
				// Platform skills are disk-backed folders under <BASE_FOLDER>/skills/,
				// not SKILL__ rows - enumerate them from disk (no skill_id; keyed by slug).
				rows = PlatformSkills.list();
				break;
			case FILTER_ACCESSIBLE:
				Set<String> visibleProjectIds = getVisibleSkillProjectIds(user);
				List<Map<String, Object>> all = ModelInferenceLogsUtils.listSkills(null, null);
				rows = new ArrayList<>(all.size());
				for (Map<String, Object> row : all) {
					String skillId = stringOf(row.get("skill_id"));
					if (skillId != null && visibleProjectIds.contains(skillId)) {
						rows.add(row);
					}
				}
				// Platform skills are global, read-only built-ins available to everyone,
				// so they are always part of the accessible set (origin=PLATFORM, keyed by slug).
				rows.addAll(PlatformSkills.list());
				break;
			default:
				throw new IllegalArgumentException(
						"Unknown filter '" + filterInput + "' - expected one of: mine, platform, accessible");
		}

		return new NounMetadata(rows, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	/**
	 * Returns the set of project ids the user can view that are tagged
	 * {@code Skill_Project}. Mirrors the pattern used by
	 * {@code ListWorkspacesReactor} for workspace projects.
	 */
	private static Set<String> getVisibleSkillProjectIds(User user) {
		Map<String, Object> projectMetadataFilter = new HashMap<>();
		projectMetadataFilter.put("tag", ModelInferenceLogsUtils.SKILL_PROJECT_TAG);
		List<Map<String, Object>> projectInfo = SecurityProjectUtils.getUserProjectList(user, null, null, false, false,
				projectMetadataFilter, null, null, null, null);
		Set<String> ids = new HashSet<>();
		for (Map<String, Object> project : projectInfo) {
			Object id = project.get("project_id");
			if (id != null) {
				ids.add(id.toString());
			}
		}
		return ids;
	}

	private static String stringOf(Object o) {
		return o == null ? null : o.toString();
	}

	private static String resolveUserId(User user) {
		if (user == null || user.getLogins() == null || user.getLogins().isEmpty()) {
			return null;
		}
		AuthProvider login = user.getLogins().get(0);
		return user.getAccessToken(login) == null ? null : user.getAccessToken(login).getId();
	}

	@Override
	public String getReactorDescription() {
		return "Lists skills the caller can see. Filter scopes: mine | platform | accessible (default).";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (FILTER.equals(key)) {
			return "Scope filter: 'mine' (skills I created), 'platform' (platform skills), "
					+ "or 'accessible' (default - every skill-project I can view, plus all platform skills)";
		}
		return super.getDescriptionForKey(key);
	}
}
