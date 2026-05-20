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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists skills the caller can see, with an optional scope filter.
 *
 * <p>Inputs (all optional):
 * <ul>
 *   <li>{@code filter} - one of {@code mine | platform | accessible}.
 *       Default {@code accessible}.
 *     <ul>
 *       <li>{@code mine}       - skills where {@code CREATED_BY = me}</li>
 *       <li>{@code platform}   - skills with {@code ORIGIN = 'PLATFORM'}</li>
 *       <li>{@code accessible} - union of the two (v1 rule). Once
 *           {@code SecuritySkillUtils} lands this will also include skills
 *           shared via {@code SKILLPERMISSION} / {@code GROUPSKILLPERMISSION}.</li>
 *     </ul>
 *   </li>
 *   <li>{@code includeArchived} - when true, {@code STATUS='ARCHIVED'} rows are
 *       returned. Default false.</li>
 * </ul>
 *
 * <p>Returns a list of skill maps; each map has the same shape as
 * {@code ModelInferenceLogsUtils.getSkillEntry}. Rows are sorted by
 * {@code DATE_UPDATED} desc.
 */
public class GetSkillsReactor extends AbstractReactor {

	private static final String FILTER          = "filter";
	private static final String INCLUDE_ARCHIVED = "includeArchived";

	private static final String FILTER_MINE       = "mine";
	private static final String FILTER_PLATFORM   = "platform";
	private static final String FILTER_ACCESSIBLE = "accessible";

	public GetSkillsReactor() {
		this.keysToGet = new String[] { FILTER, INCLUDE_ARCHIVED };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String filterInput = this.keyValue.get(FILTER);
		String filter = (filterInput == null || filterInput.isEmpty())
				? FILTER_ACCESSIBLE
				: filterInput.trim().toLowerCase();
		boolean includeArchived = parseBool(this.keyValue.get(INCLUDE_ARCHIVED), false);

		User user = this.insight.getUser();
		String userId = resolveUserId(user);

		List<Map<String, Object>> rows;
		switch (filter) {
			case FILTER_MINE:
				if (userId == null) {
					throw new IllegalArgumentException(
							"filter='mine' requires an authenticated user");
				}
				rows = ModelInferenceLogsUtils.listSkills(null, userId, includeArchived);
				break;
			case FILTER_PLATFORM:
				rows = ModelInferenceLogsUtils.listSkills(Skill.ORIGIN_PLATFORM, null, includeArchived);
				break;
			case FILTER_ACCESSIBLE:
				// v1 visibility = mine union platform. Once SecuritySkillUtils lands,
				// this will also union in SKILLPERMISSION / GROUPSKILLPERMISSION rows.
				List<Map<String, Object>> platform = ModelInferenceLogsUtils.listSkills(
						Skill.ORIGIN_PLATFORM, null, includeArchived);
				List<Map<String, Object>> mine = (userId == null)
						? new ArrayList<>()
						: ModelInferenceLogsUtils.listSkills(null, userId, includeArchived);
				rows = dedupBySkillId(platform, mine);
				break;
			default:
				throw new IllegalArgumentException(
						"Unknown filter '" + filterInput + "' - expected one of: mine, platform, accessible");
		}

		return new NounMetadata(rows, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	/**
	 * Returns the concatenation of the supplied lists with rows of duplicate
	 * {@code skill_id} dropped. Earlier lists win on duplicates so the natural
	 * "platform first, then mine" ordering is preserved.
	 */
	private static List<Map<String, Object>> dedupBySkillId(
			List<Map<String, Object>> first, List<Map<String, Object>> second) {
		Set<String> seen = new LinkedHashSet<>();
		List<Map<String, Object>> out = new ArrayList<>(first.size() + second.size());
		for (Map<String, Object> row : first) {
			String id = stringOf(row.get("skill_id"));
			if (id != null && seen.add(id)) {
				out.add(row);
			}
		}
		for (Map<String, Object> row : second) {
			String id = stringOf(row.get("skill_id"));
			if (id != null && seen.add(id)) {
				out.add(row);
			}
		}
		return out;
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

	private static boolean parseBool(String s, boolean defaultValue) {
		if (s == null || s.isEmpty()) {
			return defaultValue;
		}
		return Boolean.parseBoolean(s);
	}

	@Override
	public String getReactorDescription() {
		return "Lists skills the caller can see. Filter scopes: mine | platform | accessible (default).";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (FILTER.equals(key)) {
			return "Scope filter: 'mine' (skills I created), 'platform' (platform skills), "
					+ "or 'accessible' (default - union of mine + platform; later includes shared)";
		}
		if (INCLUDE_ARCHIVED.equals(key)) {
			return "When true, include skills with STATUS='ARCHIVED'. Default false.";
		}
		return super.getDescriptionForKey(key);
	}
}
