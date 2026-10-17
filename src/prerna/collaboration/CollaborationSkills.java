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
package prerna.collaboration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.agent.skill.CreateSkillReactor;
import prerna.reactor.agent.skill.SkillProjects;

/**
 * The owner's own skills for Collaboration: private skill projects the owner made, tagged collaboration. Only these
 * are offered to routines and the owner's assistant, so a shared or public skill never comes in by search.
 */
public final class CollaborationSkills {

	private static final Logger classLogger = LogManager.getLogger(CollaborationSkills.class);

	public static final String TAG = "collaboration";
	static final int MAX_CONTENT_CHARS = 20_000;

	private CollaborationSkills() {
	}

	/** Creates a private skill owned by the user and tags it for Collaboration. */
	public static Map<String, Object> create(User user, String name, String description, String content) {
		if (AbstractSecurityUtils.adminOnlySkillAdd() && !Boolean.TRUE.equals(SecurityAdminUtils.userIsAdmin(user))) {
			throw new IllegalArgumentException("Only an administrator can create skills on this server");
		}
		if (content == null || content.isBlank()) {
			throw new IllegalArgumentException("The skill needs its instructions");
		}
		if (content.length() > MAX_CONTENT_CHARS) {
			throw new IllegalArgumentException("A skill can be at most " + MAX_CONTENT_CHARS + " characters");
		}
		Map<String, Object> created = CreateSkillReactor.create(user, content, name, description);
		String skillId = String.valueOf(created.get("skill_id"));
		SecurityProjectUtils.updateProjectMetadata(skillId,
				Map.of("tag", List.of(ProjectHelper.SKILL_PROJECT_TAG, TAG)));
		return describe(skillId);
	}

	/** The user's own Collaboration skills, by name. */
	public static List<Map<String, Object>> mine(User user) {
		List<Map<String, Object>> skills = new ArrayList<>();
		for (String skillId : tagged()) {
			if (isMine(user, skillId)) {
				skills.add(describe(skillId));
			}
		}
		skills.sort((a, b) -> String.valueOf(a.get("name")).compareToIgnoreCase(String.valueOf(b.get("name"))));
		return skills;
	}

	/** Whether the user owns this skill and it is tagged for Collaboration. */
	public static boolean isMine(User user, String skillId) {
		if (user == null || skillId == null || skillId.isBlank() || !SkillProjects.isSkillProject(skillId)) {
			return false;
		}
		try {
			if (!SecurityProjectUtils.userIsOwner(user, skillId)) {
				return false;
			}
			Object tags = SecurityProjectUtils.getAggregateProjectMetadata(skillId, List.of("tag"), true).get("tag");
			return tags instanceof Collection<?> list ? list.contains(TAG) : TAG.equals(tags);
		} catch (RuntimeException e) {
			classLogger.warn("Could not check skill {}: {}", skillId, e.getMessage());
			return false;
		}
	}

	/** Keeps the ids the user owns and tagged, in order and without repeats; throws on any other. */
	public static List<String> requireMine(User user, Collection<?> skillIds) {
		Set<String> ids = new LinkedHashSet<>();
		if (skillIds != null) {
			for (Object value : skillIds) {
				String skillId = value == null ? "" : String.valueOf(value).trim();
				if (!isMine(user, skillId)) {
					throw new IllegalArgumentException("Pick one of your own Collaboration skills; " + skillId
							+ " is not one of them");
				}
				ids.add(skillId);
			}
		}
		if (ids.size() > 10) {
			throw new IllegalArgumentException("Use at most 10 skills");
		}
		return new ArrayList<>(ids);
	}

	/** Drops the ids the user no longer owns or untagged; used at run time, when nobody can be asked. */
	public static List<String> keepMine(User user, Collection<?> skillIds) {
		List<String> kept = new ArrayList<>();
		if (skillIds != null) {
			for (Object value : skillIds) {
				String skillId = value == null ? null : String.valueOf(value).trim();
				if (isMine(user, skillId) && !kept.contains(skillId)) {
					kept.add(skillId);
				}
			}
		}
		return kept;
	}

	static Map<String, Object> describe(String skillId) {
		SkillProjects.SkillInfo info = SkillProjects.resolve(skillId);
		Map<String, Object> skill = new LinkedHashMap<>();
		skill.put("id", skillId);
		skill.put("name", info.displayName);
		skill.put("description", info.description);
		return skill;
	}

	private static List<String> tagged() {
		return SecurityProjectUtils.getProjectIdsWithMetaValue("tag", TAG);
	}

}
