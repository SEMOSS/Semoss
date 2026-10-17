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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import prerna.reactor.agent.skill.DeleteSkillReactor;
import prerna.reactor.agent.skill.SkillProjects;

/**
 * The owner's own skills for Collaboration: private skill projects the owner made, tagged collaboration. Only these
 * are offered to routines and the owner's assistant, so a shared or public skill never comes in by search.
 */
public final class CollaborationSkills {

	private static final Logger classLogger = LogManager.getLogger(CollaborationSkills.class);

	public static final String TAG = "collaboration";
	static final int MAX_READ_CHARS = 30_000;

	private CollaborationSkills() {
	}

	/**
	 * Creates a private skill owned by the user, tagged for Collaboration, with a placeholder SKILL.md the skill
	 * builder then writes.
	 */
	public static Map<String, Object> create(User user, String name, String description) {
		if (AbstractSecurityUtils.adminOnlySkillAdd() && !Boolean.TRUE.equals(SecurityAdminUtils.userIsAdmin(user))) {
			throw new IllegalArgumentException("Only an administrator can create skills on this server");
		}
		if (name == null || name.isBlank() || description == null || description.isBlank()) {
			throw new IllegalArgumentException("A new skill needs a name and a description");
		}
		Map<String, Object> created = CreateSkillReactor.create(user,
				"# " + name.trim() + "\n\n" + description.trim() + "\n", name.trim(), description.trim());
		String skillId = String.valueOf(created.get("skill_id"));
		SecurityProjectUtils.updateProjectMetadata(skillId,
				Map.of("tag", List.of(ProjectHelper.SKILL_PROJECT_TAG, TAG)));
		return describe(skillId);
	}

	/** The files of one of the user's own skills, as paths relative to its folder with their sizes. */
	public static List<Map<String, Object>> files(User user, String skillId) {
		Path dir = requireDir(user, skillId);
		List<Map<String, Object>> files = new ArrayList<>();
		for (String path : CollaborationSkillBuilder.snapshot(dir).keySet()) {
			Map<String, Object> file = new LinkedHashMap<>();
			file.put("path", path);
			try {
				file.put("bytes", Files.size(dir.resolve(path)));
			} catch (IOException e) {
				file.put("bytes", null);
			}
			files.add(file);
		}
		return files;
	}

	/** One text file of one of the user's own skills, cut at the read limit. */
	public static Map<String, Object> read(User user, String skillId, String path) {
		Path dir = requireDir(user, skillId);
		Path file = dir.resolve(path == null ? "" : path.trim()).normalize();
		if (!file.startsWith(dir) || !Files.isRegularFile(file) || dir.relativize(file).toString().startsWith(".")) {
			throw new IllegalArgumentException("The skill has no file " + path + "; list its files first");
		}
		try {
			String text = Files.readString(file, StandardCharsets.UTF_8);
			Map<String, Object> out = new LinkedHashMap<>();
			out.put("path", dir.relativize(file).toString().replace('\\', '/'));
			out.put("truncated", text.length() > MAX_READ_CHARS);
			out.put("content", text.length() > MAX_READ_CHARS ? text.substring(0, MAX_READ_CHARS) : text);
			return out;
		} catch (IOException | RuntimeException e) {
			throw new IllegalArgumentException("Could not read " + path + " as text: " + e.getMessage());
		}
	}

	/** Deletes one of the user's own skills; routines and the assistant drop it on their next run. */
	public static void delete(User user, String skillId) {
		if (!isMine(user, skillId)) {
			throw new IllegalArgumentException("Pick one of your own Collaboration skills; " + skillId
					+ " is not one of them");
		}
		if (AbstractSecurityUtils.adminOnlySkillDelete() && !Boolean.TRUE.equals(SecurityAdminUtils.userIsAdmin(user))) {
			throw new IllegalArgumentException("Only an administrator can delete skills on this server");
		}
		DeleteSkillReactor.delete(user, skillId);
	}

	private static Path requireDir(User user, String skillId) {
		if (!isMine(user, skillId)) {
			throw new IllegalArgumentException("Pick one of your own Collaboration skills; " + skillId
					+ " is not one of them");
		}
		Path dir = SkillProjects.resolve(skillId).skillDir;
		if (dir == null) {
			throw new IllegalStateException("The skill has no files yet");
		}
		return dir.normalize();
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
