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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.skill.SkillScanner.DiscoveredSkill;
import prerna.reactor.agent.skill.SkillScanner.SkillFile;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Lists every skill discovered on disk under the conventional skill-host directories of a working
 * directory, deduplicated by name with the same first-match-wins precedence as the
 * {@code LoadSkill} tool.
 *
 * <p>The directory scanned is chosen, in order:
 * <ol>
 *   <li>{@code projectId} (optional) - the project's assets folder
 *       ({@code AssetUtility.getProjectAssetsFolder}). Requires that the caller can view the
 *       project ({@code SecurityProjectUtils.userCanViewProject}).</li>
 *   <li>{@code roomId} (optional) - the room's folder
 *       ({@code Room.getRoomFolderPath}). Resolved via {@code getRoomById(roomId, userId)}, which
 *       only returns rooms the caller owns.</li>
 *   <li>otherwise - the current insight's working directory ({@code Insight.getInsightFolder}).</li>
 * </ol>
 * {@code projectId} and {@code roomId} are mutually exclusive.
 *
 * <p>This is the public/core counterpart of the {@code Agent_Tools} {@code ListSkill} reactor.
 * Discovery is delegated entirely to {@link SkillScanner#scan(String)} -- the same logic the
 * {@link prerna.reactor.agent.runtime.SemossAgentHarness} uses to build its
 * {@code <available_skills>} system-prompt block -- so all paths agree on what is available.
 * Unlike {@link GetSkillsReactor}, this reactor reads the physical filesystem rather than the
 * skill-project records in the DB.
 *
 * <p>Returns a list of skill maps (one per skill), each with {@code name}, {@code path} (the
 * working-dir-relative path to its {@code SKILL.md}), {@code directory} (the relative path to the
 * skill folder), and {@code description} (a one-liner from YAML frontmatter {@code description:}
 * when present, else the first non-blank line after the H1). When {@code includeContent} is true,
 * each map also carries {@code content} - the SKILL.md body (everything after the frontmatter).
 * When {@code includeAll} is true, each map also carries {@code files} - an array of the other
 * files under the skill directory, each {@code {path, directory, content}} with {@code path} and
 * {@code directory} relative to the working directory (same shape as the top-level skill, so the
 * tree can be recreated; empty directories are not represented). {@code includeAll} implies
 * {@code includeContent}.
 * Empty list when no skills are found. Mirrors the row shape of {@link GetSkillsReactor}.
 */
public class ListSkillsReactor extends AbstractReactor {

	/** Key carrying the target project id - uses the platform-standard {@code project} noun. */
	private static final String PROJECT = ReactorKeysEnum.PROJECT.getKey();

	/** Optional flag - when true, each skill map includes a {@code content} entry (body after frontmatter). */
	private static final String INCLUDE_CONTENT = "includeContent";

	/** Optional flag - when true, also crawl the rest of each skill folder into a {@code files} array (implies includeContent). */
	private static final String INCLUDE_ALL = "includeAll";

	public ListSkillsReactor() {
		this.keysToGet   = new String[] { PROJECT, ReactorKeysEnum.ROOM_ID.getKey(), INCLUDE_CONTENT, INCLUDE_ALL };
		this.keyRequired = new int[]    { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(this.getClass().getName());

		String projectId = trimToNull(this.keyValue.get(PROJECT));
		String roomId    = trimToNull(this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey()));
		if (projectId != null && roomId != null) {
			throw new IllegalArgumentException("Specify only one of project or roomId, not both.");
		}
		boolean includeContent = Boolean.parseBoolean(this.keyValue.get(INCLUDE_CONTENT));
		boolean includeAll = Boolean.parseBoolean(this.keyValue.get(INCLUDE_ALL));
		if (includeAll) {
			// includeAll implies content - the SKILL.md body is part of "everything".
			includeContent = true;
		}

		String workingDir = resolveWorkingDir(projectId, roomId);
		List<DiscoveredSkill> skills = SkillScanner.scan(workingDir, includeContent, includeAll);
		logger.info("ListSkillsReactor: discovered {} skill(s) projectId={} roomId={} includeContent={} includeAll={} workingDir={}",
				skills.size(), projectId, roomId, includeContent, includeAll, workingDir);

		List<Map<String, Object>> rows = new ArrayList<>(skills.size());
		for (DiscoveredSkill s : skills) {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("name", s.getName());
			row.put("path", s.getPath());
			row.put("directory", s.getDirectory());
			row.put("description", s.getDescription());
			if (includeContent) {
				row.put("content", s.getContent());
			}
			if (includeAll) {
				row.put("files", toFileMaps(s.getFiles()));
			}
			rows.add(row);
		}
		return new NounMetadata(rows, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	/**
	 * Resolves the directory to scan based on the optional {@code projectId}/{@code roomId} inputs,
	 * enforcing access. Falls back to the current insight's working directory when neither is given.
	 */
	private String resolveWorkingDir(String projectId, String roomId) {
		User user = this.insight == null ? null : this.insight.getUser();

		if (projectId != null) {
			if (user == null || !SecurityProjectUtils.userCanViewProject(user, projectId)) {
				throw new IllegalArgumentException("Project '" + projectId + "' not found or not accessible.");
			}
			return AssetUtility.getProjectAssetsFolder(projectId);
		}

		if (roomId != null) {
			String userId = resolveUserId(user);
			if (userId == null) {
				throw new IllegalArgumentException("Listing skills by roomId requires an authenticated user.");
			}
			Room room = ModelInferenceLogsUtils.getRoomById(roomId, userId);
			if (room == null) {
				throw new IllegalArgumentException("Room '" + roomId + "' not found or not accessible.");
			}
			return room.getRoomFolderPath();
		}

		return this.insight == null ? null : this.insight.getInsightFolder();
	}

	/** Converts crawled {@link SkillFile}s into response maps: {@code {path, directory, content}}. */
	private static List<Map<String, Object>> toFileMaps(List<SkillFile> files) {
		List<Map<String, Object>> out = new ArrayList<>();
		if (files == null) {
			return out;
		}
		for (SkillFile f : files) {
			Map<String, Object> fm = new LinkedHashMap<>();
			fm.put("path", f.getPath());
			fm.put("directory", f.getDirectory());
			fm.put("content", f.getContent());
			out.add(fm);
		}
		return out;
	}

	private static String trimToNull(String s) {
		if (s == null) {
			return null;
		}
		String trimmed = s.trim();
		return trimmed.isEmpty() ? null : trimmed;
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
		return "Lists every skill discovered on disk under <root|client|java|py>/(.skills|.agents/skills|"
		     + ".agents/skill|.claude/skills|.claude/skill)/<name>/SKILL.md, deduplicated by name "
		     + "(first-match-wins, matching LoadSkill precedence). Scans the given project's assets folder "
		     + "(project) or room folder (roomId); defaults to the current insight when neither is given. "
		     + "Returns a list of skill maps {name, path, directory, description}; pass includeContent=true "
		     + "to add a 'content' entry with each SKILL.md body (everything after the frontmatter), or "
		     + "includeAll=true to also add a 'files' array crawling the rest of each skill folder "
		     + "(implies includeContent). Reads the filesystem, not the DB (see GetSkills for the skill-project listing).";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (PROJECT.equals(key)) {
			return "Optional. Project id whose assets folder to scan for skills. Requires view access "
					+ "to the project. Mutually exclusive with roomId.";
		}
		if (ReactorKeysEnum.ROOM_ID.getKey().equals(key)) {
			return "Optional. Room id whose folder to scan for skills. Must be a room you own. "
					+ "Mutually exclusive with projectId.";
		}
		if (INCLUDE_CONTENT.equals(key)) {
			return "Optional (default false). When true, each skill map includes a 'content' entry "
					+ "with the SKILL.md body - everything after the YAML frontmatter.";
		}
		if (INCLUDE_ALL.equals(key)) {
			return "Optional (default false). When true, also crawl every other file in each skill's "
					+ "directory into a 'files' array, each {path, directory, content} relative to the "
					+ "working directory (same shape as the skill itself). Implies includeContent=true.";
		}
		return super.getDescriptionForKey(key);
	}
}
