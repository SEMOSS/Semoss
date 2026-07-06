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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Disk-backed catalog of platform skills.
 *
 * <p>Platform skills ship with the product as plain folders under
 * {@code <BASE_FOLDER>/skills/<slug>/}, each holding a {@code SKILL.md}. Unlike
 * user skills they are NOT Projects: they have no ids, no permissions, are
 * read-only, and cannot be edited or deleted by users. They are referenced by
 * folder name (slug) from {@code WORKSPACE.CONFIG_JSON.platform_skills[]} and
 * {@code room.options.platform_skills[]}, and materialized into a run's
 * {@code .claude/skills/} by {@link SkillStager#stagePlatform}.
 *
 * <p>The directory is the {@code PLATFORM_SKILLS_DIR} property when set,
 * otherwise {@code <BASE_FOLDER>/skills} - the same tree the legacy
 * {@code PlatformSkillBootstrap} scanned.
 */
public final class PlatformSkills {

	private static final Logger logger = LogManager.getLogger(PlatformSkills.class);

	/** Optional RDF_Map / DIHelper override for the platform skills directory. */
	public static final String PLATFORM_SKILLS_DIR_PROP = "PLATFORM_SKILLS_DIR";

	/** Default subfolder under BASE_FOLDER that holds the platform skill folders. */
	public static final String DEFAULT_SKILLS_SUBDIR = "skills";

	/**
	 * Discriminator written into the {@code type} field of a platform skill entry
	 * in workspace payloads (e.g. {@code GetWorkspaceReactor}), so the UI can tell a
	 * disk-backed platform skill apart from a project-backed {@code SKILL} resource.
	 */
	public static final String PLATFORM_SKILL_TYPE = "PLATFORM_SKILL";

	private PlatformSkills() {}

	/**
	 * Root directory holding the platform skill folders: the
	 * {@code PLATFORM_SKILLS_DIR} property when set (and non-blank), otherwise
	 * {@code <BASE_FOLDER>/skills}. The returned path may not exist yet; callers
	 * tolerate that.
	 */
	public static Path baseDir() {
		String override = DIHelper.getInstance().getProperty(PLATFORM_SKILLS_DIR_PROP);
		if (override != null && !override.trim().isEmpty()) {
			return Paths.get(override.trim());
		}
		return Paths.get(Utility.getBaseFolder(), DEFAULT_SKILLS_SUBDIR);
	}

	/**
	 * Resolves the folder for a single platform skill, or {@code null} when the
	 * slug is blank, attempts path traversal, or has no {@code SKILL.md}.
	 */
	public static Path resolveDir(String slug) {
		if (slug == null || slug.trim().isEmpty()) {
			return null;
		}
		String clean = slug.trim();
		// Platform slugs are simple folder names - reject anything that could escape baseDir.
		if (clean.contains("/") || clean.contains("\\") || clean.contains("..")) {
			logger.warn("PlatformSkills: rejecting unsafe slug '{}'", slug);
			return null;
		}
		Path dir = baseDir().resolve(clean);
		if (!Files.isDirectory(dir)) {
			return null;
		}
		if (!Files.isRegularFile(dir.resolve(Skill.SKILL_FILE))) {
			return null;
		}
		return dir;
	}

	/** True when {@code slug} resolves to a platform skill folder containing a {@code SKILL.md}. */
	public static boolean exists(String slug) {
		return resolveDir(slug) != null;
	}

	/**
	 * Lists every platform skill: one entry per immediate subdirectory of
	 * {@link #baseDir()} that contains a {@code SKILL.md}. Each entry carries
	 * {@code slug}, {@code name}, {@code description} (parsed from the frontmatter,
	 * falling back to the slug for name), and {@code origin = PLATFORM}. There is
	 * no {@code skill_id} - platform skills are referenced by slug. Returns an
	 * empty list when the directory is absent or unreadable.
	 */
	public static List<Map<String, Object>> list() {
		List<Map<String, Object>> out = new ArrayList<>();
		Path base = baseDir();
		if (!Files.isDirectory(base)) {
			logger.info("PlatformSkills: skills dir '{}' does not exist; returning empty catalog", base);
			return out;
		}
		File[] children = base.toFile().listFiles(File::isDirectory);
		if (children == null) {
			return out;
		}
		Arrays.sort(children, (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
		for (File child : children) {
			Path skillMd = child.toPath().resolve(Skill.SKILL_FILE);
			if (!Files.isRegularFile(skillMd)) {
				continue;
			}
			String slug = child.getName();
			String name = slug;
			String description = null;
			try {
				String content = new String(Files.readAllBytes(skillMd), StandardCharsets.UTF_8);
				Skill.Frontmatter fm = Skill.parseFrontmatter(content);
				if (fm.name != null && !fm.name.isEmpty()) {
					name = fm.name;
				}
				if (fm.description != null && !fm.description.isEmpty()) {
					description = fm.description;
				}
			} catch (IOException e) {
				logger.warn("PlatformSkills: failed reading '{}': {}", skillMd, e.getMessage());
			}
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("slug", slug);
			entry.put("name", name);
			entry.put("description", description);
			entry.put("origin", Skill.ORIGIN_PLATFORM);
			out.add(entry);
		}
		return out;
	}
}
