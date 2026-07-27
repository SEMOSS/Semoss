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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.util.AssetUtility;

/**
 * Identity resolution for skill projects. A skill is a Project of type
 * {@code SKILL}; its name/description live in the YAML frontmatter of the
 * project's {@code SKILL.md} and are read on demand - there is no separate
 * registry table.
 *
 * <p>The skill content folder is {@code <project>/version/assets/public/}
 * (written by {@code CreateSkillReactor}), matching where the built-in
 * platform skill projects ship their {@code SKILL.md}; skills created before
 * content moved to {@code public/} may still have theirs under the legacy
 * {@code version/assets/skill/} folder instead, so resolution probes both.
 *
 * <p>{@link #resolve(String)} is best-effort and never throws: {@code name}
 * and {@code slug} always fall back (frontmatter name, then the securitydb
 * display name, then the project id), and {@code displayName} falls back in
 * the opposite order (securitydb display name, then frontmatter name, then
 * the project id), so callers can safely build responses and staging paths
 * for stale or partially-broken attachments.
 */
public final class SkillProjects {

	private static final Logger logger = LogManager.getLogger(SkillProjects.class);

	private SkillProjects() {}

	/**
	 * Immutable identity snapshot of one skill project.
	 */
	public static final class SkillInfo {
		/** Project id (== skill id). Never null. */
		public final String projectId;
		/**
		 * Canonical, immutable content identity: frontmatter name, else display name,
		 * else project id. This is what {@link UpdateSkillReactor} anchors its
		 * name-immutability guard and frontmatter rewrite to, and what {@link #slug}
		 * is derived from. Never null.
		 */
		public final String name;
		/**
		 * The project's own stored name (what the creator supplied at creation time),
		 * else frontmatter name, else project id - for user-facing listings where the
		 * caller's chosen name should be shown even if it differs from the content's
		 * own declared {@code name:}. Never null.
		 */
		public final String displayName;
		/** Frontmatter description, or null when absent/unreadable. */
		public final String description;
		/** Filesystem-safe folder name derived from {@link #name}. Never null. */
		public final String slug;
		/** Directory containing SKILL.md, or null when the project/folder is unresolvable. */
		public final Path skillDir;
		/** True when the SKILL.md frontmatter was actually read. */
		public final boolean found;

		SkillInfo(String projectId, String name, String displayName, String description, String slug, Path skillDir,
				boolean found) {
			this.projectId = projectId;
			this.name = name;
			this.displayName = displayName;
			this.description = description;
			this.slug = slug;
			this.skillDir = skillDir;
			this.found = found;
		}
	}

	/**
	 * True when the securitydb catalogs {@code projectId} as a {@code SKILL}-type
	 * project. False when the project row is absent.
	 */
	public static boolean isSkillProject(String projectId) {
		if (projectId == null || projectId.trim().isEmpty()) {
			return false;
		}
		return IProject.PROJECT_TYPE.SKILL.name()
				.equals(SecurityProjectUtils.getProjectTypeForId(projectId.trim()));
	}

	/**
	 * Resolves the identity of a skill project. Best-effort: never throws and
	 * never returns null; on failure the returned {@link SkillInfo} carries
	 * fallback name/slug values, a null description, and {@code found=false}.
	 */
	public static SkillInfo resolve(String projectId) {
		Path skillDir = resolveSkillDir(projectId);

		Skill.Frontmatter fm = new Skill.Frontmatter();
		boolean found = false;
		if (skillDir != null) {
			Path skillFile = skillDir.resolve(Skill.SKILL_FILE);
			try {
				String content = new String(Files.readAllBytes(skillFile), StandardCharsets.UTF_8);
				fm = Skill.parseFrontmatter(content);
				found = true;
			} catch (Exception e) {
				logger.warn("SkillProjects: failed to read '{}' for skill project '{}': {}",
						skillFile, projectId, e.getMessage());
			}
		}

		String name = firstNonBlank(fm.name, displayNameQuiet(projectId), projectId);
		String displayName = firstNonBlank(displayNameQuiet(projectId), fm.name, projectId);
		String description = (fm.description == null || fm.description.isEmpty()) ? null : fm.description;
		String slug = Skill.slugify(name);
		return new SkillInfo(projectId, name, displayName, description, slug, skillDir, found);
	}

	/**
	 * Returns the directory holding the project's {@code SKILL.md} - the
	 * {@code public/} assets subfolder, else the legacy {@code skill/} subfolder -
	 * or null when the project cannot be resolved or neither folder has a
	 * {@code SKILL.md}.
	 */
	public static Path resolveSkillDir(String projectId) {
		if (projectId == null || projectId.trim().isEmpty()) {
			return null;
		}
		String assetsFolder;
		try {
			// single-arg form resolves the project folder via its smss alias, which is
			// required for the platform__<id> folder convention
			assetsFolder = AssetUtility.getProjectAssetsFolder(projectId);
		} catch (Exception e) {
			logger.warn("SkillProjects: could not resolve assets folder for skill project '{}': {}",
					projectId, e.getMessage());
			return null;
		}
		Path primary = Paths.get(assetsFolder, Skill.SKILL_ASSET_SUBFOLDER);
		if (Files.isRegularFile(primary.resolve(Skill.SKILL_FILE))) {
			return primary;
		}
		Path legacy = Paths.get(assetsFolder, Skill.LEGACY_SKILL_ASSET_SUBFOLDER);
		if (Files.isRegularFile(legacy.resolve(Skill.SKILL_FILE))) {
			return legacy;
		}
		return null;
	}

	private static String displayNameQuiet(String projectId) {
		try {
			return SecurityProjectUtils.getProjectDisplayNameForId(projectId);
		} catch (Exception e) {
			logger.warn("SkillProjects: failed display-name lookup for '{}': {}", projectId, e.getMessage());
			return null;
		}
	}

	private static String firstNonBlank(String... values) {
		for (String v : values) {
			if (v != null && !v.trim().isEmpty()) {
				return v.trim();
			}
		}
		return null;
	}
}
