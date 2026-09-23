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

/**
 * Stateless helpers for skill content.
 *
 * <p>A skill is a Project of type {@code SKILL} (tagged {@code SKILL} in
 * {@code PROJECTMETA}). The Project owns the {@code SKILL.md} (stored under
 * {@code version/assets/public/}, or the legacy {@code version/assets/skill/}
 * for skills created before that change), git-based versioning, and
 * permissions; the
 * SKILL.md frontmatter is the source of truth for the skill's name and
 * description (see {@link SkillProjects}). This class holds the frontmatter
 * parse/build/strip helpers, {@link #slugify}, and the folder/file constants
 * used by the reactors and the run-time stager.
 */
public final class Skill {

	private Skill() {}

	/**
	 * Filename inside a skill folder. Every Anthropic-style skill folder contains
	 * exactly one {@code SKILL.md} at its root (frontmatter + body); helper files
	 * live alongside it.
	 */
	public static final String SKILL_FILE = "SKILL.md";

	/**
	 * Subfolder under {@code <project>/version/assets/} that holds the skill
	 * content for newly-created skills, matching where the shipped platform
	 * skill projects already keep theirs (see
	 * {@link prerna.util.Constants#PUBLIC_ASSETS_FOLDER}) - so custom skills are
	 * readable by users with only view (not edit) access to the skill project,
	 * same as built-in ones. Mirrored by {@code SkillStager} when copying to a
	 * working dir.
	 */
	public static final String SKILL_ASSET_SUBFOLDER = "public";

	/**
	 * Legacy subfolder under {@code <project>/version/assets/} used for skill
	 * content written before it moved to {@link #SKILL_ASSET_SUBFOLDER}. Still
	 * probed as a fallback (by {@link SkillProjects#resolveSkillDir}) so skills
	 * created before that change keep resolving; no longer written to.
	 */
	public static final String LEGACY_SKILL_ASSET_SUBFOLDER = "skill";

	// frontmatter keys
	public static final String FRONTMATTER_DELIM = "---";
	public static final String FM_KEY_NAME = "name";
	public static final String FM_KEY_DESCRIPTION = "description";

	/**
	 * Extracts {@code name} and {@code description} from the YAML frontmatter of a
	 * {@code SKILL.md}. Only the simple line-based form is supported:
	 *
	 * <pre>
	 * ---
	 * name: skill-name
	 * description: When to invoke this skill...
	 * ---
	 * # body
	 * </pre>
	 *
	 * <p>Both quoted ({@code 'value'} or {@code "value"}) and unquoted values are
	 * accepted. Multi-line values, block scalars ({@code |}, {@code >}), and nested
	 * keys are not supported - those uploads should be rejected by the caller.
	 */
	public static Frontmatter parseFrontmatter(String skillMdContent) {
		Frontmatter fm = new Frontmatter();
		if (skillMdContent == null || skillMdContent.isEmpty()) {
			return fm;
		}

		String[] lines = skillMdContent.split("\\R", -1);
		int i = 0;
		while (i < lines.length && lines[i].trim().isEmpty()) {
			i++;
		}
		if (i >= lines.length || !FRONTMATTER_DELIM.equals(lines[i].trim())) {
			return fm;
		}
		i++;

		for (; i < lines.length; i++) {
			String line = lines[i];
			if (FRONTMATTER_DELIM.equals(line.trim())) {
				break;
			}
			int colon = line.indexOf(':');
			if (colon <= 0) {
				continue;
			}
			String key = line.substring(0, colon).trim();
			String value = stripQuotes(line.substring(colon + 1).trim());
			if (FM_KEY_NAME.equalsIgnoreCase(key)) {
				fm.name = value;
			} else if (FM_KEY_DESCRIPTION.equalsIgnoreCase(key)) {
				fm.description = value;
			}
		}
		return fm;
	}

	private static String stripQuotes(String s) {
		if (s == null || s.length() < 2) {
			return s;
		}
		char first = s.charAt(0);
		char last = s.charAt(s.length() - 1);
		if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
			return s.substring(1, s.length() - 1);
		}
		return s;
	}

	public static final class Frontmatter {
		public String name;
		public String description;

		public boolean isComplete() {
			return name != null && !name.isEmpty() && description != null && !description.isEmpty();
		}
	}

	/**
	 * True when {@code content} begins (after any leading blank lines) with a
	 * {@code ---} frontmatter opening delimiter. Does not validate the body of
	 * the block - a malformed/partial frontmatter still counts as "present" so
	 * the caller leaves it alone instead of stacking a synthesized block on top.
	 */
	public static boolean hasFrontmatterBlock(String content) {
		if (content == null || content.isEmpty()) {
			return false;
		}
		String[] lines = content.split("\\R", -1);
		int i = 0;
		while (i < lines.length && lines[i].trim().isEmpty()) {
			i++;
		}
		return i < lines.length && FRONTMATTER_DELIM.equals(lines[i].trim());
	}

	/**
	 * Returns a YAML frontmatter block carrying {@code name} and {@code description},
	 * followed by a blank line so it sits cleanly above the body. Both values are
	 * normalized to a single line.
	 */
	public static String buildFrontmatter(String name, String description) {
		return FRONTMATTER_DELIM + "\n"
				+ FM_KEY_NAME + ": " + singleLine(name) + "\n"
				+ FM_KEY_DESCRIPTION + ": " + singleLine(description) + "\n"
				+ FRONTMATTER_DELIM + "\n\n";
	}

	/**
	 * Returns the body of {@code content} with any leading YAML frontmatter block
	 * removed. If the content has no frontmatter (or has a malformed/unclosed one)
	 * the original string is returned unchanged. A single blank line immediately
	 * after the closing {@code ---} is also dropped, since {@link #buildFrontmatter}
	 * always emits one and we don't want to stack them on a re-write.
	 */
	public static String stripFrontmatter(String content) {
		if (content == null || content.isEmpty()) {
			return "";
		}
		if (!hasFrontmatterBlock(content)) {
			return content;
		}
		String[] lines = content.split("\\R", -1);
		int i = 0;
		while (i < lines.length && lines[i].trim().isEmpty()) {
			i++;
		}
		// i points at the opening ---
		i++;
		while (i < lines.length && !FRONTMATTER_DELIM.equals(lines[i].trim())) {
			i++;
		}
		if (i >= lines.length) {
			// no closing delimiter - leave the original alone
			return content;
		}
		i++; // past closing ---
		if (i < lines.length && lines[i].trim().isEmpty()) {
			i++;
		}
		StringBuilder sb = new StringBuilder();
		for (int j = i; j < lines.length; j++) {
			sb.append(lines[j]);
			if (j < lines.length - 1) {
				sb.append("\n");
			}
		}
		return sb.toString();
	}

	private static String singleLine(String s) {
		if (s == null) {
			return "";
		}
		return s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").trim();
	}

	/**
	 * Returns a stable, filesystem-safe slug for the given display name.
	 *
	 * <p>Lowercases, replaces whitespace with {@code -}, strips anything that
	 * isn't {@code [a-z0-9-]}, collapses repeated dashes, trims leading/trailing
	 * dashes. Falls back to {@code "skill"} when the input would slugify to an
	 * empty string.
	 */
	public static String slugify(String name) {
		if (name == null) {
			return "skill";
		}
		String slug = name.toLowerCase()
				.replaceAll("\\s+", "-")
				.replaceAll("[^a-z0-9-]", "")
				.replaceAll("-+", "-")
				.replaceAll("^-|-$", "");
		return slug.isEmpty() ? "skill" : slug;
	}
}
