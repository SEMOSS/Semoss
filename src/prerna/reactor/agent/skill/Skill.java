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

import java.util.Map;

import org.json.JSONObject;

/**
 * POJO mirror of a {@code SKILL__} row.
 *
 * <p>A skill is a Project of type {@code SKILL} (tagged {@code Skill_Project} in
 * {@code PROJECTMETA}). The underlying Project owns the {@code SKILL.md} (stored
 * under {@code version/assets/skill/}), git-based versioning, and permissions.
 * This class is just a thin DTO over the slim metadata row in
 * {@code modellogs.SKILL__}: row-mapping in {@link #fromRow(Map)} plus a couple of
 * stateless helpers (frontmatter parse, slugify) used by the reactors and the
 * run-time stager.
 */
public class Skill {

	/**
	 * Filename inside a skill folder. Every Anthropic-style skill folder contains
	 * exactly one {@code SKILL.md} at its root (frontmatter + body); helper files
	 * live alongside it.
	 */
	public static final String SKILL_FILE = "SKILL.md";

	/**
	 * Subfolder under {@code <project>/version/assets/} that holds the skill
	 * content. Mirrored by {@code SkillStager} when copying to a working dir.
	 */
	public static final String SKILL_ASSET_SUBFOLDER = "skill";

	// SKILL.ORIGIN values
	public static final String ORIGIN_USER      = "USER";
	public static final String ORIGIN_PLATFORM  = "PLATFORM";
	public static final String ORIGIN_IMPORTED  = "IMPORTED";
	public static final String ORIGIN_GENERATED = "GENERATED";

	// frontmatter keys
	public static final String FRONTMATTER_DELIM = "---";
	public static final String FM_KEY_NAME = "name";
	public static final String FM_KEY_DESCRIPTION = "description";

	// SKILL__ fields (SKILL_ID == underlying Project ID)
	private String skillId;
	private String slug;
	private String name;
	private String description;
	private String createdBy;
	private String origin;
	private JSONObject configJson;
	private String dateCreated;
	private String dateUpdated;

	public Skill() {}

	/**
	 * Builds a {@code Skill} from a row map returned by
	 * {@code ModelInferenceLogsUtils.getSkillEntry / getSkillBySlug}. Unknown
	 * columns are ignored; missing columns default to null.
	 */
	public static Skill fromRow(Map<String, Object> row) {
		if (row == null) {
			return null;
		}
		Skill s = new Skill();
		s.skillId      = asString(row.get("skill_id"));
		s.slug         = asString(row.get("slug"));
		s.name         = asString(row.get("name"));
		s.description  = asString(row.get("description"));
		s.createdBy    = asString(row.get("created_by"));
		s.origin       = asString(row.get("origin"));
		s.configJson   = asJson(row.get("config_json"));
		s.dateCreated  = asString(row.get("date_created"));
		s.dateUpdated  = asString(row.get("date_updated"));
		return s;
	}

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

	private static String asString(Object o) {
		return o == null ? null : o.toString();
	}

	private static JSONObject asJson(Object o) {
		if (o == null) {
			return null;
		}
		String text = o.toString().trim();
		if (text.isEmpty()) {
			return null;
		}
		try {
			return new JSONObject(text);
		} catch (Exception ignored) {
			return null;
		}
	}

	public String getSkillId()              { return skillId; }
	public void   setSkillId(String v)      { this.skillId = v; }

	public String getSlug()                 { return slug; }
	public void   setSlug(String v)         { this.slug = v; }

	public String getName()                 { return name; }
	public void   setName(String v)         { this.name = v; }

	public String getDescription()          { return description; }
	public void   setDescription(String v)  { this.description = v; }

	public String getCreatedBy()            { return createdBy; }
	public void   setCreatedBy(String v)    { this.createdBy = v; }

	public String getOrigin()               { return origin; }
	public void   setOrigin(String v)       { this.origin = v; }

	public JSONObject getConfigJson()       { return configJson; }
	public void       setConfigJson(JSONObject v) { this.configJson = v; }

	public String getDateCreated()          { return dateCreated; }
	public void   setDateCreated(String v)  { this.dateCreated = v; }

	public String getDateUpdated()          { return dateUpdated; }
	public void   setDateUpdated(String v)  { this.dateUpdated = v; }
}
