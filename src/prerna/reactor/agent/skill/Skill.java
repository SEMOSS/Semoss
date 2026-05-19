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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.json.JSONObject;

/**
 * POJO mirror of a {@code SKILL__} row (plus the resolved current
 * {@code SKILL_VERSION__} fields that {@code ModelInferenceLogsUtils.getSkillEntry}
 * surfaces).
 *
 * <p>Skills live as a folder in an {@link prerna.engine.api.IStorageEngine} pointed
 * to by {@link #storageEngineId} + {@link #storagePrefix}. The folder contains a
 * {@code SKILL.md} (Anthropic skill format — YAML frontmatter with {@code name} and
 * {@code description}, then a markdown body) plus any helper files.
 *
 * <p>This class is intentionally a thin DTO: row-mapping in {@link #fromRow(Map)}
 * and a few stateless helpers (frontmatter parse, slugify, content hash) used by
 * the reactors and the run-time stager. Storage I/O and the run-time stage step
 * live in {@code SkillStager}, not here.
 */
public class Skill {

	/**
	 * Filename inside a skill folder. Every Anthropic-style skill folder contains
	 * exactly one {@code SKILL.md} at its root (frontmatter + body); helper files
	 * live alongside it. This is a domain truth, not a layout choice.
	 *
	 * <p>Where a skill folder lives on disk after staging is the stager's concern,
	 * not this class's — see e.g. {@code SkillStager}.
	 */
	public static final String SKILL_FILE = "SKILL.md";

	// ── SKILL.STATUS values ──
	public static final String STATUS_DRAFT      = "DRAFT";
	public static final String STATUS_PUBLISHED  = "PUBLISHED";
	public static final String STATUS_ARCHIVED   = "ARCHIVED";
	public static final String STATUS_DEPRECATED = "DEPRECATED";

	// ── SKILL.ORIGIN values ──
	public static final String ORIGIN_USER      = "USER";
	public static final String ORIGIN_PLATFORM  = "PLATFORM";
	public static final String ORIGIN_IMPORTED  = "IMPORTED";
	public static final String ORIGIN_GENERATED = "GENERATED";

	// ── frontmatter keys ──
	public static final String FRONTMATTER_DELIM = "---";
	public static final String FM_KEY_NAME = "name";
	public static final String FM_KEY_DESCRIPTION = "description";

	// ── SKILL__ fields ──
	private String skillId;
	private String slug;
	private String name;
	private String description;
	private String createdBy;
	private boolean sharingEnabled;
	private String storageEngineId;
	private String storagePrefix;
	private int currentVersion;
	private String contentHash;
	private long sizeBytes;
	private String status;
	private String origin;
	private JSONObject configJson;
	private String dateCreated;
	private String dateUpdated;

	public Skill() {}

	/**
	 * Builds a {@code Skill} from a row map returned by
	 * {@code ModelInferenceLogsUtils.getSkillEntry / getSkillBySlug}. Unknown
	 * columns are ignored; missing columns default to null / 0 / false.
	 *
	 * @param row column-to-value map; the column keys match the lowercased aliases
	 *            used in the {@code SelectQueryStruct} ({@code skill_id}, {@code slug}, ...)
	 * @return populated skill, or {@code null} when {@code row} is null
	 */
	public static Skill fromRow(Map<String, Object> row) {
		if (row == null) {
			return null;
		}
		Skill s = new Skill();
		s.skillId         = asString(row.get("skill_id"));
		s.slug            = asString(row.get("slug"));
		s.name            = asString(row.get("name"));
		s.description     = asString(row.get("description"));
		s.createdBy       = asString(row.get("created_by"));
		s.sharingEnabled  = asBool(row.get("sharing_enabled"));
		s.storageEngineId = asString(row.get("storage_engine_id"));
		s.storagePrefix   = asString(row.get("storage_prefix"));
		s.currentVersion  = asInt(row.get("current_version"));
		s.contentHash     = asString(row.get("content_hash"));
		s.sizeBytes       = asLong(row.get("size_bytes"));
		s.status          = asString(row.get("status"));
		s.origin          = asString(row.get("origin"));
		s.configJson      = asJson(row.get("config_json"));
		s.dateCreated     = asString(row.get("date_created"));
		s.dateUpdated     = asString(row.get("date_updated"));
		return s;
	}

	// ============================================================
	// Frontmatter parsing
	// ============================================================

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
	 * keys are not supported — those uploads should be rejected by the caller.
	 *
	 * @param skillMdContent full {@code SKILL.md} text
	 * @return parsed frontmatter; {@code name} / {@code description} may be null
	 *         when absent. Never returns null itself.
	 */
	public static Frontmatter parseFrontmatter(String skillMdContent) {
		Frontmatter fm = new Frontmatter();
		if (skillMdContent == null || skillMdContent.isEmpty()) {
			return fm;
		}

		String[] lines = skillMdContent.split("\\R", -1);
		int i = 0;
		// skip leading blank lines
		while (i < lines.length && lines[i].trim().isEmpty()) {
			i++;
		}
		if (i >= lines.length || !FRONTMATTER_DELIM.equals(lines[i].trim())) {
			return fm; // no frontmatter
		}
		i++; // past opening ---

		for (; i < lines.length; i++) {
			String line = lines[i];
			if (FRONTMATTER_DELIM.equals(line.trim())) {
				break; // closing ---
			}
			int colon = line.indexOf(':');
			if (colon <= 0) {
				continue; // skip malformed line
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

	/** Strips a single matched pair of leading/trailing single or double quotes. */
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

	/** Small holder for parsed frontmatter fields. */
	public static final class Frontmatter {
		public String name;
		public String description;

		public boolean isComplete() {
			return name != null && !name.isEmpty() && description != null && !description.isEmpty();
		}
	}

	// ============================================================
	// Slug + content hash helpers
	// ============================================================

	/**
	 * Returns a stable, filesystem-safe slug for the given display name.
	 *
	 * <p>Lowercases, replaces whitespace with {@code -}, strips anything that
	 * isn't {@code [a-z0-9-]}, collapses repeated dashes, trims leading/trailing
	 * dashes. Falls back to {@code "skill"} when the input would slugify to an
	 * empty string.
	 *
	 * <p>This is a stricter variant of
	 * {@code AppBuilderHarnessConfiguration.slugify} (which only lower-cases and
	 * replaces spaces); use this one when the slug is exposed as a directory
	 * name in a working dir.
	 *
	 * @param name human-readable name
	 * @return slugified form, never null
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

	/**
	 * Computes the sha-256 hex digest of the given bytes. Used as the staging
	 * cache key written to {@code .skill-meta} alongside the staged
	 * {@code SKILL.md} and as the {@code CONTENT_HASH} column on {@code SKILL__}
	 * / {@code SKILL_VERSION__}.
	 *
	 * @param content bytes to hash; null is treated as empty
	 * @return lowercase hex string (64 chars)
	 */
	public static String contentHash(byte[] content) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			byte[] digest = md.digest(content == null ? new byte[0] : content);
			StringBuilder sb = new StringBuilder(digest.length * 2);
			for (byte b : digest) {
				sb.append(Character.forDigit((b >> 4) & 0xF, 16));
				sb.append(Character.forDigit(b & 0xF, 16));
			}
			return sb.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 not available on this JVM", e);
		}
	}

	/** Convenience overload — hashes the UTF-8 bytes of the given string. */
	public static String contentHash(String content) {
		return contentHash(content == null ? null : content.getBytes(StandardCharsets.UTF_8));
	}

	// ============================================================
	// row-map value coercion
	// ============================================================

	private static String asString(Object o) {
		return o == null ? null : o.toString();
	}

	private static boolean asBool(Object o) {
		if (o instanceof Boolean) {
			return (Boolean) o;
		}
		if (o instanceof Number) {
			return ((Number) o).intValue() != 0;
		}
		if (o instanceof String) {
			return Boolean.parseBoolean((String) o);
		}
		return false;
	}

	private static int asInt(Object o) {
		if (o instanceof Number) {
			return ((Number) o).intValue();
		}
		if (o instanceof String) {
			try {
				return Integer.parseInt((String) o);
			} catch (NumberFormatException ignored) {
				return 0;
			}
		}
		return 0;
	}

	private static long asLong(Object o) {
		if (o instanceof Number) {
			return ((Number) o).longValue();
		}
		if (o instanceof String) {
			try {
				return Long.parseLong((String) o);
			} catch (NumberFormatException ignored) {
				return 0L;
			}
		}
		return 0L;
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

	// ============================================================
	// getters / setters
	// ============================================================

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

	public boolean isSharingEnabled()       { return sharingEnabled; }
	public void    setSharingEnabled(boolean v) { this.sharingEnabled = v; }

	public String getStorageEngineId()      { return storageEngineId; }
	public void   setStorageEngineId(String v) { this.storageEngineId = v; }

	public String getStoragePrefix()        { return storagePrefix; }
	public void   setStoragePrefix(String v) { this.storagePrefix = v; }

	public int  getCurrentVersion()         { return currentVersion; }
	public void setCurrentVersion(int v)    { this.currentVersion = v; }

	public String getContentHash()          { return contentHash; }
	public void   setContentHash(String v)  { this.contentHash = v; }

	public long getSizeBytes()              { return sizeBytes; }
	public void setSizeBytes(long v)        { this.sizeBytes = v; }

	public String getStatus()               { return status; }
	public void   setStatus(String v)       { this.status = v; }

	public String getOrigin()               { return origin; }
	public void   setOrigin(String v)       { this.origin = v; }

	public JSONObject getConfigJson()       { return configJson; }
	public void       setConfigJson(JSONObject v) { this.configJson = v; }

	public String getDateCreated()          { return dateCreated; }
	public void   setDateCreated(String v)  { this.dateCreated = v; }

	public String getDateUpdated()          { return dateUpdated; }
	public void   setDateUpdated(String v)  { this.dateUpdated = v; }
}
