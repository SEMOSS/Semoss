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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.util.RuntimeTimeContext;
import prerna.util.Utility;

// Brain "you": BRAIN_PROFILE, BRAIN_SETTINGS, and the overview counts
public final class BrainProfileUtils {

	private static final Logger classLogger = LogManager.getLogger(BrainProfileUtils.class);

	public static final int DEFAULT_FILE_AT = 85;
	public static final int DEFAULT_ASK_AT = 40;

	// ROLE_STATE and STYLE_STATE values
	public static final String LEARNED = "learned";
	public static final String CONFIRMED = "confirmed";
	public static final String YOU = "you";

	private BrainProfileUtils() {

	}

	// ---- profile ----

	/** Read only the owner's timezone without creating a profile or loading its other fields. */
	public static ZoneId timeZone(User user) {
		ZoneId userZone = user == null ? null : user.getZoneId();
		String profileZone = null;
		if (user != null) {
			try {
				Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
				profileZone = CollaborationDbUtils.queryOne("SELECT TIMEZONE FROM BRAIN_PROFILE WHERE OWNER_ID = ? "
						+ "AND OWNER_TYPE = ?", rs -> rs.getString(1), owner.getValue0(), owner.getValue1());
			} catch (RuntimeException e) {
				classLogger.warn("Could not read the Collaboration timezone; using the authenticated user's timezone");
			}
		}
		return RuntimeTimeContext.resolveZone(profileZone, userZone);
	}

	// creates the row on first read, seeded from the login's name and email
	public static Map<String, Object> getProfile(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		ensureProfile(owner.getValue0(), owner.getValue1(), user);
		return getProfile(owner.getValue0(), owner.getValue1());
	}

	public static Map<String, Object> getProfile(String ownerId, String ownerType) {
		Map<String, Object> profile = CollaborationDbUtils
				.queryOne(
						"SELECT DISPLAY_NAME, EMAIL, ORG, ROLE, ROLE_STATE, ROLE_NOTE, TIMEZONE, WORKING_HOURS_JSON, "
								+ "STYLE_SUMMARY, STYLE_STATE, STYLE_EXAMPLES_JSON, STYLE_VARIANTS_JSON, STYLE_PENDING_JSON, "
								+ "STYLE_LEARNED_AT FROM BRAIN_PROFILE "
								+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
						BrainProfileUtils::mapProfile, ownerId, ownerType);
		if (profile != null) {
			profile.put("vips", getVipIds(ownerId, ownerType));
		}
		return profile;
	}

	// who the assistant writes as; role and style only once the owner set or confirmed them
	@SuppressWarnings("unchecked")
	public static String promptBlock(User user) {
		Map<String, Object> profile;
		try {
			profile = user == null ? null : getProfile(user);
		} catch (RuntimeException e) {
			classLogger.warn("Could not read the Collaboration profile for the assistant prompt", e);
			return null;
		}
		if (profile == null) {
			return null;
		}
		StringBuilder out = new StringBuilder("## Owner\nThe owner you work for. Write and sign as them.");
		appendField(out, "Name", profile.get("name"));
		appendField(out, "Email", profile.get("email"));
		appendField(out, "Organization", profile.get("org"));
		appendField(out, "Timezone", profile.get("timezone"));
		Map<String, Object> role = (Map<String, Object>) profile.get("role");
		if (role != null && YOU.equals(role.get("source"))) {
			appendField(out, "Role", role.get("value"));
		}
		Map<String, Object> style = (Map<String, Object>) profile.get("style");
		if (style != null && Boolean.TRUE.equals(style.get("confirmed"))) {
			appendField(out, "Writing style", style.get("summary"));
			appendStyleDetails(out, style, user, CollaborationDbUtils.asString(profile.get("email")));
		}
		return out.toString();
	}

	// the agreed examples and audience variants; a pending proposal never reaches here
	@SuppressWarnings("unchecked")
	private static void appendStyleDetails(StringBuilder out, Map<String, Object> style, User user,
			String email) {
		appendExamples(out, "Writing examples", style.get("examples"));
		Object variants = style.get("variants");
		if (!(variants instanceof Map<?, ?> byAudience) || byAudience.isEmpty()) {
			return;
		}
		out.append("\n- Adjust the writing style to the audience: use the internal style when every recipient is in the "
				+ "owner's organisation, and the external style when any recipient is outside it");
		try {
			Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
			BrainOrgDomains.Org org = BrainOrgDomains.load(owner.getValue0(), owner.getValue1(),
					email == null ? null : email.substring(email.indexOf('@') + 1));
			if (!org.domains().isEmpty()) {
				appendField(out, "Organisation domains", String.join(", ", new java.util.TreeSet<>(org.domains())));
			}
		} catch (RuntimeException e) {
			classLogger.warn("Could not read the organisation domains for the writing style prompt", e);
		}
		for (String audience : AUDIENCES) {
			if (byAudience.get(audience) instanceof Map<?, ?> variant) {
				Map<String, Object> entry = (Map<String, Object>) variant;
				appendField(out, "Writing style for " + audience + " recipients", entry.get("summary"));
				appendExamples(out, "Writing examples for " + audience + " recipients", entry.get("examples"));
			}
		}
	}

	private static void appendExamples(StringBuilder out, String label, Object examples) {
		if (!(examples instanceof List<?> list)) {
			return;
		}
		for (Object example : list) {
			if (example instanceof String text && !text.isBlank()) {
				out.append("\n- ").append(label).append(": ").append(text.strip().replace("\n", " "));
			}
		}
	}

	private static void appendField(StringBuilder out, String label, Object value) {
		if (value instanceof String text && !text.isBlank()) {
			out.append("\n- ").append(label).append(": ").append(text.strip());
		}
	}

	// partial Profile: only keys present are written; a field the owner types
	// becomes source "you"
	@SuppressWarnings("unchecked")
	public static Map<String, Object> saveProfile(User user, Map<String, Object> changes) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		ensureProfile(ownerId, ownerType, user);

		List<String> sets = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		CollaborationDbUtils.setIfPresent(changes, "name", "DISPLAY_NAME", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "email", "EMAIL", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "org", "ORG", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "timezone", "TIMEZONE", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "workingHours", "WORKING_HOURS_JSON", sets, params);
		if (changes.get("role") instanceof Map) {
			Map<String, Object> role = (Map<String, Object>) changes.get("role");
			if (role.containsKey("value")) {
				CollaborationDbUtils.addSet(sets, params, "ROLE", CollaborationDbUtils.asString(role.get("value")));
				CollaborationDbUtils.addSet(sets, params, "ROLE_STATE", YOU);
			}
			CollaborationDbUtils.setIfPresent(role, "note", "ROLE_NOTE", sets, params);
		}
		if (changes.get("style") instanceof Map) {
			Map<String, Object> style = (Map<String, Object>) changes.get("style");
			if (style.containsKey("summary")) {
				CollaborationDbUtils.addSet(sets, params, "STYLE_SUMMARY",
						CollaborationDbUtils.asString(style.get("summary")));
				// an accepted proposal stays "learned and confirmed"; what the owner typed is "you"
				boolean accepted = Boolean.TRUE.equals(style.get("confirmed")) && LEARNED.equals(style.get("source"));
				CollaborationDbUtils.addSet(sets, params, "STYLE_STATE", accepted ? CONFIRMED : YOU);
			} else if (style.containsKey("confirmed")) {
				CollaborationDbUtils.addSet(sets, params, "STYLE_STATE",
						Boolean.TRUE.equals(style.get("confirmed")) ? CONFIRMED : LEARNED);
			}
			if (style.containsKey("examples")) {
				CollaborationDbUtils.addSet(sets, params, "STYLE_EXAMPLES_JSON",
						CollaborationDbUtils.toJson(cleanExamples(style.get("examples"))));
			}
			if (style.containsKey("variants")) {
				CollaborationDbUtils.addSet(sets, params, "STYLE_VARIANTS_JSON",
						CollaborationDbUtils.toJson(cleanVariants(style.get("variants"))));
			}
			// only the proposal's review clears it; a proposal is written by the style learning, never by a client
			if (style.containsKey("pending") && isEmpty(style.get("pending"))) {
				CollaborationDbUtils.addSet(sets, params, "STYLE_PENDING_JSON", null);
			}
		}
		List<String> vips = changes.get("vips") instanceof List
				? CollaborationDbUtils.toStringList((List<Object>) changes.get("vips"))
				: null;

		CollaborationDbUtils.inTransaction(conn -> {
			if (!sets.isEmpty()) {
				CollaborationDbUtils.addSet(sets, params, "UPDATED_AT", CollaborationDbUtils.now());
				params.add(ownerId);
				params.add(ownerType);
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_PROFILE SET " + String.join(", ", sets)
						+ " WHERE OWNER_ID = ? AND OWNER_TYPE = ?", params.toArray());
			}
			if (vips != null) {
				CollaborationDbUtils.update(conn,
						"UPDATE BRAIN_PERSON SET IS_VIP = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND IS_VIP = ?",
						false, ownerId, ownerType, true);
				for (String personId : vips) {
					CollaborationDbUtils.update(conn,
							"UPDATE BRAIN_PERSON SET IS_VIP = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?",
							true, ownerId, ownerType, personId);
				}
			}
		});
		return getProfile(ownerId, ownerType);
	}

	private static void ensureProfile(String ownerId, String ownerType, User user) {
		if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_PROFILE WHERE OWNER_ID = ? AND OWNER_TYPE = ?", ownerId,
				ownerType)) {
			return;
		}
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		// time zone comes from the browser (runPixel tz), so no MailboxSettings.Read is
		// needed
		CollaborationDbUtils.update(
				"INSERT INTO BRAIN_PROFILE (OWNER_ID, OWNER_TYPE, DISPLAY_NAME, EMAIL, TIMEZONE, ROLE_STATE, "
						+ "STYLE_STATE, UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
				ownerId, ownerType, token == null ? null : token.getName(), token == null ? null : token.getEmail(),
				user.getZoneId() == null ? null : user.getZoneId().getId(), LEARNED, LEARNED,
				CollaborationDbUtils.now());
	}

	private static Map<String, Object> mapProfile(ResultSet rs) throws SQLException {
		String roleState = CollaborationDbUtils.getString(rs, "ROLE_STATE");
		Map<String, Object> role = new LinkedHashMap<>();
		role.put("value", CollaborationDbUtils.getString(rs, "ROLE"));
		role.put("source", YOU.equals(roleState) ? YOU : LEARNED);
		String roleNote = CollaborationDbUtils.getString(rs, "ROLE_NOTE");
		if (roleNote != null) {
			role.put("note", roleNote);
		}

		// learned = suggested, confirmed = learned and accepted, you = typed by the
		// owner
		String styleState = CollaborationDbUtils.getString(rs, "STYLE_STATE");
		Map<String, Object> style = new LinkedHashMap<>();
		style.put("summary", CollaborationDbUtils.getString(rs, "STYLE_SUMMARY"));
		style.put("source", YOU.equals(styleState) ? YOU : LEARNED);
		style.put("confirmed", YOU.equals(styleState) || CONFIRMED.equals(styleState));
		style.put("examples",
				CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "STYLE_EXAMPLES_JSON")));
		style.put("variants", cleanVariants(
				CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs, "STYLE_VARIANTS_JSON"))));
		String pending = CollaborationDbUtils.getString(rs, "STYLE_PENDING_JSON");
		if (pending != null && !pending.isBlank()) {
			style.put("pending", CollaborationDbUtils.parseMap(pending));
		}
		java.sql.Timestamp learnedAt = rs.getTimestamp("STYLE_LEARNED_AT");
		if (learnedAt != null) {
			style.put("learnedAt", learnedAt.toInstant().toString());
		}

		Map<String, Object> profile = new LinkedHashMap<>();
		profile.put("id", "me");
		profile.put("name", CollaborationDbUtils.getString(rs, "DISPLAY_NAME"));
		profile.put("email", CollaborationDbUtils.getString(rs, "EMAIL"));
		profile.put("org", CollaborationDbUtils.getString(rs, "ORG"));
		profile.put("role", role);
		profile.put("timezone", CollaborationDbUtils.getString(rs, "TIMEZONE"));
		profile.put("workingHours", CollaborationDbUtils.getString(rs, "WORKING_HOURS_JSON"));
		profile.put("style", style);
		return profile;
	}

	/** The audiences a style variant can be written for. */
	static final List<String> AUDIENCES = List.of("internal", "external");
	static final int MAX_EXAMPLES = 5;
	static final int MAX_EXAMPLE_CHARS = 600;
	static final int MAX_SUMMARY_CHARS = 1500;

	private static boolean isEmpty(Object value) {
		return value == null || (value instanceof String text && text.isBlank())
				|| (value instanceof Map<?, ?> map && map.isEmpty());
	}

	// only short, non-blank strings, so a style can never smuggle in a large block
	static List<String> cleanExamples(Object value) {
		List<String> out = new ArrayList<>();
		if (value instanceof List<?> list) {
			for (Object item : list) {
				if (item instanceof String text && !text.isBlank() && out.size() < MAX_EXAMPLES) {
					String trimmed = text.strip();
					out.add(trimmed.length() > MAX_EXAMPLE_CHARS ? trimmed.substring(0, MAX_EXAMPLE_CHARS) : trimmed);
				}
			}
		}
		return out;
	}

	// {internal, external}, each {summary, examples}; a variant with no summary is dropped
	static Map<String, Object> cleanVariants(Object value) {
		Map<String, Object> out = new LinkedHashMap<>();
		if (!(value instanceof Map<?, ?> map)) {
			return out;
		}
		for (String audience : AUDIENCES) {
			if (!(map.get(audience) instanceof Map<?, ?> variant)) {
				continue;
			}
			String summary = variant.get("summary") instanceof String text ? text.strip() : "";
			if (summary.isEmpty()) {
				continue;
			}
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("summary",
					summary.length() > MAX_SUMMARY_CHARS ? summary.substring(0, MAX_SUMMARY_CHARS) : summary);
			entry.put("examples", cleanExamples(variant.get("examples")));
			out.put(audience, entry);
		}
		return out;
	}

	private static List<String> getVipIds(String ownerId, String ownerType) {
		return CollaborationDbUtils
				.query("SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND IS_VIP = ? "
						+ "ORDER BY PERSON_ID", rs -> rs.getString("PERSON_ID"), ownerId, ownerType, true);
	}

	// ---- settings ----

	public static Map<String, Object> getSettings(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> settings = getSettings(owner.getValue0(), owner.getValue1());
		// read-only: the platform agent behind each thread's assistant, when this user
		// can use it
		settings.put("assistantAgent", CollaborationUtils.threadAgent(user));
		return settings;
	}

	// creates the row with FILE_AT 85 / ASK_AT 40 on first read
	public static Map<String, Object> getSettings(String ownerId, String ownerType) {
		ensureSettings(ownerId, ownerType);
		Map<String, Object> settings = CollaborationDbUtils.queryOne(
				"SELECT FILE_AT, ASK_AT, VERSION, MEMORY_USE, MEMORY_LEARN, DRAFT_FOLDER_ID, DRAFT_FOLDER_NAME, "
						+ "ASSISTANT_SKILLS_JSON "
						+ "FROM BRAIN_SETTINGS WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					// blank when drafts stay in the mailbox's Drafts
					String folderId = CollaborationDbUtils.getString(rs, "DRAFT_FOLDER_ID");
					row.put("draftFolderId", folderId == null ? "" : folderId);
					String folderName = CollaborationDbUtils.getString(rs, "DRAFT_FOLDER_NAME");
					row.put("draftFolderName", folderName == null ? "" : folderName);
					// read-only: one platform classifier (RDF_Map)
					row.put("classifierEngineId", BrainThreadClassifier.platformEngine());
					row.put("fileAt", CollaborationDbUtils.getInteger(rs, "FILE_AT"));
					row.put("askAt", CollaborationDbUtils.getInteger(rs, "ASK_AT"));
					row.put("version", CollaborationDbUtils.getInteger(rs, "VERSION"));
					Map<String, Object> memory = new LinkedHashMap<>();
					memory.put("use", !Boolean.FALSE.equals(CollaborationDbUtils.getBoolean(rs, "MEMORY_USE")));
					memory.put("learn", !Boolean.FALSE.equals(CollaborationDbUtils.getBoolean(rs, "MEMORY_LEARN")));
					row.put("memory", memory);
					row.put("assistantSkills", skillIds(CollaborationDbUtils.getString(rs, "ASSISTANT_SKILLS_JSON")));
					return row;
				}, ownerId, ownerType);
		settings.put("sourcesJson", CollaborationSourceUtils.getSourcesEnabled(ownerId, ownerType));
		return settings;
	}

	// partial Settings; a version, when sent, must match or the save is refused
	@SuppressWarnings("unchecked")
	public static Map<String, Object> saveSettings(User user, Map<String, Object> changes) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Map<String, Object> current = getSettings(ownerId, ownerType);
		int currentVersion = (Integer) current.get("version");
		if (changes.containsKey("version")
				&& CollaborationDbUtils.toInt(changes.get("version"), "version") != currentVersion) {
			throw new IllegalArgumentException("Settings were changed elsewhere; reload and try again");
		}

		int fileAt = changes.containsKey("fileAt") ? CollaborationDbUtils.toInt(changes.get("fileAt"), "fileAt")
				: (Integer) current.get("fileAt");
		int askAt = changes.containsKey("askAt") ? CollaborationDbUtils.toInt(changes.get("askAt"), "askAt")
				: (Integer) current.get("askAt");
		if (askAt < 0 || fileAt > 100 || askAt >= fileAt) {
			throw new IllegalArgumentException("Bands must satisfy 0 <= askAt < fileAt <= 100");
		}
		Map<String, Object> sources = changes.get("sourcesJson") instanceof Map
				? (Map<String, Object>) changes.get("sourcesJson")
				: Collections.emptyMap();
		for (String source : sources.keySet()) {
			if (!CollaborationSourceUtils.isKnownSource(source)) {
				throw new IllegalArgumentException("Unknown source: " + source);
			}
		}

		List<String> sets = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		CollaborationDbUtils.addSet(sets, params, "FILE_AT", fileAt);
		CollaborationDbUtils.addSet(sets, params, "ASK_AT", askAt);
		if (changes.get("memory") instanceof Map) {
			Map<String, Object> memory = (Map<String, Object>) changes.get("memory");
			if (memory.containsKey("use")) {
				CollaborationDbUtils.addSet(sets, params, "MEMORY_USE", isTrue(memory.get("use")));
			}
			if (memory.containsKey("learn")) {
				CollaborationDbUtils.addSet(sets, params, "MEMORY_LEARN", isTrue(memory.get("learn")));
			}
		}
		if (changes.containsKey("draftFolderId") || changes.containsKey("draftFolderName")) {
			// the id picks the folder; a blank id puts drafts back in Drafts
			String folderId = trimToNull(changes.get("draftFolderId"));
			String folderName = trimToNull(changes.get("draftFolderName"));
			if (folderId != null && (folderId.length() > 255 || (folderName != null && folderName.length() > 255))) {
				throw new IllegalArgumentException("The folder id or name is too long");
			}
			CollaborationDbUtils.addSet(sets, params, "DRAFT_FOLDER_ID", folderId);
			CollaborationDbUtils.addSet(sets, params, "DRAFT_FOLDER_NAME", folderId == null ? null : folderName);
		}
		if (changes.containsKey("assistantSkills")) {
			if (!(changes.get("assistantSkills") instanceof List<?> skills)) {
				throw new IllegalArgumentException("assistantSkills must be a list of skill ids");
			}
			CollaborationDbUtils.addSet(sets, params, "ASSISTANT_SKILLS_JSON",
					CollaborationDbUtils.toJson(CollaborationSkills.requireMine(user, skills)));
		}
		CollaborationDbUtils.addSet(sets, params, "VERSION", currentVersion + 1);
		CollaborationDbUtils.addSet(sets, params, "UPDATED_AT", CollaborationDbUtils.now());
		params.add(ownerId);
		params.add(ownerType);
		params.add(currentVersion);
		int updated = CollaborationDbUtils.update("UPDATE BRAIN_SETTINGS SET " + String.join(", ", sets)
				+ " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND VERSION = ?", params.toArray());
		if (updated == 0) {
			throw new IllegalArgumentException("Settings were changed elsewhere; reload and try again");
		}
		for (Map.Entry<String, Object> source : sources.entrySet()) {
			CollaborationSourceUtils.setSourceEnabled(ownerId, ownerType, source.getKey(),
					Boolean.TRUE.equals(source.getValue()));
		}
		return getSettings(ownerId, ownerType);
	}

	/**
	 * The Outlook folder id the user keeps drafts in, or null for their own Drafts. Read only: it
	 * creates nothing and answers null when Collaboration is off or the user has no settings yet.
	 */
	public static String draftFolderId(User user) {
		try {
			if (user == null || !Utility.isCollaborationDatabaseEnabled() || !CollaborationDbUtils.isInitalized()) {
				return null;
			}
			Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
			return CollaborationDbUtils.queryOne(
					"SELECT DRAFT_FOLDER_ID FROM BRAIN_SETTINGS WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
					rs -> trimToNull(CollaborationDbUtils.getString(rs, "DRAFT_FOLDER_ID")), owner.getValue0(),
					owner.getValue1());
		} catch (Exception e) {
			// a draft still gets saved, in Drafts, when the setting cannot be read
			classLogger.warn("Could not read the draft folder setting: {}", e.getMessage());
			return null;
		}
	}

	/** Skill ids the owner turned on for their assistant; empty when Collaboration is off or nothing is set. */
	public static List<String> assistantSkills(User user) {
		try {
			if (user == null || !Utility.isCollaborationDatabaseEnabled() || !CollaborationDbUtils.isInitalized()) {
				return List.of();
			}
			Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
			List<String> ids = CollaborationDbUtils.queryOne(
					"SELECT ASSISTANT_SKILLS_JSON FROM BRAIN_SETTINGS WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
					rs -> skillIds(CollaborationDbUtils.getString(rs, "ASSISTANT_SKILLS_JSON")), owner.getValue0(),
					owner.getValue1());
			return ids == null ? List.of() : ids;
		} catch (Exception e) {
			// the chat still runs, without the owner's skills, when the setting cannot be read
			classLogger.warn("Could not read the assistant skills setting: {}", e.getMessage());
			return List.of();
		}
	}

	private static List<String> skillIds(String json) {
		List<String> ids = new ArrayList<>();
		if (json != null && !json.isBlank()) {
			for (Object id : CollaborationDbUtils.parseList(json)) {
				ids.add(String.valueOf(id));
			}
		}
		return ids;
	}

	private static String trimToNull(Object value) {
		if (value == null) {
			return null;
		}
		String text = String.valueOf(value).trim();
		return text.isEmpty() ? null : text;
	}

	/** Memories are recalled and the assistant gets its memory tools; on unless the owner turned it off. */
	static boolean usesMemory(String ownerId, String ownerType) {
		return !Boolean.FALSE.equals(memorySetting(ownerId, ownerType, "MEMORY_USE"));
	}

	/** Finished chats are reviewed for memories to suggest; also needs memory itself to be on. */
	static boolean learnsMemory(String ownerId, String ownerType) {
		return usesMemory(ownerId, ownerType)
				&& !Boolean.FALSE.equals(memorySetting(ownerId, ownerType, "MEMORY_LEARN"));
	}

	private static Boolean memorySetting(String ownerId, String ownerType, String column) {
		return CollaborationDbUtils.queryOne("SELECT " + column + " FROM BRAIN_SETTINGS WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ?", rs -> CollaborationDbUtils.getBoolean(rs, column), ownerId, ownerType);
	}

	// pixel booleans arrive as Boolean or text
	private static boolean isTrue(Object value) {
		return value instanceof Boolean b ? b : Boolean.parseBoolean(String.valueOf(value).trim());
	}

	private static void ensureSettings(String ownerId, String ownerType) {
		if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_SETTINGS WHERE OWNER_ID = ? AND OWNER_TYPE = ?", ownerId,
				ownerType)) {
			return;
		}
		CollaborationDbUtils.update(
				"INSERT INTO BRAIN_SETTINGS (OWNER_ID, OWNER_TYPE, FILE_AT, ASK_AT, VERSION, UPDATED_AT) "
						+ "VALUES (?, ?, ?, ?, ?, ?)",
				ownerId, ownerType, DEFAULT_FILE_AT, DEFAULT_ASK_AT, 1, CollaborationDbUtils.now());
	}
}
