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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;

/**
 * A person's persona for their Digital Twins (BRAIN_PERSONA, one row per owner): how they sound, how they decide and
 * what a twin must leave to them, in eight short sections of plain text. Every twin the person created is given it as
 * a section of its prompt, ahead of the twin's rules, unless they switched it off for that twin. Only its owner reads
 * or changes it here; the people a twin is shared with never get it from a reactor.
 */
public final class WorkTwinPersona {

	private static final Logger classLogger = LogManager.getLogger(WorkTwinPersona.class);

	static final String LOCK = "persona";

	/**
	 * The sections and the most characters each may hold. The order is which to keep when a prompt has no room for all
	 * of them: what the twin must leave alone first, examples last.
	 */
	static final Map<String, Integer> SECTIONS = sections();

	/** The most characters of persona text a twin's prompt is given. */
	static final int MAX_PROMPT = 6000;

	private WorkTwinPersona() {
	}

	private record Row(Map<String, String> sections, List<String> offTwins, String updatedAt) {
	}

	/**
	 * The signed-in person's persona.
	 *
	 * @return {@code exists}, whether any section holds text; {@code sections}, the text of every section by its key,
	 *         empty where nothing is written; {@code offTwins}, the ids of the twins they switched it off for;
	 *         {@code interview}, where the interview that writes it is, or null when there is none; and
	 *         {@code updatedAt}
	 */
	public static Map<String, Object> get(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Row row = read(owner.getValue0(), owner.getValue1());
		Map<String, String> sections = new LinkedHashMap<>();
		for (String key : SECTIONS.keySet()) {
			sections.put(key, row == null ? "" : row.sections().getOrDefault(key, ""));
		}
		Map<String, Object> persona = new LinkedHashMap<>();
		persona.put("exists", row != null && !row.sections().isEmpty());
		persona.put("sections", sections);
		persona.put("offTwins", row == null ? List.of() : row.offTwins());
		persona.put("interview", WorkTwinPersonaInterview.summary(owner.getValue0(), owner.getValue1()));
		persona.put("updatedAt", row == null ? null : row.updatedAt());
		return persona;
	}

	/**
	 * Writes sections of the signed-in person's persona. A section that is passed replaces what was there, and one
	 * left out stays as it is. Their twins use the result from their next chat.
	 *
	 * @param changes the new text by section key; blank text empties a section
	 * @throws IllegalArgumentException for a key that is not a section, and when no section would hold text
	 */
	public static Map<String, Object> save(User user, Map<String, Object> changes) {
		if (changes == null || changes.isEmpty() || !SECTIONS.keySet().containsAll(changes.keySet())) {
			throw new IllegalArgumentException("sections takes " + String.join(", ", SECTIONS.keySet()));
		}
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Row row = read(ownerId, ownerType);
			Map<String, String> sections = new LinkedHashMap<>(row == null ? Map.of() : row.sections());
			for (Map.Entry<String, Object> change : changes.entrySet()) {
				String text = clean(CollaborationDbUtils.asString(change.getValue()), SECTIONS.get(change.getKey()));
				if (text.isEmpty()) {
					sections.remove(change.getKey());
				} else {
					sections.put(change.getKey(), text);
				}
			}
			if (sections.isEmpty()) {
				throw new IllegalArgumentException("Write at least one section, or delete the persona");
			}
			write(ownerId, ownerType, "BRIEF_JSON", CollaborationDbUtils.toJson(sections));
		}
		return get(user);
	}

	/**
	 * Removes the signed-in person's persona and any interview in progress. The twins it was switched off for stay
	 * on record, so a persona written later does not come back on for them.
	 */
	public static Map<String, Object> delete(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		synchronized (CollaborationDbUtils.ownerLock(LOCK, owner.getValue0(), owner.getValue1())) {
			CollaborationDbUtils.update(
					"UPDATE BRAIN_PERSONA SET BRIEF_JSON = NULL, INTERVIEW_JSON = NULL, UPDATED_AT = ? "
							+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
					CollaborationDbUtils.now(), owner.getValue0(), owner.getValue1());
		}
		return get(user);
	}

	/**
	 * Switches the signed-in person's persona on or off for one of their twins. A twin uses it until it is switched
	 * off. The caller checks that the twin is theirs.
	 */
	public static Map<String, Object> setTwin(User user, String projectId, boolean enabled) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Row row = read(ownerId, ownerType);
			List<String> off = new ArrayList<>(row == null ? List.of() : row.offTwins());
			boolean changed = enabled ? off.remove(projectId) : !off.contains(projectId) && off.add(projectId);
			if (changed) {
				write(ownerId, ownerType, "OFF_TWINS_JSON", CollaborationDbUtils.toJson(off));
			}
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("project", projectId);
		result.put("enabled", enabled);
		return result;
	}

	/**
	 * The persona a Digital Twin's run is given, as a section of its prompt. It is the persona of the person who
	 * created the twin, and it stays theirs: it stops going to a twin they can no longer change. Never fails a run.
	 *
	 * @param projectId the twin
	 * @param ownerName the display name of the person it answers about, as its rules name them
	 * @return the section, or null when the twin runs without one: Collaboration is off, the twin's creator is not
	 *         on record with their login type, they have no persona or switched it off for this twin, they can no
	 *         longer change the twin, or it could not be read
	 */
	public static String promptBlock(String projectId, String ownerName) {
		try {
			if (projectId == null || !CollaborationDbUtils.isInitalized()) {
				return null;
			}
			Pair<String, String> creator = SecurityProjectUtils.getProjectCreator(projectId);
			// an id without its login type could be another person's
			if (creator == null || creator.getValue1() == null) {
				return null;
			}
			Integer level = SecurityProjectUtils.getUserProjectPermission(creator.getValue0(), projectId);
			if (level == null || level > AccessPermissionEnum.EDIT.getId()) {
				return null;
			}
			return block(creator.getValue0(), creator.getValue1(), projectId, ownerName);
		} catch (Exception e) {
			classLogger.warn("Could not read the persona for Digital Twin {}; it runs without one", projectId, e);
			return null;
		}
	}

	// the section for one owner's twin; null when they have no persona or switched it off for this twin
	static String block(String ownerId, String ownerType, String projectId, String ownerName) {
		Row row = read(ownerId, ownerType);
		if (row == null || row.offTwins().contains(projectId)) {
			return null;
		}
		Map<String, String> kept = new LinkedHashMap<>();
		int room = MAX_PROMPT;
		for (String key : SECTIONS.keySet()) {
			String text = fit(row.sections().get(key), room);
			if (text != null) {
				kept.put(key, text);
				room -= text.length();
			}
		}
		return CollaborationPrompts.twinPersona(ownerName, kept);
	}

	// the text when it fits, else its whole lines that do; null when none does
	private static String fit(String text, int room) {
		if (text == null || text.length() <= room) {
			return text;
		}
		int cut = text.lastIndexOf('\n', room);
		return cut <= 0 ? null : text.substring(0, cut).strip();
	}

	/**
	 * Plain text of at most max characters, with no line that could pass for a heading of the prompt it goes into.
	 */
	static String clean(String text, int max) {
		if (text == null) {
			return "";
		}
		String plain = text.replace("\r\n", "\n").replace('\r', '\n').replaceAll("(?m)^[ \\t]*#+[ \\t]*", "")
				.replaceAll("\\n{3,}", "\n\n").strip();
		return plain.length() > max ? plain.substring(0, max).strip() : plain;
	}

	// One column of the owner's row, which is made the first time anything is written. Callers hold the persona lock.
	static void write(String ownerId, String ownerType, String column, String json) {
		Timestamp now = CollaborationDbUtils.now();
		int updated = CollaborationDbUtils.update(
				"UPDATE BRAIN_PERSONA SET " + column + " = ?, UPDATED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ?", json,
				now, ownerId, ownerType);
		if (updated == 0) {
			CollaborationDbUtils.update("INSERT INTO BRAIN_PERSONA (OWNER_ID, OWNER_TYPE, " + column
					+ ", CREATED_AT, UPDATED_AT) VALUES (?, ?, ?, ?, ?)", ownerId, ownerType, json, now, now);
		}
	}

	private static Row read(String ownerId, String ownerType) {
		return CollaborationDbUtils.queryOne(
				"SELECT BRIEF_JSON, OFF_TWINS_JSON, UPDATED_AT FROM BRAIN_PERSONA WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> new Row(stored(CollaborationDbUtils.getString(rs, "BRIEF_JSON")),
						CollaborationDbUtils.toStringList(
								CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "OFF_TWINS_JSON"))),
						CollaborationDbUtils.getTimestamp(rs, "UPDATED_AT")),
				ownerId, ownerType);
	}

	// The sections a row holds, cleaned again on the way out: what is stored is never trusted to be a prompt's text.
	// A row that cannot be read holds none, so its owner can write the persona again.
	private static Map<String, String> stored(String json) {
		Map<String, String> sections = new LinkedHashMap<>();
		Map<String, Object> saved;
		try {
			saved = CollaborationDbUtils.parseMap(json);
		} catch (RuntimeException e) {
			classLogger.warn("A stored persona could not be read: {}", e.getMessage());
			return sections;
		}
		if (saved != null) {
			for (Map.Entry<String, Integer> section : SECTIONS.entrySet()) {
				String text = clean(CollaborationDbUtils.asString(saved.get(section.getKey())), section.getValue());
				if (!text.isEmpty()) {
					sections.put(section.getKey(), text);
				}
			}
		}
		return sections;
	}

	private static Map<String, Integer> sections() {
		Map<String, Integer> caps = new LinkedHashMap<>();
		caps.put("limits", 900);
		caps.put("voice", 900);
		caps.put("short", 400);
		caps.put("stands", 1200);
		caps.put("decide", 700);
		caps.put("people", 500);
		caps.put("review", 500);
		caps.put("examples", 1500);
		return Collections.unmodifiableMap(caps);
	}
}
