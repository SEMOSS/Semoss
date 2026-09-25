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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;

import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.User;

// Brain "you": BRAIN_PROFILE, BRAIN_SETTINGS, and the overview counts
public final class BrainProfileUtils {

	public static final int DEFAULT_FILE_AT = 85;
	public static final int DEFAULT_ASK_AT = 40;

	// ROLE_STATE and STYLE_STATE values
	public static final String LEARNED = "learned";
	public static final String CONFIRMED = "confirmed";
	public static final String YOU = "you";

	private static final Gson GSON = new Gson();

	private BrainProfileUtils() {

	}

	// ---- profile ----

	// creates the row on first read, seeded from the login's name and email
	public static Map<String, Object> getProfile(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		ensureProfile(owner.getValue0(), owner.getValue1(), user);
		return getProfile(owner.getValue0(), owner.getValue1());
	}

	public static Map<String, Object> getProfile(String ownerId, String ownerType) {
		Map<String, Object> profile = CollaborationDbUtils.queryOne(
				"SELECT DISPLAY_NAME, EMAIL, ORG, ROLE, ROLE_STATE, ROLE_NOTE, TIMEZONE, WORKING_HOURS_JSON, "
						+ "STYLE_SUMMARY, STYLE_STATE, STYLE_EXAMPLES_JSON FROM BRAIN_PROFILE "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				BrainProfileUtils::mapProfile, ownerId, ownerType);
		if (profile != null) {
			profile.put("vips", getVipIds(ownerId, ownerType));
		}
		return profile;
	}

	// partial Profile: only keys present are written; a field the owner types becomes source "you"
	@SuppressWarnings("unchecked")
	public static Map<String, Object> saveProfile(User user, Map<String, Object> changes) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		ensureProfile(ownerId, ownerType, user);

		List<String> sets = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		setIfPresent(changes, "name", "DISPLAY_NAME", sets, params);
		setIfPresent(changes, "email", "EMAIL", sets, params);
		setIfPresent(changes, "org", "ORG", sets, params);
		setIfPresent(changes, "timezone", "TIMEZONE", sets, params);
		setIfPresent(changes, "workingHours", "WORKING_HOURS_JSON", sets, params);
		if (changes.get("role") instanceof Map) {
			Map<String, Object> role = (Map<String, Object>) changes.get("role");
			if (role.containsKey("value")) {
				addSet(sets, params, "ROLE", asString(role.get("value")));
				addSet(sets, params, "ROLE_STATE", YOU);
			}
			setIfPresent(role, "note", "ROLE_NOTE", sets, params);
		}
		if (changes.get("style") instanceof Map) {
			Map<String, Object> style = (Map<String, Object>) changes.get("style");
			if (style.containsKey("summary")) {
				addSet(sets, params, "STYLE_SUMMARY", asString(style.get("summary")));
				addSet(sets, params, "STYLE_STATE", YOU);
			} else if (style.containsKey("confirmed")) {
				addSet(sets, params, "STYLE_STATE", Boolean.TRUE.equals(style.get("confirmed")) ? CONFIRMED : LEARNED);
			}
			if (style.containsKey("examples")) {
				addSet(sets, params, "STYLE_EXAMPLES_JSON", GSON.toJson(style.get("examples")));
			}
		}
		List<String> vips = changes.get("vips") instanceof List ? toStringList((List<Object>) changes.get("vips"))
				: null;

		CollaborationDbUtils.inTransaction(conn -> {
			if (!sets.isEmpty()) {
				addSet(sets, params, "UPDATED_AT", CollaborationDbUtils.now());
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
		CollaborationDbUtils.update(
				"INSERT INTO BRAIN_PROFILE (OWNER_ID, OWNER_TYPE, DISPLAY_NAME, EMAIL, ROLE_STATE, STYLE_STATE, "
						+ "UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)",
				ownerId, ownerType, token == null ? null : token.getName(), token == null ? null : token.getEmail(),
				LEARNED, LEARNED, CollaborationDbUtils.now());
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

		// learned = suggested, confirmed = learned and accepted, you = typed by the owner
		String styleState = CollaborationDbUtils.getString(rs, "STYLE_STATE");
		Map<String, Object> style = new LinkedHashMap<>();
		style.put("summary", CollaborationDbUtils.getString(rs, "STYLE_SUMMARY"));
		style.put("source", YOU.equals(styleState) ? YOU : LEARNED);
		style.put("confirmed", YOU.equals(styleState) || CONFIRMED.equals(styleState));
		style.put("examples", parseList(CollaborationDbUtils.getString(rs, "STYLE_EXAMPLES_JSON")));

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

	private static List<String> getVipIds(String ownerId, String ownerType) {
		return CollaborationDbUtils.query(
				"SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND IS_VIP = ? "
						+ "ORDER BY PERSON_ID",
				rs -> rs.getString("PERSON_ID"), ownerId, ownerType, true);
	}

	// ---- settings ----

	public static Map<String, Object> getSettings(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		return getSettings(owner.getValue0(), owner.getValue1());
	}

	// creates the row with FILE_AT 85 / ASK_AT 40 on first read (DEC-05)
	public static Map<String, Object> getSettings(String ownerId, String ownerType) {
		ensureSettings(ownerId, ownerType);
		Map<String, Object> settings = CollaborationDbUtils.queryOne(
				"SELECT CLASSIFIER_ENGINE_ID, FILE_AT, ASK_AT, WEIGHTS_JSON, VERSION FROM BRAIN_SETTINGS "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("classifierEngineId", CollaborationDbUtils.getString(rs, "CLASSIFIER_ENGINE_ID"));
					row.put("fileAt", CollaborationDbUtils.getInteger(rs, "FILE_AT"));
					row.put("askAt", CollaborationDbUtils.getInteger(rs, "ASK_AT"));
					row.put("sourcesJson", null);
					row.put("weightsJson", parseMap(CollaborationDbUtils.getString(rs, "WEIGHTS_JSON")));
					row.put("version", CollaborationDbUtils.getInteger(rs, "VERSION"));
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
		if (changes.containsKey("version") && toInt(changes.get("version"), "version") != currentVersion) {
			throw new IllegalArgumentException("Settings were changed elsewhere; reload and try again");
		}

		int fileAt = changes.containsKey("fileAt") ? toInt(changes.get("fileAt"), "fileAt")
				: (Integer) current.get("fileAt");
		int askAt = changes.containsKey("askAt") ? toInt(changes.get("askAt"), "askAt")
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
		addSet(sets, params, "FILE_AT", fileAt);
		addSet(sets, params, "ASK_AT", askAt);
		setIfPresent(changes, "classifierEngineId", "CLASSIFIER_ENGINE_ID", sets, params);
		if (changes.containsKey("weightsJson")) {
			Object weights = changes.get("weightsJson");
			addSet(sets, params, "WEIGHTS_JSON", weights == null ? null : GSON.toJson(weights));
		}
		addSet(sets, params, "VERSION", currentVersion + 1);
		addSet(sets, params, "UPDATED_AT", CollaborationDbUtils.now());
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

	// ---- overview ----

	public static Map<String, Object> getOverview(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Map<String, Object> full = getProfile(user);
		Map<String, Object> profile = new LinkedHashMap<>();
		for (String key : new String[] { "id", "name", "email", "org" }) {
			profile.put(key, full.get(key));
		}

		Map<String, Object> counts = new LinkedHashMap<>();
		counts.put("topics", CollaborationDbUtils.count("SELECT COUNT(*) FROM BRAIN_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND (STATUS IS NULL OR STATUS <> ?)", ownerId, ownerType,
				"archived"));
		counts.put("threads", CollaborationDbUtils.count(
				"SELECT COUNT(*) FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ?", ownerId, ownerType));
		counts.put("people", CollaborationDbUtils.count(
				"SELECT COUNT(*) FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ?", ownerId, ownerType));

		Map<String, Object> overview = new LinkedHashMap<>();
		overview.put("profile", profile);
		overview.put("sources", CollaborationSourceUtils.getSourceStatuses(ownerId, ownerType));
		overview.put("counts", counts);
		overview.put("openReviewCount", CollaborationDbUtils.count(
				"SELECT COUNT(*) FROM BRAIN_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND STATUS = ?", ownerId,
				ownerType, "open"));
		return overview;
	}

	// ---- helpers ----

	private static void setIfPresent(Map<String, Object> changes, String key, String column, List<String> sets,
			List<Object> params) {
		if (changes.containsKey(key)) {
			addSet(sets, params, column, asString(changes.get(key)));
		}
	}

	private static void addSet(List<String> sets, List<Object> params, String column, Object value) {
		sets.add(column + " = ?");
		params.add(value);
	}

	private static String asString(Object value) {
		return value == null ? null : String.valueOf(value);
	}

	// pixel numbers arrive as Integer or Double
	private static int toInt(Object value, String name) {
		if (value instanceof Number number) {
			return number.intValue();
		}
		try {
			return Integer.parseInt(String.valueOf(value).trim());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(name + " must be a number");
		}
	}

	private static List<String> toStringList(List<Object> values) {
		List<String> strings = new ArrayList<>();
		for (Object value : values) {
			if (value != null) {
				strings.add(String.valueOf(value));
			}
		}
		return strings;
	}

	@SuppressWarnings("unchecked")
	private static List<Object> parseList(String json) {
		return json == null ? new ArrayList<>() : GSON.fromJson(json, List.class);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> parseMap(String json) {
		return json == null ? null : GSON.fromJson(json, Map.class);
	}
}
