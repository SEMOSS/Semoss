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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;

import prerna.auth.User;

// Owner and source health: COLLAB_OWNER, SOURCE_CONNECTION, SOURCE_SYNC_STATE, INBOUND_EVENT
public final class CollaborationSourceUtils {

	public static final String EVENT_PENDING = "PENDING";
	public static final String EVENT_DONE = "DONE";
	public static final String EVENT_FAILED = "FAILED";
	public static final String EVENT_SKIPPED = "SKIPPED";

	private CollaborationSourceUtils() {

	}

	// pilot sources in display order: id, label, badge, scope, permission
	private static final String[][] SOURCE_CATALOG = {
			{ "email", "Outlook mail", "E", "Inbox + Sent Items, last 90 days", "Mail.Read (delegated)" },
			{ "calendar", "Outlook calendar", "C", "Primary calendar, -30 / +60 days", "Calendars.Read (delegated)" },
			{ "teams", "Teams chats", "T", "1:1 and group chats", "Chat.Read (delegated)" },
			{ "teamsChannels", "Teams channels", "T", "Not in pilot scope", "ChannelMessage.Read.All" },
			{ "files", "OneDrive / SharePoint", "F", "Attachments only for now", "Sites.Selected" } };

	public static boolean isKnownSource(String source) {
		for (String[] entry : SOURCE_CATALOG) {
			if (entry[0].equals(source)) {
				return true;
			}
		}
		return false;
	}

	// SourceStatus[] from the reactor contract: every catalog source, "off" until it has a connection row
	public static List<Map<String, Object>> getSourceStatuses(String ownerId, String ownerType) {
		Map<String, Map<String, Object>> rows = new HashMap<>();
		for (Map<String, Object> row : getSources(ownerId, ownerType)) {
			rows.put((String) row.get("id"), row);
		}
		List<Map<String, Object>> statuses = new ArrayList<>();
		for (String[] entry : SOURCE_CATALOG) {
			Map<String, Object> row = rows.get(entry[0]);
			String status = "off";
			if (row != null && Boolean.TRUE.equals(row.get("enabled"))) {
				status = Boolean.TRUE.equals(row.get("reauthNeeded")) ? "needs_permission" : "connected";
			}
			Map<String, Object> source = new LinkedHashMap<>();
			source.put("id", entry[0]);
			source.put("label", entry[1]);
			source.put("mark", entry[2]);
			source.put("status", status);
			source.put("lastSync", row == null ? null : row.get("lastEventAt"));
			source.put("scope", entry[3]);
			source.put("volume", null);
			source.put("permission", entry[4]);
			statuses.add(source);
		}
		return statuses;
	}

	// source id -> enabled, for Settings.sourcesJson
	public static Map<String, Boolean> getSourcesEnabled(String ownerId, String ownerType) {
		Map<String, Boolean> enabled = new LinkedHashMap<>();
		for (Map<String, Object> source : getSourceStatuses(ownerId, ownerType)) {
			enabled.put((String) source.get("id"), !"off".equals(source.get("status")));
		}
		return enabled;
	}

	// ---- owner ----

	public static Map<String, Object> getOwner(String ownerId, String ownerType) {
		return CollaborationDbUtils.queryOne(
				"SELECT MS_USER_ID, MS_UPN, STATUS, CREATED_AT, UPDATED_AT FROM COLLAB_OWNER "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("ownerId", ownerId);
					row.put("ownerType", ownerType);
					row.put("msUserId", CollaborationDbUtils.getString(rs, "MS_USER_ID"));
					row.put("msUpn", CollaborationDbUtils.getString(rs, "MS_UPN"));
					row.put("status", CollaborationDbUtils.getString(rs, "STATUS"));
					row.put("createdAt", CollaborationDbUtils.getTimestamp(rs, "CREATED_AT"));
					row.put("updatedAt", CollaborationDbUtils.getTimestamp(rs, "UPDATED_AT"));
					return row;
				}, ownerId, ownerType);
	}

	public static void saveOwner(String ownerId, String ownerType, String msUserId, String msUpn, String status) {
		int updated = CollaborationDbUtils.update(
				"UPDATE COLLAB_OWNER SET MS_USER_ID = ?, MS_UPN = ?, STATUS = ?, UPDATED_AT = ? "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				msUserId, msUpn, status, CollaborationDbUtils.now(), ownerId, ownerType);
		if (updated == 0) {
			CollaborationDbUtils.update(
					"INSERT INTO COLLAB_OWNER (OWNER_ID, OWNER_TYPE, MS_USER_ID, MS_UPN, STATUS, CREATED_AT, UPDATED_AT) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?)",
					ownerId, ownerType, msUserId, msUpn, status, CollaborationDbUtils.now(),
					CollaborationDbUtils.now());
		}
	}

	// webhooks arrive with the Microsoft user id; resolve it to the SEMOSS owner (id, type)
	public static Pair<String, String> findOwnerByMsUserId(String msUserId) {
		return CollaborationDbUtils.queryOne("SELECT OWNER_ID, OWNER_TYPE FROM COLLAB_OWNER WHERE MS_USER_ID = ?",
				rs -> Pair.with(rs.getString("OWNER_ID"), rs.getString("OWNER_TYPE")), msUserId);
	}

	// ---- source connections (one row per owner and source: email, calendar, teams) ----

	public static List<Map<String, Object>> getSources(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		return getSources(owner.getValue0(), owner.getValue1());
	}

	public static List<Map<String, Object>> getSources(String ownerId, String ownerType) {
		return CollaborationDbUtils.query(
				"SELECT SOURCE, ENABLED, LAST_EVENT_AT, LAST_ERROR, LAST_ERROR_AT, REAUTH_NEEDED FROM SOURCE_CONNECTION "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? ORDER BY SOURCE",
				rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("id", CollaborationDbUtils.getString(rs, "SOURCE"));
					row.put("enabled", CollaborationDbUtils.getBoolean(rs, "ENABLED"));
					row.put("lastEventAt", CollaborationDbUtils.getTimestamp(rs, "LAST_EVENT_AT"));
					row.put("lastError", CollaborationDbUtils.getString(rs, "LAST_ERROR"));
					row.put("lastErrorAt", CollaborationDbUtils.getTimestamp(rs, "LAST_ERROR_AT"));
					row.put("reauthNeeded", CollaborationDbUtils.getBoolean(rs, "REAUTH_NEEDED"));
					return row;
				}, ownerId, ownerType);
	}

	public static void setSourceEnabled(String ownerId, String ownerType, String source, boolean enabled) {
		int updated = CollaborationDbUtils.update(
				"UPDATE SOURCE_CONNECTION SET ENABLED = ?, UPDATED_AT = ? "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND SOURCE = ?",
				enabled, CollaborationDbUtils.now(), ownerId, ownerType, source);
		if (updated == 0) {
			insertSource(ownerId, ownerType, source, enabled);
		}
	}

	// a notification or delta page arrived; clears any earlier error
	public static void recordSourceEvent(String ownerId, String ownerType, String source) {
		int updated = CollaborationDbUtils.update(
				"UPDATE SOURCE_CONNECTION SET LAST_EVENT_AT = ?, LAST_ERROR = NULL, LAST_ERROR_AT = NULL, "
						+ "REAUTH_NEEDED = ?, UPDATED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND SOURCE = ?",
				CollaborationDbUtils.now(), false, CollaborationDbUtils.now(), ownerId, ownerType, source);
		if (updated == 0) {
			insertSource(ownerId, ownerType, source, true);
			recordSourceEvent(ownerId, ownerType, source);
		}
	}

	public static void recordSourceError(String ownerId, String ownerType, String source, String error,
			boolean reauthNeeded) {
		int updated = CollaborationDbUtils.update(
				"UPDATE SOURCE_CONNECTION SET LAST_ERROR = ?, LAST_ERROR_AT = ?, REAUTH_NEEDED = ?, UPDATED_AT = ? "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND SOURCE = ?",
				error, CollaborationDbUtils.now(), reauthNeeded, CollaborationDbUtils.now(), ownerId, ownerType,
				source);
		if (updated == 0) {
			insertSource(ownerId, ownerType, source, true);
			recordSourceError(ownerId, ownerType, source, error, reauthNeeded);
		}
	}

	private static void insertSource(String ownerId, String ownerType, String source, boolean enabled) {
		CollaborationDbUtils.update(
				"INSERT INTO SOURCE_CONNECTION (OWNER_ID, OWNER_TYPE, SOURCE, ENABLED, REAUTH_NEEDED, CREATED_AT, "
						+ "UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)",
				ownerId, ownerType, source, enabled, false, CollaborationDbUtils.now(),
				CollaborationDbUtils.now());
	}

	// ---- delta sync checkpoints ----

	public static String getDeltaLink(String ownerId, String ownerType, String source, String folder) {
		return CollaborationDbUtils.queryOne(
				"SELECT DELTA_LINK FROM SOURCE_SYNC_STATE "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND SOURCE = ? AND FOLDER = ?",
				rs -> CollaborationDbUtils.getString(rs, "DELTA_LINK"), ownerId, ownerType, source, folder);
	}

	public static void saveDeltaLink(String ownerId, String ownerType, String source, String folder,
			String deltaLink) {
		int updated = CollaborationDbUtils.update(
				"UPDATE SOURCE_SYNC_STATE SET DELTA_LINK = ?, LAST_SYNC_AT = ? "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND SOURCE = ? AND FOLDER = ?",
				deltaLink, CollaborationDbUtils.now(), ownerId, ownerType, source, folder);
		if (updated == 0) {
			CollaborationDbUtils.update(
					"INSERT INTO SOURCE_SYNC_STATE (OWNER_ID, OWNER_TYPE, SOURCE, FOLDER, DELTA_LINK, LAST_SYNC_AT) "
							+ "VALUES (?, ?, ?, ?, ?, ?)",
					ownerId, ownerType, source, folder, deltaLink, CollaborationDbUtils.now());
		}
	}

	// ---- inbound events ----

	// returns the new event id, or null if this event key was already recorded (duplicate delivery)
	public static String recordInboundEvent(String ownerId, String ownerType, String provider, String eventKey,
			String source, String resourceLocator, String changeType) {
		if (CollaborationDbUtils.exists(
				"SELECT 1 FROM INBOUND_EVENT WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND EVENT_KEY = ?", ownerId,
				ownerType, eventKey)) {
			return null;
		}
		String eventId = CollaborationDbUtils.deterministicId(ownerId, ownerType, eventKey);
		CollaborationDbUtils.update(
				"INSERT INTO INBOUND_EVENT (OWNER_ID, OWNER_TYPE, EVENT_ID, PROVIDER, EVENT_KEY, SOURCE, "
						+ "RESOURCE_LOCATOR, CHANGE_TYPE, STATUS, ATTEMPT_COUNT, RECEIVED_AT) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				ownerId, ownerType, eventId, provider, eventKey, source, resourceLocator, changeType, EVENT_PENDING, 0,
				CollaborationDbUtils.now());
		return eventId;
	}

	public static void markInboundEvent(String ownerId, String ownerType, String eventId, String status,
			String runId, String errorSummary) {
		CollaborationDbUtils.update(
				"UPDATE INBOUND_EVENT SET STATUS = ?, RUN_ID = ?, ERROR_SUMMARY = ?, "
						+ "ATTEMPT_COUNT = ATTEMPT_COUNT + 1, PROCESSED_AT = ? "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND EVENT_ID = ?",
				status, runId, errorSummary, CollaborationDbUtils.now(), ownerId, ownerType, eventId);
	}
}
