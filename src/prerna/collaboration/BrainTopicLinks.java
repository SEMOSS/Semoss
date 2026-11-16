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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import prerna.auth.User;

/** Owner-scoped pointers to OneDrive, Teams, or SharePoint folders linked to topics. */
public final class BrainTopicLinks {

	public static final String DRIVE_FOLDER = "drive_folder";
	public static final String CHANNEL_FILES = "channel_files";
	public static final String SHAREPOINT_FOLDER = "sharepoint_folder";
	public static final String OK = "ok";
	public static final String NO_ACCESS = "no_access";

	private static final String LOCK = "topic-link";
	private static final String ON_TOPIC = " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?";
	private static final int MAX_DISPLAY_NAME = 255;

	private BrainTopicLinks() {
	}

	/**
	 * Folder identifiers and metadata obtained from Microsoft Graph using the owner's delegated token.
	 * The caller must validate the folder before saving it.
	 */
	public record FolderPointer(String kind, String driveId, String itemId, String teamId, String channelId,
			String siteId, String displayName, String webUrl) {

		private Identity identity() {
			return new Identity(kind, driveId, itemId, teamId, channelId, siteId);
		}
	}

	private record Identity(String kind, String driveId, String itemId, String teamId, String channelId,
			String siteId) {
	}

	private record StoredLink(String linkId, FolderPointer folder) {
	}

	/** Lists a topic's linked folders, scoped to the signed-in owner. */
	public static List<Map<String, Object>> list(User user, String topicId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		BrainTopicUtils.requireTopic(owner.getValue0(), owner.getValue1(), topicId);
		return ofTopic(owner.getValue0(), owner.getValue1(), topicId);
	}

	/** Saves a folder pointer after its metadata and access have been verified with Microsoft Graph. */
	public static Map<String, Object> link(User user, String topicId, FolderPointer folder) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
		folder = validate(topicId, folder);

		String linkId = linkId(ownerId, ownerType, topicId, folder);
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			if (!CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC_LINK WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND LINK_ID = ?", ownerId, ownerType, linkId)) {
				CollaborationDbUtils.update("INSERT INTO BRAIN_TOPIC_LINK (OWNER_ID, OWNER_TYPE, LINK_ID, TOPIC_ID, "
						+ "KIND, DRIVE_ID, ITEM_ID, TEAM_ID, CHANNEL_ID, SITE_ID, DISPLAY_NAME, WEB_URL, STATE, "
						+ "CHECKED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
						ownerId, ownerType, linkId, topicId, folder.kind(), folder.driveId(), folder.itemId(),
						folder.teamId(), folder.channelId(), folder.siteId(), clip(folder.displayName(), MAX_DISPLAY_NAME),
						folder.webUrl(), OK, CollaborationDbUtils.now());
			}
		}
		return result(topicId, ofTopic(ownerId, ownerType, topicId));
	}

	/** Removes one of the signed-in owner's links from the specified topic. */
	public static Map<String, Object> unlink(User user, String topicId, String linkId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
		requireText(linkId, "A linked folder id is required");

		CollaborationDbUtils.update("DELETE FROM BRAIN_TOPIC_LINK" + ON_TOPIC + " AND LINK_ID = ?", ownerId,
				ownerType, topicId, linkId);
		return result(topicId, ofTopic(ownerId, ownerType, topicId));
	}

	/** Records the most recent live access check for a linked folder. */
	public static void updateState(User user, String topicId, String linkId, String state) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
		requireText(linkId, "A linked folder id is required");
		if (!OK.equals(state) && !NO_ACCESS.equals(state)) {
			throw new IllegalArgumentException("Folder state must be ok or no_access");
		}
		int updated = CollaborationDbUtils.update("UPDATE BRAIN_TOPIC_LINK SET STATE = ?, CHECKED_AT = ?"
				+ ON_TOPIC + " AND LINK_ID = ?", state, CollaborationDbUtils.now(), ownerId, ownerType, topicId,
				linkId);
		if (updated == 0) {
			throw new IllegalArgumentException("Linked folder not found");
		}
	}

	static List<Map<String, Object>> ofTopic(String ownerId, String ownerType, String topicId) {
		return CollaborationDbUtils.query("SELECT LINK_ID, KIND, DRIVE_ID, ITEM_ID, TEAM_ID, CHANNEL_ID, SITE_ID, "
				+ "DISPLAY_NAME, WEB_URL, STATE, CHECKED_AT FROM BRAIN_TOPIC_LINK" + ON_TOPIC
				+ " ORDER BY DISPLAY_NAME, LINK_ID", rs -> {
					Map<String, Object> link = new LinkedHashMap<>();
					link.put("linkId", rs.getString("LINK_ID"));
					link.put("kind", rs.getString("KIND"));
					link.put("driveId", rs.getString("DRIVE_ID"));
					link.put("itemId", rs.getString("ITEM_ID"));
					link.put("teamId", rs.getString("TEAM_ID"));
					link.put("channelId", rs.getString("CHANNEL_ID"));
					link.put("siteId", rs.getString("SITE_ID"));
					link.put("displayName", rs.getString("DISPLAY_NAME"));
					link.put("webUrl", CollaborationDbUtils.getString(rs, "WEB_URL"));
					link.put("state", rs.getString("STATE"));
					link.put("checkedAt", CollaborationDbUtils.getTimestamp(rs, "CHECKED_AT"));
					return link;
				}, ownerId, ownerType, topicId);
	}

	/** Moves source-topic links to the target, retaining the target row for duplicate folder pointers. */
	static void merge(Connection conn, String ownerId, String ownerType, String sourceTopicId, String targetTopicId)
			throws SQLException {
		List<StoredLink> source = links(conn, ownerId, ownerType, sourceTopicId);
		Set<Identity> targetFolders = new HashSet<>();
		for (StoredLink row : links(conn, ownerId, ownerType, targetTopicId)) {
			targetFolders.add(row.folder().identity());
		}

		for (StoredLink row : source) {
			if (!targetFolders.add(row.folder().identity())) {
				CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_TOPIC_LINK WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND LINK_ID = ?", ownerId, ownerType, row.linkId());
				continue;
			}
			CollaborationDbUtils.update(conn, "UPDATE BRAIN_TOPIC_LINK SET TOPIC_ID = ?, LINK_ID = ? WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND LINK_ID = ?", targetTopicId,
					linkId(ownerId, ownerType, targetTopicId, row.folder()), ownerId, ownerType, row.linkId());
		}
	}

	private static List<StoredLink> links(Connection conn, String ownerId, String ownerType, String topicId)
			throws SQLException {
		return CollaborationDbUtils.query(conn, "SELECT LINK_ID, KIND, DRIVE_ID, ITEM_ID, TEAM_ID, CHANNEL_ID, "
				+ "SITE_ID, DISPLAY_NAME, WEB_URL FROM BRAIN_TOPIC_LINK" + ON_TOPIC + " ORDER BY LINK_ID", rs -> {
					FolderPointer folder = new FolderPointer(rs.getString("KIND"), rs.getString("DRIVE_ID"),
							rs.getString("ITEM_ID"), rs.getString("TEAM_ID"), rs.getString("CHANNEL_ID"),
							rs.getString("SITE_ID"), rs.getString("DISPLAY_NAME"),
							CollaborationDbUtils.getString(rs, "WEB_URL"));
					return new StoredLink(rs.getString("LINK_ID"), folder);
				}, ownerId, ownerType, topicId);
	}

	private static String linkId(String ownerId, String ownerType, String topicId, FolderPointer folder) {
		return CollaborationDbUtils.deterministicId(ownerId, ownerType, topicId, folder.kind(), folder.driveId(),
				folder.itemId(), folder.teamId(), folder.channelId(), folder.siteId());
	}

	private static FolderPointer validate(String topicId, FolderPointer folder) {
		requireText(topicId, "A topic id is required");
		if (folder == null) {
			throw new IllegalArgumentException("Folder details are required");
		}
		String kind = requireText(folder.kind(), "A folder kind is required");
		String driveId = optionalText(folder.driveId());
		String itemId = optionalText(folder.itemId());
		String teamId = optionalText(folder.teamId());
		String channelId = optionalText(folder.channelId());
		String siteId = optionalText(folder.siteId());
		switch (kind) {
		case DRIVE_FOLDER -> {
			requireText(driveId, "A drive id is required for a OneDrive folder");
			requireText(itemId, "A folder item id is required");
		}
		case CHANNEL_FILES -> {
			requireText(teamId, "A team id is required for channel files");
			requireText(channelId, "A channel id is required for channel files");
		}
		case SHAREPOINT_FOLDER -> requireText(siteId, "A site id is required for a SharePoint folder");
		default -> throw new IllegalArgumentException("Unsupported folder kind");
		}
		return new FolderPointer(kind, driveId, itemId, teamId, channelId, siteId,
				requireText(folder.displayName(), "A folder name is required"), optionalText(folder.webUrl()));
	}

	private static String optionalText(String value) {
		return value == null || value.isBlank() ? null : value.trim();
	}

	private static String requireText(String value, String message) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(message);
		}
		return value.trim();
	}

	private static String clip(String value, int maxLength) {
		return value.length() <= maxLength ? value : value.substring(0, maxLength);
	}

	private static Map<String, Object> result(String topicId, List<Map<String, Object>> links) {
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("topicId", topicId);
		out.put("folders", links);
		return out;
	}
}
