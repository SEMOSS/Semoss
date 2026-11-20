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
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveHelper;
import prerna.io.connector.ms.teams.MicrosoftTeamsHelper;
import prerna.sablecc2.om.execptions.SemossPixelException;

/** Owner-scoped pointers to OneDrive, Teams, or SharePoint folders linked to topics. */
public final class BrainTopicLinks {

	private static final Logger classLogger = LogManager.getLogger(BrainTopicLinks.class);

	public static final String DRIVE_FOLDER = "drive_folder";
	public static final String CHANNEL_FILES = "channel_files";
	public static final String SHAREPOINT_FOLDER = "sharepoint_folder";
	public static final String OK = "ok";
	public static final String NO_ACCESS = "no_access";

	private static final String LOCK = "topic-link";
	private static final String ON_TOPIC = " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?";
	private static final int MAX_DISPLAY_NAME = 255;
	private static final String INSERT_LINK = "INSERT INTO BRAIN_TOPIC_LINK (OWNER_ID, OWNER_TYPE, LINK_ID, TOPIC_ID, "
			+ "KIND, DRIVE_ID, ITEM_ID, TEAM_ID, CHANNEL_ID, SITE_ID, DISPLAY_NAME, WEB_URL, STATE, CHECKED_AT) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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

	private record LinkKey(String ownerId, String ownerType, String linkId) {
	}

	private record LinkRow(String topicId, FolderPointer folder, String state, Timestamp checkedAt) {
	}

	/** Lists a topic's linked folders, scoped to the signed-in owner. */
	public static Map<String, Object> list(User user, String topicId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
		List<Map<String, Object>> links = ofTopic(ownerId, ownerType, topicId);
		if (links.stream().anyMatch(BrainTopicLinks::isLiveMicrosoftFolder)) {
			String accessToken = accessToken(user);
			for (Map<String, Object> link : links) {
				if (!isLiveMicrosoftFolder(link)) {
					continue;
				}
				String linkId = String.valueOf(link.get("linkId"));
				try {
					String state = isAccessible(accessToken, link) ? OK : NO_ACCESS;
					updateState(user, topicId, linkId, state);
				} catch (Exception e) {
					if (isUnavailable(e)) {
						classLogger.warn("Microsoft folder link {} is no longer available: {}", linkId,
								e.getMessage());
						updateState(user, topicId, linkId, NO_ACCESS);
					} else {
						throw new SemossPixelException("Could not check access to linked Microsoft folder "
								+ link.get("displayName") + ": " + e.getMessage());
					}
				}
			}
			links = ofTopic(ownerId, ownerType, topicId);
		}
		return result(topicId, links);
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
			if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC_LINK WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND LINK_ID = ?", ownerId, ownerType, linkId)) {
				if (refreshLink(ownerId, ownerType, topicId, linkId, folder) == 0) {
					insertLink(ownerId, ownerType, topicId, linkId, folder);
				}
			} else {
				try {
					insertLink(ownerId, ownerType, topicId, linkId, folder);
				} catch (IllegalStateException e) {
					if (!isUniqueConstraintViolation(e)
							|| !CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC_LINK WHERE OWNER_ID = ? "
									+ "AND OWNER_TYPE = ? AND LINK_ID = ?", ownerId, ownerType, linkId)
							|| refreshLink(ownerId, ownerType, topicId, linkId, folder) == 0) {
						throw e;
					}
				}
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
		int updated = updateState(ownerId, ownerType, topicId, linkId, state);
		if (updated == 0) {
			throw new IllegalArgumentException("Linked folder not found");
		}
	}

	private static int updateState(String ownerId, String ownerType, String topicId, String linkId, String state) {
		return CollaborationDbUtils.update("UPDATE BRAIN_TOPIC_LINK SET STATE = ?, CHECKED_AT = ?" + ON_TOPIC
				+ " AND LINK_ID = ?", state, CollaborationDbUtils.now(), ownerId, ownerType, topicId, linkId);
	}

	private static boolean isLiveMicrosoftFolder(Map<String, Object> link) {
		Object kind = link.get("kind");
		return DRIVE_FOLDER.equals(kind) || CHANNEL_FILES.equals(kind);
	}

	private static String accessToken(User user) {
		try {
			return MicrosoftLoginUtils.getValidAccessToken(user);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			throw new SemossPixelException("Could not retrieve a Microsoft access token: " + e.getMessage());
		}
	}

	private static boolean isAccessible(String accessToken, Map<String, Object> link) throws Exception {
		String kind = String.valueOf(link.get("kind"));
		if (DRIVE_FOLDER.equals(kind)) {
			Map<String, Object> item = MicrosoftOneDriveHelper.getItem(accessToken,
					String.valueOf(link.get("driveId")), String.valueOf(link.get("itemId")), null);
			return Boolean.TRUE.equals(item.get("isFolder"));
		}
		if (CHANNEL_FILES.equals(kind)) {
			MicrosoftTeamsHelper.getChannelFilesFolder(accessToken, String.valueOf(link.get("teamId")),
					String.valueOf(link.get("channelId")));
			return true;
		}
		return false;
	}

	private static boolean isUnavailable(Throwable error) {
		for (Throwable cause = error; cause != null; cause = cause.getCause()) {
			String message = cause.getMessage();
			if (message != null
					&& (message.contains(" returned HTTP 403") || message.contains(" returned HTTP 404"))) {
				return true;
			}
		}
		return false;
	}

	private static void insertLink(String ownerId, String ownerType, String topicId, String linkId,
			FolderPointer folder) {
		insertLink(ownerId, ownerType, linkId, topicId, folder, OK, CollaborationDbUtils.now());
	}

	private static void insertLink(String ownerId, String ownerType, String linkId, String topicId,
			FolderPointer folder, String state, Timestamp checkedAt) {
		CollaborationDbUtils.update(INSERT_LINK,
				ownerId, ownerType, linkId, topicId, folder.kind(), folder.driveId(), folder.itemId(), folder.teamId(),
				folder.channelId(), folder.siteId(), clip(folder.displayName(), MAX_DISPLAY_NAME), folder.webUrl(),
				state, checkedAt);
	}

	private static void insertLink(Connection conn, String ownerId, String ownerType, String linkId, String topicId,
			FolderPointer folder, String state, Timestamp checkedAt) throws SQLException {
		CollaborationDbUtils.update(conn, INSERT_LINK, ownerId, ownerType, linkId, topicId, folder.kind(),
				folder.driveId(), folder.itemId(), folder.teamId(), folder.channelId(), folder.siteId(),
				clip(folder.displayName(), MAX_DISPLAY_NAME), folder.webUrl(), state, checkedAt);
	}

	private static int refreshLink(String ownerId, String ownerType, String topicId, String linkId,
			FolderPointer folder) {
		return CollaborationDbUtils.update("UPDATE BRAIN_TOPIC_LINK SET TOPIC_ID = ?, KIND = ?, DRIVE_ID = ?, "
				+ "ITEM_ID = ?, TEAM_ID = ?, CHANNEL_ID = ?, SITE_ID = ?, DISPLAY_NAME = ?, WEB_URL = ?, STATE = ?, "
				+ "CHECKED_AT = ?" + ON_TOPIC + " AND LINK_ID = ?", topicId, folder.kind(), folder.driveId(),
				folder.itemId(), folder.teamId(), folder.channelId(), folder.siteId(),
				clip(folder.displayName(), MAX_DISPLAY_NAME), folder.webUrl(), OK, CollaborationDbUtils.now(),
				ownerId, ownerType, topicId, linkId);
	}

	private static boolean isUniqueConstraintViolation(Throwable error) {
		for (Throwable cause = error; cause != null; cause = cause.getCause()) {
			if (cause instanceof SQLException sqlException && sqlException.getSQLState() != null
					&& sqlException.getSQLState().startsWith("23")) {
				return true;
			}
		}
		return false;
	}

	/** Consolidates duplicate link ids created before the unique database index is installed. */
	static void deduplicate(Connection conn) throws SQLException {
		List<LinkKey> duplicates = CollaborationDbUtils.query(conn, "SELECT OWNER_ID, OWNER_TYPE, LINK_ID "
				+ "FROM BRAIN_TOPIC_LINK GROUP BY OWNER_ID, OWNER_TYPE, LINK_ID HAVING COUNT(*) > 1",
				rs -> new LinkKey(rs.getString("OWNER_ID"), rs.getString("OWNER_TYPE"), rs.getString("LINK_ID")));
		if (duplicates.isEmpty()) {
			return;
		}

		boolean autoCommit = conn.getAutoCommit();
		if (autoCommit) {
			conn.setAutoCommit(false);
		}
		try {
			for (LinkKey key : duplicates) {
				List<LinkRow> rows = CollaborationDbUtils.query(conn, "SELECT TOPIC_ID, KIND, DRIVE_ID, ITEM_ID, "
						+ "TEAM_ID, CHANNEL_ID, SITE_ID, DISPLAY_NAME, WEB_URL, STATE, CHECKED_AT "
						+ "FROM BRAIN_TOPIC_LINK WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND LINK_ID = ?",
						rs -> new LinkRow(rs.getString("TOPIC_ID"),
								new FolderPointer(rs.getString("KIND"), rs.getString("DRIVE_ID"),
										rs.getString("ITEM_ID"), rs.getString("TEAM_ID"), rs.getString("CHANNEL_ID"),
										rs.getString("SITE_ID"), rs.getString("DISPLAY_NAME"),
										CollaborationDbUtils.getString(rs, "WEB_URL")),
								rs.getString("STATE"), rs.getTimestamp("CHECKED_AT")),
						key.ownerId(), key.ownerType(), key.linkId());
				if (rows.size() < 2) {
					continue;
				}
				LinkRow latest = rows.get(0);
				for (LinkRow row : rows) {
					if (row.checkedAt() != null
							&& (latest.checkedAt() == null || row.checkedAt().after(latest.checkedAt()))) {
						latest = row;
					}
				}
				CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_TOPIC_LINK WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND LINK_ID = ?", key.ownerId(), key.ownerType(), key.linkId());
				insertLink(conn, key.ownerId(), key.ownerType(), key.linkId(), latest.topicId(), latest.folder(),
						latest.state(), latest.checkedAt());
			}
			if (autoCommit) {
				conn.commit();
			}
		} catch (SQLException | RuntimeException e) {
			if (autoCommit) {
				conn.rollback();
			}
			throw e;
		} finally {
			if (autoCommit) {
				conn.setAutoCommit(true);
			}
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
