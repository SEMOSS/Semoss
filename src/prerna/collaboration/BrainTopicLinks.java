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

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.execptions.SemossPixelException;

/** Microsoft 365 files linked to a topic. The database holds their Graph address and metadata, never contents. */
public final class BrainTopicLinks {
	private static final Logger LOG = LogManager.getLogger(BrainTopicLinks.class);
	private static final String OWNED = " WHERE OWNER_ID = ? AND OWNER_TYPE = ?";
	private static final String COLUMNS = "LINK_ID, TOPIC_ID, DRIVE_ID, ITEM_ID, PARENT_ID, NAME, LOCATION, WEB_URL, "
			+ "SIZE, CTAG, MODIFIED_AT, STATUS, LINKED_AT";
	private static final Pattern GRAPH_ID = Pattern.compile("[A-Za-z0-9!_.,:@\\-]{1,255}");
	private static final int MAX_PER_TOPIC = 100;
	private static final int BROWSE_LIMIT = 200;
	static final String OK = "ok", MISSING = "missing", NO_ACCESS = "no_access";

	private BrainTopicLinks() {}

	private record Link(String id, String topicId, String driveId, String itemId, String parentId, String name,
			String location, String webUrl, Long size, String cTag, Timestamp modifiedAt, String status, Timestamp linkedAt) {
		Map<String, Object> describe() {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("linkId", id); row.put("topicId", topicId); row.put("source", source(webUrl));
			row.put("name", name); row.put("location", location); row.put("webUrl", webUrl);
			row.put("size", size); row.put("modifiedAt", CollaborationDbUtils.toIso(modifiedAt));
			row.put("status", status); row.put("linkedAt", CollaborationDbUtils.toIso(linkedAt));
			row.put("driveId", driveId); row.put("itemId", itemId); row.put("parentId", parentId);
			return row;
		}

		// the same link with the item's current metadata
		Link with(Map<String, Object> item, String status) {
			if (item == null) return new Link(id, topicId, driveId, itemId, parentId, name, location, webUrl, size, cTag,
					modifiedAt, status, linkedAt);
			Object itemSize = item.get("size");
			return new Link(id, topicId, driveId, String.valueOf(item.get("id")), text(item.get("parentId")),
					Objects.requireNonNullElse(text(item.get("name")), name), locationOf(item), text(item.get("webUrl")),
					itemSize instanceof Number number ? number.longValue() : null, text(item.get("cTag")),
					CollaborationDbUtils.toTimestamp(item.get("lastModifiedDateTime"), "lastModifiedDateTime"), status,
					linkedAt);
		}
	}

	private static Link map(ResultSet rs) throws SQLException {
		long size = rs.getLong("SIZE");
		return new Link(rs.getString("LINK_ID"), rs.getString("TOPIC_ID"), rs.getString("DRIVE_ID"),
				rs.getString("ITEM_ID"), rs.getString("PARENT_ID"), rs.getString("NAME"),
				CollaborationDbUtils.getString(rs, "LOCATION"), CollaborationDbUtils.getString(rs, "WEB_URL"),
				rs.wasNull() ? null : size, rs.getString("CTAG"), rs.getTimestamp("MODIFIED_AT"),
				rs.getString("STATUS"), rs.getTimestamp("LINKED_AT"));
	}

	/** Links on the given topics, newest first; the caller has already scoped the topics to the owner or chat. */
	static List<Map<String, Object>> rows(User user, List<String> topics) {
		if (topics.isEmpty()) return List.of();
		var owner = CollaborationDbUtils.ownerOf(user);
		List<Object> params = new ArrayList<>(List.of(owner.getValue0(), owner.getValue1())); params.addAll(topics);
		return CollaborationDbUtils.query("SELECT " + COLUMNS + " FROM BRAIN_TOPIC_LINK" + OWNED + " AND TOPIC_ID IN ("
				+ CollaborationDbUtils.placeholders(topics.size()) + ") ORDER BY LINKED_AT DESC, LINK_ID",
				rs -> map(rs).describe(), params.toArray());
	}

	public static Map<String, Object> link(User user, String topicId, String shareUrl, String driveId, String itemId) {
		var owner = CollaborationDbUtils.ownerOf(user);
		BrainTopicFiles.writable(user, topicId);
		BrainDriveSource source = BrainDriveSource.current();
		Map<String, Object> item;
		try {
			if (shareUrl != null) item = source.resolve(user, requireUrl(shareUrl));
			else item = source.item(user, requireId(driveId), requireId(itemId));
		} catch (BrainDriveSource.Unavailable e) {
			throw new IllegalArgumentException("That file was not found, or you do not have access to it");
		} catch (SemossPixelException | IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			LOG.warn("Could not read a file to link to topic {}", topicId, e);
			throw new IllegalStateException("Could not read the file from Microsoft 365. Try again later.");
		}
		if (Boolean.TRUE.equals(item.get("isFolder"))) throw new IllegalArgumentException("Link a file, not a folder");
		String drive = requireId(text(item.get("driveId"))), id = requireId(text(item.get("id")));
		validateName(text(item.get("name")));
		return BrainTopicReviewProfiles.serialized(owner.getValue0(), owner.getValue1(), () -> {
			CollaborationDbUtils.query("SELECT TOPIC_ID FROM BRAIN_TOPIC" + OWNED + " AND TOPIC_ID = ? FOR UPDATE",
					rs -> rs.getString(1), owner.getValue0(), owner.getValue1(), topicId);
			BrainTopicFiles.writable(user, topicId);
			Link existing = CollaborationDbUtils.queryOne("SELECT " + COLUMNS + " FROM BRAIN_TOPIC_LINK" + OWNED
					+ " AND TOPIC_ID = ? AND DRIVE_ID = ? AND ITEM_ID = ?", BrainTopicLinks::map, owner.getValue0(),
					owner.getValue1(), topicId, drive, id);
			if (existing != null) {
				Map<String, Object> result = existing.describe();
				result.put("alreadyLinked", true);
				return result;
			}
			if (CollaborationDbUtils.count("SELECT COUNT(*) FROM BRAIN_TOPIC_LINK" + OWNED + " AND TOPIC_ID = ?",
					owner.getValue0(), owner.getValue1(), topicId) >= MAX_PER_TOPIC)
				throw new IllegalArgumentException("A topic can link at most " + MAX_PER_TOPIC + " files");
			Link link = new Link(UUID.randomUUID().toString(), topicId, drive, id, null, null, null, null, null, null,
					null, OK, CollaborationDbUtils.now()).with(item, OK);
			CollaborationDbUtils.update("INSERT INTO BRAIN_TOPIC_LINK (OWNER_ID, OWNER_TYPE, " + COLUMNS
					+ ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", owner.getValue0(), owner.getValue1(),
					link.id(), link.topicId(), link.driveId(), link.itemId(), link.parentId(), link.name(),
					link.location(), link.webUrl(), link.size(), link.cTag(), link.modifiedAt(), link.status(),
					link.linkedAt());
			return link.describe();
		});
	}

	public static Map<String, Object> unlink(User user, String topicId, String linkId) {
		var owner = CollaborationDbUtils.ownerOf(user);
		return BrainTopicReviewProfiles.serialized(owner.getValue0(), owner.getValue1(), () -> {
			BrainTopicFiles.writable(user, topicId);
			if (CollaborationDbUtils.update("DELETE FROM BRAIN_TOPIC_LINK" + OWNED + " AND TOPIC_ID = ? AND LINK_ID = ?",
					owner.getValue0(), owner.getValue1(), topicId, linkId) == 0)
				throw new IllegalArgumentException("Linked file not found");
			return Map.of("linkId", linkId, "unlinked", true);
		});
	}

	/** Re-reads every link on an owned topic, or on the topics linked to an owned chat, from Microsoft 365. */
	public static Map<String, Object> refresh(User user, String topicId, String roomId) {
		var owner = CollaborationDbUtils.ownerOf(user);
		List<String> topics;
		if (roomId != null) topics = BrainTopicBrief.chatTopics(user, roomId);
		else {
			BrainTopicUtils.requireTopic(owner.getValue0(), owner.getValue1(), topicId);
			topics = List.of(topicId);
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("refreshed", true);
		if (!topics.isEmpty()) {
			List<Object> params = new ArrayList<>(List.of(owner.getValue0(), owner.getValue1())); params.addAll(topics);
			List<Link> links = CollaborationDbUtils.query("SELECT " + COLUMNS + " FROM BRAIN_TOPIC_LINK" + OWNED
					+ " AND TOPIC_ID IN (" + CollaborationDbUtils.placeholders(topics.size()) + ")", BrainTopicLinks::map,
					params.toArray());
			BrainDriveSource source = BrainDriveSource.current();
			for (Link link : links) {
				try {
					refreshOne(user, source, link);
				} catch (SemossPixelException e) {
					// not signed in to Microsoft: keep the last known metadata
					result.put("refreshed", false);
					result.put("error", "Connect Microsoft 365 to refresh linked files");
					break;
				} catch (RuntimeException e) {
					LOG.warn("Could not refresh linked topic file {}", link.id(), e);
					result.put("refreshed", false);
					result.put("error", "Some linked files could not be refreshed");
				}
			}
		}
		result.put("links", rows(user, topics));
		return result;
	}

	/** Brings a linked file into an owned chat, downloading only when Microsoft 365 holds different content. */
	public static Map<String, Object> copyToRoom(Insight insight, String topicId, String linkId, String roomId) {
		User user = insight.getUser();
		var owner = CollaborationDbUtils.ownerOf(user);
		if (!BrainTopicBrief.chatTopics(user, roomId).contains(topicId))
			throw new IllegalArgumentException("Topic is not linked to this chat");
		Link stored = CollaborationDbUtils.queryOne("SELECT " + COLUMNS + " FROM BRAIN_TOPIC_LINK" + OWNED
				+ " AND TOPIC_ID = ? AND LINK_ID = ?", BrainTopicLinks::map, owner.getValue0(), owner.getValue1(),
				topicId, linkId);
		if (stored == null) throw new IllegalArgumentException("Linked file not found");
		BrainDriveSource source = BrainDriveSource.current();
		Link link = refreshOne(user, source, stored);
		if (NO_ACCESS.equals(link.status()))
			throw new IllegalArgumentException(link.name() + " is linked to this topic, but you no longer have access to it");
		if (MISSING.equals(link.status()))
			throw new IllegalArgumentException(link.name() + " was moved or deleted in Microsoft 365; link it to the topic again");
		validateName(link.name());
		long maxBytes = BrainAttachments.maxBytes();
		if (link.size() != null && link.size() > maxBytes)
			throw new IllegalArgumentException(link.name() + " exceeds the topic attachment size limit");
		Path root = Path.of(RoomUtils.getOrLoadRoom(roomId, insight).getRoomFolderPath()).toAbsolutePath().normalize();
		synchronized (CollaborationDbUtils.ownerLock("topic-link:" + roomId, owner.getValue0(), owner.getValue1())) {
			try {
				Path folder = folder(root, linkId);
				// the cTag and name of the copy already in this chat
				Path version = folder.resolve(".version");
				List<String> held = Files.isRegularFile(version, LinkOption.NOFOLLOW_LINKS)
						? Files.readAllLines(version, StandardCharsets.UTF_8) : List.of();
				String heldTag = held.isEmpty() ? null : held.get(0);
				Path heldFile = held.size() < 2 || !isValidName(held.get(1)) ? null : folder.resolve(held.get(1));
				Path target = folder.resolve(link.name());
				boolean current = link.cTag() != null && link.cTag().equals(heldTag) && heldFile != null
						&& Files.isRegularFile(heldFile, LinkOption.NOFOLLOW_LINKS);
				if (current && !heldFile.equals(target)) Files.move(heldFile, target, StandardCopyOption.REPLACE_EXISTING);
				if (!current) {
					Path temporary = Files.createTempDirectory("semoss-topic-link-");
					try {
						Path fetched = source.download(user, link.driveId(), link.itemId(), temporary);
						if (Files.size(fetched) > maxBytes)
							throw new IllegalArgumentException(link.name() + " exceeds the topic attachment size limit");
						if (heldFile != null) Files.deleteIfExists(heldFile);
						Files.move(fetched, target, StandardCopyOption.REPLACE_EXISTING);
					} finally {
						FileUtils.deleteDirectory(temporary.toFile());
					}
				}
				Files.write(version, List.of(Objects.toString(link.cTag(), ""), link.name()), StandardCharsets.UTF_8);
				ClusterUtil.pushRoom(roomId);
				String relative = root.toRealPath().relativize(target).toString().replace('\\', '/');
				Map<String, Object> result = link.describe();
				result.put("fileName", link.name()); result.put("fileLocation", relative); result.put("filePath", relative);
				result.put("downloaded", !current);
				result.put("changedSinceLastRead", !current && heldTag != null);
				result.put("instruction", (!current && heldTag != null
						? "This file changed in Microsoft 365 since it was last read in this chat; earlier reads are out of date. " : "")
						+ "Read filePath with the existing file/document tools before answering; no text has been extracted yet.");
				return result;
			} catch (IllegalArgumentException e) {
				throw e;
			} catch (SemossPixelException e) {
				throw e;
			} catch (Exception e) {
				if (e instanceof InterruptedException) Thread.currentThread().interrupt();
				LOG.warn("Could not bring linked topic file {} into room {}", linkId, roomId, e);
				throw new IllegalStateException("Could not download " + link.name() + " from Microsoft 365. Try again "
						+ "later, or open it with its webUrl.");
			}
		}
	}

	/** Places the topic picker can open: drives, folders, Teams, sites, shared files and search, all through the owner's token. */
	public static Map<String, Object> browse(User user, String view, String driveId, String itemId, String teamId,
			String channelId, String siteId, String search) {
		CollaborationDbUtils.ownerOf(user);
		BrainDriveSource source = BrainDriveSource.current();
		Map<String, Object> result = new LinkedHashMap<>();
		try {
			List<Map<String, Object>> entries = new ArrayList<>();
			if (search != null) {
				if (search.length() > 255) throw new IllegalArgumentException("Search text is too long");
				entries.addAll(files(source.search(user, search, BROWSE_LIMIT)));
			} else if (driveId != null) {
				entries.addAll(files(source.children(user, requireId(driveId), itemId == null ? null : requireId(itemId),
						BROWSE_LIMIT)));
			} else if (channelId != null) {
				Map<String, Object> folder = source.channelFolder(user, requireId(teamId), requireId(channelId));
				result.put("driveId", folder.get("driveId")); result.put("itemId", folder.get("itemId"));
				entries.addAll(files(source.children(user, requireId(text(folder.get("driveId"))),
						requireId(text(folder.get("itemId"))), BROWSE_LIMIT)));
			} else if (teamId != null) {
				entries.addAll(places(source.channels(user, requireId(teamId)), "channel"));
			} else if (siteId != null) {
				entries.addAll(places(source.libraries(user, requireId(siteId)), "library"));
			} else if ("shared".equals(view)) {
				entries.addAll(files(source.shared(user, BROWSE_LIMIT)));
			} else if ("teams".equals(view)) {
				entries.addAll(places(source.teams(user), "team"));
			} else if ("sites".equals(view)) {
				entries.addAll(places(source.sites(user), "site"));
			} else if (view == null || "mine".equals(view)) {
				entries.addAll(files(source.children(user, null, null, BROWSE_LIMIT)));
			} else {
				throw new IllegalArgumentException("Unknown view: use mine, shared, teams or sites");
			}
			result.put("entries", entries);
			return result;
		} catch (BrainDriveSource.Unavailable e) {
			throw new IllegalArgumentException(e.getMessage());
		} catch (SemossPixelException | IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			LOG.warn("Could not browse Microsoft 365 files", e);
			throw new IllegalStateException("Could not read Microsoft 365. Try again later.");
		}
	}

	// re-reads one link, falls back to a same-named file in its last folder, and saves what changed
	private static Link refreshOne(User user, BrainDriveSource source, Link link) {
		Map<String, Object> item = null;
		String status = OK;
		try {
			item = source.item(user, link.driveId(), link.itemId());
		} catch (BrainDriveSource.Unavailable e) {
			if (e.forbidden()) status = NO_ACCESS;
			else {
				item = replacement(user, source, link);
				if (item == null) status = MISSING;
			}
		} catch (SemossPixelException | IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException("Could not read " + link.name() + " from Microsoft 365", e);
		}
		Link updated = link.with(item, status);
		if (!updated.equals(link)) {
			var owner = CollaborationDbUtils.ownerOf(user);
			CollaborationDbUtils.update("UPDATE BRAIN_TOPIC_LINK SET ITEM_ID = ?, PARENT_ID = ?, NAME = ?, LOCATION = ?, "
					+ "WEB_URL = ?, SIZE = ?, CTAG = ?, MODIFIED_AT = ?, STATUS = ?" + OWNED + " AND LINK_ID = ?",
					updated.itemId(), updated.parentId(), updated.name(), updated.location(), updated.webUrl(),
					updated.size(), updated.cTag(), updated.modifiedAt(), updated.status(), owner.getValue0(),
					owner.getValue1(), link.id());
		}
		return updated;
	}

	// a file deleted and uploaded again under the same name gets a new item id in the same folder
	private static Map<String, Object> replacement(User user, BrainDriveSource source, Link link) {
		if (link.parentId() == null || link.name() == null) return null;
		try {
			for (Map<String, Object> child : source.children(user, link.driveId(), link.parentId(), BROWSE_LIMIT)) {
				if (!Boolean.TRUE.equals(child.get("isFolder")) && link.name().equalsIgnoreCase(text(child.get("name")))
						&& isValidId(text(child.get("id")))) return child;
			}
		} catch (BrainDriveSource.Unavailable e) {
			return null;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			LOG.warn("Could not look for a replacement of linked topic file {}", link.id(), e);
		}
		return null;
	}

	private static List<Map<String, Object>> files(List<Map<String, Object>> items) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> item : items) {
			if (!isValidId(text(item.get("id"))) || !isValidId(text(item.get("driveId")))) continue;
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("kind", Boolean.TRUE.equals(item.get("isFolder")) ? "folder" : "file");
			entry.put("name", item.get("name")); entry.put("driveId", item.get("driveId")); entry.put("itemId", item.get("id"));
			entry.put("location", locationOf(item)); entry.put("size", item.get("size"));
			entry.put("modifiedAt", item.get("lastModifiedDateTime")); entry.put("webUrl", item.get("webUrl"));
			out.add(entry);
		}
		return out;
	}

	private static List<Map<String, Object>> places(List<Map<String, Object>> rows, String kind) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> row : rows) {
			Map<String, Object> entry = new LinkedHashMap<>(row);
			entry.put("kind", kind);
			out.add(entry);
		}
		return out;
	}

	private static Path folder(Path root, String linkId) throws IOException {
		Path realRoot = root.toRealPath();
		Path folder = realRoot.resolve("topic-links").resolve(requireId(linkId));
		Files.createDirectories(folder);
		// the chat can write to its own folder, so a planted symlink must not redirect the copy
		if (!folder.toRealPath().equals(folder)) throw new IllegalStateException("Unsafe linked file folder in this chat");
		return folder;
	}

	// the folder a file sits in, read off its web url so it says which site or OneDrive it is in
	private static String locationOf(Map<String, Object> item) {
		String webUrl = text(item.get("webUrl"));
		if (webUrl != null) {
			try {
				URI uri = URI.create(webUrl);
				String path = uri.getPath();
				if (uri.getHost() != null && path != null) {
					int slash = path.lastIndexOf('/');
					return uri.getHost() + (slash > 0 ? path.substring(0, slash) : "");
				}
			} catch (IllegalArgumentException e) {
				LOG.debug("Unparseable web url for a linked topic file", e);
			}
		}
		return text(item.get("path"));
	}

	private static String source(String webUrl) {
		if (webUrl == null) return "microsoft365";
		String url = webUrl.toLowerCase(Locale.ROOT);
		return url.contains("-my.sharepoint") || url.contains("onedrive.live.com") || url.contains("/personal/")
				? "onedrive" : "sharepoint";
	}

	private static String requireUrl(String url) {
		String trimmed = url.trim();
		if (trimmed.length() > 2048 || !trimmed.toLowerCase(Locale.ROOT).startsWith("https://"))
			throw new IllegalArgumentException("Paste an https link to a OneDrive, SharePoint or Teams file");
		try {
			if (URI.create(trimmed).getHost() == null) throw new IllegalArgumentException("Invalid link");
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Paste an https link to a OneDrive, SharePoint or Teams file");
		}
		return trimmed;
	}

	private static String requireId(String id) {
		if (!isValidId(id)) throw new IllegalArgumentException("Invalid Microsoft 365 id");
		return id;
	}

	private static boolean isValidId(String id) {
		return id != null && GRAPH_ID.matcher(id).matches() && !id.contains("..");
	}

	private static void validateName(String name) {
		if (!isValidName(name)) throw new IllegalArgumentException("Invalid file name");
	}

	private static boolean isValidName(String name) {
		return name != null && !name.isBlank() && name.length() <= 255 && !name.equals(".") && !name.equals("..")
				&& !name.startsWith(".") && !name.contains("/") && !name.contains("\\")
				&& name.chars().noneMatch(Character::isISOControl);
	}

	private static String text(Object value) {
		return value == null || String.valueOf(value).isBlank() ? null : String.valueOf(value);
	}
}
