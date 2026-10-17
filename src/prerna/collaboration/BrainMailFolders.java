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
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;

// The owner's Outlook folders: which are read, which the owner made, and the topics they stand for. Folders are
// matched by id only; names are shown to the owner and given to the models as the owner's own grouping.
public final class BrainMailFolders {

	private static final Logger classLogger = LogManager.getLogger(BrainMailFolders.class);

	static final String CUSTOM = "custom";
	static final String SKIPPED = "skipped";
	static final String SENT = "sentitems";
	// system folders mail is read from; every other system folder (and what is under it) is skipped
	private static final Set<String> READ = Set.of("inbox", "sentitems", "archive", "scheduled");
	// a topic link made by onboarding from the folder, or by the owner
	static final String FROM_OUTLOOK = "outlook";
	// classifier version on thread links filed because of a folder
	static final String FILED = "folder";
	// the tree is read again after this, or sooner when mail turns up in a folder it does not know
	static final Duration STALE = Duration.ofHours(12);
	private static final Duration REREAD_AFTER = Duration.ofMinutes(10);
	private static final int MAX_PATH = 1000;

	record Folder(String id, String parentId, String name, String path, String kind, int total) {
	}

	/** One owner's folders by id; empty when the mail source has none (fixtures) or it was never read. */
	record Tree(Map<String, Folder> byId, Timestamp updatedAt) {

		boolean isEmpty() {
			return byId.isEmpty();
		}

		boolean knows(String folderId) {
			return folderId != null && byId.containsKey(folderId);
		}

		boolean skipped(String folderId) {
			Folder folder = folderId == null ? null : byId.get(folderId);
			return folder != null && SKIPPED.equals(folder.kind());
		}

		boolean sent(String folderId) {
			Folder folder = folderId == null ? null : byId.get(folderId);
			return folder != null && SENT.equals(folder.kind());
		}

		// the path of a folder the owner made, null for system folders and ones not known
		String custom(String folderId) {
			Folder folder = folderId == null ? null : byId.get(folderId);
			return folder != null && CUSTOM.equals(folder.kind()) ? folder.path() : null;
		}

		String path(String folderId) {
			Folder folder = folderId == null ? null : byId.get(folderId);
			return folder == null ? null : folder.path();
		}
	}

	private BrainMailFolders() {
	}

	// ---- the tree ----

	/** Reads the folder tree from the mailbox and saves it; null when the source has no folders or the read failed. */
	static Tree refresh(User user, BrainMailHeaderSource source, String ownerId, String ownerType) {
		List<Map<String, Object>> rows;
		try {
			rows = source.folders(user);
		} catch (Exception e) {
			classLogger.warn("Could not read the Outlook folders; reading Inbox and Sent only: {}", e.getMessage());
			return null;
		}
		if (rows == null || rows.isEmpty()) {
			return null;
		}
		Tree tree = build(rows, CollaborationDbUtils.now());
		CollaborationDbUtils.batch(conn -> {
			CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_MAIL_FOLDER WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
					ownerId, ownerType);
			for (Folder f : tree.byId().values()) {
				CollaborationDbUtils.update(conn,
						"INSERT INTO BRAIN_MAIL_FOLDER (OWNER_ID, OWNER_TYPE, FOLDER_ID, PARENT_ID, NAME, PATH, KIND, "
								+ "TOTAL, UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
						ownerId, ownerType, f.id(), f.parentId(), clip(f.name(), 255), f.path(), f.kind(), f.total(),
						tree.updatedAt());
			}
		});
		return tree;
	}

	/** The saved tree, read again first when it is older than STALE; never null. */
	static Tree current(User user, BrainMailHeaderSource source, String ownerId, String ownerType) {
		Tree saved = load(ownerId, ownerType);
		if (!saved.isEmpty() && saved.updatedAt() != null && saved.updatedAt().toLocalDateTime()
				.toInstant(ZoneOffset.UTC).plus(STALE).isAfter(java.time.Instant.now())) {
			return saved;
		}
		Tree fresh = refresh(user, source, ownerId, ownerType);
		return fresh != null ? fresh : saved;
	}

	static Tree load(String ownerId, String ownerType) {
		Map<String, Folder> byId = new LinkedHashMap<>();
		Timestamp[] at = { null };
		CollaborationDbUtils.query("SELECT FOLDER_ID, PARENT_ID, NAME, PATH, KIND, TOTAL, UPDATED_AT FROM "
				+ "BRAIN_MAIL_FOLDER WHERE OWNER_ID = ? AND OWNER_TYPE = ? ORDER BY PATH", rs -> {
					Integer total = CollaborationDbUtils.getInteger(rs, "TOTAL");
					byId.put(rs.getString(1), new Folder(rs.getString(1), rs.getString(2), rs.getString(3),
							CollaborationDbUtils.getString(rs, "PATH"), rs.getString(5), total == null ? 0 : total));
					at[0] = rs.getTimestamp(7);
					return null;
				}, ownerId, ownerType);
		return new Tree(byId, at[0]);
	}

	// kinds and paths from Graph's rows; a folder under a skipped one is skipped too
	static Tree build(List<Map<String, Object>> rows, Timestamp at) {
		Map<String, Map<String, Object>> raw = new LinkedHashMap<>();
		for (Map<String, Object> row : rows) {
			if (row.get("id") instanceof String id) {
				raw.putIfAbsent(id, row);
			}
		}
		Map<String, Folder> byId = new LinkedHashMap<>();
		for (String id : raw.keySet()) {
			Map<String, Object> self = raw.get(id);
			String own = self.get("wellKnownName") instanceof String w ? w : null;
			// a system folder is its own kind; one the owner made is skipped under a skipped system folder
			String kind = own != null ? (READ.contains(own) ? own : SKIPPED) : CUSTOM;
			List<String> names = new ArrayList<>();
			Set<String> seen = new HashSet<>();
			for (String up = id; up != null && raw.containsKey(up) && seen.add(up);) {
				Map<String, Object> f = raw.get(up);
				names.add(0, Objects.toString(f.get("displayName"), ""));
				if (f.get("wellKnownName") instanceof String w && !READ.contains(w)) {
					kind = SKIPPED;
				}
				up = f.get("parentFolderId") instanceof String p ? p : null;
			}
			String path = clip(String.join(" / ", names), MAX_PATH);
			int total = self.get("totalItemCount") instanceof Number n ? n.intValue() : 0;
			byId.put(id, new Folder(id, self.get("parentFolderId") instanceof String p ? p : null,
					Objects.toString(self.get("displayName"), ""), path, kind, total));
		}
		return new Tree(byId, at);
	}

	/**
	 * The headers to import: no drafts and nothing from a skipped folder. Sent mail loses its internet headers, as
	 * when Sent was read on its own (they are the owner's, not a sender's machine-sent marks).
	 */
	static List<Map<String, Object>> keep(Tree tree, List<Map<String, Object>> headers) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> header : headers) {
			String folder = header.get("parentFolderId") instanceof String f ? f : null;
			if (Boolean.TRUE.equals(header.get("isDraft")) || tree.skipped(folder)) {
				continue;
			}
			if (tree.sent(folder) && header.containsKey("internetMessageHeaders")) {
				header = new LinkedHashMap<>(header);
				header.remove("internetMessageHeaders");
			}
			out.add(header);
		}
		return out;
	}

	/**
	 * The tree, read again when a header sits in a folder it does not know (made since it was read), at most once
	 * every few minutes: a hidden folder stays unknown however often it is read.
	 */
	static Tree covering(User user, BrainMailHeaderSource source, String ownerId, String ownerType, Tree tree,
			List<Map<String, Object>> headers) {
		boolean unknown = false;
		for (Map<String, Object> header : headers) {
			unknown |= header.get("parentFolderId") instanceof String f && !tree.knows(f);
		}
		if (!unknown || tree.isEmpty() || (tree.updatedAt() != null && tree.updatedAt().toLocalDateTime()
				.toInstant(ZoneOffset.UTC).plus(REREAD_AFTER).isAfter(java.time.Instant.now()))) {
			return tree;
		}
		Tree fresh = refresh(user, source, ownerId, ownerType);
		return fresh != null ? fresh : tree;
	}

	/** Folders read, folders the owner made, and when the tree was read; for Connections. */
	static Map<String, Object> summary(String ownerId, String ownerType) {
		Tree tree = load(ownerId, ownerType);
		Map<String, Object> out = new LinkedHashMap<>();
		int custom = 0;
		int read = 0;
		for (Folder f : tree.byId().values()) {
			custom += CUSTOM.equals(f.kind()) ? 1 : 0;
			read += SKIPPED.equals(f.kind()) ? 0 : 1;
		}
		out.put("known", !tree.isEmpty());
		out.put("read", read);
		out.put("custom", custom);
		out.put("linked", linkedTopics(ownerId, ownerType).size());
		out.put("updatedAt", CollaborationDbUtils.toIso(tree.updatedAt()));
		return out;
	}

	/** The folders the owner made, for linking one to a topic: id, path, total, and the topics linked to it. */
	static List<Map<String, Object>> list(String ownerId, String ownerType) {
		Map<String, List<String>> topics = new HashMap<>();
		CollaborationDbUtils.query("SELECT FOLDER_ID, TOPIC_ID FROM BRAIN_TOPIC_FOLDER WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ?", rs -> topics.computeIfAbsent(rs.getString(1), k -> new ArrayList<>())
						.add(rs.getString(2)), ownerId, ownerType);
		List<Map<String, Object>> out = new ArrayList<>();
		for (Folder f : load(ownerId, ownerType).byId().values()) {
			if (!CUSTOM.equals(f.kind())) {
				continue;
			}
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("id", f.id());
			row.put("name", f.name());
			row.put("path", f.path());
			row.put("total", f.total());
			row.put("topicIds", topics.getOrDefault(f.id(), List.of()));
			out.add(row);
		}
		return out;
	}

	// ---- topic links ----

	/** Saved and dormant topics by the folders linked to them; a folder linked to two topics files under neither. */
	static Map<String, String> linkedTopics(String ownerId, String ownerType) {
		Map<String, Set<String>> byFolder = new HashMap<>();
		CollaborationDbUtils.query("SELECT f.FOLDER_ID, f.TOPIC_ID FROM BRAIN_TOPIC_FOLDER f JOIN BRAIN_TOPIC t ON "
				+ "t.OWNER_ID = f.OWNER_ID AND t.OWNER_TYPE = f.OWNER_TYPE AND t.TOPIC_ID = f.TOPIC_ID WHERE "
				+ "f.OWNER_ID = ? AND f.OWNER_TYPE = ? AND t.STATUS IN (?, ?)",
				rs -> byFolder.computeIfAbsent(rs.getString(1), k -> new HashSet<>()).add(rs.getString(2)), ownerId,
				ownerType, BrainTopicUtils.ACTIVE, BrainTopicUtils.DORMANT);
		Map<String, String> out = new HashMap<>();
		byFolder.forEach((folder, topics) -> {
			if (topics.size() == 1) {
				out.put(folder, topics.iterator().next());
			}
		});
		return out;
	}

	/** Folder paths by topic, for the classifier's topic descriptions and the topic pages. */
	static Map<String, List<String>> topicFolders(String ownerId, String ownerType) {
		Tree tree = load(ownerId, ownerType);
		Map<String, List<String>> out = new HashMap<>();
		CollaborationDbUtils.query("SELECT TOPIC_ID, FOLDER_ID FROM BRAIN_TOPIC_FOLDER WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? ORDER BY CREATED_AT", rs -> {
					String path = tree.path(rs.getString(2));
					if (path != null) {
						out.computeIfAbsent(rs.getString(1), k -> new ArrayList<>()).add(path);
					}
					return null;
				}, ownerId, ownerType);
		return out;
	}

	/** The topic for the folder the thread's newest mail in a linked folder sits in; null when there is none. */
	static String topicFor(String ownerId, String ownerType, String threadId, Map<String, String> linked) {
		if (linked.isEmpty()) {
			return null;
		}
		for (String folder : CollaborationDbUtils.query("SELECT FOLDER FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND THREAD_ID = ? AND FOLDER IS NOT NULL ORDER BY RECEIVED_AT DESC",
				rs -> rs.getString(1), ownerId, ownerType, threadId)) {
			if (linked.containsKey(folder)) {
				return linked.get(folder);
			}
		}
		return null;
	}

	/**
	 * What the mailbox says about a thread, for the classifier: the folders the owner filed it in (theirs, newest
	 * first), their categories, whether they flagged it, and whether a sender marked it important or attached
	 * files. Null when there is nothing to say.
	 */
	static Map<String, Object> facts(String ownerId, String ownerType, String threadId, Tree tree) {
		Set<String> folders = new LinkedHashSet<>();
		Set<String> categories = new LinkedHashSet<>();
		boolean[] flags = new boolean[3];
		CollaborationDbUtils.query("SELECT FOLDER, CATEGORIES_JSON, FLAGGED, IMPORTANCE, ATTACHMENTS FROM "
				+ "BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? ORDER BY RECEIVED_AT DESC",
				rs -> {
					String path = tree.custom(rs.getString(1));
					if (path != null) {
						folders.add(path);
					}
					categories.addAll(CollaborationDbUtils
							.toStringList(CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "CATEGORIES_JSON"))));
					flags[0] |= Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "FLAGGED"));
					flags[1] |= "high".equalsIgnoreCase(rs.getString(4));
					flags[2] |= Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "ATTACHMENTS"));
					return null;
				}, ownerId, ownerType, threadId);
		Map<String, Object> out = new LinkedHashMap<>();
		if (!folders.isEmpty()) {
			out.put("folders", folders.stream().limit(3).toList());
		}
		if (!categories.isEmpty()) {
			out.put("categories", categories.stream().limit(5).toList());
		}
		if (flags[0]) {
			out.put("flaggedByOwner", true);
		}
		if (flags[1]) {
			out.put("markedImportant", true);
		}
		if (flags[2]) {
			out.put("hasAttachments", true);
		}
		return out.isEmpty() ? null : out;
	}

	/** A topic's folders: id and path, oldest link first. */
	static List<Map<String, Object>> ofTopic(String ownerId, String ownerType, String topicId) {
		Tree tree = load(ownerId, ownerType);
		return CollaborationDbUtils.query("SELECT FOLDER_ID, ORIGIN FROM BRAIN_TOPIC_FOLDER WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND TOPIC_ID = ? ORDER BY CREATED_AT", rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("id", rs.getString(1));
					// a folder deleted in Outlook since keeps its link until the owner removes it
					row.put("path", Objects.requireNonNullElse(tree.path(rs.getString(1)), "(folder not found)"));
					row.put("origin", rs.getString(2));
					return row;
				}, ownerId, ownerType, topicId);
	}

	/**
	 * The owner links one of their folders to a topic (or removes the link). Mail already in the folder is filed
	 * under the topic, as new mail there will be.
	 */
	public static Map<String, Object> linkTopic(User user, String topicId, String folderId, boolean remove) {
		var owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
		if (folderId == null || folderId.isBlank()) {
			throw new IllegalArgumentException("Choose a folder");
		}
		int filed = 0;
		if (remove) {
			CollaborationDbUtils.update("DELETE FROM BRAIN_TOPIC_FOLDER WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND "
					+ "TOPIC_ID = ? AND FOLDER_ID = ?", ownerId, ownerType, topicId, folderId);
		} else {
			if (load(ownerId, ownerType).custom(folderId) == null) {
				throw new IllegalArgumentException("That is not one of your Outlook folders");
			}
			CollaborationDbUtils.batch(conn -> {
				if (!CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC_FOLDER WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND TOPIC_ID = ? AND FOLDER_ID = ?", ownerId, ownerType, topicId, folderId)) {
					CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_TOPIC_FOLDER (OWNER_ID, OWNER_TYPE, TOPIC_ID, "
							+ "FOLDER_ID, ORIGIN, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?)", ownerId, ownerType, topicId,
							folderId, BrainProfileUtils.YOU, CollaborationDbUtils.now());
				}
			});
			filed = BrainThreadClassifier.fileByFolder(ownerId, ownerType, CollaborationDbUtils.query(
					"SELECT DISTINCT THREAD_ID FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND FOLDER = ? "
							+ "AND THREAD_ID IS NOT NULL", rs -> rs.getString(1), ownerId, ownerType, folderId));
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("topicId", topicId);
		out.put("folders", ofTopic(ownerId, ownerType, topicId));
		out.put("filed", filed);
		return out;
	}

	/** The owner's folders to pick from, and what is read; for the topic page and Connections. */
	public static Map<String, Object> listFor(User user) {
		var owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("folders", list(owner.getValue0(), owner.getValue1()));
		out.put("summary", summary(owner.getValue0(), owner.getValue1()));
		return out;
	}

	// ---- message metadata ----

	// the owner's categories on a header, as stored; null when none
	static String categories(Map<String, Object> header) {
		if (header.get("categories") instanceof List<?> list && !list.isEmpty()) {
			List<String> out = new ArrayList<>();
			for (Object c : list) {
				if (c instanceof String s && !s.isBlank()) {
					out.add(clip(s.trim(), 100));
				}
			}
			return out.isEmpty() ? null : CollaborationDbUtils.toJson(out);
		}
		return null;
	}

	static boolean flagged(Map<String, Object> header) {
		return header.get("flag") instanceof Map<?, ?> flag && "flagged".equals(flag.get("flagStatus"));
	}

	static String importance(Map<String, Object> header) {
		return header.get("importance") instanceof String i ? clip(i.toLowerCase(java.util.Locale.ROOT), 20) : null;
	}

	static Boolean attachments(Map<String, Object> header) {
		return header.get("hasAttachments") instanceof Boolean b ? b : null;
	}

	private static String clip(String value, int max) {
		return value == null || value.length() <= max ? value : value.substring(0, max);
	}
}
