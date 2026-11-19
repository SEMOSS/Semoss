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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveHelper;
import prerna.io.connector.ms.teams.MicrosoftTeamsMessageHelper;
import prerna.project.api.IProject;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;

// Whether the copies in an owner's Digital Twins are older than what they were copied from. A twin's pages record
// which email thread, Teams chat or drive file each copy came from (twin-sources.json at the top of its project).
// A check reads, with the owner's own sign-in, when each of those last changed and compares it with when the copy
// was made: newer mail in the thread, newer messages in the chat, a later change to the file. It also finds a copy
// whose original is gone and the older of two copies of one item. What it finds waits as one row a copy
// (WORK_TWIN_COPY) for the owner to act on. Nothing in a twin is changed here: the owner's page saves the item again
// and writes it over the copy, or removes the copy, and then settles the row.
// Only what tells a change is read: an email's subject, sender, date and folder, and a file's properties. Teams has
// no read of a chat without its messages, so those arrive with their text, which is cut to one character and never
// kept. Nothing is given to a model.
// A check runs when the owner asks (start), after a mail sync and when the open app lists the rows (watch), at most
// once every TWIN_CHECK_HOURS.
public final class WorkTwinUpdates {

	private static final Logger classLogger = LogManager.getLogger(WorkTwinUpdates.class);

	public static final String KIND = "twin_check";
	/** Hours between checks until the owner chooses; they choose 0 to turn them off. */
	static final int DEFAULT_EVERY_HOURS = 24;

	// where a twin's pages record what each copy came from (digital-twin.ts): copy path -> item key, and for a
	// drive file "drive:<item id>" -> the drive it is in
	static final String RECORD = "twin-sources.json";
	private static final String DRIVE_ENTRY = "drive:";
	private static final String THREAD_KEY = "conversation:";
	// a copy's path as those pages write it: one file in one of the folders they copy into, never the folder
	// itself or the one above it
	private static final Pattern COPY_PATH = Pattern
			.compile("public/(emails|teams-chats|files)/(?!\\.{1,2}$)[^/\\\\]+");

	static final String EMAIL = "email";
	static final String CHAT = "chat";
	static final String FILE = "file";

	// what a check concludes about a copy; the first three are kept on its row
	static final String CHANGED = "changed";
	static final String GONE = "gone";
	static final String DUPLICATE = "duplicate";
	static final String CURRENT = "current";
	static final String UNKNOWN = "unknown";

	static final String OPEN = "open";
	public static final String DISMISSED = "dismissed";
	public static final String APPLIED = "applied";

	// the emails of a thread that are listed to find its newest; a thread that comes back with this many may hold more
	static final int PER_THREAD = 100;
	// the newest messages of a chat that are read, which is what a copy of it holds
	static final int CHAT_MESSAGES = 50;
	// shortcut: one Graph read per file, one after another, so a check stops at this many; send them as $batch
	// requests (it takes 20) when twins come to hold more files than this
	static final int MAX_FILE_READS = 200;

	private static final String LOCK = "twin-copies";

	private WorkTwinUpdates() {
	}

	/**
	 * A copy in a twin and the item it was copied from.
	 *
	 * @param key      what the copy's viewer knows the item by: conversation:&lt;id&gt;, a chat id, or a drive item id
	 * @param driveId  the drive a file is in, null when the record does not say; the owner's own drive is then read
	 * @param copiedAt when the copy was made: the file's own time until a check has seen it, then what its row keeps
	 */
	record Copy(String projectId, String path, String kind, String key, String driveId, Instant copiedAt) {
	}

	/** What a check concluded about one copy: state is changed, gone, duplicate, current or unknown. */
	record Finding(Copy copy, String state, Instant sourceAt, Map<String, Object> detail) {
		static Finding of(Copy copy, String state) {
			return new Finding(copy, state, null, Map.of());
		}
	}

	/** The owner's Microsoft 365 as a check reads it. A test stands a fake in for it. */
	interface Source {
		/**
		 * The emails of these conversations without their bodies, in Graph shape, at most PER_THREAD of each and in no
		 * order; null when the mailbox cannot list them. unread counts the conversations that could not be listed.
		 */
		List<Map<String, Object>> threads(Collection<String> conversationIds, AtomicInteger unread) throws Exception;

		/** The newest messages of a chat; null when the chat is not there. */
		List<Map<String, Object>> chat(String chatId) throws Exception;

		/** What its drive holds for a file now; null when it is not there. */
		Map<String, Object> file(String driveId, String itemId) throws Exception;
	}

	private record Row(String id, String projectId, String key, Instant copiedAt, String change, Instant sourceAt,
			String status) {
	}

	// ---- running a check ----

	/**
	 * Starts a check of this person's twins, or answers the one that is running.
	 *
	 * @return the job, to poll with BrainGetJob(jobId)
	 * @throws RuntimeException when they are not signed in to Microsoft 365, as the sign-in error their page answers
	 */
	public static Map<String, Object> start(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		// before the job, so a missing sign-in reaches the page as itself and not as a failed job
		token(user);
		return CollaborationJobUtils.start(owner.getValue0(), owner.getValue1(), KIND, Map.of(),
				job -> check(user, job));
	}

	/**
	 * Starts a check when one is due: the owner did not turn them off, and the last one started longer ago than their
	 * interval. Called after a mail sync, which also runs while the app is closed, and when the open app lists the
	 * rows, so an owner whose mail is not synced is checked too. A failed check counts as one, as a failed look for
	 * new topics does. Returns whether a check was started.
	 */
	public static boolean watch(User user, String ownerId, String ownerType) {
		Integer own = CollaborationDbUtils.queryOne(
				"SELECT TWIN_CHECK_HOURS FROM BRAIN_SETTINGS WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> CollaborationDbUtils.getInteger(rs, "TWIN_CHECK_HOURS"), ownerId, ownerType);
		long every = own == null ? DEFAULT_EVERY_HOURS : own;
		if (every <= 0) {
			return false;
		}
		Map<String, Object> last = CollaborationJobUtils.latest(ownerId, ownerType, KIND);
		// a job's times are read as ISO text
		Timestamp lastAt = last == null ? null : CollaborationDbUtils.toTimestamp(last.get("startedAt"), "startedAt");
		if (lastAt != null && CollaborationDbUtils.now().getTime() - lastAt.getTime() < every * 3_600_000L) {
			return false;
		}
		try {
			start(user);
		} catch (RuntimeException e) {
			// not signed in to Microsoft 365 here: there is nothing to check with, and the next ask tries again
			return false;
		}
		return true;
	}

	static void check(User user, CollaborationJobUtils.Job job) throws Exception {
		String ownerId = job.ownerId();
		String ownerType = job.ownerType();
		job.step("twins", 5);
		// the twins whose record could be read; one that could not is left as it is, not emptied
		Set<String> twins = new LinkedHashSet<>();
		List<Copy> copies = new ArrayList<>();
		for (String twinId : twinIds(user)) {
			List<Copy> held = copies(twinId);
			if (held != null) {
				twins.add(twinId);
				copies.addAll(held);
			}
		}
		job.count("twins", twins.size());
		job.count("copies", copies.size());
		List<Finding> findings = List.of();
		if (!copies.isEmpty()) {
			String token = token(user);
			BrainMessageSource mailbox = BrainMessageSource.current();
			Source source = new Source() {
				@Override
				public List<Map<String, Object>> threads(Collection<String> conversationIds, AtomicInteger unread)
						throws Exception {
					return mailbox.conversationMessages(user, conversationIds, PER_THREAD, unread);
				}

				@Override
				public List<Map<String, Object>> chat(String chatId) throws Exception {
					try {
						// the text is cut to one character: only who wrote and when is used
						return MicrosoftTeamsMessageHelper.listChatMessages(token, chatId, 1, CHAT_MESSAGES);
					} catch (IllegalArgumentException e) {
						if (e.getMessage() != null && e.getMessage().contains(" returned HTTP 404")) {
							return null;
						}
						throw e;
					}
				}

				@Override
				public Map<String, Object> file(String driveId, String itemId) throws Exception {
					return MicrosoftOneDriveHelper.fileState(token, driveId, itemId, null);
				}
			};
			// only a copied thread needs the folders, and reading them walks the mailbox when they are old
			BrainMailFolders.Tree folders = copies.stream().anyMatch(copy -> EMAIL.equals(copy.kind()))
					? BrainMailFolders.current(user, BrainMailHeaderSource.current(), ownerId, ownerType)
					: new BrainMailFolders.Tree(Map.of(), null);
			findings = compare(known(ownerId, ownerType, copies), source, folders, job::step);
		}
		job.step("saving", 95);
		save(ownerId, ownerType, twins, findings);
		for (String state : List.of(CHANGED, GONE, DUPLICATE, UNKNOWN)) {
			job.count(state, findings.stream().filter(finding -> state.equals(finding.state())).count());
		}
	}

	// the Microsoft token of this sign-in, renewed when it ran out; a missing sign-in keeps its own error
	private static String token(User user) {
		try {
			return MicrosoftLoginUtils.getValidAccessToken(user);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException("Could not sign in to Microsoft: " + e.getMessage(), e);
		}
	}

	// the twins this person created and still owns, which is what their pages list as theirs
	private static List<String> twinIds(User user) {
		List<String> ids = new ArrayList<>();
		for (Map<String, Object> twin : SecurityProjectUtils.getUserProjectList(user, null, null, false,
				Map.of(SecurityProjectUtils.DIGITAL_TWIN_METAKEY, List.of("true")), null, null, null, null, null, false,
				List.of(AccessPermissionEnum.OWNER.getId()), User.getUserIdAndType(user))) {
			ids.add(String.valueOf(twin.get("project_id")));
		}
		return ids;
	}

	// ---- what a twin holds ----

	// null when the twin cannot be opened
	private static List<Copy> copies(String projectId) {
		try {
			IProject project = Utility.getProject(projectId);
			if (project == null) {
				return null;
			}
			return copies(projectId, Path.of(EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT,
					project.getEngineId(), project.getEngineName())));
		} catch (RuntimeException e) {
			classLogger.warn("Digital Twin {} could not be opened to check its copies", projectId, e);
			return null;
		}
	}

	/**
	 * The copies a twin holds that a check can compare with where they came from, each with its file's time. A copy
	 * of anything else (an upload, an attachment, a single email, a meeting) is not listed.
	 *
	 * @param assets the twin project's assets folder
	 * @return the copies; empty when nothing is recorded; null when the record cannot be read, so the twin is left
	 *         as it is
	 */
	static List<Copy> copies(String projectId, Path assets) {
		Path record = assets.resolve(RECORD);
		if (!Files.isRegularFile(record, LinkOption.NOFOLLOW_LINKS)) {
			return List.of();
		}
		Map<String, Object> entries;
		try {
			entries = CollaborationDbUtils.parseMap(Files.readString(record));
		} catch (IOException | RuntimeException e) {
			classLogger.warn("The record of what Digital Twin {} was copied from could not be read", projectId, e);
			return null;
		}
		if (entries == null) {
			return null;
		}
		Path twinFiles = assets.resolve(Constants.PUBLIC_ASSETS_FOLDER).normalize();
		List<Copy> copies = new ArrayList<>();
		for (Map.Entry<String, Object> entry : entries.entrySet()) {
			String path = entry.getKey();
			if (!(entry.getValue() instanceof String key) || !COPY_PATH.matcher(path).matches()) {
				continue;
			}
			String kind = kind(path, key);
			// the record is a file anyone who can edit the twin can change: a path names one file among the twin's
			// own, or it is not looked at
			Path file = assets.resolve(path).normalize();
			if (kind == null || !file.startsWith(twinFiles) || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
				continue;
			}
			String driveId = FILE.equals(kind) && entries.get(DRIVE_ENTRY + key) instanceof String drive
					&& !drive.isBlank() ? drive : null;
			try {
				copies.add(new Copy(projectId, path, kind, key, driveId,
						Files.getLastModifiedTime(file, LinkOption.NOFOLLOW_LINKS).toInstant()));
			} catch (IOException e) {
				classLogger.warn("Could not read when {} was copied into Digital Twin {}", path, projectId, e);
			}
		}
		return copies;
	}

	// what a recorded copy is, from the folder its viewer copies into and the key that viewer gives its items;
	// null for what is not compared: an attachment (messageId:attachmentId), a single email (message:id)
	static String kind(String path, String key) {
		if (path.startsWith("public/emails/")) {
			return key.startsWith(THREAD_KEY) && key.length() > THREAD_KEY.length() ? EMAIL : null;
		}
		if (path.startsWith("public/teams-chats/")) {
			// a chat id as Graph writes one (19:...@thread.v2, 19:..._...@unq.gbl.spaces, 48:notes)
			return key.startsWith("19:") || key.startsWith("48:") ? CHAT : null;
		}
		return !key.isBlank() && key.indexOf(':') < 0 ? FILE : null;
	}

	// ---- comparing ----

	/**
	 * Compares each copy with what it was copied from, reading each thread, chat and file once however many copies
	 * of it there are.
	 *
	 * @param copies   every copy of the owner's twins, each with the time it was last known current
	 * @param folders  the owner's mail folders, to tell mail in a folder the import skips
	 * @param progress told as each kind starts: its name and how far the check is, 0 to 100; may be null
	 * @return one finding for each copy
	 */
	static List<Finding> compare(List<Copy> copies, Source source, BrainMailFolders.Tree folders,
			BiConsumer<String, Integer> progress) {
		List<Finding> findings = new ArrayList<>();
		// of two copies of one item in a twin the older is the finding, and only the newest is compared
		Map<String, Copy> newest = new HashMap<>();
		for (Copy copy : copies) {
			newest.merge(item(copy), copy, (held, next) -> next.copiedAt().isAfter(held.copiedAt()) ? next : held);
		}
		Map<String, List<Copy>> byKind = new HashMap<>();
		for (Copy copy : copies) {
			Copy kept = newest.get(item(copy));
			if (kept == copy) {
				byKind.computeIfAbsent(copy.kind(), kind -> new ArrayList<>()).add(copy);
			} else {
				findings.add(new Finding(copy, DUPLICATE, null, Map.of("newerCopy", kept.path())));
			}
		}
		tell(progress, "mail", 15);
		findings.addAll(threads(byKind.getOrDefault(EMAIL, List.of()), source, folders));
		tell(progress, "chats", 45);
		findings.addAll(chats(byKind.getOrDefault(CHAT, List.of()), source));
		tell(progress, "files", 65);
		findings.addAll(files(byKind.getOrDefault(FILE, List.of()), source));
		return findings;
	}

	private static String item(Copy copy) {
		return copy.projectId() + '|' + copy.kind() + '|' + copy.key();
	}

	private static void tell(BiConsumer<String, Integer> progress, String step, int percent) {
		if (progress != null) {
			progress.accept(step, percent);
		}
	}

	private static List<Finding> threads(List<Copy> copies, Source source, BrainMailFolders.Tree folders) {
		List<Finding> findings = new ArrayList<>();
		if (copies.isEmpty()) {
			return findings;
		}
		Set<String> ids = new LinkedHashSet<>();
		for (Copy copy : copies) {
			ids.add(copy.key().substring(THREAD_KEY.length()));
		}
		AtomicInteger unread = new AtomicInteger();
		List<Map<String, Object>> messages = null;
		try {
			messages = source.threads(ids, unread);
		} catch (Exception e) {
			classLogger.warn("A Digital Twin check could not list the copied email threads", e);
		}
		if (messages == null) {
			for (Copy copy : copies) {
				findings.add(Finding.of(copy, UNKNOWN));
			}
			return findings;
		}
		Map<String, List<Map<String, Object>>> byThread = new HashMap<>();
		for (Map<String, Object> message : messages) {
			byThread.computeIfAbsent(CollaborationDbUtils.asString(message.get("conversationId")),
					id -> new ArrayList<>()).add(message);
		}
		for (Copy copy : copies) {
			List<Map<String, Object>> listed = byThread.getOrDefault(copy.key().substring(THREAD_KEY.length()),
					List.of());
			int kept = 0;
			int newer = 0;
			Instant latestAt = null;
			Map<String, Object> latest = null;
			for (Map<String, Object> message : listed) {
				// what the import leaves out is no part of the thread here either: a draft, and mail in a folder it
				// skips, such as Deleted Items
				if (Boolean.TRUE.equals(message.get("isDraft"))
						|| folders.skipped(CollaborationDbUtils.asString(message.get("parentFolderId")))) {
					continue;
				}
				kept++;
				Instant at = at(message.get("receivedDateTime"));
				if (at == null || !at.isAfter(copy.copiedAt())) {
					continue;
				}
				newer++;
				if (latestAt == null || at.isAfter(latestAt)) {
					latestAt = at;
					latest = message;
				}
			}
			if (newer > 0) {
				Map<String, Object> detail = new LinkedHashMap<>();
				detail.put("count", newer);
				put(detail, "latestFrom", BrainTopicBodySearch.sender(latest));
				put(detail, "title", CollaborationDbUtils.asString(latest.get("subject")));
				findings.add(new Finding(copy, CHANGED, latestAt, detail));
				continue;
			}
			// a newer email is proof of a change wherever it turns up. That none turned up proves nothing when a
			// listing failed (the count does not say whose) or the thread came back at its limit and may hold more
			boolean whole = unread.get() == 0 && listed.size() < PER_THREAD;
			findings.add(Finding.of(copy, !whole ? UNKNOWN : kept == 0 ? GONE : CURRENT));
		}
		return findings;
	}

	private static List<Finding> chats(List<Copy> copies, Source source) {
		List<Finding> findings = new ArrayList<>();
		// each chat's messages, or GONE, or UNKNOWN when it could not be read
		Map<String, Object> read = new HashMap<>();
		for (Copy copy : copies) {
			if (!read.containsKey(copy.key())) {
				Object answer;
				try {
					List<Map<String, Object>> messages = source.chat(copy.key());
					answer = messages == null ? GONE : messages;
				} catch (Exception e) {
					classLogger.warn("A Digital Twin check could not read a copied Teams chat", e);
					answer = UNKNOWN;
				}
				read.put(copy.key(), answer);
			}
			if (!(read.get(copy.key()) instanceof List<?> messages)) {
				findings.add(Finding.of(copy, (String) read.get(copy.key())));
				continue;
			}
			int newer = 0;
			Instant latestAt = null;
			Map<?, ?> latest = null;
			for (Object each : messages) {
				// what people wrote, not that someone joined or the chat was renamed, and not what was taken back
				if (!(each instanceof Map<?, ?> message) || Boolean.TRUE.equals(message.get("isDeleted"))
						|| (message.get("messageType") != null && !"message".equals(message.get("messageType")))) {
					continue;
				}
				Instant at = at(message.get("createdDateTime"));
				if (at == null || !at.isAfter(copy.copiedAt())) {
					continue;
				}
				newer++;
				if (latestAt == null || at.isAfter(latestAt)) {
					latestAt = at;
					latest = message;
				}
			}
			if (newer == 0) {
				findings.add(Finding.of(copy, CURRENT));
				continue;
			}
			Map<String, Object> detail = new LinkedHashMap<>();
			detail.put("count", newer);
			put(detail, "latestFrom", CollaborationDbUtils.asString(latest.get("fromName")));
			findings.add(new Finding(copy, CHANGED, latestAt, detail));
		}
		return findings;
	}

	private static List<Finding> files(List<Copy> copies, Source source) {
		List<Finding> findings = new ArrayList<>();
		// each file's state, or GONE, or UNKNOWN when it could not be read
		Map<String, Object> read = new HashMap<>();
		int reads = 0;
		boolean silent = false;
		for (Copy copy : copies) {
			String id = (copy.driveId() == null ? "" : copy.driveId()) + '|' + copy.key();
			if (!read.containsKey(id)) {
				Object answer = UNKNOWN;
				if (!silent && reads < MAX_FILE_READS) {
					reads++;
					try {
						Map<String, Object> state = source.file(copy.driveId(), copy.key());
						answer = state == null ? GONE : state;
					} catch (Exception e) {
						classLogger.warn("A Digital Twin check could not read a copied file's state", e);
						// Graph did not answer, and the files after this one would only wait for the same
						silent = e.getMessage() == null || !e.getMessage().contains(" returned HTTP ");
					}
				}
				read.put(id, answer);
			}
			if (!(read.get(id) instanceof Map<?, ?> state)) {
				// an id the record names no drive for was looked for in the owner's own drive, and a file of another
				// drive is not there either: that says nothing about the original
				boolean gone = GONE.equals(read.get(id)) && copy.driveId() != null;
				findings.add(Finding.of(copy, gone ? GONE : UNKNOWN));
				continue;
			}
			// shortcut: the time moves when a file is renamed too, so a rename reads as a change; keep the file's
			// content version (cTag) beside its copy when that shows up as noise
			Instant at = at(state.get("lastModifiedDateTime"));
			if (at == null) {
				findings.add(Finding.of(copy, UNKNOWN));
			} else if (!at.isAfter(copy.copiedAt())) {
				findings.add(Finding.of(copy, CURRENT));
			} else {
				Map<String, Object> detail = new LinkedHashMap<>();
				put(detail, "latestFrom", CollaborationDbUtils.asString(state.get("lastModifiedBy")));
				put(detail, "title", CollaborationDbUtils.asString(state.get("name")));
				put(detail, "webUrl", CollaborationDbUtils.asString(state.get("webUrl")));
				findings.add(new Finding(copy, CHANGED, at, detail));
			}
		}
		return findings;
	}

	private static void put(Map<String, Object> detail, String key, String value) {
		if (value != null && !value.isBlank()) {
			detail.put(key, value);
		}
	}

	// a time as Graph writes it; null when there is none or it cannot be read
	private static Instant at(Object value) {
		String text = CollaborationDbUtils.asString(value);
		if (text == null || text.isBlank()) {
			return null;
		}
		try {
			return Instant.parse(text.trim());
		} catch (DateTimeParseException e) {
			return null;
		}
	}

	// ---- the rows ----

	private static String id(String ownerId, String ownerType, String projectId, String path) {
		return CollaborationDbUtils.deterministicId(ownerId, ownerType, "twin-copy", projectId, path);
	}

	// timestamps are stored as UTC wall time
	private static Timestamp stamp(Instant at) {
		return at == null ? null : Timestamp.valueOf(at.atOffset(ZoneOffset.UTC).toLocalDateTime());
	}

	private static Instant instant(Timestamp at) {
		return at == null ? null : at.toLocalDateTime().toInstant(ZoneOffset.UTC);
	}

	private static Map<String, Row> rows(String ownerId, String ownerType) {
		Map<String, Row> rows = new LinkedHashMap<>();
		CollaborationDbUtils.query("SELECT COPY_ID, PROJECT_ID, COPIED_AT, CHANGE_KIND, SOURCE_AT, STATUS, DETAIL_JSON "
				+ "FROM WORK_TWIN_COPY WHERE OWNER_ID = ? AND OWNER_TYPE = ?", rs -> {
					Map<String, Object> detail = CollaborationDbUtils
							.parseMap(CollaborationDbUtils.getString(rs, "DETAIL_JSON"));
					Row row = new Row(rs.getString("COPY_ID"), rs.getString("PROJECT_ID"),
							detail == null ? null : CollaborationDbUtils.asString(detail.get("key")),
							instant(rs.getTimestamp("COPIED_AT")), rs.getString("CHANGE_KIND"),
							instant(rs.getTimestamp("SOURCE_AT")), rs.getString("STATUS"));
					rows.put(row.id(), row);
					return row;
				}, ownerId, ownerType);
		return rows;
	}

	/**
	 * Each copy with the time it was last known current: what its row keeps, which an Update moves, or the file's own
	 * time when no check has seen it yet or its path has come to name another item. The file's time is not asked again
	 * after that, since a server that loads a project from storage does not keep it.
	 */
	static List<Copy> known(String ownerId, String ownerType, List<Copy> copies) {
		Map<String, Row> rows = rows(ownerId, ownerType);
		List<Copy> known = new ArrayList<>();
		for (Copy copy : copies) {
			Row row = rows.get(id(ownerId, ownerType, copy.projectId(), copy.path()));
			known.add(row == null || row.copiedAt() == null || !copy.key().equals(row.key()) ? copy
					: new Copy(copy.projectId(), copy.path(), copy.kind(), copy.key(), copy.driveId(),
							row.copiedAt()));
		}
		return known;
	}

	/**
	 * Keeps what a check found. A copy seen for the first time gets its row. A change is written when the source is
	 * newer than the copy is now known to be (an Update may have landed while the check ran) and, for one the owner
	 * dismissed, only once the source has moved again. A copy found current loses its change; one that could not be
	 * read keeps what it had. The rows of copies these twins no longer hold are dropped.
	 *
	 * @param twins the twins whose record was read; the rows of any other twin are left as they are
	 */
	static void save(String ownerId, String ownerType, Collection<String> twins, List<Finding> findings) {
		Timestamp now = CollaborationDbUtils.now();
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			CollaborationDbUtils.batch(conn -> {
				Map<String, Row> rows = rows(ownerId, ownerType);
				Set<String> held = new HashSet<>();
				for (Finding finding : findings) {
					Copy copy = finding.copy();
					String id = id(ownerId, ownerType, copy.projectId(), copy.path());
					held.add(id);
					Row row = rows.get(id);
					boolean found = CHANGED.equals(finding.state()) || GONE.equals(finding.state())
							|| DUPLICATE.equals(finding.state());
					if (row != null && !copy.key().equals(row.key())) {
						// the path names another item now: what was known of the old one is not about this one
						CollaborationDbUtils.update("DELETE FROM WORK_TWIN_COPY WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
								+ "AND COPY_ID = ?", ownerId, ownerType, id);
						row = null;
					}
					if (row == null) {
						CollaborationDbUtils.update("INSERT INTO WORK_TWIN_COPY (OWNER_ID, OWNER_TYPE, COPY_ID, "
								+ "PROJECT_ID, COPIED_AT, CHANGE_KIND, SOURCE_AT, STATUS, DETAIL_JSON, FOUND_AT) "
								+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, id, copy.projectId(),
								stamp(copy.copiedAt()), found ? finding.state() : null, stamp(finding.sourceAt()),
								found ? OPEN : null, detail(copy, finding), found ? now : null);
						continue;
					}
					if (UNKNOWN.equals(finding.state())) {
						continue;
					}
					if (!found) {
						if (row.change() != null) {
							CollaborationDbUtils.update("UPDATE WORK_TWIN_COPY SET CHANGE_KIND = NULL, SOURCE_AT = NULL, "
									+ "STATUS = NULL, DETAIL_JSON = ?, FOUND_AT = NULL WHERE OWNER_ID = ? AND "
									+ "OWNER_TYPE = ? AND COPY_ID = ?", detail(copy, finding), ownerId, ownerType, id);
						}
						continue;
					}
					boolean same = finding.state().equals(row.change());
					boolean dismissed = same && DISMISSED.equals(row.status());
					if (CHANGED.equals(finding.state())) {
						// what an Update during this check already brought in, and what the owner dismissed until the
						// source moves again
						if (row.copiedAt() != null && !finding.sourceAt().isAfter(row.copiedAt())) {
							continue;
						}
						if (dismissed && row.sourceAt() != null && !finding.sourceAt().isAfter(row.sourceAt())) {
							continue;
						}
					} else if (dismissed) {
						continue;
					}
					// a suggestion still waiting keeps the time it was first made
					boolean waiting = same && OPEN.equals(row.status());
					CollaborationDbUtils.update("UPDATE WORK_TWIN_COPY SET CHANGE_KIND = ?, SOURCE_AT = ?, STATUS = ?, "
							+ "DETAIL_JSON = ?" + (waiting ? "" : ", FOUND_AT = ?") + " WHERE OWNER_ID = ? AND "
							+ "OWNER_TYPE = ? AND COPY_ID = ?",
							waiting ? new Object[] { finding.state(), stamp(finding.sourceAt()), OPEN,
									detail(copy, finding), ownerId, ownerType, id }
									: new Object[] { finding.state(), stamp(finding.sourceAt()), OPEN,
											detail(copy, finding), now, ownerId, ownerType, id });
				}
				for (Row row : rows.values()) {
					if (twins.contains(row.projectId()) && !held.contains(row.id())) {
						CollaborationDbUtils.update("DELETE FROM WORK_TWIN_COPY WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
								+ "AND COPY_ID = ?", ownerId, ownerType, row.id());
					}
				}
			});
		}
	}

	// what a row says of its copy and, when it has one, of the change found
	private static String detail(Copy copy, Finding finding) {
		Map<String, Object> detail = new LinkedHashMap<>();
		detail.put("path", copy.path());
		detail.put("kind", copy.kind());
		detail.put("key", copy.key());
		if (copy.driveId() != null) {
			detail.put("driveId", copy.driveId());
		}
		detail.putAll(finding.detail());
		return CollaborationDbUtils.toJson(detail);
	}

	// ---- for the owner's pages ----

	/**
	 * The suggestions waiting for this person, the newest first, and their last check. Starts a check first when one
	 * is due.
	 *
	 * @return {@code updates}, each with id, twinId, kind (email, chat or file), change (changed, gone or duplicate),
	 *         path, key, copiedAt and, where they apply, driveId, title, count, latestFrom, latestAt, webUrl and
	 *         newerCopy; and {@code lastCheck}, the job of their last check, null when there was none
	 */
	public static Map<String, Object> list(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		try {
			watch(user, ownerId, ownerType);
		} catch (RuntimeException e) {
			classLogger.warn("A Digital Twin check that was due could not start", e);
		}
		List<Map<String, Object>> updates = open(ownerId, ownerType);
		if (!updates.isEmpty()) {
			// a twin they deleted or no longer own keeps its rows until a reset; those are not theirs to see
			Set<String> mine = new HashSet<>(twinIds(user));
			updates.removeIf(update -> !mine.contains(update.get("twinId")));
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("updates", updates);
		out.put("lastCheck", CollaborationJobUtils.latest(ownerId, ownerType, KIND));
		return out;
	}

	static List<Map<String, Object>> open(String ownerId, String ownerType) {
		return CollaborationDbUtils.query("SELECT COPY_ID, PROJECT_ID, COPIED_AT, CHANGE_KIND, SOURCE_AT, DETAIL_JSON "
				+ "FROM WORK_TWIN_COPY WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND STATUS = ? ORDER BY FOUND_AT DESC, "
				+ "COPY_ID", rs -> {
					Map<String, Object> update = new LinkedHashMap<>();
					update.put("id", rs.getString("COPY_ID"));
					update.put("twinId", rs.getString("PROJECT_ID"));
					update.put("change", rs.getString("CHANGE_KIND"));
					Map<String, Object> detail = CollaborationDbUtils
							.parseMap(CollaborationDbUtils.getString(rs, "DETAIL_JSON"));
					if (detail != null) {
						update.putAll(detail);
						// JSON read back makes every number a double
						if (detail.get("count") instanceof Number count) {
							update.put("count", count.intValue());
						}
					}
					update.put("copiedAt", CollaborationDbUtils.getTimestamp(rs, "COPIED_AT"));
					String latestAt = CollaborationDbUtils.getTimestamp(rs, "SOURCE_AT");
					if (latestAt != null) {
						update.put("latestAt", latestAt);
					}
					return update;
				}, ownerId, ownerType, OPEN);
	}

	/** Dismiss a pending update for a copy removed in chat, as the owner's Files page does. */
	static void dismissRemovedCopy(User user, String projectId, String path) {
		if (COPY_PATH.matcher(path).matches()) {
			try {
				Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
				settle(user, id(owner.getValue0(), owner.getValue1(), projectId, path), DISMISSED);
			} catch (RuntimeException e) {
				// The file is already gone; the next update check also drops rows for missing files.
				classLogger.warn("Could not dismiss a Digital Twin update after its copy was removed", e);
			}
		}
	}

	/**
	 * Records what the owner did about a suggestion. Applied: their page wrote the item over the copy, so the copy is
	 * current as of now and the suggestion is gone. Dismissed: the suggestion stays away until the source moves
	 * again; used too once a copy was removed, whose row the next check drops.
	 *
	 * @return whether a row of theirs was changed
	 */
	// shortcut: the copy counts as current from now on, not from the moment its item was read again, so mail that
	// arrived in the seconds between is first seen with the next email; pass that moment in if it shows up
	public static boolean settle(User user, String copyId, String action) {
		if (copyId == null || copyId.isBlank()) {
			throw new IllegalArgumentException("Pass the suggestion's id as updateId");
		}
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		synchronized (CollaborationDbUtils.ownerLock(LOCK, owner.getValue0(), owner.getValue1())) {
			if (APPLIED.equals(action)) {
				return CollaborationDbUtils.update("UPDATE WORK_TWIN_COPY SET COPIED_AT = ?, CHANGE_KIND = NULL, "
						+ "SOURCE_AT = NULL, STATUS = NULL, FOUND_AT = NULL WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND "
						+ "COPY_ID = ?", CollaborationDbUtils.now(), owner.getValue0(), owner.getValue1(),
						copyId.trim()) > 0;
			}
			if (DISMISSED.equals(action)) {
				return CollaborationDbUtils.update("UPDATE WORK_TWIN_COPY SET STATUS = ? WHERE OWNER_ID = ? AND "
						+ "OWNER_TYPE = ? AND COPY_ID = ? AND STATUS = ?", DISMISSED, owner.getValue0(),
						owner.getValue1(), copyId.trim(), OPEN) > 0;
			}
		}
		throw new IllegalArgumentException("action must be " + APPLIED + " or " + DISMISSED);
	}
}
