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

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.javatuples.Pair;

import prerna.auth.User;

// Brain memory (BRAIN_MEMORY and BRAIN_MEMORY_LINK): what the assistant keeps for the owner across threads.
// The owner writes through BrainSaveMemory and its siblings; the assistant through Remember and Forget, which never
// confirm a memory or change one the owner wrote.
public final class BrainMemoryUtils {

	// KIND
	public static final String PREFERENCE = "preference";
	public static final String FACT = "fact";
	public static final Set<String> KINDS = Set.of(PREFERENCE, FACT);

	// STATE: active is recalled; suggested waits for the owner; superseded was replaced; dismissed was undone or
	// turned down and stays hidden so it is not proposed again
	public static final String ACTIVE = "active";
	public static final String SUGGESTED = "suggested";
	public static final String SUPERSEDED = "superseded";
	public static final String DISMISSED = "dismissed";
	public static final Set<String> STATES = Set.of(ACTIVE, SUGGESTED, SUPERSEDED, DISMISSED);

	// ORIGIN: BrainProfileUtils.YOU, ASSISTANT (Remember in a chat), or WorkThreadInsights.BRAIN (the chat review)
	public static final String ASSISTANT = "assistant";
	private static final Set<String> ORIGINS = Set.of(BrainProfileUtils.YOU, ASSISTANT, WorkThreadInsights.BRAIN);

	// BRAIN_MEMORY_LINK.REF_TYPE
	public static final String PERSON = "person";
	public static final String TOPIC = "topic";
	public static final String ACCOUNT = "account";
	public static final String THREAD = "thread";
	public static final Set<String> REF_TYPES = Set.of(PERSON, TOPIC, ACCOUNT, THREAD);

	// SOURCE_KIND
	static final String FROM_UI = "ui";
	static final String FROM_CHAT = "chat";
	static final String FROM_CHAT_REVIEW = "chat_review";
	static final String FROM_TOPIC_NOTE = "topic_note";
	static final String FROM_THREAD_FACT = "thread_fact";

	// what Remember and Forget did
	public static final String SAVED = "saved";
	public static final String UPDATED = "updated";
	public static final String EXISTS = "exists";
	public static final String UNDONE_HERE = "undone_here";
	public static final String NEEDS_OWNER = "needs_owner";
	public static final String FORGOTTEN = "forgotten";

	// BrainResolveMemory actions
	public static final String ACCEPT = "accept";
	public static final String CONFIRM = "confirm";
	public static final String DISMISS = "dismiss";
	public static final String RESTORE = "restore";
	public static final String REOPEN = "reopen";
	public static final String UNCONFIRM = "unconfirm";
	public static final Set<String> ACTIONS = Set.of(ACCEPT, CONFIRM, DISMISS, RESTORE, REOPEN, UNCONFIRM);

	public static final int MAX_CHARS = 500;
	public static final int MAX_LINKS = 8;
	public static final int DEFAULT_LIMIT = 50;
	public static final int SEARCH_LIMIT = 10;
	private static final int MAX_LIMIT = 500;
	private static final int MAX_SEARCH_LIMIT = 30;

	private static final String LOCK = "memory";
	private static final String OWNED = " WHERE OWNER_ID = ? AND OWNER_TYPE = ?";
	private static final String COLUMNS = "MEMORY_ID, KIND, TEXT, STATE, ORIGIN, CONFIRMED, PINNED, REPLACES_ID, "
			+ "EXPIRES_AT, SOURCE_KIND, SOURCE_THREAD_ID, SOURCE_ROOM_ID, SOURCE_REF, SOURCE_PERSON_ID, SOURCE_LABEL, "
			+ "CREATED_AT, UPDATED_AT, CONFIRMED_AT";
	private static final int COLUMN_COUNT = 18;

	// a /new session's thread is never saved, so a link to it is dropped instead of refused
	static final String SESSION_THREAD_PREFIX = "session:";

	// passwords, keys and tokens never go into a memory
	private static final Pattern SECRET = Pattern.compile("(?i)(\\b(password|passcode|passwd)\\b\\s*(is|was|:|=)"
			+ "|\\b(api[ _-]?key|access[ _-]?token|refresh[ _-]?token|client[ _-]?secret|private[ _-]?key"
			+ "|secret[ _-]?key|bearer)\\b\\s*(is|was|:|=)|\\bsecret\\s*[:=]|-----BEGIN)");
	private static final Pattern TOKEN = Pattern.compile("[A-Za-z0-9_\\-]{32,}");
	private static final Pattern UUID_TEXT = Pattern
			.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
	private static final Pattern ID = Pattern.compile("[A-Za-z0-9_\\-]{1,50}");
	private static final Set<String> STOP_WORDS = Set.of("a", "an", "the", "and", "or", "of", "to", "in", "on",
			"for", "with", "at", "by", "from", "is", "are", "was", "were", "be", "it", "its", "this", "that", "about",
			"what", "who", "whom", "my", "me", "i", "you", "your", "we", "our", "do", "does", "did", "has", "have",
			"any", "know", "remember");
	private static final String ID_ALPHABET = "abcdefghijkmnpqrstuvwxyz23456789";
	private static final SecureRandom RANDOM = new SecureRandom();

	private BrainMemoryUtils() {
	}

	/** What a memory is about: a person, topic, account, or thread id. */
	public record Ref(String type, String id) {
	}

	/** Where a write came from; the assistant's tools fill it from the run's room. */
	public record Source(String kind, String threadId, String roomId, String ref, String personId, String label) {

		static Source ui() {
			return new Source(FROM_UI, null, null, null, null, null);
		}
	}

	/** One BRAIN_MEMORY row with its links. */
	public record Memory(String id, String kind, String text, String state, String origin, boolean confirmed,
			boolean pinned, String replacesId, Timestamp expiresAt, Source source, Timestamp createdAt,
			Timestamp updatedAt, Timestamp confirmedAt, List<Ref> about) {

		Memory withAbout(List<Ref> refs) {
			return new Memory(id, kind, text, state, origin, confirmed, pinned, replacesId, expiresAt, source,
					createdAt, updatedAt, confirmedAt, refs);
		}

		/** Typed or approved by the owner: the assistant may not change it on its own. */
		boolean ownerWrote() {
			return confirmed || BrainProfileUtils.YOU.equals(origin);
		}

		boolean expired(Timestamp now) {
			return expiresAt != null && !expiresAt.after(now);
		}
	}

	// ---- owner: read ----

	// active and suggested when states is empty; newest change first, or best match first for a query
	public static Map<String, Object> listMemories(User user, List<String> states, String refType, String refId,
			String query, int limit, int offset) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Set<String> wanted = new LinkedHashSet<>();
		for (String state : states == null ? List.<String>of() : states) {
			wanted.add(check(STATES, state, "memory state"));
		}
		if (wanted.isEmpty()) {
			wanted.addAll(List.of(ACTIVE, SUGGESTED));
		}
		Ref about = null;
		if (refType != null || refId != null) {
			if (refType == null || refId == null) {
				throw new IllegalArgumentException("Pass refType and refId together");
			}
			about = new Ref(check(REF_TYPES, refType, "refType"), refId);
		}
		List<Memory> memories = new ArrayList<>(load(ownerId, ownerType, wanted));
		if (about != null) {
			Ref ref = about;
			memories.removeIf(m -> !m.about().contains(ref));
		}
		if (query != null && !query.isBlank()) {
			memories = rank(memories, query, names(ownerId, ownerType, allRefs(memories)));
		} else {
			memories.sort(Comparator.comparing(Memory::updatedAt, Comparator.nullsLast(Comparator.reverseOrder()))
					.thenComparing(Memory::id));
		}
		int from = Math.min(Math.max(offset, 0), memories.size());
		int to = Math.min(from + Math.min(Math.max(limit, 1), MAX_LIMIT), memories.size());
		List<Map<String, Object>> items = new ArrayList<>();
		for (Memory memory : memories.subList(from, to)) {
			items.add(toMap(memory, null));
		}
		Map<String, Object> page = new LinkedHashMap<>();
		page.put("items", items);
		page.put("total", memories.size());
		return page;
	}

	// ---- owner: write ----

	/**
	 * Creates a memory when there is no id; otherwise changes only the keys passed. Any owner save confirms the
	 * memory, and editing a suggestion accepts it. An id with no row puts back a memory the owner deleted (the
	 * session Undo), with the kind, text, links, state, origin and confirmed it had.
	 */
	public static Map<String, Object> saveMemory(User user, Map<String, Object> memory) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String id = blankToNull(memory.get("id"));
		Timestamp now = CollaborationDbUtils.now();
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Memory current = id == null ? null : find(ownerId, ownerType, id);
			if (current == null) {
				Memory created = id == null ? created(ownerId, ownerType, memory, now)
						: restored(ownerId, ownerType, id, memory, now);
				CollaborationDbUtils.inTransaction(conn -> insert(conn, ownerId, ownerType, created));
				return toMap(find(ownerId, ownerType, created.id()), null);
			}
			if (!ACTIVE.equals(current.state()) && !SUGGESTED.equals(current.state())) {
				throw new IllegalArgumentException("Memory not found");
			}
			List<String> sets = new ArrayList<>();
			List<Object> values = new ArrayList<>();
			if (memory.containsKey("text")) {
				CollaborationDbUtils.addSet(sets, values, "TEXT", cleanText(memory.get("text")));
			}
			if (memory.containsKey("kind")) {
				CollaborationDbUtils.addSet(sets, values, "KIND", kind(memory.get("kind"), current.kind()));
			}
			if (memory.containsKey("pinned")) {
				CollaborationDbUtils.addSet(sets, values, "PINNED", isTrue(memory.get("pinned")));
			}
			if (memory.containsKey("expiresAt")) {
				CollaborationDbUtils.addSet(sets, values, "EXPIRES_AT",
						CollaborationDbUtils.toTimestamp(memory.get("expiresAt"), "expiresAt"));
			}
			List<Ref> about = memory.containsKey("about") ? refs(ownerId, ownerType, memory.get("about")) : null;
			CollaborationDbUtils.addSet(sets, values, "CONFIRMED", true);
			if (current.confirmedAt() == null) {
				CollaborationDbUtils.addSet(sets, values, "CONFIRMED_AT", now);
			}
			boolean accept = SUGGESTED.equals(current.state());
			if (accept) {
				CollaborationDbUtils.addSet(sets, values, "STATE", ACTIVE);
			}
			CollaborationDbUtils.addSet(sets, values, "UPDATED_AT", now);
			values.addAll(List.of(ownerId, ownerType, id));
			CollaborationDbUtils.inTransaction(conn -> {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_MEMORY SET " + String.join(", ", sets) + OWNED
						+ " AND MEMORY_ID = ?", values.toArray());
				if (about != null) {
					CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_MEMORY_LINK" + OWNED + " AND MEMORY_ID = ?",
							ownerId, ownerType, id);
					insertLinks(conn, ownerId, ownerType, id, about, now);
				}
				if (accept) {
					supersede(conn, ownerId, ownerType, current.replacesId(), now);
				}
			});
			return toMap(find(ownerId, ownerType, id), null);
		}
	}

	// erases the memory, its links, and the superseded versions it replaced
	public static Map<String, Object> deleteMemory(User user, String memoryId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Memory memory = find(ownerId, ownerType, memoryId);
			if (memory == null) {
				throw new IllegalArgumentException("Memory not found");
			}
			List<String> ids = new ArrayList<>(List.of(memory.id()));
			for (String prior = memory.replacesId(); prior != null && !ids.contains(prior);) {
				Memory replaced = find(ownerId, ownerType, prior);
				if (replaced == null || !SUPERSEDED.equals(replaced.state())) {
					break;
				}
				ids.add(replaced.id());
				prior = replaced.replacesId();
			}
			List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
			params.addAll(ids);
			String in = " AND MEMORY_ID IN (" + CollaborationDbUtils.placeholders(ids.size()) + ")";
			CollaborationDbUtils.inTransaction(conn -> {
				CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_MEMORY_LINK" + OWNED + in, params.toArray());
				CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_MEMORY" + OWNED + in, params.toArray());
			});
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("id", memoryId);
		result.put("deleted", true);
		return result;
	}

	// every memory, suggestion and hidden tombstone; the review keeps its place, so old chats are not re-read
	public static Map<String, Object> deleteAllMemories(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		int[] deleted = new int[1];
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			CollaborationDbUtils.inTransaction(conn -> {
				CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_MEMORY_LINK" + OWNED, ownerId, ownerType);
				deleted[0] = CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_MEMORY" + OWNED, ownerId,
						ownerType);
			});
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("deleted", deleted[0]);
		return result;
	}

	/**
	 * accept a suggestion; confirm a learned memory; dismiss a suggestion or a learned memory (the chat card's
	 * Undo), which puts back the memory it replaced; restore a dismissed memory as active (the Undo of a Forget);
	 * reopen a dismissed memory, or an accepted one, as a suggestion (the Undo of Keep); unconfirm a memory the
	 * assistant saved (the Undo of Confirm).
	 */
	public static Map<String, Object> resolveMemory(User user, String memoryId, String action) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		check(ACTIONS, action, "action");
		Timestamp now = CollaborationDbUtils.now();
		String[] restored = new String[1];
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Memory memory = find(ownerId, ownerType, memoryId);
			if (memory == null) {
				throw new IllegalArgumentException("Memory not found");
			}
			String state = memory.state();
			CollaborationDbUtils.inTransaction(conn -> {
				switch (action) {
				case ACCEPT -> {
					require(SUGGESTED.equals(state), "Only a suggestion can be accepted");
					setState(conn, ownerId, ownerType, memory, ACTIVE, true, now);
					supersede(conn, ownerId, ownerType, memory.replacesId(), now);
				}
				case CONFIRM -> {
					require(ACTIVE.equals(state), "Only an active memory can be confirmed");
					setState(conn, ownerId, ownerType, memory, ACTIVE, true, now);
				}
				case DISMISS -> {
					require(SUGGESTED.equals(state) || (ACTIVE.equals(state) && !memory.ownerWrote()),
							"Only a suggestion or a memory you have not confirmed can be dismissed; delete it instead");
					setState(conn, ownerId, ownerType, memory, DISMISSED, false, now);
					if (ACTIVE.equals(state) && reactivate(conn, ownerId, ownerType, memory.replacesId(), now)) {
						restored[0] = memory.replacesId();
					}
				}
				case RESTORE -> {
					require(DISMISSED.equals(state), "Only a dismissed memory can be restored");
					setState(conn, ownerId, ownerType, memory, ACTIVE, false, now);
					supersede(conn, ownerId, ownerType, memory.replacesId(), now);
				}
				case REOPEN -> {
					// the owner's Undo of Keep: back to a suggestion, and what it replaced comes back
					boolean accepted = ACTIVE.equals(state);
					require(DISMISSED.equals(state) || accepted, "Only a dismissed or active memory can be reopened");
					CollaborationDbUtils.update(conn, "UPDATE BRAIN_MEMORY SET STATE = ?, CONFIRMED = ?, "
							+ "CONFIRMED_AT = ?, UPDATED_AT = ?" + OWNED + " AND MEMORY_ID = ?", SUGGESTED, false,
							null, now, ownerId, ownerType, memory.id());
					if (accepted && reactivate(conn, ownerId, ownerType, memory.replacesId(), now)) {
						restored[0] = memory.replacesId();
					}
				}
				case UNCONFIRM -> {
					require(ACTIVE.equals(state) && !BrainProfileUtils.YOU.equals(memory.origin()),
							"Only a memory the assistant saved can be unconfirmed");
					CollaborationDbUtils.update(conn, "UPDATE BRAIN_MEMORY SET CONFIRMED = ?, CONFIRMED_AT = ?, "
							+ "UPDATED_AT = ?" + OWNED + " AND MEMORY_ID = ?", false, null, now, ownerId, ownerType,
							memory.id());
				}
				default -> throw new IllegalArgumentException("Unknown action: " + action);
				}
			});
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("memory", toMap(find(ownerId, ownerType, memoryId), null));
		result.put("restored", restored[0] == null ? null : toMap(find(ownerId, ownerType, restored[0]), null));
		return result;
	}

	// ---- assistant: Remember, Forget, SearchMemories ----

	/**
	 * The assistant saves what the owner told it. The memory is used right away but stays unconfirmed. A memory the
	 * owner wrote is never changed here: the result carries the proposed change for the owner to apply.
	 */
	public static Map<String, Object> remember(User user, Map<String, Object> args, Source source) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String text = cleanText(args.get("text"));
		String kind = kind(args.get("kind"), null);
		List<Ref> about = refs(ownerId, ownerType, args.get("about"));
		Set<String> excluded = neverIngest(ownerId, ownerType);
		for (Ref ref : about) {
			if (PERSON.equals(ref.type()) && excluded.contains(ref.id())) {
				throw new IllegalArgumentException("The owner keeps person " + ref.id()
						+ " out of the assistant's context; do not save memories about them");
			}
		}
		Timestamp expiresAt = CollaborationDbUtils.toTimestamp(args.get("expiresAt"), "expiresAt");
		String replaces = blankToNull(args.get("replaces"));
		Timestamp now = CollaborationDbUtils.now();
		if (expiresAt != null && !expiresAt.after(now)) {
			throw new IllegalArgumentException("expiresAt is in the past; a memory that already expired is not useful");
		}
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			if (replaces != null) {
				Memory target = find(ownerId, ownerType, replaces);
				if (target == null || !ACTIVE.equals(target.state())) {
					throw new IllegalArgumentException("Memory not found: " + replaces);
				}
				Map<String, Object> proposed = proposed(kind, text, about.isEmpty() ? target.about() : about,
						expiresAt);
				if (target.ownerWrote()) {
					return toolResult(ownerId, ownerType, NEEDS_OWNER,
							"The owner wrote or confirmed this memory, so it was not changed. The chat shows them your "
									+ "change to apply.",
							target, null, proposed);
				}
				Memory created = new Memory(newId(ownerId, ownerType), kind, text, ACTIVE, ASSISTANT, false,
						target.pinned(), target.id(), expiresAt, source, now, now, null,
						about.isEmpty() ? target.about() : about);
				CollaborationDbUtils.inTransaction(conn -> {
					insert(conn, ownerId, ownerType, created);
					supersede(conn, ownerId, ownerType, target.id(), now);
				});
				return toolResult(ownerId, ownerType, UPDATED, "Saved; it replaces the older memory.",
						find(ownerId, ownerType, created.id()), find(ownerId, ownerType, target.id()), null);
			}
			Memory same = alike(load(ownerId, ownerType, Set.of(ACTIVE, SUGGESTED, DISMISSED)), text, about);
			if (same != null && ACTIVE.equals(same.state())) {
				return toolResult(ownerId, ownerType, EXISTS, "Already remembered; nothing changed.", same, null,
						null);
			}
			if (same != null && SUGGESTED.equals(same.state())) {
				CollaborationDbUtils.inTransaction(conn -> {
					setState(conn, ownerId, ownerType, same, ACTIVE, false, now);
					supersede(conn, ownerId, ownerType, same.replacesId(), now);
				});
				return toolResult(ownerId, ownerType, SAVED,
						"Saved. It was waiting as a suggestion; it is now in use, and the owner can undo it.",
						find(ownerId, ownerType, same.id()), null, null);
			}
			if (same != null && source.roomId() != null && source.roomId().equals(same.source().roomId())) {
				return toolResult(ownerId, ownerType, UNDONE_HERE,
						"The owner removed this memory in this conversation, so it was not saved again. If they ask "
								+ "for it now, tell them to add it with /remember or in Brain > Memory.",
						same, null, null);
			}
			Memory created = new Memory(newId(ownerId, ownerType), kind, text, ACTIVE, ASSISTANT, false, false, null,
					expiresAt, source, now, now, null, about);
			CollaborationDbUtils.inTransaction(conn -> insert(conn, ownerId, ownerType, created));
			return toolResult(ownerId, ownerType, SAVED, "Saved. The owner sees it in the chat and can undo it.",
					find(ownerId, ownerType, created.id()), null, null);
		}
	}

	// hides a memory the assistant saved; one the owner wrote only goes when they say so
	public static Map<String, Object> forget(User user, String memoryId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Timestamp now = CollaborationDbUtils.now();
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Memory memory = memoryId == null ? null : find(ownerId, ownerType, memoryId);
			if (memory == null || !ACTIVE.equals(memory.state())) {
				throw new IllegalArgumentException("Memory not found: " + memoryId);
			}
			if (memory.ownerWrote()) {
				return toolResult(ownerId, ownerType, NEEDS_OWNER,
						"The owner wrote or confirmed this memory, so it was kept. The chat asks them to remove it.",
						memory, null, null);
			}
			CollaborationDbUtils.inTransaction(conn -> {
				setState(conn, ownerId, ownerType, memory, DISMISSED, false, now);
				reactivate(conn, ownerId, ownerType, memory.replacesId(), now);
			});
			return toolResult(ownerId, ownerType, FORGOTTEN, "Forgotten. The owner can undo it from the chat.",
					find(ownerId, ownerType, memoryId), null, null);
		}
	}

	// active memories ranked against the query; about narrows them to memories sharing one of those links
	public static Map<String, Object> search(User user, String query, List<Ref> about, Integer limit) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Timestamp now = CollaborationDbUtils.now();
		Set<String> excluded = neverIngest(ownerId, ownerType);
		List<Memory> memories = new ArrayList<>();
		for (Memory memory : load(ownerId, ownerType, Set.of(ACTIVE))) {
			if (!memory.expired(now) && !blocked(memory, excluded)
					&& (about == null || about.isEmpty() || memory.about().stream().anyMatch(about::contains))) {
				memories.add(memory);
			}
		}
		Map<Ref, String> names = names(ownerId, ownerType, allRefs(memories));
		List<Memory> ranked = query == null || query.isBlank() ? memories : rank(memories, query, names);
		if (query == null || query.isBlank()) {
			ranked.sort(Comparator.comparing(Memory::updatedAt, Comparator.nullsLast(Comparator.reverseOrder())));
		}
		int max = Math.min(Math.max(limit == null ? SEARCH_LIMIT : limit, 1), MAX_SEARCH_LIMIT);
		List<Map<String, Object>> items = new ArrayList<>();
		for (Memory memory : ranked.subList(0, Math.min(max, ranked.size()))) {
			items.add(toMap(memory, names));
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("items", items);
		result.put("total", ranked.size());
		if (ranked.isEmpty()) {
			result.put("note", "No memory matches. Do not guess; say you do not have it.");
		}
		return result;
	}

	/** One memory the review of a finished chat proposes; refs and replaces are already resolved and checked. */
	record Suggestion(String kind, String text, List<Ref> about, String replacesId, String messageId) {
	}

	/**
	 * Saves the review's proposals as suggestions, which nothing uses until the owner accepts them. One that says
	 * what a memory already says, a pending suggestion, or one the owner turned down is skipped.
	 */
	static List<Memory> suggest(String ownerId, String ownerType, List<Suggestion> suggestions, String threadId,
			String roomId) {
		List<Memory> created = new ArrayList<>();
		Timestamp now = CollaborationDbUtils.now();
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			List<Memory> known = new ArrayList<>(load(ownerId, ownerType, Set.of(ACTIVE, SUGGESTED, DISMISSED)));
			for (Suggestion suggestion : suggestions) {
				if (alike(known, suggestion.text(), suggestion.about()) != null) {
					continue;
				}
				Memory memory = new Memory(newId(ownerId, ownerType), suggestion.kind(), suggestion.text(), SUGGESTED,
						WorkThreadInsights.BRAIN, false, false, suggestion.replacesId(), null,
						new Source(FROM_CHAT_REVIEW, threadId, roomId, suggestion.messageId(), null, null), now, now,
						null, suggestion.about());
				CollaborationDbUtils.inTransaction(conn -> insert(conn, ownerId, ownerType, memory));
				known.add(memory);
				created.add(memory);
			}
		}
		return created;
	}

	/** Where a Remember in a chat came from: the run's room and the thread it belongs to. */
	public static Source chatSource(String threadId, String roomId) {
		return new Source(FROM_CHAT, threadId, roomId, null, null, null);
	}

	/** The id inside "[m:abc]" or "m:abc", as the prompt shows it, or the id itself. */
	public static String memoryIdOf(String value) {
		String id = blankToNull(value);
		if (id == null) {
			return null;
		}
		if (id.startsWith("[") && id.endsWith("]")) {
			id = id.substring(1, id.length() - 1).trim();
		}
		return id.startsWith("m:") ? id.substring(2).trim() : id;
	}

	/** The owner lets the assistant recall and keep memories. */
	public static boolean assistantMemoryOn(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		return BrainProfileUtils.usesMemory(owner.getValue0(), owner.getValue1());
	}

	/** For the assistant's memory tools; the owner's own memory pages work either way. */
	public static void requireAssistantMemory(User user) {
		if (!assistantMemoryOn(user)) {
			throw new IllegalArgumentException("The owner turned memory off; nothing is saved, forgotten, or searched");
		}
	}

	// ---- reading ----

	static List<Memory> load(String ownerId, String ownerType, Collection<String> states) {
		if (states.isEmpty()) {
			return List.of();
		}
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		params.addAll(states);
		List<Memory> rows = CollaborationDbUtils.query("SELECT " + COLUMNS + " FROM BRAIN_MEMORY" + OWNED
				+ " AND STATE IN (" + CollaborationDbUtils.placeholders(states.size()) + ") ORDER BY CREATED_AT, "
				+ "MEMORY_ID", BrainMemoryUtils::mapRow, params.toArray());
		if (rows.isEmpty()) {
			return rows;
		}
		Map<String, List<Ref>> links = links(ownerId, ownerType, null);
		List<Memory> memories = new ArrayList<>(rows.size());
		for (Memory row : rows) {
			memories.add(row.withAbout(links.getOrDefault(row.id(), List.of())));
		}
		return memories;
	}

	static Memory find(String ownerId, String ownerType, String memoryId) {
		Memory row = CollaborationDbUtils.queryOne(
				"SELECT " + COLUMNS + " FROM BRAIN_MEMORY" + OWNED + " AND MEMORY_ID = ?", BrainMemoryUtils::mapRow,
				ownerId, ownerType, memoryId);
		return row == null ? null : row.withAbout(links(ownerId, ownerType, memoryId).getOrDefault(memoryId, List.of()));
	}

	private static Map<String, List<Ref>> links(String ownerId, String ownerType, String memoryId) {
		String one = memoryId == null ? "" : " AND MEMORY_ID = ?";
		Object[] params = memoryId == null ? new Object[] { ownerId, ownerType }
				: new Object[] { ownerId, ownerType, memoryId };
		Map<String, List<Ref>> links = new HashMap<>();
		for (String[] row : CollaborationDbUtils.query("SELECT MEMORY_ID, REF_TYPE, REF_ID FROM BRAIN_MEMORY_LINK"
				+ OWNED + one + " ORDER BY MEMORY_ID, CREATED_AT, REF_TYPE, REF_ID",
				rs -> new String[] { rs.getString("MEMORY_ID"), rs.getString("REF_TYPE"), rs.getString("REF_ID") },
				params)) {
			links.computeIfAbsent(row[0], k -> new ArrayList<>()).add(new Ref(row[1], row[2]));
		}
		return links;
	}

	private static Memory mapRow(ResultSet rs) throws SQLException {
		Source source = new Source(CollaborationDbUtils.getString(rs, "SOURCE_KIND"),
				CollaborationDbUtils.getString(rs, "SOURCE_THREAD_ID"),
				CollaborationDbUtils.getString(rs, "SOURCE_ROOM_ID"), CollaborationDbUtils.getString(rs, "SOURCE_REF"),
				CollaborationDbUtils.getString(rs, "SOURCE_PERSON_ID"),
				CollaborationDbUtils.getString(rs, "SOURCE_LABEL"));
		return new Memory(CollaborationDbUtils.getString(rs, "MEMORY_ID"), CollaborationDbUtils.getString(rs, "KIND"),
				CollaborationDbUtils.getString(rs, "TEXT"), CollaborationDbUtils.getString(rs, "STATE"),
				CollaborationDbUtils.getString(rs, "ORIGIN"),
				Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "CONFIRMED")),
				Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "PINNED")),
				CollaborationDbUtils.getString(rs, "REPLACES_ID"), rs.getTimestamp("EXPIRES_AT"), source,
				rs.getTimestamp("CREATED_AT"), rs.getTimestamp("UPDATED_AT"), rs.getTimestamp("CONFIRMED_AT"),
				List.of());
	}

	/** People the owner excluded from assistant context; their memories are never recalled or searched. */
	static Set<String> neverIngest(String ownerId, String ownerType) {
		return new HashSet<>(CollaborationDbUtils.query("SELECT PERSON_ID FROM BRAIN_PERSON" + OWNED
				+ " AND NEVER_INGEST = ?", rs -> rs.getString("PERSON_ID"), ownerId, ownerType, true));
	}

	/** About, or said by, someone in excluded. */
	static boolean blocked(Memory memory, Set<String> excluded) {
		if (excluded.isEmpty()) {
			return false;
		}
		if (memory.source() != null && memory.source().personId() != null
				&& excluded.contains(memory.source().personId())) {
			return true;
		}
		for (Ref ref : memory.about()) {
			if (PERSON.equals(ref.type()) && excluded.contains(ref.id())) {
				return true;
			}
		}
		return false;
	}

	/** Display names of the people, topics, accounts and threads memories point at. */
	static Map<Ref, String> names(String ownerId, String ownerType, Collection<Ref> refs) {
		Map<String, Set<String>> byType = new HashMap<>();
		for (Ref ref : refs) {
			byType.computeIfAbsent(ref.type(), k -> new LinkedHashSet<>()).add(ref.id());
		}
		Map<Ref, String> names = new HashMap<>();
		nameQuery(names, ownerId, ownerType, PERSON, byType.get(PERSON),
				"SELECT PERSON_ID AS ID, COALESCE(DISPLAY_NAME, EMAIL_NORM) AS NAME FROM BRAIN_PERSON", "PERSON_ID");
		nameQuery(names, ownerId, ownerType, TOPIC, byType.get(TOPIC),
				"SELECT TOPIC_ID AS ID, NAME FROM BRAIN_TOPIC", "TOPIC_ID");
		nameQuery(names, ownerId, ownerType, ACCOUNT, byType.get(ACCOUNT),
				"SELECT ACCOUNT_ID AS ID, NAME FROM BRAIN_ACCOUNT", "ACCOUNT_ID");
		nameQuery(names, ownerId, ownerType, THREAD, byType.get(THREAD),
				"SELECT THREAD_ID AS ID, SUBJECT AS NAME FROM BRAIN_THREAD", "THREAD_ID");
		return names;
	}

	private static void nameQuery(Map<Ref, String> names, String ownerId, String ownerType, String type,
			Set<String> ids, String select, String idColumn) {
		if (ids == null || ids.isEmpty()) {
			return;
		}
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		params.addAll(ids);
		for (String[] row : CollaborationDbUtils.query(select + OWNED + " AND " + idColumn + " IN ("
				+ CollaborationDbUtils.placeholders(ids.size()) + ")",
				rs -> new String[] { rs.getString("ID"), CollaborationDbUtils.getString(rs, "NAME") },
				params.toArray())) {
			if (row[1] != null && !row[1].isBlank()) {
				names.put(new Ref(type, row[0]), row[1]);
			}
		}
	}

	static Set<Ref> allRefs(Collection<Memory> memories) {
		Set<Ref> refs = new LinkedHashSet<>();
		for (Memory memory : memories) {
			refs.addAll(memory.about());
		}
		return refs;
	}

	// ---- output ----

	static Map<String, Object> toMap(Memory memory, Map<Ref, String> names) {
		if (memory == null) {
			return null;
		}
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("id", memory.id());
		map.put("kind", memory.kind());
		map.put("text", memory.text());
		map.put("state", memory.state());
		map.put("origin", memory.origin());
		map.put("confirmed", memory.confirmed());
		map.put("pinned", memory.pinned());
		List<Map<String, Object>> about = new ArrayList<>();
		for (Ref ref : memory.about()) {
			Map<String, Object> link = new LinkedHashMap<>();
			link.put("type", ref.type());
			link.put("id", ref.id());
			if (names != null && names.get(ref) != null) {
				link.put("name", names.get(ref));
			}
			about.add(link);
		}
		map.put("about", about);
		map.put("expiresAt", CollaborationDbUtils.toIso(memory.expiresAt()));
		map.put("replacesId", memory.replacesId());
		Map<String, Object> source = new LinkedHashMap<>();
		Source from = memory.source();
		putIfPresent(source, "kind", from.kind());
		putIfPresent(source, "threadId", from.threadId());
		putIfPresent(source, "roomId", from.roomId());
		putIfPresent(source, "ref", from.ref());
		putIfPresent(source, "personId", from.personId());
		putIfPresent(source, "label", from.label());
		map.put("source", source);
		map.put("createdAt", CollaborationDbUtils.toIso(memory.createdAt()));
		map.put("updatedAt", CollaborationDbUtils.toIso(memory.updatedAt()));
		map.put("confirmedAt", CollaborationDbUtils.toIso(memory.confirmedAt()));
		return map;
	}

	private static Map<String, Object> toolResult(String ownerId, String ownerType, String status, String note,
			Memory memory, Memory replaced, Map<String, Object> proposed) {
		List<Memory> shown = new ArrayList<>(List.of(memory));
		if (replaced != null) {
			shown.add(replaced);
		}
		Map<Ref, String> names = names(ownerId, ownerType, allRefs(shown));
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("status", status);
		result.put("note", note);
		result.put("memory", toMap(memory, names));
		if (replaced != null) {
			result.put("replaced", toMap(replaced, names));
		}
		if (proposed != null) {
			result.put("proposed", proposed);
		}
		return result;
	}

	private static Map<String, Object> proposed(String kind, String text, List<Ref> about, Timestamp expiresAt) {
		Map<String, Object> proposed = new LinkedHashMap<>();
		proposed.put("kind", kind);
		proposed.put("text", text);
		List<Map<String, Object>> links = new ArrayList<>();
		for (Ref ref : about) {
			links.add(Map.of("type", ref.type(), "id", ref.id()));
		}
		proposed.put("about", links);
		proposed.put("expiresAt", CollaborationDbUtils.toIso(expiresAt));
		return proposed;
	}

	// ---- matching ----

	/** The memory that already says this: alike text and a link in common, or neither linked. */
	static Memory alike(List<Memory> memories, String text, List<Ref> about) {
		for (Memory memory : memories) {
			if (WorkThreadInsights.alike(memory.text(), text) && sameScope(memory.about(), about)) {
				return memory;
			}
		}
		return null;
	}

	private static boolean sameScope(List<Ref> a, List<Ref> b) {
		if (a.isEmpty() || b.isEmpty()) {
			return true;
		}
		for (Ref ref : a) {
			if (b.contains(ref)) {
				return true;
			}
		}
		return false;
	}

	/** Matching memories, best first: share of the query's words in the text and link names, then confirmed. */
	static List<Memory> rank(List<Memory> memories, String query, Map<Ref, String> names) {
		Set<String> wanted = terms(query);
		Map<Memory, Double> scores = new HashMap<>();
		for (Memory memory : memories) {
			List<String> linkNames = new ArrayList<>();
			for (Ref ref : memory.about()) {
				if (names.get(ref) != null) {
					linkNames.add(names.get(ref));
				}
			}
			double score = score(wanted, memory.text(), linkNames);
			if (score > 0) {
				scores.put(memory, score);
			}
		}
		List<Memory> ranked = new ArrayList<>(scores.keySet());
		ranked.sort(Comparator.<Memory, Double>comparing(scores::get).reversed()
				.thenComparing(Memory::confirmed, Comparator.reverseOrder())
				.thenComparing(Memory::pinned, Comparator.reverseOrder())
				.thenComparing(Memory::updatedAt, Comparator.nullsLast(Comparator.reverseOrder()))
				.thenComparing(Memory::id));
		return ranked;
	}

	static double score(Set<String> wanted, String text, Collection<String> names) {
		if (wanted.isEmpty()) {
			return 0;
		}
		Set<String> have = terms(text);
		for (String name : names) {
			have.addAll(terms(name));
		}
		int hits = 0;
		for (String word : wanted) {
			if (have.contains(word)) {
				hits++;
			}
		}
		return (double) hits / wanted.size();
	}

	static Set<String> terms(String text) {
		Set<String> terms = new LinkedHashSet<>();
		String norm = text == null ? "" : text.toLowerCase(Locale.ROOT).replaceAll("[^\\p{L}\\p{N}]+", " ").trim();
		if (norm.isEmpty()) {
			return terms;
		}
		for (String word : norm.split(" ")) {
			if (!STOP_WORDS.contains(word)) {
				// plurals match their singular: budgets, budget
				terms.add(word.length() > 3 && word.endsWith("s") && !word.endsWith("ss")
						? word.substring(0, word.length() - 1)
						: word);
			}
		}
		return terms;
	}

	// ---- validation ----

	/** One statement on one line, at most MAX_CHARS, and nothing that looks like a secret. */
	static String cleanText(Object value) {
		String text = value == null ? "" : String.valueOf(value).replaceAll("\\s+", " ").trim();
		if (text.isEmpty()) {
			throw new IllegalArgumentException("Memory text is required");
		}
		if (text.length() > MAX_CHARS) {
			throw new IllegalArgumentException(
					"A memory is at most " + MAX_CHARS + " characters; keep it to one statement");
		}
		if (looksSecret(text)) {
			throw new IllegalArgumentException("Memories cannot hold passwords, keys, or tokens");
		}
		return text;
	}

	static boolean looksSecret(String text) {
		if (SECRET.matcher(text).find()) {
			return true;
		}
		Matcher token = TOKEN.matcher(text);
		while (token.find()) {
			String run = token.group();
			if (!UUID_TEXT.matcher(run).matches() && run.chars().anyMatch(Character::isDigit)
					&& run.chars().anyMatch(Character::isLetter)) {
				return true;
			}
		}
		return false;
	}

	static String kind(Object value, String fallback) {
		String kind = blankToNull(value);
		if (kind == null) {
			if (fallback == null) {
				throw new IllegalArgumentException("kind is required: preference or fact");
			}
			return fallback;
		}
		return check(KINDS, kind.trim().toLowerCase(Locale.ROOT), "kind");
	}

	/**
	 * Links from a list of {type, id} maps or "type:id" strings; each must exist for this owner. Links to a /new
	 * session's thread are dropped, since that thread is never saved.
	 */
	@SuppressWarnings("unchecked")
	static List<Ref> refs(String ownerId, String ownerType, Object value) {
		if (value == null || (value instanceof String s && s.isBlank())) {
			return List.of();
		}
		List<Object> raw = value instanceof List ? (List<Object>) value : List.of(value);
		Set<Ref> refs = new LinkedHashSet<>();
		for (Object item : raw) {
			Ref ref = parseRef(item);
			if (ref == null || (THREAD.equals(ref.type()) && ref.id().startsWith(SESSION_THREAD_PREFIX))) {
				continue;
			}
			try {
				switch (ref.type()) {
				case PERSON -> BrainPeopleUtils.requirePerson(ownerId, ownerType, ref.id());
				case TOPIC -> BrainTopicUtils.requireTopic(ownerId, ownerType, ref.id());
				case ACCOUNT -> BrainPeopleUtils.requireAccount(ownerId, ownerType, ref.id());
				case THREAD -> BrainThreadUtils.requireThread(ownerId, ownerType, ref.id());
				default -> throw new IllegalArgumentException("about type must be one of " + REF_TYPES);
				}
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(
						"about: there is no " + ref.type() + " with id " + ref.id() + "; use an id from the context");
			}
			refs.add(ref);
		}
		if (refs.size() > MAX_LINKS) {
			throw new IllegalArgumentException("A memory can be about at most " + MAX_LINKS + " people or things");
		}
		return new ArrayList<>(refs);
	}

	/** One {type, id} map or "type:id" string; null for an empty entry. Does not check that it exists. */
	public static Ref parseRef(Object item) {
		String type;
		String id;
		if (item == null) {
			return null;
		} else if (item instanceof Map<?, ?> map) {
			type = blankToNull(map.get("type"));
			id = blankToNull(map.get("id"));
		} else {
			String text = String.valueOf(item).trim();
			int colon = text.indexOf(':');
			if (text.isEmpty()) {
				return null;
			}
			if (colon <= 0) {
				throw new IllegalArgumentException("about entries are {type, id}, for example {\"type\": \"person\", "
						+ "\"id\": \"...\"}");
			}
			type = text.substring(0, colon);
			id = text.substring(colon + 1);
		}
		if (type == null || id == null) {
			throw new IllegalArgumentException("about entries need a type and an id");
		}
		return new Ref(check(REF_TYPES, type.trim().toLowerCase(Locale.ROOT), "about type"), id.trim());
	}

	// ---- writing ----

	private static Memory created(String ownerId, String ownerType, Map<String, Object> memory, Timestamp now) {
		return new Memory(newId(ownerId, ownerType), kind(memory.get("kind"), FACT), cleanText(memory.get("text")),
				ACTIVE, BrainProfileUtils.YOU, true, isTrue(memory.get("pinned")), null,
				CollaborationDbUtils.toTimestamp(memory.get("expiresAt"), "expiresAt"), Source.ui(), now, now, now,
				refs(ownerId, ownerType, memory.get("about")));
	}

	// the session Undo of a delete: the same id, with what the client still holds
	private static Memory restored(String ownerId, String ownerType, String id, Map<String, Object> memory,
			Timestamp now) {
		if (!ID.matcher(id).matches()) {
			throw new IllegalArgumentException("Memory not found");
		}
		String state = blankToNull(memory.get("state"));
		state = state == null ? ACTIVE : state;
		if (!ACTIVE.equals(state) && !SUGGESTED.equals(state)) {
			throw new IllegalArgumentException("A restored memory is active or suggested");
		}
		String origin = blankToNull(memory.get("origin"));
		origin = origin == null ? BrainProfileUtils.YOU : check(ORIGINS, origin, "origin");
		boolean confirmed = memory.containsKey("confirmed") ? isTrue(memory.get("confirmed"))
				: BrainProfileUtils.YOU.equals(origin);
		return new Memory(id, kind(memory.get("kind"), FACT), cleanText(memory.get("text")), state, origin, confirmed,
				isTrue(memory.get("pinned")), null,
				CollaborationDbUtils.toTimestamp(memory.get("expiresAt"), "expiresAt"), Source.ui(), now, now,
				confirmed ? now : null, refs(ownerId, ownerType, memory.get("about")));
	}

	static void insert(Connection conn, String ownerId, String ownerType, Memory memory) throws SQLException {
		Source source = memory.source() == null ? Source.ui() : memory.source();
		CollaborationDbUtils.update(conn,
				"INSERT INTO BRAIN_MEMORY (OWNER_ID, OWNER_TYPE, " + COLUMNS + ") VALUES (?, ?, "
						+ CollaborationDbUtils.placeholders(COLUMN_COUNT) + ")",
				ownerId, ownerType, memory.id(), memory.kind(), memory.text(), memory.state(), memory.origin(),
				memory.confirmed(), memory.pinned(), memory.replacesId(), memory.expiresAt(), source.kind(),
				source.threadId(), source.roomId(), source.ref(), source.personId(), source.label(),
				memory.createdAt(), memory.updatedAt(), memory.confirmedAt());
		insertLinks(conn, ownerId, ownerType, memory.id(), memory.about(), memory.createdAt());
	}

	private static void insertLinks(Connection conn, String ownerId, String ownerType, String memoryId,
			List<Ref> about, Timestamp now) throws SQLException {
		for (Ref ref : about) {
			CollaborationDbUtils.update(conn,
					"INSERT INTO BRAIN_MEMORY_LINK (OWNER_ID, OWNER_TYPE, MEMORY_ID, REF_TYPE, REF_ID, CREATED_AT) "
							+ "VALUES (?, ?, ?, ?, ?, ?)",
					ownerId, ownerType, memoryId, ref.type(), ref.id(), now);
		}
	}

	private static void setState(Connection conn, String ownerId, String ownerType, Memory memory, String state,
			boolean confirm, Timestamp now) throws SQLException {
		if (confirm) {
			CollaborationDbUtils.update(conn,
					"UPDATE BRAIN_MEMORY SET STATE = ?, CONFIRMED = ?, CONFIRMED_AT = ?, UPDATED_AT = ?" + OWNED
							+ " AND MEMORY_ID = ?",
					state, true, memory.confirmedAt() == null ? now : memory.confirmedAt(), now, ownerId, ownerType,
					memory.id());
		} else {
			CollaborationDbUtils.update(conn,
					"UPDATE BRAIN_MEMORY SET STATE = ?, UPDATED_AT = ?" + OWNED + " AND MEMORY_ID = ?", state, now,
					ownerId, ownerType, memory.id());
		}
	}

	// the memory a newer one replaced steps aside
	private static void supersede(Connection conn, String ownerId, String ownerType, String memoryId, Timestamp now)
			throws SQLException {
		if (memoryId != null) {
			CollaborationDbUtils.update(conn, "UPDATE BRAIN_MEMORY SET STATE = ?, UPDATED_AT = ?" + OWNED
					+ " AND MEMORY_ID = ? AND STATE = ?", SUPERSEDED, now, ownerId, ownerType, memoryId, ACTIVE);
		}
	}

	// and comes back when the newer one goes
	private static boolean reactivate(Connection conn, String ownerId, String ownerType, String memoryId,
			Timestamp now) throws SQLException {
		return memoryId != null && CollaborationDbUtils.update(conn, "UPDATE BRAIN_MEMORY SET STATE = ?, "
				+ "UPDATED_AT = ?" + OWNED + " AND MEMORY_ID = ? AND STATE = ?", ACTIVE, now, ownerId, ownerType,
				memoryId, SUPERSEDED) > 0;
	}

	// ---- ids ----

	static String newId(String ownerId, String ownerType) {
		for (;;) {
			String id = shortId(RANDOM.nextLong());
			if (!CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_MEMORY" + OWNED + " AND MEMORY_ID = ?", ownerId,
					ownerType, id)) {
				return id;
			}
		}
	}

	/** The same id for the same source row on every run, so a repeated migration finds what it already moved. */
	static String stableId(String ownerId, String ownerType, String... keyParts) {
		UUID uuid = UUID.fromString(CollaborationDbUtils.deterministicId(ownerId, ownerType, keyParts));
		return shortId(uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits());
	}

	// 12 characters from 60 bits; short ids keep the prompt small
	static String shortId(long bits) {
		char[] id = new char[12];
		for (int i = 0; i < id.length; i++) {
			id[i] = ID_ALPHABET.charAt((int) ((bits >>> (59 - 5 * i)) & 31));
		}
		return new String(id);
	}

	// ---- small helpers ----

	private static String check(Set<String> allowed, String value, String name) {
		if (value == null || !allowed.contains(value)) {
			throw new IllegalArgumentException(name + " must be one of " + allowed);
		}
		return value;
	}

	private static void require(boolean condition, String message) {
		if (!condition) {
			throw new IllegalArgumentException(message);
		}
	}

	private static void putIfPresent(Map<String, Object> map, String key, Object value) {
		if (value != null) {
			map.put(key, value);
		}
	}

	static String blankToNull(Object value) {
		String text = CollaborationDbUtils.asString(value);
		return text == null || text.isBlank() ? null : text.trim();
	}

	// pixel booleans arrive as Boolean or text
	static boolean isTrue(Object value) {
		return value instanceof Boolean b ? b : value != null && Boolean.parseBoolean(String.valueOf(value).trim());
	}
}
