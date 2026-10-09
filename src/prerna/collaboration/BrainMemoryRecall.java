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
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.collaboration.BrainMemoryUtils.Memory;
import prerna.collaboration.BrainMemoryUtils.Ref;
import prerna.collaboration.BrainRulesGate.Rule;
import prerna.engine.impl.model.Room;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * What a thread's assistant is told it remembers: the owner's active memories that apply to the thread, ranked into
 * the prompt's budget. Memories about people the owner keeps out, said by them, or about a thread a rule keeps out
 * are left out, by the same rules the context block uses for messages and facts.
 */
public final class BrainMemoryRecall {

	private static final Logger classLogger = LogManager.getLogger(BrainMemoryRecall.class);

	static final int MAX_MEMORIES = 40;
	static final int DEFAULT_PROMPT_CHARS = 4000;
	// a line's id, brackets, and notes beside the text
	private static final int LINE_OVERHEAD = 60;

	// why a memory applies, in prompt order
	static final String PINNED = "pinned";
	static final String PREFERENCES = "preferences";
	static final String THIS_THREAD = "thread";
	static final String PEOPLE = "people";
	static final String TOPICS = "topics";
	static final String GENERAL = "general";
	private static final List<String> ORDER = List.of(PINNED, PREFERENCES, THIS_THREAD, PEOPLE, TOPICS, GENERAL);

	private BrainMemoryRecall() {
	}

	/** The people, topics and accounts of one thread, and who on it the owner keeps out. */
	record Scope(String threadId, Set<String> people, Set<String> topics, Set<String> accounts, boolean keptOut,
			Set<String> excluded) {
	}

	/** One recalled memory and why it applies. */
	record Line(Memory memory, String bucket) {
	}

	/** The memories that fit the budget, and how many applied but did not. */
	record Recall(List<Line> lines, int hidden, Map<Ref, String> names) {
	}

	// ---- for the harness and the client ----

	/**
	 * The Memory section that ends a thread assistant's prompt, or null when the owner has memory off. Never throws:
	 * a failure here only costs the run its memories.
	 */
	public static String promptBlock(User user, Room room) {
		String threadId = CollaborationUtils.threadIdOf(room);
		try {
			Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
			if (!BrainProfileUtils.usesMemory(owner.getValue0(), owner.getValue1())) {
				return null;
			}
			return render(recall(owner.getValue0(), owner.getValue1(), threadId,
					BrainTopicBrief.chatTopics(user, room), promptChars()));
		} catch (RuntimeException e) {
			classLogger.warn("Could not recall memories for room {}; the run goes on without them", room.getId(), e);
			return null;
		}
	}

	/**
	 * BrainRecallMemories: exactly what the thread's assistant gets, for the thread's Context panel. With a roomId
	 * the chat's topics are used, as in that chat's runs.
	 */
	public static Map<String, Object> recallMemories(User user, String threadId, String roomId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> result = new LinkedHashMap<>();
		boolean on = BrainProfileUtils.usesMemory(owner.getValue0(), owner.getValue1());
		result.put("enabled", on);
		List<Map<String, Object>> items = new ArrayList<>();
		if (!on) {
			result.put("items", items);
			result.put("hidden", 0);
			result.put("prompt", null);
			return result;
		}
		List<String> chatTopics = null;
		if (roomId != null) {
			threadId = threadId != null ? threadId : BrainTopicRoomUtils.requireChat(user, roomId).threadId();
			chatTopics = BrainTopicBrief.applying(user, BrainTopicRoomUtils.topicsOf(user, roomId));
		}
		Recall recall = recall(owner.getValue0(), owner.getValue1(), threadId, chatTopics, promptChars());
		for (Line line : recall.lines()) {
			Map<String, Object> item = BrainMemoryUtils.toMap(line.memory(), recall.names());
			item.put("bucket", line.bucket());
			items.add(item);
		}
		result.put("items", items);
		result.put("hidden", recall.hidden());
		result.put("prompt", render(recall));
		return result;
	}

	// chatTopics replaces the thread's own topics; null keeps them
	static Recall recall(String ownerId, String ownerType, String threadId, List<String> chatTopics, int maxChars) {
		Scope scope = scope(ownerId, ownerType, threadId, chatTopics);
		List<Memory> active = BrainMemoryUtils.load(ownerId, ownerType, Set.of(BrainMemoryUtils.ACTIVE));
		List<Line> lines = select(active, scope, CollaborationDbUtils.now());
		List<Line> shown = budget(lines, maxChars);
		List<Memory> memories = new ArrayList<>();
		for (Line line : shown) {
			memories.add(line.memory());
		}
		return new Recall(shown, lines.size() - shown.size(),
				BrainMemoryUtils.names(ownerId, ownerType, BrainMemoryUtils.allRefs(memories)));
	}

	// ---- the thread ----

	static Scope scope(String ownerId, String ownerType, String threadId, List<String> chatTopics) {
		Set<String> neverIngest = BrainMemoryUtils.neverIngest(ownerId, ownerType);
		String source = threadId == null || threadId.startsWith(BrainMemoryUtils.SESSION_THREAD_PREFIX) ? null
				: CollaborationDbUtils.queryOne(
						"SELECT SOURCE FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?",
						rs -> String.valueOf(rs.getString("SOURCE")), ownerId, ownerType, threadId);
		if (source == null) {
			// a /new session or an unknown thread: what applies everywhere, and the chat's topics
			Set<String> topics = chatTopics == null ? Set.of() : new HashSet<>(chatTopics);
			return new Scope(null, Set.of(), topics, topicAccounts(ownerId, ownerType, topics), false, neverIngest);
		}
		List<String> allTopics = new ArrayList<>();
		Set<String> topics = new HashSet<>();
		for (String[] link : CollaborationDbUtils.query("SELECT TOPIC_ID, SOURCE FROM BRAIN_THREAD_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?",
				rs -> new String[] { rs.getString("TOPIC_ID"), rs.getString("SOURCE") }, ownerId, ownerType,
				threadId)) {
			allTopics.add(link[0]);
			if (!BrainTopicUtils.SUGGESTED.equals(link[1])) {
				topics.add(link[0]);
			}
		}
		List<Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		boolean keptOut = keptOut(rules, source, allTopics);
		Set<String> excluded = new HashSet<>(neverIngest);
		Set<String> people = new HashSet<>();
		Set<String> accounts = new HashSet<>();
		for (String[] row : CollaborationDbUtils.query("SELECT tp.PERSON_ID, tp.INCLUDED, p.EMAIL_NORM, p.ACCOUNT_ID "
				+ "FROM BRAIN_THREAD_PARTICIPANT tp LEFT JOIN BRAIN_PERSON p ON p.OWNER_ID = tp.OWNER_ID "
				+ "AND p.OWNER_TYPE = tp.OWNER_TYPE AND p.PERSON_ID = tp.PERSON_ID "
				+ "WHERE tp.OWNER_ID = ? AND tp.OWNER_TYPE = ? AND tp.THREAD_ID = ?",
				rs -> new String[] { rs.getString("PERSON_ID"),
						String.valueOf(!Boolean.FALSE.equals(CollaborationDbUtils.getBoolean(rs, "INCLUDED"))),
						rs.getString("EMAIL_NORM"), rs.getString("ACCOUNT_ID") },
				ownerId, ownerType, threadId)) {
			String personId = row[0];
			boolean keep = !keptOut && Boolean.parseBoolean(row[1]) && !neverIngest.contains(personId)
					&& BrainRulesGate.neverRule(rules, row[2] == null ? "" : row[2], personId, null) == null
					&& BrainRulesGate.exclusionRule(rules, allTopics, source, personId) == null;
			if (keep) {
				people.add(personId);
				if (row[3] != null) {
					accounts.add(row[3]);
				}
			} else {
				excluded.add(personId);
			}
		}
		// in a chat, its own topics stand in for the thread's
		if (chatTopics != null) {
			topics = new HashSet<>(chatTopics);
		}
		accounts.addAll(topicAccounts(ownerId, ownerType, topics));
		return new Scope(threadId, people, topics, accounts, keptOut, excluded);
	}

	private static Set<String> topicAccounts(String ownerId, String ownerType, Set<String> topics) {
		if (topics.isEmpty()) {
			return new HashSet<>();
		}
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		params.addAll(topics);
		return new HashSet<>(CollaborationDbUtils.query("SELECT ACCOUNT_ID FROM BRAIN_TOPIC WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND ACCOUNT_ID IS NOT NULL AND TOPIC_ID IN ("
				+ CollaborationDbUtils.placeholders(topics.size()) + ")", rs -> rs.getString("ACCOUNT_ID"),
				params.toArray()));
	}

	// a channel or topic rule with no person keeps the whole thread out of the assistant
	static boolean keptOut(List<Rule> rules, String source, List<String> topicIds) {
		for (Rule rule : rules) {
			if (rule.personId() != null) {
				continue;
			}
			String channel = rule.channel() != null ? rule.channel() : rule.value();
			String topicId = rule.topicId() != null ? rule.topicId() : rule.value();
			if (("exclude_channel".equals(rule.kind()) && source.equals(channel))
					|| ("exclude_topic".equals(rule.kind()) && topicIds.contains(topicId))) {
				return true;
			}
		}
		return false;
	}

	// ---- choosing ----

	/** Every memory that applies to the scope, in prompt order. */
	static List<Line> select(List<Memory> active, Scope scope, Timestamp now) {
		List<Line> lines = new ArrayList<>();
		for (Memory memory : active) {
			if (!BrainMemoryUtils.ACTIVE.equals(memory.state()) || memory.expired(now)
					|| BrainMemoryUtils.blocked(memory, scope.excluded())) {
				continue;
			}
			String bucket = bucket(memory, scope);
			if (bucket != null) {
				lines.add(new Line(memory, bucket));
			}
		}
		lines.sort(Comparator.<Line>comparingInt(line -> ORDER.indexOf(line.bucket()))
				.thenComparing(line -> line.memory().confirmed(), Comparator.reverseOrder())
				.thenComparing(line -> line.memory().updatedAt(), Comparator.nullsLast(Comparator.reverseOrder()))
				.thenComparing(line -> line.memory().id()));
		return lines;
	}

	// null when the memory is about other people, topics or threads
	static String bucket(Memory memory, Scope scope) {
		if (memory.pinned()) {
			return PINNED;
		}
		if (memory.about().isEmpty()) {
			return BrainMemoryUtils.PREFERENCE.equals(memory.kind()) ? PREFERENCES : GENERAL;
		}
		boolean thread = false;
		boolean people = false;
		boolean topics = false;
		for (Ref ref : memory.about()) {
			switch (ref.type()) {
			case BrainMemoryUtils.THREAD -> thread |= !scope.keptOut() && ref.id().equals(scope.threadId());
			case BrainMemoryUtils.PERSON -> people |= scope.people().contains(ref.id());
			case BrainMemoryUtils.TOPIC -> topics |= scope.topics().contains(ref.id());
			case BrainMemoryUtils.ACCOUNT -> topics |= scope.accounts().contains(ref.id());
			default -> {
			}
			}
		}
		return thread ? THIS_THREAD : people ? PEOPLE : topics ? TOPICS : null;
	}

	/** The lines that fit, in order; a long memory that does not fit lets shorter ones after it in. */
	static List<Line> budget(List<Line> lines, int maxChars) {
		List<Line> shown = new ArrayList<>();
		int used = 0;
		for (Line line : lines) {
			int cost = line.memory().text().length() + LINE_OVERHEAD;
			if (shown.size() >= MAX_MEMORIES) {
				break;
			}
			if (used + cost <= maxChars) {
				shown.add(line);
				used += cost;
			}
		}
		return shown;
	}

	// ---- the prompt ----

	static String render(Recall recall) {
		StringBuilder out = new StringBuilder(CollaborationPrompts.MEMORY);
		out.append("\n\n### What you remember for this thread\n");
		List<Line> confirmed = new ArrayList<>();
		List<Line> learned = new ArrayList<>();
		for (Line line : recall.lines()) {
			(line.memory().confirmed() ? confirmed : learned).add(line);
		}
		if (confirmed.isEmpty() && learned.isEmpty()) {
			out.append("Nothing yet. Memories saved in other threads that do not apply here are not shown; use "
					+ "SearchMemories if the owner asks about them.");
			return out.toString();
		}
		out.append("Each line starts with the memory's id, for replaces and Forget.");
		if (!confirmed.isEmpty()) {
			out.append("\nConfirmed by the owner:");
			for (Line line : confirmed) {
				out.append('\n').append(line(line, recall.names()));
			}
		}
		if (!learned.isEmpty()) {
			out.append("\nLearned, not confirmed (background only):");
			for (Line line : learned) {
				out.append('\n').append(line(line, recall.names()));
			}
		}
		if (recall.hidden() > 0) {
			out.append("\n").append(recall.hidden()).append(recall.hidden() == 1 ? " more memory applies" :
					" more memories apply").append(" but did not fit; use SearchMemories to find them.");
		}
		return out.toString();
	}

	private static String line(Line line, Map<Ref, String> names) {
		Memory memory = line.memory();
		List<String> notes = new ArrayList<>();
		if (BrainMemoryUtils.PREFERENCE.equals(memory.kind())) {
			notes.add("preference");
		}
		List<String> about = new ArrayList<>();
		for (Ref ref : memory.about()) {
			if (about.size() == 3) {
				about.add("others");
				break;
			}
			String name = names.get(ref);
			about.add(BrainMemoryUtils.THREAD.equals(ref.type()) ? "this thread"
					: name != null ? name : ref.type() + " " + ref.id());
		}
		if (!about.isEmpty()) {
			notes.add("about " + String.join(", ", about));
		}
		if (memory.expiresAt() != null) {
			notes.add("until " + day(memory.expiresAt()));
		}
		if (!memory.confirmed() && memory.updatedAt() != null) {
			notes.add("saved in chat " + day(memory.updatedAt()));
		}
		return "- [m:" + memory.id() + "] " + memory.text() + (notes.isEmpty() ? "" : " (" + String.join("; ", notes)
				+ ")");
	}

	private static String day(Timestamp at) {
		return at.toLocalDateTime().toLocalDate().toString();
	}

	private static int promptChars() {
		String value = Utility.getDIHelperProperty(Constants.COLLAB_MEMORY_PROMPT_CHARS);
		if (value == null || value.isBlank()) {
			return DEFAULT_PROMPT_CHARS;
		}
		try {
			return Math.max(500, Integer.parseInt(value.trim()));
		} catch (NumberFormatException e) {
			classLogger.warn("{} is not a number: {}; using {}", Constants.COLLAB_MEMORY_PROMPT_CHARS, value,
					DEFAULT_PROMPT_CHARS);
			return DEFAULT_PROMPT_CHARS;
		}
	}
}
