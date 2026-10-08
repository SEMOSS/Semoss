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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.om.Insight;

/**
 * What the owner's assistant can change in Brain: one call per topic, one per thread. Each step is the
 * same code the owner's own screens run, so the assistant can do nothing the owner could not.
 */
public final class BrainAgentEdits {

	private static final int MAX_CANDIDATES = 5;

	private BrainAgentEdits() {
	}

	// ---- topic ----

	public static Map<String, Object> editTopic(User user, String topicId, String topicName, String name,
			String description, String status, List<String> addPeople, List<String> removePeople, String addGoal,
			String addNote, String deleteNoteId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String topic = BrainThreadFinder.resolveTopic(ownerId, ownerType, topicId, topicName);

		// find everyone first, so a person Brain does not know stops the call before anything changes
		List<Map<String, String>> adds = new ArrayList<>();
		for (String ref : clean(addPeople)) {
			adds.add(person(ownerId, ownerType, ref));
		}
		List<Map<String, String>> removes = new ArrayList<>();
		for (String ref : clean(removePeople)) {
			removes.add(person(ownerId, ownerType, ref));
		}

		List<String> changes = new ArrayList<>();
		try {
			if (isSet(status)) {
				BrainTopicUtils.setTopicStatus(user, topic, status.trim());
				changes.add("status set to " + status.trim());
			}
			Map<String, Object> fields = new LinkedHashMap<>();
			fields.put("id", topic);
			if (isSet(name)) {
				fields.put("name", name.trim());
				changes.add("renamed to " + name.trim());
			}
			if (description != null) {
				fields.put("description", description);
				changes.add("description updated");
			}
			if (fields.size() > 1) {
				BrainTopicUtils.saveTopic(user, fields);
			}
			for (Map<String, String> person : adds) {
				BrainTopicUtils.setTopicPerson(user, topic, person.get("id"), BrainTopicUtils.MEMBER, null);
				changes.add("added " + label(person));
			}
			for (Map<String, String> person : removes) {
				BrainTopicUtils.setTopicPerson(user, topic, person.get("id"), BrainTopicUtils.REMOVED, null);
				changes.add("removed " + label(person));
			}
			if (isSet(addGoal)) {
				BrainTopicUtils.saveTopicNote(user, topic, null, BrainTopicUtils.GOAL, addGoal.trim(), "open");
				changes.add("goal added");
			}
			if (isSet(addNote)) {
				// a topic's notes are Brain memories about it, saved as the owner's topic page saves them
				Map<String, Object> note = new LinkedHashMap<>();
				note.put("kind", BrainMemoryUtils.FACT);
				note.put("text", addNote.trim());
				note.put("about", List.of(Map.of("type", BrainMemoryUtils.TOPIC, "id", topic)));
				BrainMemoryUtils.saveMemory(user, note);
				changes.add("note added");
			}
			if (isSet(deleteNoteId)) {
				deleteNote(user, ownerId, ownerType, topic, deleteNoteId.trim());
				changes.add("goal or note deleted");
			}
		} catch (RuntimeException e) {
			throw partial(changes, e);
		}
		if (changes.isEmpty()) {
			throw new IllegalArgumentException("Pass at least one change to make to the topic");
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("changes", changes);
		out.put("topic", BrainTopicUtils.getTopic(user, topic, null));
		return out;
	}

	// a goal is a BRAIN_TOPIC_NOTE row; a note is a memory linked to the topic (its id from ListTopics' notes)
	private static void deleteNote(User user, String ownerId, String ownerType, String topic, String noteId) {
		boolean goal = CollaborationDbUtils.queryOne("SELECT NOTE_ID FROM BRAIN_TOPIC_NOTE WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND TOPIC_ID = ? AND NOTE_ID = ?", rs -> rs.getString(1), ownerId, ownerType, topic,
				noteId) != null;
		if (goal) {
			BrainTopicUtils.deleteTopicNote(user, topic, noteId);
			return;
		}
		String memoryId = BrainMemoryUtils.memoryIdOf(noteId);
		boolean note = CollaborationDbUtils.queryOne("SELECT MEMORY_ID FROM BRAIN_MEMORY_LINK WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND MEMORY_ID = ? AND REF_TYPE = ? AND REF_ID = ?", rs -> rs.getString(1), ownerId,
				ownerType, memoryId, BrainMemoryUtils.TOPIC, topic) != null;
		if (!note) {
			throw new IllegalArgumentException("This topic has no goal or note " + noteId + "; take the id from ListTopics");
		}
		BrainMemoryUtils.deleteMemory(user, memoryId);
	}

	// ---- thread ----

	public static Map<String, Object> editThread(User user, Insight insight, String threadId, String addTopic,
			String removeTopic, Boolean makePrimary, Boolean muted, Boolean notAutomated) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		if (!isSet(threadId)) {
			throw new IllegalArgumentException("Must pass a threadId");
		}
		BrainThreadUtils.requireThread(ownerId, ownerType, threadId);

		List<String> changes = new ArrayList<>();
		Map<String, Object> out = new LinkedHashMap<>();
		try {
			if (isSet(addTopic)) {
				String topic = topic(ownerId, ownerType, addTopic);
				out.put("topicLinks", BrainThreadUtils.linkThreadTopic(user, threadId, topic,
						Boolean.TRUE.equals(makePrimary), false));
				changes.add("tagged with " + topicName(ownerId, ownerType, topic));
			}
			if (isSet(removeTopic)) {
				String topic = topic(ownerId, ownerType, removeTopic);
				out.put("topicLinks", BrainThreadUtils.linkThreadTopic(user, threadId, topic, false, true));
				changes.add("untagged from " + topicName(ownerId, ownerType, topic));
			}
			if (muted != null) {
				BrainThreadUtils.setThreadMuted(user, threadId, muted);
				changes.add(muted ? "muted" : "unmuted");
			}
			if (notAutomated != null) {
				BrainThreadUtils.setThreadNotAutomated(user, insight, threadId, notAutomated);
				changes.add(notAutomated ? "marked not automated" : "not-automated correction removed");
			}
		} catch (RuntimeException e) {
			throw partial(changes, e);
		}
		if (changes.isEmpty()) {
			throw new IllegalArgumentException("Pass at least one change to make to the thread");
		}
		out.put("threadId", threadId);
		out.put("changes", changes);
		return out;
	}

	// ---- lookups ----

	// a topic id, or a topic name that picks exactly one
	private static String topic(String ownerId, String ownerType, String ref) {
		String value = ref.trim();
		boolean isId = CollaborationDbUtils.queryOne("SELECT TOPIC_ID FROM BRAIN_TOPIC WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND TOPIC_ID = ?", rs -> rs.getString(1), ownerId, ownerType, value) != null;
		return isId ? value : BrainThreadFinder.resolveTopic(ownerId, ownerType, null, value);
	}

	private static String topicName(String ownerId, String ownerType, String topicId) {
		String name = CollaborationDbUtils.queryOne("SELECT NAME FROM BRAIN_TOPIC WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND TOPIC_ID = ?", rs -> rs.getString(1), ownerId, ownerType, topicId);
		return name == null ? topicId : name;
	}

	// a person by id, email, or name ("Weaver, Chrissy" and "Chrissy Weaver" both work); must pick exactly one
	private static Map<String, String> person(String ownerId, String ownerType, String ref) {
		String needle = ref.trim().toLowerCase(Locale.ROOT);
		List<Map<String, String>> people = CollaborationDbUtils.query(
				"SELECT PERSON_ID, DISPLAY_NAME, EMAIL_NORM FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> {
					Map<String, String> row = new LinkedHashMap<>();
					row.put("id", rs.getString(1));
					row.put("name", CollaborationDbUtils.getString(rs, "DISPLAY_NAME"));
					row.put("email", CollaborationDbUtils.getString(rs, "EMAIL_NORM"));
					return row;
				}, ownerId, ownerType);
		List<Map<String, String>> found = new ArrayList<>();
		for (Map<String, String> p : people) {
			if (needle.equals(p.get("id").toLowerCase(Locale.ROOT))
					|| needle.equals(lower(p.get("email")))) {
				return p;
			}
		}
		// an address the person uses besides their main one
		String byAddress = CollaborationDbUtils.queryOne("SELECT PERSON_ID FROM BRAIN_PERSON_ADDRESS WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND VALUE_NORM = ?", rs -> rs.getString(1), ownerId, ownerType, needle);
		if (byAddress != null) {
			for (Map<String, String> p : people) {
				if (byAddress.equals(p.get("id"))) {
					return p;
				}
			}
		}
		for (Map<String, String> p : people) {
			if (needle.equals(lower(p.get("name")))) {
				found.add(p);
			}
		}
		if (found.isEmpty()) {
			String[] tokens = needle.split("[\\s,]+");
			for (Map<String, String> p : people) {
				String name = lower(p.get("name"));
				boolean all = !name.isEmpty();
				for (String token : tokens) {
					all &= !token.isEmpty() && name.contains(token);
				}
				if (all) {
					found.add(p);
				}
			}
		}
		if (found.isEmpty()) {
			throw new IllegalArgumentException("No one in Brain matches " + ref + "; use a name or email Brain knows");
		}
		if (found.size() > 1) {
			List<String> options = new ArrayList<>();
			for (Map<String, String> p : found.subList(0, Math.min(found.size(), MAX_CANDIDATES))) {
				options.add(label(p) + " [" + p.get("id") + "]");
			}
			throw new IllegalArgumentException("More than one person matches " + ref + ": " + String.join("; ", options)
					+ ". Pass the email or id.");
		}
		return found.get(0);
	}

	// ---- small helpers ----

	private static String label(Map<String, String> person) {
		String name = person.get("name");
		String email = person.get("email");
		return name != null && !name.isBlank() ? name : email != null ? email : person.get("id");
	}

	private static List<String> clean(List<String> values) {
		List<String> out = new ArrayList<>();
		if (values != null) {
			for (String v : values) {
				if (isSet(v)) {
					out.add(v.trim());
				}
			}
		}
		return out;
	}

	private static boolean isSet(String value) {
		return value != null && !value.isBlank();
	}

	private static String lower(String value) {
		return value == null ? "" : value.toLowerCase(Locale.ROOT);
	}

	// later steps can fail after earlier ones landed; say which
	private static IllegalArgumentException partial(List<String> done, RuntimeException e) {
		String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
		return new IllegalArgumentException(
				done.isEmpty() ? message : "Already done: " + String.join(", ", done) + ". Then failed: " + message, e);
	}
}
