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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * What is filed under each topic, read fresh for every classifier run and shown to the model as example subjects.
 * So each filing and each correction changes what the next email or chat is matched against. Nothing is stored:
 * a merge, a delete or an undo moves the links this reads.
 */
final class BrainTopicEvidence {

	// examples a topic shows; fewer each as the topic list grows, so a model call stays about the size the filer
	// was measured at (25 topics with 8 example subjects each)
	private static final int MAX_EXAMPLES = 8;
	private static final int MIN_EXAMPLES = 3;
	private static final int ALL_EXAMPLES = 200;
	private static final int SUBJECT_CHARS = 70;
	// the wording the filer was measured with
	private static final String LABEL = " Threads such as: ";
	// a thread whose subject can stand as an example: not muted, not automated, and not a chat named only by who
	// is in it (BrainTopicSuggest leaves those out the same way)
	private static final String USABLE = " AND (t.MUTED IS NULL OR t.MUTED = ?) AND (t.AUTOMATED IS NULL OR "
			+ "t.AUTOMATED = ?) AND t.SUBJECT IS NOT NULL AND NOT (t.SOURCE = ? AND t.SUBJECT LIKE ?)";

	private BrainTopicEvidence() {
	}

	/** One example subject; threadId is null when more than one thread carries it. */
	record Example(String threadId, String subject) {
	}

	/** Example subjects by topic id, strongest first ("other" holds the way out's); each is how many a topic shows. */
	record Evidence(Map<String, List<Example>> examples, int each) {
	}

	/** The examples for these topics (the way out included); no queries when there is no topic. */
	static Evidence load(String ownerId, String ownerType, List<BrainClassifier.TopicOption> topics,
			List<BrainRulesGate.Rule> rules) {
		Set<String> topicIds = new LinkedHashSet<>();
		for (BrainClassifier.TopicOption topic : topics) {
			if (!BrainThreadClassifier.OTHER_TOPIC.equals(topic.id())) {
				topicIds.add(topic.id());
			}
		}
		if (topicIds.isEmpty()) {
			return new Evidence(Map.of(), 0);
		}
		int each = Math.max(MIN_EXAMPLES, Math.min(MAX_EXAMPLES, ALL_EXAMPLES / topicIds.size()));
		// one more than shown, so a topic keeps its full list when the scored thread is one of them
		int keep = each + 1;
		String chats = BrainGraphHeaderSource.CHAT_PREFIX + "%";
		Map<String, List<Example>> examples = new HashMap<>();

		// what is filed: the owner's own links first, then a folder's or onboarding's, then Brain's surest and
		// newest. A topic Brain only asked about is no example, but its thread is not left unfiled either
		Set<String> linked = new HashSet<>();
		CollaborationDbUtils.query("SELECT l.TOPIC_ID, l.SOURCE, t.THREAD_ID, t.SUBJECT FROM BRAIN_THREAD_TOPIC l "
				+ "JOIN BRAIN_THREAD t ON t.OWNER_ID = l.OWNER_ID AND t.OWNER_TYPE = l.OWNER_TYPE "
				+ "AND t.THREAD_ID = l.THREAD_ID WHERE l.OWNER_ID = ? AND l.OWNER_TYPE = ?" + USABLE
				+ " ORDER BY l.TOPIC_ID, CASE WHEN l.SOURCE = ? THEN 0 WHEN l.CLASSIFIER_VERSION IN (?, ?) THEN 1 "
				+ "ELSE 2 END, COALESCE(l.CONFIDENCE, 0) DESC, COALESCE(t.LAST_MESSAGE_AT, t.CREATED_AT) DESC, "
				+ "t.THREAD_ID", rs -> {
					linked.add(rs.getString("THREAD_ID"));
					if (!BrainTopicUtils.SUGGESTED.equals(rs.getString("SOURCE"))
							&& topicIds.contains(rs.getString("TOPIC_ID"))) {
						add(examples, rs.getString("TOPIC_ID"), rs.getString("THREAD_ID"),
								CollaborationDbUtils.getString(rs, "SUBJECT"), rules, keep);
					}
					return null;
				}, ownerId, ownerType, false, false, "teams", chats, BrainProfileUtils.YOU,
				BrainTopicOnboarding.VERSION, BrainMailFolders.FILED);

		// the owner's "not this topic": a thread they then left with no topic is an example of the way out, so mail
		// like it stops landing on the topic. One they filed elsewhere is already that topic's example. Nothing is
		// written into the rejected topic's own text: a model scoring a topic against it would read that as a match
		Set<String> rejected = new HashSet<>();
		CollaborationDbUtils.query("SELECT r.TOPIC_ID, t.THREAD_ID, t.SUBJECT FROM BRAIN_THREAD_TOPIC_REJECTION r "
				+ "JOIN BRAIN_THREAD t ON t.OWNER_ID = r.OWNER_ID AND t.OWNER_TYPE = r.OWNER_TYPE "
				+ "AND t.THREAD_ID = r.THREAD_ID WHERE r.OWNER_ID = ? AND r.OWNER_TYPE = ?" + USABLE
				+ " ORDER BY r.CHANGED_AT DESC, t.THREAD_ID", rs -> {
					rejected.add(rs.getString("TOPIC_ID") + " " + rs.getString("THREAD_ID"));
					if (!linked.contains(rs.getString("THREAD_ID"))) {
						add(examples, BrainThreadClassifier.OTHER_TOPIC, rs.getString("THREAD_ID"),
								CollaborationDbUtils.getString(rs, "SUBJECT"), rules, keep);
					}
					return null;
				}, ownerId, ownerType, false, false, "teams", chats);

		// a topic with few filed threads (every topic, right after onboarding) shows the threads it was set up
		// from: its group in the applied onboarding review, less any the owner took out or that went elsewhere
		// shortcut: a group outlives the owner rewriting its topic; drop a topic's group once the topic's
		// UPDATED_AT is later than the review's
		if (topicIds.stream().anyMatch(id -> examples.getOrDefault(id, List.of()).size() < keep)) {
			Map<String, Object> draft = CollaborationDbUtils.parseMap(CollaborationDbUtils.queryOne(
					"SELECT DRAFT_JSON FROM BRAIN_TOPIC_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
							+ "AND APPLIED_REVISION IS NOT NULL",
					rs -> CollaborationDbUtils.getString(rs, "DRAFT_JSON"), ownerId, ownerType));
			Map<String, List<String>> groups = new LinkedHashMap<>();
			Set<String> wanted = new LinkedHashSet<>();
			for (Map<String, Object> topic : BrainTopicReviewEvidence.maps(draft == null ? null : draft.get("topics"))) {
				if (topic.get("id") instanceof String id && topicIds.contains(id)
						&& examples.getOrDefault(id, List.of()).size() < keep) {
					// a few spare: some have no usable subject or repeat one
					List<String> group = BrainTopicReviewEvidence.strings(topic.get("threadIds")).stream()
							.filter(threadId -> !linked.contains(threadId) && !rejected.contains(id + " " + threadId))
							.limit(3L * keep).toList();
					groups.computeIfAbsent(id, k -> new ArrayList<>()).addAll(group);
					wanted.addAll(group);
				}
			}
			Map<String, String> subjects = new HashMap<>();
			BrainThreadFinder.byIds("SELECT t.THREAD_ID, t.SUBJECT FROM BRAIN_THREAD t WHERE t.OWNER_ID = ? "
					+ "AND t.OWNER_TYPE = ?" + USABLE + " AND t.THREAD_ID IN (", ")", wanted, rs -> {
						subjects.put(rs.getString("THREAD_ID"), CollaborationDbUtils.getString(rs, "SUBJECT"));
						return null;
					}, ownerId, ownerType, false, false, "teams", chats);
			groups.forEach((id, group) -> {
				for (String threadId : group) {
					add(examples, id, threadId, subjects.get(threadId), rules, keep);
				}
			});
		}
		return new Evidence(examples, each);
	}

	/**
	 * The topics with their example subjects added, for scoring one thread (threadId null for a chat). A thread is
	 * never its own example: it would confirm the topic it is already in, the first filing after onboarding would
	 * file every thread by its own subject, and a re-sort could never find a thread that left its topic.
	 */
	static List<BrainClassifier.TopicOption> describe(List<BrainClassifier.TopicOption> topics, Evidence evidence,
			String threadId) {
		if (evidence == null || evidence.examples().isEmpty()) {
			return topics;
		}
		List<BrainClassifier.TopicOption> described = new ArrayList<>();
		// the way out comes last (topicOptions), so it lists no subject a topic already shows
		Set<String> shown = new HashSet<>();
		for (BrainClassifier.TopicOption topic : topics) {
			boolean wayOut = BrainThreadClassifier.OTHER_TOPIC.equals(topic.id());
			String text = topic.description() == null ? "" : topic.description();
			String lower = text.toLowerCase(Locale.ROOT);
			List<String> subjects = new ArrayList<>();
			for (Example example : evidence.examples().getOrDefault(topic.id(), List.of())) {
				if (subjects.size() >= evidence.each()) {
					break;
				}
				String key = example.subject().toLowerCase(Locale.ROOT);
				// its own subject, one the description already names, or one a topic claims
				if ((threadId != null && threadId.equals(example.threadId())) || lower.contains(key)
						|| (wayOut && shown.contains(key))) {
					continue;
				}
				subjects.add(example.subject());
				if (!wayOut) {
					shown.add(key);
				}
			}
			described.add(subjects.isEmpty() ? topic
					: new BrainClassifier.TopicOption(topic.id(), topic.name(),
							(text + LABEL + String.join("; ", subjects) + ".").trim()));
		}
		return described;
	}

	// adds a subject to a topic's list unless the list is full, the subject is blank or a never-ingest keyword
	// rule covers it; a second thread with a listed subject makes that subject no single thread's own
	private static void add(Map<String, List<Example>> examples, String topicId, String threadId, String subject,
			List<BrainRulesGate.Rule> rules, int keep) {
		String text = subject == null ? "" : subject.strip();
		if (text.length() > SUBJECT_CHARS) {
			text = text.substring(0, SUBJECT_CHARS).strip();
		}
		if (text.isEmpty()) {
			return;
		}
		List<Example> list = examples.computeIfAbsent(topicId, k -> new ArrayList<>());
		for (int i = 0; i < list.size(); i++) {
			Example example = list.get(i);
			if (example.subject().equalsIgnoreCase(text)) {
				if (example.threadId() != null && !example.threadId().equals(threadId)) {
					list.set(i, new Example(null, example.subject()));
				}
				return;
			}
		}
		if (list.size() < keep && BrainRulesGate.keywordRule(rules, subject, null) == null) {
			list.add(new Example(threadId, text));
		}
	}
}
