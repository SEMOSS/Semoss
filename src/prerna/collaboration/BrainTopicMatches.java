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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.om.Insight;

// Re-sorting existing conversations after the owner adds or edits a topic, only when the owner asks. A preview job
// scores the newest eligible conversations with a classifier dry run (nothing written) and lists the ones that fit
// the topic; the owner then applies move, also_link or reject per conversation. Links the owner, onboarding or an
// Outlook folder made are never removed here, chats keep their threads and topics, and Work keeps its state.
public final class BrainTopicMatches {

	public static final String KIND = "topic_match";
	// the topic replaces the topics Brain guessed for the conversation
	public static final String MOVE = "move";
	// the topic is linked next to whatever the conversation has
	public static final String ALSO_LINK = "also_link";
	// not about the topic: remembered, so neither this preview nor the classifier suggests it again
	public static final String REJECT = "reject";
	static final int DEFAULT_LIMIT = 200;
	static final int MAX_LIMIT = 1000;
	// the topic is the clear best fit, or fits next to a topic the conversation already has
	static final String BEST = "best";
	static final String SHARED = "shared";

	private BrainTopicMatches() {
	}

	/** A topic link on a conversation; a kept one is the owner's, onboarding's or an Outlook folder's. */
	record Link(String topicId, String source, boolean primary, boolean kept) {
	}

	// ---- preview ----

	// starts the preview job, or returns the one already running for this topic; poll with BrainGetJob(jobId)
	public static Map<String, Object> start(User user, String topicId, Integer limit) {
		BrainThreadClassifier.requireEngine(user);
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		requireMatchable(ownerId, ownerType, topicId);
		int scan = limit == null ? DEFAULT_LIMIT : limit;
		if (scan < 1 || scan > MAX_LIMIT) {
			throw new IllegalArgumentException("limit must be between 1 and " + MAX_LIMIT);
		}
		Map<String, Object> running = CollaborationJobUtils.latest(ownerId, ownerType, KIND);
		if (running != null && CollaborationJobUtils.RUNNING.equals(running.get("status"))
				&& running.get("params") instanceof Map<?, ?> params && !topicId.equals(params.get("topicId"))) {
			throw new IllegalArgumentException("A preview for another topic is still running; try again when it finishes");
		}
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("topicId", topicId);
		params.put("limit", scan);
		return CollaborationJobUtils.start(ownerId, ownerType, KIND, params, job -> {
			// own insight so the run does not depend on the page that started it
			Insight insight = new Insight();
			insight.setUser(user);
			job.step("finding", 1);
			List<String> candidates = candidates(ownerId, ownerType, topicId, scan);
			job.count("scanned", candidates.size());
			List<Map<String, Object>> matches = List.of();
			// an empty list would make the classifier score every thread
			if (!candidates.isEmpty()) {
				Map<String, Object> summary = BrainThreadClassifier.classify(user, insight, candidates, true,
						(done, total) -> {
							job.count("done", done);
							job.count("total", total);
							job.step("scoring", total == 0 ? 99 : Math.max(1, 99 * done / total));
						});
				job.count("errors", summary.get("errors"));
				job.count("errorReason", summary.get("errorReason"));
				matches = matches(ownerId, ownerType, topicId, summary);
			}
			job.count("matches", matches);
		});
	}

	// unmuted, not automated, not on the topic and not turned down for it; newest first
	static List<String> candidates(String ownerId, String ownerType, String topicId, int limit) {
		return CollaborationDbUtils.query(CollaborationDbUtils.page("SELECT t.THREAD_ID FROM BRAIN_THREAD t "
				+ "WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ? AND (t.MUTED IS NULL OR t.MUTED = ?) "
				+ "AND (t.AUTOMATED IS NULL OR t.AUTOMATED = ?) AND NOT EXISTS (SELECT 1 FROM BRAIN_THREAD_TOPIC l "
				+ "WHERE l.OWNER_ID = t.OWNER_ID AND l.OWNER_TYPE = t.OWNER_TYPE AND l.THREAD_ID = t.THREAD_ID "
				+ "AND l.TOPIC_ID = ?) AND NOT EXISTS (SELECT 1 FROM BRAIN_THREAD_TOPIC_REJECTION r "
				+ "WHERE r.OWNER_ID = t.OWNER_ID AND r.OWNER_TYPE = t.OWNER_TYPE AND r.THREAD_ID = t.THREAD_ID "
				+ "AND r.TOPIC_ID = ?) ORDER BY t.LAST_MESSAGE_AT DESC, t.THREAD_ID", limit, 0), rs -> rs.getString(1),
				ownerId, ownerType, false, false, topicId, topicId);
	}

	// the dry run's results as preview rows, best fits first; failed, skipped and automated threads drop out
	@SuppressWarnings("unchecked")
	static List<Map<String, Object>> matches(String ownerId, String ownerType, String topicId,
			Map<String, Object> summary) {
		Map<String, Object> settings = BrainProfileUtils.getSettings(ownerId, ownerType);
		int fileAt = (Integer) settings.get("fileAt");
		int askAt = (Integer) settings.get("askAt");
		List<Map<String, Object>> rows = new ArrayList<>();
		for (Map<String, Object> result : (List<Map<String, Object>>) summary.get("results")) {
			if (result.get("error") != null || "automated".equals(result.get("work"))
					|| !(result.get("scores") instanceof Map<?, ?> scores)
					|| !(scores.get("topics") instanceof Map<?, ?> topics)) {
				continue;
			}
			String threadId = (String) result.get("threadId");
			Map<String, Double> probabilities = new LinkedHashMap<>();
			topics.forEach((id, p) -> {
				if (p instanceof Number n) {
					probabilities.put(String.valueOf(id), n.doubleValue());
				}
			});
			Map<String, Object> row = match(topicId, probabilities,
					BrainThreadTopicDecisions.rejected(ownerId, ownerType, threadId), links(ownerId, ownerType, threadId),
					fileAt, askAt);
			if (row == null) {
				continue;
			}
			Map<String, Object> described = CollaborationDbUtils.queryOne("SELECT SUBJECT, SOURCE, LAST_MESSAGE_AT "
					+ "FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?", rs -> {
						Map<String, Object> thread = new LinkedHashMap<>();
						thread.put("threadId", threadId);
						thread.put("subject", CollaborationDbUtils.getString(rs, "SUBJECT"));
						thread.put("channel", CollaborationDbUtils.getString(rs, "SOURCE"));
						thread.put("lastMessageAt", CollaborationDbUtils.getTimestamp(rs, "LAST_MESSAGE_AT"));
						return thread;
					}, ownerId, ownerType, threadId);
			if (described != null) {
				described.putAll(row);
				rows.add(described);
			}
		}
		rows.sort(Comparator.comparingInt((Map<String, Object> row) -> (Integer) row.get("confidence")).reversed());
		return rows;
	}

	// The preview row for one conversation, or null when the topic does not fit it: the topic is the best of the
	// topics the conversation may have with at least the classifier's ask band, or one of the top two next to a topic
	// it already has with at least askAt. The scores are a dry run over all the owner's topics and the way out, so
	// confidence reads as the classifier's own. Move is offered only when every current link is a Brain guess.
	static Map<String, Object> match(String topicId, Map<String, Double> topics, Set<String> rejected,
			List<Link> links, int fileAt, int askAt) {
		Double fit = topics.get(topicId);
		// turned down or filed under the topic while the preview ran
		if (fit == null || rejected.contains(topicId)
				|| links.stream().anyMatch(link -> link.topicId().equals(topicId))) {
			return null;
		}
		List<Map.Entry<String, Double>> ranked = new ArrayList<>(
				topics.entrySet().stream().filter(entry -> !rejected.contains(entry.getKey())).toList());
		ranked.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
		int confidence = (int) Math.round(100 * fit);
		boolean best = ranked.get(0).getKey().equals(topicId);
		boolean second = ranked.size() > 1 && ranked.get(1).getKey().equals(topicId);
		String rival = best ? (ranked.size() > 1 ? ranked.get(1).getKey() : null) : ranked.get(0).getKey();
		boolean strong = best
				&& confidence >= Math.max(askAt, fileAt - BrainThreadClassifier.WAY_OUT_ASK_BAND);
		boolean shared = !strong && (best || second) && confidence >= askAt
				&& links.stream().anyMatch(link -> link.topicId().equals(rival));
		if (!strong && !shared) {
			return null;
		}
		boolean guessesOnly = !links.isEmpty() && links.stream().noneMatch(Link::kept);
		List<Map<String, Object>> current = new ArrayList<>();
		for (Link link : links) {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("topicId", link.topicId());
			row.put("source", link.source());
			row.put("primary", link.primary());
			row.put("kept", link.kept());
			current.add(row);
		}
		Map<String, Object> row = new LinkedHashMap<>();
		row.put("confidence", confidence);
		row.put("fit", strong ? BEST : SHARED);
		row.put("links", current);
		row.put("ops", guessesOnly ? List.of(MOVE, ALSO_LINK) : List.of(ALSO_LINK));
		row.put("op", guessesOnly && strong ? MOVE : ALSO_LINK);
		return row;
	}

	// ---- apply ----

	// each change is {threadId, op} with op move, also_link or reject; every one is checked against the
	// conversation as it is now, so a stale preview cannot undo what changed since. Each runs on its own, and one
	// that no longer applies comes back skipped with the reason.
	public static Map<String, Object> apply(User user, String topicId, List<Map<String, Object>> changes) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		requireMatchable(ownerId, ownerType, topicId);
		if (changes == null || changes.isEmpty()) {
			throw new IllegalArgumentException("Pass changes as a list of {threadId, op}");
		}
		if (changes.size() > MAX_LIMIT) {
			throw new IllegalArgumentException("At most " + MAX_LIMIT + " changes at once");
		}
		for (Map<String, Object> change : changes) {
			String threadId = CollaborationDbUtils.asString(change.get("threadId"));
			if (threadId == null || threadId.isBlank()) {
				throw new IllegalArgumentException("Every change needs a threadId");
			}
			if (!Set.of(MOVE, ALSO_LINK, REJECT).contains(CollaborationDbUtils.asString(change.get("op")))) {
				throw new IllegalArgumentException("op must be " + MOVE + ", " + ALSO_LINK + " or " + REJECT);
			}
		}
		List<Map<String, Object>> results = new ArrayList<>();
		int applied = 0;
		for (Map<String, Object> change : changes) {
			String threadId = CollaborationDbUtils.asString(change.get("threadId"));
			String op = CollaborationDbUtils.asString(change.get("op"));
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("threadId", threadId);
			row.put("op", op);
			String skipped;
			try {
				skipped = switch (op) {
				case MOVE -> move(ownerId, ownerType, threadId, topicId);
				case ALSO_LINK -> {
					BrainThreadUtils.linkThreadTopic(user, threadId, topicId, false, false);
					yield null;
				}
				default -> reject(user, ownerId, ownerType, threadId, topicId);
				};
			} catch (IllegalArgumentException e) {
				// the conversation or topic went away meanwhile
				skipped = e.getMessage();
			}
			row.put("status", skipped == null ? "applied" : "skipped");
			if (skipped == null) {
				applied++;
				row.put("links", BrainThreadUtils.getLinks(ownerId, ownerType, threadId));
			} else {
				row.put("reason", skipped);
			}
			results.add(row);
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("topicId", topicId);
		result.put("applied", applied);
		result.put("skipped", results.size() - applied);
		result.put("results", results);
		return result;
	}

	// The topic becomes the conversation's one topic in place of the ones Brain guessed; refused while another
	// link is kept. The topics it leaves are remembered as not this conversation's, as unlinking by hand does.
	// Brain's cards on it follow unless the owner set a card's topic; status, snooze, owner and chat stay.
	private static String move(String ownerId, String ownerType, String threadId, String topicId) {
		Timestamp now = CollaborationDbUtils.now();
		String[] skipped = { null };
		CollaborationDbUtils.batch(conn -> {
			BrainTopicReviewProfiles.lock(ownerId, ownerType);
			requireMatchable(ownerId, ownerType, topicId);
			BrainThreadTopicDecisions.lockThread(ownerId, ownerType, threadId);
			List<String> from = new ArrayList<>();
			boolean linked = false;
			for (Link link : links(ownerId, ownerType, threadId)) {
				if (link.topicId().equals(topicId)) {
					linked = true;
				} else if (link.kept()) {
					skipped[0] = "It has a topic you filed yourself; use " + ALSO_LINK + " instead";
					return;
				} else {
					from.add(link.topicId());
				}
			}
			String owned = " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?";
			for (String old : from) {
				CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_THREAD_TOPIC" + owned + " AND TOPIC_ID = ?",
						ownerId, ownerType, threadId, old);
				BrainThreadTopicDecisions.remember(ownerId, ownerType, threadId, old, true);
			}
			BrainThreadTopicDecisions.remember(ownerId, ownerType, threadId, topicId, false);
			if (linked) {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD_TOPIC SET SOURCE = ?, CONFIDENCE = ?, "
						+ "IS_PRIMARY = ?, CHANGED_BY = ?, CHANGED_AT = ?" + owned + " AND TOPIC_ID = ?",
						BrainProfileUtils.YOU, 100, true, BrainProfileUtils.YOU, now, ownerId, ownerType, threadId,
						topicId);
			} else {
				CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, "
						+ "TOPIC_ID, SOURCE, CONFIDENCE, IS_PRIMARY, CHANGED_BY, CHANGED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
						ownerId, ownerType, threadId, topicId, BrainProfileUtils.YOU, 100, true, BrainProfileUtils.YOU,
						now);
			}
			List<Object> params = new ArrayList<>(List.of(topicId, ownerId, ownerType, threadId, WorkItemUtils.BRAIN));
			params.addAll(from);
			params.addAll(List.of(ownerId, ownerType, "linkTopicId", WorkItemUtils.YOU));
			CollaborationDbUtils.update(conn, "UPDATE WORK_ITEM SET LINK_TOPIC_ID = ?" + owned + " AND ORIGIN = ? "
					+ "AND (LINK_TOPIC_ID IS NULL" + (from.isEmpty() ? ""
							: " OR LINK_TOPIC_ID IN (" + CollaborationDbUtils.placeholders(from.size()) + ")")
					+ ") AND ITEM_ID NOT IN (SELECT ITEM_ID FROM WORK_ITEM_HISTORY WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND FIELD = ? AND CHANGED_BY = ?)", params.toArray());
		});
		if (skipped[0] == null) {
			// a question about which topic it is no longer applies
			BrainReviewUtils.dismissForRef(ownerId, ownerType, "thread", threadId);
		}
		return skipped[0];
	}

	// remembered as not about the topic; a link Brain guessed to it goes, one the owner kept stays
	private static String reject(User user, String ownerId, String ownerType, String threadId, String topicId) {
		String[] skipped = { null };
		CollaborationDbUtils.batch(conn -> {
			BrainTopicReviewProfiles.lock(ownerId, ownerType);
			BrainThreadTopicDecisions.lockThread(ownerId, ownerType, threadId);
			if (links(ownerId, ownerType, threadId).stream()
					.anyMatch(link -> link.topicId().equals(topicId) && link.kept())) {
				skipped[0] = "You filed it under this topic; unlink it on the conversation instead";
				return;
			}
			// joins this transaction
			BrainThreadUtils.linkThreadTopic(user, threadId, topicId, false, true);
		});
		return skipped[0];
	}

	// ---- helpers ----

	static List<Link> links(String ownerId, String ownerType, String threadId) {
		return CollaborationDbUtils.query("SELECT TOPIC_ID, SOURCE, IS_PRIMARY, CLASSIFIER_VERSION "
				+ "FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? "
				+ "ORDER BY IS_PRIMARY DESC, TOPIC_ID", rs -> {
					String source = CollaborationDbUtils.getString(rs, "SOURCE");
					String version = CollaborationDbUtils.getString(rs, "CLASSIFIER_VERSION");
					// the links BrainThreadClassifier.hasKeptLink never refiles
					boolean kept = BrainProfileUtils.YOU.equals(source) || BrainTopicOnboarding.VERSION.equals(version)
							|| BrainMailFolders.FILED.equals(version);
					return new Link(rs.getString("TOPIC_ID"), source,
							Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "IS_PRIMARY")), kept);
				}, ownerId, ownerType, threadId);
	}

	// the classifier scores active and dormant topics only
	private static void requireMatchable(String ownerId, String ownerType, String topicId) {
		if (topicId == null || topicId.isBlank()) {
			throw new IllegalArgumentException("topicId is required");
		}
		String status = BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
		if (!BrainTopicUtils.ACTIVE.equals(status) && !BrainTopicUtils.DORMANT.equals(status)) {
			throw new IllegalArgumentException("Only an active or dormant topic can be matched");
		}
	}
}
