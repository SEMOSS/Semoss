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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ITypeSafeEngine;
import prerna.om.Insight;
import prerna.util.Utility;

// Classifier v0 (BRAIN-04 / WORK-02 stand-in): files threads under topics and makes work items with a Jev
// (TypeSafe) engine. Questions and thresholds were tuned on brain-mail-v1 (tools/classifier_eval.py).
public final class BrainThreadClassifier {

	private static final Logger classLogger = LogManager.getLogger(BrainThreadClassifier.class);

	public static final String VERSION = "jev-v0";
	// one TypeSafe call per thread; a few in parallel keeps a 100-thread run near half a minute
	private static final int PARALLEL = 4;
	private static final int TEXT_CHARS = 1500;
	// fyi noul: at or above FYI_AT is FYI, below ASKS_AT asks the owner, in between the owner decides
	private static final double FYI_AT = 0.6;
	private static final double ASKS_AT = 0.4;
	private static final double AUTOMATED_AT = 0.8;
	private static final String[] URGENCY = { "Whenever", "This week", "Today", "Right now" };

	private BrainThreadClassifier() {
	}

	/** What one thread came out as; the caller sees these in the run summary. */
	public record Result(String threadId, String topicId, Integer confidence, String band, String work,
			String priority, String error) {
	}

	// classifies the given threads, or every unmuted thread with no owner-made topic link and no work item
	public static Map<String, Object> classify(User user, Insight insight, List<String> threadIds, String engineId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Map<String, Object> settings = BrainProfileUtils.getSettings(ownerId, ownerType);
		String engine = engineId != null ? engineId : (String) settings.get("classifierEngineId");
		if (engine == null) {
			throw new IllegalArgumentException("Set a classifier engine in Brain settings first");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, engine)) {
			throw new IllegalArgumentException("Model " + engine + " does not exist or you do not have access to it");
		}
		IModelEngine model = Utility.getModel(engine);
		if (!(model instanceof ITypeSafeEngine jev)) {
			throw new IllegalArgumentException("The classifier engine must be a TYPESAFE (Jev) model");
		}
		int fileAt = (Integer) settings.get("fileAt");
		int askAt = (Integer) settings.get("askAt");
		List<Map<String, Object>> topics = topics(ownerId, ownerType);
		List<String> ids = threadIds == null || threadIds.isEmpty() ? pending(ownerId, ownerType) : threadIds;
		String selfId = CollaborationDbUtils.queryOne("SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND RELATIONSHIP = ?", rs -> rs.getString(1), ownerId, ownerType, "self");

		List<Result> results = new ArrayList<>();
		ExecutorService pool = Executors.newFixedThreadPool(PARALLEL);
		try {
			List<Future<Result>> futures = new ArrayList<>();
			for (String threadId : ids) {
				futures.add(pool.submit(() -> classifyOne(user, insight, ownerId, ownerType, jev, topics, selfId,
						threadId, fileAt, askAt)));
			}
			for (int i = 0; i < futures.size(); i++) {
				try {
					results.add(futures.get(i).get());
				} catch (Exception e) {
					classLogger.warn("Classifier failed on thread {}", ids.get(i), e);
					results.add(new Result(ids.get(i), null, null, null, null, null, rootMessage(e)));
				}
			}
		} finally {
			pool.shutdown();
		}
		return summary(engine, results);
	}

	private static Result classifyOne(User user, Insight insight, String ownerId, String ownerType, ITypeSafeEngine jev,
			List<Map<String, Object>> topics, String selfId, String threadId, int fileAt, int askAt) {
		Map<String, Object> read = BrainThreadMessages.read(user, ownerId, ownerType, threadId, 5,
				BrainMessageSource.current());
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> messages = (List<Map<String, Object>>) read.get("messages");
		if (Boolean.TRUE.equals(read.get("muted")) || messages == null || messages.isEmpty()) {
			return new Result(threadId, null, null, null, "skipped", null, null);
		}
		Map<String, Object> newest = messages.get(messages.size() - 1);
		boolean fromMe = selfId != null && selfId.equals(newest.get("fromId"));
		Map<String, Object> thread = CollaborationDbUtils.queryOne("SELECT SUBJECT, SOURCE FROM BRAIN_THREAD "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?", rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("subject", CollaborationDbUtils.getString(rs, "SUBJECT"));
					row.put("source", CollaborationDbUtils.getString(rs, "SOURCE"));
					return row;
				}, ownerId, ownerType, threadId);

		Map<String, Object> state = new LinkedHashMap<>();
		state.put("subject", thread.get("subject"));
		state.put("participants", participants(ownerId, ownerType, threadId, selfId));
		state.put("messageCount", messages.size());
		Map<String, Object> message = new LinkedHashMap<>();
		message.put("from", fromMe ? "me" : newest.get("fromName"));
		String text = newest.get("text") == null ? "" : String.valueOf(newest.get("text"));
		message.put("text", text.length() > TEXT_CHARS ? text.substring(0, TEXT_CHARS) : text);
		state.put("newestMessage", message);

		Map<String, Object> questions = new LinkedHashMap<>();
		// no "none of these" choice: it drew most answers in the eval; a small margin goes to review instead
		if (topics.size() > 1) {
			Map<String, Object> criteria = new LinkedHashMap<>();
			for (Map<String, Object> topic : topics) {
				criteria.put((String) topic.get("name"), topic.get("describe"));
			}
			questions.put("topic", question("choice", "Which of these work topics is this email thread about?", criteria));
		}
		questions.put("fyi", question("noul", "Is this message only informing (an update, heads-up, approval, or "
				+ "sign-off) with nothing for anyone to do?", null));
		questions.put("automated", question("noul", "Is this an automated or bulk message (newsletter, notification, "
				+ "no-reply, alert) rather than a person writing?", null));
		questions.put("urgency", question("score", "How urgently does this need a response?", List.of(URGENCY)));
		@SuppressWarnings("unchecked")
		Map<String, Object> answers = (Map<String, Object>) jev.evaluate(state, questions, insight, null).getResponse()
				.get("answers");

		// topic: file, ask, or leave; owner-made links are never touched
		String topicId = null;
		Integer confidence = null;
		String band = null;
		Map<String, Object> topic = answer(answers, "topic");
		if (topic != null && !hasOwnerLink(ownerId, ownerType, threadId)) {
			List<Map.Entry<String, Double>> ranked = ranked(topic);
			if (!ranked.isEmpty()) {
				double margin = ranked.get(0).getValue() - (ranked.size() > 1 ? ranked.get(1).getValue() : 0);
				confidence = (int) Math.min(100, Math.round(50 + 400 * margin));
				String bestId = topicIdByName(topics, ranked.get(0).getKey());
				if (confidence >= fileAt) {
					band = "filed";
					topicId = bestId;
					link(ownerId, ownerType, threadId, bestId, "confirmed", confidence, topic);
				} else if (confidence >= askAt && ranked.size() > 1) {
					band = "asked";
					link(ownerId, ownerType, threadId, bestId, "suggested", confidence, topic);
					BrainReviewUtils.addReview(ownerId, ownerType, BrainReviewUtils.TOPIC_CHOICE, "thread", threadId,
							VERSION, Map.of("candidates", List.of(bestId,
									topicIdByName(topics, ranked.get(1).getKey()))));
				} else {
					band = "unassigned";
				}
			}
		}

		// work: the newest message decides; one item per newest message
		double fyi = noul(answers, "fyi");
		double automated = noul(answers, "automated");
		Map<String, Object> urgencyAnswer = answer(answers, "urgency");
		double urgency = urgencyAnswer != null && urgencyAnswer.get("score") instanceof Number n ? n.doubleValue() : 0;
		String work;
		String askType;
		boolean suggested = false;
		List<String> reasons = new ArrayList<>();
		if (automated >= AUTOMATED_AT) {
			CollaborationDbUtils.update("UPDATE BRAIN_THREAD SET AUTOMATED = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND THREAD_ID = ?", true, ownerId, ownerType, threadId);
			return new Result(threadId, topicId, confidence, band, "automated", null, null);
		} else if (fromMe) {
			work = "waiting";
			askType = "waiting_on";
			reasons.add("You wrote last; waiting on a reply");
		} else if (fyi >= FYI_AT) {
			work = "fyi";
			askType = "fyi";
			reasons.add("Looks like an update with nothing to do");
		} else {
			work = fyi < ASKS_AT ? "needs_me" : "suggested";
			askType = "reply";
			suggested = fyi >= ASKS_AT;
			reasons.add(suggested ? "Might need you; confirm" : "Asks you to act");
		}
		String priority = urgency >= 2.5 ? "P0" : urgency >= 1.8 ? "P1" : urgency >= 1.0 ? "P2" : "P3";
		reasons.add("Urgency: " + URGENCY[(int) Math.max(0, Math.min(3, Math.round(urgency)))]);
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("dedupeKey", VERSION + ":" + threadId + ":" + newest.get("id"));
		item.put("threadId", threadId);
		item.put("channel", "teams".equals(thread.get("source")) ? "teams" : "email");
		item.put("title", thread.get("subject") == null ? "(no subject)" : thread.get("subject"));
		item.put("askType", askType);
		item.put("receivedAt", newest.get("at"));
		item.put("sourceRef", newest.get("id"));
		if (newest.get("fromId") != null) {
			item.put("actorType", "person");
			item.put("actorId", newest.get("fromId"));
		}
		item.put("actorName", newest.get("fromName"));
		item.put("status", "waiting".equals(work) ? "waiting" : "open");
		item.put("suggested", suggested);
		item.put("priority", priority);
		item.put("score", (int) Math.round(urgency / 3 * 100));
		item.put("reasons", reasons);
		item.put("linkTopicId", topicId);
		item.put("classifierVersion", VERSION);
		item.put("reason", "classifier");
		WorkItemUtils.createFromIngest(ownerId, ownerType, item);
		return new Result(threadId, topicId, confidence, band, work, priority, null);
	}

	// active and dormant topics with a description the model can match against
	private static List<Map<String, Object>> topics(String ownerId, String ownerType) {
		return CollaborationDbUtils.query("SELECT t.TOPIC_ID, t.NAME, t.DESCRIPTION, t.KEYWORDS_JSON, a.NAME AS ACCOUNT "
				+ "FROM BRAIN_TOPIC t LEFT JOIN BRAIN_ACCOUNT a ON a.OWNER_ID = t.OWNER_ID AND a.OWNER_TYPE = t.OWNER_TYPE "
				+ "AND a.ACCOUNT_ID = t.ACCOUNT_ID WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ? AND t.STATUS IN (?, ?) "
				+ "ORDER BY t.TOPIC_ID", rs -> {
					Map<String, Object> topic = new LinkedHashMap<>();
					topic.put("id", CollaborationDbUtils.getString(rs, "TOPIC_ID"));
					topic.put("name", CollaborationDbUtils.getString(rs, "NAME"));
					StringBuilder describe = new StringBuilder();
					String description = CollaborationDbUtils.getString(rs, "DESCRIPTION");
					if (description != null) {
						describe.append(description);
					}
					String account = CollaborationDbUtils.getString(rs, "ACCOUNT");
					if (account != null) {
						describe.append(" Account: ").append(account).append('.');
					}
					List<Object> keywords = CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "KEYWORDS_JSON"));
					if (!keywords.isEmpty()) {
						describe.append(" Keywords: ").append(String.join(", ", CollaborationDbUtils.toStringList(keywords)))
								.append('.');
					}
					topic.put("describe", describe.toString().trim());
					return topic;
				}, ownerId, ownerType, BrainTopicUtils.ACTIVE, BrainTopicUtils.DORMANT);
	}

	private static List<String> pending(String ownerId, String ownerType) {
		return CollaborationDbUtils.query("SELECT t.THREAD_ID FROM BRAIN_THREAD t WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ? "
				+ "AND (t.MUTED IS NULL OR t.MUTED = ?) AND NOT EXISTS (SELECT 1 FROM WORK_ITEM w WHERE w.OWNER_ID = t.OWNER_ID "
				+ "AND w.OWNER_TYPE = t.OWNER_TYPE AND w.THREAD_ID = t.THREAD_ID) ORDER BY t.LAST_MESSAGE_AT DESC",
				rs -> rs.getString(1), ownerId, ownerType, false);
	}

	private static List<String> participants(String ownerId, String ownerType, String threadId, String selfId) {
		return CollaborationDbUtils.query("SELECT p.PERSON_ID, p.DISPLAY_NAME, p.JOB_TITLE, p.COMPANY FROM "
				+ "BRAIN_THREAD_PARTICIPANT tp JOIN BRAIN_PERSON p ON p.OWNER_ID = tp.OWNER_ID AND p.OWNER_TYPE = tp.OWNER_TYPE "
				+ "AND p.PERSON_ID = tp.PERSON_ID WHERE tp.OWNER_ID = ? AND tp.OWNER_TYPE = ? AND tp.THREAD_ID = ? "
				+ "ORDER BY p.DISPLAY_NAME", rs -> {
					if (rs.getString("PERSON_ID").equals(selfId)) {
						return null;
					}
					String name = CollaborationDbUtils.getString(rs, "DISPLAY_NAME");
					List<String> extra = new ArrayList<>();
					for (String column : new String[] { "JOB_TITLE", "COMPANY" }) {
						String value = CollaborationDbUtils.getString(rs, column);
						if (value != null && !value.isBlank()) {
							extra.add(value);
						}
					}
					return extra.isEmpty() ? name : name + " (" + String.join(", ", extra) + ")";
				}, ownerId, ownerType, threadId).stream().filter(Objects::nonNull).toList();
	}

	private static boolean hasOwnerLink(String ownerId, String ownerType, String threadId) {
		return CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND THREAD_ID = ? AND SOURCE = ?", ownerId, ownerType, threadId, BrainProfileUtils.YOU);
	}

	// a classifier link replaces an earlier classifier link on the same topic; the first link is the primary
	private static void link(String ownerId, String ownerType, String threadId, String topicId, String source,
			int confidence, Map<String, Object> answer) {
		Timestamp now = CollaborationDbUtils.now();
		boolean hasPrimary = CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND THREAD_ID = ? AND IS_PRIMARY = ? AND TOPIC_ID <> ?", ownerId, ownerType, threadId,
				true, topicId);
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND THREAD_ID = ? AND TOPIC_ID = ?", ownerId, ownerType, threadId, topicId);
			CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, "
					+ "SOURCE, CONFIDENCE, IS_PRIMARY, SIGNALS_JSON, CLASSIFIER_VERSION, CHANGED_BY, CHANGED_AT) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, threadId, topicId, source, confidence,
					!hasPrimary, CollaborationDbUtils.toJson(answer.get("probabilities")), VERSION, "brain", now);
		});
	}

	private static Map<String, Object> question(String type, String instructions, Object criteria) {
		Map<String, Object> q = new LinkedHashMap<>();
		q.put("type", type);
		q.put("instructions", instructions);
		if (criteria != null) {
			q.put("criteria", criteria);
		}
		return q;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> answer(Map<String, Object> answers, String key) {
		return answers != null && answers.get(key) instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
	}

	private static double noul(Map<String, Object> answers, String key) {
		Map<String, Object> a = answer(answers, key);
		return a != null && a.get("noul") instanceof Number n ? n.doubleValue() : 0;
	}

	@SuppressWarnings("unchecked")
	private static List<Map.Entry<String, Double>> ranked(Map<String, Object> topic) {
		List<Map.Entry<String, Double>> ranked = new ArrayList<>();
		if (topic.get("probabilities") instanceof Map<?, ?> probs) {
			for (Map.Entry<String, Object> e : ((Map<String, Object>) probs).entrySet()) {
				if (e.getValue() instanceof Number n) {
					ranked.add(Map.entry(e.getKey(), n.doubleValue()));
				}
			}
		}
		ranked.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
		return ranked;
	}

	private static String topicIdByName(List<Map<String, Object>> topics, String name) {
		for (Map<String, Object> topic : topics) {
			if (topic.get("name").equals(name)) {
				return (String) topic.get("id");
			}
		}
		throw new IllegalStateException("Classifier answered an unknown topic: " + name);
	}

	private static Map<String, Object> summary(String engine, List<Result> results) {
		Map<String, Integer> bands = new LinkedHashMap<>();
		Map<String, Integer> work = new LinkedHashMap<>();
		int errors = 0;
		List<Map<String, Object>> rows = new ArrayList<>();
		for (Result r : results) {
			if (r.error() != null) {
				errors++;
			}
			if (r.band() != null) {
				bands.merge(r.band(), 1, Integer::sum);
			}
			if (r.work() != null) {
				work.merge(r.work(), 1, Integer::sum);
			}
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("threadId", r.threadId());
			row.put("topicId", r.topicId());
			row.put("confidence", r.confidence());
			row.put("band", r.band());
			row.put("work", r.work());
			row.put("priority", r.priority());
			row.put("error", r.error());
			rows.add(row);
		}
		Map<String, Object> summary = new LinkedHashMap<>();
		summary.put("engine", engine);
		summary.put("version", VERSION);
		summary.put("threads", results.size());
		summary.put("topics", bands);
		summary.put("work", work);
		summary.put("errors", errors);
		summary.put("results", rows);
		return summary;
	}

	private static String rootMessage(Throwable e) {
		Throwable t = e;
		while (t.getCause() != null) {
			t = t.getCause();
		}
		return t.getMessage();
	}
}
