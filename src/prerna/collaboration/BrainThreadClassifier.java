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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

// Sorts permitted thread content into Work state. Onboarding discovers topics afterward, then startTopics files
// against the accepted profiles without changing Work state. Thresholds were tuned on the fixture mailbox.
public final class BrainThreadClassifier {

	private static final Logger classLogger = LogManager.getLogger(BrainThreadClassifier.class);

	// threads classified at once; RDF_Map COLLAB_CLASSIFY_PARALLEL raises it for a model that keeps up
	private static final int PARALLEL = 8;
	private static final int MAX_PARALLEL = 32;
	private static final int MESSAGES = 2;
	private static final int TEXT_CHARS = 1500;
	// a forward, or mail from before the thread: the part that matters is under the
	// note
	private static final int HISTORY_CHARS = 5000;
	// an engine with a window set (COLLAB_CLASSIFIER_WINDOW {engineId: contextTokens}) reads more: up to
	// WIDE_MESSAGES messages, each as long as the cleaner keeps, with its footer, in half the window
	private static final int WIDE_MESSAGES = 10;
	private static final int WIDE_TEXT_CHARS = 12000;
	private static final int WIDE_FOOTER_CHARS = 2000;
	private static final int CHARS_PER_TOKEN = 3;
	private static final int MIN_WINDOW_TOKENS = 4000;
	private static final int KEY_PEOPLE = 8;
	private static final String[] URGENCY = { "Whenever", "This week", "Today", "Right now" };
	// the model always gets a way out, or every thread lands in one of the topics
	static final String OTHER_TOPIC = "other";
	// urgency is about the newest message now: older mail caps at Today, then This week
	private static final int TODAY_DAYS = 1;
	private static final int WEEK_DAYS = 7;
	private static final int WAY_OUT_ASK_BAND = 15;

	private BrainThreadClassifier() {
	}

	/** One thread's outcome; dry runs return scores and write nothing. */
	public record Result(String threadId, String topicId, Integer confidence, String band, String work, String priority,
			Map<String, Object> scores, String error) {
	}

	public static final String JOB_KIND = "classify";

	// the given threads, or every unmuted thread with no work item yet; dryRun
	// scores without writing
	public static Map<String, Object> classify(User user, Insight insight, List<String> threadIds, boolean dryRun) {
		return classify(user, insight, threadIds, dryRun, null);
	}

	// same as classify, as a background job polled with BrainGetJob(kind=classify);
	// results stay out of the job row
	public static Map<String, Object> start(User user, List<String> threadIds) {
		// fail here, not inside the job, when no model is set or the caller cannot use
		// it
		requireEngine(user);
		var owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> params = new LinkedHashMap<>();
		if (threadIds != null && !threadIds.isEmpty()) {
			params.put("threads", threadIds.size());
		}
		return CollaborationJobUtils.start(owner.getValue0(), owner.getValue1(), JOB_KIND, params, job -> {
			// own insight so the run does not depend on the page that started it
			Insight insight = new Insight();
			insight.setUser(user);
			job.step("classifying", 1);
			Map<String, Object> summary = classify(user, insight, threadIds, false, (done, total) -> {
				job.count("done", done);
				job.count("total", total);
				job.step("classifying", total == 0 ? 99 : Math.max(1, 99 * done / total));
			});
			for (String key : new String[] { "classifier", "threads", "topics", "work", "errors", "automatedPeople",
					"automatedSenders", "senderVoteCalls" }) {
				job.count(key, summary.get(key));
			}
		});
	}

	// onboarding, after topics are picked: real mail the sort kept (not automated, no topic link) is filed against
	// the kept topics; Work items keep their state and only gain the topic
	public static Map<String, Object> startTopics(User user) {
		requireEngine(user);
		var owner = CollaborationDbUtils.ownerOf(user);
		return CollaborationJobUtils.start(owner.getValue0(), owner.getValue1(), JOB_KIND, Map.of("mode", "topics"),
				job -> {
					Insight insight = new Insight();
					insight.setUser(user);
					job.step("filing", 1);
					Map<String, Object> summary = fileTopics(user, insight, (done, total) -> {
						job.count("done", done);
						job.count("total", total);
						job.step("filing", total == 0 ? 99 : Math.max(1, 99 * done / total));
					});
					for (String key : new String[] { "classifier", "threads", "topics", "errors" }) {
						job.count(key, summary.get(key));
					}
				});
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> fileTopics(User user, Insight insight, Progress progress) {
		Context ctx = context(user, insight, false);
		List<String> ids = ctx.topics().isEmpty() ? List.of() : CollaborationDbUtils.query(
				"SELECT t.THREAD_ID FROM BRAIN_THREAD t WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ? "
						+ "AND (t.MUTED IS NULL OR t.MUTED = ?) AND (t.AUTOMATED IS NULL OR t.AUTOMATED = ?) "
						+ "AND NOT EXISTS (SELECT 1 FROM BRAIN_THREAD_TOPIC l WHERE l.OWNER_ID = t.OWNER_ID "
						+ "AND l.OWNER_TYPE = t.OWNER_TYPE AND l.THREAD_ID = t.THREAD_ID) ORDER BY t.LAST_MESSAGE_AT DESC",
				rs -> rs.getString(1), ctx.ownerId(), ctx.ownerType(), false, false);
		List<Result> results = new ArrayList<>();
		ExecutorService pool = Executors.newFixedThreadPool(parallel());
		try {
			results.addAll(runAll(pool, ids, threadId -> {
					Map<String, Object> read = BrainThreadMessages.read(ctx.user(), ctx.ownerId(), ctx.ownerType(),
							threadId, ctx.window().messages(), BrainMessageSource.current());
					List<Map<String, Object>> messages = (List<Map<String, Object>>) read.get("messages");
					if (Boolean.TRUE.equals(read.get("muted")) || messages == null || messages.isEmpty()) {
						return new Result(threadId, null, null, null, "skipped", null, null, null);
					}
					String subject = CollaborationDbUtils.queryOne("SELECT SUBJECT FROM BRAIN_THREAD WHERE OWNER_ID = ? "
							+ "AND OWNER_TYPE = ? AND THREAD_ID = ?", rs -> CollaborationDbUtils.getString(rs, "SUBJECT"),
							ctx.ownerId(), ctx.ownerType(), threadId);
					Filing filing = fileTopic(ctx, threadId,
							modelScores(ctx, threadId, subject, messages, false), true);
					if (filing.topicId() != null) {
						CollaborationDbUtils.update("UPDATE WORK_ITEM SET LINK_TOPIC_ID = ? WHERE OWNER_ID = ? AND "
								+ "OWNER_TYPE = ? AND THREAD_ID = ? AND LINK_TOPIC_ID IS NULL", filing.topicId(),
								ctx.ownerId(), ctx.ownerType(), threadId);
					}
					return new Result(threadId, filing.topicId(), filing.confidence(), filing.band(), null, null, null,
							null);
			}, "Topic filing", progress));
		} finally {
			pool.shutdown();
		}
		return summary(ctx.classifier().version(), false, results);
	}

	@FunctionalInterface
	private interface Task {
		Result run(String threadId) throws Exception;
	}

	// runs every thread on the pool and reports progress as each one finishes, in whatever order they finish
	private static List<Result> runAll(ExecutorService pool, List<String> ids, Task task, String label,
			Progress progress) {
		CompletionService<Result> done = new ExecutorCompletionService<>(pool);
		for (String threadId : ids) {
			done.submit(() -> {
				try {
					return task.run(threadId);
				} catch (Exception e) {
					classLogger.warn("{} failed on thread {}", label, threadId, e);
					return new Result(threadId, null, null, null, null, null, null, rootMessage(e));
				}
			});
		}
		if (progress != null) {
			progress.report(0, ids.size());
		}
		List<Result> results = new ArrayList<>();
		for (int i = 0; i < ids.size(); i++) {
			try {
				results.add(done.take().get());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Classification was interrupted", e);
			} catch (ExecutionException e) {
				throw new IllegalStateException(e.getCause());
			}
			if (progress != null) {
				progress.report(i + 1, ids.size());
			}
		}
		return results;
	}

	@FunctionalInterface
	public interface Progress {
		void report(int done, int total);
	}

	static Map<String, Object> classify(User user, Insight insight, List<String> threadIds, boolean dryRun,
			Progress progress) {
		Context ctx = context(user, insight, dryRun);
		String ownerId = ctx.ownerId();
		String ownerType = ctx.ownerType();
		BrainClassifier classifier = ctx.classifier();
		List<String> ids = threadIds == null || threadIds.isEmpty() ? pending(ownerId, ownerType, dryRun) : threadIds;

		List<Result> results = new ArrayList<>();
		ExecutorService pool = Executors.newFixedThreadPool(parallel());
		BrainSenderVote.Outcome vote = null;
		try {
			// senders first: an automated one's threads then need no model call
			if (!dryRun) {
				vote = BrainSenderVote.run(ownerId, ownerType, ctx.self().personId(), new HashSet<>(ids), threadId -> {
					BrainClassifier.Scores scores = scoreOnly(ctx, threadId);
					if (scores == null) {
						return null;
					}
					ctx.scored().put(threadId, scores);
					return scores.automated();
				}, pool);
				ctx.automatedSenders().addAll(vote.automated());
			}
			results.addAll(runAll(pool, ids, threadId -> classifyOne(ctx, threadId), "Classifier", progress));
		} finally {
			pool.shutdown();
		}
		Map<String, Object> summary = summary(classifier.version(), dryRun, results);
		if (vote != null) {
			summary.put("automatedSenders", vote.typed());
			summary.put("senderVoteCalls", vote.calls());
		}
		if (!dryRun) {
			// senders of only automated threads leave People, Topics and Follow
			summary.put("automatedPeople", BrainSenderTyping.fromThreads(ownerId, ownerType));
		}
		return summary;
	}

	private record Self(String personId, String name, Set<String> addresses) {
	}

	private record Context(User user, Insight insight, String ownerId, String ownerType, BrainClassifier classifier,
			BrainClassifier.Cutoffs cutoffs, List<BrainClassifier.TopicOption> topics, int fileAt, int askAt,
			boolean dryRun, Self self, Set<String> vips, Set<String> followed, Set<String> automatedSenders,
			Map<String, BrainClassifier.Scores> scored, Window window) {
	}

	// how much of a thread one model call carries; budget is characters for all messages together
	private record Window(int messages, int textChars, int historyChars, int footerChars, int budget,
			boolean earlier) {
	}

	// the model, cutoffs, topics and people a run needs; fails when no model is set or the caller cannot use it
	private static Context context(User user, Insight insight, boolean dryRun) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Map<String, Object> settings = BrainProfileUtils.getSettings(ownerId, ownerType);
		String engine = requireEngine(user);
		IModelEngine model = Utility.getModel(engine);
		if (model == null) {
			throw new IllegalArgumentException("Model " + engine + " could not be loaded");
		}
		BrainClassifier classifier = BrainClassifier.forEngine(engine, model);
		List<BrainClassifier.TopicOption> topics = new ArrayList<>(topics(ownerId, ownerType));
		if (!topics.isEmpty()) {
			topics.add(new BrainClassifier.TopicOption(OTHER_TOPIC, "Something else",
					"Not about the other topics: other work, personal, travel, or automated mail."));
		}
		Set<String> vips = new HashSet<>(CollaborationDbUtils.query(
				"SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? " + "AND OWNER_TYPE = ? AND IS_VIP = ?",
				rs -> rs.getString(1), ownerId, ownerType, true));
		Set<String> followed = new HashSet<>(CollaborationDbUtils.query(
				"SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND FOLLOW_STATE = ?",
				rs -> rs.getString(1), ownerId, ownerType, BrainFollow.FOLLOWING));
		return new Context(user, insight, ownerId, ownerType, classifier, cutoffs(engine, classifier), topics,
				(Integer) settings.get("fileAt"), (Integer) settings.get("askAt"), dryRun, self(ownerId, ownerType),
				vips, followed, ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), window(engine));
	}

	@SuppressWarnings("unchecked")
	private static Result classifyOne(Context ctx, String threadId) {
		// database checks first: an automated thread needs no Graph read and no model call
		Map<String, Object> thread = CollaborationDbUtils
				.queryOne("SELECT SUBJECT, SOURCE, AUTOMATED FROM BRAIN_THREAD "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?", rs -> {
							Map<String, Object> row = new LinkedHashMap<>();
							row.put("subject", CollaborationDbUtils.getString(rs, "SUBJECT"));
							row.put("source", CollaborationDbUtils.getString(rs, "SOURCE"));
							row.put("automated", CollaborationDbUtils.getBoolean(rs, "AUTOMATED"));
							return row;
						}, ctx.ownerId(), ctx.ownerType(), threadId);
		if (thread == null) {
			throw new IllegalArgumentException("Thread not found");
		}
		// marked automated by an earlier run: no model call
		if (!ctx.dryRun() && Boolean.TRUE.equals(thread.get("automated"))) {
			return new Result(threadId, null, null, null, "automated", null, null, null);
		}
		// the owner never wrote here and everyone else is an automated sender or says machine-sent: no model call
		if (!ctx.dryRun() && machineOnly(ctx, threadId)) {
			CollaborationDbUtils.update("UPDATE BRAIN_THREAD SET AUTOMATED = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND THREAD_ID = ?", true, ctx.ownerId(), ctx.ownerType(), threadId);
			ctx.scored().remove(threadId);
			return new Result(threadId, null, null, null, "automated", null, null, null);
		}
		Map<String, Object> read = BrainThreadMessages.read(ctx.user(), ctx.ownerId(), ctx.ownerType(), threadId,
				ctx.window().messages(), BrainMessageSource.current());
		List<Map<String, Object>> messages = (List<Map<String, Object>>) read.get("messages");
		if (Boolean.TRUE.equals(read.get("muted")) || messages == null || messages.isEmpty()) {
			return new Result(threadId, null, null, null, "skipped", null, null, null);
		}
		Map<String, Object> newest = messages.get(messages.size() - 1);
		boolean fromMe = ctx.self().personId() != null && ctx.self().personId().equals(newest.get("fromId"));
		String onIt = recipientRole(ctx.self(), newest);
		// filed by the owner or by onboarding: no topic question, the link stays
		boolean kept = !ctx.dryRun() && hasKeptLink(ctx, threadId);

		// a sender vote may have scored it already
		BrainClassifier.Scores cached = ctx.scored().remove(threadId);
		BrainClassifier.Scores scores = cached != null ? cached
				: modelScores(ctx, threadId, (String) thread.get("subject"), messages, kept);
		Map<String, Object> signals = signals(scores);
		boolean automated = scores.automated() >= ctx.cutoffs().automatedAt();

		// topic: file, ask, or leave; owner-made links are never touched and automated
		// mail gets no topic (a dry run scores both anyway)
		Filing filing = fileTopic(ctx, threadId, scores, ctx.dryRun() || (!automated && !kept));
		String topicId = filing.topicId();
		Integer confidence = filing.confidence();
		String band = filing.band();

		// work: the newest message decides; one item per newest message
		String work;
		String askType;
		boolean suggested = false;
		List<String> reasons = new ArrayList<>();
		if (automated) {
			if (!ctx.dryRun()) {
				CollaborationDbUtils
						.update("UPDATE BRAIN_THREAD SET AUTOMATED = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
								+ "AND THREAD_ID = ?", true, ctx.ownerId(), ctx.ownerType(), threadId);
			}
			return new Result(threadId, topicId, confidence, band, "automated", null, signals, null);
		} else if (Boolean.TRUE.equals(newest.get("meeting"))) {
			// an invite, reply, or cancellation: the calendar has it, nothing to wait on or answer here
			work = "fyi";
			askType = "fyi";
			reasons.add("Calendar message");
		} else if (fromMe) {
			work = "waiting";
			askType = "waiting_on";
			reasons.add("You wrote last; waiting on a reply");
		} else if (!"to".equals(onIt)) {
			// copied, or reached through a list or Bcc: worth knowing, not an ask of the
			// owner
			work = "fyi";
			askType = "fyi";
			reasons.add("cc".equals(onIt) ? "You were copied" : "Not addressed to you");
		} else if (scores.fyi() >= ctx.cutoffs().fyiAt()) {
			work = "fyi";
			askType = "fyi";
			reasons.add("Looks like an update with nothing to do");
		} else {
			suggested = scores.fyi() >= ctx.cutoffs().asksAt();
			work = suggested ? "suggested" : "needs_me";
			askType = "reply";
			reasons.add(suggested ? "Might need you; confirm" : "Asks you to act");
		}
		double urgency = scores.urgency();
		// a VIP's ask moves up one level, someone you follow half a level
		boolean fromVip = newest.get("fromId") != null && ctx.vips().contains(newest.get("fromId"));
		boolean fromFollowed = newest.get("fromId") != null && ctx.followed().contains(newest.get("fromId"));
		if (fromVip && !"fyi".equals(work)) {
			urgency = Math.min(3, urgency + 0.8);
			reasons.add("From a VIP");
		} else if (fromFollowed && !"fyi".equals(work) && !"waiting".equals(work)) {
			urgency = Math.min(3, urgency + 0.4);
			reasons.add("From someone you follow");
		}
		long age = ageDays((String) newest.get("at"));
		if (age > WEEK_DAYS) {
			urgency = Math.min(urgency, 1);
		} else if (age > TODAY_DAYS) {
			urgency = Math.min(urgency, 2);
		}
		String priority = urgency >= 2.5 ? "P0" : urgency >= 1.8 ? "P1" : urgency >= 1.0 ? "P2" : "P3";
		reasons.add("Urgency: " + URGENCY[(int) Math.max(0, Math.min(3, Math.round(urgency)))]);
		if (!ctx.dryRun()) {
			Map<String, Object> item = new LinkedHashMap<>();
			item.put("dedupeKey", "classifier:" + threadId + ":" + newest.get("id"));
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
			item.put("classifierVersion", ctx.classifier().version());
			item.put("reason", "classifier");
			// the thread's existing Work decides whether this is a new card, an update, or nothing
			Map<String, Object> created = WorkItemUtils.ingestOnThread(ctx.ownerId(), ctx.ownerType(), item);
			boolean wrote = Boolean.TRUE.equals(created.get("created")) || Boolean.TRUE.equals(created.get("updated"));
			if (wrote && created.get("item") instanceof Map<?, ?> saved) {
				CollaborationDbUtils.update(
						"UPDATE WORK_ITEM SET SIGNALS_JSON = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
								+ "AND ITEM_ID = ?",
						CollaborationDbUtils.toJson(signals), ctx.ownerId(), ctx.ownerType(), saved.get("id"));
			}
		}
		return new Result(threadId, topicId, confidence, band, work, priority, signals, null);
	}

	private record Filing(String topicId, Integer confidence, String band) {
	}

	// files, asks about, or leaves the thread's topic from its scores; writes links unless a dry run
	private static Filing fileTopic(Context ctx, String threadId, BrainClassifier.Scores scores, boolean allowed) {
		String topicId = null;
		Integer confidence = null;
		String band = null;
		List<Map.Entry<String, Double>> ranked = new ArrayList<>(scores.topics().entrySet());
		ranked.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
		if (!ranked.isEmpty() && allowed) {
			String bestId = ranked.get(0).getKey();
			String nextId = ranked.size() > 1 ? ranked.get(1).getKey() : null;
			// with a "Something else" choice the probability itself is the confidence
			// (fixture, one topic:
			// real threads 0.87 and up, others mostly under 0.75), and only near misses are
			// asked
			boolean wayOut = scores.topics().containsKey(OTHER_TOPIC);
			int askAt = wayOut ? Math.max(ctx.askAt(), ctx.fileAt() - WAY_OUT_ASK_BAND) : ctx.askAt();
			if (wayOut) {
				confidence = (int) Math.round(100 * ranked.get(0).getValue());
			} else {
				double margin = ranked.get(0).getValue() - (nextId != null ? ranked.get(1).getValue() : 0);
				// 0.1 of margin reads as 90 (a top-two gap that size was right every time in
				// the eval); a near
				// tie reads as no pick, so it stays unassigned instead of suggesting a topic
				confidence = (int) Math.min(100, Math.round(900 * margin));
			}
			if (OTHER_TOPIC.equals(bestId)) {
				band = "unassigned";
			} else if (confidence >= ctx.fileAt()) {
				band = "filed";
				topicId = bestId;
				if (!ctx.dryRun()) {
					link(ctx, threadId, bestId, "confirmed", confidence, scores.topics());
				}
			} else if (confidence >= askAt && nextId != null) {
				band = "asked";
				if (!ctx.dryRun()) {
					link(ctx, threadId, bestId, "suggested", confidence, scores.topics());
					List<String> candidates = OTHER_TOPIC.equals(nextId) ? List.of(bestId) : List.of(bestId, nextId);
					// the runner-up is linked too, so the owner can pick either one or both
					if (candidates.size() > 1) {
						link(ctx, threadId, nextId, "suggested", confidence, scores.topics());
					}
					BrainReviewUtils.addReview(ctx.ownerId(), ctx.ownerType(), BrainReviewUtils.TOPIC_CHOICE, "thread",
							threadId, ctx.classifier().version(), Map.of("candidates", candidates));
				}
			} else {
				band = "unassigned";
			}
		}
		return new Filing(topicId, confidence, band);
	}

	private static BrainClassifier.Scores modelScores(Context ctx, String threadId, String subject,
			List<Map<String, Object>> messages, boolean kept) {
		Window window = ctx.window();
		// newest first, so the budget always keeps the newest message
		List<BrainClassifier.Message> input = new ArrayList<>();
		int left = window.budget();
		for (int i = messages.size() - 1; i >= 0 && left > 0; i--) {
			Map<String, Object> m = messages.get(i);
			int max = Math.min(left,
					Boolean.TRUE.equals(m.get("history")) ? window.historyChars() : window.textChars());
			String text = clip(m.get("text"), max);
			left -= text.length();
			String footer = window.footerChars() == 0 ? null : clip(m.get("footer"), Math.min(left, window.footerChars()));
			left -= footer == null ? 0 : footer.length();
			input.add(0, new BrainClassifier.Message(from(ctx, m), names(ctx.self(), m.get("to")),
					names(ctx.self(), m.get("cc")), (String) m.get("at"), text,
					footer == null || footer.isEmpty() ? null : footer));
		}
		return ctx.classifier().score(new BrainClassifier.ThreadInput(threadId, ctx.self().name(), subject,
				participants(ctx, threadId), input, window.earlier()), kept ? List.of() : ctx.topics(), ctx.insight());
	}

	private static String clip(Object value, int max) {
		String text = value == null ? "" : String.valueOf(value);
		return text.length() > max ? text.substring(0, Math.max(0, max)) : text;
	}

	// "Name <address>": the address says no-reply or a notification service when the name says a person
	private static String from(Context ctx, Map<String, Object> m) {
		if (Objects.equals(ctx.self().personId(), m.get("fromId"))) {
			return "me";
		}
		String name = (String) m.get("fromName");
		String address = (String) m.get("fromAddress");
		if (address == null || address.isBlank()) {
			return name;
		}
		return name == null || name.isBlank() ? address : name + " <" + address + ">";
	}

	// the short default the cutoffs were tuned on, unless RDF_Map gives this engine a context window
	private static Window window(String engine) {
		Map<String, Object> all = CollaborationDbUtils
				.parseMap(Utility.getDIHelperProperty(Constants.COLLAB_CLASSIFIER_WINDOW));
		Object tokens = all == null ? null : all.get(engine);
		if (!(tokens instanceof Number n) || n.intValue() < MIN_WINDOW_TOKENS) {
			return new Window(MESSAGES, TEXT_CHARS, HISTORY_CHARS, 0, Integer.MAX_VALUE, false);
		}
		return new Window(WIDE_MESSAGES, WIDE_TEXT_CHARS, WIDE_TEXT_CHARS, WIDE_FOOTER_CHARS,
				n.intValue() / 2 * CHARS_PER_TOKEN, true);
	}

	// the same scores classifyOne would ask for, without writing anything; null for a muted or empty thread
	@SuppressWarnings("unchecked")
	private static BrainClassifier.Scores scoreOnly(Context ctx, String threadId) {
		Map<String, Object> read = BrainThreadMessages.read(ctx.user(), ctx.ownerId(), ctx.ownerType(), threadId,
				ctx.window().messages(), BrainMessageSource.current());
		List<Map<String, Object>> messages = (List<Map<String, Object>>) read.get("messages");
		if (Boolean.TRUE.equals(read.get("muted")) || messages == null || messages.isEmpty()) {
			return null;
		}
		String subject = CollaborationDbUtils.queryOne("SELECT SUBJECT FROM BRAIN_THREAD WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND THREAD_ID = ?", rs -> CollaborationDbUtils.getString(rs, "SUBJECT"), ctx.ownerId(),
				ctx.ownerType(), threadId);
		return modelScores(ctx, threadId, subject, messages, hasKeptLink(ctx, threadId));
	}

	// every other sender on the thread is automated or sent it as machine mail, and the owner never wrote
	private static boolean machineOnly(Context ctx, String threadId) {
		List<Object[]> rows = CollaborationDbUtils.query("SELECT SENDER_PERSON_ID, AUTO FROM BRAIN_MESSAGE WHERE "
				+ "OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND SENDER_PERSON_ID IS NOT NULL",
				rs -> new Object[] { rs.getString(1), CollaborationDbUtils.getBoolean(rs, "AUTO") }, ctx.ownerId(),
				ctx.ownerType(), threadId);
		boolean allSenders = true;
		boolean allAuto = true;
		for (Object[] r : rows) {
			if (r[0].equals(ctx.self().personId())) {
				return false;
			}
			allSenders &= ctx.automatedSenders().contains(r[0]);
			allAuto &= Boolean.TRUE.equals(r[1]);
		}
		return !rows.isEmpty() && (allSenders || allAuto);
	}

	// the platform model (COLLAB_CLASSIFIER_ENGINE_ID); the caller needs access to
	// it
	static String requireEngine(User user) {
		String engine = platformEngine();
		if (engine == null) {
			throw new IllegalArgumentException("No classifier model is set; an admin sets "
					+ Constants.COLLAB_CLASSIFIER_ENGINE_ID + " in RDF_Map.prop");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, engine)) {
			throw new IllegalArgumentException("You do not have access to the classifier model (" + engine
					+ "); ask an admin to share it with you");
		}
		return engine;
	}

	/** The platform classifier engine id from RDF_Map.prop, or null when unset. */
	public static String platformEngine() {
		String id = Utility.getDIHelperProperty(Constants.COLLAB_CLASSIFIER_ENGINE_ID);
		return id == null || id.isBlank() ? null : id.trim();
	}

	// the classifier's defaults, unless RDF_Map carries cutoffs for this engine
	@SuppressWarnings("unchecked")
	private static BrainClassifier.Cutoffs cutoffs(String engine, BrainClassifier classifier) {
		BrainClassifier.Cutoffs base = classifier.cutoffs();
		Map<String, Object> all = CollaborationDbUtils
				.parseMap(Utility.getDIHelperProperty(Constants.COLLAB_CLASSIFIER_CUTOFFS));
		Object mine = all == null ? null : all.get(engine);
		if (!(mine instanceof Map<?, ?> m)) {
			return base;
		}
		Map<String, Object> c = (Map<String, Object>) m;
		return new BrainClassifier.Cutoffs(number(c.get("fyiAt"), base.fyiAt()), number(c.get("asksAt"), base.asksAt()),
				number(c.get("automatedAt"), base.automatedAt()));
	}

	private static double number(Object value, double fallback) {
		return value instanceof Number n ? n.doubleValue() : fallback;
	}

	// what gets stored and returned: the scores the policy used, plus the model's
	// own answer
	private static Map<String, Object> signals(BrainClassifier.Scores scores) {
		Map<String, Object> signals = new LinkedHashMap<>();
		signals.put("topics", scores.topics());
		signals.put("fyi", scores.fyi());
		signals.put("automated", scores.automated());
		signals.put("urgency", scores.urgency());
		signals.put("raw", scores.raw());
		return signals;
	}

	// the owner as a person: id, display name, and every address, to mark "me" in
	// from, to, and cc
	private static Self self(String ownerId, String ownerType) {
		Map<String, Object> person = CollaborationDbUtils.queryOne("SELECT PERSON_ID, DISPLAY_NAME, EMAIL_NORM "
				+ "FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND RELATIONSHIP = ?", rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("id", rs.getString("PERSON_ID"));
					row.put("name", CollaborationDbUtils.getString(rs, "DISPLAY_NAME"));
					row.put("email", CollaborationDbUtils.getString(rs, "EMAIL_NORM"));
					return row;
				}, ownerId, ownerType, "self");
		if (person == null) {
			return new Self(null, "the owner", Set.of());
		}
		Set<String> addresses = new HashSet<>(CollaborationDbUtils.query(
				"SELECT VALUE_NORM FROM BRAIN_PERSON_ADDRESS "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?",
				rs -> rs.getString(1), ownerId, ownerType, person.get("id")));
		if (person.get("email") != null) {
			addresses.add((String) person.get("email"));
		}
		return new Self((String) person.get("id"), (String) person.get("name"), addresses);
	}

	// "to" or "cc" when the owner is on the newest message that way, else "none"
	private static String recipientRole(Self self, Map<String, Object> message) {
		// the owner's addresses are not known yet: do not guess
		if (self.addresses().isEmpty()) {
			return "to";
		}
		for (String field : new String[] { "to", "cc" }) {
			if (message.get(field) instanceof List<?> list) {
				for (Object r : list) {
					if (r instanceof Map<?, ?> m && (self.addresses()
							.contains(BrainRulesGate.norm(CollaborationDbUtils.asString(m.get("address"))))
							|| (self.name() != null
									&& self.name().equalsIgnoreCase(String.valueOf(m.get("name")).trim())))) {
						return field;
					}
				}
			}
		}
		return "none";
	}

	private static List<String> names(Self self, Object recipients) {
		List<String> names = new ArrayList<>();
		if (recipients instanceof List<?> list) {
			for (Object r : list) {
				if (r instanceof Map<?, ?> m) {
					String address = BrainRulesGate.norm(CollaborationDbUtils.asString(m.get("address")));
					Object name = m.get("name");
					names.add(address != null && self.addresses().contains(address) ? "me"
							: name != null ? String.valueOf(name) : String.valueOf(m.get("address")));
				}
			}
		}
		return names;
	}

	// active and dormant topics with a description the model can match against
	private static List<BrainClassifier.TopicOption> topics(String ownerId, String ownerType) {
		// the topic's profile: its key people, and the outside domains they write from
		String myDomain = BrainMailImport.domain(CollaborationDbUtils.queryOne("SELECT EMAIL_NORM FROM BRAIN_PERSON "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND RELATIONSHIP = ?", rs -> rs.getString(1), ownerId, ownerType,
				"self"));
		BrainOrgDomains.Org ownOrg = BrainOrgDomains.load(ownerId, ownerType, myDomain);
		Map<String, Set<String>> people = new HashMap<>();
		Map<String, Set<String>> domains = new HashMap<>();
		CollaborationDbUtils.query("SELECT tp.TOPIC_ID, p.DISPLAY_NAME, p.EMAIL_NORM FROM BRAIN_TOPIC_PERSON tp JOIN "
				+ "BRAIN_PERSON p ON p.OWNER_ID = tp.OWNER_ID AND p.OWNER_TYPE = tp.OWNER_TYPE AND p.PERSON_ID = tp.PERSON_ID "
				+ "WHERE tp.OWNER_ID = ? AND tp.OWNER_TYPE = ? AND tp.STATE IN (?, ?) ORDER BY tp.CHANGED_AT", rs -> {
					String name = CollaborationDbUtils.getString(rs, "DISPLAY_NAME");
					if (name != null && !name.isBlank()) {
						people.computeIfAbsent(rs.getString(1), k -> new LinkedHashSet<>()).add(name);
					}
					String domain = BrainMailImport.domain(CollaborationDbUtils.getString(rs, "EMAIL_NORM"));
					if (domain != null && !ownOrg.isMine(domain)) {
						domains.computeIfAbsent(rs.getString(1), k -> new LinkedHashSet<>()).add(domain);
					}
					return null;
				}, ownerId, ownerType, BrainTopicUtils.MEMBER, BrainTopicUtils.SUGGESTED);
		return CollaborationDbUtils.query(
				"SELECT t.TOPIC_ID, t.NAME, t.DESCRIPTION, t.SUGGEST_REASON, t.KEYWORDS_JSON, " + "a.NAME AS ACCOUNT "
						+ "FROM BRAIN_TOPIC t LEFT JOIN BRAIN_ACCOUNT a ON a.OWNER_ID = t.OWNER_ID AND a.OWNER_TYPE = t.OWNER_TYPE "
						+ "AND a.ACCOUNT_ID = t.ACCOUNT_ID WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ? AND t.STATUS IN (?, ?) "
						+ "ORDER BY t.TOPIC_ID",
				rs -> {
					StringBuilder describe = new StringBuilder();
					// a suggested topic has no description yet; why it was suggested is the next
					// best thing
					String description = CollaborationDbUtils.getString(rs, "DESCRIPTION");
					if (description == null || description.isBlank()) {
						description = CollaborationDbUtils.getString(rs, "SUGGEST_REASON");
					}
					if (description != null) {
						describe.append(description);
					}
					String account = CollaborationDbUtils.getString(rs, "ACCOUNT");
					if (account != null) {
						describe.append(" Account: ").append(account).append('.');
					}
					String topicId = rs.getString("TOPIC_ID");
					if (people.containsKey(topicId)) {
						describe.append(" Key people: ")
								.append(String.join(", ", people.get(topicId).stream().limit(KEY_PEOPLE).toList())).append('.');
					}
					if (domains.containsKey(topicId)) {
						describe.append(" Domains: ").append(String.join(", ", domains.get(topicId))).append('.');
					}
					List<Object> keywords = CollaborationDbUtils
							.parseList(CollaborationDbUtils.getString(rs, "KEYWORDS_JSON"));
					if (!keywords.isEmpty()) {
						describe.append(" Keywords: ")
								.append(String.join(", ", CollaborationDbUtils.toStringList(keywords))).append('.');
					}
					return new BrainClassifier.TopicOption(rs.getString("TOPIC_ID"),
							CollaborationDbUtils.getString(rs, "NAME"), describe.toString().trim());
				}, ownerId, ownerType, BrainTopicUtils.ACTIVE, BrainTopicUtils.DORMANT);
	}

	// a dry run looks at every unmuted thread; a real run only at threads with no
	// work item yet
	private static List<String> pending(String ownerId, String ownerType, boolean all) {
		return CollaborationDbUtils
				.query("SELECT t.THREAD_ID FROM BRAIN_THREAD t WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ? "
						+ "AND (t.MUTED IS NULL OR t.MUTED = ?) AND (t.AUTOMATED IS NULL OR t.AUTOMATED = ?)"
						+ (all ? ""
								: " AND NOT EXISTS (SELECT 1 FROM WORK_ITEM w "
										+ "WHERE w.OWNER_ID = t.OWNER_ID AND w.OWNER_TYPE = t.OWNER_TYPE AND w.THREAD_ID = t.THREAD_ID)")
						+ " ORDER BY t.LAST_MESSAGE_AT DESC", rs -> rs.getString(1), ownerId, ownerType, false, false);
	}

	private static List<String> participants(Context ctx, String threadId) {
		return CollaborationDbUtils.query("SELECT p.PERSON_ID, p.DISPLAY_NAME, p.JOB_TITLE, p.COMPANY FROM "
				+ "BRAIN_THREAD_PARTICIPANT tp JOIN BRAIN_PERSON p ON p.OWNER_ID = tp.OWNER_ID AND p.OWNER_TYPE = tp.OWNER_TYPE "
				+ "AND p.PERSON_ID = tp.PERSON_ID WHERE tp.OWNER_ID = ? AND tp.OWNER_TYPE = ? AND tp.THREAD_ID = ? "
				+ "ORDER BY p.DISPLAY_NAME", rs -> {
					if (rs.getString("PERSON_ID").equals(ctx.self().personId())) {
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
				}, ctx.ownerId(), ctx.ownerType(), threadId).stream().filter(Objects::nonNull).toList();
	}

	// a link the owner made, or one onboarding filed from the topic's own threads
	private static boolean hasKeptLink(Context ctx, String threadId) {
		return CollaborationDbUtils.exists(
				"SELECT 1 FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND THREAD_ID = ? AND (SOURCE = ? OR CLASSIFIER_VERSION = ?)",
				ctx.ownerId(), ctx.ownerType(), threadId, BrainProfileUtils.YOU, BrainTopicOnboarding.VERSION);
	}

	private static int parallel() {
		String value = Utility.getDIHelperProperty(Constants.COLLAB_CLASSIFY_PARALLEL);
		try {
			return value == null || value.isBlank() ? PARALLEL
					: Math.max(1, Math.min(MAX_PARALLEL, Integer.parseInt(value.trim())));
		} catch (NumberFormatException e) {
			classLogger.warn("{} is not a number; using {}", Constants.COLLAB_CLASSIFY_PARALLEL, PARALLEL);
			return PARALLEL;
		}
	}

	// whole days since an ISO time; 0 when unknown
	private static long ageDays(String at) {
		try {
			return at == null ? 0 : java.time.Duration.between(java.time.Instant.parse(at), java.time.Instant.now()).toDays();
		} catch (java.time.format.DateTimeParseException e) {
			return 0;
		}
	}

	// a classifier link replaces an earlier classifier link on the same topic; the
	// first link is the primary
	private static void link(Context ctx, String threadId, String topicId, String source, int confidence,
			Map<String, Double> topics) {
		Timestamp now = CollaborationDbUtils.now();
		boolean hasPrimary = CollaborationDbUtils.exists(
				"SELECT 1 FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? "
						+ "AND OWNER_TYPE = ? AND THREAD_ID = ? AND IS_PRIMARY = ? AND TOPIC_ID <> ?",
				ctx.ownerId(), ctx.ownerType(), threadId, true, topicId);
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND THREAD_ID = ? AND TOPIC_ID = ?", ctx.ownerId(), ctx.ownerType(), threadId, topicId);
			CollaborationDbUtils.update(conn,
					"INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, "
							+ "SOURCE, CONFIDENCE, IS_PRIMARY, SIGNALS_JSON, CLASSIFIER_VERSION, CHANGED_BY, CHANGED_AT) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					ctx.ownerId(), ctx.ownerType(), threadId, topicId, source, confidence, !hasPrimary,
					CollaborationDbUtils.toJson(topics), ctx.classifier().version(), "brain", now);
		});
	}

	private static Map<String, Object> summary(String version, boolean dryRun, List<Result> results) {
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
			row.put("scores", r.scores());
			row.put("error", r.error());
			rows.add(row);
		}
		Map<String, Object> summary = new LinkedHashMap<>();
		summary.put("classifier", version);
		summary.put("dryRun", dryRun);
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
