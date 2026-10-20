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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.collaboration.BrainMemoryReview.Turn;
import prerna.collaboration.BrainRulesGate.Rule;
import prerna.engine.api.IModelEngine;
import prerna.om.Insight;
import prerna.util.RuntimeTimeContext;
import prerna.util.Utility;

/**
 * Orients a chat to the owner's topics as they chat (D6): after each assistant turn, the chat is scored by the same
 * classifier that files mail, with the owner's filing thresholds. A confident topic is linked, a near one is a soft
 * tag; a topic the owner set or dismissed is never touched and nothing is ever removed.
 */
public final class BrainChatTopics {

	private static final Logger classLogger = LogManager.getLogger(BrainChatTopics.class);

	static final String BRAIN = "brain";
	// how long after a turn the chat is scored; a newer turn restarts the wait
	private static final int DELAY_SECONDS = 3;
	// characters of the newest turns the classifier reads
	private static final int MAX_CHARS = 12000;
	private static final int MAX_TURN_CHARS = 2000;

	private static final ScheduledExecutorService TIMER = Executors.newSingleThreadScheduledExecutor(r -> {
		Thread t = new Thread(r, "collaboration-chat-topics-timer");
		t.setDaemon(true);
		return t;
	});
	private static final ExecutorService POOL = Executors.newFixedThreadPool(1, r -> {
		Thread t = new Thread(r, "collaboration-chat-topics");
		t.setDaemon(true);
		return t;
	});
	private static final Map<String, ScheduledFuture<?>> PENDING = new ConcurrentHashMap<>();

	private BrainChatTopics() {
	}

	/** After a run in the owner's chat: score it once the turn has settled. Does nothing without a classifier. */
	public static void schedule(User user, String roomId) {
		if (user == null || roomId == null || BrainThreadClassifier.platformEngine() == null) {
			return;
		}
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String key = owner.getValue1() + ":" + owner.getValue0() + ":" + roomId;
		PENDING.compute(key, (k, previous) -> {
			if (previous != null) {
				previous.cancel(false);
			}
			ScheduledFuture<?>[] self = new ScheduledFuture<?>[1];
			self[0] = TIMER.schedule(() -> {
				PENDING.remove(k, self[0]);
				POOL.execute(() -> {
					try {
						classify(user, roomId);
					} catch (RuntimeException e) {
						classLogger.warn("Topic check of chat {} failed; the next turn tries again", roomId, e);
					}
				});
			}, DELAY_SECONDS, TimeUnit.SECONDS);
			return self[0];
		});
	}

	/** Scores the chat now and applies the result: { roomId, topicId, confidence, action }. */
	static Map<String, Object> classify(User user, String roomId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainTopicRoomUtils.Chat chat = BrainTopicRoomUtils.requireChat(user, roomId);
		Map<String, Object> result = new HashMap<>();
		result.put("roomId", roomId);
		result.put("action", "none");

		List<Turn> turns = window(BrainMemoryReview.read(BrainMemoryReview.messages(user, roomId), null).turns());
		if (turns.stream().noneMatch(turn -> BrainMemoryReview.OWNER.equals(turn.role()))) {
			return result;
		}
		List<Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		List<BrainClassifier.TopicOption> topics = new ArrayList<>();
		for (BrainClassifier.TopicOption topic : BrainThreadClassifier.topicOptions(ownerId, ownerType)) {
			if (!BrainTopicBrief.keptOut(rules, topic.id())) {
				topics.add(topic);
			}
		}
		if (topics.size() < 2) {
			return result;
		}

		String engine = BrainThreadClassifier.requireEngine(user);
		IModelEngine model = Utility.getModel(engine);
		if (model == null) {
			throw new IllegalArgumentException("Model " + engine + " could not be loaded");
		}
		BrainClassifier classifier = BrainClassifier.forEngine(engine, model);
		Insight insight = new Insight();
		insight.setUser(user);
		List<BrainClassifier.Message> messages = new ArrayList<>();
		for (Turn turn : turns) {
			boolean mine = BrainMemoryReview.OWNER.equals(turn.role());
			messages.add(new BrainClassifier.Message(mine ? "me" : "Assistant", List.of(mine ? "Assistant" : "me"),
					List.of(), "", turn.text(), null));
		}
		BrainClassifier.Scores scores = classifier.score(new BrainClassifier.ThreadInput(roomId, "me",
				"Chat with the owner's assistant", List.of("me", "Assistant"), messages, false,
				RuntimeTimeContext.capture(BrainProfileUtils.timeZone(user))), topics, insight);

		// the best topic and how sure, read the way mail filing reads it
		String bestId = null;
		double best = -1;
		for (Map.Entry<String, Double> score : scores.topics().entrySet()) {
			if (score.getValue() > best) {
				best = score.getValue();
				bestId = score.getKey();
			}
		}
		if (bestId == null || BrainThreadClassifier.OTHER_TOPIC.equals(bestId)) {
			return result;
		}
		int confidence = (int) Math.round(100 * best);
		Map<String, Object> settings = BrainProfileUtils.getSettings(ownerId, ownerType);
		int fileAt = (Integer) settings.get("fileAt");
		int askAt = Math.max((Integer) settings.get("askAt"), fileAt - BrainThreadClassifier.WAY_OUT_ASK_BAND);
		result.put("topicId", bestId);
		result.put("confidence", confidence);
		if (confidence >= askAt) {
			String action = BrainTopicRoomUtils.brainTag(ownerId, ownerType, chat, bestId, confidence >= fileAt);
			result.put("action", action);
			if (!"none".equals(action)) {
				classLogger.info("Chat {} {} topic {} at {}", roomId, action, bestId, confidence);
			}
		}
		return result;
	}

	// the newest turns that fit, oldest first; a long turn is cut to its start
	private static List<Turn> window(List<Turn> turns) {
		List<Turn> kept = new ArrayList<>();
		int used = 0;
		for (int i = turns.size() - 1; i >= 0; i--) {
			Turn turn = turns.get(i);
			String text = turn.text().length() > MAX_TURN_CHARS ? turn.text().substring(0, MAX_TURN_CHARS) + "..."
					: turn.text();
			if (used + text.length() > MAX_CHARS) {
				break;
			}
			kept.add(0, new Turn(turn.role(), text, turn.messageId()));
			used += text.length();
		}
		return kept;
	}
}
