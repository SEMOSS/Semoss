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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * Topic deletes and merges with their undo, and the topic setup's apply and chat, against an in-memory H2
 * Collaboration schema.
 */
class BrainTopicChangesDbUnitTests {

	// a no-break space, as pasted from a web page: JS trim() drops it and Java's trim() keeps it
	private static final String NBSP = "\u00A0";
	private static final String REVIEW = "review-1";
	// as Graph gives them: a folder id runs past 100 characters, and a nested folder's path past 50
	private static final String FOLDER_ID = "AAMkAD" + "x".repeat(114);
	private static final String FOLDER_PATH = "Inbox / Clients / Northwind Traders / 2026 Contract Renewal";

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private User user;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_topic_changes_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString()))
				.thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		try (Statement statement = connection.createStatement()) {
			for (Pair<String, List<Pair<String, String>>> table : new CollaborationOwlCreator(queryUtil)
					.getDBSchema()) {
				List<String> columns = new ArrayList<>();
				for (Pair<String, String> column : table.getValue1()) {
					// quoted as the server quotes them: H2 2.x reserves words such as VALUE
					String name = column.getValue0();
					columns.add((queryUtil.isSelectorKeyword(name) ? queryUtil.getEscapeKeyword(name) : name) + " "
							+ column.getValue1());
				}
				statement.execute("CREATE TABLE " + table.getValue0() + " (" + String.join(", ", columns) + ")");
			}
		}
		AccessToken token = new AccessToken();
		token.setId("owner-" + UUID.randomUUID());
		token.setName("Owner");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		Pair<String, String> owner = User.getPrimaryUserIdAndTypePair(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
	}

	@AfterEach
	void tearDown() throws Exception {
		registry.close();
		connection.close();
	}

	// ---- delete, merge and undo ----

	@Test
	void undoPutsBackTheRulesOfADeletedOrMergedTopic() throws Exception {
		topic("t-a", "Acme", "active");
		topic("t-b", "Beta", "active");
		sql("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, DISPLAY_NAME) VALUES (?, ?, 'p-1', 'Pat')",
				ownerId, ownerType);
		// the owner ignores Pat on Acme, saved with Pat's address: a rule's VALUE, which H2 2.x reserves, is in the
		// snapshot only when it is set
		sql("INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, \"VALUE\", TOPIC_ID, PERSON_ID, CREATED_AT) "
				+ "VALUES (?, ?, 'r-1', 'exclude_topic', 'pat@test.com', 't-a', 'p-1', CURRENT_TIMESTAMP)", ownerId,
				ownerType);

		BrainTopicChangeUtils.undo(user, (String) BrainTopicUtils.deleteTopic(user, "t-a").get("changeId"));
		assertEquals("pat@test.com", value("SELECT \"VALUE\" FROM BRAIN_RULE WHERE TOPIC_ID = 't-a'"));

		BrainTopicChangeUtils.undo(user, (String) BrainTopicUtils.mergeTopics(user, "t-a", "t-b").get("changeId"));
		assertEquals("pat@test.com", value("SELECT \"VALUE\" FROM BRAIN_RULE WHERE TOPIC_ID = 't-a'"));
	}

	@Test
	void openTopicQuestionsOfferTheMergedTopicAndDropADeletedOne() throws Exception {
		topic("t-a", "Acme", "active");
		topic("t-b", "Beta", "active");
		topic("t-c", "Cobalt", "active");
		choice("q-ac", "t-a", "t-c");
		choice("q-ab", "t-a", "t-b");
		choice("q-c", "t-c");

		BrainTopicUtils.mergeTopics(user, "t-a", "t-b");
		assertEquals(List.of("t-b", "t-c"), candidates("q-ac"));
		// named for the topic left, not by the id of the one gone
		assertEquals(List.of("Both", "Beta", "Ignore"), BrainReviewUtils.getReview(user, "q-ab").get("actions"));

		BrainTopicUtils.deleteTopic(user, "t-c");
		assertEquals(List.of("t-b"), candidates("q-ac"));
		assertEquals(BrainReviewUtils.OPEN, BrainReviewUtils.getReview(user, "q-ac").get("status"));
		// a question about the deleted topic alone no longer applies
		assertEquals(BrainReviewUtils.DISMISSED, BrainReviewUtils.getReview(user, "q-c").get("status"));
	}

	// ---- topic setup ----

	@Test
	void applyingSetupAgainLeavesATopicTheOwnerArchived() throws Exception {
		topic("t-old", "Old project", "archived");
		topic("t-sugg", "Suggested", "suggested");
		review(setupTopic("t-old", "t-old", "Old project", "", "", ""),
				setupTopic("t-sugg", "t-sugg", "Suggested", "", "", ""),
				setupTopic("added-new", null, "New work", "", "", ""));

		apply();

		assertEquals(BrainTopicUtils.ARCHIVED, value("SELECT STATUS FROM BRAIN_TOPIC WHERE TOPIC_ID = 't-old'"));
		// keeping a suggested topic still accepts it, and a new one starts active
		assertEquals(BrainTopicUtils.ACTIVE, value("SELECT STATUS FROM BRAIN_TOPIC WHERE TOPIC_ID = 't-sugg'"));
		assertEquals(BrainTopicUtils.ACTIVE, value("SELECT STATUS FROM BRAIN_TOPIC WHERE NAME = 'New work'"));
	}

	@Test
	void aTopicIsSavedTrimmedTheWayTheSetupPageChecksIt() throws Exception {
		review(setupTopic("added-beta", null, NBSP + "Beta" + NBSP, NBSP, "About beta" + NBSP,
				NBSP + "UNC" + NBSP + "\nVCU" + NBSP));

		Map<String, Object> receipt = receipts(apply()).get(0);

		assertEquals("Beta", receipt.get("name"));
		// a short label of only a no-break space is blank, so it is the name
		assertEquals("Beta", receipt.get("short"));
		assertEquals("About beta", receipt.get("description"));
		assertEquals(List.of("UNC", "VCU"), receipt.get("keywords"));
	}

	@Test
	void theTopicMapIsBuiltForATopicFromAnOutlookFolder() throws Exception {
		topic("t-sugg", "Suggested", "suggested");
		Map<String, Object> suggestion = folderTopic("t-sugg", "t-sugg");
		suggestion.put("threadIds", List.of("th-1"));
		try (MockedStatic<BrainTopicSuggest> suggest = mockStatic(BrainTopicSuggest.class);
				MockedStatic<BrainTopicModel> model = mockStatic(BrainTopicModel.class);
				MockedStatic<CollaborationJobUtils> jobs = mockStatic(CollaborationJobUtils.class)) {
			suggest.when(() -> BrainTopicSuggest.topics(any(), anyBoolean(), any()))
					.thenReturn(Map.of("topics", List.of(suggestion)));
			// the job's work, run here instead of on the job pool
			jobs.when(() -> CollaborationJobUtils.start(anyString(), anyString(), anyString(), anyMap(), any()))
					.thenAnswer(call -> {
						call.<CollaborationJobUtils.Work>getArgument(4).run(mock(CollaborationJobUtils.Job.class));
						return Map.of();
					});
			BrainTopicReviewUtils.startMap(user);
		}

		assertEquals(1L, value("SELECT COUNT(*) FROM BRAIN_TOPIC_REVIEW"));
	}

	@Test
	void aTopicFromAnOutlookFolderIsSavedAndApplied() throws Exception {
		Map<String, Object> topic = folderTopic("added-proj", null);
		review(topic);

		// the page sends the folder choice back with each draft save
		BrainTopicReviewUtils.save(user, REVIEW, 1, Map.of("topics", List.of(topic)));
		apply(2);

		assertEquals(FOLDER_ID, value("SELECT FOLDER_ID FROM BRAIN_TOPIC_FOLDER"));
	}

	@Test
	void aFailedSuggestionIsShownOnceBeforeItIsMadeAgain() throws Exception {
		AtomicInteger started = new AtomicInteger();
		try (MockedStatic<BrainTopicSuggest> suggest = mockStatic(BrainTopicSuggest.class);
				MockedStatic<CollaborationJobUtils> jobs = mockStatic(CollaborationJobUtils.class)) {
			suggest.when(() -> BrainTopicSuggest.topics(any(), anyBoolean(), any()))
					.thenReturn(Map.of("topics", List.of(), "modelError", "No topic model is set up"));
			// each job runs its work at once, so it is done by the time the page asks again
			jobs.when(() -> CollaborationJobUtils.start(anyString(), anyString(), anyString(), anyMap(), any()))
					.thenAnswer(call -> {
						started.incrementAndGet();
						call.<CollaborationJobUtils.Work>getArgument(4).run(mock(CollaborationJobUtils.Job.class));
						return Map.of();
					});

			// the page starts the map and asks again when its job ends: the failed draft comes back with its reason
			assertEquals(false, BrainTopicReviewUtils.start(user).get("exists"));
			assertEquals(true, BrainTopicReviewUtils.start(user).get("exists"));
			assertEquals(1, started.get());
			// Retry makes it again
			assertEquals(false, BrainTopicReviewUtils.start(user).get("exists"));
			assertEquals(2, started.get());
		}
	}

	@Test
	void aTopicNameLongerThanFiftyCharactersIsApplied() throws Exception {
		// the topic model may name a topic in up to 60 characters, and its short name defaults to the name
		String name = "International Pharmaceutical Manufacturing Compliance Audits";
		review(setupTopic("added-long", null, name, "", "", ""));

		apply();

		assertEquals(name, value("SELECT SHORT_NAME FROM BRAIN_TOPIC WHERE NAME = '" + name + "'"));
	}

	@Test
	void anEmptyModelReplyIsAnUnusableAnswer() {
		Map<String, Object> context = Map.of("topics", List.of(), "candidatePeople", List.of());
		for (String reply : new String[] { "", "<think>checking</think>", "null" }) {
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> BrainTopicReviewChat.run(context, (prompt, instructions, params) -> reply));
			assertTrue(error.getMessage().contains("did not return a usable answer"), reply);
		}
	}

	// ---- helpers ----

	private void topic(String topicId, String name, String status) throws Exception {
		sql("INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, SHORT_NAME, STATUS, CREATED_AT, UPDATED_AT) "
				+ "VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", ownerId, ownerType, topicId, name,
				name, status);
	}

	// an open "which topic?" question, as the classifier asks it about a conversation
	private void choice(String reviewId, String... candidates) throws Exception {
		sql("INSERT INTO BRAIN_REVIEW (OWNER_ID, OWNER_TYPE, REVIEW_ID, KIND, REF_TYPE, REF_ID, DETAIL_JSON, STATUS, "
				+ "CREATED_AT) VALUES (?, ?, ?, ?, 'thread', ?, ?, 'open', CURRENT_TIMESTAMP)", ownerId, ownerType,
				reviewId, BrainReviewUtils.TOPIC_CHOICE, "thread-" + reviewId,
				CollaborationDbUtils.toJson(Map.of("candidates", List.of(candidates))));
	}

	private Object candidates(String reviewId) {
		return ((Map<?, ?>) BrainReviewUtils.getReview(user, reviewId).get("data")).get("candidates");
	}

	private static Map<String, Object> setupTopic(String key, String id, String name, String shortName,
			String description, String terms) {
		Map<String, Object> topic = new LinkedHashMap<>();
		topic.put("key", key);
		topic.put("id", id);
		topic.put("name", name);
		topic.put("short", shortName);
		topic.put("description", description);
		topic.put("terms", terms);
		topic.put("keep", true);
		return topic;
	}

	// a draft topic whose mail sits in one of the owner's Outlook folders
	private static Map<String, Object> folderTopic(String key, String id) {
		Map<String, Object> topic = setupTopic(key, id, "Project", "", "", "");
		topic.put("folderIds", List.of(FOLDER_ID));
		topic.put("folders", List.of(FOLDER_PATH));
		topic.put("fileFromFolder", true);
		return topic;
	}

	@SafeVarargs
	private void review(Map<String, Object>... topics) throws Exception {
		sql("INSERT INTO BRAIN_TOPIC_REVIEW (OWNER_ID, OWNER_TYPE, REVIEW_ID, REVISION, DRAFT_JSON, CREATED_AT, "
				+ "UPDATED_AT) VALUES (?, ?, ?, 1, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", ownerId, ownerType, REVIEW,
				CollaborationDbUtils.toJson(Map.of("topics", List.of(topics))));
	}

	private Map<String, Object> apply() {
		return apply(1);
	}

	private Map<String, Object> apply(int revision) {
		// the filing job the apply starts reads mail with a model; here it only has to answer for this review
		try (MockedStatic<BrainThreadClassifier> classifier = mockStatic(BrainThreadClassifier.class)) {
			classifier.when(() -> BrainThreadClassifier.startTopics(any(), anyMap())).thenReturn(Map.of("id", "job-1",
					"params", Map.of("mode", "topics", "reviewId", REVIEW, "reviewRevision", revision)));
			return BrainTopicReviewUtils.apply(user, REVIEW, revision, false);
		}
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> receipts(Map<String, Object> applied) {
		Map<String, Object> review = (Map<String, Object>) applied.get("review");
		return (List<Map<String, Object>>) ((Map<String, Object>) review.get("result")).get("topics");
	}

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}

	private Object value(String sql) throws Exception {
		try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
			return rs.next() ? rs.getObject(1) : null;
		}
	}
}
