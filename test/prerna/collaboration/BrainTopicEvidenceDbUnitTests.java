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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.collaboration.BrainClassifier.TopicOption;
import prerna.collaboration.BrainTopicEvidence.Evidence;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * What the classifier is shown as filed under each topic, against an in-memory H2 Collaboration schema: which
 * conversations stand as a topic's examples, in what order, and where a conversation the owner took off a topic
 * goes.
 */
class BrainTopicEvidenceDbUnitTests {

	private static final String A = "topic-a";
	private static final String B = "topic-b";
	private static final String OTHER = BrainThreadClassifier.OTHER_TOPIC;
	private static final String OWNER_ID = "owner-1";
	private static final String OWNER_TYPE = "NATIVE";
	private static final String JEV = "jev-v2:engine";

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_topic_evidence_" + UUID.randomUUID());
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
	}

	@AfterEach
	void tearDown() throws Exception {
		registry.close();
		connection.close();
	}

	@Test
	void theOwnersFilingComesFirstThenAFoldersThenBrainsSurestAndNewest() throws Exception {
		thread("th-low", "Brain low");
		link("th-low", A, "confirmed", 86, JEV);
		thread("th-older", "Brain older", "2026-11-01 09:00:00");
		link("th-older", A, "confirmed", 95, JEV);
		thread("th-newer", "Brain newer", "2026-11-02 09:00:00");
		link("th-newer", A, "confirmed", 95, JEV);
		thread("th-folder", "Folder filed");
		link("th-folder", A, "confirmed", 100, BrainMailFolders.FILED);
		thread("th-owner", "Owner filed");
		link("th-owner", A, "you", 100, null);

		Evidence evidence = load(options(A, B));

		assertEquals(List.of("Owner filed", "Folder filed", "Brain newer", "Brain older", "Brain low"),
				subjects(evidence, A));
		assertEquals(List.of(), subjects(evidence, B));
		assertEquals("About topic-a Threads such as: Owner filed; Folder filed; Brain newer; Brain older; Brain low.",
				describe(evidence, options(A, B), null, A));
		// a topic with nothing filed is offered as it was
		List<TopicOption> topics = options(A, B);
		assertSame(topics.get(1), BrainTopicEvidence.describe(topics, evidence, null).get(1));
	}

	@Test
	void whatCannotStandAsAnExampleIsLeftOut() throws Exception {
		thread("th-asked", "Only asked about");
		link("th-asked", A, "suggested", 75, JEV);
		thread("th-muted", "Muted thread");
		sql("UPDATE BRAIN_THREAD SET MUTED = TRUE WHERE THREAD_ID = 'th-muted'");
		link("th-muted", A, "you", 100, null);
		thread("th-auto", "Automated digest");
		sql("UPDATE BRAIN_THREAD SET AUTOMATED = TRUE WHERE THREAD_ID = 'th-auto'");
		link("th-auto", A, "confirmed", 99, JEV);
		thread("th-null", null);
		link("th-null", A, "you", 100, null);
		thread("th-blank", "   ");
		link("th-blank", A, "you", 100, null);
		// a chat named only by who is in it says nothing about a topic
		thread("th-chat", BrainGraphHeaderSource.CHAT_PREFIX + "Ann, Bob");
		sql("UPDATE BRAIN_THREAD SET SOURCE = 'teams' WHERE THREAD_ID = 'th-chat'");
		link("th-chat", A, "you", 100, null);
		// the owner never wants this word read
		thread("th-secret", "Salary review for Q4");
		link("th-secret", A, "you", 100, null);
		thread("th-kept", "Kept");
		link("th-kept", A, "confirmed", 90, JEV);

		Evidence evidence = BrainTopicEvidence.load(OWNER_ID, OWNER_TYPE, options(A),
				List.of(new BrainRulesGate.Rule("rule-1", "never_keyword", "salary", null, null, null)));

		assertEquals(List.of("Kept"), subjects(evidence, A));
		assertEquals(List.of(), subjects(evidence, OTHER));
	}

	@Test
	void aSubjectIsListedOnceAndAThreadIsNeverItsOwnExample() throws Exception {
		thread("th-1", "Budget review");
		link("th-1", A, "you", 100, null);
		thread("th-2", "budget REVIEW");
		link("th-2", A, "confirmed", 90, JEV);
		thread("th-3", "Vendor contract");
		link("th-3", A, "confirmed", 88, JEV);

		Evidence evidence = load(options(A));

		assertEquals(List.of("Budget review", "Vendor contract"), subjects(evidence, A));
		// a chat, or a thread filed nowhere: every example
		assertEquals("About topic-a Threads such as: Budget review; Vendor contract.",
				describe(evidence, options(A), null, A));
		// scoring the only thread with a subject leaves that subject out, or the thread would confirm itself
		assertEquals("About topic-a Threads such as: Budget review.", describe(evidence, options(A), "th-3", A));
		// a subject two threads carry stays: the other thread still stands for it
		assertEquals("About topic-a Threads such as: Budget review; Vendor contract.",
				describe(evidence, options(A), "th-1", A));
	}

	@Test
	void aTopicShortOfExamplesShowsTheThreadsItWasSetUpFrom() throws Exception {
		thread("s1", "Seed one");
		thread("s2", "Seed the owner took out");
		reject("s2", A);
		thread("s3", "Seed filed elsewhere");
		link("s3", B, "confirmed", 92, JEV);
		thread("s4", "Seed four");
		thread("s5", "Seed five");
		sql("INSERT INTO BRAIN_TOPIC_REVIEW (OWNER_ID, OWNER_TYPE, REVIEW_ID, REVISION, DRAFT_JSON) VALUES (?, ?, "
				+ "'review-1', 3, ?)", OWNER_ID, OWNER_TYPE, "{\"topics\":[{\"id\":\"" + A
						+ "\",\"threadIds\":[\"s1\",\"s2\",\"s3\",\"s4\",\"gone\"]},{\"id\":\"" + B
						+ "\",\"threadIds\":[\"s5\"]},{\"key\":\"skipped\",\"threadIds\":[\"s4\"]}]}");

		// a draft the owner has not applied says nothing yet
		assertEquals(List.of(), subjects(load(options(A, B)), A));

		sql("UPDATE BRAIN_TOPIC_REVIEW SET APPLIED_REVISION = 3");
		Evidence evidence = load(options(A, B));

		// right after onboarding nothing is filed yet: the group the topic was made from is what it shows
		assertEquals(List.of("Seed one", "Seed four"), subjects(evidence, A));
		// what is filed comes before the group
		assertEquals(List.of("Seed filed elsewhere", "Seed five"), subjects(evidence, B));
	}

	@Test
	void aConversationTakenOffATopicAndLeftUnfiledIsAnExampleOfTheWayOut() throws Exception {
		thread("th-unfiled", "Lunch order");
		reject("th-unfiled", A);
		thread("th-moved", "Moved to the other topic");
		reject("th-moved", A);
		link("th-moved", B, "you", 100, null);
		thread("th-asked", "Still being asked about");
		reject("th-asked", A);
		link("th-asked", B, "suggested", 74, JEV);

		List<TopicOption> topics = options(A, B);
		Evidence evidence = load(topics);

		assertEquals(List.of("Lunch order"), subjects(evidence, OTHER));
		assertEquals(List.of("Moved to the other topic"), subjects(evidence, B));
		// nothing is written into the topic it was taken off: a model would read that as a match
		assertEquals(List.of(), subjects(evidence, A));
		assertEquals("About topic-a", describe(evidence, topics, null, A));
		assertEquals("Not about the other topics. Threads such as: Lunch order.",
				describe(evidence, topics, null, OTHER));
	}

	@Test
	void theWayOutLeavesATopicsSubjectToTheTopicAndADescriptionIsNotRepeated() throws Exception {
		thread("th-a", "Team offsite");
		link("th-a", A, "you", 100, null);
		thread("th-out", "team offsite");
		reject("th-out", B);
		thread("th-named", "Quarterly numbers");
		link("th-named", B, "you", 100, null);

		List<TopicOption> topics = List.of(new TopicOption(A, A, "About topic-a"),
				new TopicOption(B, B, "Threads such as: Quarterly Numbers; Forecast."),
				new TopicOption(OTHER, "Something else", "Not about the other topics."));
		List<TopicOption> described = BrainTopicEvidence.describe(topics, load(topics), null);

		assertEquals("About topic-a Threads such as: Team offsite.", described.get(0).description());
		assertEquals("Threads such as: Quarterly Numbers; Forecast.", described.get(1).description());
		assertEquals("Not about the other topics.", described.get(2).description());
	}

	@Test
	void aTopicShowsFewerExamplesAsTheTopicListGrows() throws Exception {
		for (int i = 0; i < 12; i++) {
			thread("th-" + i, "Subject " + i);
			link("th-" + i, "topic-0", "confirmed", 99 - i, JEV);
		}

		assertEquals(8, load(numbered(25)).each());
		assertEquals(3, load(numbered(70)).each());
		Evidence evidence = load(numbered(26));
		assertEquals(7, evidence.each());
		// one spare is kept, so the topic still shows seven when the scored thread is one of them
		assertEquals(8, subjects(evidence, "topic-0").size());
		assertEquals("About topic-0 Threads such as: Subject 0; Subject 1; Subject 2; Subject 3; Subject 4; Subject 5; "
				+ "Subject 6.", describe(evidence, numbered(26), null, "topic-0"));
		assertEquals("About topic-0 Threads such as: Subject 0; Subject 1; Subject 3; Subject 4; Subject 5; Subject 6; "
				+ "Subject 7.", describe(evidence, numbered(26), "th-2", "topic-0"));
	}

	@Test
	void anOwnerWithNoTopicsHasNothingToShow() {
		List<TopicOption> none = List.of();
		Evidence evidence = load(none);
		assertEquals(0, evidence.each());
		assertSame(none, BrainTopicEvidence.describe(none, evidence, "th-1"));
	}

	// ---- helpers ----

	private static Evidence load(List<TopicOption> topics) {
		return BrainTopicEvidence.load(OWNER_ID, OWNER_TYPE, topics, List.of());
	}

	// the topics offered: each named and described by its id, then the way out
	private static List<TopicOption> options(String... topicIds) {
		List<TopicOption> topics = new ArrayList<>();
		for (String id : topicIds) {
			topics.add(new TopicOption(id, id, "About " + id));
		}
		topics.add(new TopicOption(OTHER, "Something else", "Not about the other topics."));
		return topics;
	}

	private static List<TopicOption> numbered(int count) {
		String[] ids = new String[count];
		for (int i = 0; i < count; i++) {
			ids[i] = "topic-" + i;
		}
		return options(ids);
	}

	private static List<String> subjects(Evidence evidence, String topicId) {
		return evidence.examples().getOrDefault(topicId, List.of()).stream().map(BrainTopicEvidence.Example::subject)
				.toList();
	}

	// one topic's text as the classifier gets it when it scores threadId (null for a chat)
	private static String describe(Evidence evidence, List<TopicOption> topics, String threadId, String topicId) {
		return BrainTopicEvidence.describe(topics, evidence, threadId).stream()
				.filter(topic -> topicId.equals(topic.id())).findFirst().orElseThrow().description();
	}

	private void thread(String threadId, String subject) throws Exception {
		thread(threadId, subject, "2026-11-01 08:00:00");
	}

	private void thread(String threadId, String subject, String lastMessageAt) throws Exception {
		sql("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, SOURCE, SUBJECT, LAST_MESSAGE_AT) "
				+ "VALUES (?, ?, ?, 'email', ?, ?)", OWNER_ID, OWNER_TYPE, threadId, subject,
				Timestamp.valueOf(lastMessageAt));
	}

	private void link(String threadId, String topicId, String source, int confidence, String version)
			throws Exception {
		sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE, CONFIDENCE, "
				+ "CLASSIFIER_VERSION, CHANGED_BY) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", OWNER_ID, OWNER_TYPE, threadId,
				topicId, source, confidence, version, "you".equals(source) ? "you" : "brain");
	}

	private void reject(String threadId, String topicId) throws Exception {
		sql("INSERT INTO BRAIN_THREAD_TOPIC_REJECTION (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, CHANGED_BY, "
				+ "CHANGED_AT) VALUES (?, ?, ?, ?, 'you', ?)", OWNER_ID, OWNER_TYPE, threadId, topicId,
				Timestamp.valueOf("2026-11-03 08:00:00"));
	}

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				ps.setObject(i + 1, params[i]);
			}
			ps.executeUpdate();
		}
	}
}
