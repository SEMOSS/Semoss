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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.om.Insight;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * How the classifier files a thread or asks about its topic, against an in-memory H2 Collaboration schema: the links
 * it leaves and the question the owner is shown. The model's scores are given; with the default cutoffs and the
 * "Something else" choice, a best topic at 70 to 84 is asked about and one at 85 or more is filed.
 */
class BrainThreadClassifierDbUnitTests {

	private static final String A = "topic-a";
	private static final String B = "topic-b";
	private static final String C = "topic-c";
	private static final String THREAD = "th-1";

	private static final BrainClassifier CLASSIFIER = new BrainClassifier() {
		@Override
		public String version() {
			return "test-v1";
		}

		@Override
		public Scores score(ThreadInput thread, List<TopicOption> topics, Insight insight) {
			throw new UnsupportedOperationException("the tests give the scores");
		}
	};

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private User user;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_classifier_" + UUID.randomUUID());
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
		for (String topic : List.of(A, B, C)) {
			sql("INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, STATUS) VALUES (?, ?, ?, ?, 'active')",
					ownerId, ownerType, topic, topic);
		}
		sql("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, SOURCE, SUBJECT) VALUES (?, ?, ?, 'email', "
				+ "'Q4 budget')", ownerId, ownerType, THREAD);
	}

	@AfterEach
	void tearDown() throws Exception {
		registry.close();
		connection.close();
	}

	@Test
	void aConfidentFilingReplacesBrainsGuessesAndClosesTheirQuestion() throws Exception {
		file(A, 0.75, B, 0.15);
		assertEquals(List.of(List.of(A, "suggested", true), List.of(B, "suggested", false)), links());
		assertEquals(List.of(List.of(A, B)), openQuestions());

		file(C, 0.9, A, 0.05);
		assertEquals(List.of(List.of(C, "confirmed", true)), links());
		assertEquals(List.of(), openQuestions());

		// unsure again later: the question Brain closed itself is asked again
		file(A, 0.75, B, 0.15);
		assertEquals(List.of(List.of(A, B)), openQuestions());
	}

	@Test
	void aQuestionAboutOtherTopicsReplacesTheOpenOne() throws Exception {
		file(A, 0.75, B, 0.15);
		file(C, 0.75, B, 0.15);
		assertEquals(List.of(List.of(C, B)), openQuestions());
	}

	@Test
	void theRunnerUpIsLinkedWithItsOwnConfidence() throws Exception {
		file(A, 0.75, B, 0.15);
		assertEquals(Map.of(A, 75, B, 15), confidences());
	}

	@Test
	void aConfidentPickWithNoneOfTheTopicsPeopleOnTheThreadIsAsked() throws Exception {
		person(A, "p-ana");
		file(A, 0.95, B, 0.03);
		assertEquals(List.of(List.of(A, "suggested", true), List.of(B, "suggested", false)), links());
		assertEquals(List.of(List.of(A, B)), openQuestions());

		// one of the topic's people on the thread: filed as before
		sql("INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID, OWNER_TYPE, THREAD_ID, PERSON_ID) VALUES (?, ?, ?, ?)",
				ownerId, ownerType, THREAD, "p-ana");
		file(A, 0.95, B, 0.03);
		assertEquals(List.of(List.of(A, "confirmed", true)), links());
		assertEquals(List.of(), openQuestions());
	}

	@Test
	void aTopicWithNoPeopleFilesOnTheModelAlone() throws Exception {
		person(B, "p-ben");
		file(A, 0.95, B, 0.03);
		assertEquals(List.of(List.of(A, "confirmed", true)), links());
	}

	@Test
	void aFilingNeverTouchesAThreadTheOwnerFiled() throws Exception {
		sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE, CONFIDENCE, IS_PRIMARY, "
				+ "CHANGED_BY) VALUES (?, ?, ?, ?, 'you', 100, TRUE, 'you')", ownerId, ownerType, THREAD, A);
		sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE, CONFIDENCE, IS_PRIMARY, "
				+ "CLASSIFIER_VERSION, CHANGED_BY) VALUES (?, ?, ?, ?, 'suggested', 75, FALSE, 'test-v1', 'brain')",
				ownerId, ownerType, THREAD, B);
		file(C, 0.9, A, 0.05);
		assertEquals(List.of(List.of(A, "you", true), List.of(B, "suggested", false)), links());
	}

	@Test
	void aThreadThatCannotBeFiledByItsFolderDoesNotStopTheRest() throws Exception {
		sql("INSERT INTO BRAIN_TOPIC_FOLDER (OWNER_ID, OWNER_TYPE, TOPIC_ID, FOLDER_ID) VALUES (?, ?, ?, 'folder-1')",
				ownerId, ownerType, A);
		// mail in the folder on two threads; the first has no thread row, so its link fails
		for (String thread : List.of("th-gone", THREAD)) {
			sql("INSERT INTO BRAIN_MESSAGE (OWNER_ID, OWNER_TYPE, MESSAGE_KEY, THREAD_ID, FOLDER, RECEIVED_AT) VALUES "
					+ "(?, ?, ?, ?, 'folder-1', CURRENT_TIMESTAMP)", ownerId, ownerType, "m-" + thread, thread);
		}

		assertEquals(1, BrainThreadClassifier.fileByFolder(ownerId, ownerType, List.of("th-gone", THREAD)));

		assertEquals(List.of(List.of(A, "confirmed", true)), links());
	}

	@Test
	void anOwnersClosingNoteIsNotWaitingButTheirQuestionIs() throws Exception {
		// the model reads both threads: "Thanks, done" is an update with nothing to do, the question is not
		assertEquals("fyi", ownerWroteLast("th-done", "Thanks, done - the PO is approved.", 0.95));
		assertEquals("waiting", ownerWroteLast("th-ask", "Can you send me the signed PO by Friday?", 0.1));
		assertEquals(List.of("waiting_on", "fyi"), askTypes());
	}

	private List<String> askTypes() throws Exception {
		List<String> out = new ArrayList<>();
		try (Statement statement = connection.createStatement();
				ResultSet rs = statement.executeQuery("SELECT ASK_TYPE FROM WORK_ITEM ORDER BY THREAD_ID")) {
			while (rs.next()) {
				out.add(rs.getString(1));
			}
		}
		return out;
	}

	// the verdict for a thread whose only message is the owner's, with the model giving it this fyi score
	@SuppressWarnings("unchecked")
	private String ownerWroteLast(String threadId, String text, double fyi) throws Exception {
		sql("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, DISPLAY_NAME, EMAIL_NORM) VALUES (?, ?, 'self', "
				+ "'Owner', 'owner@test.com')", ownerId, ownerType);
		sql("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, THREAD_KEY, SOURCE, SUBJECT, MUTED, AUTOMATED) "
				+ "VALUES (?, ?, ?, ?, 'email', 'PO', FALSE, FALSE)", ownerId, ownerType, threadId, "email:" + threadId);
		BrainClassifier classifier = new BrainClassifier() {
			@Override
			public String version() {
				return "test-v1";
			}

			@Override
			public Scores score(ThreadInput thread, List<TopicOption> topics, Insight insight) {
				return new Scores(Map.of(), fyi, 0, 0.5, Map.of());
			}
		};
		Map<String, Object> message = new LinkedHashMap<>();
		message.put("id", "g-" + threadId);
		message.put("at", "2026-11-05T10:00:00Z");
		message.put("text", text);
		message.put("fromId", "self");
		message.put("fromName", "Owner");
		message.put("to", List.of());
		message.put("cc", List.of());
		try (MockedStatic<BrainThreadMessages> messages = mockStatic(BrainThreadMessages.class)) {
			messages.when(() -> BrainThreadMessages.read(any(), anyString(), anyString(), anyString(), any(), any()))
					.thenReturn(Map.of("messages", List.of(message)));
			Class<?> context = Class.forName(BrainThreadClassifier.class.getName() + "$Context");
			Map<String, Object> extra = new LinkedHashMap<>();
			extra.put("classifier", classifier);
			extra.put("cutoffs", classifier.cutoffs());
			extra.put("topics", List.of());
			extra.put("self", new BrainThreadClassifier.Self("self", "Owner", Set.of("owner@test.com")));
			extra.put("window", BrainThreadClassifier.window((Object) null));
			extra.put("timeZone", java.time.ZoneOffset.UTC);
			extra.put("vips", Set.of());
			extra.put("topicVips", Map.of());
			extra.put("rules", List.of());
			extra.put("followed", Set.of());
			extra.put("automatedSenders", Set.of());
			extra.put("scored", new java.util.concurrent.ConcurrentHashMap<String, BrainClassifier.Scores>());
			extra.put("linkedFolders", Map.of());
			extra.put("ownOrg", BrainOrgDomains.of(List.of(), "test.com"));
			extra.put("partners", new java.util.concurrent.ConcurrentHashMap<String, Boolean>());
			Method classify = BrainThreadClassifier.class.getDeclaredMethod("classifyOne", context, String.class);
			classify.setAccessible(true);
			return ((BrainThreadClassifier.Result) classify.invoke(null, context(context, extra), threadId)).work();
		}
	}

	// ---- helpers ----

	@Test
	void aNoticeFromTheOwnersOrganisationThatNeedsThemRaisesOneItemPerTicketAndAClosingNoticeFinishesIt()
			throws Exception {
		sql("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, DISPLAY_NAME, EMAIL_NORM) VALUES (?, ?, 'desk', "
				+ "'EHS System', 'ehs-system@test.com')", ownerId, ownerType);
		// assigned, then a reminder in its own thread: the same ticket, so still one item
		assertTrue(notice("assigned", "Corrective Action CA-318 Assigned to You", "desk", 0.9));
		assertTrue(notice("reminder", "Escalation: CA-318 is overdue", "desk", 0.9));
		assertEquals(List.of(List.of("notice:CA-318", "errand", "open")), items());
		// an approval with no ticket id is its own item
		assertTrue(notice("timesheet", "Timesheet awaiting your approval - Marco Bellini", "desk", 0.8));
		assertEquals(2, items().size());
		// a status notice raises nothing, and one that says the ticket is closed finishes its item
		assertFalse(notice("update", "CA-318 photo evidence received", "desk", 0.1));
		assertEquals("open", items().get(0).get(2));
		assertFalse(notice("closed", "Corrective Action CA-318 Closed", "desk", 0.1));
		assertEquals(List.of("notice:CA-318", "errand", "done"), items().get(0));
		// each notice is asked about once: a second run over the same mail does not call the model again
		assertFalse(noticeAgain("assigned", "Corrective Action CA-318 Assigned to You"));
	}

	@Test
	void aNoticeFromOutsideTheOwnersOrganisationIsNeverAskedAbout() throws Exception {
		sql("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, DISPLAY_NAME, EMAIL_NORM) VALUES (?, ?, 'spam', "
				+ "'IT Helpdesk', 'it-helpdesk@elsewhere.example')", ownerId, ownerType);
		// the classifier would say it needs the owner ("verify your mailbox"): it is not asked
		assertFalse(notice("phish", "Verify Your Mailbox to Avoid Suspension", "spam", null));
		assertTrue(items().isEmpty());
	}

	@Test
	void aNoticeFromAnOrganisationTheOwnerWorksWithIsAskedAbout() throws Exception {
		sql("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, DISPLAY_NAME, EMAIL_NORM, RELATIONSHIP) VALUES "
				+ "(?, ?, 'portal', 'Vendor Portal', 'no-reply@portal.adatum.com', 'automated')", ownerId, ownerType);
		sql("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, DISPLAY_NAME, EMAIL_NORM, RELATIONSHIP) VALUES "
				+ "(?, ?, 'kira', 'Kira Holt', 'kira@adatum.com', 'external')", ownerId, ownerType);
		// mail from someone there is not enough: the owner has not written to them
		sql("INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID, OWNER_TYPE, THREAD_ID, PERSON_ID) VALUES (?, ?, 'talk', "
				+ "'kira')", ownerId, ownerType);
		assertFalse(notice("early", "Ticket VP-11 assigned to you", "portal", null));
		// the owner wrote on a thread Kira is on: the organisation's portal is asked about from then on
		sql("INSERT INTO BRAIN_MESSAGE (OWNER_ID, OWNER_TYPE, MESSAGE_KEY, THREAD_ID, GRAPH_ID, SENDER_PERSON_ID, "
				+ "RECEIVED_AT) VALUES (?, ?, 'key-talk', 'talk', 'g-talk', 'self', ?)", ownerId, ownerType,
				java.sql.Timestamp.valueOf("2026-11-04 10:00:00"));
		assertTrue(notice("later", "Ticket VP-12 assigned to you", "portal", 0.9));
		assertEquals(List.of(List.of("notice:VP-12", "errand", "open")), items());
	}

	@Test
	void aNoticeFromAVipIsAskedAboutWhereverItComesFrom() throws Exception {
		sql("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, DISPLAY_NAME, EMAIL_NORM) VALUES (?, ?, 'desk', "
				+ "'Client Desk', 'desk@client.example')", ownerId, ownerType);
		vips = Set.of("desk");
		assertTrue(notice("vip", "Change request CR-7 needs your approval", "desk", 0.9));
	}

	@Test
	void acceptingANewTopicFoundInIncomingMailKeepsItAndFilesItsConversations() throws Exception {
		String review = proposal("t-new", "Harbour crane inspection");
		BrainReviewUtils.resolveReview(ownerId, ownerType, review, "Accept", null, BrainProfileUtils.YOU);
		assertEquals("active", value("SELECT STATUS FROM BRAIN_TOPIC WHERE TOPIC_ID = 't-new'"));
		assertEquals("member", value("SELECT STATE FROM BRAIN_TOPIC_PERSON WHERE TOPIC_ID = 't-new'"));
		assertEquals(List.of(List.of("t-new", "confirmed", true)), links());
	}

	@Test
	void acceptingANewTopicTheScreenAlreadyKeptStillFilesItsConversations() throws Exception {
		String review = proposal("t-new", "Harbour crane inspection");
		// the owner's screen saves the topic as active before it answers the question
		sql("UPDATE BRAIN_TOPIC SET STATUS = 'active' WHERE TOPIC_ID = 't-new'");
		BrainReviewUtils.resolveReview(ownerId, ownerType, review, "Accept", null, BrainProfileUtils.YOU);
		assertEquals("member", value("SELECT STATE FROM BRAIN_TOPIC_PERSON WHERE TOPIC_ID = 't-new'"));
		assertEquals(List.of(List.of("t-new", "confirmed", true)), links());
	}

	@Test
	void dismissingANewTopicRemovesItAndFilesNothing() throws Exception {
		String review = proposal("t-new", "Harbour crane inspection");
		BrainReviewUtils.resolveReview(ownerId, ownerType, review, "dismiss", null, BrainProfileUtils.YOU);
		assertEquals(null, value("SELECT STATUS FROM BRAIN_TOPIC WHERE TOPIC_ID = 't-new'"));
		assertTrue(links().isEmpty());
		// the answered question stays: it is what keeps the same group from being asked about again
		assertEquals("dismissed", value("SELECT STATUS FROM BRAIN_REVIEW WHERE REVIEW_ID = '" + review + "'"));
	}

	// a suggested topic with one suggested person and the question about it, as a look at incoming mail leaves them
	private String proposal(String topicId, String name) throws Exception {
		sql("INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, STATUS, ORIGIN) VALUES (?, ?, ?, ?, "
				+ "'suggested', 'brain')", ownerId, ownerType, topicId, name);
		sql("INSERT INTO BRAIN_TOPIC_PERSON (OWNER_ID, OWNER_TYPE, TOPIC_ID, PERSON_ID, STATE) VALUES (?, ?, ?, 'kira', "
				+ "'suggested')", ownerId, ownerType, topicId);
		Map<String, Object> detail = new LinkedHashMap<>();
		detail.put("text", "New topic suggested: " + name);
		detail.put("candidate", topicId);
		detail.put("threadIds", List.of(THREAD, "th-gone"));
		return (String) ((Map<?, ?>) BrainReviewUtils.addReview(ownerId, ownerType, BrainReviewUtils.NEW_TOPIC, "topic",
				topicId, "key-1", detail).get("review")).get("id");
	}

	private String value(String query) throws Exception {
		try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(query)) {
			return rs.next() ? rs.getString(1) : null;
		}
	}

	// one machine notice on its own thread, judged as classifyOne would after sorting it automated; demand is
	// the classifier's answer, null when asking at all is a failure
	private Set<String> vips = Set.of();

	private boolean notice(String threadId, String subject, String senderId, Double demand) throws Exception {
		insertNotice(threadId, subject, senderId);
		return judge(threadId, subject, demand);
	}

	private boolean noticeAgain(String threadId, String subject) throws Exception {
		return judge(threadId, subject, null);
	}

	private void insertNotice(String threadId, String subject, String senderId) throws Exception {
		sql("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, THREAD_KEY, SOURCE, SUBJECT, MUTED, AUTOMATED) "
				+ "VALUES (?, ?, ?, ?, 'email', ?, FALSE, TRUE)", ownerId, ownerType, threadId, "email:" + threadId, subject);
		sql("INSERT INTO BRAIN_MESSAGE (OWNER_ID, OWNER_TYPE, MESSAGE_KEY, THREAD_ID, GRAPH_ID, SENDER_PERSON_ID, "
				+ "RECEIVED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, "key-" + threadId, threadId,
				"g-" + threadId, senderId, java.sql.Timestamp.valueOf("2026-11-05 10:00:00"));
	}

	private boolean judge(String threadId, String subject, Double demand) throws Exception {
		BrainClassifier classifier = new BrainClassifier() {
			@Override
			public String version() {
				return "test-v1";
			}

			@Override
			public Scores score(ThreadInput thread, List<TopicOption> topics, Insight insight) {
				throw new UnsupportedOperationException();
			}

			@Override
			public Double demand(ThreadInput thread, Insight insight) {
				if (demand == null) {
					throw new AssertionError("the model was asked about a notice it should not see");
				}
				return demand;
			}
		};
		Class<?> context = Class.forName(BrainThreadClassifier.class.getName() + "$Context");
		Method notice = BrainThreadClassifier.class.getDeclaredMethod("notice", context, String.class, Map.class,
				List.class);
		notice.setAccessible(true);
		Map<String, Object> message = new LinkedHashMap<>();
		message.put("id", "g-" + threadId);
		message.put("at", "2026-11-05T10:00:00Z");
		message.put("text", subject);
		message.put("fromName", "EHS System");
		Map<String, Object> extra = Map.of("classifier", classifier, "self",
				new BrainThreadClassifier.Self("self", "Owner", Set.of("owner@test.com")), "window",
				BrainThreadClassifier.window((Object) null), "timeZone", java.time.ZoneOffset.UTC, "ownOrg",
				BrainOrgDomains.of(List.of(), "test.com"), "vips", vips, "topicVips", Map.of(), "partners",
				new java.util.concurrent.ConcurrentHashMap<String, Boolean>());
		return (Boolean) notice.invoke(null, context(context, extra), threadId,
				Map.of("subject", subject, "source", "email"), List.of(message));
	}

	// [dedupe key, ask type, status] of every item, oldest first
	private List<List<Object>> items() throws Exception {
		List<List<Object>> out = new ArrayList<>();
		try (java.sql.Statement statement = connection.createStatement();
				java.sql.ResultSet rs = statement
						.executeQuery("SELECT DEDUPE_KEY, ASK_TYPE, STATUS FROM WORK_ITEM ORDER BY CREATED_AT, ITEM_ID")) {
			while (rs.next()) {
				out.add(List.of(rs.getString(1), rs.getString(2), rs.getString(3)));
			}
		}
		return out;
	}

	// the model's topic scores, best first, with the rest on "Something else"; run as classifyOne would
	private void file(Object... ranked) throws Exception {
		Map<String, Double> topics = new LinkedHashMap<>();
		double rest = 1;
		for (int i = 0; i < ranked.length; i += 2) {
			topics.put((String) ranked[i], (Double) ranked[i + 1]);
			rest -= (Double) ranked[i + 1];
		}
		topics.put(BrainThreadClassifier.OTHER_TOPIC, rest);
		Class<?> context = Class.forName(BrainThreadClassifier.class.getName() + "$Context");
		Method fileTopic = BrainThreadClassifier.class.getDeclaredMethod("fileTopic", context, String.class,
				BrainClassifier.Scores.class, boolean.class, Set.class);
		fileTopic.setAccessible(true);
		fileTopic.invoke(null, context(context), THREAD,
				new BrainClassifier.Scores(topics, 0, 0, 0, Map.of()), true, Set.of());
	}

	// the run's context with what filing reads; everything else empty
	private Object context(Class<?> type) throws Exception {
		return context(type, Map.of());
	}

	private Object context(Class<?> type, Map<String, Object> extra) throws Exception {
		Map<String, Object> values = new LinkedHashMap<>(Map.of("user", user, "ownerId", ownerId, "ownerType",
				ownerType, "classifier", CLASSIFIER, "fileAt", BrainProfileUtils.DEFAULT_FILE_AT, "askAt",
				BrainProfileUtils.DEFAULT_ASK_AT));
		values.putAll(extra);
		RecordComponent[] parts = type.getRecordComponents();
		Class<?>[] types = new Class<?>[parts.length];
		Object[] args = new Object[parts.length];
		for (int i = 0; i < parts.length; i++) {
			types[i] = parts[i].getType();
			args[i] = values.getOrDefault(parts[i].getName(),
					types[i].isPrimitive() ? Array.get(Array.newInstance(types[i], 1), 0) : null);
		}
		Constructor<?> constructor = type.getDeclaredConstructor(types);
		constructor.setAccessible(true);
		return constructor.newInstance(args);
	}

	// [topic, source, primary] per link, primary first
	private List<List<Object>> links() throws Exception {
		List<List<Object>> links = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement("SELECT TOPIC_ID, SOURCE, IS_PRIMARY "
				+ "FROM BRAIN_THREAD_TOPIC WHERE THREAD_ID = ? ORDER BY IS_PRIMARY DESC, TOPIC_ID")) {
			statement.setString(1, THREAD);
			try (ResultSet rs = statement.executeQuery()) {
				while (rs.next()) {
					links.add(List.of(rs.getString(1), rs.getString(2), rs.getBoolean(3)));
				}
			}
		}
		return links;
	}

	private Map<String, Integer> confidences() throws Exception {
		Map<String, Integer> confidences = new LinkedHashMap<>();
		try (PreparedStatement statement = connection
				.prepareStatement("SELECT TOPIC_ID, CONFIDENCE FROM BRAIN_THREAD_TOPIC WHERE THREAD_ID = ?")) {
			statement.setString(1, THREAD);
			try (ResultSet rs = statement.executeQuery()) {
				while (rs.next()) {
					confidences.put(rs.getString(1), rs.getInt(2));
				}
			}
		}
		return confidences;
	}

	// the candidates of each open "which topic?" question, as Brain > Review lists them
	private List<Object> openQuestions() {
		List<Object> questions = new ArrayList<>();
		for (Object item : (List<?>) BrainReviewUtils.listReview(user, BrainReviewUtils.OPEN, 50, 0).get("items")) {
			Map<?, ?> review = (Map<?, ?>) item;
			if (BrainReviewUtils.TOPIC_CHOICE.equals(review.get("kind")) && THREAD.equals(review.get("refId"))) {
				questions.add(((Map<?, ?>) review.get("data")).get("candidates"));
			}
		}
		return questions;
	}

	private void person(String topic, String personId) throws Exception {
		sql("INSERT INTO BRAIN_TOPIC_PERSON (OWNER_ID, OWNER_TYPE, TOPIC_ID, PERSON_ID, STATE) VALUES (?, ?, ?, ?, "
				+ "'member')", ownerId, ownerType, topic, personId);
	}

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}
}
