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
	void aFilingNeverTouchesAThreadTheOwnerFiled() throws Exception {
		sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE, CONFIDENCE, IS_PRIMARY, "
				+ "CHANGED_BY) VALUES (?, ?, ?, ?, 'you', 100, TRUE, 'you')", ownerId, ownerType, THREAD, A);
		sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE, CONFIDENCE, IS_PRIMARY, "
				+ "CLASSIFIER_VERSION, CHANGED_BY) VALUES (?, ?, ?, ?, 'suggested', 75, FALSE, 'test-v1', 'brain')",
				ownerId, ownerType, THREAD, B);
		file(C, 0.9, A, 0.05);
		assertEquals(List.of(List.of(A, "you", true), List.of(B, "suggested", false)), links());
	}

	// ---- helpers ----

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
		Map<String, Object> values = Map.of("user", user, "ownerId", ownerId, "ownerType", ownerType, "classifier",
				CLASSIFIER, "fileAt", BrainProfileUtils.DEFAULT_FILE_AT, "askAt", BrainProfileUtils.DEFAULT_ASK_AT);
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

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}
}
