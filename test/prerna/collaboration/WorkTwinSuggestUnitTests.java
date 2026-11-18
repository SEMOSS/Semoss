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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.collaboration.WorkTwinSuggest.Read;
import prerna.collaboration.WorkTwinSuggest.RecentChat;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * Digital Twin suggestions: what reaches each place's search, what the model is given to choose from, and what comes
 * back of its answer. The places and the model are fakes; the owner's rules are read from a real schema.
 */
class WorkTwinSuggestUnitTests {

	private static final String PLAN = "{\"mail\": [\"Apollo Migration\", \"NOT from:cfo\"], \"chats\": [\"apollo\"], "
			+ "\"files\": [\"apollo plan\", \"apollo plan\"]}";

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private User user;
	private String ownerId;
	private String ownerType;
	private IModelEngine model;
	private BrainMailFolders.Tree folders;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:twin_suggest_" + UUID.randomUUID());
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
		token.setId("ownerid");
		token.setName("Owner");
		token.setEmail("owner@test.com");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		// the mailbox they are signed in to, which is how mail from themselves is known
		AccessToken microsoft = new AccessToken();
		microsoft.setId("ms-owner");
		microsoft.setEmail("Owner@Test.com");
		microsoft.setProvider(AuthProvider.MICROSOFT);
		user.setAccessToken(microsoft);
		Pair<String, String> owner = User.getPrimaryUserIdAndTypePair(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
		model = mock(IModelEngine.class);
		folders = BrainMailFolders.build(List.of(Map.of("id", "f-inbox", "displayName", "Inbox", "wellKnownName",
				"inbox"), Map.of("id", "f-deleted", "displayName", "Deleted Items", "wellKnownName", "deleteditems")),
				null);
	}

	@AfterEach
	void tearDown() throws Exception {
		registry.close();
		connection.close();
	}

	@Test
	void suggestsWhatTheModelPicksOfWhatTheRulesLetItRead() throws Exception {
		// blocked in People (Never ingest): a rule on the person's id
		sql("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, DISPLAY_NAME) VALUES (?, ?, 'p-blocked', "
				+ "'Blocked Person')", ownerId, ownerType);
		sql("INSERT INTO BRAIN_PERSON_ADDRESS (OWNER_ID, OWNER_TYPE, PERSON_ID, KIND, VALUE_NORM) VALUES (?, ?, "
				+ "'p-blocked', 'email', 'blocked@evil.com')", ownerId, ownerType);
		sql("INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, PERSON_ID, CREATED_AT) VALUES (?, ?, "
				+ "'never-blocked', 'exclude_everywhere', 'p-blocked', CURRENT_TIMESTAMP)", ownerId, ownerType);
		sql("INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, " + CollaborationDbUtils.column("VALUE")
				+ ", CREATED_AT) VALUES (?, ?, 'never-salary', 'never_keyword', 'salary', CURRENT_TIMESTAMP)", ownerId,
				ownerType);
		// a routine's result Collaboration recorded as it sent it
		BrainOwnMail.record(ownerId, ownerType, "<conv-6-m@test>", "routine");
		when(model.ask(anyString(), anyString(), any(), anyMap())).thenReturn(new AskStringModelEngineResponse(PLAN, 0, 0))
				// i1 the thread, i2 the chat, i3 the file; a made-up name and a repeat are not items
				.thenReturn(new AskStringModelEngineResponse("{\"picks\": [{\"id\": \"i3\", \"reason\": \"The plan\"}, "
						+ "{\"id\": \"i9\", \"reason\": \"Made up\"}, {\"id\": \"i1\", \"reason\": \"The cutover date\"}, "
						+ "{\"id\": \"i1\", \"reason\": \"Again\"}, {\"id\": \"i2\", \"reason\": \"The vendor call\"}]}",
						0, 0));
		List<String> mailSearches = new CopyOnWriteArrayList<>();
		List<String> fileSearches = new CopyOnWriteArrayList<>();
		Read<List<Map<String, Object>>> mail = words -> {
			mailSearches.add(words);
			return mailSearches.size() > 1 ? List.of()
					: List.of(mail("conv-1", "Apollo cutover", "priya@acme.com", "f-inbox", false, "2026-11-01T09:00:00Z",
							"We could cut over on the 14th"),
							mail("conv-1", "RE: Apollo cutover", "sam@acme.com", "f-inbox", false, "2026-11-03T09:00:00Z",
									"Confirmed for the 14th"),
							mail("conv-2", "Apollo secrets", "Blocked@Evil.com", "f-inbox", false,
									"2026-11-04T09:00:00Z", "Do not tell anyone"),
							mail("conv-3", "Apollo draft", "owner@test.com", "f-inbox", true, "2026-11-05T09:00:00Z",
									"Half written"),
							mail("conv-4", "Apollo old", "priya@acme.com", "f-deleted", false, "2026-11-06T09:00:00Z",
									"Thrown away"),
							mail("conv-5", "Apollo raises", "hr@acme.com", "f-inbox", false, "2026-11-07T09:00:00Z",
									"The salary table for the team"),
							mail("conv-6", "Vendor invoice check", "owner@test.com", "f-inbox", false,
									"2026-11-08T09:00:00Z", "Recorded result: Apollo invoices are paid"),
							// sent before the record was kept, or by another server: known by its dated title
							mail("conv-7", "Morning Work Briefing - 2026-11-08", "owner@test.com", "f-inbox", false,
									"2026-11-08T10:00:00Z", "Unrecorded result: reply to Priya about Apollo"));
		};
		Read<List<Map<String, Object>>> chats = words -> List.of(
				Map.of("id", "m-1", "kind", "chat", "chatId", "19:chat-1", "fromName", "Grace", "createdDateTime",
						"2026-11-02T10:00:00Z", "summary", "Apollo vendor call moved to Friday"),
				Map.of("id", "m-2", "kind", "chat", "chatId", "19:chat-2", "fromName", "Blocked Person", "fromEmail",
						"blocked@evil.com", "createdDateTime", "2026-11-02T11:00:00Z", "summary", "Apollo, quietly"));
		Read<List<Map<String, Object>>> files = words -> {
			fileSearches.add(words);
			return List.of(Map.of("id", "item-1", "driveId", "drive-9", "name", "Apollo plan.docx", "isFolder", false,
					"path", "Projects/Apollo", "lastModifiedBy", "Priya Shah", "lastModifiedDateTime",
					"2026-10-30T08:00:00Z"), Map.of("id", "item-2", "name", "Apollo", "isFolder", true));
		};

		Map<String, Object> out = WorkTwinSuggest.suggest(user, "The Apollo migration", model, folders, mail, chats,
				files, chatId -> Map.of("id", chatId, "displayName", "Apollo vendors"));

		// each search is the model's words made plain: no capitals for an operator, no colon, and none twice
		assertEquals(List.of("apollo migration", "not from cfo"), mailSearches);
		assertEquals(List.of("apollo plan"), fileSearches);

		// the model is given one item per thread, chat and file, and nothing the rules keep from Brain
		ArgumentCaptor<String> asked = ArgumentCaptor.forClass(String.class);
		verify(model, times(2)).ask(asked.capture(), anyString(), any(), anyMap());
		String items = asked.getAllValues().get(1);
		assertTrue(items.contains("Confirmed for the 14th"));
		assertFalse(items.contains("We could cut over"));
		for (String keptOut : List.of("Apollo secrets", "Apollo draft", "Apollo old", "salary", "quietly", "item-2",
				"conv-1", "19:chat-1", "Recorded result", "Unrecorded result")) {
			assertFalse(items.contains(keptOut), keptOut);
		}

		// what comes back is what the model picked, in its order, by the ids each place reads an item with
		List<?> suggestions = (List<?>) out.get("suggestions");
		assertEquals(3, suggestions.size());
		Map<String, Object> file = new LinkedHashMap<>();
		file.put("kind", "file");
		file.put("id", "item-1");
		file.put("title", "Apollo plan.docx");
		file.put("from", "Priya Shah");
		file.put("date", "2026-10-30T08:00:00Z");
		file.put("preview", "Projects/Apollo");
		file.put("driveId", "drive-9");
		file.put("reason", "The plan");
		assertEquals(file, suggestions.get(0));
		Map<?, ?> email = (Map<?, ?>) suggestions.get(1);
		assertEquals("email", email.get("kind"));
		assertEquals("conv-1", email.get("id"));
		assertEquals("RE: Apollo cutover", email.get("title"));
		assertEquals("The cutover date", email.get("reason"));
		Map<?, ?> chat = (Map<?, ?>) suggestions.get(2);
		assertEquals("19:chat-1", chat.get("id"));
		assertEquals("Apollo vendors", chat.get("title"));
		assertEquals(List.of(), out.get("notes"));
	}

	@Test
	void aPlaceThatCannotBeSearchedIsSaidAndTheOthersStillAnswer() {
		when(model.ask(anyString(), anyString(), any(), anyMap())).thenReturn(new AskStringModelEngineResponse(PLAN, 0, 0))
				.thenReturn(new AskStringModelEngineResponse("{\"picks\": [{\"id\": \"i1\", \"reason\": \"Fits\"}]}", 0,
						0));
		Read<List<Map<String, Object>>> fails = words -> {
			throw new IllegalStateException("Graph returned HTTP 403");
		};
		Read<List<Map<String, Object>>> files = words -> List
				.of(Map.of("id", "item-1", "name", "Apollo plan.docx", "isFolder", false));

		Map<String, Object> out = WorkTwinSuggest.suggest(user, "Apollo", model, folders, fails, fails, files,
				chatId -> null);

		assertEquals(1, ((List<?>) out.get("suggestions")).size());
		assertEquals(List.of("Outlook could not be searched: Graph returned HTTP 403",
				"Teams chats could not be searched: Graph returned HTTP 403"), out.get("notes"));

		// nothing searched is a failure, not an empty list
		when(model.ask(anyString(), anyString(), any(), anyMap())).thenReturn(new AskStringModelEngineResponse(PLAN, 0, 0));
		assertThrows(IllegalArgumentException.class,
				() -> WorkTwinSuggest.suggest(user, "Apollo", model, folders, fails, fails, fails, chatId -> null));
	}

	@Test
	void readsMailNamedForASearchFirstAndOnlySoManyThreads() throws Exception {
		when(model.ask(anyString(), anyString(), any(), anyMap())).thenReturn(new AskStringModelEngineResponse(PLAN, 0, 0))
				.thenReturn(new AskStringModelEngineResponse("{\"picks\": [{\"id\": \"i2\", \"reason\": \"Fits\"}]}", 0,
						0));
		// forty threads that only mention it, each newer than the two that are named for it
		List<Map<String, Object>> found = new ArrayList<>();
		for (int day = 10; day < 50; day++) {
			found.add(mail("conv-" + day, "Weekly notes " + day, "priya@acme.com", "f-inbox", false,
					"2026-11-" + day + "T09:00:00Z", "Also, the apollo migration came up"));
		}
		found.add(mail("conv-old", "Apollo migration plan", "sam@acme.com", "f-inbox", false, "2026-01-05T09:00:00Z",
				"The plan"));
		found.add(mail("conv-older", "RE: apollo MIGRATION dates", "sam@acme.com", "f-inbox", false,
				"2026-01-02T09:00:00Z", "The dates"));
		// what a place says of how far it reached is passed on
		Read<List<Map<String, Object>>> chats = new Read<>() {
			@Override
			public List<Map<String, Object>> of(String words) {
				return List.of();
			}

			@Override
			public String note() {
				return "Only your 30 most recent Teams chats were checked.";
			}
		};

		Map<String, Object> out = WorkTwinSuggest.suggest(user, "The Apollo migration", model, folders, words -> found,
				chats, words -> List.of(), chatId -> null);

		ArgumentCaptor<String> asked = ArgumentCaptor.forClass(String.class);
		verify(model, times(2)).ask(asked.capture(), anyString(), any(), anyMap());
		List<?> items = (List<?>) CollaborationDbUtils.parseMap(asked.getAllValues().get(1)).get("items");
		assertEquals(WorkTwinSuggest.MAX_THREADS, items.size());
		assertEquals("Apollo migration plan", ((Map<?, ?>) items.get(0)).get("title"));
		assertEquals("RE: apollo MIGRATION dates", ((Map<?, ?>) items.get(1)).get("title"));
		// then the newest of the rest
		assertEquals("Weekly notes 49", ((Map<?, ?>) items.get(2)).get("title"));
		assertEquals("conv-older", ((Map<?, ?>) ((List<?>) out.get("suggestions")).get(0)).get("id"));
		assertEquals(List.of("Only your 30 most recent Teams chats were checked."), out.get("notes"));
	}

	@Test
	void findsAChatByItsNameAndNewestMessagesWhereMicrosoftsSearchIsRefused() {
		List<RecentChat> chats = List.of(
				// named for it, though nothing said lately holds the word: its newest message speaks for it
				new RecentChat("Apollo vendors", List.of(said("19:named", "See you Friday"), said("19:named", "Lunch?"))),
				// the words are spread over its messages: the newest one holding one of them speaks for it
				new RecentChat("Dana Ortiz", List.of(said("19:spread", "ok"), said("19:spread", "The cutover is on the 15th"),
						said("19:spread", "Is Apollo still on?"))),
				new RecentChat("Lunch crew", List.of(said("19:other", "Tacos?"))),
				// nothing in it to read or to copy
				new RecentChat("Apollo archive", List.of()), new RecentChat(null, List.of(said("19:unnamed", "Hi"))));

		assertEquals(List.of("See you Friday", "Is Apollo still on?"),
				WorkTwinSuggest.matching(chats, "apollo").stream().map(hit -> hit.get("summary")).toList());
		assertEquals(List.of("The cutover is on the 15th"),
				WorkTwinSuggest.matching(chats, "apollo cutover").stream().map(hit -> hit.get("summary")).toList());
		assertEquals(List.of(), WorkTwinSuggest.matching(chats, "zeus"));
	}

	@Test
	void saysWhyGraphRefusedInItsOwnWordsAndNeverItsRawReply() {
		assertEquals("Access to ChatMessage in Graph API requires the following permissions: Chat.Read or "
				+ "Chat.ReadWrite, ChannelMessage.Read.All. However, the application only has \"User.Read\"",
				WorkTwinSuggest.why(new IllegalStateException("POST request to https://graph.microsoft.com/v1.0/"
						+ "search/query returned HTTP 403. Response body: {\"error\":{\"code\":\"Forbidden\",\"message\":"
						+ "\"Access to ChatMessage in Graph API requires the following permissions: Chat.Read or "
						+ "Chat.ReadWrite, ChannelMessage.Read.All. However, the application only has "
						+ "\\\"User.Read\\\"\",\"target\":\"\",\"httpCode\":403}}")));
		assertEquals("GET request returned HTTP 503", WorkTwinSuggest
				.why(new IllegalStateException("GET request returned HTTP 503. Response body: <html>Busy</html>")));
		assertEquals("the search failed", WorkTwinSuggest.why(new IllegalStateException()));
	}

	private static Map<String, Object> said(String chatId, String text) {
		return Map.of("chatId", chatId, "summary", text);
	}

	private static Map<String, Object> mail(String conversationId, String subject, String address, String folder,
			boolean draft, String received, String preview) {
		Map<String, Object> message = new LinkedHashMap<>();
		message.put("id", UUID.randomUUID().toString());
		message.put("internetMessageId", "<" + conversationId + "-m@test>");
		message.put("conversationId", conversationId);
		message.put("subject", subject);
		message.put("from", Map.of("emailAddress", Map.of("address", address, "name", address)));
		message.put("parentFolderId", folder);
		message.put("isDraft", draft);
		message.put("receivedDateTime", received);
		message.put("bodyPreview", preview);
		return message;
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
