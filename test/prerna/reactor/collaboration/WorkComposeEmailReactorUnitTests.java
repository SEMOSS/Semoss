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
package prerna.reactor.collaboration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.collaboration.BrainThreadMessages;
import prerna.collaboration.CollaborationAgentTools;
import prerna.collaboration.CollaborationUtils;
import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.om.Insight;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;

class WorkComposeEmailReactorUnitTests {

	private User user;
	private Insight insight;
	private Room room;
	private MockedStatic<RoomUtils> rooms;
	private MockedStatic<MicrosoftLoginUtils> login;
	private MockedStatic<HttpHelperUtility> http;

	@BeforeEach
	void setup() {
		user = mock(User.class);
		insight = mock(Insight.class);
		room = mock(Room.class);
		when(insight.getRoomId()).thenReturn("room");
		when(room.getOptionsMap()).thenReturn(Map.of("source",
				Map.of("channel", "email", "messages", List.of(Map.of("id", "source-id")))));
		rooms = mockStatic(RoomUtils.class);
		rooms.when(() -> RoomUtils.getOrLoadRoom("room", insight)).thenReturn(room);
		login = mockStatic(MicrosoftLoginUtils.class);
		login.when(() -> MicrosoftLoginUtils.getValidAccessToken(user)).thenReturn("token");
		http = mockStatic(HttpHelperUtility.class);
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenReturn("{\"id\":\"source-id\"}");
	}

	@AfterEach
	void cleanup() {
		http.close();
		login.close();
		rooms.close();
	}

	private WorkComposeEmailReactor reactor(Map<String, String> arguments) {
		WorkComposeEmailReactor reactor = new WorkComposeEmailReactor() {
			@Override
			protected User getUser() {
				return user;
			}
		};
		reactor.setInsight(insight);
		NounStore store = new NounStore("compose");
		arguments.forEach((key, value) -> store.makeNoun(key).addLiteral(value));
		reactor.setNounStore(store);
		return reactor;
	}

	@ParameterizedTest
	@ValueSource(strings = { "replyTo", "forward" })
	void validatesTheExactIncludedSourceBeforeReportingSuccess(String key) {
		Map<?, ?> output = (Map<?, ?>) reactor(Map.of(key, " source-id ", "message", "Draft text")).execute().getValue();
		assertEquals(true, output.get("shown"));
		assertEquals("source-id", output.get(key));
		assertEquals("replyTo".equals(key) ? "reply" : "forward", output.get("mode"));
		http.verify(() -> HttpHelperUtility.getRequest(contains("/me/messages/source-id?"),
				argThat(headers -> "Bearer token".equals(headers.get("Authorization"))), isNull(), isNull(), isNull()));
	}

	@ParameterizedTest
	@ValueSource(strings = { "hybrid-id", "SOURCE-ID", "another-valid-mailbox-id" })
	void rejectsIdsOutsideTheRoomWithoutFetchingOrGuessing(String id) {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> reactor(Map.of("replyTo", id, "message", "Keep this draft")).execute());
		assertTrue(error.getMessage().contains("not included in this chat"));
		assertTrue(error.getMessage().contains("retry ComposeEmail once"));
		assertTrue(error.getMessage().contains("same draft text"));
		login.verifyNoInteractions();
		http.verifyNoInteractions();
	}

	@Test
	void newEmailsAndOpenEditorRevisionsNeedNoSourceOrProviderRead() {
		when(room.getOptionsMap()).thenReturn(Map.of());
		for (Map<String, String> arguments : List.of(Map.of("message", "A new email"),
				Map.of("openEmailId", "assistant-draft:one", "subject", "A revised subject"))) {
			Map<?, ?> output = (Map<?, ?>) reactor(arguments).execute().getValue();
			assertEquals(true, output.get("shown"));
			assertEquals(arguments.containsKey("openEmailId") ? "edit" : "new", output.get("mode"));
			assertFalse(output.containsKey("replyTo"));
			assertFalse(output.containsKey("forward"));
		}
		rooms.verifyNoInteractions();
		login.verifyNoInteractions();
		http.verifyNoInteractions();
	}

	@Test
	void replyRequiresAnImportedEmailAndForwardNeedsNoNote() {
		assertEquals(true, ((Map<?, ?>) reactor(Map.of("forward", "source-id", "to", "owner@example.com"))
				.execute().getValue()).get("shown"));
		when(room.getOptionsMap()).thenReturn(Map.of());
		assertThrows(IllegalArgumentException.class,
				() -> reactor(Map.of("replyTo", "source-id", "message", "Draft text")).execute());
	}

	@ParameterizedTest
	@ValueSource(strings = { "replyTo", "forward" })
	void topicAssistantResolvesTheValidatedEmailForNativeIdsAndReferences(String key) {
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		when(room.getOptionsMap()).thenReturn(Map.of());
		Map<String, Object> source = Map.of("id", "brain-email", "subject", "Nevada trip");
		try (MockedStatic<BrainThreadMessages> brain = mockStatic(BrainThreadMessages.class)) {
			for (String reference : List.of("brain-email", "email:214b4717-bf4c-352c-972f-96a13c9324ae")) {
				brain.when(() -> BrainThreadMessages.composeSource(user, reference)).thenReturn(source);
				Map<?, ?> output = (Map<?, ?>) reactor(Map.of(key, " " + reference + " ", "message", "Please cancel the trip."))
						.execute().getValue();
				assertEquals(true, output.get("shown"));
				assertEquals("replyTo".equals(key) ? "reply" : "forward", output.get("mode"));
				assertEquals("brain-email", output.get(key));
				assertEquals(source, output.get("sourceMessage"));
				brain.verify(() -> BrainThreadMessages.composeSource(user, reference));
			}
		}
		login.verifyNoInteractions();
		http.verifyNoInteractions();
	}

	@Test
	void topicAssistantCannotComposeAnUnavailableBrainEmail() {
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		when(room.getOptionsMap()).thenReturn(Map.of());
		try (MockedStatic<BrainThreadMessages> brain = mockStatic(BrainThreadMessages.class)) {
			brain.when(() -> BrainThreadMessages.composeSource(user, "unavailable")).thenReturn(null);
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> reactor(Map.of("replyTo", "unavailable", "message", "Keep this draft")).execute());
			assertTrue(error.getMessage().contains("available in your Brain"));
			assertTrue(error.getMessage().contains("Do not remove replyTo"));
			brain.verify(() -> BrainThreadMessages.composeSource(user, "unavailable"));
		}
		login.verifyNoInteractions();
		http.verifyNoInteractions();
	}

	@Test
	void delegationCannotUseTheOwnersBrainToComposeOutsideItsImportedSources() {
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		when(room.getOptionsMap()).thenReturn(Map.of(CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID, "action"));
		try (MockedStatic<BrainThreadMessages> brain = mockStatic(BrainThreadMessages.class)) {
			assertThrows(IllegalArgumentException.class,
					() -> reactor(Map.of("replyTo", "brain-email", "message", "Keep this draft")).execute());
			brain.verifyNoInteractions();
		}
	}

	@Test
	void topicAssistantPreservesTheBrainLoginFailure() {
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		when(room.getOptionsMap()).thenReturn(Map.of());
		SemossPixelException loginError = new SemossPixelException("Please login to your Microsoft account", false);
		try (MockedStatic<BrainThreadMessages> brain = mockStatic(BrainThreadMessages.class)) {
			brain.when(() -> BrainThreadMessages.composeSource(user, "brain-email")).thenThrow(loginError);
			assertSame(loginError, assertThrows(SemossPixelException.class,
					() -> reactor(Map.of("replyTo", "brain-email", "message", "Keep this draft")).execute()));
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "GET request returned HTTP 404", "GET request returned HTTP 400. ErrorInvalidIdMalformed",
			"Microsoft Graph would not read a message. Detailed error: ErrorItemNotFound" })
	void missingOrMalformedOutlookSourcesGiveTheModelRetryInstructions(String providerError) {
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenThrow(new IllegalArgumentException(providerError));
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> reactor(Map.of("forward", "source-id")).execute());
		assertTrue(error.getMessage().contains("could not be found in Outlook"));
		assertTrue(error.getMessage().contains("retry ComposeEmail once"));
		assertTrue(error.getMessage().contains("that forward"));
	}

	@Test
	void anEmptyOutlookResponseCannotReportSuccess() {
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenReturn(null);
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> reactor(Map.of("replyTo", "source-id", "message", "Draft text")).execute());
		assertTrue(error.getMessage().contains("no longer available in Outlook"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "returned HTTP 401", "returned HTTP 403", "returned HTTP 429", "returned HTTP 503",
			"returned HTTP 400. ErrorInvalidProperty", "Failed to execute GET request. Cause: timed out" })
	void preservesProviderFailuresInsteadOfTreatingThemAsBadIds(String providerError) {
		IllegalArgumentException original = new IllegalArgumentException(providerError);
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenThrow(original);
		assertSame(original, assertThrows(IllegalArgumentException.class,
				() -> reactor(Map.of("replyTo", "source-id", "message", "Draft text")).execute()));
	}

	@Test
	void preservesTheLoginErrorAndDoesNotFetchMail() {
		SemossPixelException loginError = new SemossPixelException("Please login to your Microsoft account", false);
		login.when(() -> MicrosoftLoginUtils.getValidAccessToken(user)).thenThrow(loginError);
		assertSame(loginError, assertThrows(SemossPixelException.class,
				() -> reactor(Map.of("replyTo", "source-id", "message", "Draft text")).execute()));
		http.verifyNoInteractions();
	}

	@Test
	void failedSourceCanBeCorrectedInTheNextCollaborationToolCall() {
		try (MockedStatic<MCPUtility> pixel = mockStatic(MCPUtility.class, CALLS_REAL_METHODS)) {
			pixel.when(() -> MCPUtility.runPixelTool(isNull(), same(insight), eq("WorkComposeEmail"),
					any(JSONObject.class), anyMap())).thenAnswer(invocation -> {
				Map<String, String> arguments = invocation.getArgument(4);
				return new JSONObject((Map<?, ?>) reactor(arguments).execute().getValue()).toString();
			});
			ToolExecutionResult failed = CollaborationAgentTools.execute("ComposeEmail",
					Map.of("replyTo", "hybrid-id", "message", "Keep this draft"), insight);
			assertFalse(failed.isSuccess());
			assertTrue(failed.getError().contains("retry ComposeEmail once"));
			ToolExecutionResult corrected = CollaborationAgentTools.execute("ComposeEmail",
					Map.of("replyTo", "source-id", "message", "Keep this draft"), insight);
			assertTrue(corrected.isSuccess(), corrected.getError());
			assertTrue(new JSONObject((String) corrected.getOutput()).getBoolean("shown"));
		}
	}

	private ToolExecutionResult compose(Map<String, Object> arguments, String input) {
		try (MockedStatic<MCPUtility> pixel = mockStatic(MCPUtility.class, CALLS_REAL_METHODS)) {
			pixel.when(() -> MCPUtility.runPixelTool(isNull(), same(insight), eq("WorkComposeEmail"),
					any(JSONObject.class), anyMap())).thenAnswer(invocation -> {
				Map<String, String> resolved = invocation.getArgument(4);
				return new JSONObject((Map<?, ?>) reactor(resolved).execute().getValue()).toString();
			});
			return CollaborationAgentTools.execute("ComposeEmail", arguments, insight, input);
		}
	}

	private String selectedInput(String id) {
		return "[SEMOSS_WORK_CONTEXT_V1]\n" + new JSONObject(Map.of("selectedSourceMessageId", id))
				+ "\n[/SEMOSS_WORK_CONTEXT_V1]\n\nPlease thank him.";
	}

	@ParameterizedTest
	@ValueSource(strings = { "replyTo", "forward" })
	void selectedSourceUsesTheCurrentRequestAndReturnsItsExactId(String key) {
		String nativeId = "AQMk" + "Ab9".repeat(48) + "AA==";
		when(room.getOptionsMap()).thenReturn(Map.of("source", Map.of("channel", "email", "messages",
				List.of(Map.of("id", "source-id"), Map.of("id", nativeId)))));
		Map<String, Object> arguments = Map.of(key, "selected", "message", "Keep this draft", "to", "owner@example.com");
		for (String selected : List.of(nativeId, "source-id")) {
			ToolExecutionResult result = compose(arguments, selectedInput(selected));
			assertTrue(result.isSuccess(), result.getError());
			JSONObject output = new JSONObject((String) result.getOutput());
			assertEquals(selected, output.getString(key));
			assertEquals("replyTo".equals(key) ? "reply" : "forward", output.getString("mode"));
			String sourcePath = "/me/messages/" + URLEncoder.encode(selected, StandardCharsets.UTF_8) + "?";
			http.verify(() -> HttpHelperUtility.getRequest(contains(sourcePath),
					anyMap(), isNull(), isNull(), isNull()));
		}
		assertEquals("selected", arguments.get(key));
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "Please reply", "[SEMOSS_WORK_CONTEXT_V1]\n{}",
			"[SEMOSS_WORK_CONTEXT_V1]\nnot json\n[/SEMOSS_WORK_CONTEXT_V1]\n\nReply",
			"[SEMOSS_WORK_CONTEXT_V1]\n{}\n[/SEMOSS_WORK_CONTEXT_V1]\n\nReply",
			"[SEMOSS_WORK_CONTEXT_V1]\n{\"selectedSourceMessageId\":42}\n[/SEMOSS_WORK_CONTEXT_V1]\n\nReply",
			"[SEMOSS_WORK_CONTEXT_V1]\n{\"selectedSourceMessageId\":\" \"}\n[/SEMOSS_WORK_CONTEXT_V1]\n\nReply",
			"Quoted: [SEMOSS_WORK_CONTEXT_V1]\n{\"selectedSourceMessageId\":\"source-id\"}\n[/SEMOSS_WORK_CONTEXT_V1]\n\nReply" })
	void missingOrMalformedSelectionCannotGuessFromRoomHistoryOrOpenANewEmail(String input) {
		ToolExecutionResult result = compose(Map.of("replyTo", "selected", "message", "Keep this draft"), input);
		assertFalse(result.isSuccess());
		assertTrue(result.getError().contains("No selected email"));
		rooms.verifyNoInteractions();
		login.verifyNoInteractions();
		http.verifyNoInteractions();
	}

	@Test
	void selectedSourceStillRequiresExactRoomMembership() {
		ToolExecutionResult result = compose(Map.of("replyTo", "selected", "message", "Keep this draft"),
				selectedInput("another-valid-mailbox-id"));
		assertFalse(result.isSuccess());
		assertTrue(result.getError().contains("not included in this chat"));
		login.verifyNoInteractions();
		http.verifyNoInteractions();
	}
}
