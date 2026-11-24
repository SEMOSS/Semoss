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
package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.message.SystemMessagePart;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.util.Constants;
import prerna.util.Utility;

class ModelRouterEngineSubtaskUnitTests {

	@TempDir
	Path directory;

	private static final String ROUTES = """
			"routes": [{"name": "fast", "engine_id": "fast", "keywords": ["quick"]},
			           {"name": "code", "engine_id": "code", "keywords": ["code"]},
			           {"name": "deep", "engine_id": "deep"}]""";
	private static final String SUBTASKS = """
			{"mode": "subtask", "classifier_engine": "classifier", "default_subtask": "inspect", "version": "v7", %s,
			 "subtasks": [{"id": "inspect", "recognition_rules": ["reading files"], "route": "fast", "skill_id": "skill-inspect"},
			              {"id": "edit", "recognition_rules": ["changing files"], "route": "code"},
			              {"id": "summarize", "recognition_rules": ["final answer"], "route": "deep"}],
			 %s}""";

	/** Engines by id, a scripted classifier and the room the router sees. */
	private class Fixture {
		final Map<String, IModelEngine> engines = new HashMap<>();
		final List<String> classifierReplies = new ArrayList<>();
		final List<InputMessage> classifierPrompts = new ArrayList<>();
		final List<AbstractMessage> roomMessages = new ArrayList<>();
		final Room room = mock(Room.class);
		final Map<String, Object> lastMetadata = new HashMap<>();
		ModelRouterEngine router;

		Fixture() {
			for (String id : List.of("fast", "code", "deep")) {
				engines.put(id, mock(IModelEngine.class));
				when(engines.get(id).askRoom(any(), any(), any())).thenAnswer(call -> {
					AskModelEngineResponse<?> response = mock(AskModelEngineResponse.class);
					doAnswer(set -> {
						lastMetadata.putAll(set.<Map<String, Object>>getArgument(0));
						return null;
					}).when(response).setMetadata(any());
					return response;
				});
			}
			IModelEngine classifier = mock(IModelEngine.class);
			when(classifier.askRoom(any(), any(), any())).thenAnswer(call -> {
				classifierPrompts.add(call.getArgument(0));
				AskModelEngineResponse<?> response = mock(AskModelEngineResponse.class);
				when(response.getStringResponse()).thenReturn(classifierReplies.remove(0));
				return response;
			});
			engines.put("classifier", classifier);
			when(room.getMessages()).thenReturn(roomMessages);
			when(room.getId()).thenReturn("room");
		}

		void ask(InputMessage message, Function<String, String> skills, String config) throws Exception {
			try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class);
					MockedStatic<Utility> utility = mockStatic(Utility.class);
					MockedStatic<RoomUtils> rooms = mockStatic(RoomUtils.class)) {
				// copying a message builds a SemossDate, which reads the application zone
				utility.when(Utility::getApplicationZoneId).thenReturn("UTC");
				engines.forEach((id, engine) -> utility.when(() -> Utility.getEngine(id)).thenReturn(engine));
				rooms.when(() -> RoomUtils.createRoomForStatelessAsk(any(), any(), any(), any())).thenReturn(room);
				if (router == null) {
					router = openRouter(config, skills);
				}
				lastMetadata.clear();
				router.askCall(message, null, "room", null);
			}
		}

		void ask(InputMessage message, String config) throws Exception {
			ask(message, id -> null, config);
		}

		@SuppressWarnings("unchecked")
		Map<String, Object> trace() {
			return (Map<String, Object>) lastMetadata.get(ModelRouterEngine.METADATA_ROUTING_TRACE);
		}

		InputMessage dispatched(String engineId) {
			ArgumentCaptor<InputMessage> captor = ArgumentCaptor.forClass(InputMessage.class);
			verify(engines.get(engineId), atLeastOnce()).askRoom(captor.capture(), any(), any());
			return captor.getValue();
		}
	}

	private static InputMessage user(Room room, String text) {
		return InputMessage.text(room, text);
	}

	private static String config(String sticky) {
		return SUBTASKS.formatted(ROUTES, sticky);
	}

	@Test
	void anInvalidOrFailedClassificationUsesThePreviousSubtaskThenTheDefault() throws Exception {
		Fixture fixture = new Fixture();
		fixture.classifierReplies.add("not-a-subtask");
		fixture.ask(user(fixture.room, "hello"), config("\"sticky\": false"));
		assertEquals("default_subtask", fixture.trace().get("selection_source"));
		assertEquals("inspect", fixture.trace().get("subtask_id"));
		assertEquals("invalid_choice", fixture.trace().get("classifier_status"));

		ResponseMessage routed = routedResponse("edit");
		InputMessage again = user(fixture.room, "hello again");
		again.setParentMessageId(routed.getMessageId());
		fixture.roomMessages.add(routed);
		fixture.classifierReplies.add("???");
		fixture.ask(again, config("\"sticky\": false"));
		assertEquals("previous_subtask", fixture.trace().get("selection_source"));
		assertEquals("edit", fixture.trace().get("subtask_id"));
		verify(fixture.engines.get("code")).askRoom(any(), any(), any());
	}

	@Test
	void theClassifierChoiceSelectsTheRouteAndAToolContinuationIsClassified() throws Exception {
		Fixture fixture = new Fixture();
		InputMessage goal = user(fixture.room, "clean up the quarterly workbook");
		ResponseMessage routed = routedResponse("inspect");
		InputMessage step = InputMessage.toolExecution(fixture.room, "c1", "read_file", "17 rows", Map.of(), "success", false);
		step.setVisible(false);
		String parent = null;
		for (AbstractMessage m : List.of(goal, routed, step)) {
			m.setParentMessageId(parent);
			parent = m.getMessageId();
		}
		fixture.roomMessages.addAll(List.of(goal, routed));
		fixture.classifierReplies.add("EDIT");
		fixture.ask(step, config("\"sticky\": false"));
		verify(fixture.engines.get("code")).askRoom(any(), any(), any());
		verify(fixture.engines.get("fast"), never()).askRoom(any(), any(), any());
		Map<?, ?> inputs = (Map<?, ?>) fixture.trace().get("classifier_inputs");
		assertEquals("clean up the quarterly workbook", inputs.get("goal"));
		assertEquals("inspect", inputs.get("previous_subtask"));
		assertEquals(List.of("read_file (success)"), inputs.get("recent_steps"));
		assertEquals("17 rows", fixture.dispatched("code").getFullInputPrompt());
		assertEquals("edit", fixture.trace().get("subtask_id"));
		assertEquals("v7", fixture.trace().get("config_version"));
	}

	@Test
	void compactedBranchUsesFreshRequestAndOnlyToolNamesAndStatusForClassifier() throws Exception {
		Fixture fixture = new Fixture();
		InputMessage compactedRoot = user(fixture.room, "Summarizing Conversation");
		compactedRoot.setSummaryLeafMessageId("pruned-message");
		compactedRoot.setVisible(false);
		ResponseMessage compactedSummary = new ResponseMessage.Builder().withText("old task summary").build();
		InputMessage reflection = user(fixture.room, "internal reflection prompt");
		reflection.setOrnament("agentRunRole", "reflection_input");
		InputMessage read = InputMessage.toolExecution(fixture.room, "c1", "read_file", "private tool output", Map.of(), "success", false);
		InputMessage freshRequest = user(fixture.room, "make a fresh change to the workbook");
		compactedSummary.setParentMessageId(compactedRoot.getMessageId());
		reflection.setParentMessageId(compactedSummary.getMessageId());
		read.setParentMessageId(reflection.getMessageId());
		freshRequest.setParentMessageId(read.getMessageId());
		fixture.roomMessages.addAll(List.of(compactedRoot, compactedSummary, reflection, read));
		fixture.classifierReplies.add("edit");

		fixture.ask(freshRequest, config("\"sticky\": false"));

		Map<?, ?> inputs = (Map<?, ?>) fixture.trace().get("classifier_inputs");
		assertEquals("make a fresh change to the workbook", inputs.get("goal"));
		assertEquals(List.of(), inputs.get("recent_steps"));
		String prompt = fixture.classifierPrompts.getLast().getFullInputPrompt();
		assertTrue(prompt.contains("Goal: make a fresh change to the workbook"));
		assertFalse(prompt.contains("Summarizing Conversation"));
		assertFalse(prompt.contains("internal reflection prompt"));
		assertFalse(prompt.contains("old task summary"));
		assertFalse(prompt.contains("private tool output"));
		assertEquals("make a fresh change to the workbook", fixture.dispatched("code").getFullInputPrompt());
	}

	@Test
	void hiddenPreviousBackendDecisionStillPinsStickySubtask() throws Exception {
		Fixture fixture = new Fixture();
		AskModelEngineResponse<?> backendResponse = mock(AskModelEngineResponse.class);
		when(backendResponse.getMetadata()).thenReturn(Map.of(
				ModelRouterEngine.METADATA_ROUTED_ENGINE_ID, "code",
				ModelRouterEngine.METADATA_ROUTED_ENGINE_NAME, "Code Engine",
				ModelRouterEngine.METADATA_ROUTER_ENGINE_ID, "router",
				ModelRouterEngine.METADATA_ROUTING_TRACE, Map.of("subtask_id", "edit")));
		when(backendResponse.getMessageType()).thenReturn(AskModelEngineResponse.CHAT);
		when(backendResponse.getStringResponse()).thenReturn("Working on the edit.");
		ResponseMessage hiddenDecision = ResponseMessage.Builder.fromAskModelEngineResponse(backendResponse).build();
		hiddenDecision.setVisible(false);
		InputMessage nextRequest = user(fixture.room, "continue the edit");
		nextRequest.setParentMessageId(hiddenDecision.getMessageId());
		fixture.roomMessages.add(hiddenDecision);

		fixture.ask(nextRequest, config("\"sticky\": true"));

		assertTrue(fixture.classifierPrompts.isEmpty());
		Map<?, ?> inputs = (Map<?, ?>) fixture.trace().get("classifier_inputs");
		assertEquals("edit", inputs.get("previous_subtask"));
		assertEquals("sticky_subtask", fixture.trace().get("selection_source"));
		assertEquals("edit", fixture.trace().get("subtask_id"));
		verify(fixture.engines.get("code")).askRoom(any(), any(), any());
	}

	private static ResponseMessage routedResponse(String subtask) {
		return new ResponseMessage.Builder()
				.withMetadata("routing", Map.of("trace", Map.of("subtask_id", subtask))).build();
	}

	@Test
	void stickyPinsTheRoomsSubtaskInsteadOfTheEngine() throws Exception {
		Fixture fixture = new Fixture();
		InputMessage first = user(fixture.room, "start");
		ResponseMessage routed = routedResponse("summarize");
		InputMessage next = user(fixture.room, "continue");
		fixture.roomMessages.addAll(List.of(first, routed));
		next.setParentMessageId(routed.getMessageId());
		routed.setParentMessageId(first.getMessageId());

		fixture.ask(next, config("\"sticky\": true"));

		assertTrue(fixture.classifierPrompts.isEmpty(), "no classifier call while the subtask is pinned");
		verify(fixture.engines.get("deep")).askRoom(any(), any(), any());
		assertEquals("sticky_subtask", fixture.trace().get("selection_source"));
		assertEquals("summarize", fixture.trace().get("subtask_id"));
	}

	@Test
	void guidanceIsOnTheDispatchedCopyNeverOnTheSharedMessage() throws Exception {
		Fixture fixture = new Fixture();
		InputMessage message = user(fixture.room, "look at the file");
		fixture.classifierReplies.add("inspect");
		fixture.ask(message, id -> "---\nname: inspect\n---\nRead before you write.", config("\"sticky\": false"));

		InputMessage sent = fixture.dispatched("fast");
		assertNotSame(message, sent);
		assertTrue(sent.getParts().stream().anyMatch(p -> p instanceof SystemMessagePart s && s.getPrompt().contains("Read before you write.")));
		assertEquals(sent.getMessageId(), message.getMessageId());
		for (MessagePart part : message.getParts()) {
			assertFalse(part instanceof SystemMessagePart, "the shared message must stay untouched");
		}
		assertNull(message.getSystemPrompt());
		assertTrue(fixture.roomMessages.isEmpty());
		assertEquals("skill-inspect", fixture.trace().get("skill_id"));
		assertEquals("attached_to_dispatch", fixture.trace().get("skill_guidance_status"));
		assertEquals("7aeaeaebda4a381cafe3b1bfca9ce8c8a38c055f3a31d23376608120f24fd6b6",
				fixture.trace().get("skill_guidance_sha256"));
		String providerInput = MessageUtils.toJsonArrayWithImageData(List.of(sent));
		assertTrue(providerInput.contains("Read before you write."), providerInput);
		assertTrue(providerInput.contains("SYSTEM"), providerInput);
	}

	@Test
	void configuredButMissingGuidanceIsNotReportedAsDelivered() throws Exception {
		Fixture fixture = new Fixture();
		fixture.classifierReplies.add("inspect");
		fixture.ask(user(fixture.room, "look at the file"), id -> null, config("\"sticky\": false"));

		assertEquals("skill-inspect", fixture.trace().get("skill_id"));
		assertEquals("empty_or_missing", fixture.trace().get("skill_guidance_status"));
		assertNull(fixture.trace().get("skill_guidance_sha256"));
		assertTrue(fixture.dispatched("fast").getParts().stream()
				.noneMatch(part -> part instanceof SystemMessagePart));
	}

	@Test
	void anUpdatedConfigIsMirroredIntoTheSmssSoItNeverShowsAnOutdatedSetup() throws Exception {
		Path smss = directory.resolve("router.smss");
		Files.writeString(smss, "ENGINE\trouter\nROUTER_CONFIG_JSON\t{\"mode\": \"keyword\"}\nMODEL_TYPE\tMODEL_ROUTER\n");
		String updated = "{\"mode\": \"keyword\", \"version\": \"v2\",\n \"routes\": [{\"name\": \"fast\", \"engine_id\": \"fast\", \"keywords\": [\"quick\"]}]}";
		try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
			utility.when(() -> Utility.getEngine("fast")).thenReturn(mock(IModelEngine.class));
			ModelRouterEngine router = openRouter(updated.replace("v2", "v1"), id -> null);
			router.setSmssFilePath(smss.toString());

			router.updateConfig(updated);
		}

		List<String> lines = Files.readAllLines(smss);
		assertEquals(3, lines.size());
		assertEquals("ENGINE\trouter", lines.get(0));
		assertEquals("ROUTER_CONFIG_JSON\t" + com.google.gson.JsonParser.parseString(updated), lines.get(1));
		assertEquals("MODEL_TYPE\tMODEL_ROUTER", lines.get(2));
	}

	private ModelRouterEngine openRouter(String config, Function<String, String> skills) throws Exception {
		Path file = directory.resolve("router" + System.nanoTime() + ".json");
		Files.writeString(file, config);
		ModelRouterEngine router = new ModelRouterEngine() {
			@Override
			public File resolveConfigFile() {
				return file.toFile();
			}

			@Override
			protected String readSkillContent(String skillId) {
				return skills.apply(skillId);
			}
		};
		router.setBasic(true);
		Properties properties = new Properties();
		properties.setProperty(Constants.ENGINE, "router");
		properties.setProperty(Constants.ENGINE_ALIAS, "test-router");
		router.open(properties);
		return router;
	}
}
