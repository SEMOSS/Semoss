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
package prerna.reactor.agent.runtime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;

class PptxStructuredEditsTest {

	@org.junit.jupiter.api.io.TempDir
	java.nio.file.Path root;

	@Test
	void invalidEditArgumentsReturnMatchingToolErrorsAndAllowASecondModelTurn() {
		for (String name : List.of("ApplyPptxEdits", "PreparePptxEdit")) {
			var room = mock(Room.class);
			when(room.getMessages()).thenReturn(new ArrayList<>());
			var ctx = mock(prerna.reactor.agent.AgentRunContext.class);
			when(ctx.getRoom()).thenReturn(room);
			when(ctx.getMaxTurns()).thenReturn(40);
			when(ctx.getAgentConfig()).thenReturn(prerna.reactor.agent.config.AgentConfig.builder().build());
			var operations = mock(PptxWorkflow.Operations.class);
			var workflow = new PptxWorkflow(root, root.resolve("state"), 6, operations);
			var state = new AgentLoopState(workflow);
			var call = ResponseMessage
					.toolResponses(List.of(Map.of("id", "bad-plan", "name", name, "input", Map.of())));
			assertDoesNotThrow(() -> HarnessToolExecutor.executeToolBatch(call, state, new java.util.HashMap<>(), ctx));
			assertFalse(state.isTerminal());
			assertFalse(workflow.isTerminal());
			assertFalse(state.getToolCallRecords().getFirst().isSuccess());
			assertTrue(state.getToolCallRecords().getFirst().getResult().startsWith("Edit rejected:"));
			verify(room).addToolExecutionResultWithoutModel(eq("bad-plan"), eq(name), startsWith("Edit rejected:"),
					anyMap(), any(), any(), any(), eq("error"));
			verify(room).continueAfterToolExecutionResultsWithRuntimeContext(anyMap(), any(), any(), any(), any(),
					anyString());
			verifyNoInteractions(operations);
		}
	}

	@Test
	void incompatibleSystemReviewerIsRejectedBeforeSpawningOrCallingAModel() throws Exception {
		var ctx = mock(prerna.reactor.agent.AgentRunContext.class);
		var config = prerna.reactor.agent.config.AgentConfig.builder()
				.subagents(List.of(
						new prerna.reactor.agent.config.SubAgentSpec("agent_pptx_reviewer", "pptx-reviewer", "Review")))
				.pptxWorkflow(Map.of()).build();
		when(ctx.getAgentConfig()).thenReturn(config);
		var model = mock(prerna.engine.api.IModelEngine.class);
		when(model.getEngineId()).thenReturn("text-author");
		when(ctx.getModelEngine()).thenReturn(model);
		var insight = mock(prerna.om.Insight.class);
		var user = mock(prerna.auth.User.class);
		when(insight.getUser()).thenReturn(user);
		when(ctx.getInsight()).thenReturn(insight);
		try (var configs = mockStatic(prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils.class);
				var security = mockStatic(prerna.auth.utils.SecurityEngineUtils.class);
				var metadata = mockStatic(prerna.auth.utils.SecurityModelMetadataUtils.class);
				var utility = mockStatic(prerna.util.Utility.class);
				var dispatcher = mockStatic(prerna.reactor.agent.subagent.SubAgentDispatcher.class)) {
			configs.when(() -> prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils
					.getWorkspaceConfigJson("pptx-reviewer"))
					.thenReturn(new JSONObject().put("model_id", "text-reviewer"));
			security.when(() -> prerna.auth.utils.SecurityEngineUtils.userCanViewEngine(user, "text-reviewer"))
					.thenReturn(true);
			security.when(() -> prerna.auth.utils.SecurityEngineUtils.getEngineType("text-reviewer"))
					.thenReturn(prerna.engine.api.IEngine.CATALOG_TYPE.MODEL);
			metadata.when(() -> prerna.auth.utils.SecurityModelMetadataUtils.getModelMetadata("text-reviewer"))
					.thenReturn(Map.of("inputModalities", List.of("TEXT")));
			var error = assertThrows(IllegalArgumentException.class,
					() -> new PptxWorkflowOperations(ctx).review("deck.pptx", List.of(3), "Check contrast", null));
			assertTrue(error.getMessage().contains("does not support image input"));
			dispatcher.verifyNoInteractions();
			verify(model, never()).ask(anyString(), anyString(), any(), anyMap());
			utility.verify(() -> prerna.util.Utility.getModel(anyString()), never());
		}
	}

	@Test
	void reviewerPlainTextErrorPreservesTheOriginalCause() {
		var result = new JSONObject().put("status", "succeeded").put("result",
				"Tool execution error: Selected model does not support image input");
		String error = assertThrows(IllegalStateException.class,
				() -> PptxWorkflowOperations.parseReviewResult(result, "child")).getMessage();
		assertTrue(error.contains("does not support image input"));
		assertFalse(error.contains("JSONObject"));
		assertThrows(IllegalStateException.class, () -> PptxWorkflowOperations.parseReviewResult(
				new JSONObject().put("status", "failed").put("error", "provider unavailable"), "child"));
		assertThrows(IllegalStateException.class, () -> PptxWorkflowOperations
				.parseReviewResult(new JSONObject().put("status", "succeeded").put("result", "{broken"), "child"));
		assertEquals("child",
				PptxWorkflowOperations.parseReviewResult(
						new JSONObject().put("status", "succeeded").put("result", "{\"status\":\"complete\"}"), "child")
						.getString("reviewerRunId"));
	}

	@Test
	void buildReadsTheCompleteReportBeyondTheNodeOutputCap() throws Exception {
		var ctx = mock(prerna.reactor.agent.AgentRunContext.class);
		when(ctx.getRunId()).thenReturn("run-1");
		when(ctx.getAgentConfig())
				.thenReturn(prerna.reactor.agent.config.AgentConfig.builder().workingDir(root.toString()).build());
		var warnings = new org.json.JSONArray();
		for (int i = 0; i < 500; i++) {
			warnings.put("Slide " + (i % 18 + 1)
					+ ", \"body\": Text includes 12 pt type. Consider shortening the copy or allocating more space.");
		}
		String report = new JSONObject().put("ok", true).put("slides", 18).put("warnings", warnings).toString();
		assertTrue(report.length() > 40_000, "ExecuteNodeCode truncates output beyond 40,000 characters");
		Map<String, Object> args = Map.of("generator", "build-deck.js", "filePath", "deck.pptx", "expectedSlides", 18);
		var written = new ArrayList<java.nio.file.Path>();
		try (var tools = mockStatic(PlatformAgentTools.class)) {
			tools.when(() -> PlatformAgentTools.executeDefaultTool(eq("ExecuteNodeCode"), anyMap(), eq(ctx)))
					.thenAnswer(call -> {
						String code = String.valueOf(((Map<?, ?>) call.getArgument(1)).get("code"));
						var target = java.util.regex.Pattern
								.compile("writeFileSync\\(path\\.join\\(ROOT, \"([^\"]+)\"\\)").matcher(code);
						assertTrue(target.find());
						written.add(root.resolve(target.group(1)));
						java.nio.file.Files.writeString(written.getLast(), report);
						// Generator console output comes first and may itself start with "Error:".
						return "Error: logo.png not found, using a text mark\n\n=> Structural validation saved";
					});
			var result = new PptxWorkflowOperations(ctx).build(args);
			assertEquals(500, result.getJSONArray("warnings").length());
			assertTrue(result.getBoolean("ok"));
			assertTrue(written.getFirst().startsWith(root.resolve(".semoss/pptx-workflow/run-1")));
			assertFalse(java.nio.file.Files.exists(written.getFirst()), "The transient report is removed");

			tools.when(() -> PlatformAgentTools.executeDefaultTool(eq("ExecuteNodeCode"), anyMap(), eq(ctx)))
					.thenReturn("Error: Generator did not save the requested PPTX file");
			String error = assertThrows(IllegalStateException.class, () -> new PptxWorkflowOperations(ctx).build(args))
					.getMessage();
			assertTrue(error.contains("did not save the requested PPTX file"));
		}
	}

	@Test
	void boundedContextRetainsOriginalAndRecentRequestsWithoutGeneratorCode() {
		Room room = new Room();
		room.setId("context-test");
		List<AbstractMessage> history = new ArrayList<>();
		for (int i = 0; i < 12; i++) {
			var input = InputMessage.builder(room)
					.withText("Wrapper\nPrimary brief: request-" + i + "\n\nBefore finishing, save deck").build();
			input.setOrnament("agentRunRole", "input");
			history.add(input);
			history.add(ResponseMessage.text("old generator code".repeat(1000)));
		}
		String context = PptxEditContext.priorRequests(history);
		assertTrue(context.contains("request-0") && context.contains("request-11"));
		assertFalse(context.contains("request-6") || context.contains("old generator")
				|| context.contains("Before finishing"));
		assertTrue(context.length() < 6300);
	}

	@Test
	void operationValidationRejectsUnknownFieldsDuplicatesAndCodeIsSerializedAsData() {
		JSONObject contract = new JSONObject().put("editType", "slides").put("allowedParts",
				List.of("ppt/slides/slide1.xml"));
		var op = Map.of("type", "setBackground", "part", "ppt/slides/slide1.xml", "color", "000000");
		assertThrows(IllegalArgumentException.class, () -> PptxStructuredEdits.validate(List.of(op, op), contract));
		assertThrows(IllegalArgumentException.class, () -> PptxStructuredEdits.validate(List.of(
				Map.of("type", "setBackground", "part", "ppt/slides/slide1.xml", "color", "000000", "extra", "bad")),
				contract));
		assertThrows(IllegalArgumentException.class,
				() -> PptxStructuredEdits.validate(
						List.of(Map.of("type", "setBackground", "part", "ppt/slides/slide1.xml", "color", "black")),
						contract));
		String text = "\"; throw new Error('not code'); //\n";
		var plan = PptxStructuredEdits.validate(List.of(Map.of("type", "replaceText", "part", "ppt/slides/slide1.xml",
				"objectId", "2", "index", 0, "oldText", "a", "newText", text)), contract);
		assertTrue(
				PptxStructuredEdits.generator("snapshot.pptx", "output.pptx", plan).contains(JSONObject.quote(text)));
	}
}
