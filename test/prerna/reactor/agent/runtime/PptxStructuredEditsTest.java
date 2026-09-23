package prerna.reactor.agent.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
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
	@org.junit.jupiter.api.io.TempDir java.nio.file.Path root;

	@Test
	void invalidEditArgumentsReturnMatchingToolErrorsAndAllowASecondModelTurn() {
		for (String name : List.of("ApplyPptxEdits", "PreparePptxEdit")) {
			var room = mock(Room.class);
			when(room.getMessages()).thenReturn(new ArrayList<>());
			var ctx = mock(prerna.reactor.agent.AgentRunContext.class);
			when(ctx.getRoom()).thenReturn(room); when(ctx.getMaxTurns()).thenReturn(40);
			when(ctx.getAgentConfig()).thenReturn(prerna.reactor.agent.config.AgentConfig.builder().build());
			var operations = mock(PptxWorkflow.Operations.class);
			var workflow = new PptxWorkflow(root, root.resolve("state"), 6, operations);
			var state = new AgentLoopState(workflow);
			var call = ResponseMessage.toolResponses(List.of(Map.of("id", "bad-plan", "name", name, "input", Map.of())));
			assertDoesNotThrow(() -> HarnessToolExecutor.executeToolBatch(call, state, new java.util.HashMap<>(), ctx));
			assertFalse(state.isTerminal()); assertFalse(workflow.isTerminal());
			assertFalse(state.getToolCallRecords().getFirst().isSuccess());
			assertTrue(state.getToolCallRecords().getFirst().getResult().startsWith("Edit rejected:"));
			verify(room).addToolExecutionResultWithoutModel(eq("bad-plan"), eq(name), startsWith("Edit rejected:"), anyMap(), any(), any(), any(), eq("error"));
			verify(room).continueAfterToolExecutionResultsWithRuntimeContext(anyMap(), any(), any(), any(), any(), anyString());
			verifyNoInteractions(operations);
		}
	}

	@Test
	void incompatibleSystemReviewerIsRejectedBeforeSpawningOrCallingAModel() throws Exception {
		var ctx = mock(prerna.reactor.agent.AgentRunContext.class);
		var config = prerna.reactor.agent.config.AgentConfig.builder()
				.subagents(List.of(new prerna.reactor.agent.config.SubAgentSpec("agent_pptx_reviewer", "pptx-reviewer", "Review")))
				.pptxWorkflow(Map.of()).build();
		when(ctx.getAgentConfig()).thenReturn(config);
		var model = mock(prerna.engine.api.IModelEngine.class);
		when(model.getEngineId()).thenReturn("text-author"); when(ctx.getModelEngine()).thenReturn(model);
		var insight = mock(prerna.om.Insight.class); var user = mock(prerna.auth.User.class);
		when(insight.getUser()).thenReturn(user); when(ctx.getInsight()).thenReturn(insight);
		try (var configs = mockStatic(prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils.class);
				var security = mockStatic(prerna.auth.utils.SecurityEngineUtils.class);
				var metadata = mockStatic(prerna.auth.utils.SecurityModelMetadataUtils.class);
				var utility = mockStatic(prerna.util.Utility.class);
				var dispatcher = mockStatic(prerna.reactor.agent.subagent.SubAgentDispatcher.class)) {
			configs.when(() -> prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils.getWorkspaceConfigJson("pptx-reviewer"))
					.thenReturn(new JSONObject().put("model_id", "text-reviewer"));
			security.when(() -> prerna.auth.utils.SecurityEngineUtils.userCanViewEngine(user, "text-reviewer")).thenReturn(true);
			security.when(() -> prerna.auth.utils.SecurityEngineUtils.getEngineType("text-reviewer")).thenReturn(prerna.engine.api.IEngine.CATALOG_TYPE.MODEL);
			metadata.when(() -> prerna.auth.utils.SecurityModelMetadataUtils.getModelMetadata("text-reviewer"))
					.thenReturn(Map.of("inputModalities", List.of("TEXT")));
			var error = assertThrows(IllegalArgumentException.class, () -> new PptxWorkflowOperations(ctx).review("deck.pptx", List.of(3), "Check contrast", null));
			assertTrue(error.getMessage().contains("does not support image input"));
			dispatcher.verifyNoInteractions(); verify(model, never()).ask(anyString(), anyString(), any(), anyMap());
			utility.verify(() -> prerna.util.Utility.getModel(anyString()), never());
		}
	}

	@Test
	void reviewerPlainTextErrorPreservesTheOriginalCause() {
		var result = new JSONObject().put("status", "succeeded").put("result", "Tool execution error: Selected model does not support image input");
		String error = assertThrows(IllegalStateException.class, () -> PptxWorkflowOperations.parseReviewResult(result, "child")).getMessage();
		assertTrue(error.contains("does not support image input"));
		assertFalse(error.contains("JSONObject"));
		assertThrows(IllegalStateException.class, () -> PptxWorkflowOperations.parseReviewResult(new JSONObject().put("status", "failed").put("error", "provider unavailable"), "child"));
		assertThrows(IllegalStateException.class, () -> PptxWorkflowOperations.parseReviewResult(new JSONObject().put("status", "succeeded").put("result", "{broken"), "child"));
		assertEquals("child", PptxWorkflowOperations.parseReviewResult(new JSONObject().put("status", "succeeded").put("result", "{\"status\":\"complete\"}"), "child").getString("reviewerRunId"));
	}

	@Test
	void boundedContextRetainsOriginalAndRecentRequestsWithoutGeneratorCode() {
		Room room = new Room(); room.setId("context-test"); List<AbstractMessage> history = new ArrayList<>();
		for (int i = 0; i < 12; i++) {
			var input = InputMessage.builder(room).withText("Wrapper\nPrimary brief: request-" + i + "\n\nBefore finishing, save deck").build();
			input.setOrnament("agentRunRole", "input"); history.add(input);
			history.add(ResponseMessage.text("old generator code".repeat(1000)));
		}
		String context = PptxEditContext.priorRequests(history);
		assertTrue(context.contains("request-0") && context.contains("request-11"));
		assertFalse(context.contains("request-6") || context.contains("old generator") || context.contains("Before finishing"));
		assertTrue(context.length() < 6300);
	}

	@Test
	void operationValidationRejectsUnknownFieldsDuplicatesAndCodeIsSerializedAsData() {
		JSONObject contract = new JSONObject().put("editType", "slides").put("allowedParts", List.of("ppt/slides/slide1.xml"));
		var op = Map.of("type", "setBackground", "part", "ppt/slides/slide1.xml", "color", "000000");
		assertThrows(IllegalArgumentException.class, () -> PptxStructuredEdits.validate(List.of(op, op), contract));
		assertThrows(IllegalArgumentException.class, () -> PptxStructuredEdits.validate(List.of(Map.of("type", "setBackground", "part", "ppt/slides/slide1.xml", "color", "000000", "extra", "bad")), contract));
		assertThrows(IllegalArgumentException.class, () -> PptxStructuredEdits.validate(List.of(Map.of("type", "setBackground", "part", "ppt/slides/slide1.xml", "color", "black")), contract));
		String text = "\"; throw new Error('not code'); //\n";
		var plan = PptxStructuredEdits.validate(List.of(Map.of("type", "replaceText", "part", "ppt/slides/slide1.xml", "objectId", "2", "index", 0, "oldText", "a", "newText", text)), contract);
		assertTrue(PptxStructuredEdits.generator("snapshot.pptx", "output.pptx", plan).contains(JSONObject.quote(text)));
	}
}
