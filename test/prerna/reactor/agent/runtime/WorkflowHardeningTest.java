package prerna.reactor.agent.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.config.AgentConfig;

class WorkflowHardeningTest {
    @TempDir Path root;

    @Test void packagedResourcesRejectDirectWritesParentMovesAndSymlinkAliases() throws Exception {
        var helper = root.resolve(".claude/skills/pptx/scripts/deck.js");
        Files.createDirectories(helper.getParent());
        Files.writeString(helper, "original");
        var protectedPaths = Set.of(".claude/skills/pptx");
        assertThrows(IllegalArgumentException.class, () -> ReadOnlyPathPolicy.requireWritable(root, helper, protectedPaths));
        assertThrows(IllegalArgumentException.class, () -> ReadOnlyPathPolicy.requireWritable(root, root.resolve(".claude"), protectedPaths));
        Files.createSymbolicLink(root.resolve("alias"), helper.getParent());
        assertThrows(IllegalArgumentException.class, () -> ReadOnlyPathPolicy.requireWritable(root, root.resolve("alias/deck.js"), protectedPaths));
        assertDoesNotThrow(() -> ReadOnlyPathPolicy.requireWritable(root, root.resolve("build-deck.js"), protectedPaths));
        assertDoesNotThrow(() -> ReadOnlyPathPolicy.requireWritable(root, root.resolve(".claude/skills/pptx-extra/file.js"), protectedPaths));
        assertEquals("original", Files.readString(helper));
    }

    @Test void realReadFileHandlerCanReadProtectedResourcesWhileMutationsRemainBlocked() throws Exception {
        var helper = root.resolve(".claude/skills/pptx/scripts/deck.js");
        Files.createDirectories(helper.getParent());
        Files.writeString(helper, "first line\nfunction cover() {}\nthird line\n");
        var ctx = mock(AgentRunContext.class);
        when(ctx.getAgentConfig()).thenReturn(AgentConfig.builder().workingDir(root.toString())
                .readOnlyPaths(Set.of(".claude/skills/pptx")).build());
        var handlers = PlatformAgentToolHandlers.handlersByName();
        String relative = ".claude/skills/pptx/scripts/deck.js";
        String result = handlers.get("ReadFile").execute(Map.of("path", relative, "offset", 2, "limit", 1), ctx);
        assertTrue(result.contains("function cover() {}"));
        assertFalse(result.contains("first line"));
        assertThrows(IllegalArgumentException.class, () -> handlers.get("WriteFile").execute(Map.of("path", relative, "content", "changed"), ctx));
        assertThrows(IllegalArgumentException.class, () -> handlers.get("EditFile").execute(Map.of("path", relative, "old_string", "cover", "new_string", "other"), ctx));
        assertThrows(IllegalArgumentException.class, () -> handlers.get("DeleteFile").execute(Map.of("path", relative), ctx));
        assertThrows(IllegalArgumentException.class, () -> handlers.get("MoveFile").execute(Map.of("path", ".claude", "new_path", "moved"), ctx));
        assertEquals("first line\nfunction cover() {}\nthird line\n", Files.readString(helper));
    }

    @Test void modelAndParallelToolTimingsRemainSeparate() {
        var nanos = new AtomicLong();
        var progress = new AgentRunProgress(40, 5, nanos::get);
        progress.beginModel();
        nanos.addAndGet(200_000_000);
        progress.endModel();
        progress.beginTool("ReadFile");
        nanos.addAndGet(10_000_000);
        progress.beginTool("ReadFile");
        nanos.addAndGet(20_000_000);
        progress.endTool("ReadFile", Map.of("path", "a"), true, "ok", 30);
        nanos.addAndGet(10_000_000);
        progress.endTool("ReadFile", Map.of("path", "b"), true, "ok", 30);
        var snapshot = progress.snapshot();
        assertEquals(200L, snapshot.get("modelTimeMs"));
        assertEquals(40L, snapshot.get("toolWallTimeMs"));
        assertEquals(60L, snapshot.get("toolTimeMs"));
        assertEquals(2, snapshot.get("toolCalls"));
    }

    @Test void repeatedFailuresSurviveInterveningReadsAndFinishingBudgetIsExplicit() {
        var progress = new AgentRunProgress(40, 5, () -> 0L);
        progress.beginTool("EditFile");
        progress.endTool("EditFile", Map.of("path", "build-deck.js"), false, "old_string missing", 1);
        progress.beginTool("ReadFile");
        progress.endTool("ReadFile", Map.of("path", "build-deck.js"), true, "content", 1);
        progress.beginTool("EditFile");
        progress.endTool("EditFile", Map.of("path", "build-deck.js"), false, "old_string missing", 1);
        progress.completedTurn(36);
        assertTrue(progress.guidance().contains("4 remaining"));
        assertTrue(progress.guidance().contains("Repeated failure: EditFile"));
        assertTrue(progress.guidance().contains("essential verification and delivery"));
        assertEquals(1, ((List<?>) progress.snapshot().get("repeatedFailures")).size());
        progress.completedTurn(40);
        assertTrue(progress.guidance().contains("Give the final response now"));
    }

    @Test void toolsPersistBeforeOneModelContinuationAndFinalRoundDisablesFurtherTools() {
        Room room = mock(Room.class);
        var messages = new ArrayList<AbstractMessage>();
        when(room.getMessages()).thenReturn(messages);
        when(room.getSystemPromptForModel()).thenReturn("Reviewer workflow");
        AgentRunContext ctx = mock(AgentRunContext.class);
        when(ctx.getRoom()).thenReturn(room);
        when(ctx.getMaxTurns()).thenReturn(1);
        when(ctx.getAgentConfig()).thenReturn(AgentConfig.builder().finishingTurns(1).build());
        when(room.continueAfterToolExecutionResultsWithRuntimeContext(anyMap(), any(), any(), any(), anyString(), anyString())).thenAnswer(call -> {
            assertEquals("none", ((Map<?, ?>) call.getArgument(0)).get("tool_choice"));
            assertEquals("Reviewer workflow", call.getArgument(4));
            assertTrue(((String) call.getArgument(5)).contains("No tool rounds remain"));
            messages.add(ResponseMessage.text("Saved deck; visual review unavailable."));
            return mock(AskModelEngineResponse.class);
        });
        try (var tools = mockStatic(PlatformAgentTools.class)) {
            tools.when(() -> PlatformAgentTools.isDefaultTool("ReadFile")).thenReturn(true);
            tools.when(() -> PlatformAgentTools.executeDefaultToolResult(eq("ReadFile"), anyMap(), eq(ctx)))
                    .thenReturn(ToolExecutionResult.success("content"));
            var state = new AgentLoopState();
            state.initializeProgress(ctx);
            var call = ResponseMessage.toolResponses(List.of(Map.of("id", "read-1", "name", "ReadFile", "input", Map.of("path", "build-deck.js"))));
            var response = HarnessToolExecutor.executeToolBatch(call, state, new HashMap<>(), ctx);
            assertEquals("Saved deck; visual review unavailable.", response.getContent());
            assertEquals(1, state.getIterations());
            var order = inOrder(room);
            order.verify(room).addToolExecutionResultWithoutModel(eq("read-1"), eq("ReadFile"), eq("content"), anyMap(), any(), any(), any(), eq("success"));
            order.verify(room).continueAfterToolExecutionResultsWithRuntimeContext(anyMap(), any(), any(), any(), anyString(), anyString());
            verify(room, never()).addToolExecutionResult(any(), any(), any(), any(), any(), any(), any(), any(), any());
        }
    }

    @Test void changingBudgetsDoNotChangeTheRunsCapturedSystemPrompt() {
        var room = mock(Room.class);
        when(room.getSystemPromptForModel()).thenReturn("Stable instructions", "Later unrelated room setting");
        var ctx = mock(AgentRunContext.class);
        when(ctx.getRoom()).thenReturn(room);
        when(ctx.getMaxTurns()).thenReturn(40);
        when(ctx.getAgentConfig()).thenReturn(AgentConfig.builder().build());
        var state = new AgentLoopState();
        state.initializeProgress(ctx);
        String first = state.runtimeContext();
        state.incrementIterations();
        assertTrue(first.contains("40 remaining"));
        assertTrue(state.runtimeContext().contains("39 remaining"));
        assertEquals("Stable instructions", state.systemPrompt());
        assertFalse(state.systemPrompt().contains("Run budget:"));
        verify(room, times(1)).getSystemPromptForModel();
    }

    @Test void resumedProgressRetainsBudgetAndTimingAndStopsClockOnFailure() {
        var clock = new AtomicLong(0);
        var first = new AgentRunProgress(40, 5, clock::get);
        first.beginModel();
        clock.set(200_000_000);
        first.endModel();
        first.completedTurn(36);
        first.close("input_required");
        var saved = first.snapshot();
        clock.set(5_000_000_000L);
        assertEquals(200L, first.snapshot().get("elapsedMs"));
        var resumed = new AgentRunProgress(40, 5, clock::get);
        resumed.restore(saved);
        assertEquals(36, resumed.completedTurns());
        assertTrue(resumed.guidance().contains("4 remaining"));
        resumed.beginModel();
        clock.addAndGet(100_000_000);
        resumed.close("failed");
        assertEquals(300L, resumed.snapshot().get("modelTimeMs"));
        assertEquals(300L, resumed.snapshot().get("elapsedMs"));
        assertEquals("failed", resumed.snapshot().get("phase"));
    }

    @Test void callerVisionEngineOverridesConfiguredToolDefaultAtDispatch() throws Exception {
        var ctx = mock(AgentRunContext.class);
        var room = mock(Room.class);
        when(ctx.getRoom()).thenReturn(room);
        when(room.getId()).thenReturn("author-room");
        when(ctx.getAgentConfig()).thenReturn(AgentConfig.builder().workingDir(root.toString())
                .modelId("tool-calling-model")
                .toolParameterDefaults(Map.of("InspectPptx", Map.of("engine", "configured-vision"))).build());
        try (var utility = mockStatic(prerna.util.Utility.class);
             var inspector = mockStatic(prerna.util.pptx.SemossPptxInspector.class)) {
            inspector.when(() -> prerna.util.pptx.SemossPptxInspector.inspect(any(), anyMap(), any(), any(), any()))
                    .thenAnswer(call -> new org.json.JSONObject((Map<?, ?>) call.getArgument(1)));
            var args = Map.<String, Object>of("filePath", "deck.pptx", "instructions", "Check layout");
            var defaultResult = new org.json.JSONObject(PlatformAgentTools.executeDefaultTool("InspectPptx", args, ctx));
            assertEquals("configured-vision", defaultResult.getString("engine"));
            var explicit = new HashMap<>(args);
            explicit.put("engine", "new-provider-model-id");
            var result = new org.json.JSONObject(PlatformAgentTools.executeDefaultTool("InspectPptx", explicit, ctx));
            assertEquals("new-provider-model-id", result.getString("engine"));
            assertEquals("Check layout", result.getString("instructions"));
            assertFalse(args.containsKey("engine"));
        }
    }


    @Test void shellCommandFailureRetainsItsExitStatus() {
        var command = new prerna.util.CmdExecUtil(null, "workflow-test", root.toString());
        var failure = command.executeCommandWithStatus("semoss_missing_command_workflow_test");
        assertEquals("false", failure[0]);
        assertFalse(failure[1].isBlank());
        var success = command.executeCommandWithStatus("pwd");
        assertEquals("true", success[0]);
        assertTrue(success[1].contains(root.getFileName().toString()));
    }

}
