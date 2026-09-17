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
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.config.AgentConfig;

class PptxWorkflowTest {
    @TempDir Path root;
    FakeOperations operations;
    PptxWorkflow workflow;
    Map<String, Object> args = Map.of("generator", "build-deck.js", "filePath", "deck.pptx", "expectedSlides", 2,
            "instructions", "Preserve the user's layout", "engine", "arbitrary-caller-model-id");

    @BeforeEach void setup() throws Exception {
        Files.writeString(root.resolve("build-deck.js"), "generator v1");
        operations = new FakeOperations();
        workflow = new PptxWorkflow(root, root.resolve(".semoss/pptx-workflow/test"), 6, operations);
    }

    @Test void firstValidSaveReviewsImmediatelyDespiteAdvisoryWarnings() throws Exception {
        var result = workflow.build(args, 4);
        assertTrue(workflow.isTerminal());
        assertNull(workflow.completionError());
        assertEquals("complete", result.getString("status"));
        assertFalse(result.has("validation"), "Do not return large advisory lists to the author");
        assertEquals(List.of(List.of(1, 2)), operations.scopes);
        assertEquals("arbitrary-caller-model-id", operations.engine);
        assertTrue(operations.instructions.contains("warning 10"));
        assertTrue(workflow.finalText().contains("deck.pptx"));
        assertTrue(workflow.finalText().contains("Full-deck visual review completed"));
        assertEquals(PptxWorkflow.hash(root.resolve("deck.pptx")), workflow.snapshot().getString("sourceHash"));
    }

    @Test void minorFindingsDeliverWithoutAnotherRepairCycle() {
        operations.severity = "minor";
        workflow.build(args, 4);
        assertTrue(workflow.isTerminal());
        assertNull(workflow.completionError());
        assertTrue(workflow.finalText().contains("Advisory"));
        assertEquals(1, operations.reviewCalls);
    }

    @Test void sixRepairRoundsFinishWithOneBuildOfTheChangedGenerator() throws Exception {
        operations.severity = "major";
        workflow.build(args, 4);
        assertFalse(workflow.isTerminal());
        String saved = PptxWorkflow.hash(root.resolve("deck.pptx"));
        for (int round = 5; round < 10; round++) {
            Files.writeString(root.resolve("build-deck.js"), "another optional edit " + round);
            workflow.afterRound(round, 40);
            assertFalse(workflow.isTerminal());
        }
        operations.slide2 = "final saved correction";
        workflow.afterRound(10, 40);
        assertTrue(workflow.isTerminal());
        assertNull(workflow.completionError());
        assertNotNull(workflow.completionWarning());
        assertFalse(workflow.finalText().contains("not included"));
        assertNotEquals(saved, PptxWorkflow.hash(root.resolve("deck.pptx")));
        assertEquals(2, operations.buildCalls);
        assertEquals(2, operations.reviewCalls);
        workflow.afterRound(11, 40);
        assertEquals(2, operations.buildCalls);
    }

    @Test void oneRepairRechecksTheAffectedOriginalSlides() throws Exception {
        operations.severity = "major";
        workflow.build(args, 4);
        operations.severity = null;
        operations.slide2 = "fixed caption";
        Files.writeString(root.resolve("build-deck.js"), "fixed generator");
        workflow.build(args, 6);
        assertNull(workflow.completionError());
        assertEquals(List.of(List.of(1, 2), List.of(2)), operations.scopes);
        assertTrue(workflow.finalText().contains("revised whole deck was not rechecked"));
    }

    @Test void aRepairThatChangesOtherSlidesOrSharedAssetsExpandsCoverage() throws Exception {
        for (boolean shared : List.of(false, true)) {
            setup();
            operations.severity = "major";
            workflow.build(args, 4);
            operations.severity = null;
            if (shared) operations.theme = "new theme"; else operations.slide1 = "unrequested change";
            workflow.build(args, 6);
            assertEquals(List.of(1, 2), operations.scopes.get(1));
            assertNull(workflow.completionError());
        }
    }

    @Test void unresolvedMajorFindingsStopAfterOneRepairAndTwoReviews() {
        operations.severity = "major";
        workflow.build(args, 4);
        workflow.build(args, 6);
        assertTrue(workflow.isTerminal());
        assertNull(workflow.completionError());
        assertTrue(workflow.completionWarning().contains("Significant visual findings remain"));
        assertEquals("needs_changes", workflow.snapshot().getJSONObject("reviewOutcome").getString("verdict"));
        workflow.build(args, 7);
        assertEquals(2, operations.buildCalls);
        assertEquals(2, operations.reviewCalls);
    }

    @Test void providerFailureMissingCoverageWrongHashAndWrongFileNeverTriggerLayoutRepair() throws Exception {
        for (String corruption : List.of("provider", "coverage", "hash", "file", "inconclusive")) {
            setup();
            operations.corruption = corruption;
            workflow.build(args, 4);
            assertTrue(workflow.isTerminal(), corruption);
            assertNull(workflow.completionError(), corruption);
            assertNotNull(workflow.completionWarning(), corruption);
            assertEquals("complete_with_warnings", workflow.toolResult().getString("status"));
            assertEquals("available", workflow.snapshot().getJSONObject("artifact").getString("status"));
            assertEquals("incomplete", workflow.snapshot().getJSONObject("reviewOutcome").getString("status"));
            assertEquals("inconclusive", workflow.snapshot().getJSONObject("reviewOutcome").getString("verdict"));
            assertTrue(workflow.finalText().contains("has not passed visual review"));
            assertEquals(1, operations.buildCalls);
            assertEquals(1, operations.reviewCalls);
        }
    }

    @Test void aFileChangedDuringReviewIsRestoredAndCannotPass() throws Exception {
        operations.corruption = "changed";
        workflow.build(args, 4);
        assertTrue(workflow.isTerminal());
        assertNull(workflow.completionError());
        assertNotNull(workflow.completionWarning());
        assertEquals(workflow.snapshot().getString("sourceHash"), PptxWorkflow.hash(root.resolve("deck.pptx")));
    }

    @Test void anInconclusiveReviewWithMajorFindingsStillStopsWithoutRepair() {
        operations.corruption = "inconclusive";
        operations.severity = "major";
        workflow.build(args, 4);
        assertTrue(workflow.isTerminal());
        assertNull(workflow.completionError());
        assertNotNull(workflow.completionWarning());
        assertEquals(1, operations.buildCalls);
        assertEquals(1, operations.reviewCalls);
    }

    @Test void structuralErrorsGetOneBoundedAttemptAndNoReviewUntilValid() {
        operations.valid = false;
        workflow.build(args, 4);
        assertFalse(workflow.isTerminal());
        assertEquals(0, operations.reviewCalls);
        workflow.build(args, 6);
        assertTrue(workflow.isTerminal());
        assertNotNull(workflow.completionError());
        assertEquals("unavailable", workflow.snapshot().getJSONObject("artifact").getString("status"));
        assertEquals(0, operations.reviewCalls);
    }

    @Test void invalidRepairRecoversThePreviousValidatedFileAndItsEvidence() throws Exception {
        operations.severity = "major";
        workflow.build(args, 4);
        String validHash = PptxWorkflow.hash(root.resolve("deck.pptx"));
        operations.valid = false;
        operations.slide2 = "invalid rebuild";
        workflow.build(args, 6);
        assertNull(workflow.completionError());
        assertTrue(workflow.completionWarning().contains("bad XML"));
        assertEquals(validHash, PptxWorkflow.hash(root.resolve("deck.pptx")));
        assertTrue(workflow.snapshot().getJSONObject("validation").getBoolean("ok"));
        assertEquals("available", workflow.snapshot().getJSONObject("artifact").getString("status"));
        assertEquals(1, operations.reviewCalls);
    }

    @Test void inconclusiveOverviewExplainsTheWarningAndPersistsSeparateArtifactEvidence() throws Exception {
        operations.corruption = "overview";
        operations.severity = "major";
        var progress = new java.util.concurrent.atomic.AtomicReference<Map<String, Object>>();
        workflow.onProgress(progress::set);
        var result = workflow.build(args, 4);
        assertNull(workflow.completionError());
        assertEquals("delivered_with_warnings", workflow.phase());
        assertTrue(workflow.completionWarning().contains("overview=inconclusive"));
        assertTrue(workflow.completionWarning().contains("Images are placeholders"));
        assertTrue(workflow.finalText().contains("Slide 2 (major)"));
        assertEquals("available", result.getJSONObject("artifact").getString("status"));
        assertEquals("incomplete", result.getJSONObject("reviewOutcome").getString("status"));
        JSONObject saved = new JSONObject(Files.readString(root.resolve(".semoss/pptx-workflow/test/state.json")));
        assertEquals(result.getJSONObject("artifact").toMap(), saved.getJSONObject("artifact").toMap());
        assertEquals(workflow.completionWarning(), progress.get().get("warning"));
        assertEquals(1, operations.reviewCalls);
        var resumed = new PptxWorkflow(root, root.resolve(".semoss/pptx-workflow/test"), 6, operations);
        var restore = PptxWorkflow.class.getDeclaredMethod("restore");
        restore.setAccessible(true);
        restore.invoke(resumed);
        assertTrue(resumed.isTerminal());
        assertNull(resumed.completionError());
        assertEquals(workflow.completionWarning(), resumed.completionWarning());
        assertEquals(saved.getJSONObject("artifact").toMap(), resumed.snapshot().getJSONObject("artifact").toMap());
    }

    @Test void failedRecheckDoesNotReuseThePreviousVersionsPassingReview() throws Exception {
        operations.severity = "major";
        workflow.build(args, 4);
        operations.slide2 = "repaired caption";
        operations.corruption = "provider";
        Files.writeString(root.resolve("build-deck.js"), "updated generator");
        workflow.build(args, 6);
        assertNull(workflow.completionError());
        assertNotNull(workflow.completionWarning());
        assertFalse(workflow.snapshot().has("review"));
        assertEquals("incomplete", workflow.snapshot().getJSONObject("reviewOutcome").getString("status"));
        assertFalse(workflow.finalText().contains("Caption clipped"));
    }

    @Test void reviewWarningCannotMakeACorruptOrMissingArtifactDeliverable() {
        operations.corruption = "destroy";
        workflow.build(args, 4);
        assertNotNull(workflow.completionError());
        assertNull(workflow.completionWarning());
        assertEquals("unavailable", workflow.snapshot().getJSONObject("artifact").getString("status"));
        assertFalse(workflow.finalText().contains("Structural checks passed"));
    }

    @Test void reviewWarningCannotHideGeneratorEditsThatWereNeverBuilt() {
        operations.corruption = "edit_generator";
        workflow.build(args, 4);
        assertNull(workflow.completionError());
        assertNotNull(workflow.completionWarning());
        assertTrue(workflow.finalText().contains("not included"));
    }

    @Test void passingReviewCannotHideGeneratorEditsThatWereNeverBuilt() {
        operations.corruption = "edit_generator_after_pass";
        workflow.build(args, 4);
        assertNull(workflow.completionError());
        assertNotNull(workflow.completionWarning());
        assertTrue(workflow.finalText().contains("not included"));
    }

    @Test void literalToolMarkupAndEarlyClaimsCannotCompleteTheWorkflow() throws Exception {
        for (String text : List.of("<tool_call>ExecuteNodeCode<arg_key>code</arg_key>...</tool_call>", "Done, everything passed.")) {
            setup();
            workflow.modelStopped(text);
            assertTrue(workflow.isTerminal());
            assertNotNull(workflow.completionError());
            assertFalse(workflow.finalText().contains("<tool_call>"));
            assertEquals(0, operations.buildCalls);
        }
    }

    @Test void pathsCannotEscapeAndRepairCannotChangeTheDeliveryTarget() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> workflow.resolve("../deck.pptx", ".pptx"));
        assertThrows(IllegalArgumentException.class, () -> workflow.resolve(".claude/a.js", ".js"));
        Path outside = Files.createTempDirectory("pptx-outside-");
        try {
            Files.createSymbolicLink(root.resolve("alias"), outside);
            assertThrows(IllegalArgumentException.class, () -> workflow.resolve("alias/deck.pptx", ".pptx"));
        } finally { Files.delete(outside); }
        operations.severity = "major";
        workflow.build(args, 4);
        var changed = new HashMap<>(args); changed.put("filePath", "other.pptx");
        workflow.build(changed, 6);
        assertNull(workflow.completionError());
        assertTrue(workflow.completionWarning().contains("original generator"));
        assertFalse(Files.exists(root.resolve("other.pptx")));
    }

    @Test void lastRepairTurnCorrectionIsBuiltBeforeDelivery() throws Exception {
        operations.valid = false;
        workflow.build(args, 5);
        for (int round = 6; round <= 10; round++) workflow.afterRound(round, 40);
        assertFalse(workflow.isTerminal());
        operations.valid = true;
        Files.writeString(root.resolve("build-deck.js"), "corrected background");
        workflow.afterRound(11, 40);
        assertNull(workflow.completionError());
        assertNull(workflow.completionWarning());
        assertEquals("available", workflow.snapshot().getJSONObject("artifact").getString("status"));
        assertTrue(workflow.snapshot().getBoolean("finalBuildAttempted"));
        assertEquals(2, operations.buildCalls);
        assertEquals(1, operations.reviewCalls);
    }

    @Test void failedFinalBuildDeliversThePreviousValidatedDeck() throws Exception {
        operations.severity = "major";
        workflow.build(args, 5);
        String savedHash = PptxWorkflow.hash(root.resolve("deck.pptx"));
        Files.writeString(root.resolve("build-deck.js"), "broken repair");
        operations.valid = false;
        operations.slide2 = "broken rebuild";
        workflow.afterRound(11, 40);
        assertNull(workflow.completionError());
        assertTrue(workflow.completionWarning().contains("bad XML"));
        assertTrue(workflow.finalText().contains("not included"));
        assertEquals(savedHash, PptxWorkflow.hash(root.resolve("deck.pptx")));
        assertEquals("available", workflow.snapshot().getJSONObject("artifact").getString("status"));
        assertEquals(2, operations.buildCalls);
        assertEquals(1, operations.reviewCalls);
    }

    @Test void unchangedBrokenGeneratorIsNotRetriedAndTheOriginalErrorIsRetained() {
        operations.valid = false;
        workflow.build(args, 5);
        workflow.afterRound(11, 40);
        assertNotNull(workflow.completionError());
        assertTrue(workflow.completionError().contains("bad XML"));
        assertFalse(workflow.finalText().contains("last successful build"));
        assertFalse(workflow.snapshot().getBoolean("finalBuildAttempted"));
        assertEquals(1, operations.buildCalls);
    }

    @Test void finalBuildFailureWithoutAnyValidatedVersionStillFailsHonestly() throws Exception {
        operations.valid = false;
        workflow.build(args, 5);
        Files.writeString(root.resolve("build-deck.js"), "still broken");
        workflow.afterRound(11, 40);
        assertNotNull(workflow.completionError());
        assertEquals("unavailable", workflow.snapshot().getJSONObject("artifact").getString("status"));
        assertEquals(2, operations.buildCalls);
        assertEquals(0, operations.reviewCalls);
    }

    @Test void modelStoppingOrGlobalTurnLimitStillBuildsPendingCorrections() throws Exception {
        for (boolean globalLimit : List.of(false, true)) {
            setup();
            operations.valid = false;
            workflow.build(args, 5);
            Files.writeString(root.resolve("build-deck.js"), "fixed generator");
            operations.valid = true;
            if (globalLimit) workflow.afterRound(6, 6); else workflow.modelStopped("Finished editing");
            assertTrue(workflow.isTerminal());
            assertNull(workflow.completionError());
            assertEquals(2, operations.buildCalls);
            assertEquals(1, operations.reviewCalls);
        }
    }

    @Test void actualHarnessDeliversSavedDeckWhenModelContinuationFails() throws Exception {
        operations.severity = "major";
        workflow.build(args, 5);
        String savedHash = PptxWorkflow.hash(root.resolve("deck.pptx"));
        var room = mock(Room.class);
        var messages = new ArrayList<AbstractMessage>();
        messages.add(ResponseMessage.toolResponses(List.of(Map.of("id", "read-generator", "name", "ReadFile",
                "input", Map.of("path", "build-deck.js")))));
        when(room.getMessages()).thenReturn(messages);
        when(room.getOptionsMap()).thenReturn(new HashMap<>());
        when(room.getSystemPromptForModel()).thenReturn("Stable PPTX author instructions");
        when(room.getId()).thenReturn("delivery-test-room");
        when(room.continueAfterToolExecutionResultsWithRuntimeContext(anyMap(), any(), any(), any(), anyString(), anyString()))
                .thenThrow(new IllegalStateException("Model connection failed after saved build"));
        when(room.appendHarnessResponse(anyString(), any(), any(), any())).thenAnswer(call -> {
            var response = ResponseMessage.text(call.getArgument(0));
            messages.add(response);
            return response;
        });
        var ctx = mock(AgentRunContext.class);
        when(ctx.getRoom()).thenReturn(room);
        when(ctx.getParamMap()).thenReturn(Map.of());
        when(ctx.isResumeMode()).thenReturn(true);
        when(ctx.getMaxTurns()).thenReturn(40);
        when(ctx.getAgentConfig()).thenReturn(AgentConfig.builder().workingDir(root.toString())
                .pptxWorkflow(Map.of("enabled", true)).build());
        try (var factory = mockStatic(PptxWorkflow.class, CALLS_REAL_METHODS)) {
            factory.when(() -> PptxWorkflow.create(ctx)).thenReturn(workflow);
            var result = new SemossAgentHarness().execute(ctx);
            assertNull(result.getCompletionError());
            assertTrue(result.getFinalText().contains("Model connection failed after saved build"));
            assertTrue(result.getFinalText().contains("deck.pptx"));
            assertNotNull(result.getFinalOutputMessageId());
            assertEquals(savedHash, PptxWorkflow.hash(root.resolve("deck.pptx")));
            assertEquals(1, operations.buildCalls);
            verify(room).continueAfterToolExecutionResultsWithRuntimeContext(anyMap(), any(), any(), any(), anyString(), anyString());
            verify(room).appendHarnessResponse(eq(result.getFinalText()), any(), any(), any());
        }
    }

    @Test void runtimeFailureDuringRepairPreservesTheValidatedDeckAndWarning() throws Exception {
        operations.severity = "major";
        workflow.build(args, 5);
        String savedHash = PptxWorkflow.hash(root.resolve("deck.pptx"));
        Files.writeString(root.resolve("build-deck.js"), "pending changes");
        workflow.executionFailed("Model connection timed out");
        assertNull(workflow.completionError());
        assertTrue(workflow.completionWarning().contains("Model connection timed out"));
        assertTrue(workflow.finalText().contains("not included"));
        assertEquals(savedHash, PptxWorkflow.hash(root.resolve("deck.pptx")));
        assertEquals(1, operations.buildCalls);
    }

    @Test void cancellationDuringFinalBuildPropagatesWithoutMarkingDeliveryComplete() throws Exception {
        operations.valid = false;
        workflow.build(args, 5);
        Files.writeString(root.resolve("build-deck.js"), "fixed generator");
        operations.cancelBuild = true;
        assertThrows(prerna.reactor.agent.exceptions.AgentCancelledException.class, () -> workflow.afterRound(11, 40));
        assertFalse(workflow.isTerminal());
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void harnessDeliversWithoutAnotherAuthorRequestAfterSuccessfulBuild(boolean incompleteReview) {
        if (incompleteReview) operations.corruption = "overview";
        Room room = mock(Room.class);
        var messages = new ArrayList<AbstractMessage>();
        when(room.getMessages()).thenReturn(messages);
        AgentRunContext ctx = mock(AgentRunContext.class);
        when(ctx.getRoom()).thenReturn(room);
        when(ctx.getMaxTurns()).thenReturn(40);
        when(ctx.getAgentConfig()).thenReturn(AgentConfig.builder().build());
        when(room.continueAfterToolExecutionResults(anyMap(), any(), any(), any(), isNull(), any(ResponseMessage.class)))
                .thenAnswer(call -> { messages.add(call.getArgument(5)); return null; });
        var state = new AgentLoopState(workflow);
        ResponseMessage call = ResponseMessage.toolResponses(List.of(Map.of("id", "build", "name", PptxWorkflow.TOOL, "input", args)));
        ResponseMessage result = HarnessToolExecutor.executeToolBatch(call, state, new HashMap<>(), ctx);
        assertTrue(state.isTerminal());
        assertEquals(workflow.finalText(), result.getContent());
        verify(room, never()).continueAfterToolExecutionResults(anyMap(), any(), any(), any(), anyString());
        verify(room, never()).continueAfterToolExecutionResultsWithRuntimeContext(anyMap(), any(), any(), any(), any(), any());
        assertEquals(1, state.getToolCallRecords().size());
        assertTrue(state.getToolCallRecords().getFirst().isSuccess());
        assertNull(workflow.completionError());
    }

    final class FakeOperations implements PptxWorkflow.Operations {
        boolean valid = true, cancelBuild;
        String severity, corruption, instructions, engine;
        String slide1 = "slide1", slide2 = "slide2", theme = "theme";
        int buildCalls, reviewCalls;
        List<List<Integer>> scopes = new ArrayList<>();

        @Override public JSONObject build(Map<String, Object> arguments) throws Exception {
            buildCalls++;
            if (cancelBuild) throw new prerna.reactor.agent.exceptions.AgentCancelledException();
            try (var zip = new ZipOutputStream(Files.newOutputStream(root.resolve("deck.pptx")))) {
                for (var part : Map.of("ppt/slides/slide1.xml", slide1, "ppt/slides/slide2.xml", slide2, "ppt/theme/theme1.xml", theme).entrySet()) {
                    zip.putNextEntry(new ZipEntry(part.getKey())); zip.write(part.getValue().getBytes()); zip.closeEntry();
                }
            }
            JSONArray warnings = new JSONArray();
            for (int i = 0; i < 11; i++) warnings.put("warning " + i);
            return new JSONObject().put("ok", valid).put("slides", 2).put("errors", new JSONArray(valid ? List.of() : List.of("bad XML")))
                    .put("warnings", warnings).put("sourceHash", PptxWorkflow.hash(root.resolve("deck.pptx")))
                    .put("generatorHash", PptxWorkflow.hash(root.resolve("build-deck.js")));
        }

        @Override public JSONObject review(String file, List<Integer> slides, String instructions, String engine) throws Exception {
            reviewCalls++; scopes.add(slides); this.instructions = instructions; this.engine = engine;
            if ("provider".equals(corruption)) throw new IllegalStateException("Provider unavailable");
            JSONObject result = new JSONObject().put("filePath", file).put("sourceHash", PptxWorkflow.hash(root.resolve(file)))
                    .put("slideCount", 2).put("status", "complete").put("verdict", severity == null ? "pass" : "needs_changes")
                    .put("requestedSlides", new JSONArray(slides)).put("reviewedSlides", new JSONArray(slides))
                    .put("unreviewedSlides", new JSONArray()).put("sourceChanged", false).put("issues", new JSONArray());
            if (severity != null) result.getJSONArray("issues").put(new JSONObject().put("severity", severity).put("slide", 2).put("evidence", "Caption clipped").put("suggestedFix", "Enlarge caption box"));
            if ("coverage".equals(corruption)) result.put("reviewedSlides", new JSONArray(List.of(1)));
            if ("hash".equals(corruption)) result.put("sourceHash", "wrong");
            if ("file".equals(corruption)) result.put("filePath", "wrong.pptx");
            if ("inconclusive".equals(corruption)) result.put("verdict", "inconclusive");
            if ("overview".equals(corruption)) result.put("status", "partial").put("consistencyReview", "inconclusive")
                    .put("observations", new JSONArray().put(new JSONObject().put("stage", "consistency")
                            .put("text", "Images are placeholders; text is dynamic. No actual visual reviewed.")))
                    .put("limitations", new JSONArray(List.of("No rendered fonts visible")));
            if ("changed".equals(corruption)) Files.writeString(root.resolve(file), "changed during inspection");
            if ("destroy".equals(corruption)) {
                Files.delete(root.resolve(file));
                Files.delete(root.resolve(".semoss/pptx-workflow/test/saved.pptx"));
            }
            if ("edit_generator".equals(corruption) || "edit_generator_after_pass".equals(corruption)) {
                Files.writeString(root.resolve("build-deck.js"), "unbuilt edits");
                if ("edit_generator".equals(corruption)) result.put("verdict", "inconclusive");
            }
            return result;
        }
    }
}
