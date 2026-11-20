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
package prerna.reactor.apptest;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Real file persistence, selective summaries, safe evidence boundaries and concurrent writers. */
class AppTestHistoryUnitTests {
    @TempDir Path folder;
    private static final Instant NOW = Instant.parse("2026-11-19T12:00:00Z");

    private Map<String, Object> recording(String name) {
        return AppTestRecordings.validate(Map.of("name", name, "description", "Verify a visible result.",
                "steps", List.of(Map.of("type", "ASSERT_VISIBLE", "selector", "#result"))));
    }

    private Map<String, Object> pending(List<Map<String, Object>> tests, Instant accepted) {
        return AppTestHistory.pending(UUID.randomUUID().toString(), "app", tests, 1, accepted);
    }

    @Test void replicationFailureKeepsLocalTerminalResultAndReportsASeparateWarning() throws Exception {
        Map<String,Object> run=pending(List.of(recording("Local result")),Instant.now());
        run.put("status","COMPLETED");
        try(var cloud=org.mockito.Mockito.mockStatic(prerna.cluster.util.ClusterUtil.class)) {
            cloud.when(()->prerna.cluster.util.ClusterUtil.copyLocalFileToEngineCloudFolder(
                org.mockito.ArgumentMatchers.eq("app"),org.mockito.ArgumentMatchers.eq(prerna.engine.api.IEngine.CATALOG_TYPE.PROJECT),org.mockito.ArgumentMatchers.anyString()))
                .thenThrow(new IllegalStateException("Fixture storage unavailable"));
            String warning=AppTestHistory.publishAt("app",folder,run);
            assertTrue(warning.contains("saved locally"));
            assertEquals("COMPLETED",AppTestHistory.read(folder,(String)run.get("runId"),"app",Instant.now()).get("status"));
        }
        Path blocked=folder.resolve("not-a-directory"); Files.writeString(blocked,"blocked");
        assertThrows(IOException.class,()->AppTestHistory.publishAt("app",blocked,run));
    }

    @Test void hashIsStableAcrossMapOrderingAndEditTimestampsButChangesWithTheTest() {
        Map<String, Object> first = recording("Checkout");
        Map<String, Object> copy = new LinkedHashMap<>(first);
        copy.put("updatedAt", "a different edit date");
        copy.put("id", UUID.randomUUID().toString());
        copy.put("steps", List.of(Map.of("selector", "#result", "type", "ASSERT_VISIBLE", "framePath", List.of())));
        assertEquals(AppTestHistory.definitionHash(first), AppTestHistory.definitionHash(copy));
        copy.put("name", "Changed workflow");
        assertNotEquals(AppTestHistory.definitionHash(first), AppTestHistory.definitionHash(copy));
    }

    @Test void roundTripKeepsCountsAndNeverCopiesPrivateEvidenceIncludingRepeatAttempts() throws Exception {
        Map<String, Object> run = pending(List.of(recording("Checkout")), NOW);
        Map<String, Object> test = new LinkedHashMap<>(tests(run).getFirst());
        test.put("status", "FAILED"); test.put("stepsAttempted", 1); test.put("stepsCompleted", 0);
        test.put("evidence", Map.of("screenshot", "private-image", "selector", "private-selector"));
        test.put("expected", "private expected text"); test.put("actual", "private actual text");
        test.put("attempts", List.of(new LinkedHashMap<>(test)));
        run.put("sessionId", "private cookie"); run.put("tests", List.of(test));
        AppTestHistory.write(folder, run);
        String content = Files.readString(folder.resolve(run.get("runId") + ".json"));
        assertFalse(content.contains("private"));
        Map<String, Object> read = AppTestHistory.read(folder, (String) run.get("runId"), "app", NOW);
        assertEquals(1, ((Number) tests(read).getFirst().get("stepsAttempted")).intValue());
        assertEquals(0, ((Number) tests(read).getFirst().get("stepsCompleted")).intValue());
        assertTrue(tests(read).getFirst().containsKey("attempts"));
    }

    @Test void summariesFilterByStableTestIdAndPageWithoutReadingDefinitions() throws Exception {
        Map<String, Object> one = recording("Before rename"), two = recording("Other test");
        for (int i = 0; i < 3; i++) {
            Map<String, Object> run = pending(i == 1 ? List.of(two) : List.of(one, two), NOW.plusSeconds(i));
            run.put("status", "COMPLETED");
            AppTestHistory.write(folder, run);
        }
        Map<String, Object> result = AppTestHistory.list(folder, "app", (String) one.get("id"), 1, 1, NOW.plusSeconds(3));
        assertEquals(2, result.get("total"));
        assertEquals(1, ((List<?>) result.get("runs")).size());
        assertEquals(1, ((List<?>) result.get("latest")).size());
        assertEquals(false, result.get("hasMore"));
        String json = new com.google.gson.Gson().toJson(result);
        assertFalse(json.contains("#result")); assertFalse(json.contains("definitionHash"));
        assertFalse(Files.exists(folder.resolve("tests")));
    }

    @Test void expiredWorkIsInterruptedWithoutChangingCompletedTestsOrOverwritingItsFile() throws Exception {
        Map<String, Object> run = pending(List.of(recording("Finished"), recording("Pending")), NOW.minusSeconds(600));
        tests(run).getFirst().put("status", "PASSED");
        AppTestHistory.write(folder, run);
        String before = Files.readString(folder.resolve(run.get("runId") + ".json"));
        Map<String, Object> read = AppTestHistory.read(folder, (String) run.get("runId"), "app", NOW);
        assertEquals("INTERRUPTED", read.get("status"));
        assertEquals("PASSED", tests(read).getFirst().get("status"));
        assertEquals("INTERRUPTED", tests(read).getLast().get("status"));
        assertEquals(before, Files.readString(folder.resolve(run.get("runId") + ".json")));
        Map<String, Object> active = pending(List.of(recording("Active")), NOW);
        AppTestHistory.write(folder, active);
        assertEquals("QUEUED", AppTestHistory.read(folder, (String) active.get("runId"), "app", NOW).get("status"));
    }

    @Test void retentionPrunesOnlyOldFinishedRunsAndPreservesActiveWork() throws Exception {
        Map<String, Object> test = recording("Checkout");
        for (int i = 0; i < 104; i++) {
            Map<String, Object> run = pending(List.of(test), NOW);
            run.put("status", "COMPLETED");
            AppTestHistory.write(folder, run);
            Files.setLastModifiedTime(folder.resolve(run.get("runId") + ".json"), FileTime.from(NOW.plusSeconds(i)));
        }
        Map<String, Object> active = pending(List.of(test), NOW);
        AppTestHistory.write(folder, active);
        assertEquals(4, AppTestHistory.prune(folder, "app", NOW).size());
        assertEquals(101, AppTestHistory.all(folder, "app", NOW).size());
        assertTrue(Files.exists(folder.resolve(active.get("runId") + ".json")));
        Path malformed = folder.resolve(UUID.randomUUID() + ".json");
        Files.writeString(malformed, "{\"status\":\"COMPLETED\"}");
        assertTrue(AppTestHistory.prune(folder, "app", NOW).isEmpty());
        assertTrue(Files.exists(malformed));
    }

    @Test void latestResultsNeedOnlyCurrentFilenamesAndDeletedTestsRemainQueryableByUuid() throws Exception {
        Path base = folder.resolve("app-tests"), history = base.resolve("test-runs");
        Map<String, Object> test = recording("Checkout");
        Map<String, Object> run = pending(List.of(test), NOW);
        AppTestHistory.write(history, run);
        Path definition = base.resolve("tests").resolve(test.get("id") + ".json");
        Files.createDirectories(definition.getParent());
        Files.writeString(definition, "History must not parse this definition body");
        assertEquals(1, ((List<?>) AppTestHistory.list(history, "app", null, 0, 20, NOW).get("latest")).size());
        Files.delete(definition);
        assertTrue(((List<?>) AppTestHistory.list(history, "app", null, 0, 20, NOW).get("latest")).isEmpty());
        assertEquals(1, AppTestHistory.list(history, "app", (String) test.get("id"), 0, 20, NOW).get("total"));
    }

    @Test void concurrentRunsKeepSeparateFilesAndProgressUpdatesKeepTheirOwnIdentity() throws Exception {
        List<Map<String, Object>> runs = new ArrayList<>();
        for (int i = 0; i < 12; i++) runs.add(pending(List.of(recording("Run " + i)), NOW));
        try (var pool = Executors.newFixedThreadPool(4)) {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (Map<String, Object> run : runs) futures.add(pool.submit(() -> {
                try { AppTestHistory.write(folder, run); run.put("status", "COMPLETED"); AppTestHistory.write(folder, run); }
                catch (IOException e) { throw new RuntimeException(e); }
            }));
            for (var future : futures) future.get();
        }
        assertEquals(12, AppTestHistory.all(folder, "app", NOW).size());
        assertTrue(AppTestHistory.all(folder, "app", NOW).stream().allMatch(run -> "COMPLETED".equals(run.get("status"))));
    }

    @Test void historyRejectsTraversalWrongProjectsAndCorruption() throws Exception {
        Map<String, Object> run = pending(List.of(recording("Checkout")), NOW);
        AppTestHistory.write(folder, run);
        assertThrows(IllegalArgumentException.class, () -> AppTestHistory.read(folder, "../../outside", "app", NOW));
        assertThrows(IOException.class, () -> AppTestHistory.read(folder, (String) run.get("runId"), "other-app", NOW));
        Files.writeString(folder.resolve(run.get("runId") + ".json"), "invalid JSON");
        assertThrows(IOException.class, () -> AppTestHistory.all(folder, "app", NOW));
    }

    @SuppressWarnings("unchecked") private List<Map<String, Object>> tests(Map<String, Object> run) {
        return (List<Map<String, Object>>) run.get("tests");
    }
}
