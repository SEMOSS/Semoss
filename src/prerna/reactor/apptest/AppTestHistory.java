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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/** Project-visible execution summaries. Browser evidence and session credentials never enter assets. */
final class AppTestHistory {
    static final int RETAINED_RUNS = 100;
    private static final int MAX_BYTES = 256_000;
    private static final Gson GSON = new Gson();
    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(AppTestHistory.class);
    private static final Set<String> ACTIVE = Set.of("QUEUED", "RUNNING");
    private static final List<String> TEST_FIELDS = List.of("id", "name", "description", "definitionHash", "status",
            "scope", "steps", "stepsAttempted", "stepsCompleted", "assertions", "assertionsPassed", "failedStep",
            "classification", "reason", "startedAt", "finishedAt", "durationMs", "repeats", "passedRepeats", "stepsPerReplay");

    private AppTestHistory() { }

    static Path folder(String projectId) {
        return Path.of(AssetUtility.getProjectAssetsFolder(projectId), "app-tests", "test-runs");
    }

    /** Hash definition content, excluding mutable timestamps and file identity. */
    static String definitionHash(Map<String, Object> recording) {
        Map<String, Object> definition = new TreeMap<>();
        for (String field : List.of("version", "name", "description", "startPath", "steps", "cleanupSteps", "routeMatch", "useVariables")) {
            if (recording.containsKey(field)) definition.put(field, canonical(recording.get(field)));
        }
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(GSON.toJson(definition).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    private static Object canonical(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new TreeMap<>();
            map.forEach((key, item) -> sorted.put(String.valueOf(key), canonical(item)));
            return sorted;
        }
        if (value instanceof List<?> list) return list.stream().map(AppTestHistory::canonical).toList();
        return value;
    }

    static Map<String, Object> pending(String runId, String projectId, List<Map<String, Object>> recordings,
            int repeat, Instant acceptedAt) {
        Map<String, Object> run = new LinkedHashMap<>();
        run.put("version", 1);
        run.put("runId", AppTestRecordings.id(runId));
        run.put("target", projectId);
        run.put("status", "QUEUED");
        run.put("startedAt", acceptedAt.toString());
        // The runner's fixed deadline includes queue time. Allow browser shutdown/persistence a grace period.
        run.put("expiresAt", acceptedAt.plusMillis(AppTestRunner.RUN_TIMEOUT_MS * repeat + 120_000).toString());
        run.put("repeat", repeat);
        run.put("tests", recordings.stream().map(recording -> AppTestRunner.pendingTest(recording, "QUEUED")).toList());
        return run;
    }

    /** Explicit allowlist: never persist expected/actual text, selectors, steps, screenshot data or cookies. */
    static Map<String, Object> compact(Map<String, Object> original) {
        Map<String, Object> run = new LinkedHashMap<>();
        for (String field : List.of("version", "runId", "target", "status", "startedAt", "finishedAt", "expiresAt",
                "durationMs", "repeat", "privateReportRunId", "scope", "mode", "fixtureHash", "portalHash", "coverageWarning", "reportWarning")) {
            if (original.containsKey(field)) run.put(field, original.get(field));
        }
        if (original.get("summary") instanceof Map<?, ?> source) {
            Map<String, Object> summary = new LinkedHashMap<>();
            for (String field : List.of("passed", "failed", "error", "skipped")) {
                if (source.get(field) instanceof Number count) summary.put(field, count);
            }
            run.put("summary", summary);
        }
        List<Map<String, Object>> tests = new ArrayList<>();
        if (original.get("tests") instanceof List<?> values) for (Object value : values) {
            if (!(value instanceof Map<?, ?> source)) continue;
            tests.add(compactTest(source));
        }
        run.put("tests", tests);
        return run;
    }

    private static Map<String, Object> compactTest(Map<?, ?> source) {
        Map<String, Object> test = new LinkedHashMap<>();
        for (String field : TEST_FIELDS) if (source.containsKey(field)) test.put(field, source.get(field));
        if (source.get("attempts") instanceof List<?> attempts) {
            test.put("attempts", attempts.stream().filter(Map.class::isInstance)
                    .map(attempt -> {
                        Map<?, ?> outcome = (Map<?, ?>) attempt;
                        Map<String, Object> compact = new LinkedHashMap<>();
                        for (String field : TEST_FIELDS) {
                            if (!List.of("id", "name", "description", "definitionHash", "scope").contains(field)
                                    && outcome.containsKey(field)) compact.put(field, outcome.get(field));
                        }
                        copyCleanup(outcome, compact);
                        return compact;
                    }).toList());
        }
        copyCleanup(source, test);
        return test;
    }

    private static void copyCleanup(Map<?,?> source, Map<String,Object> test) {
        if (source.get("cleanup") instanceof Map<?,?> cleanup) {
            Map<String,Object> safe = new LinkedHashMap<>();
            for (String field : List.of("status","stepsAttempted","stepsCompleted","failedStep","reason")) if (cleanup.containsKey(field)) safe.put(field,cleanup.get(field));
            test.put("cleanup",safe);
        }
    }

    static synchronized void write(Path folder, Map<String, Object> run) throws IOException {
        Map<String, Object> value = compact(run);
        String id = AppTestRecordings.id(String.valueOf(value.get("runId")));
        if (GSON.toJson(value).getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_BYTES)
            throw new IOException("Test run history exceeds its size limit");
        AppTestRecordings.write(folder.resolve(id + ".json"), value);
    }

    /** Publish only this run's file: syncing the entire folder could remove another node's runs. */
    static synchronized String publish(String projectId, Map<String, Object> run) throws IOException {
        return publishAt(projectId, folder(projectId), run);
    }

    /** Local persistence is authoritative on this node; cloud failures are a separate warning. */
    static synchronized String publishAt(String projectId, Path folder, Map<String,Object> run) throws IOException {
        write(folder, run);
        try {
            ClusterUtil.copyLocalFileToEngineCloudFolder(projectId, IEngine.CATALOG_TYPE.PROJECT, folder.resolve(run.get("runId") + ".json").toString());
            if (!ACTIVE.contains(run.get("status"))) for (Path removed : prune(folder, projectId, Instant.now()))
                ClusterUtil.deleteEngineCloudFile(projectId, IEngine.CATALOG_TYPE.PROJECT, removed.toString());
            return null;
        } catch (IOException | RuntimeException failure) {
            LOGGER.warn("App test history replication/pruning failed: project={}, run={}, status={}, folder={}", projectId, run.get("runId"), run.get("status"), folder, failure);
            return "Test history was saved locally, but cloud replication or retention could not complete.";
        }
    }

    static synchronized Path refresh(String projectId) throws IOException {
        Path folder = folder(projectId);
        Files.createDirectories(folder);
        if (!ClusterUtil.IS_CLUSTER) return folder;
        Map<String,Map<String,Object>> localFinished = new LinkedHashMap<>();
        try (var files = Files.list(folder)) {
            for (Path file : files.filter(p -> p.getFileName().toString().endsWith(".json")).toList()) {
                String name = file.getFileName().toString();
                try {
                    Map<String,Object> value = read(folder, name.substring(0,name.length()-5), projectId, Instant.now());
                    if (!ACTIVE.contains(value.get("status")) && !"INTERRUPTED".equals(value.get("status"))) localFinished.put(name,value);
                } catch (IOException | RuntimeException ignored) { /* Invalid history remains visible to normal reads. */ }
            }
        }
        try { ClusterUtil.pullProjectFolder(Utility.getProject(projectId), folder.toString()); }
        catch (RuntimeException failure) { LOGGER.warn("Unable to refresh cloud app test history for project {}; retaining local finished results", projectId, failure); }
        for (var saved : localFinished.entrySet()) {
            boolean restore = true;
            try {
                String name = saved.getKey();
                Map<String,Object> cloud = read(folder,name.substring(0,name.length()-5),projectId,Instant.now());
                restore = ACTIVE.contains(cloud.get("status")) || "INTERRUPTED".equals(cloud.get("status"));
            } catch (IOException | RuntimeException ignored) { /* A stale/missing cloud copy must not erase local completion. */ }
            if (restore) AppTestRecordings.write(folder.resolve(saved.getKey()),saved.getValue());
        }
        return folder;
    }

    @SuppressWarnings("unchecked")
    static synchronized Map<String, Object> read(Path folder, String runId, String projectId, Instant now) throws IOException {
        Path file = folder.resolve(AppTestRecordings.id(runId) + ".json");
        if (!Files.isRegularFile(file) || Files.size(file) > MAX_BYTES) throw new IOException("Test run history is missing or too large");
        try {
            Map<String, Object> parsed = GSON.fromJson(Files.readString(file), new TypeToken<Map<String, Object>>() { }.getType());
            if (parsed == null || !runId.equals(parsed.get("runId")) || !projectId.equals(parsed.get("target"))
                    || !(parsed.get("version") instanceof Number version) || version.doubleValue() != 1
                    || !(parsed.get("tests") instanceof List<?>)) throw new IllegalArgumentException();
            Map<String, Object> run = compact(parsed);
            Instant.parse(String.valueOf(run.get("startedAt")));
            for (Map<String, Object> test : (List<Map<String, Object>>) run.get("tests")) {
                if (!AppTestRuns.PLATFORM_SMOKE_SUITE.equals(test.get("id"))) AppTestRecordings.id(String.valueOf(test.get("id")));
                if (!(test.get("name") instanceof String) || !(test.get("status") instanceof String)) throw new IllegalArgumentException();
                for (String field : List.of("steps", "stepsAttempted", "stepsCompleted", "assertions", "assertionsPassed")) {
                    if (!(test.get(field) instanceof Number count) || count.doubleValue() < 0 || count.doubleValue() != count.intValue())
                        throw new IllegalArgumentException();
                }
            }
            if (ACTIVE.contains(run.get("status")) && now.isAfter(Instant.parse(String.valueOf(run.get("expiresAt"))))) {
                run.put("status", "INTERRUPTED");
                List<Map<String, Object>> tests = (List<Map<String, Object>>) run.get("tests");
                for (Map<String, Object> test : tests) if (ACTIVE.contains(test.get("status"))) {
                    test.put("status", "INTERRUPTED");
                    test.put("classification", "INTERRUPTED");
                    test.put("reason", "The worker did not persist a final result within its execution window.");
                }
            }
            return run;
        } catch (RuntimeException e) { throw new IOException("Invalid test run history: " + runId, e); }
    }

    static synchronized List<Map<String, Object>> all(Path folder, String projectId, Instant now) throws IOException {
        if (!Files.isDirectory(folder)) return List.of();
        List<Map<String, Object>> runs = new ArrayList<>();
        try (var files = Files.list(folder)) {
            for (Path file : files.filter(path -> path.getFileName().toString().endsWith(".json")).toList()) {
                String name = file.getFileName().toString();
                runs.add(read(folder, name.substring(0, name.length() - 5), projectId, now));
            }
        }
        runs.sort(Comparator.comparing((Map<String, Object> run) -> Instant.parse(String.valueOf(run.get("startedAt")))).reversed()
                .thenComparing(run -> String.valueOf(run.get("runId"))));
        return runs;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> list(Path folder, String projectId, String recordingId, int offset, int limit, Instant now) throws IOException {
        final String selectedId = recordingId == null ? null : AppTestRecordings.id(recordingId);
        List<Map<String, Object>> summaries = new ArrayList<>();
        Map<String, Map<String, Object>> latest = new LinkedHashMap<>();
        for (Map<String, Object> run : all(folder, projectId, now)) {
            List<Map<String, Object>> tests = (List<Map<String, Object>>) run.get("tests");
            Map<String, Object> matched = null;
            for (Map<String, Object> test : tests) {
                String id = String.valueOf(test.get("id"));
                if (AppTestRuns.PLATFORM_SMOKE_SUITE.equals(id)) continue;
                Map<String, Object> entry = new LinkedHashMap<>();
                for (String field : List.of("id", "name", "status", "steps", "stepsAttempted", "stepsCompleted",
                        "assertions", "assertionsPassed", "durationMs")) if (test.containsKey(field)) entry.put(field, test.get(field));
                entry.put("runId", run.get("runId"));
                entry.put("startedAt", test.getOrDefault("startedAt", run.get("startedAt")));
                if (run.containsKey("finishedAt")) entry.put("finishedAt", test.getOrDefault("finishedAt", run.get("finishedAt")));
                latest.putIfAbsent(id, entry);
                if (id.equals(recordingId)) matched = entry;
            }
            if (recordingId != null && matched == null) continue;
            Map<String, Object> summary = new LinkedHashMap<>();
            for (String field : List.of("runId", "status", "startedAt", "finishedAt", "durationMs", "summary", "mode", "fixtureHash", "portalHash"))
                if (run.containsKey(field)) summary.put(field, run.get(field));
            if (matched != null) summary.put("test", matched);
            summaries.add(summary);
        }
        int from = Math.max(0, Math.min(offset, summaries.size()));
        int to = Math.min(from + Math.max(1, Math.min(limit, 50)), summaries.size());
        return Map.of("target", projectId, "runs", summaries.subList(from, to), "latest", latest.values().stream()
                .filter(test -> selectedId != null ? selectedId.equals(test.get("id"))
                        : Files.isRegularFile(folder.getParent().resolve("tests").resolve(test.get("id") + ".json")))
                .limit(AppTestRecordings.MAX_RECORDINGS).toList(), "total", summaries.size(), "offset", from,
                "hasMore", to < summaries.size(), "retainedRuns", RETAINED_RUNS);
    }

    static synchronized List<Path> prune(Path folder, String projectId, Instant now) throws IOException {
        if (!Files.isDirectory(folder)) return List.of();
        Map<Path, Instant> finished = new LinkedHashMap<>();
        try (var files = Files.list(folder)) {
            for (Path file : files.filter(path -> path.getFileName().toString().endsWith(".json")).toList()) {
                try {
                    String name = file.getFileName().toString();
                    Map<String, Object> run = read(folder, name.substring(0, name.length() - 5), projectId, now);
                    if (!ACTIVE.contains(run.get("status"))) {
                        Object ended = run.getOrDefault("finishedAt", "INTERRUPTED".equals(run.get("status")) ? run.get("expiresAt") : run.get("startedAt"));
                        finished.put(file, Instant.parse(String.valueOf(ended)));
                    }
                } catch (IOException | RuntimeException e) { /* A malformed file is surfaced on history reads, never silently deleted. */ }
            }
        }
        List<Path> removed = new ArrayList<>();
        for (Path file : finished.keySet().stream().sorted(Comparator.comparing((Path path) -> finished.get(path)).reversed()
                .thenComparing(Path::toString)).skip(RETAINED_RUNS).toList()) {
            Files.deleteIfExists(file);
            removed.add(file);
        }
        return removed;
    }
}
