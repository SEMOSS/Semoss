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
package prerna.reactor.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.engine.impl.model.ModelRouterEngine.ReplaySettings;
import prerna.engine.impl.model.Room;

/** Golden sets from recorded runs: which runs qualify, the split, the files and the baseline snapshot. */
class RouterLearningExamplesUnitTests {

	@TempDir
	Path rooms;

	private final ReplaySettings settings = new ReplaySettings();
	private final Room room = mock(Room.class);
	private int n;

	private void capture(Path folder, int maxBytes) {
		when(room.getRoomFolderPath()).thenReturn(folder.toString());
		RouterLearningExamples.snapshotBaseline(room, maxBytes);
	}

	private static List<Object> messages(String... tools) {
		List<Object> parts = new ArrayList<>();
		for (String tool : tools) {
			parts.add(Map.of("type", "TOOL_CALL", "toolCall", Map.of("name", tool)));
		}
		return List.of(Map.of("parts", parts));
	}

	/** A run in its own room whose starting files are {@code inputs} (a baseline) and which ends holding {@code after}. */
	private Map<String, Object> run(String goal, String status, String room, Object messages, Map<String, String> inputs, Map<String, String> after)
			throws Exception {
		Path folder = Files.createDirectories(rooms.resolve(room));
		if (inputs != null) {
			for (var e : inputs.entrySet()) {
				Files.writeString(folder.resolve(e.getKey()), e.getValue());
			}
			capture(folder, settings.max_snapshot_bytes);
			for (String file : inputs.keySet()) {
				Files.delete(folder.resolve(file));
			}
		}
		for (var e : after.entrySet()) {
			Files.writeString(folder.resolve(e.getKey()), e.getValue());
		}
		Map<String, Object> run = new HashMap<>();
		run.put("runId", "r" + (n++));
		run.put("roomId", room);
		run.put("workspaceId", "original");
		run.put("status", status);
		run.put("input", goal);
		run.put("finalText", "done");
		run.put("messages", messages);
		return run;
	}

	private List<RouterLearningExamples.Usable> select(List<Map<String, Object>> runs) {
		return RouterLearningExamples.select(runs, rooms::resolve, settings.max_snapshot_bytes, workspace -> Set.of("WriteFile", "ReadFile"));
	}

	@Test
	void onlyCompletedTopLevelSingleRunApprovedToolRunsWithKnownInputsAndNoErrorOrThumbsDownQualify() throws Exception {
		List<Map<String, Object>> runs = new ArrayList<>();
		runs.add(run("good", "COMPLETED", "a", messages("WriteFile", "ReadFile"), Map.of("in.csv", "1"), Map.of("in.csv", "1", "out.csv", "2")));
		runs.add(run("failed", "FAILED", "b", messages("WriteFile"), Map.of(), Map.of()));
		runs.add(run("email", "COMPLETED", "c", messages("WriteFile", "SendEmail"), Map.of(), Map.of()));
		runs.add(run("input needed", "COMPLETED", "d", messages("AskUser"), Map.of(), Map.of()));
		runs.add(run("no baseline but files", "COMPLETED", "e", messages("ReadFile"), null, Map.of("x.txt", "1")));
		runs.add(run("no baseline no files", "COMPLETED", "f", messages("ReadFile"), null, Map.of()));
		runs.add(run("explicit fileless capture", "COMPLETED", "g", messages("ReadFile"), Map.of(), Map.of("created.txt", "output")));
		Map<String, Object> child = run("child", "COMPLETED", "h", messages("ReadFile"), Map.of(), Map.of());
		child.put("parentRunId", "p");
		runs.add(child);
		runs.add(run("two runs", "COMPLETED", "i", messages("ReadFile"), Map.of(), Map.of()));
		runs.add(run("two runs again", "COMPLETED", "i", messages("ReadFile"), Map.of(), Map.of()));
		Map<String, Object> errored = run("errored", "COMPLETED", "j", messages("ReadFile"), Map.of(), Map.of());
		errored.put("errorMessage", "boom");
		runs.add(errored);
		runs.add(run("thumbs down", "COMPLETED", "k", List.of(Map.of("feedback", Map.of("rating", false), "parts", List.of())), Map.of(), Map.of()));

		assertEquals(List.of("good", "explicit fileless capture"), select(runs).stream().map(RouterLearningExamples.Usable::goal).toList());
	}

	@Test
	void requiresExactApprovedCallsBeforeDeduplicatingCandidateRuns() throws Exception {
		String approved = "workspace_AnalyzeWorkbook";
		List<Map<String, Object>> runs = List.of(
				run("same", "COMPLETED", "newest", messages(approved, "ReadFile"), Map.of(), Map.of()),
				run("same", "COMPLETED", "older", messages(approved), Map.of(), Map.of()),
				run("wrong case", "COMPLETED", "wrong-case", messages("workspace_analyzeworkbook"), Map.of(), Map.of()),
				run("no calls", "COMPLETED", "no-calls", messages(), Map.of(), Map.of()));
		assertEquals(List.of("r1", "r3"), RouterLearningExamples.select(runs, rooms::resolve, settings.max_snapshot_bytes,
				workspace -> Set.of(approved)).stream().map(RouterLearningExamples.Usable::runId).toList());
		assertEquals(List.of("r3"), RouterLearningExamples.select(runs, rooms::resolve, settings.max_snapshot_bytes,
				workspace -> Set.of()).stream().map(RouterLearningExamples.Usable::runId).toList());
	}

	@Test
	void readsRecordedCallNamesButNotTitlesResultsOrToolDeclarations() throws Exception {
		List<?> nested = List.of(
				Map.of("io", "INPUT", "parts", List.of(Map.of("type", "TOOL_CALL", "toolCall", Map.of("name", "UnusedTool")))),
				Map.of("io", "OUTPUT", "parts", List.of(Map.of("type", "TOOL_CALL", "tool_call",
						Map.of("function", Map.of("name", "ReadFile"), "title", "Display only")))),
				Map.of("io", "INPUT", "parts", List.of(Map.of("type", "TOOL_RESULT", "toolResult", Map.of("toolName", "Display only")))),
				Map.of("io", "OUTPUT", "parts", List.of(Map.of("type", "TEXT", "text", "done"))));
		List<?> misleading = List.of(Map.of("parts", List.of(Map.of("type", "TOOL_CALL", "toolCall",
				Map.of("name", "OtherProject_ReadFile", "title", "ReadFile", "original_name", "ReadFile")))));
		List<?> malformed = List.of(Map.of("parts", List.of(Map.of("type", "TOOL_CALL", "toolCall", "ReadFile"))));
		List<?> legacy = List.of(Map.of("tool_responses", List.of(Map.of("name", "UnavailableTool"))));
		List<Map<String, Object>> runs = List.of(run("nested call", "COMPLETED", "nested-call", nested, Map.of(), Map.of()),
				run("display alias", "COMPLETED", "alias", misleading, Map.of(), Map.of()),
				run("malformed call", "COMPLETED", "malformed-call", malformed, Map.of(), Map.of()),
				run("legacy call", "COMPLETED", "legacy-call", legacy, Map.of(), Map.of()));
		assertEquals(List.of("nested call"), select(runs).stream().map(RouterLearningExamples.Usable::goal).toList());
	}

	@Test
	void deduplicatesOnlyTheSamePromptAndStartingFilesUsingTheNewestRun() throws Exception {
		List<Map<String, Object>> runs = List.of(run("same", "COMPLETED", "a", messages(), Map.of("in.txt", "a"), Map.of()),
				run("same", "COMPLETED", "b", messages(), Map.of("in.txt", "a"), Map.of("out.txt", "different output")),
				run("same", "COMPLETED", "c", messages(), Map.of("in.txt", "b"), Map.of()),
				run("same", "COMPLETED", "d", messages(), Map.of("renamed.txt", "a"), Map.of()));
		assertEquals(List.of("r0", "r2", "r3"), select(runs).stream().map(RouterLearningExamples.Usable::runId).toList());
	}

	@Test
	void usesEachOriginalWorkspacesToolsAndDoesNotSubstituteAMissingWorkspace() throws Exception {
		var a = run("same", "COMPLETED", "a", messages("ReadFile"), Map.of(), Map.of());
		var b = run("same", "COMPLETED", "b", messages("WriteFile"), Map.of(), Map.of());
		b.put("workspaceId", "other");
		var disabled = run("disabled", "COMPLETED", "c", messages("ReadFile"), Map.of(), Map.of());
		disabled.put("workspaceId", "other");
		var missing = run("missing", "COMPLETED", "d", messages(), Map.of(), Map.of());
		missing.remove("workspaceId");
		var gone = run("gone", "COMPLETED", "e", messages(), Map.of(), Map.of());
		gone.put("workspaceId", "deleted");
		var tools = Map.of("original", Set.of("ReadFile"), "other", Set.of("WriteFile"));
		var selected = RouterLearningExamples.select(List.of(a, b, disabled, missing, gone), rooms::resolve,
				settings.max_snapshot_bytes, tools::get);
		assertEquals(List.of("original", "other"), selected.stream().map(RouterLearningExamples.Usable::workspaceId).toList());
		assertEquals(List.of(Set.of("ReadFile"), Set.of("WriteFile")), selected.stream().map(RouterLearningExamples.Usable::requiredTools).toList());
		assertEquals(selected.get(0).hash(), selected.get(1).hash(), "the same inputs stay in the same holdout partition");
	}

	@Test
	void aRoomDerivedWorkspaceIsCapturedBeforeTheRoomCanChange() throws Exception {
		when(room.getOptions()).thenReturn("{\"workspace\":{\"workspace_id\":\"at-start\"}}");
		var run = run("goal", "COMPLETED", "room", messages(), Map.of(), Map.of());
		run.remove("workspaceId");
		when(room.getOptions()).thenReturn("{\"workspace\":\"changed-later\"}");
		capture(rooms.resolve("room"), 1024);
		assertEquals(List.of("at-start"), select(List.of(run)).stream().map(RouterLearningExamples.Usable::workspaceId).toList());
	}

	@Test
	void rejectsLegacyMissingChangedAndPartialCaptures() throws Exception {
		List<Map<String, Object>> runs = List.of(run("legacy", "COMPLETED", "legacy", messages(), null, Map.of()),
				run("no manifest", "COMPLETED", "no-manifest", messages(), Map.of(), Map.of()),
				run("malformed", "COMPLETED", "malformed", messages(), Map.of(), Map.of()),
				run("changed", "COMPLETED", "changed", messages(), Map.of("in.txt", "1"), Map.of()),
				run("partial", "COMPLETED", "partial", messages(), Map.of("a.txt", "1", "b.txt", "2"), Map.of()),
				run("extra", "COMPLETED", "extra", messages(), Map.of(), Map.of()));
		Files.createDirectory(rooms.resolve("legacy/.baseline"));
		Files.delete(rooms.resolve("no-manifest/.baseline/.manifest.json"));
		Files.writeString(rooms.resolve("malformed/.baseline/.manifest.json"), "{");
		Files.writeString(rooms.resolve("changed/.baseline/in.txt"), "2");
		Files.delete(rooms.resolve("partial/.baseline/b.txt"));
		Files.writeString(rooms.resolve("extra/.baseline/extra.txt"), "unexpected");
		assertTrue(select(runs).isEmpty());
	}

	private List<RouterLearningExamples.Usable> usable(int count) throws Exception {
		List<Map<String, Object>> runs = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			runs.add(run("goal " + i, "COMPLETED", "room" + i, messages("ReadFile"), Map.of("in.txt", "data " + i), Map.of("in.txt", "data " + i)));
		}
		return select(runs);
	}

	@Test
	void tooFewTasksBuildNothingAndSayHowManyMoreAreNeeded() throws Exception {
		Path replay = rooms.resolve("replay");
		RouterLearningExamples.Built built = RouterLearningExamples.write(replay, usable(1), settings);
		assertNull(built.train());
		assertEquals(settings.validation_tasks + settings.min_train_tasks - 1, built.needed());
		assertFalse(Files.exists(replay));
	}

	@Test
	void membershipStaysStableAsTheWindowChangesAndSetsAreOpaqueAndCapped() throws Exception {
		Path replay = rooms.resolve("replay");
		List<RouterLearningExamples.Usable> usable = usable(100);
		RouterLearningExamples.Built first = RouterLearningExamples.write(replay, usable.subList(0, 75), settings);
		settings.validation_tasks = 22;
		settings.max_train_tasks = 25;
		RouterLearningExamples.Built second = RouterLearningExamples.write(replay, usable.subList(25, 100), settings);
		assertNotEquals(first.train(), second.train());
		assertNotEquals(first.val(), second.val());
		assertNotEquals(first.train(), first.val());
		assertTrue(List.of(first.train(), first.val(), second.train(), second.val()).stream()
				.allMatch(id -> id.matches("auto-[0-9a-f-]{36}")));

		List<RouterLearningExamples.Task> val = RouterLearningExamples.load(replay.resolve(first.val()));
		List<String> firstVal = val.stream().map(RouterLearningExamples.Task::prompt).toList();
		List<String> firstTrain = prompts(replay.resolve(first.train()));
		List<String> secondVal = prompts(replay.resolve(second.val()));
		List<String> secondTrain = prompts(replay.resolve(second.train()));
		assertEquals(20, firstVal.size());
		assertEquals(30, firstTrain.size());
		assertEquals(22, secondVal.size());
		assertEquals(25, secondTrain.size());
		assertFalse(Collections.disjoint(firstVal, secondVal));
		assertFalse(Collections.disjoint(firstTrain, secondTrain));
		assertTrue(Collections.disjoint(firstVal, secondTrain));
		assertTrue(Collections.disjoint(firstTrain, secondVal));
		assertTrue(Collections.disjoint(firstTrain, firstVal));
		RouterLearningExamples.Task firstTask = val.get(0);
		assertEquals("original", firstTask.workspaceId());
		assertEquals("in.txt", firstTask.inputs().get(0).getFileName().toString());
		assertEquals(firstTask.prompt().replace("goal", "data"), Files.readString(firstTask.inputs().get(0)));
		assertFalse(Files.readString(replay.resolve(first.val()).resolve("manifest.json")).contains("done"));
	}

	private List<String> prompts(Path set) throws IOException {
		return RouterLearningExamples.load(set).stream().map(RouterLearningExamples.Task::prompt).toList();
	}

	@Test
	void limitsTheLastHistoryPageAndStopsAtTheCap() throws Exception {
		List<Long> offsets = new ArrayList<>();
		List<Integer> limits = new ArrayList<>();
		List<Long> runs = RouterLearningExamples.readPages(RouterLearningExamples.RUN_PAGE_SIZE, 245, (offset, limit) -> {
			offsets.add(offset);
			limits.add(limit);
			return Collections.nCopies(limit, offset);
		});
		assertEquals(List.of(0L, 200L), offsets);
		assertEquals(List.of(200, 45), limits);
		assertEquals(245, runs.size());
	}

	@Test
	void theBaselineKeepsTheStartingFilesOnceAndSkipsLargeOrHiddenOnes() throws Exception {
		Path folder = Files.createDirectories(rooms.resolve("snap"));
		Files.writeString(folder.resolve("in.txt"), "start");
		Files.writeString(folder.resolve(".secret"), "hidden");
		capture(folder, 5);
		assertEquals("start", Files.readString(folder.resolve(".baseline/in.txt")));
		assertFalse(Files.exists(folder.resolve(".baseline/.secret")));
		Files.writeString(folder.resolve("in.txt"), "changed by the agent");
		capture(folder, 100);
		assertEquals("start", Files.readString(folder.resolve(".baseline/in.txt")), "a second call keeps the first");
	}

	@Test
	void failedCaptureDoesNotPublishPartialInputsOrRetryAsFileless() throws Exception {
		Path folder = Files.createDirectory(rooms.resolve("failed"));
		Files.writeString(folder.resolve("a.txt"), "123");
		Files.writeString(folder.resolve("b.txt"), "456");
		capture(folder, 5);
		assertFalse(Files.exists(folder.resolve(".baseline")));
		Files.delete(folder.resolve("a.txt"));
		Files.delete(folder.resolve("b.txt"));
		capture(folder, 5);
		assertFalse(Files.exists(folder.resolve(".baseline")));
		assertTrue(select(List.of(run("failed", "COMPLETED", "failed", messages(), null, Map.of()))).isEmpty());
	}

	@Test
	void rejectsCaptureWhenNestedInputsCannotBeStaged() throws Exception {
		Path folder = Files.createDirectory(rooms.resolve("nested"));
		Files.writeString(Files.createDirectory(folder.resolve("dir")).resolve("in.txt"), "1");
		capture(folder, 5);
		assertFalse(Files.exists(folder.resolve(".baseline")));
	}

	@Test
	void rejectsChangedSnapshotsEvenAfterSelection() throws Exception {
		Path replay = rooms.resolve("changed-replay");
		List<RouterLearningExamples.Usable> selected = usable(12);
		settings.validation_tasks = 1;
		settings.min_train_tasks = 1;
		settings.max_train_tasks = 1;
		for (RouterLearningExamples.Usable example : selected) {
			Files.writeString(example.baseline().resolve("in.txt"), "changed after selection");
		}
		assertThrows(IOException.class, () -> RouterLearningExamples.write(replay, selected, settings));
		try (var paths = Files.list(replay)) {
			assertEquals(0, paths.count());
		}
	}

	@Test
	void officeFilesAndTextFilesAreReadAsText() throws Exception {
		ByteArrayOutputStream xlsx = new ByteArrayOutputStream();
		try (XSSFWorkbook book = new XSSFWorkbook()) {
			var row = book.createSheet("Scores").createRow(0);
			row.createCell(0).setCellValue("Bob");
			row.createCell(1).setCellValue(100);
			row.createCell(2).setCellFormula("B1*2");
			book.write(xlsx);
		}
		String sheet = RouterEvaluationEvidence.readable("score.xlsx", xlsx.toByteArray());
		assertTrue(sheet.contains("[sheet Scores]") && sheet.contains("A1=Bob") && sheet.contains("B1=100") && sheet.contains("C1==B1*2 -> 200"), sheet);

		ByteArrayOutputStream docx = new ByteArrayOutputStream();
		try (XWPFDocument doc = new XWPFDocument()) {
			doc.createParagraph().createRun().setText("Quarterly report");
			doc.write(docx);
		}
		assertTrue(RouterEvaluationEvidence.readable("r.docx", docx.toByteArray()).contains("Quarterly report"));
		assertEquals("a,b\n1,2", RouterEvaluationEvidence.readable("d.csv", "a,b\n1,2".getBytes()));
		assertNull(RouterEvaluationEvidence.readable("pic.png", new byte[] { 1, 0, 2 }));
		assertNull(RouterEvaluationEvidence.readable("broken.xlsx", "not a zip".getBytes()));
	}

	@Test
	void aToolResultDoesNotCountAsAUserFollowUp() throws Exception {
		List<Object> transcript = List.of(
				Map.of("io", "INPUT", "parts", List.of(Map.of("type", "TEXT", "text", "Total the workbook"))),
				Map.of("io", "OUTPUT", "parts", List.of(Map.of("type", "TOOL_CALL", "toolCall", Map.of("name", "ReadFile")))),
				Map.of("io", "INPUT", "visible", false, "parts", List.of(Map.of("type", "TOOL_RESULT", "toolResult", Map.of("output", "15")))),
				Map.of("io", "OUTPUT", "parts", List.of(Map.of("type", "TEXT", "text", "The total is 15"))));
		var examples = select(List.of(run("Total the workbook", "COMPLETED", "workbook", transcript,
				Map.of("budget.csv", "15"), Map.of("budget.csv", "15"))));
		assertEquals(List.of("Total the workbook"), examples.stream().map(RouterLearningExamples.Usable::goal).toList());
	}

	@Test
	void aRunWithAUserMessageAfterTheAnswerDoesNotQualify() throws Exception {
		List<Object> pushback = List.of(Map.of("io", "OUTPUT", "parts", List.of()), Map.of("io", "INPUT", "parts", List.of()));
		assertEquals(0, select(List.of(run("pushed back", "COMPLETED", "a", pushback, Map.of(), Map.of()))).size());
		List<Object> clean = List.of(Map.of("io", "INPUT", "parts", List.of()), Map.of("io", "OUTPUT", "parts", List.of()));
		assertEquals(1, select(List.of(run("clean", "COMPLETED", "b", clean, Map.of(), Map.of()))).size());
		List<Object> followup = List.of(Map.of("io", "INPUT", "parts", List.of()), Map.of("io", "OUTPUT", "parts", List.of()),
				Map.of("io", "INPUT", "parts", List.of(Map.of("text", "change the result"))), Map.of("io", "OUTPUT", "parts", List.of()));
		assertEquals(0, select(List.of(run("follow-up", "COMPLETED", "c", followup, Map.of(), Map.of()))).size());
	}
}
