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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/** Task set folders: the manifest, staging and produced files, the cache and the strongest route. */
class RouterEvaluationEvidenceUnitTests {

	@TempDir
	Path dir;

	@Test
	void loadsTasksWithTheFilesOfTheirFolderAndRefusesBadIdsAndEmptySets() throws Exception {
		Files.createDirectories(dir.resolve("t1"));
		Files.writeString(dir.resolve("t1/a.txt"), "hi");
		Files.writeString(dir.resolve("t1/.hidden"), "x");
		Files.writeString(dir.resolve("manifest.json"), "{\"tasks\":[{\"id\":\"t1\",\"prompt\":\"do it\"},{\"id\":\"t2\",\"prompt\":\"no files\"}]}");
		List<RouterLearningExamples.Task> tasks = RouterLearningExamples.load(dir);
		assertEquals(List.of(dir.resolve("t1/a.txt")), tasks.get(0).inputs());
		assertEquals(List.of(), tasks.get(1).inputs());
		Files.writeString(dir.resolve("manifest.json"), "{\"tasks\":[{\"id\":\"../x\",\"prompt\":\"p\"}]}");
		assertThrows(IllegalArgumentException.class, () -> RouterLearningExamples.load(dir));
		Files.writeString(dir.resolve("manifest.json"), "{\"tasks\":[]}");
		assertThrows(IllegalArgumentException.class, () -> RouterLearningExamples.load(dir));
		assertThrows(IllegalArgumentException.class, () -> RouterLearningExamples.setDir(dir, "../x"));
	}

	@Test
	void stagesInputsAndFindsTheFilesTheAgentWrote() throws Exception {
		Path input = Files.writeString(dir.resolve("in.txt"), "Aa");
		Path untouched = Files.writeString(dir.resolve("untouched.txt"), "untouched");
		Path room = dir.resolve("room");
		RouterLearningExamples.stage(new RouterLearningExamples.Task("t", "p", List.of(input, untouched)), room);
		Map<String, String> baseline = Map.of("in.txt", RouterLearningExamples.fingerprint(input),
				"untouched.txt", RouterLearningExamples.fingerprint(untouched));
		Files.writeString(room.resolve("out.txt"), "result");
		// These two contents collided under the old array hash.
		Files.writeString(room.resolve("in.txt"), "BB");
		Files.createDirectories(room.resolve(".baseline"));
		Files.writeString(room.resolve(".baseline/hidden.txt"), "ignored");
		assertEquals(List.of("in.txt", "out.txt"), RouterEvaluationEvidence.changed(room, baseline).stream().map(RouterEvaluationEvidence.Output::name).toList());
	}

	@Test
	void theJudgeReadsTextFilesAndSkipsBinaries() {
		assertEquals("hello", RouterEvaluationEvidence.readable("a.txt", "hello".getBytes()));
		assertEquals(null, RouterEvaluationEvidence.readable("a.bin", new byte[] { 1, 0, 2 }));
		assertEquals(null, RouterEvaluationEvidence.readable("broken.xlsx", "not a workbook".getBytes()));
	}

	@Test
	void theSideBySidePromptShowsBothResultsAndAsksForAVerdict() {
		String prompt = RouterEvaluationEvidence.sideBySidePrompt("sum it", "cand answer", List.of(new RouterEvaluationEvidence.Output("c.txt", "cand file".getBytes())), "ref answer",
				List.of(new RouterEvaluationEvidence.Output("r.txt", "ref file".getBytes())));
		assertTrue(prompt.contains("sum it") && prompt.contains("REFERENCE result") && prompt.contains("Final answer: ref answer") && prompt.contains("r.txt (8 bytes):\nref file"));
		assertTrue(prompt.indexOf("REFERENCE") < prompt.indexOf("CANDIDATE") && prompt.contains("c.txt (9 bytes):\ncand file"));
		assertTrue(prompt.contains("equal_or_better") && prompt.contains("worse"));
	}

	private static RouterEvaluationEvidence.Output out(String name, String text) {
		return new RouterEvaluationEvidence.Output(name, text.getBytes());
	}

	static RouterEvaluationEvidence.Output workbook(String name, int value) throws IOException {
		try (XSSFWorkbook book = new XSSFWorkbook(); ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
			book.createSheet("Totals").createRow(0).createCell(0).setCellValue(value);
			book.write(bytes);
			return new RouterEvaluationEvidence.Output(name, bytes.toByteArray());
		}
	}

	@Test
	void boundsRejectOversizedEvidenceInsteadOfClippingIt() {
		assertThrows(IllegalArgumentException.class, () -> RouterEvaluationEvidence.sideBySidePrompt("task", "answer",
				List.of(out("huge.csv", "value\n" + "x".repeat(4001))), "reference", List.of()));
	}

	@Test
	void thePromptShowsWorkbookFormulaAndItsEvaluatedValue() throws Exception {
		try (XSSFWorkbook book = new XSSFWorkbook(); ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
			book.createSheet("Totals").createRow(0).createCell(0).setCellFormula("6+6");
			book.write(bytes);
			String prompt = RouterEvaluationEvidence.sideBySidePrompt("sum it with a formula", "candidate",
					List.of(new RouterEvaluationEvidence.Output("out.xlsx", bytes.toByteArray())), "reference", List.of(workbook("out.xlsx", 12)));
			assertTrue(prompt.contains("[sheet Totals]\nA1==6+6 -> 12"));
			assertTrue(prompt.contains("[sheet Totals]\nA1=12"));
		}
	}

	@Test
	void cachedResultsAreUsedOnlyForTheSameSetManifestRepeatsAndScoringVersion() throws Exception {
		Path file = dir.resolve("cache/live-x.json");
		RouterEvaluationEvidence.writeCache(file, "set", "sha", 2, Map.of("t1", new RouterEvaluationEvidence.Result(0.5, 30, "d", List.of("run-1", "run-2"))));
		assertEquals(0.5, RouterEvaluationEvidence.readCache(file, "set", "sha", 2).get("t1").score());
		assertTrue(RouterEvaluationEvidence.readCache(file, "other", "sha", 2).isEmpty());
		assertTrue(RouterEvaluationEvidence.readCache(file, "set", "changed", 2).isEmpty());
		assertTrue(RouterEvaluationEvidence.readCache(file, "set", "sha", 3).isEmpty());
		assertTrue(RouterEvaluationEvidence.readCache(dir.resolve("missing.json"), "set", "sha", 2).isEmpty());
		JsonObject oldCache = JsonParser.parseString(Files.readString(file)).getAsJsonObject();
		oldCache.addProperty("scoring", "vs-strongest-structured-v3");
		Files.writeString(file, oldCache.toString());
		assertTrue(RouterEvaluationEvidence.readCache(file, "set", "sha", 2).isEmpty());
	}

	@Test
	void theStrongestRouteIsNamedOrTheHighestPricedAndNeverGuessed() {
		JsonObject config = JsonParser.parseString("{\"routes\":[{\"name\":\"cheap\",\"engine_id\":\"m1\"},{\"name\":\"big\",\"engine_id\":\"m2\"},"
				+ "{\"engine_id\":\"m3\"}]}").getAsJsonObject();
		Map<String, Double> prices = Map.of("m1", 1.0, "m2", 30.0, "m3", 5.0);
		assertEquals("m2", RouterEvaluationEvidence.strongest(config, prices::get));
		config.addProperty("strong_route", "CHEAP");
		assertEquals("m1", RouterEvaluationEvidence.strongest(config, prices::get));
		config.addProperty("strong_route", "nope");
		assertThrows(IllegalArgumentException.class, () -> RouterEvaluationEvidence.strongest(config, prices::get));
		config.remove("strong_route");
		assertThrows(IllegalArgumentException.class, () -> RouterEvaluationEvidence.strongest(config, id -> 0));
	}
}
