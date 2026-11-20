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

import java.nio.file.Path;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AppTestRecordingsUnitTests {
	@TempDir Path temporary;

	@Test
	void savesListsUpdatesAndDeletesWithoutCrossingAppFolders() throws Exception {
		Path firstApp = temporary.resolve("first/app-tests");
		Path otherApp = temporary.resolve("other/app-tests");
		Map<String, Object> saved = AppTestRecordings.save(firstApp, recording("Original"));
		assertEquals(1, AppTestRecordings.list(firstApp).size());
		assertEquals("Verify the Original workflow.", AppTestRecordings.list(firstApp).getFirst().get("description"));
		assertTrue(AppTestRecordings.list(otherApp).isEmpty());
		saved.put("name", "Renamed");
		saved.put("description", "Verify the renamed workflow.");
		AppTestRecordings.save(firstApp, saved);
		assertEquals("Renamed", AppTestRecordings.list(firstApp).getFirst().get("name"));
		assertEquals("Verify the renamed workflow.", AppTestRecordings.list(firstApp).getFirst().get("description"));
		assertTrue(AppTestRecordings.delete(firstApp, (String) saved.get("id")));
		assertTrue(AppTestRecordings.list(firstApp).isEmpty());
	}

	@Test
	void rejectsTraversalRemoteUrlsAndUnsupportedSteps() {
		for (String path : List.of("../other", "/Monolith", "//remote", "https://remote", "%2e%2e/other", "x\\other", "a/../b")) {
			assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.relativePath(path), path);
		}
		assertEquals("#/orders", AppTestRecordings.relativePath("#/orders"));
		assertEquals("orders?q=hello%20world#some%20where", AppTestRecordings.relativePath("orders?q=hello%20world#some%20where"));
		assertEquals("hello%20world", AppTestRecordings.relativePath("hello%20world"));
		assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.id("../../report"));
		assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(Map.of("name", "x", "description", "Reject arbitrary scripts.", "steps", List.of(Map.of("type", "EVAL", "value", "arbitrary script")))));
	}

	@Test
	void retainsActionOnlyWorkflowsButRejectsEmptyAndOversizedRecordings() {
		assertNotNull(AppTestRecordings.validate(recording("Workflow")).get("id"));
		assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(Map.of("name", "Empty", "description", "Reject an empty workflow.", "steps", List.of())));
		assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(Map.of("name", "Too many", "description", "Reject too many steps.", "steps", java.util.Collections.nCopies(301, Map.of("type", "CLICK", "selector", "button")))));
	}

	@Test
	void requiresMeaningfulBoundedDescriptionsForNewSaves() {
		Map<String, Object> draft = new LinkedHashMap<>(recording("Workflow"));
		draft.remove("description");
		assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.save(temporary, draft));
		for (Object value : List.of(" ", 1, "x".repeat(2001))) {
			draft.put("description", value);
			assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(draft));
		}
		draft.put("description", "  Click Save and check the confirmation.  ");
		assertEquals("Click Save and check the confirmation.", AppTestRecordings.validate(draft).get("description"));
	}

	@Test
	void migratesLegacyRecordingsAndRetainsTheirTimestamp() throws Exception {
		String id = "11111111-1111-4111-8111-111111111111";
		Path dir = temporary.resolve("tests");
		Files.createDirectories(dir);
		Path file = dir.resolve(id + ".json");
		Files.writeString(file, "{\"id\":\"" + id + "\",\"name\":\"Old workflow\",\"version\":1,\"updatedAt\":\"2026-11-13T12:00:00Z\",\"steps\":[{\"type\":\"CLICK\",\"selector\":\"button\"}]}");
		Map<String, Object> migrated = AppTestRecordings.list(temporary).getFirst();
		assertEquals("Replays the recorded workflow: Old workflow.", migrated.get("description"));
		assertEquals("2026-11-13T12:00:00Z", migrated.get("updatedAt"));
		assertTrue(Files.readString(file).contains("\"description\""));
		assertEquals(migrated, AppTestRecordings.list(temporary).getFirst());
	}

	@Test
	void noTestsAreReportedAsNoTestsInsteadOfPassed() {
		Map<String, Object> report = AppTestRunner.report("run", "app", List.of());
		assertEquals("NO_TESTS", report.get("status"));
		assertEquals(0L, ((Map<?, ?>) report.get("summary")).get("passed"));
	}

	private Map<String, Object> recording(String name) {
		return Map.of("name", name, "description", "Verify the " + name + " workflow.", "startPath", "", "steps", List.of(Map.of("type", "CLICK", "selector", "button")));
	}
    @Test
    void preservesPageBoundariesAndRejectsPathsOutsideThePortal() {
        Map<String,Object> input = Map.of("name","Orders","description","Open Orders and verify the result.","steps",List.of(
            Map.of("type","ASSERT_VISIBLE","selector","#result","pagePath","orders?state=ready#/review")));
        Map<String,Object> validated=AppTestRecordings.validate(input);
        assertEquals("orders?state=ready#/review", ((Map<?,?>)((List<?>)validated.get("steps")).getFirst()).get("pagePath"));
        assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(Map.of("name","Orders","description","Check orders.","steps",List.of(
            Map.of("type","CLICK","selector","#result","pagePath","../another-app")))));
    }
    @Test
    void neverDowngradesAnExperimentalDatabaseBoundRecordingToNormalConnections() {
        assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(Map.of("name","Legacy database test","description","Needs a separate database.",
            "engineTestConfigurationId","aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","steps",List.of(Map.of("type","ASSERT_VISIBLE","selector","#result")))));
    }

	@Test
	void movesOnlyLegacyRecordingsToAssetsAndKeepsPrivateReports() throws Exception {
		Path old = temporary.resolve("app_root/app-tests");
		Path assets = temporary.resolve("app_root/version/assets/app-tests");
		Map<String, Object> saved = AppTestRecordings.save(old, recording("Old"));
		Files.move(old.resolve("tests"), old.resolve("recordings"));
		Path report = old.resolve("reports/private/report.json");
		Files.createDirectories(report.getParent());
		Files.writeString(report, "private evidence");
		AppTestRecordings.migrate(old, assets);
		assertEquals(saved, AppTestRecordings.list(assets).getFirst());
		assertTrue(AppTestRecordings.list(old).isEmpty());
		assertTrue(Files.exists(report));
		assertFalse(Files.exists(assets.resolve("reports")));
		AppTestRecordings.migrate(old, assets);
		assertEquals(1, AppTestRecordings.list(assets).size());
	}

	@Test
	void migrationNeverOverwritesAConflictingAsset() throws Exception {
		Path old = temporary.resolve("old");
		Path assets = temporary.resolve("assets");
		Map<String, Object> original = AppTestRecordings.save(old, recording("Original"));
		Files.move(old.resolve("tests"), old.resolve("recordings"));
		Map<String, Object> changed = new LinkedHashMap<>(original);
		changed.put("description", "A deliberately edited test.");
		AppTestRecordings.save(assets, changed);
		assertThrows(java.io.IOException.class, () -> AppTestRecordings.migrate(old, assets));
		assertEquals("A deliberately edited test.", AppTestRecordings.list(assets).getFirst().get("description"));
		assertTrue(Files.exists(old.resolve("recordings/" + original.get("id") + ".json")));
	}

	@Test
	void cannotChangeARecordingIdByEditingItsContents() throws Exception {
		Map<String, Object> saved = AppTestRecordings.save(temporary, recording("Original"));
		Path file = temporary.resolve("tests/" + saved.get("id") + ".json");
		saved.put("id", "22222222-2222-4222-8222-222222222222");
		AppTestRecordings.write(file, saved);
		assertThrows(java.io.IOException.class, () -> AppTestRecordings.list(temporary));
	}

	@Test
	void anInvalidNeighbourDoesNotBlockSavingAndIsNamedWhenListed() throws Exception {
		Map<String, Object> first = AppTestRecordings.save(temporary, recording("First"));
		Files.writeString(temporary.resolve("tests/change-task-lane.json"), "{\"id\":\"change-task-lane\",\"version\":1,\"name\":\"Bad\",\"description\":\"x\",\"steps\":[{\"type\":\"CLICK\",\"selector\":\"a\"}]}");
		Map<String, Object> second = AppTestRecordings.save(temporary, recording("Second"));
		assertTrue(Files.exists(temporary.resolve("tests/" + second.get("id") + ".json")));
		assertEquals(first.get("name"), AppTestRecordings.get(temporary, (String) first.get("id")).get("name"));
		java.io.IOException failure = assertThrows(java.io.IOException.class, () -> AppTestRecordings.list(temporary));
		assertTrue(failure.getMessage().contains("change-task-lane.json"));
		assertTrue(failure.getMessage().contains("Invalid recording id"));
	}

	@Test
	void discoveryReturnsOnlyIdNameAndDescription() throws Exception {
		Map<String, Object> saved = AppTestRecordings.save(temporary, recording("Workflow"));
		Map<String, Object> summary = AppTestRecordings.summaries(temporary).getFirst();
		assertEquals(java.util.Set.of("id", "name", "description"), summary.keySet());
		assertEquals(saved.get("id"), summary.get("id"));
		assertEquals(saved, AppTestRecordings.get(temporary, (String) summary.get("id")));
	}

	@Test
	void selectedReadsIgnoreOtherFilesAndNeverCrossAppFolders() throws Exception {
		Path first = temporary.resolve("first");
		Map<String, Object> saved = AppTestRecordings.save(first, recording("Selected"));
		String selected = (String) saved.get("id");
		Files.writeString(first.resolve("tests/22222222-2222-4222-8222-222222222222.json"), "not JSON");
		assertEquals(saved, AppTestRecordings.get(first, selected));
		assertEquals(List.of(saved), AppTestRuns.loadSelected(first, null, List.of(selected), false, List.of()));
		assertEquals(List.of(saved), AppTestRuns.loadSelected(first, selected, null, false, List.of()));
		assertThrows(java.io.IOException.class, () -> AppTestRuns.loadSelected(first, null, null, true, List.of()));
		assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.get(temporary.resolve("other"), selected));
		assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.get(first, "../other"));
	}

	@Test
	void explicitSelectionLoadsOnlyChosenIdsAndAllMustBeIntentional() throws Exception {
		Map<String, Object> first = AppTestRecordings.save(temporary, recording("First"));
		Map<String, Object> second = AppTestRecordings.save(temporary, recording("Second"));
		String one = (String) first.get("id"), two = (String) second.get("id");
		assertEquals(List.of(second, first), AppTestRuns.loadSelected(temporary, null, List.of(two, one), false, List.of()));
		assertEquals(2, AppTestRuns.loadSelected(temporary, null, null, true, List.of()).size());
		assertEquals(List.of(first), AppTestRuns.loadSelected(temporary, null, List.of(one, two), false, List.of("First")));
		assertTrue(AppTestRuns.loadSelected(temporary, null, List.of(two), false, List.of("First")).isEmpty());
		assertThrows(IllegalArgumentException.class, () -> AppTestRuns.selectedIds(null, List.of(), false));
		assertThrows(IllegalArgumentException.class, () -> AppTestRuns.selectedIds(one, List.of(two), false));
		assertThrows(IllegalArgumentException.class, () -> AppTestRuns.selectedIds(null, List.of(one), true));
		assertThrows(IllegalArgumentException.class, () -> AppTestRuns.selectedIds(one, null, true));
		assertThrows(IllegalArgumentException.class, () -> AppTestRuns.selectedIds(null, List.of(one, one), false));
		assertThrows(IllegalArgumentException.class, () -> AppTestRuns.selectedIds("First", null, false));
		assertThrows(IllegalArgumentException.class, () -> AppTestRuns.loadSelected(temporary, null,
				List.of(one, "33333333-3333-4333-8333-333333333333"), false, List.of()));
	}

	@Test
	void renamesTheAssetsRecordingsFolderToTestsWithoutMovingReports() throws Exception {
		Map<String, Object> saved = AppTestRecordings.save(temporary, recording("Existing asset"));
		Files.move(temporary.resolve("tests"), temporary.resolve("recordings"));
		Files.createDirectories(temporary.resolve("reports"));
		Files.writeString(temporary.resolve("reports/evidence.json"), "private report");
		AppTestRecordings.migrate(temporary, temporary);
		assertEquals(saved, AppTestRecordings.list(temporary).getFirst());
		assertTrue(Files.exists(temporary.resolve("tests/" + saved.get("id") + ".json")));
		assertFalse(Files.exists(temporary.resolve("recordings")));
		assertEquals("private report", Files.readString(temporary.resolve("reports/evidence.json")));
		AppTestRecordings.migrate(temporary, temporary);
		assertEquals(1, AppTestRecordings.list(temporary).size());
	}

	@Test
	void validationErrorsIdentifyTheFieldStepAndSupportedFormat() {
		Map<String, Object> input = new LinkedHashMap<>(recording("Diagnostic"));
		input.put("startPath", "/portals/index.html");
		String pathError = assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(input)).getMessage();
		assertTrue(pathError.startsWith("startPath:"));
		assertTrue(pathError.contains("index.html#/"));
		input.put("startPath", "index.html?env=test#/boards/b1");
		input.put("steps", List.of(Map.of("type", "CLICK", "selector", "a"), Map.of("type", "GOTO", "url", "#/task/t1")));
		String typeError = assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(input)).getMessage();
		assertTrue(typeError.startsWith("Step 2:"));
		assertTrue(typeError.contains("GOTO"));
		assertTrue(typeError.contains("NAVIGATE uses path"));
		input.put("steps", List.of(Map.of("type", "CLICK", "selector", "a", "pagePath", "/task/t1")));
		assertTrue(assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(input)).getMessage().startsWith("Step 1: pagePath:"));
		input.put("steps", List.of(Map.of("type", "NAVIGATE", "url", "#/task/t1")));
		assertTrue(assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(input)).getMessage().contains("NAVIGATE requires a string path"));
		input.put("id", "my-test-name");
		assertTrue(assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(input)).getMessage().contains("Omit id"));
	}

	@Test
	void acceptsHashRoutingAndTestQueryOnEveryPageOfTheUserWorkflow() throws Exception {
		Map<String, Object> saved = AppTestRecordings.save(temporary, Map.of(
			"name", "Task lane", "description", "Save and verify a task lane on a separate page.", "startPath", "#/",
			"steps", List.of(
				Map.of("type", "CLICK", "selector", "[data-testid=\"task-t1\"]", "pagePath", "#/"),
				Map.of("type", "SELECT", "selector", "[data-testid=\"task-lane\"]", "value", "In Progress", "pagePath", "#/task/t1"),
				Map.of("type", "CLICK", "selector", "[data-testid=\"task-save\"]", "pagePath", "#/task/t1"),
				Map.of("type", "ASSERT_VISIBLE", "selector", "[data-testid=\"task-t1\"]", "pagePath", "#/"))));
		assertEquals(saved, AppTestRecordings.get(temporary, (String) saved.get("id")));
		assertEquals("index.html?env=test#/boards/b1/tasks/t1", AppTestRecordings.relativePath("index.html?env=test#/boards/b1/tasks/t1"));
	}

    @Test void discoveryKeepsValidMetadataAndAllowsOnlyExplicitMalformedFileDeletion() throws Exception {
        Map<String,Object> saved=AppTestRecordings.save(temporary,recording("Good"));
        Files.writeString(temporary.resolve("tests/bad-name.json"),"{\"id\":\"not-a-uuid\"}");
        Map<String,Object> listing=AppTestRecordings.discover(temporary);
        assertEquals(1,((List<?>)listing.get("recordings")).size());
        assertEquals("bad-name.json",((Map<?,?>)((List<?>)listing.get("invalid")).getFirst()).get("file"));
        assertFalse(listing.toString().contains("selector"));
        assertThrows(java.io.IOException.class,()->AppTestRuns.loadSelected(temporary,null,null,true,List.of()));
        assertEquals(saved,AppTestRecordings.get(temporary,(String)saved.get("id")));
        assertThrows(IllegalArgumentException.class,()->AppTestRecordings.deleteInvalid(temporary,"../outside.json"));
        assertThrows(IllegalArgumentException.class,()->AppTestRecordings.deleteInvalid(temporary,saved.get("id")+".json"));
        assertTrue(AppTestRecordings.deleteInvalid(temporary,"bad-name.json"));
    }

    @Test void validatesExtendedStepFieldsAndExplainsFrameSemantics() {
        Map<String,Object> definition=new LinkedHashMap<>(recording("Fields"));
        definition.put("steps",List.of(Map.of("type","ASSERT_VALUE","selector","select","value","ready","timeoutMs",30000)));
        assertNotNull(AppTestRecordings.validate(definition));
        for (Map<String,Object> step : List.of(Map.<String,Object>of("type","ASSERT_COUNT","selector","a","count",-1),Map.<String,Object>of("type","WAIT_FOR","selector","a","state","forever"),Map.<String,Object>of("type","CLICK","selector","a","timeoutMs",30001))) {
            definition.put("steps",List.of(step));assertThrows(IllegalArgumentException.class,()->AppTestRecordings.validate(definition));
        }
        definition.put("steps",List.of(Map.of("type","CLICK","selector","a","framePath",List.of(Map.of("data-testid","lane")))));
        assertTrue(assertThrows(IllegalArgumentException.class,()->AppTestRecordings.validate(definition)).getMessage().contains("nested iframes"));
        assertTrue(assertThrows(IllegalArgumentException.class,()->AppTestRecordings.validate(null)).getMessage().contains("not a JSON string"));
    }

}
