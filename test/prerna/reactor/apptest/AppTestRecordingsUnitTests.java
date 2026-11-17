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
		Path dir = temporary.resolve("recordings");
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
}
