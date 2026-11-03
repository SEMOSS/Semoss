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
package prerna.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SearchAppLogsReactorTest {

	@TempDir
	Path tempDir;

	@Test
	void searchesNewestFirstAndStopsAfterPage() throws Exception {
		File active = write("app.log", "[INFO ] newest\n[ERROR] latest error\n");
		File rotated = write("app.log.1", "[WARN ] oldest\n[INFO ] older\n");

		SearchAppLogsReactor.SearchResult result = SearchAppLogsReactor.searchFiles(
				List.of(active, rotated), "", Set.of(), 0, 2);

		assertEquals(List.of("[ERROR] latest error", "[INFO ] newest"), result.lines());
		assertTrue(result.hasMore());
	}

	@Test
	void appliesOffsetTextAndLevelFilters() throws Exception {
		File active = write("app.log",
				"[ERROR] timeout old\n[INFO ] healthy\n[ERROR] timeout new\n[WARN ] timeout warning\n");

		SearchAppLogsReactor.SearchResult result = SearchAppLogsReactor.searchFiles(
				List.of(active), "timeout", Set.of("ERROR"), 1, 5);

		assertEquals(List.of("[ERROR] timeout old"), result.lines());
		assertFalse(result.hasMore());
	}

	private File write(String name, String content) throws Exception {
		Path file = tempDir.resolve(name);
		Files.writeString(file, content);
		return file.toFile();
	}
}
