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
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AppLogManagerTest {

	@TempDir
	File tempDir;

	@Test
	void defaultsToTomcatRuntimeLogDirectory() {
		String previous = System.getProperty("catalina.base");
		try {
			System.setProperty("catalina.base", tempDir.getAbsolutePath());

			assertEquals(
					tempDir.toPath().resolve("logs").resolve("apps").resolve("project-1").resolve("app.log")
							.toString(),
					AppLogManager.getLogFilePath("project-1", "Project One"));
		} finally {
			if (previous == null) {
				System.clearProperty("catalina.base");
			} else {
				System.setProperty("catalina.base", previous);
			}
		}
	}

	@Test
	void capturesProjectLoggersAndExplicitLogMessagesOnly() {
		assertTrue(AppLogManager.isApplicationLogger("com.example.semoss.reactors.CustomReactor"));
		assertTrue(AppLogManager.isApplicationLogger("reactors.GenerateReport"));
		assertTrue(AppLogManager.isApplicationLogger("prerna.reactor.LogMessage"));

		assertFalse(AppLogManager.isApplicationLogger("prerna.om.Insight"));
		assertFalse(AppLogManager.isApplicationLogger("prerna.logging.SearchAppLogsReactor"));
		assertFalse(AppLogManager.isApplicationLogger("EngineLogger"));
	}

	@Test
	void returnsDatedAndLegacyArchivesNewestFirst() throws Exception {
		String previousCatalinaBase = System.getProperty("catalina.base");
		try {
			System.setProperty("catalina.base", tempDir.getAbsolutePath());
			File active = writeLog("app.log", "active", Instant.parse("2026-09-24T12:03:00Z"));
			File newest = writeLog("app.log.2026-09-24.1", "newest", Instant.parse("2026-09-24T12:02:00Z"));
			File older = writeLog("app.log.2026-09-23.2", "older", Instant.parse("2026-09-23T12:00:00Z"));
			File legacy = writeLog("app.log.1", "legacy", Instant.parse("2026-09-22T12:00:00Z"));
			writeLog("app.log.pre-filter", "ignored", Instant.parse("2026-09-25T12:00:00Z"));

			assertEquals(List.of(active, newest, older, legacy),
					AppLogManager.getLogFiles("project-1", "Project One"));
		} finally {
			if (previousCatalinaBase == null) {
				System.clearProperty("catalina.base");
			} else {
				System.setProperty("catalina.base", previousCatalinaBase);
			}
		}
	}

	private File writeLog(String name, String content, Instant modified) throws Exception {
		File directory = tempDir.toPath().resolve("logs").resolve("apps").resolve("project-1").toFile();
		assertTrue(directory.mkdirs() || directory.isDirectory());
		File file = new File(directory, name);
		Files.writeString(file.toPath(), content);
		Files.setLastModifiedTime(file.toPath(), FileTime.from(modified));
		return file;
	}
}
