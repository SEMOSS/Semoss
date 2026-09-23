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
}
