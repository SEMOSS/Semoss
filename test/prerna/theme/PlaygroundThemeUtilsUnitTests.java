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
package prerna.theme;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;

class PlaygroundThemeUtilsUnitTests {

	@AfterEach
	void clearCache() {
		// no themes database: the cache resets to an empty theme for later tests
		PlaygroundThemeUtils.refreshCacheFromActiveTheme();
	}

	private static void activate(String themeMap) {
		try (var registry = mockStatic(SystemEngineRegistry.class); var admin = mockStatic(AdminThemeUtils.class)) {
			registry.when(SystemEngineRegistry::getThemesDb).thenReturn(mock(IRDBMSEngine.class));
			admin.when(AdminThemeUtils::getActiveAdminTheme).thenReturn(Map.of("THEME_MAP", themeMap));
			PlaygroundThemeUtils.refreshCacheFromActiveTheme();
		}
	}

	@Test
	void readsExcludedDefaultToolsTrimmedAndDeduplicated() {
		activate("{\"playground\":{\"agentHarness\":{\"excludedDefaultTools\":"
				+ "[\" BashCommand \",\"DeleteFile\",\"BashCommand\",\"\",7,null,{}]}}}");
		assertEquals(List.of("BashCommand", "DeleteFile", "7"),
				List.copyOf(PlaygroundThemeUtils.getAgentExcludedDefaultTools()));
	}

	@Test
	void missingOrMalformedSettingExcludesNothing() {
		for (String themeMap : List.of("{}", "{\"playground\":{}}", "{\"playground\":{\"agentHarness\":[]}}",
				"{\"playground\":{\"agentHarness\":{\"excludedDefaultTools\":\"BashCommand\"}}}", "not json")) {
			activate(themeMap);
			assertTrue(PlaygroundThemeUtils.getAgentExcludedDefaultTools().isEmpty(), themeMap);
		}
	}

	@Test
	void systemAgentsAreNeverSubjectToTheExclusions() {
		activate("{\"playground\":{\"agentHarness\":{\"excludedDefaultTools\":[\"BashCommand\"]}}}");
		assertEquals(Set.of("BashCommand"), PlaygroundThemeUtils.getAgentExcludedDefaultTools(null));
		assertEquals(Set.of("BashCommand"), PlaygroundThemeUtils.getAgentExcludedDefaultTools("user-agent"));
		assertTrue(PlaygroundThemeUtils.getAgentExcludedDefaultTools(Constants.AGENT_APP_BUILDER).isEmpty());
	}
}
