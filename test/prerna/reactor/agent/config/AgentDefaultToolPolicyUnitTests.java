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
package prerna.reactor.agent.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockStatic;

import java.util.Set;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import prerna.theme.PlaygroundThemeUtils;

class AgentDefaultToolPolicyUnitTests {

	private static Set<String> disabled(String cfgJson, String workspaceId, Set<String> themeExcluded) {
		try (var theme = mockStatic(PlaygroundThemeUtils.class)) {
			theme.when(() -> PlaygroundThemeUtils.getAgentExcludedDefaultTools(workspaceId)).thenReturn(themeExcluded);
			return AgentConfigLoader.resolveDisabledDefaultTools(null, cfgJson == null ? null : new JSONObject(cfgJson),
					workspaceId);
		}
	}

	@Test
	void plainChatGetsEveryThemeExclusion() {
		assertEquals(Set.of("BashCommand", "DeleteFile"), disabled(null, null, Set.of("BashCommand", "DeleteFile")));
	}

	@Test
	void agentKeepsOnlyTheExcludedToolsItEnables() {
		String cfg = "{\"tool_policy\":{\"default_tools\":{\"disabled\":[\"TodoWrite\"],\"enabled\":[\"BashCommand\"]}}}";
		assertEquals(Set.of("TodoWrite", "DeleteFile"), disabled(cfg, "agent-1", Set.of("BashCommand", "DeleteFile")));
	}

	@Test
	void enablingDoesNotOverrideTheAgentsOwnDisabledList() {
		String cfg = "{\"tool_policy\":{\"default_tools\":{\"disabled\":[\"BashCommand\"],\"enabled\":[\"BashCommand\"]}}}";
		assertEquals(Set.of("BashCommand"), disabled(cfg, "agent-1", Set.of("BashCommand")));
	}

	@Test
	void noThemeExclusionsLeavesTheAgentsPolicyAsIs() {
		String cfg = "{\"tool_policy\":{\"default_tools\":{\"disabled\":[\"TodoWrite\"]}}}";
		assertEquals(Set.of("TodoWrite"), disabled(cfg, "agent-1", Set.of()));
		assertEquals(Set.of(), disabled(null, null, Set.of()));
	}
}
