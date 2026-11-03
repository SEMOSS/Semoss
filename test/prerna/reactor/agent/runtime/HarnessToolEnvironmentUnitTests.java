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
package prerna.reactor.agent.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import prerna.collaboration.CollaborationPrompts;

class HarnessToolEnvironmentUnitTests {

	@Test
	void describesBuiltInToolsWhenTheyAreTheDefaults() {
		String block = SemossAgentHarness.toolEnvironmentBlock(false, false, null);
		assertTrue(block.startsWith("## Tool environment\n- BashCommand, when enabled, allows: "));
		assertTrue(block.contains("tool result.\n\n- In ExecutePythonCode"));
		assertTrue(block.contains("destination.\n\n- node, npm, and npx are unavailable through BashCommand."));
	}

	@Test
	void leavesOutBuiltInToolNotesWhenAToolboxReplacesThem() {
		assertEquals("", SemossAgentHarness.toolEnvironmentBlock(true, false, null));

		String pptx = SemossAgentHarness.toolEnvironmentBlock(true, true, null);
		assertTrue(pptx.startsWith("## Tool environment\n- node, npm, npx and ExecuteNodeCode are unavailable"));
		assertTrue(pptx.contains("BuildPptx"));
		assertFalse(pptx.contains("BashCommand"));
		assertFalse(pptx.contains("ExecutePythonCode"));

		String collaboration = SemossAgentHarness.toolEnvironmentBlock(true, false,
				CollaborationPrompts.FILES_PROMPT);
		assertEquals(CollaborationPrompts.FILES_PROMPT, collaboration);
	}

	@Test
	void keepsCollaborationNotesBetweenBuiltInSections() {
		String block = SemossAgentHarness.toolEnvironmentBlock(false, false, CollaborationPrompts.FILES_PROMPT);
		assertTrue(block.contains("destination.\n\n" + CollaborationPrompts.FILES_PROMPT + "\n\n- node, npm"));
	}
}
