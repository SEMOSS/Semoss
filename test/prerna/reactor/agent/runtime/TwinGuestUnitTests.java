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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.collaboration.CollaborationUtils;
import prerna.engine.impl.model.Room;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.config.AgentConfig;
import prerna.reactor.agent.run.HumanDelegationService;
import prerna.reactor.agent.subagent.SubAgentToolSynthesizer;

/**
 * A Digital Twin is a guest in the chat of whoever asks it. Its instructions
 * are someone else's, so whether or not a tool was listed for it, it runs none
 * of the asker's Collaboration tools, looks nobody up, and starts no sub-agent.
 */
class TwinGuestUnitTests {

	private static final String REFUSED = " is not available in a chat with a Digital Twin. Nothing was run. "
			+ "Do not retry it.";

	// the asker's mail, calendar, Teams, memory, topics, routines and skills, and how they write: GetWritingStyle
	// returns the asker's own persona
	private static final List<String> ASKERS_TOOLS = List.of("SearchMail", "ReadThread", "ListM365Mail",
			"ComposeEmail", "GetWritingStyle", "ListEvents", "CreateEvent", "Remember", "Forget", "SearchMemories",
			"TagTopic", "ShowItem", "CreateRoutine", "ManageRoutine", "PublishSkill", "UseSkillInAssistant");
	private static final List<String> SUB_AGENT_TOOLS = List.of(SubAgentToolSynthesizer.TOOL_SPAWN_SUBAGENT,
			SubAgentToolSynthesizer.TOOL_CHECK_SUBAGENT, SubAgentToolSynthesizer.TOOL_WAIT_SUBAGENT);
	// its own files and the chat's working directory
	private static final List<String> ITS_OWN = List.of("ReadFile", "GrepFiles", "GlobFiles", "CopyAgentFile",
			"WriteFile", "LoadSkill");

	// the file tool a twin has in its owner's own chat with it
	private static final List<String> ITS_FILE_TOOLS = List.of("SaveToDigitalTwin", "RemoveFromDigitalTwin");

	private static AgentRunContext run(boolean inCollaboration, boolean twin) {
		return run(inCollaboration, twin, false);
	}

	private static AgentRunContext run(boolean inCollaboration, boolean twin, boolean ownersOwnChat) {
		Room room = new Room();
		room.setOptionsMap(Map.of());
		if (inCollaboration) {
			room.setProjectId(CollaborationUtils.COLLABORATION_PROJECT_ID);
		}
		AgentRunContext ctx = mock(AgentRunContext.class);
		when(ctx.getRoom()).thenReturn(room);
		when(ctx.getAgentConfig())
				.thenReturn(AgentConfig.builder().digitalTwin(twin).digitalTwinOwnChat(ownersOwnChat).build());
		return ctx;
	}

	@Test
	void aTwinRunsNoneOfTheAskersToolsListedOrNot() {
		AgentRunContext twin = run(true, true);
		for (String tool : ASKERS_TOOLS) {
			assertEquals(tool + REFUSED, HarnessToolExecutor.twinRefusal(twin, tool), tool);
		}
		assertEquals(HumanDelegationService.FIND_PERSON_TOOL_NAME + REFUSED,
				HarnessToolExecutor.twinRefusal(twin, HumanDelegationService.FIND_PERSON_TOOL_NAME));
		// a sub-agent with no agent of its own is not a twin, and would be handed them
		for (String tool : SUB_AGENT_TOOLS) {
			assertEquals(tool + REFUSED, HarnessToolExecutor.twinRefusal(twin, tool), tool);
		}
		for (String tool : ITS_OWN) {
			assertNull(HarnessToolExecutor.twinRefusal(twin, tool), tool);
		}
		// adding to its files is its owner's to ask for, not a guest's
		for (String fileTool : ITS_FILE_TOOLS) {
			assertEquals(fileTool + REFUSED, HarnessToolExecutor.twinRefusal(twin, fileTool));
		}
	}

	@Test
	void inItsOwnersOwnChatItAddsToItsFilesAndStillRunsNothingElseOfTheirs() {
		AgentRunContext own = run(true, true, true);
		for (String fileTool : ITS_FILE_TOOLS) {
			assertNull(HarnessToolExecutor.twinRefusal(own, fileTool));
		}
		for (String tool : ASKERS_TOOLS) {
			assertEquals(tool + REFUSED, HarnessToolExecutor.twinRefusal(own, tool), tool);
		}
		// a sub-agent of that chat is not handed the tool, so it does not run it
		when(own.getSpawnDepth()).thenReturn(AgentRunContext.ROOT_SPAWN_DEPTH + 1);
		for (String fileTool : ITS_FILE_TOOLS) {
			assertEquals(fileTool + REFUSED, HarnessToolExecutor.twinRefusal(own, fileTool));
		}
	}

	@Test
	void anyOtherAgentInTheChatKeepsThem() {
		AgentRunContext assistant = run(true, false);
		for (String tool : ASKERS_TOOLS) {
			assertNull(HarnessToolExecutor.twinRefusal(assistant, tool), tool);
		}
		for (String tool : SUB_AGENT_TOOLS) {
			assertNull(HarnessToolExecutor.twinRefusal(assistant, tool), tool);
		}
		assertNull(HarnessToolExecutor.twinRefusal(assistant, HumanDelegationService.FIND_PERSON_TOOL_NAME));
	}

	@Test
	void outsideCollaborationATwinsOwnToolboxKeepsANameCollaborationUsesToo() {
		AgentRunContext twin = run(false, true);
		// nothing of the asker's answers to these names there
		assertNull(HarnessToolExecutor.twinRefusal(twin, "SearchMail"));
		assertNull(HarnessToolExecutor.twinRefusal(twin, "ListEvents"));
		// it still starts no sub-agent
		for (String tool : SUB_AGENT_TOOLS) {
			assertEquals(tool + REFUSED, HarnessToolExecutor.twinRefusal(twin, tool), tool);
		}
	}
}
