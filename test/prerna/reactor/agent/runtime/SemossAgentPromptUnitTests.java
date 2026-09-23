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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.message.InputMessage;
import prerna.om.Insight;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.config.AgentConfig;
import prerna.reactor.agent.stream.AgentRunStreamService;

class SemossAgentPromptUnitTests {

	@Test
	void modelReceivesComposedPromptOnceAndOriginalRoomModeSurvivesFailure() throws Exception {
		for (Boolean override : new Boolean[] { false, true, null }) {
			Room room = spy(new Room());
			room.setId("prompt-composition-test");
			Map<String, Object> originalOptions = new HashMap<>();
			originalOptions.put("instructions", "Active engine context");
			// A workspace would be loaded a second time if the harness forgot to
			// mark its already-composed instructions as an override.
			originalOptions.put("workspace", Map.of("workspace_id", "database-explorer"));
			if (override != null) {
				originalOptions.put("overrideSystemPrompt", override);
			}
			room.setOptionsMap(new HashMap<>(originalOptions));
			String authored = Boolean.FALSE.equals(override) ? "Agent persona\n\nActive engine context"
					: "Active engine context";
			AgentConfig config = AgentConfig.builder().authoredPrompt(authored).useDefaultAgentTools(false).build();
			AgentRunContext ctx = mock(AgentRunContext.class);
			when(ctx.getRoom()).thenReturn(room);
			when(ctx.getInsight()).thenReturn(mock(Insight.class));
			when(ctx.getModelEngine()).thenReturn(mock(IModelEngine.class));
			when(ctx.getAgentConfig()).thenReturn(config);
			when(ctx.getParamMap()).thenReturn(Map.of());
			when(ctx.getInput()).thenReturn("Analyze this database");
			when(ctx.getMaxTurns()).thenReturn(30);
			RuntimeException stopped = new IllegalStateException("Stop before contacting a model");
			doAnswer(call -> {
				InputMessage message = call.getArgument(0);
				String prompt = message.getSystemPrompt();
				assertTrue(prompt.startsWith(SemossHarnessPrompts.SYSTEM_PROMPT));
				assertTrue(prompt.contains(authored));
				assertEquals(prompt.indexOf("Active engine context"), prompt.lastIndexOf("Active engine context"));
				assertEquals(true, room.getOptionsMap().get("overrideSystemPrompt"));
				throw stopped;
			}).when(room).ask(any(), any(), any());
			try (var stream = mockStatic(AgentRunStreamService.class);
					var messages = mockStatic(RoomMessageStore.class)) {
				stream.when(AgentRunStreamService::get).thenReturn(mock(AgentRunStreamService.class));
				assertSame(stopped, assertThrows(RuntimeException.class, () -> new SemossAgentHarness().execute(ctx)));
			}
			assertEquals(originalOptions, room.getOptionsMap());
		}
	}
}
