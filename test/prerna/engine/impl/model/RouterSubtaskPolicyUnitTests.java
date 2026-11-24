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
package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.RouterSubtaskPolicy.Subtask;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;

class RouterSubtaskPolicyUnitTests {

	private static final Room ROOM = mock(Room.class);

	static {
		when(ROOM.getId()).thenReturn("room");
	}

	private static Subtask subtask(String id, String route) {
		Subtask s = new Subtask();
		s.id = id;
		s.route = route;
		s.recognition_rules = List.of("rule for " + id);
		return s;
	}

	private static RouterSubtaskPolicy policy(String defaultSubtask, Subtask... subtasks) {
		return RouterSubtaskPolicy.parse(List.of(subtasks), defaultSubtask, "v1", List.of("cheap", "Strong"), "router.json");
	}

	private static void invalid(String expected, String defaultSubtask, Subtask... subtasks) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> policy(defaultSubtask, subtasks));
		assertTrue(e.getMessage().contains(expected), e.getMessage());
	}

	@Test
	void validatesSubtasksAgainstTheRoutes() {
		Subtask ok = subtask("a", "cheap");
		assertEquals("a", policy("A", ok, subtask("b", "strong")).defaultSubtask().id());
		invalid("names route 'nope'", "a", subtask("a", "nope"));
		invalid("missing or duplicate id", "a", ok, subtask("A", "cheap"));
		invalid("default_subtask", "zzz", ok);
		String base = "{\"mode\": \"subtask\", \"classifier_engine\": \"c\", \"default_subtask\": \"a\", \"routes\": "
				+ "[{\"name\": \"cheap\", \"engine_id\": \"e1\"}], \"subtasks\": [{\"id\": \"a\", \"route\": \"%s\"}]}";
		assertNotNull(ModelRouterEngine.parseAndValidateConfig(base.formatted("cheap"), "router.json", "r"));
		assertThrows(IllegalArgumentException.class,
				() -> ModelRouterEngine.parseAndValidateConfig(base.formatted("missing"), "router.json", "r"));
	}

	@Test
	void classifierPromptHasGoalPreviousSubtaskAndTheLastSteps() {
		RouterSubtaskPolicy policy = policy("a", subtask("a", "cheap"), subtask("b", "strong"));
		List<String> steps = List.of("read (success): one", "run (error): " + "x".repeat(500));
		String prompt = policy.classifierPrompt("g".repeat(600), "a", steps);
		assertTrue(prompt.contains("- a: rule for a") && prompt.contains("- b: rule for b"));
		assertTrue(prompt.contains("Goal: " + "g".repeat(600)));
		assertTrue(prompt.contains("Previous subtask: a") && prompt.contains("Last 2 completed steps"));
		assertTrue(prompt.contains("1. read (success): one"));
		assertEquals("b", policy.parseChoice(" \"B\". ").id());
		assertNull(policy.parseChoice("unknown"));
	}

	private static List<AbstractMessage> branch(AbstractMessage... messages) {
		String parent = null;
		for (AbstractMessage message : messages) {
			message.setParentMessageId(parent);
			parent = message.getMessageId();
		}
		return new ArrayList<>(List.of(messages));
	}

	private static InputMessage tool(String name, String output) {
		return InputMessage.toolExecution(ROOM, "id-" + name, name, output, Map.of(), "success", false);
	}

	@Test
	void derivesGoalPreviousSubtaskAndStepsFromTheMessages() {
		ResponseMessage routed = new ResponseMessage.Builder()
				.withMetadata("routing", Map.of("trace", Map.of("subtask_id", "edit"))).build();
		List<AbstractMessage> messages = branch(InputMessage.text(ROOM, "fix the report"), tool("t1", "o1"), routed,
				tool("t2", "o2"), tool("t3", "o3\n  with   gaps"), tool("t4", "o4"));
		RouterSubtaskPolicy policy = policy("a", subtask("a", "cheap"));
		assertEquals("fix the report", policy.goalOf(messages));
		assertEquals("edit", RouterSubtaskPolicy.previousSubtaskOf(messages));
		assertEquals(List.of("t2 (success)", "t3 (success)", "t4 (success)"),
				policy.recentStepsOf(messages));
		assertNull(RouterSubtaskPolicy.previousSubtaskOf(branch(InputMessage.text(ROOM, "hi"))));
	}

	@Test
	void goalUsesTheLatestRequestNotCompactionOrReflectionText() {
		InputMessage summary = InputMessage.text(ROOM, "Summarizing Conversation");
		summary.setSummaryLeafMessageId("old-leaf");
		InputMessage request = InputMessage.text(ROOM, "Edit the workbook");
		request.setVisible(false);
		InputMessage reflection = InputMessage.text(ROOM, "Reflect on progress");
		reflection.setOrnament("agentRunRole", "reflection_input");
		RouterSubtaskPolicy policy = policy("a", subtask("a", "cheap"));

		assertEquals("Edit the workbook", policy.goalOf(branch(summary, request, reflection)));
		assertEquals("", policy.goalOf(branch(summary, reflection)));
	}

	@Test
	void recentStepsUseToolResultsInsteadOfRuntimeStatusText() {
		String runtimeStatus = "[SEMOSS runtime status] Run budget: 4 of 25 tool rounds used; 21 remaining. "
				+ "Current phase: editing. [/SEMOSS runtime status]";
		InputMessage read = InputMessage.builder(ROOM)
				.withToolResult("read-1", "BashCommand", "Read score.xlsx: 84 rows, midterm1 column present.",
						Map.of(), "success", false)
				.withText(runtimeStatus)
				.build();
		InputMessage write = InputMessage.builder(ROOM)
				.withToolResult("write-1", "WriteFile", "Wrote top3.txt with the three highest midterm1 scores.",
						Map.of("path", "top3.txt"), "success", false)
				.withText(runtimeStatus)
				.build();

		List<String> steps = policy("a", subtask("a", "cheap")).recentStepsOf(branch(read, write));

		assertEquals(List.of("BashCommand (success)", "WriteFile (success)"), steps);
		assertFalse(steps.stream().anyMatch(step -> step.contains("Run budget") || step.contains("Current phase")
				|| step.contains("84 rows") || step.contains("three highest")));
	}
}
