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
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;

/** Reconcile's merging in code and its reading of the model's reply, with the model and the work list stubbed. */
class WorkReconcileUnitTests {

	@Test
	void onlyTheSameWordsAreTheSameAskInCode() {
		assertTrue(WorkReconcile.similar("Approve Kira's PTO request", "approve Kira's PTO request."));
		assertFalse(WorkReconcile.similar("Approve Kira's PTO request", "Approve Bob's PTO request"));
		assertFalse(WorkReconcile.similar("Review PR #3245 before the release", "Review PR #3246 before the release"));
		// a rewording is the model's same_as
		assertFalse(WorkReconcile.similar("Widen the chat window", "Allow the chat window to widen"));
	}

	@Test
	@SuppressWarnings("unchecked")
	void theSameAskTwiceIsOneStepAndAnotherNameIsAnother() {
		Map<String, Object> topic = plan("{\"decisions\": [], \"newItems\": [], \"loose\": []}",
				task("c1", "Approve Kira's PTO request"), task("c2", "approve Kira's PTO request."),
				task("c3", "Approve Bob's PTO request"));
		assertNull(topic.get("error"));
		assertEquals(List.of(List.of("c1", "c2"), List.of("c3")),
				((List<Map<String, Object>>) topic.get("looseSteps")).stream().map(s -> s.get("evidence")).toList());
	}

	@Test
	void aReplyCutOffMidListLeavesTheTopicPending() {
		// the token cap ends the reply inside the decisions list; its first whole object is one decision
		Map<String, Object> topic = plan("{\"decisions\": [{\"candidate\": \"c1\", \"action\": \"leftover\", "
				+ "\"ref\": \"\", \"reason\": \"\"}, {\"candidate\": \"c2\", \"act", task("c1", "Send Kira the deck"),
				task("c2", "Book the venue"));
		assertEquals("unreadable reconcile", topic.get("error"));
		assertNull(topic.get("looseSteps"));
	}

	// one batch with no topic, planned against an empty work list
	@SafeVarargs
	@SuppressWarnings("unchecked")
	private static Map<String, Object> plan(String reply, Map<String, Object>... candidates) {
		IModelEngine model = mock(IModelEngine.class);
		when(model.ask(anyString(), anyString(), any(), anyMap()))
				.thenReturn(new AskStringModelEngineResponse(reply, 0, 0));
		try (MockedStatic<WorkTaskUtils> work = mockStatic(WorkTaskUtils.class)) {
			work.when(() -> WorkTaskUtils.listMyWork(any(), anyMap())).thenReturn(Map.of("workItems", List.of()));
			Map<String, Object> plan = WorkReconcile.plan(new User(), "owner", "NATIVE", model, List.of(candidates));
			return ((List<Map<String, Object>>) plan.get("topics")).get(0);
		}
	}

	private static Map<String, Object> task(String id, String text) {
		Map<String, Object> c = new LinkedHashMap<>();
		c.put("id", id);
		c.put("kind", "task");
		c.put("assignee", "me");
		c.put("text", text);
		return c;
	}
}
