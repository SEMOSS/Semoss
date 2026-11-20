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

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** Thread insights: re-reading the same mail adds nothing, and the owner's steps are never rewritten. */
class WorkThreadInsightsUnitTests {

	private static final String ME = "self-person";

	private static WorkThreadInsights.Step brain(String id, String text, String status) {
		return new WorkThreadInsights.Step(id, text, status, ME, null, true, false);
	}

	private static WorkThreadInsights.Proposal keep(String stepId, String text) {
		return new WorkThreadInsights.Proposal(stepId, text, ME, null, false);
	}

	private static WorkThreadInsights.Proposal added(String text) {
		return new WorkThreadInsights.Proposal(null, text, ME, null, false);
	}

	@Test
	void theSameAnswerAgainChangesNothing() {
		List<WorkThreadInsights.Step> steps = List.of(brain("s1", "Send Kira the revised budget", "open"),
				new WorkThreadInsights.Step("s2", "Book the room", "open", ME, null, false, false));
		WorkThreadInsights.Plan plan = WorkThreadInsights.plan(steps,
				List.of(keep("s1", "Send Kira the revised budget"), added("Book the room")), ME);
		assertTrue(plan.inserts().isEmpty());
		assertTrue(plan.updates().isEmpty());
		assertTrue(plan.deletes().isEmpty());
	}

	@Test
	void anItemWithoutItsIdMatchesTheTrackedStepInsteadOfAddingOne() {
		List<WorkThreadInsights.Step> steps = List.of(brain("s1", "Send Kira the revised Q3 budget", "open"));
		WorkThreadInsights.Plan plan = WorkThreadInsights.plan(steps,
				List.of(added("send kira the revised q3 budget."), added("Send Kira the revised Q3 budget numbers")),
				ME);
		assertTrue(plan.inserts().isEmpty());
		assertTrue(plan.deletes().isEmpty());
	}

	@Test
	void dismissedAndFinishedStepsAreNotAddedAgainOrReopened() {
		List<WorkThreadInsights.Step> steps = List.of(brain("s1", "Call the vendor about pricing", "dismissed"),
				brain("s2", "Share the deck with the team", "done"));
		WorkThreadInsights.Plan plan = WorkThreadInsights.plan(steps,
				List.of(added("Call the vendor about pricing"), keep("s2", "Share the deck with the team")), ME);
		assertTrue(plan.inserts().isEmpty());
		assertTrue(plan.updates().isEmpty());
		assertTrue(plan.deletes().isEmpty());
	}

	@Test
	void aReplyClosesGeneratedStepsButNotTheOwnersOwn() {
		List<WorkThreadInsights.Step> steps = List.of(
				new WorkThreadInsights.Step("s1", "My reworded ask", "open", ME, null, true, true),
				new WorkThreadInsights.Step("s2", "My own reminder", "open", ME, null, false, false));
		WorkThreadInsights.Plan plan = WorkThreadInsights.plan(steps,
				List.of(new WorkThreadInsights.Proposal("s1", "Different text", ME, null, true),
						new WorkThreadInsights.Proposal("s2", "My own reminder", ME, null, true)),
				ME);
		assertEquals(1, plan.updates().size());
		WorkThreadInsights.Change change = plan.updates().get(0);
		assertEquals("s1", change.stepId());
		assertEquals("done", change.status());
		assertEquals("My reworded ask", change.text());
		assertFalse(change.content());
	}

	@Test
	void editedTextIsKeptAndUntouchedStepsLeftOutAreRemoved() {
		List<WorkThreadInsights.Step> steps = List.of(brain("s1", "Old generated ask", "open"),
				new WorkThreadInsights.Step("s2", "Edited by the owner", "open", ME, null, true, true),
				brain("s3", "Waiting on Kira", "waiting"), brain("s4", "Finished one", "done"),
				new WorkThreadInsights.Step("s5", "Manual one", "open", ME, null, false, false));
		WorkThreadInsights.Plan plan = WorkThreadInsights.plan(steps,
				List.of(keep("s2", "The model's wording"), added("A brand new ask")), ME);
		assertEquals(List.of("s1", "s3"), plan.deletes());
		assertTrue(plan.updates().isEmpty());
		assertEquals(List.of("A brand new ask"), plan.inserts().stream().map(WorkThreadInsights.Proposal::text).toList());
	}

	@Test
	void aGeneratedStepFollowsTheNewOwnerAndDueDate() {
		List<WorkThreadInsights.Step> steps = List.of(brain("s1", "Send the contract", "open"));
		WorkThreadInsights.Plan plan = WorkThreadInsights.plan(steps,
				List.of(new WorkThreadInsights.Proposal("s1", "Send the contract", "kira", "2026-10-09", false)), ME);
		assertEquals(1, plan.updates().size());
		WorkThreadInsights.Change change = plan.updates().get(0);
		assertEquals("waiting", change.status());
		assertEquals("kira", change.ownerId());
		assertEquals("2026-10-09", change.due());
		assertTrue(change.content());
	}

	@Test
	void newItemsAreAddedOnceAndFinishedNewOnesAreSkipped() {
		WorkThreadInsights.Plan plan = WorkThreadInsights.plan(List.of(),
				List.of(added("Review the draft agreement"), added("Review the draft agreement!"),
						new WorkThreadInsights.Proposal(null, "Already sent the invite", ME, null, true)),
				ME);
		assertEquals(1, plan.inserts().size());
	}

	@Test
	void shortIdsMapBackAndUnknownOnesFallBackSafely() {
		Map<String, String> tracked = Map.of("t1", "step-1");
		Map<String, String> people = Map.of("p1", "kira");
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("id", "t1");
		item.put("text", "  Send   the deck ");
		item.put("ownerId", "p1");
		item.put("due", "2026-10-09");
		item.put("done", false);
		WorkThreadInsights.Proposal mapped = WorkThreadInsights.proposal(item, tracked, people, ME);
		assertEquals("step-1", mapped.stepId());
		assertEquals("Send the deck", mapped.text());
		assertEquals("kira", mapped.ownerId());
		assertEquals("2026-10-09", mapped.due());

		item.put("id", "t9");
		item.put("ownerId", "someone");
		item.put("due", "2026-02-31");
		WorkThreadInsights.Proposal fallback = WorkThreadInsights.proposal(item, tracked, people, ME);
		assertNull(fallback.stepId());
		assertEquals(ME, fallback.ownerId());
		assertNull(fallback.due());
		assertEquals("open", WorkThreadInsights.statusFor(fallback.ownerId(), ME));
		assertEquals("waiting", WorkThreadInsights.statusFor("kira", ME));
	}

	@Test
	void readsAnswersWrappedInReasoningAndFences() {
		WorkThreadInsights.Answer answer = WorkThreadInsights.parse("<think>{\"summary\": 1}</think>\n```json\n"
				+ "{\"summary\": \"Kira needs the budget.\", \"actionItems\": [{\"id\": \"\", \"text\": \"Send it\", "
				+ "\"ownerId\": \"me\", \"due\": \"\", \"done\": false}, {\"text\": \"\"}]}\n```");
		assertNotNull(answer);
		assertEquals("Kira needs the budget.", answer.summary());
		assertEquals(1, answer.items().size());
		assertNull(WorkThreadInsights.parse("{\"actionItems\": []}"));
		assertNull(WorkThreadInsights.parse("not json"));
	}

	@Test
	void leavesOutExcludedMessagesAndKeepsTheNewestWithinBudget() {
		List<Map<String, Object>> read = new ArrayList<>();
		for (int i = 0; i < 15; i++) {
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("fromId", i % 2 == 0 ? "kira" : ME);
			m.put("at", "2026-10-0" + (i % 9 + 1) + "T10:00:00Z");
			m.put("text", i == 14 ? "newest" : "x".repeat(4000));
			m.put("excluded", i == 13);
			m.put("to", List.of(Map.of("name", "Kira", "address", "kira@example.com")));
			read.add(m);
		}
		List<Map<String, Object>> out = WorkThreadInsights.messages(read, Map.of("p1", "kira"), ME);
		assertEquals("newest", out.get(out.size() - 1).get("text"));
		assertEquals("p1", out.get(out.size() - 1).get("from"));
		// the excluded 14th message is gone: the 13th and then the owner's 12th come before the newest
		assertEquals("p1", out.get(out.size() - 2).get("from"));
		assertEquals("me", out.get(out.size() - 3).get("from"));
		assertTrue(out.stream().mapToInt(m -> ((String) m.get("text")).length()).sum() <= 40000);
		assertTrue(out.size() < 14);
	}

	@Test
	void aSummaryIsCurrentOnlyForTheNewestMessage() {
		assertTrue(WorkThreadInsights.covers("m2", "m2"));
		assertFalse(WorkThreadInsights.covers("m1", "m2"));
		assertFalse(WorkThreadInsights.covers(null, "m2"));
		assertTrue(WorkThreadInsights.covers(null, null));
	}
}
