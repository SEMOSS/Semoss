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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.quartz.CronExpression;

class WorkExpenseRoutineUnitTests {

	@Test
	void matchesSubjectsThatStartWithARequestPrefix() {
		assertTrue(WorkExpenseRoutine.matches("Meeting Approval - Business Approval (Requested): OSLT In Person (102783)"));
		assertTrue(WorkExpenseRoutine.matches("  business approval: team offsite"));
		assertTrue(WorkExpenseRoutine.matches("[EXT] Meeting Approval"));
		assertTrue(WorkExpenseRoutine.matches("[EXT] [SECURE] Business Approval (Requested)"));
		assertFalse(WorkExpenseRoutine.matches("RE: Meeting Approval - OSLT In Person"));
		assertFalse(WorkExpenseRoutine.matches("Approval needed"));
		assertFalse(WorkExpenseRoutine.matches(null));
	}

	@Test
	void pixelNamesEveryRequestInOrder() {
		assertEquals("WorkRunExpenseRoutine(threadId=[\"t1\",\"t2\"], messageKey=[\"k1\",\"k2\"]);",
				WorkExpenseRoutine.pixel(List.of("t1", "t2"), List.of("k1", "k2")));
	}

	@Test
	void documentsFolderIdIsAUuidTheDocumentsFolderAccepts() {
		assertEquals(WorkExpenseRoutine.DOCUMENTS_ID, UUID.fromString(WorkExpenseRoutine.DOCUMENTS_ID).toString());
	}

	@Test
	void commandNamesTheGuidelineDocumentsOrSaysThereAreNone() {
		String withDocuments = WorkExpenseRoutine.command("America/New_York",
				List.of("travel-policy.pdf", "du-meetings.docx"));
		assertTrue(withDocuments.startsWith("Routine: \"Expense review\"\nIt is now "));
		assertTrue(withDocuments.contains("travel-policy.pdf, du-meetings.docx"));
		assertTrue(withDocuments.contains("${context.reviewInput}"));
		assertFalse(withDocuments.contains("%"));
		assertTrue(WorkExpenseRoutine.command("UTC", List.of()).contains("no guideline documents"));
		// the owner reads the decision first, then what to ask and the draft that asks it, then the request itself
		int decision = withDocuments.indexOf("\"Suggested decision:\"");
		int probes = withDocuments.indexOf("heading Before you respond");
		int draft = withDocuments.indexOf("heading Draft reply");
		int request = withDocuments.indexOf("heading Request");
		assertTrue(decision > 0 && decision < probes && probes < draft && draft < request);
		assertFalse(withDocuments.contains("Guideline check"));
		// the deliver step saves the draft; one the agent saved too would be a second copy
		assertTrue(withDocuments.contains("Do not call SaveDraft"));
		// approvers scan the summary for what breaks the guidelines, not for what passes
		assertTrue(withDocuments.contains("Leave out everything that complies"));
	}

	@Test
	void unscheduledKindValidatesWithoutATimeAndNeverFiresItsCron() throws Exception {
		Map<String, Object> config = WorkRoutine.validate(WorkExpenseRoutine.KIND,
				Map.of("enabled", true, "timezone", "America/New_York"));
		assertEquals(Map.of("enabled", true, "timezone", "America/New_York", "recipients", List.of()), config);
		Map<String, Object> withTime = new HashMap<>(config);
		withTime.put("time", "08:00");
		assertThrows(IllegalArgumentException.class, () -> WorkRoutine.validate(WorkExpenseRoutine.KIND, withTime));
		assertFalse(WorkExpenseRoutine.KIND.scheduled());
		assertTrue(WorkAgendaRoutine.KIND.scheduled());
		CronExpression never = new CronExpression(WorkRoutine.cron(Map.of("timezone", "America/New_York")));
		assertTrue(never.getNextValidTimeAfter(new java.util.Date()).getYear() + 1900 >= 2099);
	}

	@Test
	void recipientsAreBareAddressesKeptOnceAndCopiedOnTheReview() {
		Map<String, Object> config = WorkRoutine.validate(WorkExpenseRoutine.KIND, Map.of("enabled", false, "timezone",
				"UTC", "recipients", List.of(" jbell@deloitte.com ", "JBell@deloitte.com", "lhughes@deloitte.com")));
		assertEquals(List.of("jbell@deloitte.com", "lhughes@deloitte.com"), config.get("recipients"));
		assertEquals(Map.of("cc", config.get("recipients")), WorkExpenseRoutine.KIND.inputs().apply(Map.of(), config));
		for (Object recipients : List.of(List.of("Jen Bell"), List.of("a@x.com, b@x.com"), "a@x.com",
				Collections.nCopies(WorkExpenseRoutine.MAX_RECIPIENTS + 1, "a@x.com"))) {
			assertThrows(IllegalArgumentException.class, () -> WorkRoutine.validate(WorkExpenseRoutine.KIND,
					Map.of("enabled", false, "timezone", "UTC", "recipients", recipients)));
		}
	}
}
