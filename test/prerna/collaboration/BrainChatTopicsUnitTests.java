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

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.collaboration.BrainMemoryReview.Turn;

/** What of a chat the topic classifier reads: the owner's words last, and the chat's name as its subject. */
class BrainChatTopicsUnitTests {

	@Test
	void theNewestTurnReadIsTheOwnersNotTheAssistantsReply() {
		List<Turn> read = BrainChatTopics.window(List.of(owner("Plan the Apollo launch"), assistant("Here is a plan"),
				owner("Add the budget"), assistant("Done, the budget is in"), assistant("Anything else?")));
		// a model that reads only the newest message reads "Add the budget"
		assertEquals(List.of("Plan the Apollo launch", "Here is a plan", "Add the budget"), texts(read));
		assertEquals(BrainMemoryReview.OWNER, read.get(read.size() - 1).role());
	}

	@Test
	void aChatTheOwnerHasNotWrittenInIsNotRead() {
		assertEquals(List.of(), BrainChatTopics.window(List.of(assistant("Welcome back"))));
		assertEquals(List.of(), BrainChatTopics.window(List.of()));
	}

	@Test
	void aLongTurnIsCutToItsStartAndOlderTurnsGoOnceTheyNoLongerFit() {
		List<Turn> turns = new ArrayList<>();
		for (int i = 0; i < 8; i++) {
			turns.add(owner(i + "x".repeat(2500)));
		}
		List<Turn> read = BrainChatTopics.window(turns);
		// 2000 characters and "..." a turn, 12000 in all: the newest five
		assertEquals(5, read.size());
		assertEquals(2003, read.get(0).text().length());
		assertEquals('3', read.get(0).text().charAt(0));
		assertEquals('7', read.get(4).text().charAt(0));
	}

	@Test
	void theChatsNameIsItsSubjectUnlessItSaysNothing() {
		assertEquals("Apollo launch plan", BrainChatTopics.subject("  Apollo launch plan "));
		assertEquals(BrainChatTopics.SUBJECT, BrainChatTopics.subject(null));
		assertEquals(BrainChatTopics.SUBJECT, BrainChatTopics.subject("   "));
		// a chat opened on an email is first named by the start of its context envelope
		assertEquals(BrainChatTopics.SUBJECT, BrainChatTopics.subject("[SEMOSS_WORK_CONTEXT_V1]\n{\"thread\":{\"id\""));
	}

	private static Turn owner(String text) {
		return new Turn(BrainMemoryReview.OWNER, text, null);
	}

	private static Turn assistant(String text) {
		return new Turn(BrainMemoryReview.ASSISTANT, text, null);
	}

	private static List<String> texts(List<Turn> turns) {
		return turns.stream().map(Turn::text).toList();
	}
}
