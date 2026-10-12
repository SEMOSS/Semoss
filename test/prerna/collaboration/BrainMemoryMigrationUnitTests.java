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

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.collaboration.BrainMemoryUtils.Memory;
import prerna.collaboration.BrainMemoryUtils.Ref;

class BrainMemoryMigrationUnitTests {

	private static final Timestamp CREATED = Timestamp.valueOf("2026-09-20 09:00:00");
	private static final Timestamp UPDATED = Timestamp.valueOf("2026-09-22 15:30:00");

	private static Map<String, Object> note(String state, String text) {
		Map<String, Object> row = new HashMap<>();
		row.put("ownerId", "owner");
		row.put("ownerType", "NATIVE");
		row.put("id", "note-1");
		row.put("refId", "topic-acme");
		row.put("text", text);
		row.put("state", state);
		row.put("origin", "you");
		row.put("sourceRef", "Kickoff call");
		row.put("createdAt", CREATED);
		row.put("updatedAt", UPDATED);
		return row;
	}

	private static Map<String, Object> fact(String state) {
		Map<String, Object> row = new HashMap<>();
		row.put("ownerId", "owner");
		row.put("ownerType", "NATIVE");
		row.put("id", "fact-1");
		row.put("refId", "thread-9");
		row.put("text", "The vendor agreed to a 10% discount");
		row.put("state", state);
		row.put("label", "Priya");
		row.put("personId", "p-priya");
		row.put("createdAt", CREATED);
		row.put("updatedAt", null);
		return row;
	}

	@Test
	void aConfirmedNoteStaysInUseAboutItsTopic() {
		Memory memory = BrainMemoryMigration.fromNote(note("confirmed", "Procurement needs three quotes"));
		assertEquals(BrainMemoryUtils.FACT, memory.kind());
		assertEquals("Procurement needs three quotes", memory.text());
		assertEquals(BrainMemoryUtils.ACTIVE, memory.state());
		assertTrue(memory.confirmed());
		assertEquals("you", memory.origin());
		assertEquals(List.of(new Ref(BrainMemoryUtils.TOPIC, "topic-acme")), memory.about());
		assertEquals(BrainMemoryUtils.FROM_TOPIC_NOTE, memory.source().kind());
		assertEquals("note-1", memory.source().ref());
		assertEquals("Kickoff call", memory.source().label());
		assertEquals(CREATED, memory.createdAt());
		assertEquals(UPDATED, memory.updatedAt());
		assertEquals(UPDATED, memory.confirmedAt());
	}

	@Test
	void aDraftNoteBecomesASuggestion() {
		Memory memory = BrainMemoryMigration.fromNote(note("draft", "Maybe renews in March"));
		assertEquals(BrainMemoryUtils.SUGGESTED, memory.state());
		assertFalse(memory.confirmed());
		assertNull(memory.confirmedAt());
	}

	@Test
	void anEmptyNoteIsDroppedAndALongOneIsCut() {
		assertNull(BrainMemoryMigration.fromNote(note("confirmed", "  \n ")));
		Memory memory = BrainMemoryMigration.fromNote(note("confirmed", "word ".repeat(200)));
		assertTrue(memory.text().length() <= BrainMemoryUtils.MAX_CHARS);
		assertFalse(memory.text().contains("  "));
	}

	@Test
	void aFactKeepsWhoSaidItAndItsThread() {
		Memory memory = BrainMemoryMigration.fromFact(fact("confirmed"));
		assertEquals(List.of(new Ref(BrainMemoryUtils.THREAD, "thread-9")), memory.about());
		assertEquals("p-priya", memory.source().personId());
		assertEquals("Priya", memory.source().label());
		assertEquals("thread-9", memory.source().threadId());
		assertEquals(BrainMemoryUtils.FROM_THREAD_FACT, memory.source().kind());
		assertEquals("you", memory.origin());
		// no UPDATED_AT: the creation time stands in
		assertEquals(CREATED, memory.updatedAt());
		assertEquals(BrainMemoryUtils.SUGGESTED, BrainMemoryMigration.fromFact(fact("draft")).state());
	}

	@Test
	void theSameRowAlwaysGetsTheSameId() {
		assertEquals(BrainMemoryMigration.fromNote(note("confirmed", "a")).id(),
				BrainMemoryMigration.fromNote(note("draft", "b")).id());
		assertFalse(BrainMemoryMigration.fromNote(note("confirmed", "a")).id()
				.equals(BrainMemoryMigration.fromFact(fact("confirmed")).id()));
	}
}
