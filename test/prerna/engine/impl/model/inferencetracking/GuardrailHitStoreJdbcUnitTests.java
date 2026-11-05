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
package prerna.engine.impl.model.inferencetracking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;
import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;
import prerna.util.SystemEngineRegistry;

public class GuardrailHitStoreJdbcUnitTests {

	private static final Pair<String, String> ALICE = Pair.with("alice", "NATIVE");
	private static final Pair<String, String> ALICE_MICROSOFT = Pair.with("alice", "MICROSOFT");
	private static final Pair<String, String> ADMIN = Pair.with("admin", "NATIVE");
	private static final Timestamp NOW = Timestamp.valueOf("2026-10-29 12:00:00");

	/**
	 * Creates the hit and lock tables, as the model inference schema declares them
	 */
	public static void createTables(JdbcTestDatabase db) throws Exception {
		db.execute("CREATE TABLE GUARDRAIL_HIT (HIT_ID VARCHAR(50), ENGINE_ID VARCHAR(50), CHECK_ID VARCHAR(255), "
				+ "GUARDRAIL_ENGINE_ID VARCHAR(50), METHOD_NAME VARCHAR(255), PHASE VARCHAR(50), ACTION VARCHAR(50), "
				+ "ROOM_ID VARCHAR(50), INSIGHT_ID VARCHAR(50), USER_ID VARCHAR(255), USER_TYPE VARCHAR(255), "
				+ "DATE_CREATED TIMESTAMP)",
				"CREATE TABLE GUARDRAIL_LOCK (LOCK_ID VARCHAR(50), ENGINE_ID VARCHAR(50), LIMIT_ID VARCHAR(255), "
						+ "USER_ID VARCHAR(255), USER_TYPE VARCHAR(255), LOCKED_AT TIMESTAMP, LOCKED_UNTIL TIMESTAMP, "
						+ "CLEARED_AT TIMESTAMP, CLEARED_BY_ID VARCHAR(255), CLEARED_BY_TYPE VARCHAR(255))");
	}

	private static GuardrailHitStore.Hit hit(String engineId, String checkId, String roomId,
			Pair<String, String> user) {
		return new GuardrailHitStore.Hit(engineId, checkId, "guardrail-1", "askRoom", "INPUT", "BLOCK", roomId, null,
				user);
	}

	@Test
	void aRoomCountsOnlyTheNamedChecksOfItsEngine() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			createTables(db);
			GuardrailHitStore.recordHit(hit("model-1", "a", "room-1", ALICE), NOW);
			GuardrailHitStore.recordHit(hit("model-1", "b", "room-1", ALICE), NOW);
			GuardrailHitStore.recordHit(hit("model-1", "masking", "room-1", ALICE), NOW);
			GuardrailHitStore.recordHit(hit("model-1", "a", "room-2", ALICE), NOW);
			GuardrailHitStore.recordHit(hit("model-2", "a", "room-1", ALICE), NOW);

			// one query for every check, each with its own count
			GuardrailHitStore.recordHit(hit("model-1", "a", "room-1", ALICE), NOW);
			assertEquals(Map.of("a", 2, "b", 1),
					GuardrailHitStore.countRoomHitsByCheck("model-1", "room-1", Set.of("a", "b")));
			assertEquals(Map.of("a", 2), GuardrailHitStore.countRoomHitsByCheck("model-1", "room-1", Set.of("a")));
			assertEquals("NATIVE", db.value("SELECT USER_TYPE FROM GUARDRAIL_HIT WHERE CHECK_ID = 'b'"));
		}
	}

	@Test
	void aUsersHitsAreReadByLoginTypeSinceAGivenTime() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			createTables(db);
			GuardrailHitStore.recordHit(hit("model-1", "a", "room-1", ALICE), Timestamp.valueOf("2026-10-29 09:00:00"));
			GuardrailHitStore.recordHit(hit("model-1", "a", "room-2", ALICE), Timestamp.valueOf("2026-10-29 11:30:00"));
			// the same id under another login is another user
			GuardrailHitStore.recordHit(hit("model-1", "a", "room-3", ALICE_MICROSOFT), NOW);

			Timestamp anHourAgo = Timestamp.valueOf("2026-10-29 11:00:00");
			assertEquals(List.of(new GuardrailHitStore.HitTime("a", Timestamp.valueOf("2026-10-29 11:30:00"))),
					GuardrailHitStore.userHitsSince("model-1", ALICE, Set.of("a"), anHourAgo));
			assertEquals(1, GuardrailHitStore.userHitsSince("model-1", ALICE_MICROSOFT, Set.of("a"), anHourAgo).size());
		}
	}

	@Test
	void aLockIsInForceUntilItEndsOrIsCleared() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			createTables(db);
			GuardrailHitStore.insertLock(new GuardrailHitStore.Lock("model-1", "lock", "alice", "NATIVE",
					Timestamp.valueOf("2026-10-29 11:00:00"), Timestamp.valueOf("2026-10-29 13:00:00")));
			GuardrailHitStore.insertLock(new GuardrailHitStore.Lock("model-1", "lock", "bob", "NATIVE",
					Timestamp.valueOf("2026-10-29 10:00:00"), Timestamp.valueOf("2026-10-29 11:00:00")));

			assertNotNull(GuardrailHitStore.activeLock("model-1", ALICE, NOW));
			assertNull(GuardrailHitStore.activeLock("model-1", ALICE_MICROSOFT, NOW));
			assertNull(GuardrailHitStore.activeLock("model-2", ALICE, NOW));
			// bob's lock ended
			List<GuardrailHitStore.Lock> active = GuardrailHitStore.activeLocks("model-1", NOW);
			assertEquals(List.of("alice"), active.stream().map(GuardrailHitStore.Lock::userId).toList());
			assertEquals(Timestamp.valueOf("2026-10-29 11:00:00"), GuardrailHitStore.latestLockStart("model-1", ALICE));

			assertTrue(GuardrailHitStore.clearLock("model-1", ALICE, ADMIN, NOW));
			assertNull(GuardrailHitStore.activeLock("model-1", ALICE, NOW));
			assertFalse(GuardrailHitStore.clearLock("model-1", ALICE, ADMIN, NOW));
			assertEquals("admin", db.value("SELECT CLEARED_BY_ID FROM GUARDRAIL_LOCK WHERE USER_ID = 'alice'"));
			// a cleared lock still clears the count of the hits before it
			assertEquals(Timestamp.valueOf("2026-10-29 11:00:00"), GuardrailHitStore.latestLockStart("model-1", ALICE));
		}
	}

	@Test
	void aSaveEndsTheLocksOfTheUserLimitsItRemoved() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			createTables(db);
			db.registry.when(SystemEngineRegistry::isModelInferenceLogsDbLoaded).thenReturn(true);
			Timestamp until = Timestamp.valueOf("2026-10-29 13:00:00");
			GuardrailHitStore.insertLock(new GuardrailHitStore.Lock("model-1", "kept", "alice", "NATIVE",
					Timestamp.valueOf("2026-10-29 11:00:00"), until));
			GuardrailHitStore.insertLock(new GuardrailHitStore.Lock("model-1", "removed", "bob", "NATIVE",
					Timestamp.valueOf("2026-10-29 11:00:00"), until));
			GuardrailHitStore.insertLock(new GuardrailHitStore.Lock("model-2", "removed", "carol", "NATIVE",
					Timestamp.valueOf("2026-10-29 11:00:00"), until));

			List<GuardrailHitStore.Lock> released = GuardrailHitStore.clearLocksOfRemovedLimits("model-1",
					List.of("kept"), ADMIN, NOW);

			assertEquals(List.of("bob"), released.stream().map(GuardrailHitStore.Lock::userId).toList());
			assertEquals(List.of("alice"), GuardrailHitStore.activeLocks("model-1", NOW).stream()
					.map(GuardrailHitStore.Lock::userId).toList());
			// another engine's locks are its own
			assertNotNull(GuardrailHitStore.activeLock("model-2", Pair.with("carol", "NATIVE"), NOW));

			// a save with no user limits left ends every lock on the engine
			assertEquals(1, GuardrailHitStore.clearLocksOfRemovedLimits("model-1", List.of(), ADMIN, NOW).size());
			assertTrue(GuardrailHitStore.activeLocks("model-1", NOW).isEmpty());
		}
	}
}
