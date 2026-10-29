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
package prerna.engine.impl.pipeline;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.sql.Timestamp;
import java.util.List;
import java.util.Set;

import org.javatuples.Pair;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.engine.api.IEngine;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.inferencetracking.GuardrailHitStore;
import prerna.engine.impl.model.inferencetracking.GuardrailHitStoreJdbcUnitTests;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.usertracking.GuardrailAudit;
import prerna.util.JdbcTestDatabase;
import prerna.util.Utility;

class GuardrailHitLimitsUnitTests {

	private static final Pair<String, String> ALICE = Pair.with("alice", "NATIVE");
	private static final GuardrailAudit.Call CALL = new GuardrailAudit.Call("model-1", "Assistant",
			IEngine.CATALOG_TYPE.MODEL, "askRoom", "room-1", false);

	private static GuardrailHitLimit roomLimit(int hits, String... checks) {
		return new GuardrailHitLimit("close", Set.of(checks), hits, GuardrailHitLimit.Scope.ROOM, 0, 0, null);
	}

	private static GuardrailHitLimit userLimit(int hits, String... checks) {
		return new GuardrailHitLimit("lock", Set.of(checks), hits, GuardrailHitLimit.Scope.USER, 60, 30, null);
	}

	/**
	 * The statics a limit touches beside its store: room closing, Redis and the
	 * audit trail
	 */
	private record Statics(MockedStatic<ModelInferenceLogsUtils> rooms, MockedStatic<RoomMessageStore> redis,
			MockedStatic<GuardrailAudit> audit) implements AutoCloseable {

		static Statics open() {
			Statics statics = new Statics(mockStatic(ModelInferenceLogsUtils.class), mockStatic(RoomMessageStore.class),
					mockStatic(GuardrailAudit.class));
			statics.rooms().when(() -> ModelInferenceLogsUtils.doSetRoomToInactive(anyString(), anyString()))
					.thenReturn(true);
			return statics;
		}

		@Override
		public void close() {
			this.rooms.close();
			this.redis.close();
			this.audit.close();
		}
	}

	@Test
	void theHitThatReachesARoomLimitClosesTheRoomAndRefusesTheCall() throws Exception {
		try (var db = new JdbcTestDatabase(); var statics = Statics.open()) {
			GuardrailHitStoreJdbcUnitTests.createTables(db);
			GuardrailHitLimits limits = new GuardrailHitLimits(List.of(roomLimit(2, "toxicity", "injection")));

			assertDoesNotThrow(() -> limits.countHit(CALL, null, ALICE, "toxicity", "g-1", "INPUT", "BLOCK"));
			statics.rooms().verify(() -> ModelInferenceLogsUtils.doSetRoomToInactive(anyString(), anyString()),
					never());

			SemossPixelException refused = assertThrows(SemossPixelException.class,
					() -> limits.countHit(CALL, null, ALICE, "injection", "g-2", "OUTPUT", "BLOCK"));
			assertEquals(GuardrailHitLimits.ROOM_LIMIT_MESSAGE, refused.getMessage());
			statics.rooms().verify(() -> ModelInferenceLogsUtils.doSetRoomToInactive("alice", "room-1"));
			statics.redis().verify(() -> RoomMessageStore.evictRedisProjection("room-1"));
			statics.audit().verify(() -> GuardrailAudit.recordRoomClose(null, CALL, "close", 2, 2));
		}
	}

	@Test
	void aHitThatReachesARoomLimitAndAUserLimitAtOnceDoesBoth() throws Exception {
		try (var db = new JdbcTestDatabase(); var statics = Statics.open()) {
			GuardrailHitStoreJdbcUnitTests.createTables(db);
			GuardrailHitLimits limits = new GuardrailHitLimits(
					List.of(roomLimit(1, "toxicity"), userLimit(1, "toxicity")));

			SemossPixelException refused = assertThrows(SemossPixelException.class,
					() -> limits.countHit(CALL, null, ALICE, "toxicity", "g-1", "INPUT", "BLOCK"));

			// the refusal says when the lock ends, and the room is closed too
			assertTrue(refused.getMessage().startsWith("You can't use Assistant until "), refused.getMessage());
			assertEquals(1, db.count("GUARDRAIL_LOCK"));
			statics.rooms().verify(() -> ModelInferenceLogsUtils.doSetRoomToInactive("alice", "room-1"));
			statics.audit().verify(() -> GuardrailAudit.recordRoomClose(null, CALL, "close", 1, 1));
			statics.audit().verify(() -> GuardrailAudit.recordUserLock(eq(null), eq(CALL), eq("lock"), eq(1), eq(1),
					eq(60), eq(30), any(Timestamp.class)));
		}
	}

	@Test
	void aCheckNoLimitNamesIsNotCounted() throws Exception {
		try (var db = new JdbcTestDatabase(); var statics = Statics.open()) {
			GuardrailHitStoreJdbcUnitTests.createTables(db);
			GuardrailHitLimits limits = new GuardrailHitLimits(List.of(roomLimit(1, "toxicity")));

			assertDoesNotThrow(() -> limits.countHit(CALL, null, ALICE, "masking", "g-3", "INPUT", "MASK"));
			assertEquals(0, db.count("GUARDRAIL_HIT"));
		}
	}

	@Test
	void reachingAUserLimitLocksTheUserOutAndTheLockRefusesTheNextCallBeforeAnyCheck() throws Exception {
		try (var db = new JdbcTestDatabase(); var statics = Statics.open()) {
			GuardrailHitStoreJdbcUnitTests.createTables(db);
			GuardrailHitLimits limits = new GuardrailHitLimits(List.of(userLimit(2, "toxicity")));

			assertDoesNotThrow(() -> limits.countHit(CALL, null, ALICE, "toxicity", "g-1", "INPUT", "BLOCK"));
			SemossPixelException locked = assertThrows(SemossPixelException.class,
					() -> limits.countHit(CALL, null, ALICE, "toxicity", "g-1", "INPUT", "MASK"));
			assertTrue(locked.getMessage().startsWith("You can't use Assistant until "), locked.getMessage());
			assertEquals(1, db.count("GUARDRAIL_LOCK"));
			statics.rooms().verify(() -> ModelInferenceLogsUtils.doSetRoomToInactive("alice", "room-1"));
			statics.audit().verify(() -> GuardrailAudit.recordUserLock(eq(null), eq(CALL), eq("lock"), eq(2), eq(2),
					eq(60), eq(30), any(Timestamp.class)));

			GuardrailAudit.Call nextRoom = new GuardrailAudit.Call("model-1", "Assistant", IEngine.CATALOG_TYPE.MODEL,
					"askRoom", "room-2", false);
			assertThrows(SemossPixelException.class, () -> limits.refuseIfLockedOrRoomClosed(nextRoom, null, ALICE));
			statics.rooms().verify(() -> ModelInferenceLogsUtils.doSetRoomToInactive("alice", "room-2"));
			statics.audit().verify(
					() -> GuardrailAudit.recordLockedRequest(eq(null), eq(nextRoom), eq("lock"), any(Timestamp.class)));
			// the same id under another login is another user
			assertDoesNotThrow(
					() -> limits.refuseIfLockedOrRoomClosed(nextRoom, null, Pair.with("alice", "MICROSOFT")));
		}
	}

	@Test
	void aLockClearsTheCountOfTheHitsBeforeIt() throws Exception {
		try (var db = new JdbcTestDatabase(); var statics = Statics.open()) {
			GuardrailHitStoreJdbcUnitTests.createTables(db);
			long now = Utility.getCurrentSqlTimestampUTC().getTime();
			Timestamp tenMinutesAgo = new Timestamp(now - 10 * 60_000L);
			GuardrailHitStore.recordHit(new GuardrailHitStore.Hit("model-1", "toxicity", "g-1", "askRoom", "INPUT",
					"BLOCK", "room-1", null, ALICE), tenMinutesAgo);
			// the lock those hits reached has ended
			GuardrailHitStore.insertLock(new GuardrailHitStore.Lock("model-1", "lock", "alice", "NATIVE", tenMinutesAgo,
					new Timestamp(now - 60_000L)));
			GuardrailHitLimits limits = new GuardrailHitLimits(List.of(userLimit(2, "toxicity")));

			// counted with the hit before the lock this would reach the limit
			assertDoesNotThrow(() -> limits.countHit(CALL, null, ALICE, "toxicity", "g-1", "INPUT", "BLOCK"));
			assertEquals(1, db.count("GUARDRAIL_LOCK"));
		}
	}

	@Test
	void aClosedRoomOrARoomAtItsLimitIsRefusedBeforeAnyCheck() throws Exception {
		try (var db = new JdbcTestDatabase(); var statics = Statics.open()) {
			GuardrailHitStoreJdbcUnitTests.createTables(db);
			GuardrailHitLimits limits = new GuardrailHitLimits(List.of(roomLimit(1, "toxicity")));
			statics.rooms().when(() -> ModelInferenceLogsUtils.isRoomInActive("alice", "room-1")).thenReturn(true);

			SemossPixelException closed = assertThrows(SemossPixelException.class,
					() -> limits.refuseIfLockedOrRoomClosed(CALL, null, ALICE));
			assertEquals(GuardrailHitLimits.CLOSED_ROOM_MESSAGE, closed.getMessage());

			// closing a room that reached its limit can fail, so its count refuses it too
			statics.rooms().when(() -> ModelInferenceLogsUtils.isRoomInActive("alice", "room-1")).thenReturn(false);
			GuardrailHitStore.recordHit(new GuardrailHitStore.Hit("model-1", "toxicity", "g-1", "askRoom", "INPUT",
					"BLOCK", "room-1", null, ALICE), Utility.getCurrentSqlTimestampUTC());
			assertThrows(SemossPixelException.class, () -> limits.refuseIfLockedOrRoomClosed(CALL, null, ALICE));
		}
	}

	@Test
	void aHitThatCannotBeStoredRefusesTheCall() throws Exception {
		try (var db = new JdbcTestDatabase(); var statics = Statics.open()) {
			// no GUARDRAIL_HIT table
			GuardrailHitLimits limits = new GuardrailHitLimits(List.of(roomLimit(5, "toxicity")));

			SemossPixelException refused = assertThrows(SemossPixelException.class,
					() -> limits.countHit(CALL, null, ALICE, "toxicity", "g-1", "INPUT", "MASK"));
			assertEquals(GuardrailHitLimits.UNCHECKED_MESSAGE, refused.getMessage());
			statics.audit().verify(() -> GuardrailAudit.recordRoomClose(any(), any(), anyString(), anyInt(), anyInt()),
					never());
		}
	}
}
