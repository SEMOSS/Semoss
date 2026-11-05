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
package prerna.engine.impl.model.usage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import prerna.engine.impl.usage.RedisUsageCounterStore;
import prerna.engine.impl.usage.UsageStoreException;
import redis.clients.jedis.UnifiedJedis;

class ModelUsageCompatibilityUnitTests {
	@Test
	void legacyNullWindowShortCircuitsArePreserved() {
		assertFalse(ModelUsageWindows.isSliding(null));
		assertEquals(0, ModelUsageWindows.previousWeight(null, null));
		assertEquals(0, ModelUsageWindows.carried(null, null, 0));
		assertNull(ModelUsageWindows.iso(null));
	}

	@Test
	void modelRedisNamespaceRemainsUnchangedAndSeparateFromNewCatalogs() {
		var modelClient = mock(UnifiedJedis.class);
		var engineClient = mock(UnifiedJedis.class);
		var clock = Clock.fixed(Instant.parse("2026-10-15T12:00:00Z"), ZoneOffset.UTC);
		new RedisModelUsageCounterStore(() -> modelClient, clock).seed("mul|existing", 4, clock.millis() + 60_000);
		new RedisUsageCounterStore(() -> engineClient, clock, "{semoss-engine-usage}|").seed("eul|new", 4,
				clock.millis() + 60_000);
		verify(modelClient).eval(anyString(), eq(List.of("{semoss-model-usage}|mul|existing")), anyList());
		verify(engineClient).eval(anyString(), eq(List.of("{semoss-engine-usage}|eul|new")), anyList());
	}

	@Test
	void counterFailuresRetainTheLegacyModelExceptionType() {
		var client = mock(UnifiedJedis.class);
		when(client.eval(anyString(), anyList(), anyList())).thenThrow(new IllegalStateException("offline"));
		var store = new RedisModelUsageCounterStore(() -> client, Clock.systemUTC());
		assertInstanceOf(UsageStoreException.class,
				assertThrows(ModelUsageStoreException.class, () -> store.get(List.of("mul|existing"))).getCause());
	}

	@Test
	void reservationPartialReleaseStillCannotReleaseMoreThanWasHeld() {
		var reservation = new ModelUsageReservation(10, Map.of("counter", 5L));
		assertEquals(3, reservation.take("counter", 3));
		assertEquals(2, reservation.take("counter", 20));
		assertTrue(reservation.takeAll().isEmpty());
		assertTrue(reservation.isEmpty());
	}
}
