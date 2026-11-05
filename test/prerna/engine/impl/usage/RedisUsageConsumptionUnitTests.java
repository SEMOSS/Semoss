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
package prerna.engine.impl.usage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.UnifiedJedis;

class RedisUsageConsumptionUnitTests {
	@Test
	void knownUsageUsesOneMultiKeyScriptWithNoTemporaryReservation() {
		var client = mock(UnifiedJedis.class);
		var clock = Clock.fixed(Instant.parse("2026-10-15T12:00:00Z"), ZoneOffset.UTC);
		var store = new RedisUsageCounterStore(() -> client, clock, "{semoss-engine-usage}|");
		long expires = clock.millis() + 60_000;
		var first = new UsageHold("first", 1, 5L, 2, expires);
		assertTrue(store.consume(List.of()));
		assertThrows(IllegalArgumentException.class, () -> store.consume(List.of(first, first)));
		verifyNoInteractions(client);
		when(client.eval(anyString(), anyList(), anyList())).thenReturn(1L, 0L);
		var holds = List.of(first, new UsageHold("second", 1, null, 0, expires));
		assertTrue(store.consume(holds));
		assertFalse(store.consume(holds));
		verify(client, times(2)).eval(contains("HINCRBY"),
				eq(List.of("{semoss-engine-usage}|first", "{semoss-engine-usage}|second")),
				eq(List.of(String.valueOf(clock.millis() / 60_000), "1", "5", "2", String.valueOf(expires / 1000), "1",
						"-1", "0", String.valueOf(expires / 1000))));
	}
}
