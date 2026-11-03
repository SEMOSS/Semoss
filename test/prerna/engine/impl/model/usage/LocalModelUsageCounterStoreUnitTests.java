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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LocalModelUsageCounterStoreUnitTests {

	private static final Instant START = Instant.parse("2026-10-12T14:30:15Z");
	private static final long EXPIRES = Instant.parse("2026-10-14T00:00:00Z").toEpochMilli();
	private static final Duration CALL = Duration.ofMinutes(15);

	private MutableTestClock clock;
	private LocalModelUsageCounterStore store;

	@BeforeEach
	void setUp() {
		clock = new MutableTestClock(START);
		store = new LocalModelUsageCounterStore(clock);
	}

	@Test
	void missingCountersAreEmpty() {
		Map<String, ModelUsageCount> counts = store.get(List.of("a"));
		assertEquals(ModelUsageCount.EMPTY, counts.get("a"));
	}

	@Test
	void reserveHoldsOnEveryCounterAtOnce() {
		ModelUsageReservation reservation = store.reserve(List.of(new ModelUsageHold("a", 300, 1000L, 0, EXPIRES),
				new ModelUsageHold("b", 300, null, 0, EXPIRES)), CALL);
		assertNotNull(reservation);
		Map<String, ModelUsageCount> counts = store.get(List.of("a", "b"));
		assertEquals(300, counts.get("a").reserved());
		assertEquals(0, counts.get("a").used());
		assertEquals(300, counts.get("b").reserved());
	}

	@Test
	void reserveIsRefusedWhenAnyCounterWouldGoOver() {
		store.settle(null, null, List.of(new ModelUsageCharge("a", 800, EXPIRES)));
		ModelUsageReservation reservation = store.reserve(List.of(new ModelUsageHold("b", 300, 1000L, 0, EXPIRES),
				new ModelUsageHold("a", 300, 1000L, 0, EXPIRES)), CALL);
		assertNull(reservation);
		// nothing was reserved on the counter that had usage left either
		assertEquals(0, store.get(List.of("b")).get("b").reserved());
		// exactly the amount fits
		assertNotNull(store.reserve(List.of(new ModelUsageHold("a", 200, 1000L, 0, EXPIRES)), CALL));
	}

	@Test
	void reservationsOfOtherCallsCount() {
		assertNotNull(store.reserve(List.of(new ModelUsageHold("a", 600, 1000L, 0, EXPIRES)), CALL));
		assertNull(store.reserve(List.of(new ModelUsageHold("a", 500, 1000L, 0, EXPIRES)), CALL));
		assertNotNull(store.reserve(List.of(new ModelUsageHold("a", 400, 1000L, 0, EXPIRES)), CALL));
	}

	@Test
	void carriedUsageCountsTowardTheLimit() {
		// a sliding window's share of the previous bucket
		assertNull(store.reserve(List.of(new ModelUsageHold("a", 300, 1000L, 750, EXPIRES)), CALL));
		assertNotNull(store.reserve(List.of(new ModelUsageHold("a", 250, 1000L, 750, EXPIRES)), CALL));
	}

	@Test
	void settleReplacesTheReservationWithWhatWasUsed() {
		ModelUsageReservation reservation = store.reserve(List.of(new ModelUsageHold("a", 500, 1000L, 0, EXPIRES)),
				CALL);
		store.settle(reservation, null, List.of(new ModelUsageCharge("a", 120, EXPIRES)));
		ModelUsageCount count = store.get(List.of("a")).get("a");
		assertEquals(120, count.used());
		assertEquals(0, count.reserved());
		assertTrue(reservation.isEmpty());
		// settling again releases nothing more
		store.settle(reservation, null, List.of());
		assertEquals(0, store.get(List.of("a")).get("a").reserved());
	}

	@Test
	void settlePartReleasesOnlyThatPart() {
		ModelUsageReservation reservation = store.reserve(List.of(new ModelUsageHold("a", 500, 1000L, 0, EXPIRES)),
				CALL);
		store.settle(reservation, Map.of("a", 200L), List.of(new ModelUsageCharge("a", 200, EXPIRES)));
		ModelUsageCount count = store.get(List.of("a")).get("a");
		assertEquals(200, count.used());
		assertEquals(300, count.reserved());
		store.settle(reservation, null, List.of());
		assertEquals(0, store.get(List.of("a")).get("a").reserved());
	}

	@Test
	void reservationsExpireAfterTheCallTimeout() {
		ModelUsageReservation reservation = store.reserve(List.of(new ModelUsageHold("a", 500, 1000L, 0, EXPIRES)),
				CALL);
		clock.advance(Duration.ofMinutes(10));
		assertEquals(500, store.get(List.of("a")).get("a").reserved());
		clock.advance(Duration.ofMinutes(7));
		assertEquals(0, store.get(List.of("a")).get("a").reserved());
		// a call that ends after its reservation expired is still billed, and the
		// lapsed reservation is not taken off again
		store.settle(reservation, null, List.of(new ModelUsageCharge("a", 50, EXPIRES)));
		ModelUsageCount count = store.get(List.of("a")).get("a");
		assertEquals(50, count.used());
		assertEquals(0, count.reserved());
	}

	@Test
	void countersExpireWithTheirWindow() {
		long minuteBucketExpires = START.plusSeconds(120).toEpochMilli();
		store.settle(null, null, List.of(new ModelUsageCharge("minute", 900, minuteBucketExpires)));
		assertEquals(900, store.get(List.of("minute")).get("minute").used());
		clock.advance(Duration.ofMinutes(3));
		assertEquals(ModelUsageCount.EMPTY, store.get(List.of("minute")).get("minute"));
	}

	@Test
	void seedFillsACounterOnce() {
		store.settle(null, null, List.of(new ModelUsageCharge("a", 100, EXPIRES)));
		assertFalse(store.get(List.of("a")).get("a").seeded());
		// the logs may already hold the call counted here, so the larger one wins
		store.seed("a", 700, EXPIRES);
		ModelUsageCount count = store.get(List.of("a")).get("a");
		assertEquals(700, count.used());
		assertTrue(count.seeded());
		store.seed("a", 5000, EXPIRES);
		assertEquals(700, store.get(List.of("a")).get("a").used());

		store.seed("b", 50, EXPIRES);
		store.settle(null, null, List.of(new ModelUsageCharge("b", 25, EXPIRES)));
		assertEquals(75, store.get(List.of("b")).get("b").used());
	}

	@Test
	void aMarkIsMadeOnceUntilItExpires() {
		Duration day = Duration.ofDays(1);
		assertTrue(store.markOnce("batch|billed|b1.r1", day));
		assertFalse(store.markOnce("batch|billed|b1.r1", day));
		assertTrue(store.markOnce("batch|billed|b1.r2", day));
		// marks are not counters
		assertEquals(ModelUsageCount.EMPTY, store.get(List.of("batch|billed|b1.r1")).get("batch|billed|b1.r1"));
		clock.advance(Duration.ofHours(25));
		assertTrue(store.markOnce("batch|billed|b1.r1", day));
	}
}
