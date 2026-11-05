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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.javatuples.Pair;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import prerna.auth.User;
import prerna.auth.utils.EngineUsagePolicyResolver;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.usage.UsagePlan.Candidate;
import prerna.engine.impl.usage.UsagePlan.Target;
import prerna.engine.impl.usage.UsageRule.Counts;
import prerna.engine.impl.usage.UsageRule.Metric;
import prerna.engine.impl.usage.UsageWindows.Window;

class UsageLimiterUnitTests {
	private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-15T12:00:00Z"), ZoneOffset.UTC);
	private static final UsageLimitKey KEY = new UsageLimitKey(CATALOG_TYPE.DATABASE, UsageLimitKey.Source.ENGINE_TOTAL,
			"db", null, null);

	static class History implements UsageLimiter.History {
		final Map<String, Long> used = new HashMap<>();
		int writes;

		@Override
		public long used(String key) {
			return used.getOrDefault(key, 0L);
		}

		@Override
		public synchronized void record(Map<String, UsageCharge> charges) {
			writes++;
			charges.forEach((key, c) -> used.merge(key, c.amount(), Long::sum));
		}

		long total() {
			return used.values().stream().mapToLong(Long::longValue).sum();
		}
	}

	static Target target(String scope, Metric metric, long amount) {
		return new Target(scope, new UsageRule(metric, amount, Window.DAY, null, Counts.SHARED));
	}

	static UsagePlan plan(Target... targets) {
		return new UsagePlan(List.of(targets), List.of(), List.of());
	}

	static UsageLimiter limiter(History history) {
		return new UsageLimiter(new LocalUsageCounterStore(CLOCK), CLOCK, history);
	}

	@Test
	void rowsAreSharedAcrossQueriesAndSettlementIsOnce() {
		var history = new History();
		var limiter = limiter(history);
		var plan = plan(target("engine", Metric.ROWS, 2));
		var first = limiter.admit(plan);
		var second = limiter.admit(plan);
		first.consume(Metric.ROWS, 1);
		second.consume(Metric.ROWS, 1);
		assertEquals(Metric.ROWS,
				assertThrows(UsageLimitException.class, () -> first.consume(Metric.ROWS, 1)).getMetric());
		first.close();
		second.close();
		first.close();
		assertEquals(2, history.total());
		assertEquals(2, history.writes);
		assertThrows(UsageLimitException.class, () -> limiter.admit(plan));
	}

	@Test
	void aRejectedMultiCounterHoldChargesNothing() {
		var history = new History();
		var limiter = limiter(history);
		var ticket = limiter.admit(plan(target("tight", Metric.ROWS, 1), target("wide", Metric.ROWS, 100)));
		assertThrows(UsageLimitException.class, () -> ticket.consume(Metric.ROWS, 2));
		ticket.consume(Metric.ROWS, 1);
		ticket.close();
		assertEquals(2, history.total());
	}

	@Test
	void elapsedTimeIsBilledAfterWorkAndCheckedOnTheNextCall() {
		var history = new History();
		var limiter = limiter(history);
		var plan = plan(target("engine", Metric.COMPUTE_TIME, 5));
		var ticket = limiter.admit(plan);
		ticket.record(Metric.COMPUTE_TIME, 8);
		ticket.close();
		assertEquals(8, history.total());
		assertThrows(UsageLimitException.class, () -> limiter.admit(plan));
	}

	@Test
	void aNewLocalStoreRecoversCompletedUsageFromHistory() {
		var history = new History();
		var plan = plan(target("engine", Metric.ROWS, 1));
		try (var ticket = limiter(history).admit(plan)) {
			ticket.consume(Metric.ROWS, 1);
		}
		assertThrows(UsageLimitException.class, () -> limiter(history).admit(plan));
	}

	@Test
	void teamsFallBackButAnEngineCeilingAlwaysApplies() {
		var history = new History();
		var limiter = limiter(history);
		var team1 = new UsagePlan.Candidate(KEY, List.of(target("team1", Metric.ROWS, 1)));
		var team2 = new UsagePlan.Candidate(
				new UsageLimitKey(CATALOG_TYPE.DATABASE, UsageLimitKey.Source.TEAM, "db", "team2", "CUSTOM"),
				List.of(target("team2", Metric.ROWS, 2)));
		var plan = new UsagePlan(List.of(target("engine", Metric.ROWS, 2)), List.of(team1, team2), List.of());
		try (var ticket = limiter.admit(plan)) {
			assertEquals(KEY, ticket.billedTo());
			ticket.consume(Metric.ROWS, 1);
		}
		try (var ticket = limiter.admit(plan)) {
			assertEquals(team2.key(), ticket.billedTo());
			ticket.consume(Metric.ROWS, 1);
		}
		assertThrows(UsageLimitException.class, () -> limiter.admit(plan));
	}

	@Test
	void unlimitedDoesNotLiftCeilingsAndTrackingDoesNotBecomeACeiling() {
		var history = new History();
		var limiter = limiter(history);
		var plan = new UsagePlan(List.of(target("ceiling", Metric.ROWS, 2)), List.of(new Candidate(KEY, List.of())),
				List.of(target("personal", Metric.ROWS, 1)));
		try (var ticket = limiter.admit(plan)) {
			ticket.consume(Metric.ROWS, 2);
		}
		assertThrows(UsageLimitException.class, () -> limiter.admit(plan));
		assertEquals(4, history.total());
	}

	@Test
	void duplicateScopesAreChargedOnceAtTheTightestBound() {
		var history = new History();
		var limiter = limiter(history);
		try (var ticket = limiter.admit(plan(target("same", Metric.ROWS, 10), target("same", Metric.ROWS, 1)))) {
			ticket.consume(Metric.ROWS, 1);
			assertThrows(UsageLimitException.class, () -> ticket.consume(Metric.ROWS, 1));
		}
		assertEquals(1, history.total());
	}

	@Test
	void noPoliciesNeverTouchTheCounterStoreOrHistory() {
		var store = Mockito.mock(IUsageCounterStore.class);
		var history = Mockito.mock(UsageLimiter.History.class);
		try (var ticket = new UsageLimiter(store, CLOCK, history).admit(plan())) {
			ticket.consume(Metric.ROWS, 1);
		}
		Mockito.verifyNoInteractions(store, history);
	}

	@Test
	void storeAndHistoryFailuresFailClosed() {
		var store = Mockito.mock(IUsageCounterStore.class);
		Mockito.when(store.get(ArgumentMatchers.anyCollection()))
				.thenThrow(new UsageStoreException("offline", null));
		var failure = assertThrows(UsageLimitException.class,
				() -> new UsageLimiter(store, CLOCK, new History()).admit(plan(target("engine", Metric.ROWS, 1))));
		assertTrue(failure.isUnavailable());
	}

	@Test
	void lastRowCannotBeSpentConcurrently() throws Exception {
		var history = new History();
		var limiter = limiter(history);
		var plan = plan(target("engine", Metric.ROWS, 1));
		var tickets = new ArrayList<UsageLimiter.Ticket>();
		for (int i = 0; i < 32; i++)
			tickets.add(limiter.admit(plan));
		var start = new CountDownLatch(1);
		var accepted = new AtomicInteger();
		try (var workers = Executors.newFixedThreadPool(8)) {
			var futures = new ArrayList<Future<?>>();
			for (var ticket : tickets)
				futures.add(workers.submit(() -> {
					try {
						start.await();
						ticket.consume(Metric.ROWS, 1);
						accepted.incrementAndGet();
					} catch (UsageLimitException expected) {
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new RuntimeException(e);
					} finally {
						ticket.close();
					}
				}));
			start.countDown();
			for (var future : futures)
				future.get();
		}
		assertEquals(1, accepted.get());
		assertEquals(1, history.total());
	}

	@Test
	void namespaceIncludesCatalogAndIdentityWithoutDelimiterCollisions() {
		var a = new UsageLimitKey(CATALOG_TYPE.DATABASE, UsageLimitKey.Source.DIRECT, "a|b", "c", "NATIVE");
		var b = new UsageLimitKey(CATALOG_TYPE.DATABASE, UsageLimitKey.Source.DIRECT, "a", "b|c", "NATIVE");
		var model = new UsageLimitKey(CATALOG_TYPE.MODEL, UsageLimitKey.Source.DIRECT, "a|b", "c", "NATIVE");
		assertNotEquals(a.id(), b.id());
		assertNotEquals(a.id(), model.id());
	}

	@Test
	void anUncappedFirstSourceDoesNotRequireACounterStore() {
		var user = Mockito.mock(User.class);
		var engine = Mockito.mock(IEngine.class);
		String id = "unlimited-" + UUID.randomUUID();
		Mockito.when(engine.getEngineId()).thenReturn(id);
		Mockito.when(engine.getCatalogType()).thenReturn(CATALOG_TYPE.DATABASE);
		var key = new UsageLimitKey(CATALOG_TYPE.DATABASE, UsageLimitKey.Source.TEAM, id, "unlimited", "CUSTOM");
		var plan = new UsagePlan(List.of(),
				List.of(new Candidate(key, List.of()), new Candidate(
						new UsageLimitKey(CATALOG_TYPE.DATABASE, UsageLimitKey.Source.TEAM, id, "bounded", "CUSTOM"),
						List.of(target("unused", Metric.ROWS, 1)))),
				List.of());
		try (var identities = Mockito.mockStatic(User.class);
				var resolver = Mockito.mockStatic(EngineUsagePolicyResolver.class)) {
			identities.when(() -> User.getPrimaryUserIdAndTypePair(user))
					.thenReturn(Pair.with("u", "NATIVE"));
			resolver.when(() -> EngineUsagePolicyResolver.resolve(user, CATALOG_TYPE.DATABASE, id))
					.thenReturn(plan);
			assertNull(EngineUsageGuard.admit(user, engine, Metric.QUERIES, 1));
		}
	}

	@Test
	void knownQuantitiesUseAtomicConsumptionWithoutAnExpiringHold() {
		var store = Mockito.spy(new LocalUsageCounterStore(CLOCK));
		var history = new History();
		try (var ticket = new UsageLimiter(store, CLOCK, history).admit(plan(target("db", Metric.ROWS, 1)))) {
			ticket.consume(Metric.ROWS, 1);
		}
		Mockito.verify(store).consume(ArgumentMatchers.anyList());
		Mockito.verify(store, Mockito.never()).reserve(ArgumentMatchers.anyList(),
				ArgumentMatchers.any());
		Mockito.verify(store, Mockito.never()).settle(ArgumentMatchers.any(),
				ArgumentMatchers.any(), ArgumentMatchers.any());
	}

	@Test
	void directConsumptionIsAllOrNoneAndRejectsDuplicateKeys() {
		var store = new LocalUsageCounterStore(CLOCK);
		long expires = CLOCK.millis() + 86_400_000;
		var first = new UsageHold("first", 1, 1L, 0, expires);
		assertThrows(IllegalArgumentException.class, () -> store.consume(List.of(first, first)));
		assertFalse(store.consume(
				List.of(new UsageHold("first", 2, 1L, 0, expires), new UsageHold("second", 1, 10L, 0, expires))));
		assertEquals(0, store.get(List.of("first", "second")).values().stream().mapToLong(UsageCount::total).sum());
		assertTrue(store.consume(List.of(first, new UsageHold("second", 1, 10L, 0, expires))));
		assertEquals(1, store.get(List.of("first")).get("first").used());
		assertEquals(0, store.get(List.of("first")).get("first").reserved());
	}

	@Test
	void durableKeysUseAFixedCompactEncoding() {
		assertEquals("3a36f611fa1bb66698395330a063a76df626cc5af71bae7fd1f17ebb316029f3", KEY.id());
	}

	@Test
	void invalidRulesAreRejected() {
		assertThrows(IllegalArgumentException.class,
				() -> new UsageRule(Metric.ROWS, 0, Window.DAY, null, Counts.SHARED).validate());
		assertThrows(IllegalArgumentException.class,
				() -> new UsageRule(Metric.ROWS, 1, Window.MINUTE, 60, Counts.SHARED).validate());
		assertThrows(IllegalArgumentException.class,
				() -> new UsageLimit(true, List.of(target("s", Metric.ROWS, 1).rule())).validate());
	}
}
