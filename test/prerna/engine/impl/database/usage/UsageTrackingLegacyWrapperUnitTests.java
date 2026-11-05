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
package prerna.engine.impl.database.usage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import prerna.auth.User;
import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.usage.LocalUsageCounterStore;
import prerna.engine.impl.usage.UsageCharge;
import prerna.engine.impl.usage.UsageLimitException;
import prerna.engine.impl.usage.UsageLimiter;
import prerna.engine.impl.usage.UsagePlan;
import prerna.engine.impl.usage.UsageRule;
import prerna.engine.impl.usage.UsageRule.Counts;
import prerna.engine.impl.usage.UsageRule.Metric;
import prerna.engine.impl.usage.UsageWindows.Window;
import prerna.om.ThreadStore;
import prerna.rdf.engine.wrappers.WrapperManager;

class UsageTrackingLegacyWrapperUnitTests {
	private static UsageLimiter limiter() {
		var clock = Clock.fixed(Instant.parse("2026-10-15T12:00:00Z"), ZoneOffset.UTC);
		return new UsageLimiter(new LocalUsageCounterStore(clock), clock, new UsageLimiter.History() {
			@Override
			public long used(String key) {
				return 0;
			}

			@Override
			public void record(Map<String, UsageCharge> charges) {
			}
		});
	}

	private static UsagePlan plan() {
		return new UsagePlan(
				List.of(new UsagePlan.Target("legacy", new UsageRule(Metric.ROWS, 1, Window.DAY, null, Counts.SHARED))),
				List.of(), List.of());
	}

	@Test
	void constructRowsUseTheSameQuotaAndCloseOnRefusal() throws Exception {
		var engine = mock(IDatabaseEngine.class);
		var raw = mock(IConstructWrapper.class);
		var row = mock(IConstructStatement.class);
		when(raw.next()).thenReturn(row);
		var limiter = limiter();
		try (var threads = mockStatic(ThreadStore.class); var guard = mockStatic(DatabaseUsageGuard.class)) {
			threads.when(ThreadStore::getUser).thenReturn(mock(User.class));
			guard.when(() -> DatabaseUsageGuard.external(engine)).thenReturn(true);
			guard.when(() -> DatabaseUsageGuard.admit(engine)).thenAnswer(call -> limiter.admit(plan()));
			var wrapper = UsageTrackingLegacyWrapper.create(engine, IConstructWrapper.class, () -> raw);
			assertSame(row, wrapper.next());
			assertThrows(UsageLimitException.class, wrapper::next);
			wrapper.close();
			verify(raw, times(1)).close();
		}
	}

	@Test
	void forEachRemainingCannotBypassRowAccounting() {
		var engine = mock(IDatabaseEngine.class);
		var raw = mock(ISelectWrapper.class);
		when(raw.hasNext()).thenReturn(true, true, false);
		when(raw.next()).thenReturn(mock(ISelectStatement.class));
		var limiter = limiter();
		try (var threads = mockStatic(ThreadStore.class); var guard = mockStatic(DatabaseUsageGuard.class)) {
			threads.when(ThreadStore::getUser).thenReturn(mock(User.class));
			guard.when(() -> DatabaseUsageGuard.external(engine)).thenReturn(true);
			guard.when(() -> DatabaseUsageGuard.admit(engine)).thenAnswer(call -> limiter.admit(plan()));
			var wrapper = UsageTrackingLegacyWrapper.create(engine, ISelectWrapper.class, () -> raw);
			var seen = new ArrayList<Object>();
			assertThrows(UsageLimitException.class, () -> wrapper.forEachRemaining(seen::add));
			assertEquals(1, seen.size());
		}
	}

	@Test
	void noPolicyReturnsTheOriginalLegacyType() {
		var engine = mock(IDatabaseEngine.class);
		var raw = mock(IConstructWrapper.class);
		try (var threads = mockStatic(ThreadStore.class); var guard = mockStatic(DatabaseUsageGuard.class)) {
			threads.when(ThreadStore::getUser).thenReturn(mock(User.class));
			guard.when(() -> DatabaseUsageGuard.external(engine)).thenReturn(true);
			assertSame(raw, UsageTrackingLegacyWrapper.create(engine, IConstructWrapper.class, () -> raw));
		}
	}

	@Test
	void reexecutionAdmitsAgainAndRetainsQueryChanges() throws Exception {
		var engine = mock(IDatabaseEngine.class);
		var raw = mock(ISelectWrapper.class);
		var query = new AtomicReference<>("old");
		var factories = new AtomicInteger();
		when(raw.getQuery()).thenAnswer(call -> query.get());
		doAnswer(call -> {
			query.set(call.getArgument(0));
			return null;
		}).when(raw).setQuery(anyString());
		doAnswer(call -> {
			assertEquals("new", query.get());
			return null;
		}).when(raw).execute();
		var limiter = limiter();
		try (var threads = mockStatic(ThreadStore.class); var guard = mockStatic(DatabaseUsageGuard.class)) {
			threads.when(ThreadStore::getUser).thenReturn(mock(User.class));
			guard.when(() -> DatabaseUsageGuard.external(engine)).thenReturn(true);
			guard.when(() -> DatabaseUsageGuard.admit(engine)).thenAnswer(call -> limiter.admit(plan()));
			try (var wrapper = UsageTrackingLegacyWrapper.create(engine, ISelectWrapper.class, () -> {
				factories.incrementAndGet();
				return raw;
			})) {
				wrapper.setQuery("new");
				wrapper.execute();
				verify(raw).execute();
				assertEquals(1, factories.get());
				guard.verify(() -> DatabaseUsageGuard.admit(engine), times(2));
			}
		}
	}

	@Test
	void legacyRdbmsFactoriesDoNotSwallowUsageRefusals() throws Exception {
		var engine = mock(IRDBMSEngine.class);
		when(engine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);
		when(engine.getEngineId()).thenReturn("legacy-db");
		var refusal = new UsageLimitException(Metric.QUERIES, Instant.parse("2026-10-16T00:00:00Z"));
		when(engine.execQuery(anyString())).thenThrow(refusal);
		try (var threads = mockStatic(ThreadStore.class)) {
			assertSame(refusal, assertThrows(UsageLimitException.class,
					() -> WrapperManager.getInstance().getSWrapper(engine, "SELECT 1")));
		}
	}
}
