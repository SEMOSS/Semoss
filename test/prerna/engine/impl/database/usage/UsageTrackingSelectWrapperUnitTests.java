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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
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

class UsageTrackingSelectWrapperUnitTests {
	private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-15T12:00:00Z"), ZoneOffset.UTC);

	private static UsagePlan plan() {
		return new UsagePlan(
				List.of(new UsagePlan.Target("db", new UsageRule(Metric.ROWS, 1, Window.DAY, null, Counts.SHARED))),
				List.of(), List.of());
	}

	private static UsageLimiter limiter() {
		return new UsageLimiter(new LocalUsageCounterStore(CLOCK), CLOCK, new UsageLimiter.History() {
			@Override
			public long used(String key) {
				return 0;
			}

			@Override
			public void record(Map<String, UsageCharge> charges) {
			}
		});
	}

	@Test
	void deferredReadsAdmitOnlyAtExecutionAndRowRefusalsCloseResources() throws Exception {
		var engine = mock(IDatabaseEngine.class);
		var raw = mock(IRawSelectWrapper.class);
		var row = mock(IHeadersDataRow.class);
		when(raw.next()).thenReturn(row);
		var calls = new AtomicInteger();
		var limiter = limiter();
		try (var threads = mockStatic(ThreadStore.class); var guard = mockStatic(DatabaseUsageGuard.class)) {
			threads.when(ThreadStore::getUser).thenReturn(mock(User.class));
			guard.when(() -> DatabaseUsageGuard.external(engine)).thenReturn(true);
			guard.when(() -> DatabaseUsageGuard.admit(engine)).thenAnswer(call -> {
				calls.incrementAndGet();
				return limiter.admit(plan());
			});
			var wrapper = UsageTrackingSelectWrapper.create(engine, () -> raw, true);
			assertEquals(0, calls.get());
			assertNull(wrapper.getQuery());
			assertEquals(0, calls.get());
			wrapper.execute();
			assertEquals(1, calls.get());
			assertSame(row, wrapper.next());
			assertThrows(UsageLimitException.class, wrapper::next);
			verify(raw).close();
			wrapper.close();
			verify(raw, times(1)).close();
		}
	}

	@Test
	void noPolicyReturnsTheExistingWrapperIncludingBulkFlush() throws Exception {
		var engine = mock(IDatabaseEngine.class);
		var raw = mock(IRawSelectWrapper.class);
		try (var threads = mockStatic(ThreadStore.class); var guard = mockStatic(DatabaseUsageGuard.class)) {
			threads.when(ThreadStore::getUser).thenReturn(mock(User.class));
			guard.when(() -> DatabaseUsageGuard.external(engine)).thenReturn(true);
			assertSame(raw, UsageTrackingSelectWrapper.create(engine, () -> raw, false));
		}
	}

	@Test
	void deferredFlushCannotBypassRowAccounting() throws Exception {
		var engine = mock(IDatabaseEngine.class);
		var raw = mock(IRawSelectWrapper.class);
		try (var threads = mockStatic(ThreadStore.class); var guard = mockStatic(DatabaseUsageGuard.class)) {
			threads.when(ThreadStore::getUser).thenReturn(mock(User.class));
			guard.when(() -> DatabaseUsageGuard.external(engine)).thenReturn(true);
			guard.when(() -> DatabaseUsageGuard.admit(engine)).thenReturn(limiter().admit(plan()));
			try (var wrapper = UsageTrackingSelectWrapper.create(engine, () -> raw, true)) {
				assertFalse(wrapper.flushable());
				assertThrows(UnsupportedOperationException.class, wrapper::flush);
				verify(raw, never()).flush();
			}
		}
	}

	@Test
	void changingADeferredQueryDoesNotExecuteTheOldQuery() throws Exception {
		var engine = mock(IDatabaseEngine.class);
		var raw = mock(IRawSelectWrapper.class);
		var executions = new AtomicInteger();
		try (var threads = mockStatic(ThreadStore.class); var guard = mockStatic(DatabaseUsageGuard.class)) {
			threads.when(ThreadStore::getUser).thenReturn(mock(User.class));
			guard.when(() -> DatabaseUsageGuard.external(engine)).thenReturn(true);
			try (var wrapper = UsageTrackingSelectWrapper.create(engine, () -> {
				executions.incrementAndGet();
				return raw;
			}, true)) {
				assertThrows(IllegalStateException.class, () -> wrapper.setQuery("different query"));
				assertEquals(0, executions.get());
				guard.verify(() -> DatabaseUsageGuard.admit(engine), never());
			}
		}
	}

	@Test
	void resetRefusalStillClosesThePreviousRead() throws Exception {
		var engine = mock(IDatabaseEngine.class);
		var raw = mock(IRawSelectWrapper.class);
		try (var threads = mockStatic(ThreadStore.class); var guard = mockStatic(DatabaseUsageGuard.class)) {
			threads.when(ThreadStore::getUser).thenReturn(mock(User.class));
			guard.when(() -> DatabaseUsageGuard.external(engine)).thenReturn(true);
			guard.when(() -> DatabaseUsageGuard.admit(engine)).thenReturn(limiter().admit(plan()))
					.thenThrow(UsageLimitException.unavailable(null));
			var wrapper = UsageTrackingSelectWrapper.create(engine, () -> raw, false);
			assertThrows(UsageLimitException.class, wrapper::reset);
			verify(raw).close();
			verify(raw, never()).reset();
		}
	}
}
