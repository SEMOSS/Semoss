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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.usage.LocalUsageCounterStore;
import prerna.engine.impl.usage.UsageCharge;
import prerna.engine.impl.usage.UsageLimitException;
import prerna.engine.impl.usage.UsageLimiter;
import prerna.engine.impl.usage.UsagePlan;
import prerna.engine.impl.usage.UsageRule;
import prerna.engine.impl.usage.UsageRule.Counts;
import prerna.engine.impl.usage.UsageRule.Metric;
import prerna.engine.impl.usage.UsageWindows.Window;
import prerna.util.QueryExecutionUtility;

class JdbcReadUsageUnitTests {
	private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-15T12:00:00Z"), ZoneOffset.UTC);

	private static class History implements UsageLimiter.History {
		long total;
		int writes;

		@Override
		public long used(String key) {
			return 0;
		}

		@Override
		public void record(Map<String, UsageCharge> charges) {
			writes++;
			total += charges.values().stream().mapToLong(UsageCharge::amount).sum();
		}
	}

	private static UsagePlan plan(Metric metric, long amount) {
		return new UsagePlan(
				List.of(new UsagePlan.Target("db", new UsageRule(metric, amount, Window.DAY, null, Counts.SHARED))),
				List.of(), List.of());
	}

	private static Connection open() throws SQLException {
		return DriverManager.getConnection("jdbc:h2:mem:" + UUID.randomUUID());
	}

	@Test
	void rowQuotaThrowsRatherThanSilentlyTruncatingAndClosesResults() throws Exception {
		var history = new History();
		var limiter = new UsageLimiter(new LocalUsageCounterStore(CLOCK), CLOCK, history);
		try (var raw = open();
				var con = JdbcReadUsage.wrap(raw, () -> limiter.admit(plan(Metric.ROWS, 2)));
				var statement = con.createStatement()) {
			var rows = statement.executeQuery("SELECT X FROM SYSTEM_RANGE(1,3)");
			assertSame(statement, rows.getStatement());
			assertSame(con, statement.getConnection());
			assertTrue(rows.next());
			assertTrue(rows.next());
			assertEquals(Metric.ROWS, assertThrows(UsageLimitException.class, rows::next).getMetric());
			assertTrue(rows.isClosed());
			assertEquals(2, history.total);
			assertEquals(1, history.writes);
		}
	}

	@Test
	void preparationIsDeferredAndStatementReexecutionAdmitsAgain() throws Exception {
		var admissions = new AtomicInteger();
		var history = new History();
		var limiter = new UsageLimiter(new LocalUsageCounterStore(CLOCK), CLOCK, history);
		try (var raw = open(); var con = JdbcReadUsage.wrap(raw, () -> {
			admissions.incrementAndGet();
			var ticket = limiter.admit(plan(Metric.QUERIES, 2));
			ticket.consume(Metric.QUERIES, 1);
			return ticket;
		}); var statement = con.prepareStatement("SELECT ?")) {
			statement.setInt(1, 1);
			assertEquals(0, admissions.get());
			try (var rows = statement.executeQuery()) {
				assertTrue(rows.next());
			}
			statement.setInt(1, 2);
			try (var rows = statement.executeQuery()) {
				assertTrue(rows.next());
			}
			assertThrows(UsageLimitException.class, statement::executeQuery);
			assertEquals(2, history.total);
		}
	}

	@Test
	void writesRemainUnmeteredAndTransactionsStayCallerOwned() throws Exception {
		var admissions = new AtomicInteger();
		try (var raw = open(); var con = JdbcReadUsage.wrap(raw, () -> {
			admissions.incrementAndGet();
			throw new AssertionError("Write was metered");
		}); var statement = con.createStatement()) {
			con.setAutoCommit(false);
			statement.execute("CREATE TABLE T (ID INT)");
			statement.executeUpdate("INSERT INTO T VALUES (1)");
			try (var prepared = con.prepareStatement("INSERT INTO T VALUES (?)")) {
				prepared.setInt(1, 2);
				prepared.executeUpdate();
			}
			con.rollback();
			assertFalse(con.getAutoCommit());
			assertEquals(0, admissions.get());
		}
	}

	@Test
	void executeGetResultSetAndImplicitStatementCloseSettleOnce() throws Exception {
		var history = new History();
		var limiter = new UsageLimiter(new LocalUsageCounterStore(CLOCK), CLOCK, history);
		try (var raw = open(); var con = JdbcReadUsage.wrap(raw, () -> limiter.admit(plan(Metric.ROWS, 10)))) {
			var statement = con.createStatement();
			assertTrue(statement.execute("SELECT 1"));
			var rows = statement.getResultSet();
			assertSame(rows, statement.getResultSet());
			assertTrue(rows.next());
			statement.close();
			statement.close();
			assertEquals(1, history.total);
			assertEquals(1, history.writes);
		}
	}

	@Test
	void connectionCloseSettlesAnAbandonedReadWithoutChangingAutoCommit() throws Exception {
		var history = new History();
		var limiter = new UsageLimiter(new LocalUsageCounterStore(CLOCK), CLOCK, history);
		try (var raw = open()) {
			raw.setAutoCommit(false);
			var con = JdbcReadUsage.wrap(raw, () -> limiter.admit(plan(Metric.ROWS, 10)));
			var statement = con.createStatement();
			var rows = statement.executeQuery("SELECT 1");
			assertTrue(rows.next());
			assertFalse(raw.getAutoCommit());
			con.close();
			assertEquals(1, history.total);
		}
	}

	@Test
	void failedQueryCountsItsAttemptAndPreservesTheSqlException() throws Exception {
		var history = new History();
		var limiter = new UsageLimiter(new LocalUsageCounterStore(CLOCK), CLOCK, history);
		try (var raw = open(); var con = JdbcReadUsage.wrap(raw, () -> {
			var ticket = limiter.admit(plan(Metric.QUERIES, 10));
			ticket.consume(Metric.QUERIES, 1);
			return ticket;
		}); var statement = con.createStatement()) {
			assertThrows(SQLException.class, () -> statement.executeQuery("SELECT * FROM NO_SUCH_TABLE"));
			assertEquals(1, history.total);
			assertEquals(1, history.writes);
		}
	}

	@Test
	void noTicketPreservesTheRawResultSetAndNormalBehavior() throws Exception {
		try (var raw = open();
				var con = JdbcReadUsage.wrap(raw, () -> null);
				var statement = con.createStatement();
				var rows = statement.executeQuery("SELECT 1")) {
			assertTrue(rows.next());
			assertEquals(1, rows.getInt(1));
			assertFalse(rows.next());
		}
	}

	@Test
	void nonPooledTransactionsKeepTheSharedPhysicalConnectionMonitor() throws Exception {
		var physical = Mockito.mock(Connection.class);
		Mockito.when(physical.getAutoCommit()).thenReturn(true);
		var engine = Mockito.mock(IRDBMSEngine.class);
		var first = JdbcReadUsage.wrap(physical, () -> null);
		var second = JdbcReadUsage.wrap(physical, () -> null);
		assertNotSame(first, second);
		assertSame(physical, JdbcReadUsage.transactionIdentity(first));
		assertSame(physical, JdbcReadUsage.transactionIdentity(second));
		for (var connection : List.of(first, second, physical)) {
			Mockito.when(engine.getConnection()).thenReturn(connection);
			QueryExecutionUtility.write(engine, ignored -> {
				assertTrue(Thread.holdsLock(physical));
				return null;
			});
		}
	}

	@Test
	void elapsedTimeExcludesIdleAndRoundsOnlyOnceAtSettlement() {
		var history = new History();
		var limiter = new UsageLimiter(new LocalUsageCounterStore(CLOCK), CLOCK, history);
		var session = new ReadUsageSession(limiter.admit(plan(Metric.COMPUTE_TIME, 10)));
		session.elapsed(500_000);
		session.elapsed(500_001);
		session.close();
		session.close();
		assertEquals(2, history.total);
	}
}
