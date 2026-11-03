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
package prerna.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.JdbcTestDatabase;
import prerna.util.Utility;

class AbstractBatchingJdbcAppenderUnitTests {

	private JdbcTestDatabase db;
	private Connection reader;
	private MockedStatic<Utility> utility;
	private TestAppender appender;

	@BeforeEach
	void createTable() throws Exception {
		this.db = new JdbcTestDatabase();
		this.db.execute("CREATE TABLE TEST_LOGS (LOG_ID VARCHAR(255), LEVEL VARCHAR(10), MESSAGE VARCHAR(20))");
		this.reader = DriverManager.getConnection(this.db.connection.getMetaData().getURL());
		this.utility = mockStatic(Utility.class);
		this.utility.when(Utility::isAuditLogsDatabaseEnabled).thenReturn(true);
	}

	@AfterEach
	void stopAppender() throws Exception {
		if (this.appender != null && this.appender.isStarted()) {
			this.appender.stop(0, TimeUnit.MILLISECONDS);
		}
		this.utility.close();
		this.reader.close();
		this.db.close();
	}

	@Test
	void appendOnlyQueuesAndNeverTouchesTheDatabase() {
		this.appender = new TestAppender(this.db.engine, 100, 50, 100, 1);

		this.appender.append(event("one"));

		assertEquals(1, this.appender.getDeliveryStatus().queuedRows());
		verifyNoInteractions(this.db.engine);
	}

	@Test
	void aQuietServersRowsAreWrittenOnceTheyHaveWaitedTheFlushInterval() throws Exception {
		this.appender = new TestAppender(this.db.engine, 100, 50, 100, 1);
		this.appender.start();

		this.appender.append(event("one"));
		this.appender.append(event("two"));

		await(() -> count() == 2);
		assertEquals(0, this.appender.getDeliveryStatus().queuedRows());
		assertEquals(2, this.appender.getDeliveryStatus().writtenRows());
	}

	@Test
	void aFailedWriteKeepsTheRowsAndRetriesThem() throws Exception {
		when(this.db.engine.getConnection()).thenThrow(new SQLException("Database is down", "08001"))
				.thenReturn(this.db.connection);
		this.appender = new TestAppender(this.db.engine, 100, 10, 100, 1);
		this.appender.start();

		this.appender.append(event("one"));
		this.appender.append(event("two"));

		await(() -> count() == 2);
		assertTrue(this.appender.getDeliveryStatus().retries() >= 1);
	}

	@Test
	void aWriteWhoseCommitDidNotReportBackIsNeverStoredTwice() throws Exception {
		// the first commit stores the rows and then reports a lost connection
		doAnswer(invocation -> {
			invocation.callRealMethod();
			throw new SQLException("Connection reset", "08006");
		}).doCallRealMethod().when(this.db.connection).commit();
		this.appender = new TestAppender(this.db.engine, 100, 10, 100, 1);
		this.appender.start();

		this.appender.append(event("one"));
		this.appender.append(event("two"));

		await(() -> this.appender.getDeliveryStatus().writtenRows() == 2);
		assertEquals(2, count());
	}

	@Test
	void aRowTheDatabaseRefusesIsDroppedWithoutBlockingTheOthers() throws Exception {
		this.appender = new TestAppender(this.db.engine, 100, 10, 100, 1);
		this.appender.start();

		this.appender.append(event("one"));
		// MESSAGE holds 20 characters, so the database refuses this row and its note
		this.appender.append(event("a message much longer than twenty characters"));
		this.appender.append(event("three"));

		await(() -> this.appender.getDeliveryStatus().queuedRows() == 0);
		assertEquals(2, count());
		assertEquals(1, this.appender.getDeliveryStatus().droppedRows());
	}

	@Test
	void aRowTheDatabaseKeepsTurningDownOutsideClass22IsDroppedWithoutBlockingTheOthers() throws Exception {
		// an insert holding a WARN row fails as Oracle reports a value too large for
		// its
		// column, with an SQLState outside class 22
		doAnswer(invocation -> {
			PreparedStatement real = (PreparedStatement) invocation.callRealMethod();
			if (!invocation.<String>getArgument(0).startsWith("INSERT")) {
				return real;
			}
			boolean[] holdsWarnRow = { false };
			return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
					new Class<?>[] { PreparedStatement.class }, (proxy, method, args) -> {
						if (method.getName().equals("setString") && "WARN".equals(args[1])) {
							holdsWarnRow[0] = true;
						}
						if (method.getName().equals("executeBatch") && holdsWarnRow[0]) {
							throw new SQLException("ORA-12899: value too large for column", "72000");
						}
						try {
							return method.invoke(real, args);
						} catch (InvocationTargetException e) {
							throw e.getTargetException();
						}
					});
		}).when(this.db.connection).prepareStatement(anyString());
		this.appender = new TestAppender(this.db.engine, 100, 10, 100, 1);
		this.appender.start();

		this.appender.append(event("one"));
		this.appender.append(event(Level.WARN, "turned down"));
		this.appender.append(event("three"));

		await(() -> this.appender.getDeliveryStatus().queuedRows() == 0);
		assertEquals(2, count());
		assertEquals(1, this.appender.getDeliveryStatus().droppedRows());
	}

	@Test
	void rowsPastTheQueueLimitAreDroppedAndCounted() {
		this.appender = new TestAppender(this.db.engine, 100, 50, 2, 1);

		this.appender.append(event("one"));
		this.appender.append(event("two"));
		this.appender.append(event("three"));

		assertEquals(2, this.appender.getDeliveryStatus().queuedRows());
		assertEquals(1, this.appender.getDeliveryStatus().droppedRows());
	}

	@Test
	void anAppenderIsHealthyUntilHalfOfItsQueueIsUsed() {
		// a database that is not loaded yet keeps every row waiting
		this.appender = new TestAppender(null, 100, 50, 4, 1);
		this.appender.start();

		this.appender.append(event("one"));
		assertTrue(this.appender.isHealthy());

		this.appender.append(event("two"));
		assertFalse(this.appender.isHealthy());
	}

	@Test
	void stoppingWithoutALoadedDatabaseDoesNotWaitForIt() {
		this.appender = new TestAppender(null, 100, 50, 100, 1);
		this.appender.start();
		this.appender.append(event("one"));

		long startedAt = System.currentTimeMillis();
		this.appender.stop(0, TimeUnit.MILLISECONDS);

		assertTrue(System.currentTimeMillis() - startedAt < 10_000);
	}

	@Test
	void stoppingWritesWhatIsStillQueued() throws Exception {
		this.appender = new TestAppender(this.db.engine, 100, 60_000, 100, 1);
		this.appender.start();

		this.appender.append(event("one"));
		this.appender.append(event("two"));
		this.appender.stop(0, TimeUnit.MILLISECONDS);

		assertEquals(2, count());
	}

	/** The committed rows, read on a connection of its own */
	private int count() {
		try (Statement statement = this.reader.createStatement();
				ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST_LOGS")) {
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	private static LogEvent event(String message) {
		return event(Level.INFO, message);
	}

	private static LogEvent event(Level level, String message) {
		return Log4jLogEvent.newBuilder().setLoggerName("test").setLevel(level).setMessage(new SimpleMessage(message))
				.setTimeMillis(System.currentTimeMillis()).build();
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 10_000;
		while (!condition.getAsBoolean()) {
			if (System.currentTimeMillis() > deadline) {
				throw new AssertionError("Timed out waiting for the appender");
			}
			Thread.sleep(20);
		}
	}

	/** Writes LEVEL and MESSAGE to TEST_LOGS */
	private static final class TestAppender extends AbstractBatchingJdbcAppender {

		private final IRDBMSEngine database;

		TestAppender(IRDBMSEngine database, int batchSize, long flushIntervalMs, int maxQueuedRows, int maxQueuedMb) {
			super("TestAppender", null, null, true, "TEST_LOGS",
					List.of(Column.text("LOG_ID", 255), Column.text("LEVEL", 10),
							Column.of("MESSAGE", ColumnType.CLOB)),
					batchSize, flushIntervalMs, maxQueuedRows, maxQueuedMb);
			this.database = database;
		}

		@Override
		protected Object[] columnValues(LogEvent event) {
			Object[] values = newValues();
			put(values, "LEVEL", event.getLevel().toString());
			put(values, "MESSAGE", event.getMessage().getFormattedMessage());
			return values;
		}

		@Override
		protected Object[] unstorableEventValues(LogEvent event, String note) {
			Object[] values = newValues();
			put(values, "MESSAGE", note);
			return values;
		}

		@Override
		protected IRDBMSEngine targetDatabase() {
			return this.database;
		}
	}
}
