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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Base for appenders that store log events as rows of a database table without
 * making logging wait on the database. {@link #append(LogEvent)} only turns the
 * event into a row and adds it to a bounded queue. One writer thread per
 * appender inserts the rows in batches and removes them from the queue only
 * after they are committed.
 *
 * <ul>
 * <li>A row is written once it has waited {@code flushIntervalMs}, however busy
 * or quiet the server is, or sooner when {@code batchSize} rows are queued.
 * {@code batchSize} is the most rows in one insert, never a count to wait
 * for.</li>
 * <li>A failed write is retried with backoff, from 1 second up to 1 minute.
 * Rows wait while the database is not loaded yet. A batch the database turns
 * down 3 times in a row for a reason that can come from one row is written one
 * row at a time, as a refused batch is.</li>
 * <li>Every row gets its {@code LOG_ID} when it is queued. When a write fails
 * after its inserts ran, so the rows may be stored, the next attempt first
 * looks up which {@code LOG_ID}s are stored and inserts only the rest.</li>
 * <li>A row the database refuses is written alone with its long text replaced
 * by a note, and dropped only when that is refused too.</li>
 * <li>The queue is bounded by row count and by text size. A row past either
 * limit is dropped and counted.</li>
 * <li>Stopping drains the queue for up to 30 seconds, or the stop's own timeout
 * when it is shorter, unless the database never loaded.</li>
 * </ul>
 *
 * <p>
 * Problems are reported through log4j's status logger ({@link #LOGGER}), never
 * through a logger that could route back into these appenders.
 * </p>
 */
public abstract class AbstractBatchingJdbcAppender extends AbstractAppender {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	/** The most rows in one insert when the configuration does not say */
	protected static final int DEFAULT_BATCH_SIZE = 100;
	/**
	 * How long a row waits before it is written when the configuration does not say
	 */
	protected static final long DEFAULT_FLUSH_INTERVAL_MS = 1_000;

	private static final long FIRST_RETRY_MS = 1_000;
	private static final long MAX_RETRY_MS = 60_000;
	private static final long STOPPING_RETRY_MS = 1_000;
	private static final long DRAIN_TIMEOUT_MS = 30_000;
	private static final long STALE_ROW_MS = TimeUnit.MINUTES.toMillis(5);
	private static final long UNHEALTHY_AFTER_DROP_MS = TimeUnit.MINUTES.toMillis(5);
	private static final long REPORT_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
	private static final long BYTES_PER_CHAR = 2;
	private static final long BYTES_PER_MB = 1024L * 1024L;
	private static final int MAX_CHAINED_ERRORS = 10;
	/**
	 * How many times in a row the database may turn down the same batch before its
	 * rows are written one at a time, so one row it never takes cannot hold back
	 * the rest
	 */
	private static final int MAX_ANSWERED_FAILURES = 3;
	/** The most LOG_IDs looked up in one query, under Oracle's limit of 1000 */
	private static final int MAX_LOOKUP_IDS = 500;
	/**
	 * The SQLState classes of failures that do not come from one row's values, so
	 * they hold back every row alike: connection, transaction rollback (such as a
	 * deadlock), syntax or access rule (such as a missing table), feature not
	 * supported, authorization, catalog or schema name, transaction state,
	 * insufficient resources, operator intervention, system error, and driver
	 * errors such as a timeout
	 */
	private static final Set<String> NOT_ROW_SQL_STATE_CLASSES = Set.of("08", "40", "42", "0A", "28", "3D", "3F", "25",
			"53", "57", "58", "HY");

	/** The kinds of value a column holds */
	protected enum ColumnType {
		/** Text with a maximum length; longer text is cut to fit */
		TEXT,
		/** Text of any length */
		CLOB,
		/** True or false */
		BOOLEAN,
		/** A whole number */
		INTEGER,
		/** A point in time */
		TIMESTAMP
	}

	/**
	 * One column of the table, in insert order.
	 *
	 * @param name      the column name
	 * @param type      the kind of value it holds
	 * @param maxLength the longest text a TEXT column holds, 0 for other types
	 */
	protected record Column(String name, ColumnType type, int maxLength) {

		/**
		 * A TEXT column.
		 *
		 * @param name      the column name
		 * @param maxLength the longest text it holds
		 * @return the column
		 */
		public static Column text(String name, int maxLength) {
			return new Column(name, ColumnType.TEXT, maxLength);
		}

		/**
		 * A column of a type other than TEXT.
		 *
		 * @param name the column name
		 * @param type the kind of value it holds
		 * @return the column
		 */
		public static Column of(String name, ColumnType type) {
			return new Column(name, type, 0);
		}
	}

	/**
	 * A snapshot of an appender's queue and what it has written.
	 *
	 * @param queuedRows       rows waiting to be written
	 * @param queuedBytes      estimated memory the waiting rows' text takes
	 * @param oldestQueuedMs   how long the oldest waiting row has waited, 0 when
	 *                         none is waiting
	 * @param writtenRows      rows written since the appender started
	 * @param retries          failed writes that were retried
	 * @param textReplacedRows rows written with their long text replaced by a note
	 * @param droppedRows      rows dropped because the queue was full or the
	 *                         database refused them
	 * @param lastError        the most recent write failure, or null
	 */
	public record DeliveryStatus(int queuedRows, long queuedBytes, long oldestQueuedMs, long writtenRows, long retries,
			long textReplacedRows, long droppedRows, String lastError) {
	}

	/** One queued row: its LOG_ID and the value of each column, LOG_ID first */
	private record Row(String logId, Object[] values, long queuedAt, long bytes) {
	}

	/** How one insert attempt ended */
	private enum Attempt {
		WRITTEN, RETRY, REFUSED
	}

	/** Thrown when one row cannot be bound, which only that row causes */
	private static final class RowBindException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		RowBindException(Throwable cause) {
			super(cause.getMessage(), cause);
		}
	}

	private final String table;
	private final List<Column> columns;
	private final Map<String, Integer> valueIndexes = new HashMap<>();
	private final String insertSql;
	private final String storedSql;
	private final int batchSize;
	private final long flushIntervalMs;
	private final int maxQueuedRows;
	private final long maxQueuedBytes;
	private final Thread writer;

	// guarded by lock
	private final Object lock = new Object();
	private final ArrayDeque<Row> queue = new ArrayDeque<>();
	private long queuedBytes;
	private boolean accepting = true;
	private boolean stopping = false;
	private long droppedSinceReport;
	private long lastDropReportAt;

	private final AtomicLong writtenRows = new AtomicLong();
	private final AtomicLong retries = new AtomicLong();
	private final AtomicLong textReplacedRows = new AtomicLong();
	private final AtomicLong droppedRows = new AtomicLong();
	private volatile long lastDropAt;
	private volatile String lastError;
	/**
	 * Set when the writer gives up: the database never loaded, or stopping ran out
	 * of time
	 */
	private volatile boolean abandoned;

	// used only by the writer thread
	private boolean checkStored;
	private long retryDelayMs;
	private long retryAt;
	private long lastAttemptAt;
	private long lastFailureReportAt;
	private long lastStaleReportAt;
	private String lastRefusal;
	// the first row of the batch the database last turned down, and how many times
	// in a row it did
	private String answeredFailureLogId;
	private int answeredFailures;

	/**
	 * An appender that writes to the given table.
	 *
	 * @param name             the appender name
	 * @param filter           the appender's filter, or null
	 * @param layout           the appender's layout, or null
	 * @param ignoreExceptions whether log4j ignores exceptions from this appender
	 * @param table            the table the rows go to
	 * @param columns          the table's columns in insert order, LOG_ID first
	 * @param batchSize        the most rows in one insert, or 0 or less for the
	 *                         default
	 * @param flushIntervalMs  how long a row waits before it is written, or 0 or
	 *                         less for the default
	 * @param maxQueuedRows    the most rows that can wait
	 * @param maxQueuedMb      the most text, in megabytes, the waiting rows can
	 *                         hold
	 */
	protected AbstractBatchingJdbcAppender(String name, Filter filter, Layout<? extends Serializable> layout,
			boolean ignoreExceptions, String table, List<Column> columns, int batchSize, long flushIntervalMs,
			int maxQueuedRows, int maxQueuedMb) {
		super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
		if (columns.isEmpty() || !"LOG_ID".equals(columns.get(0).name())) {
			throw new IllegalArgumentException("The first column must be LOG_ID");
		}
		if (maxQueuedRows <= 0 || maxQueuedMb <= 0) {
			throw new IllegalArgumentException("The queue limits must be positive");
		}
		this.table = table;
		this.columns = List.copyOf(columns);
		for (int index = 1; index < this.columns.size(); index++) {
			this.valueIndexes.put(this.columns.get(index).name(), index - 1);
		}
		this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
		this.flushIntervalMs = flushIntervalMs > 0 ? flushIntervalMs : DEFAULT_FLUSH_INTERVAL_MS;
		this.maxQueuedRows = maxQueuedRows;
		this.maxQueuedBytes = maxQueuedMb * BYTES_PER_MB;

		StringBuilder names = new StringBuilder();
		StringBuilder parameters = new StringBuilder();
		for (Column column : this.columns) {
			if (names.length() > 0) {
				names.append(", ");
				parameters.append(", ");
			}
			names.append(column.name());
			parameters.append('?');
		}
		this.insertSql = "INSERT INTO " + table + " (" + names + ") VALUES (" + parameters + ")";
		this.storedSql = "SELECT LOG_ID FROM " + table + " WHERE LOG_ID IN ";

		this.writer = new Thread(this::runWriter, name + "-Writer");
		this.writer.setDaemon(true);
	}

	/**
	 * The value of each column after LOG_ID for one event, in insert order. Use
	 * {@link #newValues()} and {@link #put(Object[], String, Object)} to fill it.
	 *
	 * @param event the log event
	 * @return one value per column after LOG_ID
	 * @throws Exception when the event cannot be turned into a row
	 */
	protected abstract Object[] columnValues(LogEvent event) throws Exception;

	/**
	 * The row stored for an event that {@link #columnValues(LogEvent)} could not
	 * turn into a row: what is known about the event, and the note in place of its
	 * message.
	 *
	 * @param event the log event
	 * @param note  why the event could not be stored
	 * @return one value per column after LOG_ID
	 */
	protected abstract Object[] unstorableEventValues(LogEvent event, String note);

	/**
	 * The database the rows go to.
	 *
	 * @return the database, or null while it is not loaded yet
	 * @throws Exception when the database cannot be opened
	 */
	protected abstract IRDBMSEngine targetDatabase() throws Exception;

	/**
	 * An empty set of values for the columns after LOG_ID.
	 *
	 * @return one null per column after LOG_ID
	 */
	protected final Object[] newValues() {
		return new Object[this.columns.size() - 1];
	}

	/**
	 * Sets one column's value.
	 *
	 * @param values the values from {@link #newValues()}
	 * @param column the column name
	 * @param value  its value
	 */
	protected final void put(Object[] values, String column, Object value) {
		Integer index = this.valueIndexes.get(column);
		if (index == null) {
			throw new IllegalArgumentException(this.table + " has no column " + column);
		}
		values[index] = value;
	}

	@Override
	public void start() {
		super.start();
		if (this.writer.getState() == Thread.State.NEW) {
			this.writer.start();
		}
	}

	@Override
	public void append(LogEvent event) {
		if (!Utility.isAuditLogsDatabaseEnabled()) {
			return;
		}
		Row row = toRow(event);
		synchronized (this.lock) {
			if (!this.accepting) {
				return;
			}
			boolean full = this.queue.size() >= this.maxQueuedRows
					|| (!this.queue.isEmpty() && this.queuedBytes + row.bytes() > this.maxQueuedBytes);
			if (full) {
				noteDropped(1, "its queue is full");
				return;
			}
			this.queue.addLast(row);
			this.queuedBytes += row.bytes();
			if (this.queue.size() == 1 || this.queue.size() >= this.batchSize) {
				this.lock.notifyAll();
			}
		}
	}

	@Override
	public boolean stop(long timeout, TimeUnit timeUnit) {
		setStopping();
		synchronized (this.lock) {
			this.accepting = false;
			this.stopping = true;
			this.lock.notifyAll();
		}
		if (this.writer.isAlive()) {
			// a timeout of 0 gives none, so the drain keeps its own limit
			long drainMs = timeout > 0 ? Math.min(DRAIN_TIMEOUT_MS, Math.max(1, timeUnit.toMillis(timeout)))
					: DRAIN_TIMEOUT_MS;
			try {
				this.writer.join(drainMs);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			if (this.writer.isAlive()) {
				// a JDBC call can swallow the interrupt, so the writer also stops at its next
				// batch
				this.abandoned = true;
				this.writer.interrupt();
			}
		}
		int left;
		synchronized (this.lock) {
			left = this.queue.size();
		}
		if (left > 0) {
			LOGGER.error("{} stopped with {} rows it could not write to {}", getName(), left, this.table);
		}
		boolean stopped = super.stop(timeout, timeUnit, false);
		setStopped();
		return stopped;
	}

	/**
	 * Whether the appender is healthy: started, under half of both queue limits, no
	 * row waiting longer than 5 minutes, and no row dropped in the last 5 minutes.
	 *
	 * @return true when the appender is healthy
	 */
	public boolean isHealthy() {
		if (!isStarted()) {
			return false;
		}
		long now = System.currentTimeMillis();
		long lastDrop = this.lastDropAt;
		if (lastDrop > 0 && now - lastDrop < UNHEALTHY_AFTER_DROP_MS) {
			return false;
		}
		synchronized (this.lock) {
			if (this.queue.size() * 2L >= this.maxQueuedRows || this.queuedBytes * 2 >= this.maxQueuedBytes) {
				return false;
			}
			Row oldest = this.queue.peekFirst();
			return oldest == null || now - oldest.queuedAt() < STALE_ROW_MS;
		}
	}

	/**
	 * The appender's queue and what it has written.
	 *
	 * @return a snapshot of its queue and counters
	 */
	public DeliveryStatus getDeliveryStatus() {
		synchronized (this.lock) {
			Row oldest = this.queue.peekFirst();
			return new DeliveryStatus(this.queue.size(), this.queuedBytes,
					oldest == null ? 0 : System.currentTimeMillis() - oldest.queuedAt(), this.writtenRows.get(),
					this.retries.get(), this.textReplacedRows.get(), this.droppedRows.get(), this.lastError);
		}
	}

	private Row toRow(LogEvent event) {
		Object[] eventValues;
		try {
			eventValues = columnValues(event);
			if (eventValues == null || eventValues.length != this.columns.size() - 1) {
				throw new IllegalStateException("Expected " + (this.columns.size() - 1) + " column values");
			}
		} catch (Exception e) {
			eventValues = unstorableEventValues(event, "This log event could not be stored: " + describe(e));
		}
		Object[] values = new Object[this.columns.size()];
		String logId = GUID.v7().toUUID().toString();
		values[0] = logId;
		long bytes = logId.length() * BYTES_PER_CHAR;
		for (int index = 1; index < values.length; index++) {
			Object value = fit(this.columns.get(index), eventValues[index - 1]);
			values[index] = value;
			if (value instanceof String text) {
				bytes += text.length() * BYTES_PER_CHAR;
			}
		}
		return new Row(logId, values, System.currentTimeMillis(), bytes);
	}

	private static Object fit(Column column, Object value) {
		if (value == null) {
			return null;
		}
		return switch (column.type()) {
		case TEXT -> {
			String text = value.toString();
			yield text.length() > column.maxLength() ? text.substring(0, column.maxLength()) : text;
		}
		case CLOB -> value instanceof String ? value : GSON.toJson(value);
		default -> value;
		};
	}

	private void runWriter() {
		try {
			List<Row> batch;
			while (!this.abandoned && (batch = nextBatch()) != null) {
				try {
					reportIfStale();
					write(batch);
				} catch (VirtualMachineError e) {
					throw e;
				} catch (RuntimeException | Error e) {
					// one failed write must never end the writer, or every later row would
					// wait until the queue fills and then be dropped
					retryLater("Failed to write to " + this.table + ": " + describe(e));
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Waits until the rows at the head of the queue are due, and returns them
	 * without removing them.
	 *
	 * @return up to {@code batchSize} rows, or null once the appender is stopping
	 *         and the queue is empty
	 */
	private List<Row> nextBatch() throws InterruptedException {
		synchronized (this.lock) {
			while (true) {
				if (this.queue.isEmpty()) {
					if (this.stopping) {
						return null;
					}
					this.lock.wait();
					continue;
				}
				long now = System.currentTimeMillis();
				long dueAt = this.stopping || this.queue.size() >= this.batchSize ? now
						: this.queue.peekFirst().queuedAt() + this.flushIntervalMs;
				// while stopping, retry sooner so the queue drains before the deadline
				long retryStart = this.stopping ? Math.min(this.retryAt, this.lastAttemptAt + STOPPING_RETRY_MS)
						: this.retryAt;
				long startAt = Math.max(dueAt, retryStart);
				if (startAt <= now) {
					List<Row> batch = new ArrayList<>(Math.min(this.batchSize, this.queue.size()));
					Iterator<Row> rows = this.queue.iterator();
					while (rows.hasNext() && batch.size() < this.batchSize) {
						batch.add(rows.next());
					}
					return batch;
				}
				this.lock.wait(startAt - now);
			}
		}
	}

	/**
	 * Writes one batch from the head of the queue. Rows leave the queue only once
	 * they are committed, or dropped because the database refuses them.
	 */
	private void write(List<Row> batch) {
		this.lastAttemptAt = System.currentTimeMillis();
		IRDBMSEngine database;
		try {
			database = targetDatabase();
		} catch (Exception e) {
			retryLater("Unable to open the database for " + this.table + ": " + describe(e));
			return;
		}
		if (database == null) {
			synchronized (this.lock) {
				if (this.stopping) {
					// a database that never loaded will not load while stopping
					this.abandoned = true;
					return;
				}
			}
			// not loaded yet: look again soon, without growing the backoff
			this.retryAt = System.currentTimeMillis() + FIRST_RETRY_MS;
			return;
		}

		Attempt attempt = insert(database, batch, false);
		if (attempt == Attempt.WRITTEN) {
			removeHead(batch.size());
			this.writtenRows.addAndGet(batch.size());
			return;
		}
		if (attempt == Attempt.RETRY) {
			return;
		}
		// the database refused something in the batch, so find the rows one at a time;
		// a row it turns down is refused at once
		for (Row row : batch) {
			Attempt single = batch.size() == 1 ? Attempt.REFUSED : insert(database, List.of(row), true);
			if (single == Attempt.REFUSED) {
				single = insert(database, List.of(withTextReplaced(row, this.lastRefusal)), true);
				if (single == Attempt.WRITTEN) {
					this.textReplacedRows.incrementAndGet();
				} else if (single == Attempt.REFUSED) {
					removeHead(1);
					synchronized (this.lock) {
						noteDropped(1, "the database refused it: " + this.lastRefusal);
					}
					continue;
				}
			}
			if (single == Attempt.RETRY) {
				return;
			}
			removeHead(1);
			this.writtenRows.incrementAndGet();
		}
	}

	/**
	 * Inserts rows in one transaction. A batch the database turns down
	 * {@value #MAX_ANSWERED_FAILURES} times in a row, for a reason that can come
	 * from one row, counts as refused.
	 *
	 * @param refuseAnswered whether any failure the database turns the rows down
	 *                       for counts as refused at once, as it does for one row
	 * @return WRITTEN once committed, REFUSED when a row's values were refused,
	 *         RETRY for any other failure, after scheduling the retry
	 */
	private Attempt insert(IRDBMSEngine database, List<Row> rows, boolean refuseAnswered) {
		boolean[] insertsRan = { false };
		try {
			QueryExecutionUtility.write(database, connection -> {
				List<Row> toInsert = this.checkStored ? notStored(connection, rows) : rows;
				if (!toInsert.isEmpty()) {
					AbstractSqlQueryUtil queryUtil = database.getQueryUtil();
					try (PreparedStatement statement = connection.prepareStatement(this.insertSql)) {
						for (Row row : toInsert) {
							try {
								bind(queryUtil, statement, row);
							} catch (Exception e) {
								throw new RowBindException(e);
							}
							statement.addBatch();
						}
						statement.executeBatch();
					}
				}
				insertsRan[0] = true;
				return null;
			});
		} catch (Exception e) {
			if (insertsRan[0]) {
				// the failure came during commit or release, so the rows may be stored
				this.checkStored = true;
			} else if (isRefused(e) || (isAnsweredFailure(e)
					&& (refuseAnswered || countAnsweredFailure(rows) >= MAX_ANSWERED_FAILURES))) {
				this.lastRefusal = describe(e);
				return Attempt.REFUSED;
			}
			retryLater("Failed to write " + rows.size() + " rows to " + this.table + ": " + describe(e));
			return Attempt.RETRY;
		}
		this.checkStored = false;
		this.answeredFailureLogId = null;
		this.answeredFailures = 0;
		if (this.retryDelayMs > 0) {
			LOGGER.warn("{} is writing to {} again", getName(), this.table);
		}
		this.retryDelayMs = 0;
		this.retryAt = 0;
		return Attempt.WRITTEN;
	}

	/**
	 * The rows whose LOG_ID is not stored yet, after a write whose outcome was
	 * unknown.
	 */
	private List<Row> notStored(Connection connection, List<Row> rows) throws SQLException {
		Set<String> stored = new HashSet<>();
		for (int start = 0; start < rows.size(); start += MAX_LOOKUP_IDS) {
			List<Row> chunk = rows.subList(start, Math.min(rows.size(), start + MAX_LOOKUP_IDS));
			StringBuilder sql = new StringBuilder(this.storedSql).append('(');
			for (int index = 0; index < chunk.size(); index++) {
				sql.append(index == 0 ? "?" : ", ?");
			}
			sql.append(')');
			try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
				for (int index = 0; index < chunk.size(); index++) {
					statement.setString(index + 1, chunk.get(index).logId());
				}
				try (ResultSet rs = statement.executeQuery()) {
					while (rs.next()) {
						stored.add(rs.getString(1));
					}
				}
			}
		}
		if (stored.isEmpty()) {
			return rows;
		}
		List<Row> missing = new ArrayList<>();
		for (Row row : rows) {
			if (!stored.contains(row.logId())) {
				missing.add(row);
			}
		}
		return missing;
	}

	private void bind(AbstractSqlQueryUtil queryUtil, PreparedStatement statement, Row row) throws Exception {
		for (int index = 0; index < this.columns.size(); index++) {
			int parameter = index + 1;
			Object value = row.values()[index];
			switch (this.columns.get(index).type()) {
			case TEXT -> statement.setString(parameter, (String) value);
			case CLOB -> queryUtil.handleInsertionOfClob(statement, value, parameter, GSON);
			case BOOLEAN -> {
				if (value == null) {
					statement.setNull(parameter, Types.BOOLEAN);
				} else {
					statement.setBoolean(parameter, (Boolean) value);
				}
			}
			case INTEGER -> {
				if (value == null) {
					statement.setNull(parameter, Types.INTEGER);
				} else {
					statement.setInt(parameter, ((Number) value).intValue());
				}
			}
			case TIMESTAMP -> {
				if (value == null) {
					statement.setNull(parameter, Types.TIMESTAMP);
				} else {
					statement.setTimestamp(parameter, (Timestamp) value);
				}
			}
			}
		}
	}

	/**
	 * A copy of a row with the text of its CLOB columns replaced by a note giving
	 * why it was refused and the text's original length.
	 */
	private Row withTextReplaced(Row row, String reason) {
		Object[] values = row.values().clone();
		long bytes = row.logId().length() * BYTES_PER_CHAR;
		for (int index = 1; index < values.length; index++) {
			if (this.columns.get(index).type() == ColumnType.CLOB) {
				int length = values[index] == null ? 0 : values[index].toString().length();
				values[index] = "[not stored: the database refused this row (" + reason + "); original length " + length
						+ "]";
			}
			if (values[index] instanceof String text) {
				bytes += text.length() * BYTES_PER_CHAR;
			}
		}
		return new Row(row.logId(), values, row.queuedAt(), bytes);
	}

	/**
	 * Whether a failure was caused by the rows' values rather than by the database
	 * being unreachable: a row that could not be bound, or SQLState class 22 (data
	 * exception) or 23 (integrity constraint violation).
	 */
	private static boolean isRefused(Throwable failure) {
		Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
		for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
			if (cause instanceof RowBindException) {
				return true;
			}
			if (cause instanceof SQLException sqlException) {
				// a batch failure can chain the error of each row behind the first
				SQLException next = sqlException;
				for (int depth = 0; next != null && depth < MAX_CHAINED_ERRORS; depth++) {
					String sqlState = next.getSQLState();
					if (sqlState != null && (sqlState.startsWith("22") || sqlState.startsWith("23"))) {
						return true;
					}
					next = next.getNextException();
				}
			}
		}
		return false;
	}

	/**
	 * Whether the database turned the rows down for a reason that can come from one
	 * row: an SQLState outside the classes that hold back every row, such as a
	 * value too large for its column on a database that reports it outside class
	 * 22.
	 */
	private static boolean isAnsweredFailure(Throwable failure) {
		Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
		for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
			if (cause instanceof SQLException sqlException) {
				String sqlState = sqlException.getSQLState();
				return sqlState != null && sqlState.length() >= 2
						&& !NOT_ROW_SQL_STATE_CLASSES.contains(sqlState.substring(0, 2).toUpperCase());
			}
		}
		return false;
	}

	/**
	 * Counts one more time the database turned down the batch starting with these
	 * rows, starting over for a batch with another first row.
	 *
	 * @return how many times in a row it turned this batch down
	 */
	private int countAnsweredFailure(List<Row> rows) {
		String logId = rows.get(0).logId();
		if (logId.equals(this.answeredFailureLogId)) {
			this.answeredFailures++;
		} else {
			this.answeredFailureLogId = logId;
			this.answeredFailures = 1;
		}
		return this.answeredFailures;
	}

	private void removeHead(int count) {
		synchronized (this.lock) {
			for (int index = 0; index < count && !this.queue.isEmpty(); index++) {
				this.queuedBytes -= this.queue.pollFirst().bytes();
			}
		}
	}

	private void retryLater(String error) {
		this.retries.incrementAndGet();
		this.lastError = error;
		this.retryDelayMs = this.retryDelayMs == 0 ? FIRST_RETRY_MS : Math.min(MAX_RETRY_MS, this.retryDelayMs * 2);
		long now = System.currentTimeMillis();
		this.retryAt = now + this.retryDelayMs;
		if (now - this.lastFailureReportAt >= REPORT_INTERVAL_MS) {
			this.lastFailureReportAt = now;
			LOGGER.error("{} could not write to {}; retrying in {} seconds with {} rows waiting: {}", getName(),
					this.table, this.retryDelayMs / 1000, getDeliveryStatus().queuedRows(), error);
		}
	}

	/**
	 * Counts dropped rows and reports them at most once a minute. Call holding the
	 * lock.
	 */
	private void noteDropped(int count, String reason) {
		long now = System.currentTimeMillis();
		this.droppedRows.addAndGet(count);
		this.lastDropAt = now;
		this.droppedSinceReport += count;
		if (now - this.lastDropReportAt >= REPORT_INTERVAL_MS) {
			LOGGER.error(
					"{} dropped {} rows for {} in the last minute because {}; {} rows and {} MB waiting, {} dropped "
							+ "since it started",
					getName(), this.droppedSinceReport, this.table, reason, this.queue.size(),
					this.queuedBytes / BYTES_PER_MB, this.droppedRows.get());
			this.droppedSinceReport = 0;
			this.lastDropReportAt = now;
		}
	}

	private void reportIfStale() {
		DeliveryStatus status = getDeliveryStatus();
		long now = System.currentTimeMillis();
		if (status.oldestQueuedMs() >= STALE_ROW_MS && now - this.lastStaleReportAt >= REPORT_INTERVAL_MS) {
			this.lastStaleReportAt = now;
			LOGGER.error("{} has {} rows waiting for {}; the oldest was logged {} minutes ago. Last error: {}",
					getName(), status.queuedRows(), this.table,
					TimeUnit.MILLISECONDS.toMinutes(status.oldestQueuedMs()), status.lastError());
		}
	}

	private static String describe(Throwable error) {
		if (error instanceof SQLException sqlException && sqlException.getSQLState() != null) {
			return "SQLState " + sqlException.getSQLState() + ": " + sqlException.getMessage();
		}
		return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
	}
}
