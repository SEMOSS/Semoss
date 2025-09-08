package prerna.logging;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

@Plugin(name = "ServerLogsJDBCAppender", category = "Core", elementType = "appender", printObject = true)
public class ServerLogsJDBCAppender extends AbstractAppender {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	private final List<LogEvent> events = new ArrayList<>();
	private final int batchSize = 100;
	private static final long FLUSH_INTERVAL_MS = 60_000; // 1 minute
	private final AtomicLong lastAppendTime = new AtomicLong(System.currentTimeMillis());
	private final ScheduledExecutorService scheduler;

	protected ServerLogsJDBCAppender(String name, Filter filter, Layout<? extends Serializable> layout,
			boolean ignoreExceptions) {
		super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);

		this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "ServerLogsJDBCAppender-Scheduler");
			t.setDaemon(true);
			return t;
		});

		this.scheduler.scheduleAtFixedRate(this::flushIfIdle, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS,
				TimeUnit.MILLISECONDS);
	}

	@Override
	public void append(LogEvent event) {
		if (!AuditLogsDbUtils.isInitalized()) {
			return;
		}

		synchronized (events) {
			events.add(event.toImmutable());
			lastAppendTime.set(System.currentTimeMillis());
			if (events.size() >= batchSize) {
				flush();
			}
		}
	}

	private void flushIfIdle() {
		if (System.currentTimeMillis() - lastAppendTime.get() >= FLUSH_INTERVAL_MS) {
			flush();
		}
	}

	private void flush() {
		List<LogEvent> processingEvents;
		synchronized (events) {
			if (events.isEmpty()) {
				return;
			}
			processingEvents = new ArrayList<>(events);
			events.clear();
		}

		if (processingEvents.isEmpty()) {
			return;
		}

		IRDBMSEngine auditLogs = (IRDBMSEngine) Utility.getDatabase(Constants.AUDIT_LOGS_DATABASE_NAME);
		AbstractSqlQueryUtil queryUtil = auditLogs.getQueryUtil();

		final String insertSql = "INSERT INTO SERVER_LOGS (LEVEL, LOGGER, THREAD_NAME, LOG_TIMESTAMP, MESSAGE) VALUES (?, ?, ?, ?, ?)";
		Connection connection = null;
		PreparedStatement stmt = null;
		try {
			connection = auditLogs.getConnection();
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement(insertSql);

			for (LogEvent event : processingEvents) {
				stmt.setString(1, event.getLevel().toString());
				stmt.setString(2, event.getLoggerName());
				stmt.setString(3, event.getThreadName());
				stmt.setTimestamp(4, new Timestamp(event.getTimeMillis()));
				queryUtil.handleInsertionOfClob(stmt, event.getMessage().getFormattedMessage(), 5, GSON);
				stmt.addBatch();
			}
			stmt.executeBatch();
			connection.commit();
		} catch (SQLException | UnsupportedEncodingException e) {
			LOGGER.error("Failed to insert audit log into database", e);
			if (connection != null) {
				try {
					connection.rollback();
				} catch (SQLException ex) {
					LOGGER.error("Failed to rollback transaction", ex);
				}
			}
		} finally {
			if (connection != null) {
				try {
					connection.setAutoCommit(true);
				} catch (SQLException e) {
					LOGGER.error("Failed to reset auto-commit", e);
				}
			}
			ConnectionUtils.closeAllDbConnectionsIfPooling(auditLogs, stmt);
		}
	}

	@Override
	public void stop() {
		flush();
		scheduler.shutdown();
		try {
			if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
				scheduler.shutdownNow();
			}
		} catch (InterruptedException e) {
			scheduler.shutdownNow();
			Thread.currentThread().interrupt();
		}
		super.stop();
	}

	@PluginFactory
	public static ServerLogsJDBCAppender createAppender(@PluginAttribute("name") String name,
			@PluginElement("Filter") Filter filter, @PluginElement("Layout") Layout<? extends Serializable> layout,
			@PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) boolean ignoreExceptions) {

		if (name == null) {
			LOGGER.error("No name provided for ServerLogsJDBCAppender");
			return null;
		}

		return new ServerLogsJDBCAppender(name, filter, layout, ignoreExceptions);
	}
}