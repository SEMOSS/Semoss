package prerna.logging;

import java.io.Serializable;
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
import org.apache.logging.log4j.util.ReadOnlyStringMap;

import com.github.f4b6a3.uuid.alt.GUID;
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

	private final String insertSQL;
	private final List<LogEvent> events = new ArrayList<>();
	private int batchSize = 100;
	private static final long FLUSH_INTERVAL_MS = 60_000; // 1 minute
	private final AtomicLong lastAppendTime = new AtomicLong(System.currentTimeMillis());
	private final ScheduledExecutorService scheduler;

	protected ServerLogsJDBCAppender(String name, Filter filter, Layout<? extends Serializable> layout,
			boolean ignoreExceptions, int batchSize) {
		super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
		if (batchSize > 0) {
			this.batchSize = batchSize;
		}
		this.insertSQL = """
				INSERT INTO SERVER_LOGS (
					LOG_ID, REQUEST_ID, SESSION_ID, USER_ID, LEVEL, LOGGER_NAME,
					LOGGER_LOCATION, THREAD_NAME, LOG_TIMESTAMP, MESSAGE
				) VALUES (
					?, ?, ?, ?, ?, ?, ?, ?, ?, ?
				);
				""";

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

		IRDBMSEngine auditLogs = (IRDBMSEngine) Utility.getDatabase(Constants.AUDIT_LOGS_DB);
		AbstractSqlQueryUtil queryUtil = auditLogs.getQueryUtil();

		Connection connection = null;
		PreparedStatement stmt = null;
		try {
			connection = auditLogs.getConnection();
			stmt = connection.prepareStatement(this.insertSQL);
			for (LogEvent event : processingEvents) {
				ReadOnlyStringMap contextData = event.getContextData();

				int paramIdx = 1;
				// create unique log id
				stmt.setString(paramIdx++, GUID.v7().toUUID().toString());
				// request id
				stmt.setString(paramIdx++, contextData.getValue(SemossLogUtils.REQUEST_ID));
				// session id
				stmt.setString(paramIdx++, contextData.getValue(SemossLogUtils.SESSION_ID));
				// user id
				stmt.setString(paramIdx++, contextData.getValue(SemossLogUtils.USER_ID));
				stmt.setString(paramIdx++, event.getLevel().toString());
				stmt.setString(paramIdx++, event.getLoggerName());
				stmt.setString(paramIdx++, SemossLogUtils.appendSourceInfo(event));
				stmt.setString(paramIdx++, event.getThreadName());
				stmt.setTimestamp(paramIdx++, new Timestamp(event.getTimeMillis()));
				queryUtil.handleInsertionOfClob(stmt, event.getMessage().getFormattedMessage(), paramIdx++, GSON);
				stmt.addBatch();
			}
			stmt.executeBatch();
			connection.commit();
		} catch (Exception e) {
			LOGGER.error("Failed to insert audit log into database", e);
			if (connection != null) {
				try {
					connection.rollback();
				} catch (SQLException ex) {
					LOGGER.error("Failed to rollback transaction", ex);
				}
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(auditLogs, connection, stmt);
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
			@PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) boolean ignoreExceptions,
			@PluginAttribute(value = "batchSize", defaultInt = 100) int batchSize) {

		if (name == null) {
			LOGGER.error("No name provided for ServerLogsJDBCAppender");
			return null;
		}

		return new ServerLogsJDBCAppender(name, filter, layout, ignoreExceptions, batchSize);
	}
}