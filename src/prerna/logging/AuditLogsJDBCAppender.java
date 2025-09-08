package prerna.logging;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.util.ReadOnlyStringMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

@Plugin(name = "AuditLogsJDBCAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class AuditLogsJDBCAppender extends AbstractAppender {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	private final String insertSQL;
	private final List<LogEvent> events = new ArrayList<>();
	private final int batchSize = 100;
	private static final long FLUSH_INTERVAL_MS = 60_000; // 1 minute
	private final AtomicLong lastAppendTime = new AtomicLong(System.currentTimeMillis());
	private final ScheduledExecutorService scheduler;

	protected AuditLogsJDBCAppender(String name, Filter filter, Layout<String> layout) {
		super(name, filter, layout, true, null);

		// SQL for inserting into audit_logs table
		this.insertSQL = """
				INSERT INTO audit_logs (
				    log_id, is_success, engine_id, engine_name, engine_type, input_reactor_name,
				    insight_id, log_level, log_timestamp, logger_name,
				    method_id, method_name, method_type, number_of_tokens_in_prompt,
				    number_of_tokens_in_response, output_reactor_name, project_id,
				    project_name, request_start_time, response_end_time, room_id,
				    session_id, span_id, user_id, message, request, response
				) VALUES (
				    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
				    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
				    ?, ?, ?, ?, ?, ?, ?
				)
				""";

		this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "AuditLogsJDBCAppender-Scheduler");
			t.setDaemon(true);
			return t;
		});

		this.scheduler.scheduleAtFixedRate(this::flushIfIdle, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS,
				TimeUnit.MILLISECONDS);
	}

	@Override
	public void append(LogEvent event) {
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

		Connection connection = null;
		PreparedStatement stmt = null;
		try {
			connection = auditLogs.getConnection();
			connection.setAutoCommit(false);
			stmt = connection.prepareStatement(insertSQL);

			for (LogEvent event : processingEvents) {
				// Get context data for custom fields
				ReadOnlyStringMap contextData = event.getContextData();
				MapMessage<?, ?> message = (MapMessage<?, ?>) event.getMessage();

				// Generate unique log_id if not provided
				String logId = contextData.getValue(SemossLogUtils.LOG_ID);
				if (logId == null || logId.isEmpty()) {
					logId = UUID.randomUUID().toString();
				}

				// Map all fields to the audit_logs table columns
				int paramIdx = 1;
				stmt.setString(paramIdx++, logId); // log_id
				stmt.setBoolean(paramIdx++, getBooleanValue(SemossLogUtils.IS_SUCCESS, contextData, message)); // is_success
				stmt.setString(paramIdx++, getValue(SemossLogUtils.ENGINE_ID, contextData, message)); // engine_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.ENGINE_NAME, contextData, message)); // engine_name
				stmt.setString(paramIdx++, getValue(SemossLogUtils.ENGINE_TYPE, contextData, message)); // engine_type
				stmt.setString(paramIdx++, getValue(SemossLogUtils.INPUT_REACTOR_NAME, contextData, message)); // input_reactor_name
				stmt.setString(paramIdx++, getValue(SemossLogUtils.INSIGHT_ID, contextData, message)); // insight_id
				stmt.setString(paramIdx++, event.getLevel().toString()); // log_level
				stmt.setTimestamp(paramIdx++, new Timestamp(event.getTimeMillis())); // log_timestamp
				stmt.setString(paramIdx++, event.getLoggerName()); // logger_name
				stmt.setString(paramIdx++, getValue(SemossLogUtils.METHOD_ID, contextData, message)); // method_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.METHOD_NAME, contextData, message)); // method_name
				stmt.setString(paramIdx++, getValue(SemossLogUtils.METHOD_TYPE, contextData, message)); // method_type
				{
					Integer tokens = getInteger(SemossLogUtils.NUMBER_OF_TOKENS_IN_PROMPT, contextData, message);
					if (tokens == null) {
						stmt.setNull(paramIdx++, java.sql.Types.INTEGER);
					} else {
						stmt.setInt(paramIdx++, tokens); // number_of_tokens_in_prompt
					}
				}
				{
					Integer tokens = getInteger(SemossLogUtils.NUMBER_OF_TOKENS_IN_RESPONSE, contextData, message);
					if (tokens == null) {
						stmt.setNull(paramIdx++, java.sql.Types.INTEGER);
					} else {
						stmt.setInt(paramIdx++, tokens); // number_of_tokens_in_response
					}
				}
				stmt.setString(paramIdx++, getValue(SemossLogUtils.OUTPUT_REACTOR_NAME, contextData, message)); // output_reactor_name
				stmt.setString(paramIdx++, getValue(SemossLogUtils.PROJECT_ID, contextData, message)); // project_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.PROJECT_NAME, contextData, message)); // project_name
				stmt.setTimestamp(paramIdx++,
						getTimestampValue(SemossLogUtils.REQUEST_START_TIME, contextData, message)); // request_start_time
				stmt.setTimestamp(paramIdx++,
						getTimestampValue(SemossLogUtils.RESPONSE_END_TIME, contextData, message)); // response_end_time
				stmt.setString(paramIdx++, getValue(SemossLogUtils.ROOM_ID, contextData, message)); // room_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.SESSION_ID, contextData, message)); // session_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.SPAN_ID, contextData, message)); // span_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.USER_ID, contextData, message)); // user_id
				queryUtil.handleInsertionOfClob(stmt, event.getMessage().getFormattedMessage(), paramIdx++, GSON); // message
				queryUtil.handleInsertionOfClob(stmt, getValue(SemossLogUtils.REQUEST, contextData, message),
						paramIdx++, GSON); // request
				queryUtil.handleInsertionOfClob(stmt, getValue(SemossLogUtils.RESPONSE, contextData, message),
						paramIdx++, GSON); // response

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

	/**
	 * Helper method to get a value from the context data or the message
	 * 
	 * @param key
	 * @param contextData
	 * @param message
	 * @return
	 */
	private String getValue(String key, ReadOnlyStringMap contextData, MapMessage<?, ?> message) {
		String value = contextData.getValue(key);
		if (value != null) {
			return value;
		}
		Object messageValue = message.get(key);
		if (messageValue != null) {
			return messageValue.toString();
		}
		return null;
	}

	/**
	 * Helper method to convert string to boolean, with default value
	 * 
	 * @param value
	 * @param defaultValue
	 * @return
	 */
	private boolean getBooleanValue(String key, ReadOnlyStringMap contextData, MapMessage<?, ?> message) {
		String value = contextData.getValue(key);
		if (value != null) {
			return Boolean.parseBoolean(value);
		}
		Object messageValue = message.get(key);
		if (messageValue != null) {
			return Boolean.parseBoolean(messageValue.toString());
		}
		return true;
	}

	/**
	 * Helper method to extract ZonedDateTime (as Object or String
	 * ISO_ZONED_DATE_TIME format) to sql Timestamp
	 * 
	 * @param key
	 * @param contextData
	 * @param message
	 * @return
	 */
	private java.sql.Timestamp getTimestampValue(String key, ReadOnlyStringMap contextData, MapMessage<?, ?> message) {
		String value = contextData.getValue(key);
		if (value != null) {
			ZonedDateTime zdt = ZonedDateTime.parse(value, DateTimeFormatter.ISO_ZONED_DATE_TIME);
			Timestamp timestamp = Timestamp.from(zdt.toInstant());
			return timestamp;
		}
		Object messageValue = message.get(key);
		if (messageValue != null) {
			if (messageValue instanceof ZonedDateTime) {
				ZonedDateTime zdt = (ZonedDateTime) messageValue;
				Timestamp timestamp = Timestamp.from(zdt.toInstant());
				return timestamp;
			} else {
				ZonedDateTime zdt = ZonedDateTime.parse(messageValue + "", DateTimeFormatter.ISO_ZONED_DATE_TIME);
				Timestamp timestamp = Timestamp.from(zdt.toInstant());
				return timestamp;
			}
		}
		return null;
	}

	/**
	 * Helper method to extract integer value
	 * 
	 * @param key
	 * @param contextData
	 * @param message
	 * @return
	 */
	private Integer getInteger(String key, ReadOnlyStringMap contextData, MapMessage<?, ?> message) {
		String value = contextData.getValue(key);
		if (value != null) {
			try {
				int val = ((Number) Double.parseDouble(value)).intValue();
				return val;
			} catch (java.lang.NumberFormatException nfe) {
				LOGGER.error(Constants.STACKTRACE, nfe);
			}
		}
		Object messageValue = message.get(key);
		if (messageValue != null) {
			if (messageValue instanceof Number) {
				int val = ((Number) messageValue).intValue();
				return val;
			} else {
				try {
					int val = ((Number) Double.parseDouble(messageValue + "")).intValue();
					return val;
				} catch (java.lang.NumberFormatException nfe) {
					LOGGER.error(Constants.STACKTRACE, nfe);
				}
			}
		}
		return null;
	}

	@PluginFactory
	public static AuditLogsJDBCAppender createAppender(@PluginAttribute("name") String name,
			@PluginElement("Filter") Filter filter, @PluginElement("Layout") Layout<String> layout) {

		if (name == null) {
			LOGGER.error("No name provided for AuditLogsJDBCAppender");
			return null;
		}

		return new AuditLogsJDBCAppender(name, filter, layout);
	}

}