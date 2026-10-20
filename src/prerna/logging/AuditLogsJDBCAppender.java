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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.message.ObjectMessage;
import org.apache.logging.log4j.util.ReadOnlyStringMap;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

@Plugin(name = "AuditLogsJDBCAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class AuditLogsJDBCAppender extends AbstractAppender {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	private final String ENGINE_ID;
	private final String INSERT_SQL;
	private final List<LogEvent> events = new ArrayList<>();
	private int batchSize = 100;
	private static final long FLUSH_INTERVAL_MS = 60_000; // 1 minute
	private final AtomicLong lastAppendTime = new AtomicLong(System.currentTimeMillis());
	private final ScheduledExecutorService SCHEDULER;

	protected AuditLogsJDBCAppender(String name, Filter filter, Layout<? extends Serializable> layout,
			boolean ignoreExceptions, String engineId, int batchSize) {
		super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
		this.ENGINE_ID = engineId;
		if (batchSize > 0) {
			this.batchSize = batchSize;
		}
		// SQL for inserting into audit_logs table
		this.INSERT_SQL = """
				INSERT INTO AUDIT_LOGS (
				    LOG_ID, REQUEST_ID, IS_SUCCESS, SESSION_ID, USER_ID, USER_NAME, USER_TYPE, SPAN_ID, INSIGHT_ID, PROJECT_ID, PROJECT_NAME, ROOM_ID,
				    ENGINE_ID, ENGINE_NAME, ENGINE_TYPE, METHOD_NAME, ENGINE_SUBTYPE, INPUT_REACTOR_NAME, OUTPUT_REACTOR_NAME, GUARDRAIL_ACTION,
				    MESSAGE, REQUEST, RESPONSE,
				    NUMBER_OF_TOKENS_IN_PROMPT, NUMBER_OF_TOKENS_IN_RESPONSE, NUMBER_OF_CACHE_READ_TOKENS, NUMBER_OF_CACHE_CREATION_TOKENS,
				    REQUEST_START_TIME, RESPONSE_END_TIME,
				    LOG_LEVEL, LOG_TIMESTAMP, LOGGER_NAME, LOGGER_LOCATION
				) VALUES (
				    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
				    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
				    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
				    ?, ?, ?
				);
				""";

		this.SCHEDULER = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "AuditLogsJDBCAppender-Scheduler");
			t.setDaemon(true);
			return t;
		});

		this.SCHEDULER.scheduleAtFixedRate(this::flushIfIdle, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS,
				TimeUnit.MILLISECONDS);
	}

	@Override
	public void append(LogEvent event) {
		if (!Utility.isAuditLogsDatabaseEnabled()) {
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
		if (!Utility.isAuditLogsDatabaseEnabled()) {
			return;
		}
		if (System.currentTimeMillis() - lastAppendTime.get() >= FLUSH_INTERVAL_MS) {
			flush();
		}
	}

	private void flush() {
		if (!Utility.isAuditLogsDatabaseEnabled()) {
			return;
		}
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

		IRDBMSEngine auditLogs = null;
		if (!Constants.AUDIT_LOGS_DB.equals(this.ENGINE_ID)) {
			auditLogs = (IRDBMSEngine) Utility.getDatabase(this.ENGINE_ID);
			try {
				AuditLogsDbUtils.initEngineAsAuditDatabase(auditLogs);
			} catch (Exception e) {
				LOGGER.error("Failed to initialize custom engine as audit logs database", e);
				return;
			}
		} else {
			auditLogs = SystemEngineRegistry.getAuditLogsDb();
		}
		if (auditLogs == null) {
			LOGGER.warn("Audit logs database has not been initialized yet");
			return;
		}
		AbstractSqlQueryUtil queryUtil = auditLogs.getQueryUtil();

		Connection connection = null;
		PreparedStatement stmt = null;
		try {
			connection = auditLogs.getConnection();
			stmt = connection.prepareStatement(INSERT_SQL);
			for (LogEvent event : processingEvents) {
				// Get context data for custom fields
				ReadOnlyStringMap contextData = event.getContextData();
				Map<String, Object> message = null;
				if (event.getMessage() instanceof ObjectMessage) {
					ObjectMessage objMessage = (ObjectMessage) event.getMessage();
					message = (Map<String, Object>) objMessage.getParameter();
				} else if (event.getMessage() instanceof MapMessage) {
					MapMessage<?, ?> mapMesssage = (MapMessage<?, ?>) event.getMessage();
					message = (Map<String, Object>) mapMesssage.getData();
				}

				// Map all fields to the audit_logs table columns
				int paramIdx = 1;
				stmt.setString(paramIdx++, GUID.v7().toUUID().toString()); // log_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.REQUEST_ID, contextData, message)); // request_id
				stmt.setBoolean(paramIdx++, getBooleanValue(SemossLogUtils.IS_SUCCESS, contextData, message)); // is_success
				stmt.setString(paramIdx++, getValue(SemossLogUtils.SESSION_ID, contextData, message)); // session_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.USER_ID, contextData, message)); // user_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.USER_NAME, contextData, message)); // user_name
				stmt.setString(paramIdx++, getValue(SemossLogUtils.USER_TYPE, contextData, message)); // user_type
				stmt.setString(paramIdx++, getValue(SemossLogUtils.SPAN_ID, contextData, message)); // span_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.INSIGHT_ID, contextData, message)); // insight_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.PROJECT_ID, contextData, message)); // project_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.PROJECT_NAME, contextData, message)); // project_name
				stmt.setString(paramIdx++, getValue(SemossLogUtils.ROOM_ID, contextData, message)); // room_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.ENGINE_ID, contextData, message)); // engine_id
				stmt.setString(paramIdx++, getValue(SemossLogUtils.ENGINE_NAME, contextData, message)); // engine_name
				stmt.setString(paramIdx++, getValue(SemossLogUtils.ENGINE_TYPE, contextData, message)); // engine_type
				stmt.setString(paramIdx++, getValue(SemossLogUtils.METHOD_NAME, contextData, message)); // method_name
				stmt.setString(paramIdx++, getValue(SemossLogUtils.ENGINE_SUBTYPE, contextData, message)); // engine_subtype
				stmt.setString(paramIdx++, getValue(SemossLogUtils.INPUT_REACTOR_NAME, contextData, message)); // input_reactor_name
				stmt.setString(paramIdx++, getValue(SemossLogUtils.OUTPUT_REACTOR_NAME, contextData, message)); // output_reactor_name
				stmt.setString(paramIdx++, getValue(SemossLogUtils.GUARDRAIL_ACTION, contextData, message)); // guardrail_action
				queryUtil.handleInsertionOfClob(stmt, event.getMessage().getFormattedMessage(), paramIdx++, GSON); // message
				queryUtil.handleInsertionOfClob(stmt, getValue(SemossLogUtils.REQUEST, contextData, message),
						paramIdx++, GSON); // request
				queryUtil.handleInsertionOfClob(stmt, getValue(SemossLogUtils.RESPONSE, contextData, message),
						paramIdx++, GSON); // response
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
				{
					Integer tokens = getInteger(SemossLogUtils.NUMBER_OF_CACHE_READ_TOKENS, contextData, message);
					if (tokens == null) {
						stmt.setNull(paramIdx++, java.sql.Types.INTEGER);
					} else {
						stmt.setInt(paramIdx++, tokens); // number_of_cache_read_tokens
					}
				}
				{
					Integer tokens = getInteger(SemossLogUtils.NUMBER_OF_CACHE_CREATION_TOKENS, contextData, message);
					if (tokens == null) {
						stmt.setNull(paramIdx++, java.sql.Types.INTEGER);
					} else {
						stmt.setInt(paramIdx++, tokens); // number_of_cache_creation_tokens
					}
				}
				stmt.setTimestamp(paramIdx++,
						getTimestampValue(SemossLogUtils.REQUEST_START_TIME, contextData, message)); // request_start_time
				stmt.setTimestamp(paramIdx++,
						getTimestampValue(SemossLogUtils.RESPONSE_END_TIME, contextData, message)); // response_end_time
				stmt.setString(paramIdx++, event.getLevel().toString()); // log_level
				stmt.setTimestamp(paramIdx++, new Timestamp(event.getTimeMillis())); // log_timestamp
				stmt.setString(paramIdx++, event.getLoggerName());// logger_name
				stmt.setString(paramIdx++, SemossLogUtils.appendSourceInfo(event)); // logger_location

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
		if (!Utility.isAuditLogsDatabaseEnabled()) {
			return;
		}
		flush();
		SCHEDULER.shutdown();
		try {
			if (!SCHEDULER.awaitTermination(1, TimeUnit.MINUTES)) {
				SCHEDULER.shutdownNow();
			}
		} catch (InterruptedException e) {
			SCHEDULER.shutdownNow();
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
	private String getValue(String key, ReadOnlyStringMap contextData, Map<String, Object> message) {
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
	private boolean getBooleanValue(String key, ReadOnlyStringMap contextData, Map<String, Object> message) {
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
	private java.sql.Timestamp getTimestampValue(String key, ReadOnlyStringMap contextData,
			Map<String, Object> message) {
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
	private Integer getInteger(String key, ReadOnlyStringMap contextData, Map<String, Object> message) {
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
			@PluginElement("Filter") Filter filter, @PluginElement("Layout") Layout<String> layout,
			@PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) boolean ignoreExceptions,
			@PluginAttribute(value = "engineId", defaultString = "AuditLogs") String engineId,
			@PluginAttribute(value = "batchSize", defaultInt = 100) int batchSize) {

		if (name == null) {
			LOGGER.error("No name provided for AuditLogsJDBCAppender");
			return null;
		}

		return new AuditLogsJDBCAppender(name, filter, layout, ignoreExceptions, engineId, batchSize);
	}

}