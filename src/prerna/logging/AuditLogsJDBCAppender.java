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
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.ObjectMessage;
import org.apache.logging.log4j.util.ReadOnlyStringMap;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Stores engine calls in the {@code AUDIT_LOGS} table: who called which engine
 * method, when, whether it succeeded, and its request and response when the
 * engine keeps them. Rows are queued and written in the background by
 * {@link AbstractBatchingJdbcAppender}, so a slow or unreachable database never
 * makes an engine call wait.
 */
@Plugin(name = "AuditLogsJDBCAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class AuditLogsJDBCAppender extends AbstractBatchingJdbcAppender {

	/** The most rows that wait when the configuration does not say */
	private static final int DEFAULT_MAX_QUEUED_ROWS = 50_000;
	/**
	 * The most text, in megabytes, the waiting rows hold when the configuration
	 * does not say
	 */
	private static final int DEFAULT_MAX_QUEUED_MB = 128;
	private static final int TEXT_LENGTH = 255;

	private static final List<Column> COLUMNS = List.of(Column.text("LOG_ID", TEXT_LENGTH),
			Column.text("REQUEST_ID", TEXT_LENGTH), Column.of("IS_SUCCESS", ColumnType.BOOLEAN),
			Column.text("SESSION_ID", TEXT_LENGTH), Column.text("USER_ID", TEXT_LENGTH),
			Column.text("USER_NAME", TEXT_LENGTH), Column.text("USER_TYPE", TEXT_LENGTH),
			Column.text("SPAN_ID", TEXT_LENGTH), Column.text("INSIGHT_ID", TEXT_LENGTH),
			Column.text("PROJECT_ID", TEXT_LENGTH), Column.text("PROJECT_NAME", TEXT_LENGTH),
			Column.text("ROOM_ID", TEXT_LENGTH), Column.text("ENGINE_ID", TEXT_LENGTH),
			Column.text("ENGINE_NAME", TEXT_LENGTH), Column.text("ENGINE_TYPE", TEXT_LENGTH),
			Column.text("METHOD_NAME", TEXT_LENGTH), Column.text("ENGINE_SUBTYPE", TEXT_LENGTH),
			Column.text("INPUT_REACTOR_NAME", TEXT_LENGTH), Column.text("OUTPUT_REACTOR_NAME", TEXT_LENGTH),
			Column.text("GUARDRAIL_ACTION", TEXT_LENGTH), Column.text("BILLED_SOURCE_TYPE", TEXT_LENGTH),
			Column.text("BILLED_SUBJECT_ID", TEXT_LENGTH), Column.text("BILLED_SUBJECT_TYPE", TEXT_LENGTH),
			Column.of("MESSAGE", ColumnType.CLOB), Column.of("REQUEST", ColumnType.CLOB),
			Column.of("RESPONSE", ColumnType.CLOB), Column.of("NUMBER_OF_TOKENS_IN_PROMPT", ColumnType.INTEGER),
			Column.of("NUMBER_OF_TOKENS_IN_RESPONSE", ColumnType.INTEGER),
			Column.of("NUMBER_OF_CACHE_READ_TOKENS", ColumnType.INTEGER),
			Column.of("NUMBER_OF_CACHE_CREATION_TOKENS", ColumnType.INTEGER),
			Column.of("REQUEST_START_TIME", ColumnType.TIMESTAMP), Column.of("RESPONSE_END_TIME", ColumnType.TIMESTAMP),
			Column.text("LOG_LEVEL", TEXT_LENGTH), Column.of("LOG_TIMESTAMP", ColumnType.TIMESTAMP),
			Column.text("LOGGER_NAME", TEXT_LENGTH), Column.text("LOGGER_LOCATION", TEXT_LENGTH));

	/**
	 * Each text column read from the logging context or the logged map, and its key
	 * there
	 */
	private static final Map<String, String> CONTEXT_COLUMNS = Map.ofEntries(
			Map.entry("REQUEST_ID", SemossLogUtils.REQUEST_ID), Map.entry("SESSION_ID", SemossLogUtils.SESSION_ID),
			Map.entry("USER_ID", SemossLogUtils.USER_ID), Map.entry("USER_NAME", SemossLogUtils.USER_NAME),
			Map.entry("USER_TYPE", SemossLogUtils.USER_TYPE), Map.entry("SPAN_ID", SemossLogUtils.SPAN_ID),
			Map.entry("INSIGHT_ID", SemossLogUtils.INSIGHT_ID), Map.entry("PROJECT_ID", SemossLogUtils.PROJECT_ID),
			Map.entry("PROJECT_NAME", SemossLogUtils.PROJECT_NAME), Map.entry("ROOM_ID", SemossLogUtils.ROOM_ID),
			Map.entry("ENGINE_ID", SemossLogUtils.ENGINE_ID), Map.entry("ENGINE_NAME", SemossLogUtils.ENGINE_NAME),
			Map.entry("ENGINE_TYPE", SemossLogUtils.ENGINE_TYPE), Map.entry("METHOD_NAME", SemossLogUtils.METHOD_NAME),
			Map.entry("ENGINE_SUBTYPE", SemossLogUtils.ENGINE_SUBTYPE),
			Map.entry("INPUT_REACTOR_NAME", SemossLogUtils.INPUT_REACTOR_NAME),
			Map.entry("OUTPUT_REACTOR_NAME", SemossLogUtils.OUTPUT_REACTOR_NAME),
			Map.entry("GUARDRAIL_ACTION", SemossLogUtils.GUARDRAIL_ACTION),
			Map.entry("BILLED_SOURCE_TYPE", SemossLogUtils.BILLED_SOURCE_TYPE),
			Map.entry("BILLED_SUBJECT_ID", SemossLogUtils.BILLED_SUBJECT_ID),
			Map.entry("BILLED_SUBJECT_TYPE", SemossLogUtils.BILLED_SUBJECT_TYPE));

	private final String engineId;
	private volatile boolean customDatabaseReady = false;

	protected AuditLogsJDBCAppender(String name, Filter filter, Layout<? extends Serializable> layout,
			boolean ignoreExceptions, String engineId, int batchSize, long flushIntervalMs, int maxQueuedRows,
			int maxQueuedMb) {
		super(name, filter, layout, ignoreExceptions, "AUDIT_LOGS", COLUMNS, batchSize, flushIntervalMs, maxQueuedRows,
				maxQueuedMb);
		this.engineId = engineId;
	}

	@Override
	protected IRDBMSEngine targetDatabase() throws Exception {
		if (Constants.AUDIT_LOGS_DB.equals(this.engineId)) {
			return AuditLogsDbUtils.isInitalized() ? SystemEngineRegistry.getAuditLogsDb() : null;
		}
		IRDBMSEngine database = (IRDBMSEngine) Utility.getDatabase(this.engineId);
		if (database != null && !this.customDatabaseReady) {
			AuditLogsDbUtils.initEngineAsAuditDatabase(database);
			this.customDatabaseReady = true;
		}
		return database;
	}

	@Override
	protected Object[] columnValues(LogEvent event) {
		ReadOnlyStringMap contextData = event.getContextData();
		Map<?, ?> message = loggedMap(event.getMessage());
		Object[] values = newValues();
		for (Map.Entry<String, String> column : CONTEXT_COLUMNS.entrySet()) {
			put(values, column.getKey(), text(column.getValue(), contextData, message));
		}
		put(values, "IS_SUCCESS", success(contextData, message));
		put(values, "MESSAGE", event.getMessage().getFormattedMessage());
		put(values, "REQUEST", text(SemossLogUtils.REQUEST, contextData, message));
		put(values, "RESPONSE", text(SemossLogUtils.RESPONSE, contextData, message));
		put(values, "NUMBER_OF_TOKENS_IN_PROMPT",
				integer(SemossLogUtils.NUMBER_OF_TOKENS_IN_PROMPT, contextData, message));
		put(values, "NUMBER_OF_TOKENS_IN_RESPONSE",
				integer(SemossLogUtils.NUMBER_OF_TOKENS_IN_RESPONSE, contextData, message));
		put(values, "NUMBER_OF_CACHE_READ_TOKENS",
				integer(SemossLogUtils.NUMBER_OF_CACHE_READ_TOKENS, contextData, message));
		put(values, "NUMBER_OF_CACHE_CREATION_TOKENS",
				integer(SemossLogUtils.NUMBER_OF_CACHE_CREATION_TOKENS, contextData, message));
		put(values, "REQUEST_START_TIME", timestamp(SemossLogUtils.REQUEST_START_TIME, contextData, message));
		put(values, "RESPONSE_END_TIME", timestamp(SemossLogUtils.RESPONSE_END_TIME, contextData, message));
		put(values, "LOG_LEVEL", event.getLevel().toString());
		put(values, "LOG_TIMESTAMP", new Timestamp(event.getTimeMillis()));
		put(values, "LOGGER_NAME", event.getLoggerName());
		put(values, "LOGGER_LOCATION", SemossLogUtils.appendSourceInfo(event));
		return values;
	}

	@Override
	protected Object[] unstorableEventValues(LogEvent event, String note) {
		Object[] values = newValues();
		put(values, "MESSAGE", note);
		put(values, "LOG_LEVEL", event.getLevel() == null ? null : event.getLevel().toString());
		put(values, "LOG_TIMESTAMP", new Timestamp(event.getTimeMillis()));
		put(values, "LOGGER_NAME", event.getLoggerName());
		return values;
	}

	/**
	 * The map an engine call was logged as, or null for any other message.
	 */
	private static Map<?, ?> loggedMap(Message message) {
		if (message instanceof ObjectMessage objectMessage && objectMessage.getParameter() instanceof Map<?, ?> map) {
			return map;
		}
		if (message instanceof MapMessage<?, ?> mapMessage) {
			return mapMessage.getData();
		}
		return null;
	}

	/**
	 * A value from the logging context, or else from the logged map.
	 */
	private static String text(String key, ReadOnlyStringMap contextData, Map<?, ?> message) {
		String value = contextData.getValue(key);
		if (value != null) {
			return value;
		}
		Object messageValue = message == null ? null : message.get(key);
		return messageValue == null ? null : messageValue.toString();
	}

	private static Boolean success(ReadOnlyStringMap contextData, Map<?, ?> message) {
		String value = text(SemossLogUtils.IS_SUCCESS, contextData, message);
		// a call that does not say how it went is stored as a success
		return value == null || Boolean.parseBoolean(value);
	}

	private static Integer integer(String key, ReadOnlyStringMap contextData, Map<?, ?> message) {
		Object messageValue = message == null ? null : message.get(key);
		if (contextData.getValue(key) == null && messageValue instanceof Number number) {
			return number.intValue();
		}
		String value = text(key, contextData, message);
		if (value == null) {
			return null;
		}
		try {
			return (int) Double.parseDouble(value);
		} catch (NumberFormatException e) {
			LOGGER.error("Ignoring the value '{}' of {}, which is not a number", value, key);
			return null;
		}
	}

	/**
	 * A time logged as a {@link ZonedDateTime} or as ISO zoned date-time text. A
	 * value that cannot be read is stored as no time rather than losing the row.
	 */
	private static Timestamp timestamp(String key, ReadOnlyStringMap contextData, Map<?, ?> message) {
		Object messageValue = message == null ? null : message.get(key);
		if (contextData.getValue(key) == null && messageValue instanceof ZonedDateTime zonedDateTime) {
			return Timestamp.from(zonedDateTime.toInstant());
		}
		String value = text(key, contextData, message);
		if (value == null) {
			return null;
		}
		try {
			return Timestamp.from(ZonedDateTime.parse(value, DateTimeFormatter.ISO_ZONED_DATE_TIME).toInstant());
		} catch (RuntimeException e) {
			LOGGER.error("Ignoring the value '{}' of {}, which is not an ISO zoned date-time", value, key);
			return null;
		}
	}

	@PluginFactory
	public static AuditLogsJDBCAppender createAppender(@PluginAttribute("name") String name,
			@PluginElement("Filter") Filter filter, @PluginElement("Layout") Layout<String> layout,
			@PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) boolean ignoreExceptions,
			@PluginAttribute(value = "engineId", defaultString = Constants.AUDIT_LOGS_DB) String engineId,
			@PluginAttribute(value = "batchSize", defaultInt = DEFAULT_BATCH_SIZE) int batchSize,
			@PluginAttribute(value = "flushIntervalMs", defaultLong = DEFAULT_FLUSH_INTERVAL_MS) long flushIntervalMs,
			@PluginAttribute(value = "maxQueuedRows", defaultInt = DEFAULT_MAX_QUEUED_ROWS) int maxQueuedRows,
			@PluginAttribute(value = "maxQueuedMb", defaultInt = DEFAULT_MAX_QUEUED_MB) int maxQueuedMb) {

		if (name == null) {
			LOGGER.error("No name provided for AuditLogsJDBCAppender");
			return null;
		}

		return new AuditLogsJDBCAppender(name, filter, layout, ignoreExceptions, engineId, batchSize, flushIntervalMs,
				maxQueuedRows, maxQueuedMb);
	}
}
