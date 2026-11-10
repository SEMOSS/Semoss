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
import java.util.List;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.util.ReadOnlyStringMap;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.util.SystemEngineRegistry;

/**
 * Stores server log lines in the {@code SERVER_LOGS} table of the AuditLogs
 * database. Rows are queued and written in the background by
 * {@link AbstractBatchingJdbcAppender}, so a slow or unreachable database never
 * makes logging wait.
 */
@Plugin(name = "ServerLogsJDBCAppender", category = "Core", elementType = "appender", printObject = true)
public class ServerLogsJDBCAppender extends AbstractBatchingJdbcAppender {

	/** The most rows that wait when the configuration does not say */
	private static final int DEFAULT_MAX_QUEUED_ROWS = 20_000;
	/**
	 * The most text, in megabytes, the waiting rows hold when the configuration
	 * does not say
	 */
	private static final int DEFAULT_MAX_QUEUED_MB = 32;
	private static final int TEXT_LENGTH = 255;

	private static final List<Column> COLUMNS = List.of(Column.text("LOG_ID", TEXT_LENGTH),
			Column.text("REQUEST_ID", TEXT_LENGTH), Column.text("SESSION_ID", TEXT_LENGTH),
			Column.text("USER_ID", TEXT_LENGTH), Column.text("USER_TYPE", TEXT_LENGTH), Column.text("LEVEL", 50),
			Column.text("LOGGER_NAME", TEXT_LENGTH), Column.text("LOGGER_LOCATION", TEXT_LENGTH),
			Column.text("THREAD_NAME", TEXT_LENGTH), Column.of("LOG_TIMESTAMP", ColumnType.TIMESTAMP),
			Column.of("MESSAGE", ColumnType.CLOB));

	protected ServerLogsJDBCAppender(String name, Filter filter, Layout<? extends Serializable> layout,
			boolean ignoreExceptions, int batchSize, long flushIntervalMs, int maxQueuedRows, int maxQueuedMb) {
		super(name, filter, layout, ignoreExceptions, "SERVER_LOGS", COLUMNS, batchSize, flushIntervalMs, maxQueuedRows,
				maxQueuedMb);
	}

	@Override
	protected IRDBMSEngine targetDatabase() {
		return AuditLogsDbUtils.isInitalized() ? SystemEngineRegistry.getAuditLogsDb() : null;
	}

	@Override
	protected Object[] columnValues(LogEvent event) {
		ReadOnlyStringMap contextData = event.getContextData();
		Object[] values = newValues();
		put(values, "REQUEST_ID", contextData.getValue(SemossLogUtils.REQUEST_ID));
		put(values, "SESSION_ID", contextData.getValue(SemossLogUtils.SESSION_ID));
		put(values, "USER_ID", contextData.getValue(SemossLogUtils.USER_ID));
		put(values, "USER_TYPE", contextData.getValue(SemossLogUtils.USER_TYPE));
		put(values, "LEVEL", event.getLevel().toString());
		put(values, "LOGGER_NAME", event.getLoggerName());
		put(values, "LOGGER_LOCATION", SemossLogUtils.appendSourceInfo(event));
		put(values, "THREAD_NAME", event.getThreadName());
		put(values, "LOG_TIMESTAMP", new Timestamp(event.getTimeMillis()));
		put(values, "MESSAGE", event.getMessage().getFormattedMessage());
		return values;
	}

	@Override
	protected Object[] unstorableEventValues(LogEvent event, String note) {
		Object[] values = newValues();
		put(values, "LEVEL", event.getLevel() == null ? null : event.getLevel().toString());
		put(values, "LOGGER_NAME", event.getLoggerName());
		put(values, "THREAD_NAME", event.getThreadName());
		put(values, "LOG_TIMESTAMP", new Timestamp(event.getTimeMillis()));
		put(values, "MESSAGE", note);
		return values;
	}

	@PluginFactory
	public static ServerLogsJDBCAppender createAppender(@PluginAttribute("name") String name,
			@PluginElement("Filter") Filter filter, @PluginElement("Layout") Layout<? extends Serializable> layout,
			@PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) boolean ignoreExceptions,
			@PluginAttribute(value = "batchSize", defaultInt = DEFAULT_BATCH_SIZE) int batchSize,
			@PluginAttribute(value = "flushIntervalMs", defaultLong = DEFAULT_FLUSH_INTERVAL_MS) long flushIntervalMs,
			@PluginAttribute(value = "maxQueuedRows", defaultInt = DEFAULT_MAX_QUEUED_ROWS) int maxQueuedRows,
			@PluginAttribute(value = "maxQueuedMb", defaultInt = DEFAULT_MAX_QUEUED_MB) int maxQueuedMb) {

		if (name == null) {
			LOGGER.error("No name provided for ServerLogsJDBCAppender");
			return null;
		}

		return new ServerLogsJDBCAppender(name, filter, layout, ignoreExceptions, batchSize, flushIntervalMs,
				maxQueuedRows, maxQueuedMb);
	}
}
