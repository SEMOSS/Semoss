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
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

@Plugin(name = "ServerLogsJDBCAppender", category = "Core", elementType = "appender", printObject = true)
public class ServerLogsJDBCAppender extends AbstractAppender {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	private final String INSERT_SQL;
	private final List<LogEvent> events = new ArrayList<>();
	private int batchSize = 100;
	private static final long FLUSH_INTERVAL_MS = 60_000; // 1 minute
	private final AtomicLong lastAppendTime = new AtomicLong(System.currentTimeMillis());
	private final ScheduledExecutorService SCHEDULER;

	protected ServerLogsJDBCAppender(String name, Filter filter, Layout<? extends Serializable> layout,
			boolean ignoreExceptions, int batchSize) {
		super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
		if (batchSize > 0) {
			this.batchSize = batchSize;
		}
		this.INSERT_SQL = """
				INSERT INTO SERVER_LOGS (
					LOG_ID, REQUEST_ID, SESSION_ID, USER_ID, USER_TYPE, LEVEL, LOGGER_NAME,
					LOGGER_LOCATION, THREAD_NAME, LOG_TIMESTAMP, MESSAGE
				) VALUES (
					?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
				);
				""";

		this.SCHEDULER = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "ServerLogsJDBCAppender-Scheduler");
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

		IRDBMSEngine auditLogs = SystemEngineRegistry.getAuditLogsDb();
		if (auditLogs == null) {
			LOGGER.warn("Audit logs database has not been initialized yet");
			return;
		}
		AbstractSqlQueryUtil queryUtil = auditLogs.getQueryUtil();

		Connection connection = null;
		PreparedStatement stmt = null;
		try {
			connection = auditLogs.getConnection();
			stmt = connection.prepareStatement(this.INSERT_SQL);
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
				// user type
				stmt.setString(paramIdx++, contextData.getValue(SemossLogUtils.USER_TYPE));
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