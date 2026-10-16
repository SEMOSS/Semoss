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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.logging.log4j.util.ReadOnlyStringMap;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.cluster.sync.IClusterSynchronizer;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Writes the security audit trail (logins, permission/role/group changes,
 * admin actions - see {@link SemossLogUtils#logSecurityAuditEvent}) to
 * SECURITY_AUDIT_LOGS, separate from {@link AuditLogsJDBCAppender}'s
 * LLM-pipeline-shaped AUDIT_LOGS table.
 *
 * <p>
 * Each row carries a SHA-256 hash of its own fields plus the previous row's
 * hash, scoped per writer node (NODE_ID = {@link IClusterSynchronizer#CONTAINER_IP}):
 * altering, deleting or reordering a row breaks the chain for that node from
 * that point forward, detectable with
 * {@link AuditLogsDbUtils#verifySecurityAuditLogChain(String)}. This is a
 * per-node chain, not a single cluster-wide ledger - verifying cross-node
 * ordering or coordinating writers across nodes would need external
 * infrastructure (a distributed log/consensus system), which is out of scope
 * for an in-process appender.
 * </p>
 */
@Plugin(name = "SecurityAuditLogsJDBCAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class SecurityAuditLogsJDBCAppender extends AbstractAppender {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	/** Hash chain seed for a node's first-ever row. */
	private static final String GENESIS_HASH = "0".repeat(64);

	private static final String NODE_ID = IClusterSynchronizer.CONTAINER_IP;

	private final String INSERT_SQL;
	private final List<LogEvent> events = new ArrayList<>();
	private int batchSize = 100;
	private static final long FLUSH_INTERVAL_MS = 60_000; // 1 minute
	private static final long PURGE_INTERVAL_MS = TimeUnit.HOURS.toMillis(24);
	private final AtomicLong lastAppendTime = new AtomicLong(System.currentTimeMillis());
	private final ScheduledExecutorService SCHEDULER;

	/** This node's running hash-chain tip; null until lazily loaded from the DB. */
	private final AtomicReference<String> chainTip = new AtomicReference<>(null);

	protected SecurityAuditLogsJDBCAppender(String name, Filter filter, Layout<? extends Serializable> layout,
			boolean ignoreExceptions, int batchSize) {
		super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
		if (batchSize > 0) {
			this.batchSize = batchSize;
		}
		this.INSERT_SQL = """
				INSERT INTO SECURITY_AUDIT_LOGS (
				    LOG_ID, NODE_ID, PREV_HASH, ROW_HASH, EVENT_TYPE, IS_SUCCESS,
				    ACTOR_USER_ID, ACTOR_USER_NAME, ACTOR_IP, TARGET_TYPE, TARGET_ID, TARGET_NAME,
				    SESSION_ID, REQUEST_ID, MESSAGE, DETAILS,
				    LOG_LEVEL, LOG_TIMESTAMP, LOGGER_NAME, LOGGER_LOCATION
				) VALUES (
				    ?, ?, ?, ?, ?, ?,
				    ?, ?, ?, ?, ?, ?,
				    ?, ?, ?, ?,
				    ?, ?, ?, ?
				);
				""";

		this.SCHEDULER = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "SecurityAuditLogsJDBCAppender-Scheduler");
			t.setDaemon(true);
			return t;
		});

		this.SCHEDULER.scheduleAtFixedRate(this::flushIfIdle, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS,
				TimeUnit.MILLISECONDS);
		// Retention is opt-in (Utility.getSecurityAuditLogsRetentionDays() defaults
		// to 0 / never); purgeExpiredSecurityAuditLogs() itself is a no-op until a
		// site configures a policy, so scheduling this unconditionally is harmless.
		this.SCHEDULER.scheduleAtFixedRate(AuditLogsDbUtils::purgeExpiredSecurityAuditLogs, PURGE_INTERVAL_MS,
				PURGE_INTERVAL_MS, TimeUnit.MILLISECONDS);
	}

	@Override
	public void append(LogEvent event) {
		if (!Utility.isSecurityAuditLogsEnabled()) {
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
		if (!Utility.isSecurityAuditLogsEnabled()) {
			return;
		}
		if (System.currentTimeMillis() - lastAppendTime.get() >= FLUSH_INTERVAL_MS) {
			flush();
		}
	}

	private void flush() {
		if (!Utility.isSecurityAuditLogsEnabled()) {
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
			String prevHash = currentChainTip(connection);

			stmt = connection.prepareStatement(INSERT_SQL);
			for (LogEvent event : processingEvents) {
				ReadOnlyStringMap contextData = event.getContextData();

				String logId = GUID.v7().toUUID().toString();
				String eventType = contextData.getValue(SemossLogUtils.EVENT_TYPE);
				boolean isSuccess = Boolean.parseBoolean(contextData.getValue(SemossLogUtils.IS_SUCCESS));
				String actorUserId = contextData.getValue(SemossLogUtils.ACTOR_USER_ID);
				String targetType = contextData.getValue(SemossLogUtils.TARGET_TYPE);
				String targetId = contextData.getValue(SemossLogUtils.TARGET_ID);
				Timestamp logTimestamp = new Timestamp(event.getTimeMillis());

				// epoch millis, not logTimestamp.toString() - LOG_TIMESTAMP reads back from
				// the database as a SemossDate (see AuditLogsDbUtils.verifySecurityAuditLogChain),
				// whose string form does not match java.sql.Timestamp's; millis round-trips
				// identically through both representations
				String rowHash = computeRowHash(prevHash, logId, eventType, Boolean.toString(isSuccess), actorUserId,
						targetType, targetId, Long.toString(logTimestamp.getTime()));

				int paramIdx = 1;
				stmt.setString(paramIdx++, logId); // log_id
				stmt.setString(paramIdx++, NODE_ID); // node_id
				stmt.setString(paramIdx++, prevHash); // prev_hash
				stmt.setString(paramIdx++, rowHash); // row_hash
				stmt.setString(paramIdx++, eventType); // event_type
				stmt.setBoolean(paramIdx++, isSuccess); // is_success
				stmt.setString(paramIdx++, actorUserId); // actor_user_id
				stmt.setString(paramIdx++, contextData.getValue(SemossLogUtils.ACTOR_USER_NAME)); // actor_user_name
				stmt.setString(paramIdx++, contextData.getValue(SemossLogUtils.ACTOR_IP)); // actor_ip
				stmt.setString(paramIdx++, targetType); // target_type
				stmt.setString(paramIdx++, targetId); // target_id
				stmt.setString(paramIdx++, contextData.getValue(SemossLogUtils.TARGET_NAME)); // target_name
				stmt.setString(paramIdx++, contextData.getValue(SemossLogUtils.SESSION_ID)); // session_id
				stmt.setString(paramIdx++, contextData.getValue(SemossLogUtils.REQUEST_ID)); // request_id
				queryUtil.handleInsertionOfClob(stmt, event.getMessage().getFormattedMessage(), paramIdx++, GSON); // message
				queryUtil.handleInsertionOfClob(stmt, contextData.getValue(SemossLogUtils.DETAILS), paramIdx++, GSON); // details
				stmt.setString(paramIdx++, event.getLevel().toString()); // log_level
				stmt.setTimestamp(paramIdx++, logTimestamp); // log_timestamp
				stmt.setString(paramIdx++, event.getLoggerName()); // logger_name
				stmt.setString(paramIdx++, SemossLogUtils.appendSourceInfo(event)); // logger_location

				stmt.addBatch();
				prevHash = rowHash;
			}

			stmt.executeBatch();
			connection.commit();
			chainTip.set(prevHash);
		} catch (Exception e) {
			LOGGER.error("Failed to insert security audit log into database", e);
			// Do not advance chainTip on failure - the next attempt must chain off the
			// last row that actually committed, not one that was rolled back.
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

	/**
	 * This node's current hash-chain tip: the in-memory value if already loaded,
	 * otherwise this node's most recent ROW_HASH from the database (so a restart
	 * continues the existing chain instead of silently starting a new one), or
	 * {@link #GENESIS_HASH} if this node has never written a row.
	 */
	private String currentChainTip(Connection connection) throws SQLException {
		String tip = chainTip.get();
		if (tip != null) {
			return tip;
		}
		String loaded = GENESIS_HASH;
		String selectSql = "SELECT ROW_HASH FROM SECURITY_AUDIT_LOGS WHERE NODE_ID = ? "
				+ "ORDER BY LOG_TIMESTAMP DESC";
		try (PreparedStatement stmt = connection.prepareStatement(selectSql)) {
			stmt.setString(1, NODE_ID);
			stmt.setMaxRows(1);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					loaded = rs.getString("ROW_HASH");
				}
			}
		}
		chainTip.set(loaded);
		return loaded;
	}

	/**
	 * Compute one row's hash from the previous row's hash and this row's own
	 * identifying fields. Used both when writing a row (here) and when verifying
	 * the chain ({@link AuditLogsDbUtils#verifySecurityAuditLogChain(String)}) -
	 * the two must stay in sync on field order and formatting, or every row will
	 * appear tampered. {@code logTimestamp} must be epoch milliseconds as a
	 * string (not a formatted date): the column reads back from the database as
	 * a different type than it's written as, and only the millis value round-trips
	 * identically through both.
	 */
	public static String computeRowHash(String prevHash, String logId, String eventType, String isSuccess,
			String actorUserId, String targetType, String targetId, String logTimestamp) {
		String canonical = String.join("|", nullToEmpty(prevHash), nullToEmpty(logId), nullToEmpty(eventType),
				nullToEmpty(isSuccess), nullToEmpty(actorUserId), nullToEmpty(targetType), nullToEmpty(targetId),
				nullToEmpty(logTimestamp));
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hash = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
			return HexFormat.of().formatHex(hash);
		} catch (NoSuchAlgorithmException e) {
			// SHA-256 is mandatory for every JDK provider (JCA standard algorithm name)
			throw new IllegalStateException("SHA-256 unavailable", e);
		}
	}

	private static String nullToEmpty(String s) {
		return s == null ? "" : s;
	}

	@Override
	public void stop() {
		if (!Utility.isSecurityAuditLogsEnabled()) {
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
	public static SecurityAuditLogsJDBCAppender createAppender(@PluginAttribute("name") String name,
			@PluginElement("Filter") Filter filter, @PluginElement("Layout") Layout<String> layout,
			@PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) boolean ignoreExceptions,
			@PluginAttribute(value = "batchSize", defaultInt = 100) int batchSize) {

		if (name == null) {
			LOGGER.error("No name provided for SecurityAuditLogsJDBCAppender");
			return null;
		}

		return new SecurityAuditLogsJDBCAppender(name, filter, layout, ignoreExceptions, batchSize);
	}

}
