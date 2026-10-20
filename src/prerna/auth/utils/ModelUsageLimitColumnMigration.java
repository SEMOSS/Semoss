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
package prerna.auth.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.ValueUtils;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Moves the model usage limits a security database keeps in four limit columns
 * into MODELUSAGELIMIT at start up, then drops the columns: a person's limit on
 * a model from ENGINEPERMISSION, and their all-models default from SMSS_USER.
 *
 * <p>
 * Several servers can start on one security database at the same time, so the
 * copy is safe to run at once and to run again:
 * </p>
 * <ul>
 * <li>Each copy is one transaction that starts by locking the rows it copies
 * from, with an update that changes nothing. Another server's copy waits on
 * those rows until the first commits, then sees what it wrote.</li>
 * <li>Each copy first clears the limits copied before that nobody has changed
 * since ({@code MODIFIEDBYID} is {@link #COPIED_BY_ID}), then copies again. A
 * limit someone has set or changed is kept, and so is its source's place in
 * MODELUSAGELIMIT. Running the copy twice leaves the same limits as once.</li>
 * <li>A copy that fails, such as on a lock wait timeout or a deadlock with
 * another server, is tried again. A table whose limit columns another server
 * dropped meanwhile is done.</li>
 * <li>The columns are dropped after the copy commits, the restriction column
 * last, each read again right before it is dropped; a column already gone is
 * done.</li>
 * </ul>
 *
 * @deprecated Only security databases from before MODELUSAGELIMIT have the four
 *             limit columns. Remove this class, and its call in
 *             {@link AbstractSecurityUtils}, once every security database has
 *             started on a release that moves them.
 */
@Deprecated
final class ModelUsageLimitColumnMigration {

	private static final Logger classLogger = LogManager.getLogger(ModelUsageLimitColumnMigration.class);

	/**
	 * The limit columns on ENGINEPERMISSION, for a person's limit on a model: the
	 * restriction, the frequency, the token amount and the compute time amount
	 */
	@Deprecated
	static final String[] ENGINE_LIMIT_COLUMNS = { "USAGERESTRICTION", "USAGEFREQUENCY", "MAXTOKENS",
			"MAXRESPONSETIME" };
	/**
	 * The limit columns on SMSS_USER, for a person's limit across all models, in
	 * the same order
	 */
	@Deprecated
	static final String[] USER_LIMIT_COLUMNS = { "MODELUSAGERESTRICTION", "MODELUSAGEFREQUENCY", "MODELMAXTOKENS",
			"MODELMAXRESPONSETIME" };
	/** The restriction column's value for a limit on tokens, in any case */
	@Deprecated
	static final String TOKEN_RESTRICTION = "token";
	/** The restriction column's value for a limit on compute time, in any case */
	@Deprecated
	static final String COMPUTE_RESTRICTION = "compute";
	/**
	 * Who the copied limits are added and changed by, so a copy can tell its own
	 */
	@Deprecated
	static final String COPIED_BY_ID = "MODELUSAGELIMIT_COPY";
	private static final Pair<String, String> COPIED_BY = Pair.with(COPIED_BY_ID, null);
	/**
	 * The people's model usage limits in MODELUSAGELIMIT, on a model or across all
	 * models
	 */
	private static final String SELECT_PERSONAL_LIMIT_KEYS = "SELECT ENGINEID, SUBJECTID, SUBJECTTYPE "
			+ "FROM MODELUSAGELIMIT WHERE LIMITTYPE = ? AND ENGINEID IS ";
	/**
	 * The copied limits nobody has changed since, on a model or across all models
	 */
	private static final String COPIED_LIMITS = "LIMITTYPE = ? AND MODIFIEDBYID = ? AND ENGINEID IS ";
	/** How many times a copy is tried */
	private static final int COPY_ATTEMPTS = 3;
	/** How long to wait before trying a copy again, in milliseconds */
	private static final long COPY_RETRY_WAIT_MS = 2000;

	private ModelUsageLimitColumnMigration() {

	}

	/**
	 * Moves the limits in a table's four limit columns into MODELUSAGELIMIT and
	 * drops the columns, when the table still has them.
	 *
	 * @param securityDb          the security database
	 * @param conn                a connection to read the table's columns with
	 * @param table               ENGINEPERMISSION or SMSS_USER
	 * @param columns             the table's columns
	 * @param allowIfExistsIndexs whether the database has if exists index syntax
	 * @param database            the database name
	 * @param schema              the schema name
	 * @throws Exception when a column that is still there cannot be dropped
	 */
	@Deprecated
	static void move(IRDBMSEngine securityDb, Connection conn, String table, List<String> columns,
			boolean allowIfExistsIndexs, String database, String schema) throws Exception {
		if (copyLimits(securityDb, table, columns)) {
			dropLimitColumns(securityDb, securityDb.getQueryUtil(), conn, table, limitColumnsOf(table),
					allowIfExistsIndexs, database, schema);
		}
	}

	/**
	 * Copies the limits in a table's four limit columns into MODELUSAGELIMIT, when
	 * the table still has them, and keeps the columns.
	 *
	 * @param securityDb the security database
	 * @param table      ENGINEPERMISSION or SMSS_USER
	 * @param columns    the table's columns
	 * @return whether the limit columns can be dropped
	 */
	@Deprecated
	static boolean copyLimits(IRDBMSEngine securityDb, String table, List<String> columns) {
		if ("ENGINEPERMISSION".equals(table)) {
			// a person's limit on a model is copied with no login type, since a grant keeps
			// only the user id; setLimit gives it the one login type SMSS_USER has for the
			// user id, and start up fills in the rest it can
			return copyLimits(securityDb, table, columns, ENGINE_LIMIT_COLUMNS, "ENGINEID", "USERID",
					(engineId, userId) -> ModelUsageLimitKey.direct(engineId, userId, null), true);
		}
		return copyLimits(securityDb, table, columns, limitColumnsOf(table), "ID", "TYPE",
				ModelUsageLimitKey::allModels, false);
	}

	/**
	 * The four limit columns of ENGINEPERMISSION or SMSS_USER, for copying and
	 * dropping them.
	 *
	 * @param table ENGINEPERMISSION or SMSS_USER
	 * @return its limit columns, the restriction first
	 */
	private static String[] limitColumnsOf(String table) {
		if ("ENGINEPERMISSION".equals(table)) {
			return ENGINE_LIMIT_COLUMNS;
		}
		if ("SMSS_USER".equals(table)) {
			return USER_LIMIT_COLUMNS;
		}
		throw new IllegalArgumentException(table + " has no model usage limit columns");
	}

	/**
	 * Copies the model usage limits kept in a table's four limit columns into
	 * MODELUSAGELIMIT, in one transaction: one limit with one rule for each row
	 * that has a restriction. It runs only while the restriction column exists.
	 *
	 * <p>
	 * The transaction locks the rows it copies from, clears the limits copied
	 * before that nobody has changed since, and copies. The values are read with
	 * {@link #readLimit}: ALL_TIME is a year, and an amount of 0 or none is no
	 * limit defined. Rows that give no limit are logged and skipped, and so are
	 * rows that do not say whose limit it is and rows whose source has a limit
	 * someone set in MODELUSAGELIMIT, which keeps it (a person's grant, on any
	 * login type).
	 * </p>
	 *
	 * @param securityDb          the security database
	 * @param table               ENGINEPERMISSION or SMSS_USER
	 * @param allCols             the table's columns
	 * @param oldColumns          the restriction, frequency, token amount and
	 *                            compute time amount columns
	 * @param firstSubjectColumn  the first column that says whose limit a row holds
	 * @param secondSubjectColumn the second one
	 * @param keyOf               which limit a row holds, from the values of those
	 *                            two columns
	 * @param onModels            whether the limits are on a model, or else across
	 *                            all models
	 * @return whether the limit columns can be dropped: the limits were copied,
	 *         there was nothing to copy, or another server starting at the same
	 *         time copied them and dropped the restriction column
	 */
	private static boolean copyLimits(IRDBMSEngine securityDb, String table, List<String> allCols, String[] oldColumns,
			String firstSubjectColumn, String secondSubjectColumn, BiFunction<String, String, ModelUsageLimitKey> keyOf,
			boolean onModels) {
		if (!hasColumn(allCols, oldColumns[0])) {
			return true;
		}
		for (int attempt = 1;; attempt++) {
			try {
				int[] counts = QueryExecutionUtility.write(securityDb, connection -> copyLimits(connection, table,
						allCols, oldColumns, firstSubjectColumn, secondSubjectColumn, keyOf, onModels));
				SecurityModelUsageLimitUtils.markChanged();
				classLogger.info("Copied {} model usage limits from {} and skipped {} rows", counts[0], table,
						counts[1]);
				return true;
			} catch (Exception e) {
				if (!columnExists(securityDb, table, oldColumns[0])) {
					classLogger.info(
							"The model usage limits of {} were copied by another server starting at the same " + "time",
							table);
					return true;
				}
				if (attempt >= COPY_ATTEMPTS) {
					classLogger.error("Unable to copy the model usage limits from " + table
							+ ". Its limit columns are kept, and the copy runs again on the next start.", e);
					return false;
				}
				classLogger.warn("Unable to copy the model usage limits from {}, such as while another server "
						+ "starting at the same time copies them. Trying again.", table, e);
				try {
					Thread.sleep(COPY_RETRY_WAIT_MS);
				} catch (InterruptedException interrupted) {
					Thread.currentThread().interrupt();
					return false;
				}
			}
		}
	}

	/**
	 * The copy, in the caller's transaction
	 *
	 * @return how many limits were copied and how many rows skipped
	 */
	private static int[] copyLimits(Connection connection, String table, List<String> allCols, String[] oldColumns,
			String firstSubjectColumn, String secondSubjectColumn, BiFunction<String, String, ModelUsageLimitKey> keyOf,
			boolean onModels) throws Exception {
		String restriction = oldColumns[0];
		String userKeyType = ModelUsageLimitKey.Type.USER.name();
		String engineIs = onModels ? "NOT NULL" : "NULL";

		// 1. lock the rows to copy from, so another server's copy waits for this one
		QueryExecutionUtility.executeUpdate(connection, "UPDATE " + table + " SET " + restriction + " = " + restriction
				+ " WHERE " + restriction + " IS NOT NULL", ps -> {
				});

		// 2. clear the limits copied before that nobody has changed since
		String copied = COPIED_LIMITS + engineIs;
		QueryExecutionUtility.executeUpdate(connection,
				"DELETE FROM MODELUSAGELIMITRULE WHERE LIMITID IN (SELECT ID FROM MODELUSAGELIMIT WHERE " + copied
						+ ")",
				ps -> {
					ps.setString(1, userKeyType);
					ps.setString(2, COPIED_BY_ID);
				});
		QueryExecutionUtility.executeUpdate(connection, "DELETE FROM MODELUSAGELIMIT WHERE " + copied, ps -> {
			ps.setString(1, userKeyType);
			ps.setString(2, COPIED_BY_ID);
		});

		// 3. copy each row whose source has no limit someone set
		List<ModelUsageLimitKey> existing = QueryExecutionUtility.queryList(connection,
				SELECT_PERSONAL_LIMIT_KEYS + engineIs, ps -> ps.setString(1, userKeyType),
				rs -> new ModelUsageLimitKey(ModelUsageLimitKey.Type.USER, rs.getString(1), rs.getString(2),
						rs.getString(3)));
		// a grant's limit is the same whatever login type it is kept with
		Set<ModelUsageLimitKey> defined = new HashSet<>();
		for (ModelUsageLimitKey key : existing) {
			defined.add(key.isDirect() ? key.withoutSubjectType() : key);
		}
		StringBuilder select = new StringBuilder("SELECT ").append(firstSubjectColumn).append(", ")
				.append(secondSubjectColumn);
		for (String column : oldColumns) {
			select.append(", ").append(hasColumn(allCols, column) ? column : "NULL");
		}
		select.append(" FROM ").append(table).append(" WHERE ").append(restriction).append(" IS NOT NULL");
		List<Object[]> rows = QueryExecutionUtility.queryList(connection, select.toString(), ps -> {
		}, rs -> new Object[] { rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getObject(5),
				rs.getObject(6) });
		int copiedCount = 0;
		int skipped = 0;
		for (Object[] row : rows) {
			ModelUsageLimitKey key = keyOf.apply((String) row[0], (String) row[1]);
			if (row[0] == null || key.subjectId() == null) {
				classLogger.warn("Skipped a model usage limit in {} that does not say whose it is", table);
				skipped++;
				continue;
			}
			String frequency = (String) row[3];
			ModelUsageLimit limit;
			try {
				limit = readLimit((String) row[2], frequency, ValueUtils.toNumberOrNull(row[4]),
						ValueUtils.toNumberOrNull(row[5]));
			} catch (IllegalArgumentException e) {
				classLogger.warn("Skipped the model usage limit of {} in {}: {}", key, table, e.getMessage());
				skipped++;
				continue;
			}
			if (limit == null) {
				classLogger.info("Skipped the model usage limit of {} in {}: it has no restriction or amount, so no "
						+ "limit is defined", key, table);
				skipped++;
				continue;
			}
			if (!defined.add(key.isDirect() ? key.withoutSubjectType() : key)) {
				classLogger.info("Skipped the model usage limit of {} in {}: it already has one", key, table);
				skipped++;
				continue;
			}
			if (frequency != null && "ALL_TIME".equalsIgnoreCase(frequency.trim())) {
				classLogger.info("The all time model usage limit of {} in {} is copied as a yearly limit", key, table);
			}
			SecurityModelUsageLimitUtils.setLimit(connection, key, limit, COPIED_BY);
			copiedCount++;
		}
		return new int[] { copiedCount, skipped };
	}

	/**
	 * Reads a row's four limit columns as one rule for one person.
	 *
	 * @param restriction     {@code token} or {@code compute}, any case, or blank
	 *                        for none
	 * @param frequency       {@code DAY}, {@code WEEK}, {@code MONTH}, {@code YEAR}
	 *                        or {@code ALL_TIME}; blank and unknown values are a
	 *                        day, and all time is a year
	 * @param maxTokens       the token amount
	 * @param maxResponseTime the compute time amount, in milliseconds
	 * @return the limit, or null for no limit defined, which is also what an amount
	 *         of 0 or none means
	 * @throws IllegalArgumentException when the restriction is neither
	 *                                  {@code token} nor {@code compute}
	 */
	@Deprecated
	static ModelUsageLimit readLimit(String restriction, String frequency, Number maxTokens, Number maxResponseTime) {
		if (restriction == null || restriction.isBlank()) {
			return null;
		}
		Metric metric;
		Number amount;
		if (TOKEN_RESTRICTION.equalsIgnoreCase(restriction.trim())) {
			metric = Metric.TOKENS;
			amount = maxTokens;
		} else if (COMPUTE_RESTRICTION.equalsIgnoreCase(restriction.trim())) {
			metric = Metric.COMPUTE_TIME;
			amount = maxResponseTime;
		} else {
			throw new IllegalArgumentException("Unknown usage restriction '" + restriction + "'. Use '"
					+ TOKEN_RESTRICTION + "' or '" + COMPUTE_RESTRICTION + "'");
		}
		if (amount == null || Math.round(amount.doubleValue()) <= 0) {
			return null;
		}
		ModelUsageLimitRule rule = new ModelUsageLimitRule(metric, Math.round(amount.doubleValue()),
				windowOf(frequency), Counts.EACH_PERSON);
		return ModelUsageLimit.of(List.of(rule));
	}

	/**
	 * Reads a frequency column's value as the window of a copied rule.
	 *
	 * @param frequency a frequency column's value
	 * @return its window: blank and unknown values are a day, and all time is a
	 *         year
	 */
	@Deprecated
	static Window windowOf(String frequency) {
		if (frequency == null || frequency.isBlank()) {
			return Window.DAY;
		}
		switch (frequency.trim().toUpperCase(Locale.ROOT)) {
		case "WEEK":
			return Window.WEEK;
		case "MONTH":
			return Window.MONTH;
		case "YEAR":
		case "ALL_TIME":
			return Window.YEAR;
		default:
			return Window.DAY;
		}
	}

	/**
	 * Reads whether a table has a column now, for a start up that another server
	 * may be running at the same time.
	 *
	 * @param securityDb the security database
	 * @param table      the table
	 * @param column     the column
	 * @return whether the table has the column; true when its columns cannot be
	 *         read, so nothing is taken as done that may not be
	 */
	private static boolean columnExists(IRDBMSEngine securityDb, String table, String column) {
		AbstractSqlQueryUtil queryUtil = securityDb.getQueryUtil();
		Connection conn = null;
		try {
			conn = securityDb.getConnection();
			List<String> columns = queryUtil.getTableColumns(conn, table, securityDb.getDatabase(),
					securityDb.getSchema());
			return columns.isEmpty() || hasColumn(columns, column);
		} catch (Exception e) {
			classLogger.warn("Unable to read the columns of {}", table, e);
			return true;
		} finally {
			if (conn != null && securityDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.warn("Unable to close the connection", e);
				}
			}
		}
	}

	/**
	 * Drops a table's model usage limit columns, and the index on any of them. The
	 * restriction column goes last, so the limits are copied again if the other
	 * columns cannot be dropped. Each column is read again right before it is
	 * dropped, and one that is gone, such as one another server starting at the
	 * same time dropped, is done already.
	 *
	 * @param securityDb          the security database
	 * @param queryUtil           its query utility
	 * @param conn                a connection to read the table's columns with
	 * @param table               ENGINEPERMISSION or SMSS_USER
	 * @param limitColumns        the limit columns, the restriction first
	 * @param allowIfExistsIndexs whether the database has if exists index syntax
	 * @param database            the database name
	 * @param schema              the schema name
	 * @throws Exception when a column that is still there cannot be dropped
	 */
	private static void dropLimitColumns(IRDBMSEngine securityDb, AbstractSqlQueryUtil queryUtil, Connection conn,
			String table, String[] limitColumns, boolean allowIfExistsIndexs, String database, String schema)
			throws Exception {
		List<String> dropOrder = new ArrayList<>(Arrays.asList(limitColumns).subList(1, limitColumns.length));
		dropOrder.add(limitColumns[0]);
		for (String obsoleteColumn : dropOrder) {
			// columns that cannot be read are taken to be there
			List<String> columns = queryUtil.getTableColumns(conn, table, database, schema);
			if (!columns.isEmpty() && !hasColumn(columns, obsoleteColumn)) {
				continue;
			}
			String indexName = table + "_" + obsoleteColumn + "_INDEX";
			try {
				if (allowIfExistsIndexs) {
					String dropIndexSql = queryUtil.dropIndexIfExists(indexName, table);
					classLogger.info("Running sql {}", dropIndexSql);
					securityDb.removeData(dropIndexSql);
				} else if (queryUtil.indexExists(securityDb, indexName, table, database, schema)) {
					String dropIndexSql = queryUtil.dropIndex(indexName, table);
					classLogger.info("Running sql {}", dropIndexSql);
					securityDb.removeData(dropIndexSql);
				}
			} catch (Exception e) {
				if (allowIfExistsIndexs || queryUtil.indexExists(securityDb, indexName, table, database, schema)) {
					throw e;
				}
				classLogger.info("Index {} was dropped already", indexName);
			}
			String dropColumnSql = queryUtil.alterTableDropColumn(table, obsoleteColumn);
			classLogger.info("Running sql {}", dropColumnSql);
			try {
				securityDb.insertData(dropColumnSql);
			} catch (Exception e) {
				if (columnExists(securityDb, table, obsoleteColumn)) {
					throw e;
				}
				classLogger.info("Column {} of {} was dropped already", obsoleteColumn, table);
			}
		}
	}

	/**
	 * Whether a column is among a table's columns, for checking which limit columns
	 * a table still has.
	 *
	 * @param allCols a table's columns
	 * @param column  a column name
	 * @return whether the table has the column, in any case
	 */
	private static boolean hasColumn(List<String> allCols, String column) {
		return allCols.stream().anyMatch(column::equalsIgnoreCase);
	}
}
