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
package prerna.engine.logging;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.logging.LogActivityDto;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AuditLogsDbUtils {

	private static final Logger classLogger = LogManager.getLogger(AuditLogsDbUtils.class);

	static IRDBMSEngine auditLogsDb;
	static boolean initialized = false;

	private AuditLogsDbUtils() {

	}

	public static void loadAuditLogsDatabase() throws Exception {
		auditLogsDb = (IRDBMSEngine) Utility.getDatabase(Constants.AUDIT_LOGS_DB);
		initEngineAsAuditDatabase(auditLogsDb);
		initialized = true;
	}

	/**
	 * @param engine
	 * @param conn
	 * @param dbSchema
	 * @throws SQLException
	 */
	private static void executeInitDatabaseSchema(IRDBMSEngine engine, Connection conn,
			List<Pair<String, List<Pair<String, String>>>> dbSchema) throws SQLException {

		String database = engine.getDatabase();
		String schema = engine.getSchema();

		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();

		// ---------------------------
		// Partitioning-handling block (Postgres / MySQL / MSSQL)
		// ---------------------------
		try {
			String dbProduct = "";
			try {
				dbProduct = conn.getMetaData().getDatabaseProductName();
			} catch (SQLException e) {
				classLogger.warn("Could not determine DB product name: " + e.getMessage(), e);
			}
			if (dbProduct == null) {
				dbProduct = "";
			}
			String dbLower = dbProduct.toLowerCase();

			boolean isPostgres = dbLower.contains("postgresql");
			boolean isMySQL = dbLower.contains("mysql");
			boolean isMSSQL = dbLower.contains("microsoft sql") || dbLower.contains("sql server");
			boolean supportsPartitioning = isPostgres || isMySQL || isMSSQL;

			// Build AUDIT_LOGS column definition list from dbSchema (for table creation)
			String auditLogColDefs = null;
			for (Pair<String, List<Pair<String, String>>> tableSchema : dbSchema) {
				String tableName = tableSchema.getValue0();
				if (tableName != null && tableName.equalsIgnoreCase("AUDIT_LOGS")) {
					StringBuilder sb = new StringBuilder();
					List<Pair<String, String>> cols = tableSchema.getValue1();
					for (int i = 0; i < cols.size(); i++) {
						Pair<String, String> col = cols.get(i);
						sb.append(col.getValue0()).append(" ").append(col.getValue1());
						if (i < cols.size() - 1) {
							sb.append(", ");
						}
					}
					auditLogColDefs = sb.toString();
					break;
				}
			}

			// Check if AUDIT_LOGS exists
			boolean auditExists = false;
			try {
				auditExists = queryUtil.tableExists(engine, "AUDIT_LOGS", database, schema);
			} catch (Exception e) {
				classLogger.warn("Could not check AUDIT_LOGS existence: " + e.getMessage(), e);
			}

			if (!supportsPartitioning) {
				classLogger.info("DB (" + dbProduct
						+ ") does not support automatic partitioning in this logic. Falling back to standard table creation flow.");
			} else {
				// ---------- Fresh install: create partitioned AUDIT_LOGS if it doesn't exist
				// ----------
				if (!auditExists) {
					classLogger.info("AUDIT_LOGS does not exist. Creating partitioned AUDIT_LOGS for DB: " + dbProduct);

					if (auditLogColDefs == null) {
						classLogger.warn(
								"AUDIT_LOGS column definitions not found in dbSchema; skipping partition creation.");
					} else {
						// Common values for monthly boundary generation
						java.time.LocalDate start = java.time.LocalDate.now().withDayOfMonth(1);
						java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter
								.ofPattern("yyyy-MM-dd");

						if (isPostgres) {
							// Postgres: create parent partitioned table + default + monthly partitions
							String createParent = "CREATE TABLE IF NOT EXISTS AUDIT_LOGS (" + auditLogColDefs
									+ ") PARTITION BY RANGE (LOG_TIMESTAMP)";
							executeSql(conn, createParent);
							classLogger.info("Created partitioned parent table AUDIT_LOGS (Postgres).");

							// Default partition
							try {
								executeSql(conn,
										"CREATE TABLE IF NOT EXISTS AUDIT_LOGS_DEFAULT PARTITION OF AUDIT_LOGS DEFAULT");
								classLogger.info("Created AUDIT_LOGS_DEFAULT partition.");
							} catch (SQLException e) {
								classLogger.warn("Could not create AUDIT_LOGS_DEFAULT partition: " + e.getMessage(), e);
							}

							// next 12 months
							for (int i = 0; i < 12; i++) {
								java.time.LocalDate s = start.plusMonths(i);
								java.time.LocalDate e = s.plusMonths(1);
								String partName = String.format("AUDIT_LOGS_%d_%02d", s.getYear(), s.getMonthValue());
								String createPartition = String.format(
										"CREATE TABLE IF NOT EXISTS %s PARTITION OF AUDIT_LOGS FOR VALUES FROM ('%s') TO ('%s')",
										partName, s.format(fmt), e.format(fmt));
								try {
									executeSql(conn, createPartition);
									classLogger.info("Created partition " + partName);
									// per-partition index (best effort)
									executeSql(conn, String.format(
											"CREATE INDEX IF NOT EXISTS IDX_%s_PROJECT_TS ON %s (PROJECT_ID, LOG_TIMESTAMP)",
											partName, partName));
								} catch (SQLException ex) {
									classLogger.warn(
											"Partition creation failed for " + partName + ": " + ex.getMessage(), ex);
								}
							}
						} else if (isMySQL) {
							// MySQL: create partitioned table with default partition and add monthly
							// partitions
							String createMySQL = "CREATE TABLE IF NOT EXISTS AUDIT_LOGS (" + auditLogColDefs
									+ ") PARTITION BY RANGE (TO_DAYS(LOG_TIMESTAMP)) (PARTITION p_default VALUES LESS THAN (MAXVALUE))";
							try {
								executeSql(conn, createMySQL);
								classLogger.info("Created partitioned AUDIT_LOGS (MySQL) with default partition.");
							} catch (SQLException exMy) {
								classLogger.warn("Failed to create MySQL partitioned table: " + exMy.getMessage(),
										exMy);
							}

							for (int i = 0; i < 12; i++) {
								java.time.LocalDate e = start.plusMonths(i + 1);
								String partName = String.format("p%04d%02d", e.getYear(), e.getMonthValue());
								String addPart = String.format(
										"ALTER TABLE AUDIT_LOGS ADD PARTITION (PARTITION %s VALUES LESS THAN (TO_DAYS('%s')))",
										partName, e.format(fmt));
								try {
									executeSql(conn, addPart);
								} catch (SQLException exPart) {
									classLogger.warn(
											"MySQL add partition failed for " + partName + ": " + exPart.getMessage());
								}
							}
						} else if (isMSSQL) {
							// MSSQL: create partition function + scheme + table + clustered index on scheme
							final String pfName = "PF_AUDIT_LOGS_DT";
							final String psName = "PS_AUDIT_LOGS_PRIMARY";

							// Build boundary list for next 12 months
							StringBuilder boundaries = new StringBuilder();
							for (int i = 1; i <= 12; i++) {
								java.time.LocalDate boundary = start.plusMonths(i);
								boundaries.append("'").append(boundary.format(fmt)).append("'");
								if (i < 12) {
									boundaries.append(", ");
								}
							}

							try {
								String createPF = String.format(
										"IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = '%s') "
												+ "BEGIN EXEC('CREATE PARTITION FUNCTION %s (datetime2) AS RANGE RIGHT FOR VALUES (%s)') END",
										pfName, pfName, boundaries.toString());
								executeSql(conn, createPF);
								classLogger.info("Created/ensured MSSQL partition function: " + pfName);

								String createPS = String.format(
										"IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = '%s') "
												+ "BEGIN EXEC('CREATE PARTITION SCHEME %s AS PARTITION %s ALL TO ([PRIMARY])') END",
										psName, psName, pfName);
								executeSql(conn, createPS);
								classLogger.info("Created/ensured MSSQL partition scheme: " + psName);

								// Create table and then clustered index on partition scheme
								executeSql(conn, "CREATE TABLE AUDIT_LOGS (" + auditLogColDefs + ")");
								classLogger.info("Created table AUDIT_LOGS (MSSQL).");

								String clusterIdxName = "PK_AUDIT_LOGS_CLUSTERED_LOGTIMESTAMP";
								String createClusterIdx = String.format(
										"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '%s' AND object_id = OBJECT_ID('AUDIT_LOGS')) "
												+ "BEGIN EXEC('CREATE CLUSTERED INDEX %s ON AUDIT_LOGS (LOG_TIMESTAMP, LOG_ID) ON %s (LOG_TIMESTAMP)') END",
										clusterIdxName, clusterIdxName, psName);
								executeSql(conn, createClusterIdx);
								classLogger
										.info("Created clustered index on AUDIT_LOGS using partition scheme " + psName);

								// best-effort nonclustered index
								try {
									executeSql(conn,
											"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IDX_AUDIT_LOGS_PROJECT_TS' AND object_id = OBJECT_ID('AUDIT_LOGS')) BEGIN CREATE NONCLUSTERED INDEX IDX_AUDIT_LOGS_PROJECT_TS ON AUDIT_LOGS (PROJECT_ID, LOG_TIMESTAMP) END");
								} catch (SQLException exIdx) {
									classLogger.warn("Could not create nonclustered project index on AUDIT_LOGS: "
											+ exIdx.getMessage(), exIdx);
								}

							} catch (SQLException ex) {
								classLogger.error("MSSQL partition creation failed: " + ex.getMessage(), ex);
							}
						}
					}
				} else {
					// ---------- Upgrade path: AUDIT_LOGS exists ----------
					classLogger
							.info("AUDIT_LOGS already exists in DB: " + dbProduct + ". Evaluating partitioning state.");

					if (isPostgres) {
						// Check partitioned state
						boolean alreadyPartitioned = false;
						try (Statement s = conn.createStatement()) {
							String checkSql = "SELECT 1 FROM pg_partitioned_table pt JOIN pg_class c ON pt.partrelid = c.oid WHERE c.relname = 'audit_logs'";
							try (java.sql.ResultSet rs = s.executeQuery(checkSql)) {
								if (rs.next()) {
									alreadyPartitioned = true;
								}
							}
						} catch (SQLException ex) {
							classLogger.warn("Failed to check Postgres partition status: " + ex.getMessage(), ex);
						}

						if (alreadyPartitioned) {
							classLogger.info(
									"AUDIT_LOGS is already partitioned on Postgres. Ensuring monthly partitions for next 12 months exist.");
							java.time.LocalDate startMon = java.time.LocalDate.now().withDayOfMonth(1);
							java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter
									.ofPattern("yyyy-MM-dd");
							for (int i = 0; i < 12; i++) {
								java.time.LocalDate s = startMon.plusMonths(i);
								java.time.LocalDate e = s.plusMonths(1);
								String partName = String.format("AUDIT_LOGS_%d_%02d", s.getYear(), s.getMonthValue());
								String createPartition = String.format(
										"CREATE TABLE IF NOT EXISTS %s PARTITION OF AUDIT_LOGS FOR VALUES FROM ('%s') TO ('%s')",
										partName, s.format(fmt), e.format(fmt));
								try {
									executeSql(conn, createPartition);
								} catch (SQLException exPart) {
									classLogger.warn(
											"Could not ensure partition " + partName + ": " + exPart.getMessage(),
											exPart);
								}
							}
						} else {
							// Convert: rename -> create partitioned -> create default -> create partitions
							// -> copy month-by-month -> indexes
							classLogger.info(
									"AUDIT_LOGS is not partitioned on Postgres. Performing automatic conversion (rename->create partitioned->copy).");
							boolean oldExists = queryUtil.tableExists(engine, "AUDIT_LOGS_OLD", database, schema);
							if (oldExists) {
								classLogger.warn(
										"AUDIT_LOGS_OLD already exists. Aborting automatic conversion to avoid data loss. Manual migration required.");
							} else if (auditLogColDefs == null) {
								classLogger.warn(
										"AUDIT_LOGS column definitions not found; aborting automatic conversion.");
							} else {
								try {
									// rename
									executeSql(conn, "ALTER TABLE AUDIT_LOGS RENAME TO AUDIT_LOGS_OLD");
									// create parent
									String createParent = "CREATE TABLE AUDIT_LOGS (" + auditLogColDefs
											+ ") PARTITION BY RANGE (LOG_TIMESTAMP)";
									executeSql(conn, createParent);
									// default partition
									executeSql(conn,
											"CREATE TABLE IF NOT EXISTS AUDIT_LOGS_DEFAULT PARTITION OF AUDIT_LOGS DEFAULT");
									// create partitions for historic + next 12 months
									java.time.LocalDate minDate = null, maxDate = null;
									try (Statement s = conn.createStatement();
											java.sql.ResultSet rs = s.executeQuery(
													"SELECT MIN(LOG_TIMESTAMP) AS min_ts, MAX(LOG_TIMESTAMP) AS max_ts FROM AUDIT_LOGS_OLD")) {
										if (rs.next()) {
											java.sql.Timestamp minTs = rs.getTimestamp("min_ts");
											java.sql.Timestamp maxTs = rs.getTimestamp("max_ts");
											if (minTs != null) {
												minDate = minTs.toLocalDateTime().toLocalDate().withDayOfMonth(1);
											}
											if (maxTs != null) {
												maxDate = maxTs.toLocalDateTime().toLocalDate().withDayOfMonth(1);
											}
										}
									} catch (SQLException ex) {
										classLogger
												.warn("Could not determine min/max LOG_TIMESTAMP from AUDIT_LOGS_OLD: "
														+ ex.getMessage(), ex);
									}

									java.time.LocalDate rangeStart = (minDate != null) ? minDate
											: java.time.LocalDate.now().withDayOfMonth(1);
									java.time.LocalDate rangeEnd = (maxDate != null) ? maxDate.plusMonths(1)
											: java.time.LocalDate.now().plusMonths(12).withDayOfMonth(1);

									java.time.LocalDate cursor = rangeStart;
									java.time.format.DateTimeFormatter dtfmt = java.time.format.DateTimeFormatter
											.ofPattern("yyyy-MM-dd");
									while (!cursor.isAfter(rangeEnd.plusMonths(12))) {
										java.time.LocalDate next = cursor.plusMonths(1);
										String partName = String.format("AUDIT_LOGS_%d_%02d", cursor.getYear(),
												cursor.getMonthValue());
										String createPartition = String.format(
												"CREATE TABLE IF NOT EXISTS %s PARTITION OF AUDIT_LOGS FOR VALUES FROM ('%s') TO ('%s')",
												partName, cursor.format(dtfmt), next.format(dtfmt));
										try {
											executeSql(conn, createPartition);
										} catch (SQLException exPart) {
											classLogger.warn("Could not create partition " + partName + ": "
													+ exPart.getMessage(), exPart);
										}
										cursor = next;
									}

									// copy month-by-month
									boolean originalAutoCommit = conn.getAutoCommit();
									try {
										if (originalAutoCommit) {
											conn.setAutoCommit(false);
										}
									} catch (SQLException e) {
										/* ignore */ }

									java.time.LocalDate copyStart = (minDate != null) ? minDate
											: java.time.LocalDate.now().withDayOfMonth(1);
									java.time.LocalDate copyEndInclusive = (maxDate != null) ? maxDate
											: java.time.LocalDate.now().withDayOfMonth(1);
									java.time.LocalDate cur = copyStart;
									while (!cur.isAfter(copyEndInclusive)) {
										java.time.LocalDate nxt = cur.plusMonths(1);
										String insertSql = String.format(
												"INSERT INTO AUDIT_LOGS SELECT * FROM AUDIT_LOGS_OLD WHERE LOG_TIMESTAMP >= '%s' AND LOG_TIMESTAMP < '%s'",
												cur.format(dtfmt), nxt.format(dtfmt));
										try {
											executeSql(conn, insertSql);
											if (!originalAutoCommit) {
												try {
													conn.commit();
												} catch (SQLException ce) {
													classLogger.warn("Commit failed: " + ce.getMessage(), ce);
												}
											}
										} catch (SQLException exInsert) {
											classLogger.error(
													"Failed copying month " + cur + ": " + exInsert.getMessage(),
													exInsert);
										}
										cur = nxt;
									}
									try {
										if (originalAutoCommit) {
											conn.setAutoCommit(true);
										}
									} catch (SQLException e) {
										/* ignore */ }

									// per-partition indexes next 12 months
									java.time.LocalDate startCreate = java.time.LocalDate.now().withDayOfMonth(1);
									for (int i = 0; i < 12; i++) {
										java.time.LocalDate s = startCreate.plusMonths(i);
										String partName = String.format("AUDIT_LOGS_%d_%02d", s.getYear(),
												s.getMonthValue());
										try {
											executeSql(conn, String.format(
													"CREATE INDEX IF NOT EXISTS IDX_%s_PROJECT_TS ON %s (PROJECT_ID, LOG_TIMESTAMP)",
													partName, partName));
											executeSql(conn, String.format(
													"CREATE INDEX IF NOT EXISTS IDX_%s_USER_TS ON %s (USER_ID, LOG_TIMESTAMP)",
													partName, partName));
										} catch (SQLException exIdx) {
											classLogger.warn("Could not create index on partition " + partName + ": "
													+ exIdx.getMessage(), exIdx);
										}
									}

									classLogger.info(
											"Postgres conversion completed. AUDIT_LOGS_OLD preserved for verification.");
								} catch (SQLException exConvert) {
									classLogger.error("Error during Postgres conversion: " + exConvert.getMessage(),
											exConvert);
									// attempt rollback
									try {
										executeSql(conn, "DROP TABLE IF EXISTS AUDIT_LOGS");
										executeSql(conn, "ALTER TABLE IF EXISTS AUDIT_LOGS_OLD RENAME TO AUDIT_LOGS");
										classLogger
												.info("Rollback attempted: restored original AUDIT_LOGS if possible.");
									} catch (SQLException exRB) {
										classLogger.error("Rollback failed: " + exRB.getMessage(), exRB);
									}
								}
							}
						}

					} else if (isMySQL) {
						// MySQL: attempt ALTER TABLE PARTITION BY in-place
						classLogger.info(
								"Attempting MySQL in-place partition conversion (ALTER TABLE PARTITION BY ...). This will rebuild the table.");
						if (auditLogColDefs == null) {
							classLogger.warn(
									"AUDIT_LOGS columns not available in dbSchema; cannot perform MySQL in-place conversion.");
						} else {
							java.time.LocalDate startMon = java.time.LocalDate.now().withDayOfMonth(1);
							java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter
									.ofPattern("yyyy-MM-dd");
							StringBuilder partDefs = new StringBuilder();
							for (int i = 0; i < 12; i++) {
								java.time.LocalDate e = startMon.plusMonths(i + 1);
								String partName = String.format("p%04d%02d", e.getYear(), e.getMonthValue());
								partDefs.append(String.format("PARTITION %s VALUES LESS THAN (TO_DAYS('%s'))", partName,
										e.format(fmt)));
								if (i < 11) {
									partDefs.append(", ");
								}
							}
							if (partDefs.length() > 0) {
								partDefs.append(", PARTITION p_max VALUES LESS THAN (MAXVALUE)");
							}
							String alterSql = "ALTER TABLE AUDIT_LOGS PARTITION BY RANGE (TO_DAYS(LOG_TIMESTAMP)) ("
									+ partDefs.toString() + ")";
							try {
								executeSql(conn, alterSql);
								classLogger.info("MySQL ALTER TABLE PARTITION executed successfully.");
							} catch (SQLException exAlter) {
								classLogger.error("MySQL partition ALTER failed: " + exAlter.getMessage(), exAlter);
								classLogger.warn(
										"MySQL auto-conversion may fail due to partitioning restrictions (unique keys, engine). Manual migration recommended.");
							}
						}
					} else if (isMSSQL) {
						// MSSQL: detect if already partitioned and convert if not
						boolean alreadyPartitioned = false;
						try (Statement st = conn.createStatement();
								java.sql.ResultSet rs = st.executeQuery(
										"SELECT COUNT(DISTINCT partition_id) AS pcount FROM sys.partitions WHERE object_id = OBJECT_ID('AUDIT_LOGS')")) {
							if (rs.next()) {
								int pcount = rs.getInt("pcount");
								if (pcount > 1) {
									alreadyPartitioned = true;
								}
							}
						} catch (SQLException ex) {
							classLogger.warn("Could not determine MSSQL partition status: " + ex.getMessage(), ex);
						}

						final String pfName = "PF_AUDIT_LOGS_DT";
						final String psName = "PS_AUDIT_LOGS_PRIMARY";
						java.time.LocalDate startMon = java.time.LocalDate.now().withDayOfMonth(1);
						java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter
								.ofPattern("yyyy-MM-dd");

						if (alreadyPartitioned) {
							classLogger.info(
									"AUDIT_LOGS is already partitioned on MSSQL. Ensuring partition boundaries for next 12 months.");
							for (int i = 1; i <= 12; i++) {
								java.time.LocalDate boundary = startMon.plusMonths(i);
								String splitSql = String.format("ALTER PARTITION FUNCTION %s() SPLIT RANGE ('%s')",
										pfName, boundary.format(fmt));
								try {
									executeSql(conn, splitSql);
								} catch (SQLException exSplit) {
									// split may fail if boundary exists - ignore
									classLogger.debug("Partition split attempt for " + boundary + " returned: "
											+ exSplit.getMessage());
								}
							}
						} else {
							// Convert non-partitioned to partitioned (rename->create PF/PS->create
							// table->clustered index->copy month-by-month)
							classLogger.info(
									"AUDIT_LOGS exists but is not partitioned on MSSQL. Performing conversion (rename->create partitioned->copy).");
							try {
								if (queryUtil.tableExists(engine, "AUDIT_LOGS_OLD", database, schema)) {
									classLogger.warn(
											"AUDIT_LOGS_OLD exists — aborting automatic MSSQL conversion. Manual migration required.");
								} else if (auditLogColDefs == null) {
									classLogger.warn("AUDIT_LOGS columns not available; aborting MSSQL conversion.");
								} else {
									// rename
									executeSql(conn, "EXEC sp_rename 'AUDIT_LOGS', 'AUDIT_LOGS_OLD';");
									// create partition function
									StringBuilder boundaries = new StringBuilder();
									for (int i = 1; i <= 12; i++) {
										java.time.LocalDate b = startMon.plusMonths(i);
										boundaries.append("'").append(b.format(fmt)).append("'");
										if (i < 12) {
											boundaries.append(", ");
										}
									}
									executeSql(conn, "CREATE PARTITION FUNCTION " + pfName
											+ " (datetime2) AS RANGE RIGHT FOR VALUES (" + boundaries.toString() + ")");
									executeSql(conn, "CREATE PARTITION SCHEME " + psName + " AS PARTITION " + pfName
											+ " ALL TO ([PRIMARY])");
									// create empty table, then clustered index on partition scheme
									executeSql(conn, "CREATE TABLE AUDIT_LOGS (" + auditLogColDefs + ")");
									executeSql(conn,
											"CREATE CLUSTERED INDEX PK_AUDIT_LOGS_CLUSTERED_LOGTIMESTAMP ON AUDIT_LOGS (LOG_TIMESTAMP, LOG_ID) ON "
													+ psName + " (LOG_TIMESTAMP)");
									// copy month-by-month from old
									java.time.LocalDate minDate = null, maxDate = null;
									try (Statement s = conn.createStatement();
											java.sql.ResultSet rs = s.executeQuery(
													"SELECT MIN(LOG_TIMESTAMP) AS min_ts, MAX(LOG_TIMESTAMP) AS max_ts FROM AUDIT_LOGS_OLD")) {
										if (rs.next()) {
											java.sql.Timestamp tsmin = rs.getTimestamp("min_ts");
											java.sql.Timestamp tsmax = rs.getTimestamp("max_ts");
											if (tsmin != null) {
												minDate = tsmin.toLocalDateTime().toLocalDate().withDayOfMonth(1);
											}
											if (tsmax != null) {
												maxDate = tsmax.toLocalDateTime().toLocalDate().withDayOfMonth(1);
											}
										}
									} catch (SQLException ex) {
										classLogger.warn("Could not compute min/max LOG_TIMESTAMP from AUDIT_LOGS_OLD: "
												+ ex.getMessage(), ex);
									}

									java.time.LocalDate copyStart = (minDate != null) ? minDate
											: java.time.LocalDate.now().withDayOfMonth(1);
									java.time.LocalDate copyEndInclusive = (maxDate != null) ? maxDate
											: java.time.LocalDate.now().withDayOfMonth(1);

									java.time.LocalDate cur = copyStart;
									while (!cur.isAfter(copyEndInclusive)) {
										java.time.LocalDate nxt = cur.plusMonths(1);
										String insertSql = String.format(
												"INSERT INTO AUDIT_LOGS SELECT * FROM AUDIT_LOGS_OLD WHERE LOG_TIMESTAMP >= '%s' AND LOG_TIMESTAMP < '%s'",
												cur.format(fmt), nxt.format(fmt));
										try {
											executeSql(conn, insertSql);
										} catch (SQLException exInsert) {
											classLogger.error(
													"Failed copying month " + cur + ": " + exInsert.getMessage(),
													exInsert);
										}
										cur = nxt;
									}

									// create nonclustered indexes
									try {
										executeSql(conn,
												"CREATE NONCLUSTERED INDEX IDX_AUDIT_LOGS_PROJECT_TS ON AUDIT_LOGS (PROJECT_ID, LOG_TIMESTAMP)");
										executeSql(conn,
												"CREATE NONCLUSTERED INDEX IDX_AUDIT_LOGS_USER_TS ON AUDIT_LOGS (USER_ID, LOG_TIMESTAMP)");
									} catch (SQLException exIdx) {
										classLogger.warn("Could not create some nonclustered indexes on AUDIT_LOGS: "
												+ exIdx.getMessage(), exIdx);
									}

									classLogger.info(
											"MSSQL conversion completed. AUDIT_LOGS_OLD retained for validation.");
								}
							} catch (SQLException exConv) {
								classLogger.error("Error during MSSQL conversion: " + exConv.getMessage(), exConv);
								try {
									executeSql(conn, "IF OBJECT_ID('AUDIT_LOGS', 'U') = 1 DROP TABLE AUDIT_LOGS;");
									executeSql(conn, "EXEC sp_rename 'AUDIT_LOGS_OLD', 'AUDIT_LOGS';");
									classLogger.info(
											"Rollback attempted: restored AUDIT_LOGS from AUDIT_LOGS_OLD (if possible).");
								} catch (SQLException exRB) {
									classLogger.error("Rollback failed for MSSQL conversion: " + exRB.getMessage(),
											exRB);
								}
							}
						}
					}
				} // end auditExists branch
			} // end supportsPartitioning branch
		} catch (Exception ex) {
			classLogger.warn("Partition setup failed (non-fatal): " + ex.getMessage(), ex);
		}

		// ------------------------------------------------------
		// Continue with original table creation / add-column logic
		// ------------------------------------------------------
		for (Pair<String, List<Pair<String, String>>> tableSchema : dbSchema) {
			String tableName = tableSchema.getValue0();
			String[] colNames = tableSchema.getValue1().stream().map(Pair::getValue0).toArray(String[]::new);
			String[] types = tableSchema.getValue1().stream().map(Pair::getValue1).toArray(String[]::new);

			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists(tableName, colNames, types);
				executeSql(conn, sql);
			} else {
				if (!queryUtil.tableExists(engine, tableName, database, schema)) {
					String sql = queryUtil.createTable(tableName, colNames, types);
					executeSql(conn, sql);
				}
			}

			List<String> allCols = queryUtil.getTableColumns(conn, tableName, database, schema);
			for (int i = 0; i < colNames.length; i++) {
				String col = colNames[i];
				if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
					String addColumnSql = queryUtil.alterTableAddColumn(tableName, col, types[i]);
					executeSql(conn, addColumnSql);
				}
			}
		}

		// ============================================================
		// Index creation logic (unchanged from earlier)
		// ============================================================
		if (allowIfExistsIndexs) {

			String sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__REQUEST_ID_INDEX", "AUDIT_LOGS", "REQUEST_ID");
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__PROJECT_TS_INDEX", "AUDIT_LOGS",
					List.of("PROJECT_ID", "LOG_TIMESTAMP"));
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__USER_TS_INDEX", "AUDIT_LOGS",
					List.of("USER_ID", "LOG_TIMESTAMP"));
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__ENGINE_TS_INDEX", "AUDIT_LOGS",
					List.of("ENGINE_ID", "LOG_TIMESTAMP"));
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__SESSION_ID_INDEX", "AUDIT_LOGS", "SESSION_ID");
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__ROOM_ID_INDEX", "AUDIT_LOGS", "ROOM_ID");
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);

		} else {

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__REQUEST_ID_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__REQUEST_ID_INDEX", "AUDIT_LOGS", "REQUEST_ID");
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__PROJECT_TS_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__PROJECT_TS_INDEX", "AUDIT_LOGS",
						List.of("PROJECT_ID", "LOG_TIMESTAMP"));
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__USER_TS_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__USER_TS_INDEX", "AUDIT_LOGS",
						List.of("USER_ID", "LOG_TIMESTAMP"));
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__ENGINE_TS_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__ENGINE_TS_INDEX", "AUDIT_LOGS",
						List.of("ENGINE_ID", "LOG_TIMESTAMP"));
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__SESSION_ID_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__SESSION_ID_INDEX", "AUDIT_LOGS", "SESSION_ID");
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__ROOM_ID_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__ROOM_ID_INDEX", "AUDIT_LOGS", "ROOM_ID");
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}
	}

	/**
	 * @param conn
	 * @param sql
	 * @throws SQLException
	 */
	private static void executeSql(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			classLogger.info("Running sql " + sql);
			stmt.execute(sql);
		}
	}

	/**
	 * 
	 * @return
	 */
	public static boolean isInitalized() {
		return initialized;
	}

	/**
	 * Transform any RDBMS engine into an audit logs database
	 * 
	 * @param auditLogsDb
	 * @throws Exception
	 */
	public static synchronized void initEngineAsAuditDatabase(IRDBMSEngine auditLogsDb) throws Exception {
		AuditLogsDbOwlCreator owlCreator = new AuditLogsDbOwlCreator(auditLogsDb);
		if (owlCreator.needsRemake()) {
			owlCreator.remakeOwl();
			// reset the local master metadata for model engine if we remade the OWL
			Utility.synchronizeEngineMetadata(auditLogsDb.getEngineId());
		}

		Connection conn = null;
		try {
			conn = auditLogsDb.getConnection();
			executeInitDatabaseSchema(auditLogsDb, conn, owlCreator.getDBSchema());
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(auditLogsDb, conn, null, null);
		}
	}

	/**
	 * 
	 * @param userId
	 * @param projectId
	 * @param engineId
	 * @param dateTime
	 * @param roomId
	 * @param sessionId
	 * @param offset
	 * @param limit
	 * @return
	 * @throws SQLException
	 */
	public static List<LogActivityDto> getAuditLogsTimeLineData(String userId, String projectId, String engineId,
			SemossDate startDate, SemossDate endDate, String roomId, String sessionId, int limit, int offset)
			throws SQLException {

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST_ID"));
		qs.addSelector(new QueryColumnSelector("MIN_MAX_DURATION__START_TIME"));
		qs.addSelector(new QueryColumnSelector("MIN_MAX_DURATION__END_TIME"));
		qs.addSelector(new QueryColumnSelector("MIN_MAX_DURATION__DURATION"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__ENGINE_NAME"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__ENGINE_TYPE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__METHOD_NAME"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__RESPONSE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__NUMBER_OF_TOKENS_IN_PROMPT"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__NUMBER_OF_TOKENS_IN_RESPONSE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__IS_SUCCESS"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__USER_ID"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__SESSION_ID"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__SPAN_ID"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__LOG_TIMESTAMP"));

		// add filters dynamically if present
		addStartDateEndDateFitler(qs, "AUDIT_LOGS__LOG_TIMESTAMP", startDate, endDate);
		addFilter(qs, "AUDIT_LOGS__USER_ID", "==", userId);
		addFilter(qs, "AUDIT_LOGS__PROJECT_ID", "==", projectId);
		addFilter(qs, "AUDIT_LOGS__ENGINE_ID", "==", engineId);
		addFilter(qs, "AUDIT_LOGS__ROOM_ID", "==", roomId);
		addFilter(qs, "AUDIT_LOGS__SESSION_ID", "==", sessionId);
		qs.addOrderBy("AUDIT_LOGS__LOG_TIMESTAMP", "desc");

		// pagination
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}

		SelectQueryStruct minMaxDuration = new SelectQueryStruct();
		minMaxDuration.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST_ID", "REQUEST_ID"));
		minMaxDuration.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
				"AUDIT_LOGS__REQUEST_START_TIME", "START_TIME"));
		minMaxDuration.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX,
				"AUDIT_LOGS__RESPONSE_END_TIME", "END_TIME"));
		minMaxDuration.addSelector(QueryFunctionSelector.makeDateDiffFunctionSelector(QueryFunctionHelper.SECOND,
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, "AUDIT_LOGS__REQUEST_START_TIME",
						"START_TIME"),
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, "AUDIT_LOGS__RESPONSE_END_TIME",
						"END_TIME"),
				"DURATION"));
		// filter for minMaxDuration
		addStartDateEndDateFitler(minMaxDuration, "AUDIT_LOGS__LOG_TIMESTAMP", startDate, endDate);
		addFilter(minMaxDuration, "AUDIT_LOGS__USER_ID", "==", userId);
		addFilter(minMaxDuration, "AUDIT_LOGS__PROJECT_ID", "==", projectId);
		addFilter(minMaxDuration, "AUDIT_LOGS__ENGINE_ID", "==", engineId);
		addFilter(minMaxDuration, "AUDIT_LOGS__ROOM_ID", "==", roomId);
		addFilter(minMaxDuration, "AUDIT_LOGS__SESSION_ID", "==", sessionId);

		minMaxDuration.addGroupBy(new QueryColumnSelector("AUDIT_LOGS__REQUEST_ID"));
		IRelation subQuery = new SubqueryRelationship(minMaxDuration, "MIN_MAX_DURATION", "inner.join",
				new String[] { "AUDIT_LOGS__REQUEST_ID", "MIN_MAX_DURATION__REQUEST_ID", "=" });
		qs.addRelation(subQuery);

		List<LogActivityDto> activityList = new ArrayList<>();
		List<Map<String, Object>> list = QueryExecutionUtility.flushRsToMap(auditLogsDb, qs);
		for (Map<String, Object> map : list) {
			Timestamp startTime = extractTimestamp(map.get("START_TIME"));
			Timestamp endTime = extractTimestamp(map.get("END_TIME"));
			String request = getOrDefault(map.get("REQUEST"), "");
			String response = getOrDefault(map.get("RESPONSE"), "");
			String engineName = getOrDefault(map.get("ENGINE_NAME"), null);
			String engineType = getOrDefault(map.get("ENGINE_TYPE"), null);
			boolean status = map.get("IS_SUCCESS") instanceof Boolean && (Boolean) map.get("IS_SUCCESS");
			long latency = map.get("DURATION") instanceof Long ? (Long) map.get("DURATION") : 0L;
			int tokens = getIntValue(map.get("NUMBER_OF_TOKENS_IN_PROMPT"))
					+ getIntValue(map.get("NUMBER_OF_TOKENS_IN_RESPONSE"));
			String methodName = getOrDefault(map.get("METHOD_NAME"), "");
			String userIdFromRow = getOrDefault(map.get("USER_ID"), null);
			String sessionIdFromRow = getOrDefault(map.get("SESSION_ID"), null);
			String spanIdFromRow = getOrDefault(map.get("SPAN_ID"), null);
			Timestamp logTimestamp = extractTimestamp(map.get("LOG_TIMESTAMP"));

			activityList.add(new LogActivityDto(startTime, endTime, request, response, tokens, latency, status,
					engineName, engineType, methodName, userIdFromRow, sessionIdFromRow, spanIdFromRow, logTimestamp));

		}
		return activityList;
	}

	// Helper Methods

	/**
	 * @param qs
	 * @param startDate
	 * @param endDate
	 */
	private static void addStartDateEndDateFitler(SelectQueryStruct qs, String column, SemossDate startDate,
			SemossDate endDate) {
		if (startDate != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(column, ">=", startDate));
		}
		if (endDate != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(column, "<=", endDate));
		}
	}

	/**
	 * 
	 * @param qs
	 * @param column
	 * @param operator
	 * @param value
	 */
	private static void addFilter(SelectQueryStruct qs, String column, String operator, String value) {
		if (value != null && !(value = value.trim()).isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(column, operator, value));
		}
	}

	/**
	 * 
	 * @param dateObj
	 * @return
	 */
	private static Timestamp extractTimestamp(Object dateObj) {
		if (dateObj instanceof SemossDate) {
			Timestamp ts = Utility.getSqlTimestampUTC((SemossDate) dateObj);
			return Timestamp.valueOf(ts.toLocalDateTime().truncatedTo(ChronoUnit.SECONDS));
		}
		return null;
	}

	/**
	 * 
	 * @param obj
	 * @param defaultValue
	 * @return
	 */
	private static String getOrDefault(Object obj, String defaultValue) {
		return (obj != null && !obj.toString().isEmpty()) ? obj.toString() : defaultValue;
	}

	/**
	 * 
	 * @param obj
	 * @return
	 */
	private static int getIntValue(Object obj) {
		return (obj instanceof Integer) ? (Integer) obj : 0;
	}

	/**
	 * Get audit log total record count
	 * 
	 * @param userId
	 * @param projectId
	 * @param engineId
	 * @param dateTime
	 * @param roomId
	 * @param sessionId
	 * @return
	 */
	public static long getAuditLogsCount(String userId, String projectId, String engineId, SemossDate startDate,
			SemossDate endDate, String roomId, String sessionId) {
		SelectQueryStruct qs = new SelectQueryStruct();

		// COUNT(AUDIT_LOGS__LOG_ID) selector
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("total_count");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("AUDIT_LOGS__LOG_ID"));
		qs.addSelector(fSelector);

		// Apply filters dynamically
		addStartDateEndDateFitler(qs, "AUDIT_LOGS__LOG_TIMESTAMP", startDate, endDate);
		addFilter(qs, "AUDIT_LOGS__USER_ID", "==", userId);
		addFilter(qs, "AUDIT_LOGS__PROJECT_ID", "==", projectId);
		addFilter(qs, "AUDIT_LOGS__ENGINE_ID", "==", engineId);
		addFilter(qs, "AUDIT_LOGS__ROOM_ID", "==", roomId);
		addFilter(qs, "AUDIT_LOGS__SESSION_ID", "==", sessionId);

		return QueryExecutionUtility.flushToLong(auditLogsDb, qs);
	}

}
