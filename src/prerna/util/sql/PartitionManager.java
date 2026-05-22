package prerna.util.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PartitionManager {
	private static final Logger logger = LogManager.getLogger(PartitionManager.class);

	/**
	 * Ensure the given table is partitioned (fresh create or conversion).
	 *
	 * @param exists            table exists or not boolean
	 * @param conn
	 * @param queryUtil
	 * @param tableName
	 * @param partitionColumn   column used to partition
	 * @param columnDefinitions column list for create (comma separated)
	 * @param freq              partition frequency (MONTHLY etc.)
	 * @param monthsAhead       months/days/years to pre-create
	 */
	public static void ensurePartitioned(Boolean exists, Connection conn, AbstractSqlQueryUtil queryUtil,
			String tableName, String partitionColumn, String columnDefinitions,
			AbstractSqlQueryUtil.PartitionFrequency freq, int monthsAhead) {

		if (!queryUtil.supportsPartitioning()) {
			logger.info("DB does not support partitioning");
			return;
		}

		try {
			if (!exists) {
				// Create fresh partitioned table
				logger.info("{} does not exist — creating partitioned table", tableName);
				List<String> createSqls = queryUtil.getCreatePartitionedTableSql(tableName, partitionColumn,
						columnDefinitions, freq, monthsAhead);
				executeStatements(conn, createSqls);
				return;
			}

			boolean alreadyPartitioned = queryUtil.isTablePartitioned(conn, tableName);
			if (alreadyPartitioned) {
				// Ensure future partitions for table
				logger.info("{} is already partitioned — ensuring future partitions (next {})", tableName, monthsAhead);
				queryUtil.getEnsureFuturePartitionsSql(conn, tableName, partitionColumn, freq, monthsAhead);
				return;
			}

			// Not partitioned -> convert
			logger.info("{} exists but is not partitioned — generating conversion SQLs", tableName);

			List<String> convertSqls = queryUtil.getConvertTableToPartitionedSql(conn, tableName, partitionColumn,
					columnDefinitions, freq, monthsAhead);

			if (convertSqls != null && !convertSqls.isEmpty()) {
				try {
					// Run conversion inside a transaction so it either fully succeeds or rolls back
					executeStatementsTransactional(conn, convertSqls);
				} catch (SQLException e) {
					logger.error("Partition conversion failed for table {}. Attempting rollback.", tableName, e);

					try {
						conn.rollback();
					} catch (SQLException rollbackEx) {
						logger.error("Rollback failed after partition conversion error", rollbackEx);
					}

					// Try restoring original table name if conversion partially happened
					try (Statement stmt = conn.createStatement()) {
						stmt.execute("ALTER TABLE " + tableName + "_old RENAME TO " + tableName);
						logger.info("Restored original table name from {}_old", tableName);
					} catch (Exception restoreEx) {
						logger.warn("Could not restore original table name after failed partition conversion: {}",
								restoreEx.getMessage());
					}
				}
			}
			logger.info("Conversion for {} completed (SQL executed).", tableName);
		} catch (Exception e) {
			logger.error("Error while ensuring partitioning for " + tableName, e);
		}
	}

	private static void executeStatements(Connection conn, List<String> sqls) throws SQLException {
		// default: non-transactional, execute each statement in its own transaction
		// (autocommit true)
		if (sqls == null || sqls.isEmpty()) {
			return;
		}

		boolean originalAuto = conn.getAutoCommit();
		try {
			// Ensure autocommit for statement-by-statement execution to avoid a single
			// failure aborting all subsequent commands.
			if (!originalAuto) {
				try {
					conn.setAutoCommit(true);
				} catch (SQLException e) {
					logger.debug("Could not set autocommit=true: {}", e.getMessage());
				}
			}
			for (String sql : sqls) {
				try (Statement st = conn.createStatement()) {
					logger.info("Running sql: " + sql);
					st.execute(sql);
				} catch (SQLException e) {
					logger.warn("Statement failed (non-fatal): {} => {}", sql, e.getMessage());
					// Roll back to reset state.
					try {
						if (!conn.getAutoCommit()) {
							conn.rollback();
						}
					} catch (SQLException rbEx) {
						logger.debug("Rollback after failure failed: {}", rbEx.getMessage());
					}
				}
			}
		} finally {
			// Restore original auto-commit
			try {
				if (!originalAuto) {
					conn.setAutoCommit(originalAuto);
				}
			} catch (SQLException e) {
				logger.debug("Failed to restore autocommit: {}", e.getMessage());
			}
		}
	}

	private static void executeStatementsTransactional(Connection conn, List<String> sqls) throws SQLException {
		if (sqls == null || sqls.isEmpty()) {
			return;
		}

		boolean originalAuto = conn.getAutoCommit();
		try {
			// Run all statements inside a single transaction
			if (originalAuto) {
				conn.setAutoCommit(false);
			}

			try (Statement st = conn.createStatement()) {
				for (String sql : sqls) {
					try {
						logger.info("Running sql (txn): " + sql);
						st.execute(sql);
					} catch (SQLException e) {
						logger.error("Transactional statement failed: {} => {}. Rolling back transaction.", sql,
								e.getMessage(), e);
						try {
							conn.rollback();
						} catch (SQLException rb) {
							logger.error("Rollback failed: {}", rb.getMessage(), rb);
						}
						// Propagate an exception so the caller knows conversion failed
						throw e;
					}
				}
				// Commit if all statements succeeded
				conn.commit();
			}
		} finally {
			// Restore autocommit
			try {
				if (originalAuto) {
					conn.setAutoCommit(true);
				}
			} catch (SQLException e) {
				logger.debug("Failed to restore autocommit after transactional execution: {}", e.getMessage());
			}
		}
	}
}
