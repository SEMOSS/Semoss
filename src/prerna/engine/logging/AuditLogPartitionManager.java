package prerna.engine.logging;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Manages partitioning for audit logs table Handles both new table creation and
 * existing table migration
 */
public class AuditLogPartitionManager {

	private static final Logger classLogger = LogManager.getLogger(AuditLogPartitionManager.class);
	private static final String AUDIT_LOGS_TABLE = "AUDIT_LOGS";
	private static final String PARTITION_COLUMN = "LOG_TIMESTAMP";
	private static final int DEFAULT_PARTITION_COUNT = 12;

	private final IRDBMSEngine engine;
	private final AbstractSqlQueryUtil queryUtil;
	private final Connection conn;
	private final String database;
	private final String schema;

	public AuditLogPartitionManager(IRDBMSEngine engine, Connection conn) {
		this.engine = engine;
		this.conn = conn;
		this.queryUtil = engine.getQueryUtil();
		this.database = engine.getDatabase();
		this.schema = engine.getSchema();
	}

	/**
	 * Main entry point for setting up audit log partitioning
	 * 
	 * @param dbSchema The database schema definition
	 * @throws SQLException
	 */
	public void setupPartitioning(List<Pair<String, List<Pair<String, String>>>> dbSchema) throws SQLException {
		try {
			// Get database product information
			String dbProduct = getDatabaseProductName();
			DatabaseType dbType = detectDatabaseType(dbProduct);

			if (!dbType.isPartitioningSupported()) {
				classLogger.info("Database {} does not support automatic partitioning. Using standard table creation.",
						dbProduct);
				return;
			}

			// Find AUDIT_LOGS schema definition
			List<Pair<String, String>> auditLogsColumns = findAuditLogsSchema(dbSchema);
			if (auditLogsColumns == null || auditLogsColumns.isEmpty()) {
				classLogger.warn("AUDIT_LOGS column definitions not found in schema. Skipping partitioning.");
				return;
			}

			// Check if table exists
			boolean tableExists = queryUtil.tableExists(engine, AUDIT_LOGS_TABLE, database, schema);

			if (!tableExists) {
				createNewPartitionedTable(dbType, auditLogsColumns);
			} else {
				handleExistingTable(dbType, auditLogsColumns, dbProduct);
			}

		} catch (Exception e) {
			classLogger.error("Error during partitioning setup: " + e.getMessage(), e);
			throw new SQLException("Partitioning setup failed", e);
		}
	}

	/**
	 * Create a new partitioned table from scratch
	 */
	private void createNewPartitionedTable(DatabaseType dbType, List<Pair<String, String>> columns)
			throws SQLException {
		classLogger.info("Creating new partitioned {} table for {}", AUDIT_LOGS_TABLE, dbType);

		String[] colNames = columns.stream().map(Pair::getValue0).toArray(String[]::new);
		String[] types = columns.stream().map(Pair::getValue1).toArray(String[]::new);

		// Use the partitioned table creation method
		String createTableSql = queryUtil.createPartitionedTableIfNotExists(AUDIT_LOGS_TABLE, colNames, types,
				PARTITION_COLUMN, "MONTH");
		executeSql(createTableSql);

		// Create initial partitions
		createInitialPartitions(dbType, AUDIT_LOGS_TABLE);

		classLogger.info("Successfully created partitioned {} table", AUDIT_LOGS_TABLE);
	}

	/**
	 * Handle existing table - either already partitioned or needs conversion
	 */
	private void handleExistingTable(DatabaseType dbType, List<Pair<String, String>> columns, String dbProduct)
			throws SQLException {
		classLogger.info("AUDIT_LOGS table already exists in {}. Evaluating partitioning state.", dbProduct);

		if (dbType == DatabaseType.POSTGRES) {
			handlePostgresExistingTable(columns);
		} else if (dbType == DatabaseType.MYSQL) {
			handleMySqlExistingTable(columns);
		} else if (dbType == DatabaseType.MSSQL) {
			handleSqlServerExistingTable(columns);
		}
	}

	/**
	 * Handle existing PostgreSQL table
	 */
	private void handlePostgresExistingTable(List<Pair<String, String>> columns) throws SQLException {
		if (isPostgresTablePartitioned()) {
			classLogger.info("AUDIT_LOGS is already partitioned. Ensuring partitions exist.");
			createInitialPartitions(DatabaseType.POSTGRES, AUDIT_LOGS_TABLE);
		} else {
			convertPostgresTableToPartitioned(columns);
		}
	}

	/**
	 * Handle existing MySQL table
	 */
	private void handleMySqlExistingTable(List<Pair<String, String>> columns) throws SQLException {
		classLogger.info("Attempting MySQL in-place partition conversion.");
		// MySQL typically requires ALTER TABLE ... PARTITION BY
		// This would use database-specific SQL since it's complex
		classLogger.warn("MySQL automatic conversion not implemented in this refactored version. "
				+ "Manual ALTER TABLE PARTITION BY required.");
	}

	/**
	 * Handle existing SQL Server table
	 */
	private void handleSqlServerExistingTable(List<Pair<String, String>> columns) throws SQLException {
		classLogger.info("Evaluating SQL Server partitioning state.");

		if (isSqlServerTablePartitioned()) {
			classLogger.info("AUDIT_LOGS is already partitioned on SQL Server. Ensuring partition boundaries.");
			ensureSqlServerPartitionBoundaries();
		} else {
			convertSqlServerTableToPartitioned(columns);
		}
	}

	/**
	 * Convert existing PostgreSQL table to partitioned table
	 */
	private void convertPostgresTableToPartitioned(List<Pair<String, String>> columns) throws SQLException {
		classLogger.info("Converting existing AUDIT_LOGS table to partitioned table. This may take time.");

		// Safety check
		if (queryUtil.tableExists(engine, AUDIT_LOGS_TABLE + "_OLD", database, schema)) {
			throw new SQLException("AUDIT_LOGS_OLD table already exists. Aborting to prevent data loss.");
		}

		String[] colNames = columns.stream().map(Pair::getValue0).toArray(String[]::new);
		String[] types = columns.stream().map(Pair::getValue1).toArray(String[]::new);

		try {
			// 1. Rename existing table
			String renameSql = queryUtil.alterTableName(AUDIT_LOGS_TABLE, AUDIT_LOGS_TABLE + "_OLD");
			executeSql(renameSql);
			classLogger.info("Renamed {} -> {}_OLD", AUDIT_LOGS_TABLE, AUDIT_LOGS_TABLE);

			// 2. Create partitioned parent table
			String createParentSql = queryUtil.createPartitionedTable(AUDIT_LOGS_TABLE, colNames, types,
					PARTITION_COLUMN, "MONTH");
			executeSql(createParentSql);
			classLogger.info("Created partitioned parent table {}", AUDIT_LOGS_TABLE);

			// 3. Create DEFAULT partition
			String defaultPartitionSql = String.format("CREATE TABLE IF NOT EXISTS %s_DEFAULT PARTITION OF %s DEFAULT",
					AUDIT_LOGS_TABLE, AUDIT_LOGS_TABLE);
			executeSql(defaultPartitionSql);
			classLogger.info("Created DEFAULT partition for historic data");

			// 4. Create partitions covering historic data + future months
			createPartitionsForDateRange(getHistoricDateRange());

			// 5. Copy data from old table (simplified - would need batch processing in
			// production)
			copyDataFromOldTable();

			// 6. Create indexes on partitions
			createPartitionIndexes();

			classLogger.info("PostgreSQL conversion completed successfully");

		} catch (SQLException e) {
			classLogger.error("Conversion failed: " + e.getMessage(), e);
			// Attempt rollback
			attemptRollback();
			throw e;
		}
	}

	/**
	 * Create initial partitions for the next N months
	 */
	private void createInitialPartitions(DatabaseType dbType, String tableName) throws SQLException {
		LocalDate startDate = LocalDate.now().withDayOfMonth(1);
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

		for (int i = 0; i < DEFAULT_PARTITION_COUNT; i++) {
			LocalDate partitionStart = startDate.plusMonths(i);
			LocalDate partitionEnd = partitionStart.plusMonths(1);

			String partitionName = String.format("%s_%d_%02d", tableName, partitionStart.getYear(),
					partitionStart.getMonthValue());

			try {
				String partitionSql = queryUtil.addTablePartition(tableName, partitionName,
						partitionStart.format(formatter));
				executeSql(partitionSql);
				classLogger.info("Created partition {}", partitionName);

				// Create indexes on the partition
				createPartitionIndex(partitionName, "PROJECT_ID");
				createPartitionIndex(partitionName, "USER_ID");
				createPartitionIndex(partitionName, "ENGINE_ID");

			} catch (SQLException e) {
				classLogger.warn("Failed to create partition {}: {}", partitionName, e.getMessage());
			}
		}
	}

	/**
	 * Create date range covering historic data + future months
	 */
	private void createPartitionsForDateRange(DateRange range) throws SQLException {
		LocalDate cursor = range.getStart();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

		while (!cursor.isAfter(range.getEnd().plusMonths(DEFAULT_PARTITION_COUNT))) {
			LocalDate next = cursor.plusMonths(1);
			String partitionName = String.format("%s_%d_%02d", AUDIT_LOGS_TABLE, cursor.getYear(),
					cursor.getMonthValue());

			try {
				String partitionSql = queryUtil.addTablePartition(AUDIT_LOGS_TABLE, partitionName,
						cursor.format(formatter));
				executeSql(partitionSql);
				classLogger.info("Created/ensured partition {}", partitionName);
			} catch (SQLException e) {
				classLogger.warn("Could not create partition {}: {}", partitionName, e.getMessage());
			}
			cursor = next;
		}
	}

	// Helper methods (simplified for clarity)
	private String getDatabaseProductName() throws SQLException {
		try {
			return conn.getMetaData().getDatabaseProductName();
		} catch (SQLException e) {
			classLogger.warn("Could not determine database product name: " + e.getMessage());
			return "Unknown";
		}
	}

	private DatabaseType detectDatabaseType(String dbProduct) {
		String lowerProduct = dbProduct.toLowerCase();
		if (lowerProduct.contains("postgresql")) {
			return DatabaseType.POSTGRES;
		} else if (lowerProduct.contains("mysql")) {
			return DatabaseType.MYSQL;
		} else if (lowerProduct.contains("microsoft sql") || lowerProduct.contains("sql server")) {
			return DatabaseType.MSSQL;
		} else {
			return DatabaseType.OTHER;
		}
	}

	private List<Pair<String, String>> findAuditLogsSchema(List<Pair<String, List<Pair<String, String>>>> dbSchema) {
		return dbSchema.stream().filter(table -> AUDIT_LOGS_TABLE.equalsIgnoreCase(table.getValue0()))
				.map(Pair::getValue1).findFirst().orElse(null);
	}

	private boolean isPostgresTablePartitioned() throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			String checkSql = "SELECT 1 FROM pg_partitioned_table pt " + "JOIN pg_class c ON pt.partrelid = c.oid "
					+ "WHERE c.relname = 'audit_logs'";
			try (java.sql.ResultSet rs = stmt.executeQuery(checkSql)) {
				return rs.next();
			}
		} catch (SQLException e) {
			classLogger.warn("Failed to check partition status: " + e.getMessage());
			return false;
		}
	}

	private boolean isSqlServerTablePartitioned() throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			String checkSql = "SELECT COUNT(DISTINCT partition_id) AS pcount FROM sys.partitions "
					+ "WHERE object_id = OBJECT_ID('AUDIT_LOGS')";
			try (java.sql.ResultSet rs = stmt.executeQuery(checkSql)) {
				if (rs.next()) {
					int partitionCount = rs.getInt("pcount");
					return partitionCount > 1; // More than 1 partition means it's partitioned
				}
			}
		} catch (SQLException e) {
			classLogger.warn("Failed to check SQL Server partition status: " + e.getMessage());
			return false;
		}
		return false;
	}

	private DateRange getHistoricDateRange() throws SQLException {
		// Simplified - would query the actual min/max dates from AUDIT_LOGS_OLD
		LocalDate minDate = LocalDate.now().minusMonths(6).withDayOfMonth(1);
		LocalDate maxDate = LocalDate.now().withDayOfMonth(1);
		return new DateRange(minDate, maxDate);
	}

	private void copyDataFromOldTable() throws SQLException {
		// Simplified - production version would do batch processing
		String copySql = String.format("INSERT INTO %s SELECT * FROM %s_OLD", AUDIT_LOGS_TABLE, AUDIT_LOGS_TABLE);
		executeSql(copySql);
		classLogger.info("Copied data from {}_OLD to {}", AUDIT_LOGS_TABLE, AUDIT_LOGS_TABLE);
	}

	private void createPartitionIndexes() throws SQLException {
		// Create indexes on recent partitions
		LocalDate startDate = LocalDate.now().withDayOfMonth(1);
		for (int i = 0; i < DEFAULT_PARTITION_COUNT; i++) {
			LocalDate partitionDate = startDate.plusMonths(i);
			String partitionName = String.format("%s_%d_%02d", AUDIT_LOGS_TABLE, partitionDate.getYear(),
					partitionDate.getMonthValue());

			createPartitionIndex(partitionName, "PROJECT_ID");
			createPartitionIndex(partitionName, "USER_ID");
			createPartitionIndex(partitionName, "ENGINE_ID");
		}
	}

	private void createPartitionIndex(String partitionName, String columnName) throws SQLException {
		String indexName = String.format("IDX_%s_%s_TS", partitionName, columnName);
		String indexSql = String.format("CREATE INDEX IF NOT EXISTS %s ON %s (%s, %s)", indexName, partitionName,
				columnName, PARTITION_COLUMN);
		try {
			executeSql(indexSql);
		} catch (SQLException e) {
			classLogger.warn("Failed to create index {}: {}", indexName, e.getMessage());
		}
	}

	private void attemptRollback() {
		try {
			executeSql("DROP TABLE IF EXISTS " + AUDIT_LOGS_TABLE);
			executeSql(String.format("ALTER TABLE IF EXISTS %s_OLD RENAME TO %s", AUDIT_LOGS_TABLE, AUDIT_LOGS_TABLE));
			classLogger.info("Rollback completed: restored original table");
		} catch (SQLException rollbackEx) {
			classLogger.error("Rollback failed: " + rollbackEx.getMessage(), rollbackEx);
		}
	}

	private void executeSql(String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			classLogger.debug("Executing SQL: {}", sql);
			stmt.execute(sql);
		}
	}

	// Helper classes
	private enum DatabaseType {
		POSTGRES(true), MYSQL(true), MSSQL(true), OTHER(false);

		private final boolean partitioningSupported;

		DatabaseType(boolean partitioningSupported) {
			this.partitioningSupported = partitioningSupported;
		}

		public boolean isPartitioningSupported() {
			return partitioningSupported;
		}
	}

	private static class DateRange {
		private final LocalDate start;
		private final LocalDate end;

		public DateRange(LocalDate start, LocalDate end) {
			this.start = start;
			this.end = end;
		}

		public LocalDate getStart() {
			return start;
		}

		public LocalDate getEnd() {
			return end;
		}
	}

	/**
	 * Ensure SQL Server partition boundaries exist for next N months
	 */
	private void ensureSqlServerPartitionBoundaries() throws SQLException {
		// Use the SQL Server specific method to split partitions
		if (queryUtil instanceof prerna.util.sql.MicrosoftSqlServerQueryUtil) {
			prerna.util.sql.MicrosoftSqlServerQueryUtil msSqlUtil = (prerna.util.sql.MicrosoftSqlServerQueryUtil) queryUtil;

			java.util.List<String> splitStatements = msSqlUtil.splitPartitions(DEFAULT_PARTITION_COUNT);
			for (String sql : splitStatements) {
				try {
					executeSql(sql);
					classLogger.info("Added SQL Server partition boundary");
				} catch (SQLException e) {
					// Split may fail if boundary already exists - this is normal
					classLogger.debug("Partition boundary split returned: " + e.getMessage());
				}
			}
		}
	}

	/**
	 * Convert existing SQL Server table to partitioned table
	 */
	private void convertSqlServerTableToPartitioned(List<Pair<String, String>> columns) throws SQLException {
		classLogger.info("Converting existing SQL Server AUDIT_LOGS table to partitioned table.");

		// Safety check
		if (queryUtil.tableExists(engine, AUDIT_LOGS_TABLE + "_OLD", database, schema)) {
			throw new SQLException("AUDIT_LOGS_OLD table already exists. Aborting to prevent data loss.");
		}

		// Build column definitions for SQL Server
		StringBuilder colDefs = new StringBuilder();
		for (int i = 0; i < columns.size(); i++) {
			Pair<String, String> col = columns.get(i);
			colDefs.append(col.getValue0()).append(" ").append(col.getValue1());
			if (i < columns.size() - 1) {
				colDefs.append(", ");
			}
		}

		try {
			// Use SQL Server specific conversion method
			if (queryUtil instanceof prerna.util.sql.MicrosoftSqlServerQueryUtil) {
				prerna.util.sql.MicrosoftSqlServerQueryUtil msSqlUtil = (prerna.util.sql.MicrosoftSqlServerQueryUtil) queryUtil;

				// Get date range for partitions
				DateRange range = getHistoricDateRange();

				// Get conversion statements
				java.util.List<String> conversionStatements = msSqlUtil.convertToPartitionedTable(colDefs.toString(),
						range.getStart(), range.getEnd().plusMonths(DEFAULT_PARTITION_COUNT));

				// Execute each statement
				for (String sql : conversionStatements) {
					executeSql(sql);
					classLogger.info("Executed SQL Server conversion step");
				}

				classLogger.info("SQL Server conversion completed successfully");
			} else {
				classLogger.warn("SQL Server query utility not available for conversion");
			}

		} catch (SQLException e) {
			classLogger.error("SQL Server conversion failed: " + e.getMessage(), e);
			attemptRollback();
			throw e;
		}
	}
}