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
package prerna.engine.impl.rdbms;

import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.tools.DeleteDbFiles;

import com.zaxxer.hikari.HikariDataSource;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.SqlTransactionException;
import prerna.engine.api.SqlTransactionResult;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.parsers.SqlScriptSplitter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.rdf.util.AbstractQueryParser;
import prerna.rdf.util.SQLQueryParser;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class RDBMSNativeEngine extends AbstractDatabaseEngine implements IRDBMSEngine {

	private static final Logger classLogger = LogManager.getLogger(RDBMSNativeEngine.class);

	public static final String USE_FILE = "USE_FILE";
	public static final String DATA_FILE = "DATA_FILE";

	/** The most results one statement of a script is read for */
	private static final int MAX_STATEMENT_RESULTS = 10_000;
	/** The most rows a script's statement asks the driver to fetch at a time */
	private static final int MAX_FETCH_SIZE = 1_000;
	/** A CREATE statement, in any case and after any leading whitespace */
	private static final Pattern CREATE_STATEMENT = Pattern.compile("\\s*CREATE\\b", Pattern.CASE_INSENSITIVE);
	/** A CREATE DATABASE statement, in any case and with any whitespace between */
	private static final Pattern CREATE_DATABASE_STATEMENT = Pattern.compile("\\s*CREATE\\s+DATABASE\\b",
			Pattern.CASE_INSENSITIVE);

	private boolean engineConnected = false;
	private boolean datasourceConnected = false;
	protected RdbmsTypeEnum dbType;
	protected HikariDataSource dataSource = null;
	protected Connection engineConn = null;
	private boolean useConnectionPooling = false;

	private String userName = null;
	private String password = null;
	private String driver = null;
	private String originalConnectionURL = null;
	private String connectionURL = null;
	private String database = null;
	private String schema = null;
	// parameterized in SMSS
	// fetch size -1 which will
	private int fetchSize = -1;
	private int poolMinSize = 1;
	private int poolMaxSize = 16;
	private int queryTimeout = -1;
	private Boolean autoCommit = null;
	private String connectionTestQuery = null;
	private int transactionIsolationType = -1;

	private long leakDetectionThresholdMilliseconds = 30_000;
	private long idelTimeout = 60_000;

	private AbstractSqlQueryUtil queryUtil = null;

	private String fileDB = null;
	private Map<String, String> fileConceptAndType = null;
	private String fileCreateString = null;

	/** The rows kept from one statement */
	private record RowsRead(List<String> headers, List<Object[]> rows, boolean hasMoreRows) {
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		// grab the values from the prop file
		String dbTypeString = this.smssProp.getProperty(Constants.RDBMS_TYPE);
		if (dbTypeString == null) {
			dbTypeString = this.smssProp.getProperty(AbstractSqlQueryUtil.DRIVER_NAME);
		}
		this.driver = this.smssProp.getProperty(Constants.DRIVER);
		// get the dbType from the input or from the driver itself
		this.dbType = (dbTypeString != null) ? RdbmsTypeEnum.getEnumFromString(dbTypeString)
				: RdbmsTypeEnum.getEnumFromDriver(this.driver);
		if (this.dbType == null) {
			this.dbType = RdbmsTypeEnum.H2_DB;
		}
		// override the driver based on the db type enum
		this.driver = this.dbType.getDriver();

		// make the query util first
		// since this will help with getting the correct keys for the connection
		this.queryUtil = SqlQueryUtilFactory.initialize(this.dbType);

		// get the database so we have it - can be used for filtering tables/columns
		this.database = this.smssProp.getProperty(AbstractSqlQueryUtil.DATABASE);

		// get the schema so we have it - can be used for filtering tables/columns
		this.schema = this.smssProp.getProperty(AbstractSqlQueryUtil.SCHEMA);

		// grab the username/password
		// keys can be username/password
		// but some will have it as accessKey/secretKey
		// so accounting for that here
		this.userName = this.smssProp.getProperty(queryUtil.getConnectionUserKey());
		this.password = this.smssProp.getProperty(queryUtil.getConnectionPasswordKey());

		// grab the connection url
		this.connectionURL = this.smssProp.getProperty(Constants.CONNECTION_URL);
		try {
			// update the query util values
			// also
			// if the connection url is not defined
			// use the query util to construct the connection url from the parts
			// if this fails ignore, it will through an error when trying to establish the
			// connection
			String queryUtilConnectionURL = this.queryUtil.setConnectionDetailsFromSMSS(this.smssProp);
			if (this.connectionURL == null || (this.connectionURL = this.connectionURL.trim()).isEmpty()) {
				this.connectionURL = queryUtilConnectionURL;
			}
		} catch (Exception ignore) {

		}
		if (this.dbType == RdbmsTypeEnum.H2_DB || this.dbType == RdbmsTypeEnum.SQLITE) {
			this.connectionURL = this.queryUtil.fillFileParameterizedConnectionUrl(this.connectionURL, this.engineId,
					this.engineName);
			this.smssProp.put(Constants.CONNECTION_URL, this.connectionURL);
		}
		this.originalConnectionURL = this.connectionURL;

		// make a check to see if it is asking to use file
		boolean useFile = false;
		if (this.smssProp.containsKey(USE_FILE)) {
			useFile = Boolean.valueOf(this.smssProp.getProperty(USE_FILE));
		}

		// see if connection pooling
		this.useConnectionPooling = Boolean.valueOf(this.smssProp.getProperty(Constants.USE_CONNECTION_POOLING));

		// fetch size
		if (this.smssProp.getProperty(Constants.FETCH_SIZE) != null) {
			String fetchSizeStr = this.smssProp.getProperty(Constants.FETCH_SIZE);
			if (!(fetchSizeStr = fetchSizeStr.trim()).isEmpty()) {
				try {
					this.fetchSize = Integer.parseInt(fetchSizeStr);
				} catch (Exception e) {
					classLogger.warn("Error occurred trying to parse and get the fetch size");
					classLogger.error("Failed to parse fetch size value '{}': {}", fetchSizeStr, e.getMessage(), e);
				}
			}
		}
		// connection query timeout
		if (this.smssProp.getProperty(Constants.CONNECTION_QUERY_TIMEOUT) != null) {
			String queryTimeoutStr = this.smssProp.getProperty(Constants.CONNECTION_QUERY_TIMEOUT);
			if (!(queryTimeoutStr = queryTimeoutStr.trim()).isEmpty()) {
				try {
					this.queryTimeout = Integer.parseInt(queryTimeoutStr);
				} catch (Exception e) {
					classLogger.warn("Error occurred trying to parse and get the query timeout");
					classLogger.error("Failed to parse query timeout value '{}': {}", queryTimeoutStr, e.getMessage(),
							e);
				}
			}
		}
		// auto commit connection
		if (this.smssProp.getProperty(Constants.AUTO_COMMIT) != null) {
			String autoCommitStr = this.smssProp.getProperty(Constants.AUTO_COMMIT);
			if (!(autoCommitStr = autoCommitStr.trim()).isEmpty()) {
				this.autoCommit = Boolean.parseBoolean(autoCommitStr);
			}
		}
		// connection test query
		if (this.smssProp.getProperty(Constants.CONNECTION_TEST_QUERY) != null) {
			String connectionTestQuery = this.smssProp.getProperty(Constants.CONNECTION_TEST_QUERY);
			if (!(connectionTestQuery = connectionTestQuery.trim()).isEmpty()) {
				this.connectionTestQuery = connectionTestQuery;
			}
		}
		// connection transaction type
		if (this.smssProp.getProperty(Constants.TRANSACTION_TYPE) != null) {
			this.transactionIsolationType = AbstractSqlQueryUtil
					.getConnectionTypeValueFromString(this.smssProp.getProperty(Constants.TRANSACTION_TYPE) + "");
		}

		// leak detection threshold
		if (this.smssProp.getProperty(Constants.LEAK_DETECTION_THRESHOLD_MILLISECONDS) != null) {
			String leakDetectionStr = this.smssProp.getProperty(Constants.LEAK_DETECTION_THRESHOLD_MILLISECONDS);
			if (!(leakDetectionStr = leakDetectionStr.trim()).isEmpty()) {
				try {
					this.leakDetectionThresholdMilliseconds = Long.parseLong(leakDetectionStr);
				} catch (Exception e) {
					classLogger.warn("Error occurred trying to parse and get the leak detection threshold");
					classLogger.error("Failed to parse leak detection threshold value '{}': {}", leakDetectionStr,
							e.getMessage(), e);
				}
			}
		}
		// idle timeout
		if (this.smssProp.getProperty(Constants.IDLE_TIMEOUT) != null) {
			String idleTimeoutStr = this.smssProp.getProperty(Constants.IDLE_TIMEOUT);
			if (!(idleTimeoutStr = idleTimeoutStr.trim()).isEmpty()) {
				try {
					this.idelTimeout = Long.parseLong(idleTimeoutStr);
				} catch (Exception e) {
					classLogger.warn("Error occurred trying to parse and get the idle timeout");
					classLogger.error("Failed to parse idle timeout value '{}': {}", idleTimeoutStr, e.getMessage(), e);
				}
			}
		}
		// pool min size
		if (this.smssProp.getProperty(Constants.POOL_MIN_SIZE) != null) {
			String minPoolSizeStr = this.smssProp.getProperty(Constants.POOL_MIN_SIZE);
			if (!(minPoolSizeStr = minPoolSizeStr.trim()).isEmpty()) {
				try {
					this.poolMinSize = Integer.parseInt(minPoolSizeStr);
				} catch (Exception e) {
					classLogger.warn("Error occurred trying to parse and get the min pool size");
					classLogger.error("Failed to parse min pool size value '{}': {}", minPoolSizeStr, e.getMessage(),
							e);
				}
			}
		}
		// pool max size
		if (this.smssProp.getProperty(Constants.POOL_MAX_SIZE) != null) {
			String maxPoolSizeStr = this.smssProp.getProperty(Constants.POOL_MAX_SIZE);
			if (!(maxPoolSizeStr = maxPoolSizeStr.trim()).isEmpty()) {
				try {
					this.poolMaxSize = Integer.parseInt(maxPoolSizeStr);
				} catch (Exception e) {
					classLogger.warn("Error occurred trying to parse and get the max pool size");
					classLogger.error("Failed to parse max pool size value '{}': {}", maxPoolSizeStr, e.getMessage(),
							e);
				}
			}
		}

		try {
			// account for files
			if (useFile) {
				// also update the connection url
				Hashtable<String, String> paramHash = new Hashtable<String, String>();
				String dbName = this.fileDB.replace(".csv", "").replace(".tsv", "");
				paramHash.put("database", dbName);
				this.connectionURL = Utility.fillParam2(connectionURL, paramHash);

				// set the types
				Vector<String> concepts = this.getConcepts();
				this.fileConceptAndType = new HashMap<>();
				for (int conceptIndex = 0; conceptIndex < concepts.size(); conceptIndex++) {
					List<String> propList = getPropertyUris4PhysicalUri(concepts.get(conceptIndex));
					String[] propArray = propList.toArray(new String[propList.size()]);
					Map<String, String> typeMap = getDataTypes(propArray);
					this.fileConceptAndType.putAll(typeMap);
				}

				this.fileCreateString = RdbmsQueryBuilder.createTableFromFile(this.fileDB, this.fileConceptAndType);

				// this makes the connection and creates the table
				makeConnectionFromFile(dbType.getDriver(), this.userName, this.password, this.connectionURL,
						this.fileCreateString);
			}

			// init - example of this is H2Server where we spin up and have a new connection
			// url
			String initUrl = init(this.originalConnectionURL);
			if (initUrl != null) {
				this.connectionURL = initUrl;
			}

			// update the query utility values
			// note the smss values are already added previously
			this.queryUtil.setConnectionUrl(this.connectionURL);
			// if we are connection pooling
			if (useConnectionPooling) {
				this.dataSource = RdbmsConnectionHelper.getDataSourceFromPool(this.driver,
						this.queryUtil.getConnectionUrl(), this.userName, this.password);
				setDataSourceProperties(this.dataSource);
				this.datasourceConnected = true;
				classLogger.info("Established connection pooling for {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId));
			} else {
				this.engineConn = AbstractSqlQueryUtil.makeConnection(this.queryUtil, this.connectionURL,
						this.smssProp);
				if (this.autoCommit != null) {
					this.engineConn.setAutoCommit(this.autoCommit);
				}
				if (this.transactionIsolationType > -1) {
					this.engineConn.setTransactionIsolation(this.transactionIsolationType);
				}
				this.queryUtil.enhanceConnection(this.engineConn);
				classLogger.info("Established connection for {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId));
			}
			this.engineConnected = true;
		} catch (SQLException e) {
			classLogger.error("Failed to establish database connection during open: {}", e.getMessage(), e);
		}
	}

	/**
	 * This is for when there are other engines that extend the base
	 * RDBMSNativeEngine that need to do additional processing before a connection
	 * can be made
	 * 
	 * @param connectionUrl
	 * @return
	 */
	protected String init(String connectionUrl) {
		// default does nothing
		return init(connectionUrl, false);
	}

	/**
	 * This is for when there are other engines that extend the base
	 * RDBMSNativeEngine that need to do additional processing before a connection
	 * can be made
	 * 
	 * @param connectionUrl
	 * @param force         force the init againrce
	 * @return
	 */
	protected String init(String connectionUrl, boolean force) {
		// default does nothing
		return null;
	}

	protected void setDataSourceProperties(HikariDataSource dataSource) {
		if (this.autoCommit != null) {
			dataSource.setAutoCommit(this.autoCommit);
		}
		dataSource.setMinimumIdle(this.poolMinSize);
		dataSource.setMaximumPoolSize(this.poolMaxSize);
		dataSource.setLeakDetectionThreshold(this.leakDetectionThresholdMilliseconds);
		dataSource.setIdleTimeout(this.idelTimeout);
		if (this.connectionTestQuery != null && !this.connectionTestQuery.isEmpty()) {
			dataSource.setConnectionTestQuery(this.connectionTestQuery);
		}
	}

	@Override
	public AbstractSqlQueryUtil getQueryUtil() {
		return this.queryUtil;
	}

	@Override
	public String getDatabase() {
		return this.database;
	}

	@Override
	public String getSchema() {
		// if not set in the prop file
		// try to grab from the connection details
		if (this.schema == null) {
			Connection conn = null;
			try {
				conn = getConnection();
				DatabaseMetaData meta = conn.getMetaData();
				this.schema = RdbmsConnectionHelper.getSchema(meta, conn, this.connectionURL, this.dbType);
			} catch (SQLException e) {
				classLogger.error("Failed to retrieve database schema from connection metadata: {}", e.getMessage(), e);
			} finally {
				if (this.datasourceConnected && conn != null) {
					try {
						conn.close();
					} catch (SQLException e) {
						classLogger.error("Failed to close connection after schema retrieval: {}", e.getMessage(), e);
					}
				}
			}
		}
		return this.schema;
	}

	private void makeConnectionFromFile(String driver, String userName, String password, String connectionUrl,
			String createString) {
		Statement stmt = null;
		try {
			Class.forName(driver);
			this.connectionURL = connectionUrl;
			this.userName = userName;
			this.password = password;
			this.engineConn = DriverManager.getConnection(connectionUrl, userName, password);
			if (createString != null) {
				stmt = this.engineConn.createStatement();
				stmt.execute(createString);
			}
			this.engineConnected = true;
			if (this.autoCommit != null) {
				this.engineConn.setAutoCommit(this.autoCommit);
			}
			if (this.transactionIsolationType > -1) {
				this.engineConn.setTransactionIsolation(this.transactionIsolationType);
			}
			this.queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.getEnumFromDriver(driver), this.connectionURL,
					this.userName, this.password);
			this.queryUtil.setConnectionUrl(this.connectionURL);
			this.queryUtil.setUsername(this.userName);
			this.queryUtil.setPassword(this.password);
			this.queryUtil.setDatabase(this.database);
			this.queryUtil.setSchema(this.schema);
		} catch (ClassNotFoundException e) {
			classLogger.error("Failed to load JDBC driver class '{}': {}", driver, e.getMessage(), e);
		} catch (SQLException e) {
			classLogger.error("Failed to establish connection from file or execute create statement: {}",
					e.getMessage(), e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close statement after file-based connection setup: {}", e.getMessage(),
							e);
				}
			}
		}
	}

	// you cant change owl right now
	public void reloadFile() {
		try {
			this.engineConn.close();
			makeConnectionFromFile(this.driver, this.userName, this.password, this.connectionURL,
					this.fileCreateString);
		} catch (SQLException e) {
			classLogger.error("Failed to close existing connection while reloading file-based database: {}",
					e.getMessage(), e);
		}
	}

	@Override
	public HikariDataSource getDataSource() {
		return this.dataSource;
	}

	@Override
	public Connection getConnection() throws SQLException {
		if (this.dataSource != null) {
			// re-establish bad connections
			if (this.dataSource.isClosed()) {
				String initUrl = init(this.originalConnectionURL, true);
				if (initUrl != null) {
					this.connectionURL = initUrl;
					this.queryUtil.setConnectionUrl(this.connectionURL);
				}
				this.dataSource = RdbmsConnectionHelper.getDataSourceFromPool(driver, this.queryUtil.getConnectionUrl(),
						userName, password);
				setDataSourceProperties(this.dataSource);
				this.datasourceConnected = true;
				classLogger.info("Established connection pooling for {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId));
			}

			// return/generate a connection object
			Connection conn = dataSource.getConnection();
			if (this.autoCommit != null) {
				conn.setAutoCommit(this.autoCommit);
			}
			if (this.transactionIsolationType > -1) {
				conn.setTransactionIsolation(this.transactionIsolationType);
			}
			this.queryUtil.enhanceConnection(conn);
			return conn;
		}

		// re-establish bad connections
		if (!isConnected() || this.engineConn.isClosed() || !testIsConnectionValid(this.engineConn)) {
			String initUrl = init(this.originalConnectionURL, true);
			if (initUrl != null) {
				this.connectionURL = initUrl;
				this.queryUtil.setConnectionUrl(this.connectionURL);
			}
			if (useConnectionPooling) {
				this.dataSource = RdbmsConnectionHelper.getDataSourceFromPool(driver, this.queryUtil.getConnectionUrl(),
						this.userName, this.password);
				setDataSourceProperties(this.dataSource);
				this.engineConn = this.dataSource.getConnection();
				this.datasourceConnected = true;
				classLogger.info("Established connection pooling for {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId));
			} else {
				this.engineConn = AbstractSqlQueryUtil.makeConnection(this.queryUtil, this.connectionURL,
						this.smssProp);
				classLogger.info("Established connection for {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId));
			}
			this.queryUtil.enhanceConnection(this.engineConn);
			if (this.autoCommit != null) {
				this.engineConn.setAutoCommit(this.autoCommit);
			}
			if (this.transactionIsolationType > -1) {
				this.engineConn.setTransactionIsolation(this.transactionIsolationType);
			}
			this.engineConnected = true;
		}

		return this.engineConn;
	}

	@Override
	public void insertData(String query) throws SQLException {
		// a CREATE runs as a plain statement: its body (a trigger, procedure or
		// function) can hold text a prepared statement reads as bind markers, such as
		// Oracle's :NEW and :OLD
		boolean plainStatement = CREATE_STATEMENT.matcher(query).lookingAt()
				&& !CREATE_DATABASE_STATEMENT.matcher(query).lookingAt();
		executeWrite(query, plainStatement);
	}

	/**
	 * Runs one write on the engine's connection and commits it when the connection
	 * does not commit on its own. A write whose statement or commit fails is rolled
	 * back before the connection is used again, so no caller runs inside the failed
	 * write's transaction: an engine without a pool shares one connection with
	 * every caller, and PostgreSQL, for one, refuses every statement in a
	 * transaction a statement failed in until it ends. The write's own failure is
	 * thrown, with a failed rollback added to it as suppressed. A pooled connection
	 * goes back to its pool either way; a failure to give it back is logged and
	 * never changes the write's outcome. An engine without a pool holds the lock on
	 * its shared connection for the whole write, the same lock
	 * QueryExecutionUtility takes, so another caller's transaction on that
	 * connection is never committed or rolled back by this write.
	 *
	 * @param query          the SQL to run
	 * @param plainStatement true to run the SQL as a plain statement, false to run
	 *                       it as a prepared statement
	 * @throws SQLException when the statement or its commit fails
	 */
	private void executeWrite(String query, boolean plainStatement) throws SQLException {
		Connection conn = getConnection();
		if (this.dataSource != null) {
			executeWrite(conn, query, plainStatement);
			return;
		}
		synchronized (conn) {
			executeWrite(conn, query, plainStatement);
		}
	}

	/**
	 * Runs the write on the connection, commits or rolls it back, and gives a
	 * pooled connection back to its pool, as described in
	 * {@link #executeWrite(String, boolean)}.
	 *
	 * @param conn           the connection the write runs on
	 * @param query          the SQL to run
	 * @param plainStatement true to run the SQL as a plain statement, false to run
	 *                       it as a prepared statement
	 * @throws SQLException when the statement or its commit fails
	 */
	private void executeWrite(Connection conn, String query, boolean plainStatement) throws SQLException {
		try {
			if (plainStatement) {
				try (Statement statement = conn.createStatement()) {
					statement.executeUpdate(query);
				}
			} else {
				try (PreparedStatement statement = conn.prepareStatement(query)) {
					statement.execute();
				}
			}
			// you have to commit on the connection itself
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException | RuntimeException | Error e) {
			classLogger.error("Failed to execute write statement: {}", e.getMessage(), e);
			rollbackFailedWrite(conn, e);
			releaseWriteConnection(conn, e);
			throw e;
		}
		releaseWriteConnection(conn, null);
	}

	/**
	 * Rolls back what a failed write left open when the connection does not commit
	 * on its own. When the connection cannot say whether it commits on its own, the
	 * rollback is tried anyway. A failure here is logged and added to the write's
	 * failure as suppressed.
	 *
	 * @param conn    the connection the write ran on
	 * @param failure the write's failure
	 */
	private static void rollbackFailedWrite(Connection conn, Throwable failure) {
		try {
			if (conn.getAutoCommit()) {
				return;
			}
		} catch (SQLException | RuntimeException e) {
			ConnectionUtils.jdbcCleanupFailure(failure, e, "Error reading the commit mode after a failed write");
		}
		ConnectionUtils.jdbcCleanupFailure(failure, ConnectionUtils.rollbackConnection(conn));
	}

	/**
	 * Gives a pooled connection back to its pool once a write is done. An engine
	 * without a pool keeps its shared connection open.
	 *
	 * @param conn    the connection the write ran on
	 * @param failure the write's failure, which a failure to give the connection
	 *                back is added to as suppressed, or null when the write
	 *                committed
	 */
	private void releaseWriteConnection(Connection conn, Throwable failure) {
		if (this.dataSource != null) {
			ConnectionUtils.jdbcCleanupFailure(failure, ConnectionUtils.closeConnection(conn));
		}
	}

	@Override
	public SqlTransactionResult executeInTransaction(List<String> statements, int timeoutSeconds, int maxRows)
			throws SQLException {
		if (statements == null || statements.isEmpty()) {
			throw new IllegalArgumentException("At least one SQL statement is required");
		}
		if (timeoutSeconds < 0) {
			throw new IllegalArgumentException("The timeout cannot be negative");
		}
		SqlTransactionResult result = new SqlTransactionResult(statements.size());
		Connection connection = openTransactionConnection();
		try {
			refuseSelfCommittingStatements(connection, statements);
		} catch (SQLException | RuntimeException e) {
			// nothing ran and auto-commit is untouched, so the connection goes back as is
			ConnectionUtils.closeConnection(connection);
			throw e;
		}
		Boolean originalAutoCommit = null;
		boolean inTransaction = false;
		boolean transactionEnded = false;
		try {
			originalAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			inTransaction = true;

			// the limit covers the statements, so it starts once the connection is ready
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
			SQLException failure = runStatements(connection, statements, result, deadline, timeoutSeconds, maxRows);
			if (failure != null || result.getFailedStatement() != null) {
				Throwable rollbackFailure = ConnectionUtils.rollbackConnection(connection);
				transactionEnded = rollbackFailure == null;
				if (rollbackFailure == null) {
					result.rolledBack(null);
				} else {
					// the connection is closed next, and a driver such as Oracle's commits what
					// is pending when its connection closes
					classLogger.error("Failed to roll back the SQL script on database {}", this.engineId,
							rollbackFailure);
					result.unknown("A statement failed and the rollback did not report back, so whether the "
							+ "changes of the statements before it were kept is unknown: "
							+ rollbackFailure.getMessage());
				}
				throw new SqlTransactionException(result, failure);
			}

			try {
				connection.commit();
			} catch (SQLException e) {
				String sqlState = e.getSQLState();
				if (sqlState == null || sqlState.startsWith("08")) {
					// the connection was lost, so the commit may or may not have happened
					result.unknown("The commit did not report back, so whether the changes were kept is unknown: "
							+ e.getMessage());
				} else {
					// the database rejected the commit, for example for a deferred constraint;
					// roll back so no part of the transaction can stay open
					Throwable rollbackFailure = ConnectionUtils.rollbackConnection(connection);
					if (rollbackFailure == null) {
						transactionEnded = true;
						result.rolledBack("The database rejected the commit: " + e.getMessage());
					} else {
						classLogger.error("Failed to roll back the SQL script on database {} after its commit failed",
								this.engineId, rollbackFailure);
						result.unknown("The database rejected the commit and the rollback failed, so whether the "
								+ "changes were kept is unknown: " + e.getMessage());
					}
				}
				throw new SqlTransactionException(result, e);
			}
			transactionEnded = true;
			result.committed();
			return result;
		} catch (RuntimeException | Error e) {
			// an unexpected failure once statements could have run: roll them back before
			// the connection is closed or evicted, since a driver such as Oracle's commits
			// what is pending when its connection closes
			if (inTransaction && !transactionEnded) {
				ConnectionUtils.jdbcCleanupFailure(e, ConnectionUtils.rollbackConnection(connection));
			}
			throw e;
		} finally {
			releaseTransactionConnection(connection, transactionEnded, originalAutoCommit);
		}
	}

	/**
	 * Refuses a script of more than one statement with a statement that does not
	 * only read or change rows, such as CREATE TABLE, LOCK TABLES or CALL, when the
	 * database commits data definition as soon as it runs. Such a database can
	 * commit other statements on their own too, and that commit would keep the
	 * statements before it whatever happened after, so the script could not run in
	 * one transaction. The driver reports whether the database does this.
	 *
	 * @throws SQLFeatureNotSupportedException when the database commits data
	 *                                         definition on its own and a statement
	 *                                         does not only read or change rows
	 * @throws IllegalArgumentException        when the database commits data
	 *                                         definition on its own and a statement
	 *                                         cannot be read to tell
	 */
	private static void refuseSelfCommittingStatements(Connection connection, List<String> statements)
			throws SQLException {
		if (!connection.getMetaData().dataDefinitionCausesTransactionCommit()) {
			return;
		}
		List<List<SqlScriptSplitter.Statement>> parts = new ArrayList<>(statements.size());
		int partCount = 0;
		for (int index = 0; index < statements.size(); index++) {
			try {
				parts.add(SqlScriptSplitter.split(statements.get(index)));
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(
						"Statement " + (index + 1) + " cannot be checked for what it changes: " + e.getMessage(), e);
			}
			partCount += parts.get(index).size();
		}
		// a script of one statement commits or fails as a whole, whatever the
		// statement is
		if (partCount <= 1) {
			return;
		}
		for (int index = 0; index < parts.size(); index++) {
			for (SqlScriptSplitter.Statement part : parts.get(index)) {
				if (part.isDataDefinition()) {
					throw new SQLFeatureNotSupportedException("Statement " + (index + 1) + " changes the database's "
							+ "structure or permissions, which this database commits as soon as the statement runs, "
							+ "so it cannot run in one transaction with other statements", "0A000");
				}
				if (!part.isRowStatement()) {
					throw new SQLFeatureNotSupportedException("Statement " + (index + 1) + " is not a query or an "
							+ "insert, update, delete or merge, and this database can commit such a statement as soon "
							+ "as it runs, so it cannot run in one transaction with other statements", "0A000");
				}
			}
		}
	}

	/**
	 * Runs each statement in order on the transaction's connection, stopping at the
	 * first failure or when the time limit is reached.
	 *
	 * @param deadline       the {@link System#nanoTime()} the statements must end
	 *                       by, read only when there is a timeout
	 * @param timeoutSeconds the time limit, or 0 for none
	 * @param maxRows        the most rows to keep from each statement that returns
	 *                       rows; 0 keeps none, and a negative number keeps them
	 *                       all
	 * @return the error that stopped the statements, or null when they all ran or
	 *         the time limit stopped them before one ran
	 */
	private SQLException runStatements(Connection connection, List<String> statements, SqlTransactionResult result,
			long deadline, int timeoutSeconds, int maxRows) {
		boolean timed = timeoutSeconds > 0;
		for (int index = 0; index < statements.size(); index++) {
			long startedAt = System.nanoTime();
			int secondsLeft = 0;
			if (timed) {
				long remaining = deadline - startedAt;
				if (remaining <= 0) {
					result.failed(index, null,
							"The script reached its " + timeoutSeconds + " second timeout before this statement ran",
							0);
					return null;
				}
				// round up so the driver never cancels before the deadline
				long second = TimeUnit.SECONDS.toNanos(1);
				secondsLeft = (int) Math.max(1, (remaining + second - 1) / second);
			}
			// a plain statement, so a ? in the SQL is never read as a parameter
			try (Statement statement = connection.createStatement()) {
				if (secondsLeft > 0) {
					statement.setQueryTimeout(secondsLeft);
				}
				fetchRowsInBatches(statement, maxRows);
				boolean isResultSet = statement.execute(withoutTerminator(statements.get(index)));
				boolean returnedRows = isResultSet;
				Integer updateCount = null;
				RowsRead rowsRead = isResultSet && maxRows != 0 ? readRows(statement.getResultSet(), maxRows) : null;
				// read every result, since some drivers report an error in a later result,
				// such as one a trigger raised, only once it is read
				for (int read = 0; read < MAX_STATEMENT_RESULTS; read++) {
					int count = isResultSet ? -1 : statement.getUpdateCount();
					if (!isResultSet && count == -1) {
						break;
					}
					if (read == 0 && count >= 0) {
						updateCount = count;
					}
					isResultSet = statement.getMoreResults();
				}
				result.succeeded(index, returnedRows, updateCount,
						TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
				if (rowsRead != null) {
					result.keptRows(index, rowsRead.headers(), rowsRead.rows(), rowsRead.hasMoreRows());
				}
			} catch (SQLException e) {
				// drivers report a cancelled statement differently, so a failure once the
				// deadline has passed is the timeout
				boolean timedOut = timed && System.nanoTime() - deadline >= 0;
				String message = timedOut
						? "The script reached its " + timeoutSeconds + " second timeout: " + e.getMessage()
						: e.getMessage();
				result.failed(index, e.getSQLState(), message,
						TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
				return e;
			} catch (RuntimeException e) {
				String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
				result.failed(index, null, message, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
				return new SQLException(message, e);
			}
		}
		return null;
	}

	/**
	 * A statement without its final semicolon, which some drivers, such as
	 * Oracle's, reject.
	 */
	private static String withoutTerminator(String sql) {
		String trimmed = sql.stripTrailing();
		return trimmed.endsWith(";") ? trimmed.substring(0, trimmed.length() - 1) : sql;
	}

	/**
	 * Asks the driver to fetch rows in batches no bigger than what is kept, plus
	 * one row to tell whether more were left out, so a driver that streams rows, as
	 * PostgreSQL's does with auto-commit off, never loads a whole result to keep a
	 * few rows. A driver that does not take the hint runs the statement as it
	 * would.
	 */
	private static void fetchRowsInBatches(Statement statement, int maxRows) {
		int fetchSize = maxRows < 0 ? MAX_FETCH_SIZE : Math.min(maxRows, MAX_FETCH_SIZE - 1) + 1;
		try {
			statement.setFetchSize(fetchSize);
		} catch (SQLException | RuntimeException e) {
			classLogger.debug("The driver does not take a fetch size; reading the statement's rows as it sends them",
					e);
		}
	}

	/**
	 * Keeps up to {@code maxRows} rows of a statement's result, each value
	 * converted as an engine query converts it, so dates, CLOBs and BLOBs come back
	 * the same. Only the result set is closed, once its last row is read; the
	 * statement stays open for the results after it.
	 *
	 * @param maxRows the most rows to keep, or a negative number for all of them
	 */
	private RowsRead readRows(ResultSet resultSet, int maxRows) throws SQLException {
		RawRDBMSSelectWrapper wrapper = RawRDBMSSelectWrapper.wrapResultSet(resultSet, getDatabaseZoneId());
		List<Object[]> rows = new ArrayList<>();
		boolean hasMoreRows = false;
		while (wrapper.hasNext()) {
			if (maxRows >= 0 && rows.size() >= maxRows) {
				hasMoreRows = true;
				break;
			}
			rows.add(wrapper.next().getValues());
		}
		return new RowsRead(Arrays.asList(wrapper.getHeaders()), rows, hasMoreRows);
	}

	/**
	 * Opens a connection only the transaction uses. A pooled engine lends one from
	 * its pool; otherwise a new connection is opened with the engine's settings, so
	 * the shared engine connection is never part of the transaction.
	 */
	private Connection openTransactionConnection() throws SQLException {
		if (this.dataSource != null) {
			return getConnection();
		}
		Connection connection = AbstractSqlQueryUtil.makeConnection(this.queryUtil, this.connectionURL, this.smssProp);
		try {
			if (this.transactionIsolationType > -1) {
				connection.setTransactionIsolation(this.transactionIsolationType);
			}
			this.queryUtil.enhanceConnection(connection);
		} catch (SQLException | RuntimeException e) {
			ConnectionUtils.closeConnection(connection);
			throw e;
		}
		return connection;
	}

	/**
	 * Restores and releases the transaction's connection. A pooled connection whose
	 * transaction did not end cleanly is evicted instead of returned, so no one is
	 * handed a connection with an open transaction.
	 */
	private void releaseTransactionConnection(Connection connection, boolean transactionEnded,
			Boolean originalAutoCommit) {
		boolean clean = transactionEnded;
		if (clean && originalAutoCommit != null) {
			try {
				connection.setAutoCommit(originalAutoCommit);
			} catch (SQLException e) {
				clean = false;
				classLogger.error("Failed to restore auto-commit after the SQL script on database {}", this.engineId,
						e);
			}
		}
		if (!clean && this.dataSource != null) {
			this.dataSource.evictConnection(connection);
			return;
		}
		ConnectionUtils.closeConnection(connection);
	}

	/**
	 * 
	 * @param conn
	 * @param rs
	 * @param stmt
	 */
	private void closeConnections(Connection conn, ResultSet rs, Statement stmt) {
		if (isConnected()) {
			conn = null;
		}
		ConnectionUtils.closeAllConnections(conn, stmt, null);
	}

	/**
	 * 
	 * @param conn
	 * @return
	 * @throws SQLException
	 */
	private boolean testIsConnectionValid(Connection conn) throws SQLException {
		if (this.connectionTestQuery == null || this.connectionTestQuery.isEmpty()) {
			return conn.isValid(1);
		}

		try (PreparedStatement statment = conn.prepareStatement(this.connectionTestQuery);
				ResultSet rs = statment.executeQuery()) {
			// if you can execute, you are valid
			return true;
		} catch (Exception e) {
			classLogger.warn("Connection is not valid: the connection test query failed for database {}",
					SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
		}

		return false;
	}

	@Override
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.RDBMS;
	}

	@Override
	public Map<String, Object> execQuery(String query) throws SQLException {
		Map<String, Object> map = new HashMap<>();
		boolean hasError = false;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			if (this.queryTimeout > 0) {
				stmt.setQueryTimeout(queryTimeout);
			}
			rs = null;
			// System.out.println("PRINT QUERY: "+ query);
			rs = stmt.executeQuery(query);
			if (this.fetchSize > 0) {
				try {
					rs.setFetchSize(this.fetchSize);
				} catch (Exception e) {
					classLogger.error("Failed to set fetch size on result set: {}", e.getMessage(), e);
				}
			}
		} catch (SQLTimeoutException e) {
			classLogger.error("Query execution timed out: {}", e.getMessage(), e);
			hasError = true;
			if (this.queryTimeout > 0) {
				throw new SQLTimeoutException(
						"Query execution cancelled - processing time took longer than set limit of " + this.queryTimeout
								+ " seconds");
			}
			int statementTimeout = -1;
			try {
				if (stmt != null) {
					statementTimeout = stmt.getQueryTimeout();
				}
			} catch (Exception e2) {
				// ignore
			}
			if (statementTimeout > 0) {
				throw new SQLTimeoutException(
						"Query execution cancelled - processing time took longer than set limit of " + statementTimeout
								+ " seconds");
			}
			throw e;
		} catch (SQLException e) {
			classLogger.warn("Error with query {}", query);
			classLogger.error("Failed to execute query: {}", e.getMessage(), e);
			hasError = true;
			throw e;
		} finally {
			if (hasError) {
				if (this.dataSource != null) {
					ConnectionUtils.closeAllConnections(conn, stmt, rs);
				} else {
					ConnectionUtils.closeAllConnections(null, stmt, rs);
				}
			}
		}
		// normally would use instance.getClass() but when we retrieve the
		// references from the object we can't guarantee that they will not be null
		// this makes it cleaner and less error prone.
		map.put(IRDBMSEngine.RESULTSET_OBJECT, rs);
		if (isConnected()) {
			map.put(IRDBMSEngine.CONNECTION_OBJECT, null);
			map.put(IRDBMSEngine.ENGINE_CONNECTION_OBJECT, conn);
		} else {
			map.put(IRDBMSEngine.CONNECTION_OBJECT, conn);
			map.put(IRDBMSEngine.ENGINE_CONNECTION_OBJECT, null);
		}
		if (this.dataSource != null) {
			map.put(IRDBMSEngine.DATASOURCE_POOLING_OBJECT, this.dataSource);
		}
		map.put(IRDBMSEngine.STATEMENT_OBJECT, stmt);
		return map;
	}

	@Override
	public boolean isConnected() {
		if (this.useConnectionPooling) {
			return this.dataSource != null && this.datasourceConnected;
		}
		return engineConn != null && this.engineConnected;
	}

	@Override
	public boolean isConnectionPooling() {
		return this.useConnectionPooling;
	}

	@Override
	public void close() throws IOException {
		super.close();
		try {
			if (this.useConnectionPooling) {
				this.engineConnected = false;
				ConnectionUtils.closeConnection(this.engineConn);
				closeDataSource();
			} else {
				if (this.engineConn != null && !this.engineConn.isClosed()) {
					if (!this.engineConn.getAutoCommit()) {
						this.engineConn.commit();
					}
					this.shutdown();
					this.engineConn.close();
					this.engineConnected = false;
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to close database connection: {}", e.getMessage(), e);
			throw new IOException("Error closing the connection to the database");
		}
	}

	@Override
	public void closeDataSource() {
		if (this.dataSource != null) {
			try {
				this.dataSource.close();
				this.datasourceConnected = false;
			} catch (Exception e) {
				classLogger.error("Failed to close HikariCP data source: {}", e.getMessage(), e);
			}
		}
	}

	public AbstractQueryParser getQueryParser() {
		return new SQLQueryParser();
	}

	protected void shutdown() {
		// shift to grabing from query util
		if (this.dbType == RdbmsTypeEnum.H2_DB) {
			Connection conn = null;
			Statement stmt = null;
			try {
				conn = getConnection();
				stmt = conn.createStatement();
				stmt.execute("SHUTDOWN");
				// return to pool
			} catch (Exception e) {
				classLogger.error("Unable to shutdown.", e);
			} finally {
				if (stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e) {
						classLogger.error("Failed to close statement after H2 SHUTDOWN: {}", e.getMessage(), e);
					}
				}
			}
		}
	}

	@Override
	public void removeData(String query) throws SQLException {
		executeWrite(query, false);
	}

	@Override
	public void commit() {
		try {
			if (this.datasourceConnected && this.dataSource != null) {
				classLogger.warn(
						"You are using commit with connection pooling - this is a mistake! You must commit on the specific connection object being used");
				return;
			}
			Connection conn = getConnection();
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to commit transaction: {}", e.getMessage(), e);
		}
	}

	@Override
	public void delete() {
		classLogger.debug("Deleting RDBMS Engine: {}", this.engineName);
		try {
			close();
			if (this.dbType == RdbmsTypeEnum.H2_DB) {
				String path = EngineUtility.getSpecificEngineBaseFolder(getCatalogType(), this.engineId,
						this.engineName);
				DeleteDbFiles.execute(path, "database", false);
			}
		} catch (Exception e) {
			classLogger.error("Failed to delete RDBMS engine '{}': {}", this.engineName, e.getMessage(), e);
		}
		// clean up remaining files
		super.delete();
	}

	@Override
	public RdbmsTypeEnum getDbType() {
		return this.dbType;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
		if (this.engineConn != null) {
			try {
				this.engineConn.setAutoCommit(this.autoCommit);
			} catch (SQLException e) {
				classLogger.error("Failed to set auto-commit to '{}' on connection: {}", autoCommit, e.getMessage(), e);
			}
		}
	}

	public void setTransactionIsolationType(int transactionIsolationType) {
		this.transactionIsolationType = transactionIsolationType;
		if (this.engineConn != null) {
			try {
				if (this.transactionIsolationType > -1) {
					this.engineConn.setTransactionIsolation(this.transactionIsolationType);
				}
			} catch (SQLException e) {
				classLogger.error("Failed to set transaction isolation type '{}' on connection: {}",
						transactionIsolationType, e.getMessage(), e);
			}
		}
	}

	@Override
	public java.sql.PreparedStatement bulkInsertPreparedStatement(Object[] args) throws SQLException {
		if (this.datasourceConnected && this.dataSource != null) {
			classLogger.warn(
					"You are using bulkInsertPreparedStatement with connection pooling - this method creates a connection that must be closed");
		}
		// if a table name and a column name are not specified, do nothing
		// not enough information to be meaningful
		if (args.length < 2) {
			return null;
		}

		// generate the sql for the prepared statement
		String tableName = (String) args[0];
		if (this.queryUtil.isSelectorKeyword(tableName)) {
			tableName = this.queryUtil.getEscapeKeyword(tableName);
		}

		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(tableName).append(" (");
		for (int colIndex = 1; colIndex < args.length; colIndex++) {
			String columnName = (String) args[colIndex];
			if (this.queryUtil.isSelectorKeyword(columnName)) {
				columnName = this.queryUtil.getEscapeKeyword(columnName);
			}
			sql.append(columnName);
			if ((colIndex + 1) != args.length) {
				sql.append(", ");
			}
		}
		sql.append(") VALUES (?");
		// remember, we already assumed one col and first index is table name
		for (int colIndex = 1; colIndex < args.length - 1; colIndex++) {
			sql.append(", ?");
		}
		sql.append(")");

		return getPreparedStatement(sql.toString());
	}

	@Override
	public java.sql.PreparedStatement getPreparedStatement(String sql) throws SQLException {
		if (this.datasourceConnected && this.dataSource != null) {
			classLogger.warn(
					"You are using getPreparedStatement with connection pooling - this method creates a connection that must be closed");
		}
		boolean error = false;
		Connection conn = null;
		try {
			conn = getConnection();
			return conn.prepareStatement(sql);
		} catch (SQLException e) {
			error = true;
			classLogger.error("Failed to create prepared statement for SQL: {}", e.getMessage(), e);
			throw e;
		} finally {
			if (error) {
				if (this.datasourceConnected && this.dataSource != null && conn != null) {
					conn.close();
				}
			}
		}
	}

	@Override
	public DatabaseMetaData getConnectionMetadata() {
		if (this.datasourceConnected && this.dataSource != null) {
			classLogger.warn(
					"You are using getConnectionMetadata with connection pooling - this method creates a connection that must be closed");
		}
		boolean error = false;
		Connection conn = null;
		try {
			conn = getConnection();
			return conn.getMetaData();
		} catch (SQLException e) {
			error = true;
			classLogger.error("Failed to retrieve connection metadata: {}", e.getMessage(), e);
		} finally {
			if (error) {
				if (this.datasourceConnected && this.dataSource != null && conn != null) {
					try {
						conn.close();
					} catch (SQLException e) {
						classLogger.error("Failed to close connection after metadata retrieval error: {}",
								e.getMessage(), e);
					}
				}
			}
		}
		return null;
	}

	@Override
	public IQueryInterpreter getQueryInterpreter() {
		return this.queryUtil.getInterpreter(this);
	}

	@Override
	public void setConnection(Connection engineConn) {
		this.engineConn = engineConn;
		try {
			this.engineConnected = !this.engineConn.isClosed();
			this.autoCommit = this.engineConn.getAutoCommit();
		} catch (SQLException e) {
			classLogger.error("Failed to read connection state while setting engine connection: {}", e.getMessage(), e);
		}
	}

	@Override
	public Clob createClob(Connection connection) throws SQLException {
		return connection.createClob();
	}

	@Override
	public void setQueryUtil(AbstractSqlQueryUtil queryUtil) {
		this.queryUtil = queryUtil;
	}

	@Override
	public String getConnectionUrl() {
		return this.connectionURL;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		if (this.dbType == null) {
			String dbTypeString = smssProp.getProperty(Constants.RDBMS_TYPE);
			if (dbTypeString == null) {
				dbTypeString = smssProp.getProperty(AbstractSqlQueryUtil.DRIVER_NAME);
			}
			String driver = smssProp.getProperty(Constants.DRIVER);
			// get the dbType from the input or from the driver itself
			this.dbType = (dbTypeString != null) ? RdbmsTypeEnum.getEnumFromString(dbTypeString)
					: RdbmsTypeEnum.getEnumFromDriver(driver);
		}
		return this.dbType.getLabel();
	}

	@Override
	public boolean holdsFileLocks() {
		if (this.dbType == null) {
			String dbTypeString = smssProp.getProperty(Constants.RDBMS_TYPE);
			if (dbTypeString == null) {
				dbTypeString = smssProp.getProperty(AbstractSqlQueryUtil.DRIVER_NAME);
			}
			String driver = smssProp.getProperty(Constants.DRIVER);
			// get the dbType from the input or from the driver itself
			this.dbType = (dbTypeString != null) ? RdbmsTypeEnum.getEnumFromString(dbTypeString)
					: RdbmsTypeEnum.getEnumFromDriver(driver);
		}

		// will assume all h2 are file locks (might not be the case but 99% of the
		// time...)
		// and then sqlite is always file based
		if (this.dbType == RdbmsTypeEnum.H2_DB || this.dbType == RdbmsTypeEnum.SQLITE) {
			return true;
		}

		return false;
	}

	/*
	 * 
	 * BELOW METHODS USE RDF JARS
	 * 
	 */

	@Override
	public Vector<Object> getEntityOfType(String type) {
		String table; // table in RDBMS
		String column; // column of table in RDBMS
		String query;

		// ugh... for legacy stuff, we do not have the table name on the property
		// so we need to do the check that the type is not "contains"
		if (type.contains("http://semoss.org/ontologies") && !Utility.getClassName(type).equals("Contains")) {
			// we are dealing with the physical uri which is in the form
			// ...Concept/Column/Table
			query = "SELECT DISTINCT " + Utility.getClassName(type) + " FROM " + Utility.getInstanceName(type);
		} else if (type.contains("http://semoss.org/ontologies/Relation/Contains")) {// this is such a mess...
			String xmlQuery = "SELECT ?concept WHERE { ?concept rdfs:subClassOf <http://semoss.org/ontologies/Concept>. ?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> <"
					+ type + ">}";
			org.openrdf.query.TupleQueryResult ret = (org.openrdf.query.TupleQueryResult) this
					.execOntoSelectQuery(xmlQuery);
			String conceptURI = null;
			try {
				if (ret.hasNext()) {
					org.openrdf.query.BindingSet row = ret.next();
					conceptURI = row.getBinding("concept").getValue().toString();
				}
			} catch (org.openrdf.query.QueryEvaluationException e) {
				classLogger.error("Failed to evaluate ontology query for type '{}': {}", type, e.getMessage(), e);
			}
			query = "SELECT DISTINCT " + Utility.getInstanceName(type) + " FROM " + Utility.getInstanceName(conceptURI);
		} else if (type.contains(":")) {
			int tableStartIndex = type.indexOf("-") + 1;
			int columnStartIndex = type.indexOf(":") + 1;
			table = type.substring(tableStartIndex, columnStartIndex - 1);
			column = type.substring(columnStartIndex);
			query = "SELECT DISTINCT " + column + " FROM " + table;
		} else {
			query = "SELECT DISTINCT " + type + " FROM " + type;
		}
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement stmt = null;
		try {
			conn = getConnection();
			stmt = conn.prepareStatement(query);
			rs = stmt.executeQuery();
			Vector<Object> columnsFromResult = getColumnsFromResultSet(1, rs);
			return columnsFromResult;
		} catch (Exception e) {
			classLogger.error("Failed to execute entity-of-type query for type '{}': {}", type, e.getMessage(), e);
		} finally {
			closeConnections(conn, rs, stmt);
		}
		return null;
	}

	public Vector getColumnsFromResultSet(int columns, ResultSet rs) {
		Vector retVector = new Vector();
		// each value is an array in itself as well
		try {
			while (rs.next()) {
				ArrayList list = new ArrayList();
				Object output = null;
				for (int colIndex = 1; colIndex <= columns; colIndex++) {
					// output = rs.getString(colIndex);
					output = rs.getObject(colIndex);
					// System.out.print(rs.getObject(colIndex));
					list.add(output);
				}
				if (columns == 1) {
					retVector.addElement(output);
				} else {
					retVector.addElement(list);
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to read columns from result set: {}", e.getMessage(), e);
		}
		classLogger.info("Found {} elements in result set", retVector.size());
		return retVector;
	}

	// traverse from a type to a type
	public String traverseOutputQuery(String fromType, String toType, List<String> fromInstances) {
		/*
		 * 1. Get the relation for the type 2. For every relation create a join 3. If
		 * Properties are included get the properties 4. Add the properties 5. For
		 * every, type
		 */
		IQueryInterpreter builder = getQueryInterpreter();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.setEngine(this);

		String fromTableName = Utility.getInstanceName(fromType);
		String toTableName = Utility.getInstanceName(toType);
		qs.addSelector(fromTableName, SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
		qs.addSelector(toTableName, SelectQueryStruct.PRIM_KEY_PLACEHOLDER);

		// determine relationship order
		String relationQuery = "SELECT ?relation WHERE {" + "{" + "<" + fromType + "> ?relation <" + toType + ">}"
				+ "{?relation <" + org.apache.jena.vocabulary.RDFS.subClassOf
				+ "> <http://semoss.org/ontologies/Relation>}" + "}";

		String relationName = getRelation(relationQuery);
		if (relationName != null && relationName.length() != 0) {
			qs.addRelation(fromTableName, toTableName, "inner.join");
		} else {
			qs.addRelation(toTableName, fromTableName, "inner.join");
		}

		if (fromInstances != null) {
			// convert instances to simple instance
			List<String> simpleFromInstances = new Vector<String>();
			for (int fromIndex = 0; fromIndex < fromInstances.size(); fromIndex++) {
				simpleFromInstances.add(Utility.getInstanceName(fromInstances.get(fromIndex)));
			}
			NounMetadata lComparison = new NounMetadata(fromTableName, PixelDataType.COLUMN);
			NounMetadata rComparison = new NounMetadata(simpleFromInstances, PixelDataType.CONST_STRING);
			IQueryFilter simple = new SimpleQueryFilter(lComparison, "==", rComparison);
			qs.addExplicitFilter(simple);
		}

		String retQuery = builder.composeQuery();
		return retQuery;
	}

	private String getRelation(String query) {
		String relation = null;
		try {
			org.openrdf.query.TupleQueryResult tqr = (org.openrdf.query.TupleQueryResult) execOntoSelectQuery(query);
			while (tqr.hasNext()) {
				org.openrdf.query.BindingSet bs = tqr.next();
				relation = bs.getBinding("relation").getValue() + "";
				if (!relation.equalsIgnoreCase("http://semoss.org/ontologies/Relation")) {
					break;
				}
			}
		} catch (org.openrdf.query.QueryEvaluationException e) {
			classLogger.error("Failed to evaluate ontology relation query: {}", e.getMessage(), e);
		}

		return relation;
	}

}
