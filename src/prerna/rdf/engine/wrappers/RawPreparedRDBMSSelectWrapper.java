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
package prerna.rdf.engine.wrappers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.interpreters.sql.ParameterizedSqlInterpreter.CompiledQuery;
import prerna.usertracking.UserQueryTrackingThread;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.QueryExecutionUtility.ParameterizedQuery;

/**
 * Streams a compiled SELECT through JDBC prepared statements, retaining the SQL
 * templates and ordered bindings for data, count, and reset operations.
 * Inherits row conversion, headers, types, and the iterator contract from
 * {@link RawRDBMSSelectWrapper}; this class owns the prepared execution
 * lifecycle.
 *
 * <p>
 * Use the factories to choose engine-acquired or caller-owned connections and
 * close the returned wrapper after use. Ownership is fixed at construction:
 * inherited connection-close flags do not change it. Neither execution nor
 * cleanup changes auto-commit or commits a transaction.
 */
public class RawPreparedRDBMSSelectWrapper extends RawRDBMSSelectWrapper {

	/**
	 * Data-query SQL template, ordered JDBC bindings, and maximum-row limit.
	 * Retained for deferred execution and reset so neither operation needs to
	 * reconstruct values from SQL.
	 */
	private final ParameterizedQuery parameterizedQuery;

	/**
	 * Compiler-generated count template with its own bindings. SQL pagination is
	 * included, so it counts the returned page rather than an unrestricted result
	 * set.
	 */
	private final ParameterizedQuery parameterizedCountQuery;

	/**
	 * Connection supplied by the caller. The wrapper closes its own statements and
	 * results but never commits, rolls back, changes auto-commit, or closes this
	 * connection. It is also reused for count queries and reset.
	 */
	private final Connection borrowedConnection;

	/**
	 * True for engine-acquired execution, where the wrapper manages the read
	 * lifecycle and releases pooled connections. False for a borrowed connection,
	 * regardless of its auto-commit mode. Shared non-pooled engine connections
	 * remain open.
	 */
	private final boolean ownsReadTransaction;

	/**
	 * True only when this wrapper owns the read and its connection has auto-commit
	 * disabled. Cleanup then rolls back to end that read transaction. This records
	 * the existing mode; execution does not change auto-commit or commit reads.
	 */
	private boolean manualReadTransaction;

	/**
	 * Whether numRows contains a successfully computed count, including zero.
	 * Cleared on execution/reset so an old count is not reused for a new execution.
	 */
	private boolean parameterizedCountKnown;

	/**
	 * JDBC statement timeout applied to data and count statements. Zero means no
	 * statement timeout; this is not a limit on how long a caller may retain the
	 * wrapper.
	 */
	private final int parameterizedTimeoutSeconds;

	/**
	 * Accepts only the retained SQL template so bindings cannot become detached.
	 *
	 * @param query SQL template already associated with this wrapper
	 * @throws IllegalStateException if a different template is supplied
	 */
	@Override
	public void setQuery(String query) {
		if (!parameterizedQuery.sql().equals(query)) {
			throw new IllegalStateException("Create a new compiled wrapper to change its SQL or bindings");
		}
		super.setQuery(query);
	}

	/**
	 * Accepts only the original engine, preserving connection ownership and
	 * dialect.
	 *
	 * @param engine engine already associated with this wrapper
	 * @throws IllegalStateException if a different engine is supplied
	 */
	@Override
	public void setEngine(IDatabaseEngine engine) {
		if (this.engine != engine) {
			throw new IllegalStateException("A compiled wrapper must retain its database and connection ownership");
		}
		super.setEngine(engine);
	}

	/**
	 * Retains the compiled plans and fixes ownership before the wrapper is exposed.
	 * No JDBC resources are acquired until {@link #execute()} is called.
	 *
	 * @param database       engine supplying connections and result metadata
	 *                       conventions
	 * @param connection     caller-owned connection, or null to acquire from the
	 *                       engine
	 * @param compiled       data and count plans with their ordered values
	 * @param timeoutSeconds non-negative JDBC statement timeout; zero disables it
	 * @throws NullPointerException     if the database or compiled plans are null
	 * @throws IllegalArgumentException if the timeout is negative
	 */
	private RawPreparedRDBMSSelectWrapper(IRDBMSEngine database, Connection connection, CompiledQuery compiled,
			int timeoutSeconds) {
		this.engine = Objects.requireNonNull(database, "database");
		Objects.requireNonNull(compiled, "compiled query");
		if (timeoutSeconds < 0) {
			throw new IllegalArgumentException("Query timeout must be non-negative");
		}
		this.parameterizedQuery = compiled.query();
		this.parameterizedCountQuery = compiled.countQuery();
		this.query = parameterizedQuery.sql();
		this.borrowedConnection = connection;
		this.ownsReadTransaction = connection == null;
		this.parameterizedTimeoutSeconds = timeoutSeconds;
		this.closedConnection = true;
	}

	/**
	 * Creates a prepared wrapper without acquiring a connection or executing SQL.
	 * Calling execute starts the engine-owned read lifecycle; close or exhaustion
	 * ends any owned manual transaction and releases pooled connections. Do not
	 * interleave unrelated work on a shared engine connection while the wrapper is
	 * open.
	 *
	 * @param database       non-null engine supplying connections and result
	 *                       metadata conventions
	 * @param compiled       non-null data and count plans with their ordered
	 *                       parameter values
	 * @param timeoutSeconds non-negative timeout for data and count statements;
	 *                       zero means no JDBC statement timeout
	 * @return an unexecuted wrapper retaining both compiled plans
	 * @throws NullPointerException     if the database or compiled plan is null
	 * @throws IllegalArgumentException if the timeout is negative
	 */
	public static RawPreparedRDBMSSelectWrapper prepareParameterized(IRDBMSEngine database, CompiledQuery compiled,
			int timeoutSeconds) {
		return new RawPreparedRDBMSSelectWrapper(database, null, compiled, timeoutSeconds);
	}

	/**
	 * Executes a compiled query using an engine-acquired connection. The wrapper
	 * owns read cleanup and must be closed by the caller after consuming the
	 * results.
	 *
	 * @param database       engine supplying the connection and result metadata
	 *                       conventions
	 * @param query          compiled data and count plans with their ordered
	 *                       bindings
	 * @param timeoutSeconds non-negative JDBC statement timeout; zero means no
	 *                       timeout
	 * @return an executed wrapper that streams the result rows
	 * @throws Exception if arguments are invalid or execution/resource
	 *                   initialization fails
	 */
	public static RawPreparedRDBMSSelectWrapper executeParameterized(IRDBMSEngine database, CompiledQuery query,
			int timeoutSeconds) throws Exception {
		RawPreparedRDBMSSelectWrapper wrapper = prepareParameterized(database, query, timeoutSeconds);
		wrapper.execute();
		return wrapper;
	}

	/**
	 * Executes a compiled query using the caller's connection. The returned wrapper
	 * closes only its statements and results, including on failure. It never
	 * commits, rolls back, changes auto-commit, or closes the supplied connection.
	 *
	 * @param database       engine supplying result metadata conventions and
	 *                       tracking identity
	 * @param connection     non-null connection whose transaction and lifecycle
	 *                       remain with the caller
	 * @param query          compiled data and count plans with their ordered
	 *                       bindings
	 * @param timeoutSeconds non-negative JDBC statement timeout; zero means no
	 *                       timeout
	 * @return an executed streaming wrapper that the caller must close
	 * @throws Exception if arguments are invalid or execution/resource
	 *                   initialization fails
	 */
	public static RawPreparedRDBMSSelectWrapper executeParameterized(IRDBMSEngine database, Connection connection,
			CompiledQuery query, int timeoutSeconds) throws Exception {
		Objects.requireNonNull(connection, "connection");
		RawPreparedRDBMSSelectWrapper wrapper = new RawPreparedRDBMSSelectWrapper(database, connection, query,
				timeoutSeconds);
		wrapper.execute();
		return wrapper;
	}

	/**
	 * Closes the previous execution, clears row/count state, and prepares and binds
	 * the retained data query. Acquires an engine connection or reuses the borrowed
	 * one according to ownership. Used for both initial execution and reset;
	 * cleanup also runs if preparation, binding, execution, or metadata
	 * initialization fails.
	 *
	 * @throws Exception if execution or resource initialization/cleanup fails
	 */
	@Override
	public void execute() throws Exception {
		close();
		this.conn = null;
		this.stmt = null;
		this.rs = null;
		this.currRow = null;
		this.manualReadTransaction = false;
		this.parameterizedCountKnown = false;
		this.numRows = 0;
		this.closedConnection = false;
		UserQueryTrackingThread tracker = WrapperManager.preparedQueryTracker(engine, parameterizedQuery.sql());
		try {
			if (tracker != null) {
				tracker.setStartTimeNow();
			}
			IRDBMSEngine database = (IRDBMSEngine) this.engine;
			this.conn = ownsReadTransaction ? database.getConnection() : borrowedConnection;
			// Observe the existing mode without changing auto-commit. A borrowed
			// connection never becomes our transaction to finish, even in manual mode.
			this.manualReadTransaction = ownsReadTransaction && !this.conn.getAutoCommit();
			this.databaseZoneId = database.getDatabaseZoneId();
			PreparedStatement prepared = conn.prepareStatement(parameterizedQuery.sql());
			this.stmt = prepared;
			prepared.setMaxRows(parameterizedQuery.limit());
			prepared.setQueryTimeout(parameterizedTimeoutSeconds);
			bindParameterized(prepared, parameterizedQuery);
			this.rs = prepared.executeQuery();
			setVariables();
		} catch (Exception | Error failure) {
			if (tracker != null) {
				tracker.setFailed();
			}
			closeAfterParameterizedFailure(failure);
			throw failure;
		} finally {
			if (tracker != null) {
				WrapperManager.finishPreparedQueryTracking(tracker);
			}
		}
	}

	/**
	 * Binds a plan's values in list order using one-based JDBC parameter positions.
	 * The same operation is used for data and count plans with their respective
	 * lists.
	 *
	 * @param statement prepared statement whose placeholders match the plan
	 * @param bindings  plan containing the ordered values, including nulls
	 * @throws SQLException if the driver rejects a parameter binding
	 */
	private void bindParameterized(PreparedStatement statement, ParameterizedQuery bindings) throws SQLException {
		for (int i = 0; i < bindings.parameters().size(); i++) {
			statement.setObject(i + 1, bindings.parameters().get(i));
		}
	}

	/**
	 * Executes and caches the independent count plan, including a legitimate zero.
	 * Uses the live execution connection when available. After engine-owned
	 * execution has closed, counting acquires a separate engine-managed read; a
	 * borrowed connection remains the caller's responsibility in every case.
	 *
	 * @return the count including SQL pagination, capped by a positive JDBC row
	 *         limit
	 * @throws IllegalArgumentException if the count cannot be obtained
	 */
	@Override
	public long getNumRows() {
		if (!this.parameterizedCountKnown) {
			// The compiler supplies an independent template and bindings for counting.
			String countSql = parameterizedCountQuery.sql();
			QueryExecutionUtility.StatementBinder binder = statement -> {
				statement.setQueryTimeout(parameterizedTimeoutSeconds);
				bindParameterized(statement, parameterizedCountQuery);
			};
			UserQueryTrackingThread tracker = WrapperManager.preparedQueryTracker(engine, countSql);
			try {
				if (tracker != null) {
					tracker.setStartTimeNow();
				}
				long count;
				if (closedConnection && ownsReadTransaction) {
					count = QueryExecutionUtility.queryOne((IRDBMSEngine) engine, countSql, binder,
							result -> result.getLong(1));
				} else {
					Connection countConnection = ownsReadTransaction ? conn : borrowedConnection;
					count = QueryExecutionUtility.queryOne(countConnection, countSql, binder,
							result -> result.getLong(1));
				}
				this.numRows = parameterizedQuery.limit() > 0 ? Math.min(count, parameterizedQuery.limit()) : count;
				this.parameterizedCountKnown = true;
			} catch (Exception | Error failure) {
				if (tracker != null) {
					tracker.setFailed();
				}
				closeAfterParameterizedFailure(failure);
				if (failure instanceof Error error) {
					throw error;
				}
				throw new IllegalArgumentException("Error counting parameterized query", failure);
			} finally {
				WrapperManager.finishPreparedQueryTracking(tracker);
			}
		}
		return this.numRows;
	}

	/**
	 * Attempts prepared-query cleanup while retaining the original failure. A
	 * distinct cleanup failure is attached as a suppressed exception instead of
	 * replacing it.
	 *
	 * @param failure original execution or counting failure to preserve
	 */
	private void closeAfterParameterizedFailure(Throwable failure) {
		try {
			close();
		} catch (RuntimeException | Error cleanup) {
			// failure is the non-null original exception being propagated by the caller.
			ConnectionUtils.jdbcCleanupFailure(failure, cleanup);
		}
	}

	/**
	 * Closes this execution's result set and statement. An owned manual read is
	 * ended with rollback, and an owned pooled connection is released. Borrowed
	 * connections and their transactions are left to the caller. Every cleanup step
	 * is attempted, and subsequent failures are suppressed onto the first failure.
	 *
	 * @throws IllegalStateException if resource or transaction cleanup fails with a
	 *                               non-Error failure; the first failure is the
	 *                               cause
	 */
	@Override
	public void close() {
		if (this.closedConnection) {
			return;
		}
		this.closedConnection = true;
		this.currRow = null;
		Throwable failure = ConnectionUtils.closeAllConnections(null, this.stmt, this.rs);
		if (this.manualReadTransaction && this.conn != null) {
			failure = ConnectionUtils.jdbcCleanupFailure(failure, ConnectionUtils.rollbackConnection(this.conn));
		}
		if (this.ownsReadTransaction && this.conn != null) {
			failure = ConnectionUtils.jdbcCleanupFailure(failure,
					ConnectionUtils.closeConnectionIfPooling((IRDBMSEngine) engine, this.conn));
		}
		if (failure instanceof Error error) {
			throw error;
		}
		if (failure != null) {
			throw new IllegalStateException("Unable to close parameterized query", failure);
		}
	}

	/**
	 * Reads ahead through the inherited JDBC row converter. Any cursor, conversion,
	 * or exhaustion cleanup failure closes this execution before it propagates.
	 *
	 * @return true if a buffered row is available; false before execution or after
	 *         close
	 * @throws IllegalArgumentException if a JDBC row read or exhaustion cleanup
	 *                                  fails
	 */
	@Override
	public boolean hasNext() {
		if (this.closedConnection) {
			return false;
		}
		try {
			if (this.currRow == null) {
				this.currRow = getNextRow();
			}
			return this.currRow != null;
		} catch (SQLException failure) {
			closeAfterParameterizedFailure(failure);
			throw new IllegalArgumentException("Error reading parameterized query", failure);
		} catch (RuntimeException | Error failure) {
			closeAfterParameterizedFailure(failure);
			throw failure;
		}
	}

	/**
	 * Uses the shared column mappings, failing execution if metadata is
	 * unavailable. The enclosing execute call then releases the resources it
	 * acquired.
	 *
	 * @throws IllegalArgumentException if the driver cannot provide result metadata
	 */
	@Override
	protected void setVariables() {
		try {
			readResultMetadata();
		} catch (SQLException failure) {
			throw new IllegalArgumentException("Unable to read prepared result metadata", failure);
		}
	}

	/**
	 * Re-executes the retained data plan with its original bindings and timeout.
	 * Any previous result is closed and the cached row count is cleared first.
	 *
	 * @throws Exception if cleanup, execution, or result initialization fails
	 */
	@Override
	public void reset() throws Exception {
		execute();
	}

}
