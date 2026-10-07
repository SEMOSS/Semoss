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
package prerna.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;

/**
 * Closes JDBC resources and completes explicitly requested transactions while
 * retaining failures for the caller. Methods return null on success; otherwise
 * they return the first failure with later failures suppressed. Cleanup
 * continues after both exceptions and errors so one failed close does not leak
 * the remaining resources. Callers may propagate the returned failure or retain
 * log-only behavior by ignoring it.
 *
 * <p>
 * Resources close in acquisition-reverse order: result set, statement, then
 * connection. Pool-aware methods release connections only when the engine
 * reports pooling. A statement's connection is resolved before closing the
 * statement, since some drivers reject getConnection after close.
 */
public class ConnectionUtils {

	private static final Logger classLogger = LogManager.getLogger(ConnectionUtils.class);

	/**
	 * Logs a new failure and combines it with an existing failure. A null cleanup
	 * result represents success and is neither logged nor suppressed.
	 *
	 * @param failure original failure to preserve, or null
	 * @param cleanup newly caught failure, or null if the operation succeeded
	 * @param message description of the failed JDBC operation
	 * @return the original failure with cleanup suppressed, or cleanup if no
	 *         original failure exists; null if both arguments are null
	 */
	public static Throwable jdbcCleanupFailure(Throwable failure, Throwable cleanup, String message) {
		if (cleanup != null) {
			classLogger.error(message, cleanup);
		}
		return jdbcCleanupFailure(failure, cleanup);
	}

	/**
	 * Combines failures that have already been logged by a JDBC helper. Preserves
	 * the original throwable and avoids illegal self-suppression.
	 *
	 * @param failure original failure to preserve, or null
	 * @param cleanup returned cleanup failure, or null on successful cleanup
	 * @return the first failure with any distinct later failure suppressed, or null
	 */
	public static Throwable jdbcCleanupFailure(Throwable failure, Throwable cleanup) {
		if (cleanup == null) {
			return failure;
		}
		if (failure == null) {
			return cleanup;
		}
		if (failure != cleanup) {
			failure.addSuppressed(cleanup);
		}
		return failure;
	}

	/**
	 * Releases a pooled connection, leaving non-pooled engine connections open.
	 *
	 * @param engine     engine that owns the connection; may be null
	 * @param connection connection to release; may be null
	 * @return a logged failure, or null if no operation failed
	 */
	public static Throwable closeConnectionIfPooling(IRDBMSEngine engine, Connection connection) {
		return closeAllConnectionsIfPooling(engine, connection, (Statement[]) null);
	}

	/**
	 * Closes a result set and statement, then releases their connection if pooled.
	 * If no connection is supplied, resolves it from the statement before closing
	 * that statement. A failed lookup does not prevent closing the statement.
	 *
	 * @param engine engine that owns the resources; may be null
	 * @param con    connection to release, or null to derive it from stmt
	 * @param stmt   statement to close; may be null
	 * @param rs     result set to close; may be null
	 * @return the first logged failure with later failures suppressed, or null
	 */
	public static Throwable closeAllConnectionsIfPooling(IRDBMSEngine engine, Connection con, Statement stmt,
			ResultSet rs) {
		Throwable failure = closeResultSet(rs);
		return closePooledStatements(engine, con, new Statement[] { stmt }, failure);
	}

	/**
	 * Closes statements in order, then releases one pooled connection. When con is
	 * null, tries to resolve it from each non-null statement until one provides a
	 * connection. Lookup and close failures are recorded independently.
	 *
	 * @param engine engine that owns the resources; may be null
	 * @param con    connection to release, or null to derive it from a statement
	 * @param stmts  statements to close; the array and its entries may be null
	 * @return the first logged failure with later failures suppressed, or null
	 */
	public static Throwable closeAllConnectionsIfPooling(IRDBMSEngine engine, Connection con, Statement... stmts) {
		return closePooledStatements(engine, con, stmts, null);
	}

	/**
	 * Performs pooled statement cleanup while continuing an existing failure chain.
	 *
	 * @param engine  engine used to determine connection ownership; may be null
	 * @param con     connection to release, or null to derive it from a statement
	 * @param stmts   statements to close; the array and its entries may be null
	 * @param failure previously captured failure, or null
	 * @return the first failure with subsequent failures suppressed, or null
	 */
	private static Throwable closePooledStatements(IRDBMSEngine engine, Connection con, Statement[] stmts,
			Throwable failure) {
		boolean pooling = false;
		try {
			pooling = engine != null && engine.isConnectionPooling();
		} catch (Exception | Error cleanup) {
			failure = jdbcCleanupFailure(failure, cleanup, "Error determining JDBC connection pooling");
		}
		if (stmts != null) {
			for (Statement stmt : stmts) {
				if (stmt == null) {
					continue;
				}
				if (pooling && con == null) {
					try {
						con = stmt.getConnection();
					} catch (Exception | Error cleanup) {
						failure = jdbcCleanupFailure(failure, cleanup, "Error resolving JDBC statement connection");
					}
				}
				failure = jdbcCleanupFailure(failure, closeStatement(stmt));
			}
		}
		if (pooling) {
			failure = jdbcCleanupFailure(failure, closeConnection(con));
		}
		return failure;
	}

	/**
	 * Closes a result set and statement, deriving the connection from the statement
	 * before close and releasing it only for a pooled engine.
	 *
	 * @param engine engine that owns the resources; may be null
	 * @param stmt   statement to close; may be null
	 * @param rs     result set to close; may be null
	 * @return the first logged failure with later failures suppressed, or null
	 */
	public static Throwable closeAllConnectionsIfPooling(IRDBMSEngine engine, Statement stmt, ResultSet rs) {
		return closeAllConnectionsIfPooling(engine, null, stmt, rs);
	}

	/**
	 * Closes a result set, statement, and connection in that order. Every supplied
	 * resource is attempted, even when an earlier close fails.
	 *
	 * @param con  connection to close; may be null
	 * @param stmt statement to close; may be null
	 * @param rs   result set to close; may be null
	 * @return the first logged failure with later failures suppressed, or null
	 */
	public static Throwable closeAllConnections(Connection con, Statement stmt, ResultSet rs) {
		Throwable failure = closeResultSet(rs);
		failure = jdbcCleanupFailure(failure, closeStatement(stmt));
		return jdbcCleanupFailure(failure, closeConnection(con));
	}

	/**
	 * Closes a statement followed by its connection.
	 *
	 * @param con connection to close; may be null
	 * @param ps  statement to close; may be null
	 * @return the first logged failure with later failures suppressed, or null
	 */
	public static Throwable closeAllConnections(Connection con, Statement ps) {
		return closeAllConnections(con, ps, null);
	}

	/**
	 * Closes a result set followed by its statement without closing a connection.
	 *
	 * @param stmt statement to close; may be null
	 * @param rs   result set to close; may be null
	 * @return the first logged failure with later failures suppressed, or null
	 */
	public static Throwable closeAllConnections(Statement stmt, ResultSet rs) {
		return closeAllConnections(null, stmt, rs);
	}

	/**
	 * Closes a result set and captures any failure.
	 *
	 * @param rs result set to close; may be null
	 * @return a logged failure, or null if no operation failed
	 */
	public static Throwable closeResultSet(ResultSet rs) {
		return closeResource(rs, "Error closing JDBC result set");
	}

	/**
	 * Closes a statement and captures any failure.
	 *
	 * @param stmt statement to close; may be null
	 * @return a logged failure, or null if no operation failed
	 */
	public static Throwable closeStatement(Statement stmt) {
		return closeResource(stmt, "Error closing JDBC statement");
	}

	/**
	 * Closes a connection and captures any failure without checking pool ownership.
	 *
	 * @param con connection to close; may be null
	 * @return a logged failure, or null if no operation failed
	 */
	public static Throwable closeConnection(Connection con) {
		return closeResource(con, "Error closing JDBC connection");
	}

	/**
	 * Uses JDBC's standard AutoCloseable contract to capture resource-close
	 * failures in one place without introducing a separate cleanup interface.
	 *
	 * @param resource result set, statement, or connection to close; may be null
	 * @param message  description identifying the resource whose close failed
	 * @return a logged failure, or null if no operation failed
	 */
	private static Throwable closeResource(AutoCloseable resource, String message) {
		if (resource != null) {
			try {
				resource.close();
			} catch (Exception | Error cleanup) {
				return jdbcCleanupFailure(null, cleanup, message);
			}
		}
		return null;
	}

	/**
	 * Releases a connection only when its engine reports pooling.
	 *
	 * @param engine engine that owns the connection; may be null
	 * @param con    connection to release; may be null
	 * @return a logged failure, or null if no operation failed
	 */
	public static Throwable closeAllConnectionsIfPooling(IRDBMSEngine engine, Connection con) {
		return closeConnectionIfPooling(engine, con);
	}

	/**
	 * Closes a statement and releases a pooled connection. If con is null, resolves
	 * the connection from the statement before closing it.
	 *
	 * @param engine engine that owns the resources; may be null
	 * @param con    connection to release, or null to derive it from stmt
	 * @param stmt   statement to close; may be null
	 * @return the first logged failure with later failures suppressed, or null
	 */
	public static Throwable closeAllConnectionsIfPooling(IRDBMSEngine engine, Connection con, Statement stmt) {
		return closeAllConnectionsIfPooling(engine, con, stmt, null);
	}

	/**
	 * Closes a statement and releases its connection if the engine reports pooling.
	 *
	 * @param engine engine that owns the statement; may be null
	 * @param stmt   statement to close; may be null
	 * @return the first logged failure with later failures suppressed, or null
	 */
	public static Throwable closeAllConnectionsIfPooling(IRDBMSEngine engine, Statement stmt) {
		return closeAllConnectionsIfPooling(engine, stmt, null);
	}

	/**
	 * Commits pending work when auto-commit is disabled. Callers must inspect the
	 * returned failure before treating the transaction as successfully committed.
	 * This method does not roll back, change auto-commit, or close the connection.
	 *
	 * @param conn connection whose work should be committed; may be null
	 * @return a logged commit/state-read failure, or null if no operation failed
	 */
	public static Throwable commitConnection(Connection conn) {
		if (conn != null) {
			try {
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
			} catch (Exception | Error failure) {
				return jdbcCleanupFailure(null, failure, "Error committing JDBC transaction");
			}
		}
		return null;
	}

	/**
	 * Rolls back an owned transaction without changing auto-commit or closing the
	 * connection. The caller must determine transaction ownership and whether a
	 * rollback is required; no auto-commit check is made here.
	 *
	 * @param conn connection whose transaction should end; may be null
	 * @return a logged rollback failure, or null if no operation failed
	 */
	public static Throwable rollbackConnection(Connection conn) {
		if (conn != null) {
			try {
				conn.rollback();
			} catch (Exception | Error failure) {
				return jdbcCleanupFailure(null, failure, "Error rolling back JDBC transaction");
			}
		}
		return null;
	}

	/**
	 * Closes each statement and releases its own connection if pooling is enabled.
	 * Unlike the single-connection varargs overload, statements may belong to
	 * different connections. Each connection is resolved before its statement
	 * closes.
	 *
	 * @param engine     engine that owns the statements; may be null
	 * @param statements statements to close; the array and its entries may be null
	 * @return the first logged failure with later failures suppressed, or null
	 */
	public static Throwable closeAllDbConnectionsIfPooling(IRDBMSEngine engine, Statement... statements) {
		Throwable failure = null;
		if (statements != null) {
			for (Statement stmt : statements) {
				if (stmt != null) {
					// Each statement may have its own connection; the batch helper releases only
					// one.
					failure = closePooledStatements(engine, null, new Statement[] { stmt }, failure);
				}
			}
		}
		return failure;
	}
}
