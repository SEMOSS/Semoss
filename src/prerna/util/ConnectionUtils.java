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
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;

/**
 * Utility methods for safely closing JDBC resources ({@link Connection},
 * {@link Statement}, and {@link ResultSet}).
 * <p>
 * Each {@code close*} method silently swallows and logs any exception raised
 * while closing a resource, so callers can invoke them from a {@code finally}
 * block without additional try/catch handling. Resources are closed in the
 * standard reverse order of acquisition: result set, then statement, then
 * connection.
 * <p>
 * The {@code *IfPooling} variants are connection-pool aware: the underlying
 * {@link Connection} is only closed (returned to the pool) when the supplied
 * {@link IRDBMSEngine} reports {@link IRDBMSEngine#isConnectionPooling()} as
 * {@code true}. This prevents a non-pooled engine's long-lived connection from
 * being closed prematurely while still ensuring pooled connections are released
 * back to the pool.
 */
public class ConnectionUtils {

	private static final Logger classLogger = LogManager.getLogger(ConnectionUtils.class);

	/**
	 * Close a result set, statement, and connection that belong to a pooling-aware
	 * engine. The result set and statement are always closed. If {@code con} is
	 * {@code null} and the engine is pooling, the connection backing the statement
	 * is resolved via {@link Statement#getConnection()}. The connection is only
	 * closed (released to the pool) when the engine reports connection pooling.
	 *
	 * @param engine the engine the resources belong to; may be {@code null}
	 * @param con    the connection to close; may be {@code null} to derive it from
	 *               {@code stmt}
	 * @param stmt   the statement to close; may be {@code null}
	 * @param rs     the result set to close; may be {@code null}
	 */
	public static void closeAllConnectionsIfPooling(IRDBMSEngine engine, Connection con, Statement stmt, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
				classLogger.error("Error closing result set", e);
			}
		}
		if (stmt != null) {
			try {
				if (engine != null && engine.isConnectionPooling() && con == null) {
					con = stmt.getConnection();
				}
				stmt.close();
			} catch (Exception e) {
				classLogger.error("Error closing statement", e);
			}
		}
		if (engine != null && engine.isConnectionPooling()) {
			try {
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
				classLogger.error("Error closing connection", e);
			}
		}
	}

	/**
	 * Close a connection and a variable number of statements for a pooling-aware
	 * engine. Each statement is closed in turn. If {@code con} is {@code null} and
	 * the engine is pooling, the connection is resolved from the first statement
	 * that provides one. The connection is only closed (released to the pool) when
	 * the engine reports connection pooling.
	 *
	 * @param engine the engine the resources belong to; may be {@code null}
	 * @param con    the connection to close; may be {@code null} to derive it from
	 *               a statement
	 * @param stmts  the statements to close; may be {@code null}
	 */
	public static void closeAllConnectionsIfPooling(IRDBMSEngine engine, Connection con, Statement... stmts) {
		if (stmts != null) {
			for (Statement stmt : stmts) {
				try {
					if (engine != null && engine.isConnectionPooling() && con == null) {
						con = stmt.getConnection();
					}
					stmt.close();
				} catch (Exception e) {
					classLogger.error("Error closing statement", e);
				}
			}
		}
		if (engine != null && engine.isConnectionPooling()) {
			try {
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
				classLogger.error("Error closing connection", e);
			}
		}
	}

	/**
	 * Close a statement and result set for a pooling-aware engine, deriving the
	 * connection from the statement. The result set and statement are always
	 * closed. The statement's connection is only closed (released to the pool) when
	 * the engine reports connection pooling.
	 *
	 * @param engine the engine the resources belong to; may be {@code null}
	 * @param stmt   the statement to close; may be {@code null}
	 * @param rs     the result set to close; may be {@code null}
	 */
	public static void closeAllConnectionsIfPooling(IRDBMSEngine engine, Statement stmt, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
				classLogger.error("Error closing result set", e);
			}
		}
		if (stmt != null) {
			try {
				stmt.close();
			} catch (Exception e) {
				classLogger.error("Error closing statement", e);
			}
		}
		if (engine != null && engine.isConnectionPooling()) {
			try {
				if (stmt != null && stmt.getConnection() != null) {
					stmt.getConnection().close();
				}
			} catch (Exception e) {
				classLogger.error("Error closing connection", e);
			}
		}
	}

	/**
	 * Close a result set, statement, and connection unconditionally, in that order.
	 * Each resource is closed only if non-{@code null}, and any exception is logged
	 * rather than propagated.
	 *
	 * @param con  the connection to close; may be {@code null}
	 * @param stmt the statement to close; may be {@code null}
	 * @param rs   the result set to close; may be {@code null}
	 */
	public static void closeAllConnections(Connection con, Statement stmt, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
				classLogger.error("Error closing result set", e);
			}
		}
		if (stmt != null) {
			try {
				stmt.close();
			} catch (Exception e) {
				classLogger.error("Error closing statement", e);
			}
		}
		if (con != null) {
			try {
				con.close();
			} catch (Exception e) {
				classLogger.error("Error closing connection", e);
			}
		}
	}

	/**
	 * Close a statement and connection unconditionally.
	 *
	 * @param con the connection to close; may be {@code null}
	 * @param ps  the statement to close; may be {@code null}
	 */
	public static void closeAllConnections(Connection con, Statement ps) {
		closeAllConnections(con, ps, null);
	}

	/**
	 * Close a statement and result set unconditionally.
	 *
	 * @param stmt the statement to close; may be {@code null}
	 * @param rs   the result set to close; may be {@code null}
	 */
	public static void closeAllConnections(Statement stmt, ResultSet rs) {
		closeAllConnections(null, stmt, rs);
	}

	/**
	 * Close a result set, logging any exception.
	 *
	 * @param rs the result set to close; may be {@code null}
	 */
	public static void closeResultSet(ResultSet rs) {
		closeAllConnections(null, null, rs);
	}

	/**
	 * Close a statement, logging any exception.
	 *
	 * @param stmt the statement to close; may be {@code null}
	 */
	public static void closeStatement(Statement stmt) {
		closeAllConnections(null, stmt, null);
	}

	/**
	 * Close a connection, logging any exception.
	 *
	 * @param con the connection to close; may be {@code null}
	 */
	public static void closeConnection(Connection con) {
		closeAllConnections(con, null, null);
	}

	/**
	 * Close a connection for a pooling-aware engine. The connection is only closed
	 * (released to the pool) when the engine reports connection pooling.
	 *
	 * @param engine the engine the connection belongs to; may be {@code null}
	 * @param con    the connection to close; may be {@code null}
	 */
	public static void closeAllConnectionsIfPooling(IRDBMSEngine engine, Connection con) {
		closeAllConnectionsIfPooling(engine, con, null, null);
	}

	/**
	 * Close a connection and statement for a pooling-aware engine. The connection
	 * is only closed (released to the pool) when the engine reports connection
	 * pooling.
	 *
	 * @param engine the engine the resources belong to; may be {@code null}
	 * @param con    the connection to close; may be {@code null}
	 * @param stmt   the statement to close; may be {@code null}
	 */
	public static void closeAllConnectionsIfPooling(IRDBMSEngine engine, Connection con, Statement stmt) {
		closeAllConnectionsIfPooling(engine, con, stmt, null);
	}

	/**
	 * Close a statement for a pooling-aware engine, deriving the connection from
	 * the statement. The statement's connection is only closed (released to the
	 * pool) when the engine reports connection pooling.
	 *
	 * @param engine the engine the statement belongs to; may be {@code null}
	 * @param stmt   the statement to close; may be {@code null}
	 */
	public static void closeAllConnectionsIfPooling(IRDBMSEngine engine, Statement stmt) {
		closeAllConnectionsIfPooling(engine, stmt, null);
	}

	/**
	 * Commit all pending transactions on the connection. No commit is issued if the
	 * connection is already in auto-commit mode.
	 *
	 * @param conn the connection to commit
	 * @throws IllegalArgumentException if the commit fails
	 */
	public static void commitConnection(Connection conn) {
		try {
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException e) {
			classLogger.error("Error committing the transaction to the database", e);
			throw new IllegalArgumentException(
					"An error occurred commiting the transaction to the database. See logs for details.");
		}
	}

	/**
	 * Close a variable number of statements for a pooling-aware engine. Each
	 * statement is closed in turn, and the connection backing each statement is
	 * only closed (released to the pool) when the engine reports connection
	 * pooling.
	 *
	 * @param engine     the engine the statements belong to; may be {@code null}
	 * @param statements the statements to close; may be {@code null}
	 */
	public static void closeAllDbConnectionsIfPooling(IRDBMSEngine engine, Statement... statements) {
		if (statements != null) {
			for (Statement stmt : statements) {
				if (stmt != null) {
					try {
						stmt.close();
					} catch (Exception e) {
						classLogger.error("Error closing statement", e);
					}
					if (engine != null && engine.isConnectionPooling()) {
						try {
							stmt.getConnection().close();
						} catch (Exception e) {
							classLogger.error("Error closing connection", e);
						}
					}
				}
			}
		}
	}
}
