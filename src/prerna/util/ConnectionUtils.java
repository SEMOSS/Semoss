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

public class ConnectionUtils {

	private static final Logger classLogger = LogManager.getLogger(ConnectionUtils.class);

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

	public static void closeAllConnections(Connection con, Statement ps) {
		closeAllConnections(con, ps, null);
	}

	public static void closeAllConnections(Statement stmt, ResultSet rs) {
		closeAllConnections(null, stmt, rs);
	}

	public static void closeResultSet(ResultSet rs) {
		closeAllConnections(null, null, rs);
	}

	public static void closeStatement(Statement stmt) {
		closeAllConnections(null, stmt, null);
	}

	public static void closeConnection(Connection con) {
		closeAllConnections(con, null, null);
	}

	public static void closeAllConnectionsIfPooling(IRDBMSEngine engine, Connection con) {
		closeAllConnectionsIfPooling(engine, con, null, null);
	}

	public static void closeAllConnectionsIfPooling(IRDBMSEngine engine, Connection con, Statement stmt) {
		closeAllConnectionsIfPooling(engine, con, stmt, null);
	}

	public static void closeAllConnectionsIfPooling(IRDBMSEngine engine, Statement stmt) {
		closeAllConnectionsIfPooling(engine, stmt, null);
	}

	/**
	 * Commit all pending transactions on the connection
	 * 
	 * @param conn
	 */
	public static void commitConnection(Connection conn) {
		try {
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException e) {
			classLogger.error(e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(
					"An error occurred commiting the transaction to the database. See logs for details.");
		}
	}

	/**
	 * 
	 * @param engine
	 * @param statements
	 */
	public static void closeAllDbConnectionsIfPooling(IRDBMSEngine engine, Statement... statements) {
		if (statements != null) {
			for (Statement stmt : statements) {
				if (stmt != null) {
					try {
						stmt.close();
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
					if (engine != null && engine.isConnectionPooling()) {
						try {
							stmt.getConnection().close();
						} catch (Exception e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}
		}
	}
}
