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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.zaxxer.hikari.HikariDataSource;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.om.ThreadStore;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.parsers.PraseSqlQueryForCount;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.usertracking.UserQueryTrackingThread;
import prerna.util.ConnectionUtils;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class RawRDBMSSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private static final Logger classLogger = LogManager.getLogger(RawRDBMSSelectWrapper.class);

	protected HikariDataSource dataSource = null;
	protected Connection conn = null;
	protected Statement stmt = null;
	protected ResultSet rs = null;
	protected boolean closedConnection = false;
	protected ZoneId databaseZoneId = null;

	// we only keep the colTypes as defined by their int values
	// headers, rawHeaders, types are defined in AbstractWrapper
	protected int[] colTypes = null;

	protected IHeadersDataRow currRow = null;

	// this is used so we do not close the engine connection
	protected boolean useEngineConnection = false;

	// use this if we want to close the connection once the iterator is done
	protected boolean closeConnectionAfterExecution = false;

	// set when the caller supplied the prepared statement and its bindings, which
	// leaves the wrapper no query of its own to run again
	private boolean suppliedStatement = false;

	/**
	 * Runs the query through the engine and reads the result's columns. An
	 * execution still open is closed first, keeping its connection unless it came
	 * from a pool, and the row read ahead, the cached row count and the closed
	 * state are cleared, so the wrapper iterates the new result from its first row
	 * and {@link #close()} releases the new execution's resources. When the query
	 * fails, the new execution's statement and result set are closed, with its
	 * connection when it came from a pool or is not the engine's own, and the
	 * wrapper is left closed.
	 *
	 * @throws Exception if the engine cannot run the query or its result cannot be
	 *                   read
	 */
	@Override
	public void execute() throws Exception {
		release(this.dataSource != null);
		startExecution();
		this.conn = null;
		this.dataSource = null;
		this.useEngineConnection = false;
		try {
			Map<String, Object> map = (Map<String, Object>) engine.execQuery(query);
			this.stmt = (Statement) map.get(IRDBMSEngine.STATEMENT_OBJECT);
			Object connObj = map.get(IRDBMSEngine.CONNECTION_OBJECT);
			if (connObj == null) {
				this.useEngineConnection = true;
				connObj = map.get(IRDBMSEngine.ENGINE_CONNECTION_OBJECT);
			}
			this.conn = (Connection) connObj;
			this.rs = (ResultSet) map.get(IRDBMSEngine.RESULTSET_OBJECT);
			this.dataSource = (HikariDataSource) map.get(IRDBMSEngine.DATASOURCE_POOLING_OBJECT);
			// go through and collect the metadata around the query
			setVariables();
			this.databaseZoneId = engine.getDatabaseZoneId();
		} catch (Exception e) {
			classLogger.error("Failed to execute RDBMS select query", e);
			// a pooled connection goes back to its pool, and the engine's shared
			// connection stays open
			boolean closeConnection = !this.useEngineConnection || this.dataSource != null;
			ConnectionUtils.closeAllConnections(closeConnection ? this.conn : null, this.stmt, this.rs);
			this.closedConnection = true;
			throw e;
		}
	}

	/**
	 * Runs the query again on the connection the caller supplied. The current
	 * execution is closed first, and the supplied connection is never closed here,
	 * not even when the query fails.
	 *
	 * @throws SQLException when the query fails to run; the wrapper is left closed
	 */
	private void executeOnSuppliedConnection() throws SQLException {
		release(false);
		startExecution();
		try {
			this.stmt = this.conn.createStatement();
			this.rs = this.stmt.executeQuery(this.query);
			setVariables();
		} catch (SQLException | RuntimeException e) {
			classLogger.error("Failed to run the query again on the supplied connection", e);
			ConnectionUtils.closeAllConnections(null, this.stmt, this.rs);
			this.closedConnection = true;
			throw e;
		}
	}

	/**
	 * Clears what an earlier execution left behind: its statement and result set,
	 * the row read ahead, the cached row count and the closed state. The next
	 * execution then iterates from its first row and is released by
	 * {@link #close()}.
	 */
	private void startExecution() {
		this.stmt = null;
		this.rs = null;
		this.currRow = null;
		this.numRows = 0;
		this.closedConnection = false;
	}

	@Override
	public IHeadersDataRow next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		if (currRow == null) {
			hasNext();
		}
		// grab the current row we have
		IHeadersDataRow retRow = currRow;
		// set the reference to null so we can get a new one
		// on the next hasNext() call;
		currRow = null;
		// return the row
		return retRow;
	}

	@Override
	public boolean hasNext() {
		if (this.closedConnection) {
			return false;
		}
		try {
			// if it is null, try and get the next row
			// from the result set
			if (currRow == null) {
				currRow = getNextRow();
			}

			// if after attempting to get the next row it is
			// still null, then there are no new returns within the rs
			if (currRow != null) {
				return true;
			}

		} catch (SQLException e) {
			classLogger.error("Failed to read the next RDBMS result row", e);
			if (e.getMessage() != null && !e.getMessage().isEmpty()) {
				throw new IllegalArgumentException(
						"Error occurred grabbing next row for query. Detailed message = " + e.getMessage());
			}
			throw new IllegalArgumentException("Error occurred grabbing next row for query");
		}

		return false;
	}

	/**
	 * Advances the result set and converts the row using the shared JDBC mappings.
	 * Calls {@link #close()} when no more rows are available.
	 *
	 * @return the next row, or null after exhaustion
	 * @throws SQLException if advancing, reading, or exhaustion cleanup fails
	 */
	protected IHeadersDataRow getNextRow() throws SQLException {
		if (!rs.next()) {
			close();
			return null;
		}

		Object[] row = new Object[numColumns];
		// iterate through all the columns to get the appropriate data types
		for (int colNum = 1; colNum <= numColumns; colNum++) {
			Object val = null;
			int type = colTypes[colNum - 1];
			if (type == Types.INTEGER) {
				val = rs.getInt(colNum);
			} else if (type == Types.BIGINT) {
				val = rs.getLong(colNum);
			} else if (type == Types.FLOAT || type == Types.DOUBLE || type == Types.NUMERIC || type == Types.DECIMAL
					|| type == Types.REAL) {
				val = rs.getDouble(colNum);
			} else if (type == Types.DATE) {
				try {
					Date dVal = rs.getDate(colNum);
					if (dVal == null) {
						val = null;
					} else {
						val = new SemossDate(dVal.toInstant(), this.databaseZoneId, "yyyy-MM-dd");
					}
				} catch (Exception e) {
					// some rdbms do not actually support dates
					// and just return a string
					// ex: SQLite
					try {
						String dateValStr = rs.getString(colNum);
						// does the string represent a long?
						try {
							long dateLong = Long.parseLong(dateValStr);
							val = new SemossDate(dateLong, "yyyy-MM-dd", this.databaseZoneId);
						} catch (NumberFormatException nfee) {
							val = new SemossDate(dateValStr, "yyyy-MM-dd", this.databaseZoneId);
						}
					} catch (Exception e2) {
						// out of luck...
						classLogger.error("Failed to read DATE value from result column {}", colNum, e);
						classLogger.error("Failed to convert string fallback to DATE for result column {}", colNum, e2);
					}
				}
			} else if (type == Types.TIMESTAMP) {
				try {
					Timestamp dVal = rs.getTimestamp(colNum);
					if (dVal == null) {
						val = null;
					} else {
						val = new SemossDate(dVal, this.databaseZoneId, "yyyy-MM-dd HH:mm:ss");
					}
				} catch (Exception e) {
					// some rdbms do not actually support dates
					// and just return a string
					// ex: SQLite
					try {
						String dateValStr = rs.getString(colNum);
						val = new SemossDate(dateValStr, "yyyy-MM-dd HH:mm:ss", this.databaseZoneId);
					} catch (Exception e2) {
						// out of luck...
						classLogger.error("Failed to read TIMESTAMP value from result column {}", colNum, e);
						classLogger.error("Failed to convert string fallback to TIMESTAMP for result column {}", colNum,
								e2);
					}
				}
			} else if (type == Types.CLOB) {
				val = rs.getClob(colNum);
				try {
					val = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) val);
				} catch (Exception e) {
					classLogger.error("Failed to convert CLOB to text for result column {}", colNum, e);
					if (!rs.wasNull()) {
						val = rs.getString(colNum);
					}
				}
			} else if (type == Types.BLOB) {
				val = rs.getBlob(colNum);
				try {
					val = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) val);
				} catch (IOException e) {
					classLogger.error("Failed to convert BLOB to text for result column {}", colNum, e);
				} catch (NullPointerException e) {
					if (!rs.wasNull()) {
						val = rs.getString(colNum);
					}
				}
			} else if (type == Types.BINARY) {
				try (InputStream is = rs.getBinaryStream(colNum);
						ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
					if (is != null) {
						byte[] buffer = new byte[1024];
						int length;

						while ((length = is.read(buffer)) != -1) {
							baos.write(buffer, 0, length);
						}

						val = baos.toString("UTF-8");
					}
				} catch (IOException e) {
					classLogger.error("Failed to read binary stream for result column {}", colNum, e);
				}
			} else if (type == Types.ARRAY) {
				Array arrVal = rs.getArray(colNum);
				if (arrVal != null) {
					val = arrVal.getArray();
				}
			} else if (type == Types.VARBINARY) {
				byte[] bytes = rs.getBytes(colNum);
				if (bytes != null) {
					try {
						val = new String(bytes, "UTF-8");
					} catch (UnsupportedEncodingException e) {
						classLogger.error("Failed to decode VARBINARY as UTF-8 for result column {}", colNum, e);
					}
				}
			} else if (type == Types.BOOLEAN || type == Types.BIT) {
				try {
					val = rs.getBoolean(colNum);
				} catch (SQLDataException e) {
					// sometimes, this is stored as an integer or string
					// as an example, opensearch
					try {
						val = rs.getInt(colNum);
						if (val != null) {
							if (((int) val) == 0) {
								val = false;
							} else {
								val = true;
							}
						}
					} catch (SQLDataException e2) {
						val = rs.getString(colNum);
						if (val != null) {
							if (Integer.parseInt(val + "") == 0) {
								val = false;
							} else {
								val = true;
							}
						}
					}
				}
			}
			// just grab the object and see what happens...
			else if (type == Types.OTHER) {
				try {
					val = rs.getObject(colNum);
				} catch (Exception e) {
					classLogger.error("Failed to read JDBC OTHER value from result column {}", colNum, e);
				}
			} else {
				val = rs.getString(colNum);
			}

			// need to account for null values
			if (rs.wasNull()) {
				val = null;
			}

			row[colNum - 1] = val;
		}

		// return the header row
		return new HeadersDataRow(headers, rawHeaders, row, row);
	}

	protected void setVariables() {
		try {
			readResultMetadata();
		} catch (SQLException e) {
			classLogger.error("Failed to read RDBMS result metadata", e);
		}
	}

	/**
	 * Populates headers and column types from the current result set. Keeping the
	 * JDBC work separate lets subclasses choose how to handle metadata failures
	 * without duplicating the column mappings.
	 *
	 * @throws SQLException if the driver cannot provide result metadata
	 */
	protected void readResultMetadata() throws SQLException {
		// get the result set metadata
		ResultSetMetaData rsmd = rs.getMetaData();
		numColumns = rsmd.getColumnCount();

		// create the arrays to store the column types,
		// the physical variable names and the display variable names
		colTypes = new int[numColumns];
		types = new SemossDataType[numColumns];
		rawHeaders = new String[numColumns];
		headers = new String[numColumns];

		for (int colIndex = 1; colIndex <= numColumns; colIndex++) {
			rawHeaders[colIndex - 1] = rsmd.getColumnName(colIndex);
			headers[colIndex - 1] = rsmd.getColumnLabel(colIndex);
			colTypes[colIndex - 1] = rsmd.getColumnType(colIndex);
			types[colIndex - 1] = SemossDataType.convertStringToDataType(rsmd.getColumnTypeName(colIndex));
		}
	}

	@Override
	public String[] getHeaders() {
		return headers;
	}

	@Override
	public SemossDataType[] getTypes() {
		return types;
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		return this.rs.getMetaData();
	}

	public void setCloseConenctionAfterExecution(boolean closeConnectionAfterExecution) {
		this.closeConnectionAfterExecution = closeConnectionAfterExecution;
	}

	/**
	 * Closes the result set and statement. The connection is closed too when it
	 * came from a pool, which gives it back, or when
	 * {@link #setCloseConenctionAfterExecution(boolean)} asked for it; otherwise it
	 * stays open for its owner. Closing a closed wrapper does nothing.
	 */
	@Override
	public void close() {
		// if using a datasource
		// we need to close the connection
		// to give it back to the pool
		release(this.dataSource != null || this.closeConnectionAfterExecution);
	}

	/**
	 * Closes the current execution's result set and statement, and its connection
	 * when asked, then marks the wrapper closed and drops the row read ahead. A
	 * failure to close is logged, not thrown. Does nothing when the wrapper is
	 * already closed.
	 *
	 * @param closeConnection whether to close the connection as well
	 */
	private void release(boolean closeConnection) {
		if (this.closedConnection) {
			return;
		}
		this.closedConnection = true;
		this.currRow = null;
		ConnectionUtils.closeAllConnections(closeConnection ? this.conn : null, this.stmt, this.rs);
	}

	@Override
	public long getNumRows() {
		if (this.numRows == 0) {
			UserQueryTrackingThread queryT = null;
			// since we pass via the engine object
			if (this.engine != null) {
				User user = ThreadStore.getUser();
				queryT = new UserQueryTrackingThread(user, this.engine.getEngineId());

				// account for multi rdbms engine as well as base rdmbs engine
				IRDBMSEngine activeEngine = (IRDBMSEngine) this.engine;
				if (this.dataSource == null) {
					this.dataSource = activeEngine.getDataSource();
				}
				if (this.dataSource == null && this.conn == null) {
					try {
						this.conn = activeEngine.getConnection();
					} catch (SQLException e) {
						classLogger.error("Failed to acquire database connection for result counting", e);
					}
				}
			}

			PraseSqlQueryForCount parser = new PraseSqlQueryForCount();
			String query;
			try {
				if (this.engine != null && ((IRDBMSEngine) this.engine).getDbType() == RdbmsTypeEnum.SQL_SERVER) {
					query = this.query;
				} else {
					query = parser.processQuery(this.query);
				}
			} catch (Exception e) {
				classLogger.error("Failed to rewrite SELECT for result counting; using original SQL", e);
				query = this.query;
			}
			if (query.endsWith(";")) {
				query = query.substring(0, query.length() - 1);
			}

			query = "select count(*) from (" + query + ") t";
			Connection connection = null;
			PreparedStatement statement = null;
			ResultSet resultSet = null;
			try {
				if (this.dataSource != null) {
					connection = this.dataSource.getConnection();
				} else {
					connection = this.conn;
				}
				if (connection == null) {
					throw new NullPointerException("The connection is not defined (null)");
				}
				statement = connection.prepareStatement(query);
				if (queryT != null) {
					queryT.setStartTimeNow();
				}

				if (queryT != null) {
					queryT.setQuery(query);
				}
				resultSet = statement.executeQuery();
				if (queryT != null) {
					queryT.setEndTimeNow();
				}
				if (resultSet.next()) {
					this.numRows = resultSet.getLong(1);
				}
			} catch (SQLException e) {
				if (queryT != null) {
					queryT.setFailed();
				}
				classLogger.error("Failed to execute RDBMS row-count query", e);
			} finally {
				if (this.dataSource != null) {
					ConnectionUtils.closeAllConnections(connection, statement, resultSet);
				} else {
					ConnectionUtils.closeAllConnections(null, statement, resultSet);
				}
				if (queryT != null) {
					Thread.ofVirtual().start(queryT);
				}
			}
		}
		return this.numRows;
	}

	@Override
	public long getNumRecords() {
		return getNumRows() * this.numColumns;
	}

	/**
	 * Runs the query again so the wrapper iterates from its first row, whether or
	 * not the current execution was read to its end. The current execution is
	 * closed first and a row read ahead by {@link #hasNext()} is dropped. A query
	 * that ran through the engine runs through it again: a pooled connection goes
	 * back to its pool and a new one is taken, and the engine's shared connection
	 * stays open. A query that ran on a connection the caller supplied runs on that
	 * connection again, and a reset never closes it. A later {@link #close()}
	 * releases the new execution's resources. When the query fails to run, the
	 * wrapper is left closed and the failure is thrown; a later reset can try
	 * again.
	 *
	 * @throws UnsupportedOperationException when the wrapper was made from a result
	 *                                       set or from a caller's prepared
	 *                                       statement, which leave it no query of
	 *                                       its own to run again; the wrapper is
	 *                                       left as it was
	 * @throws Exception                     when the query fails to run again
	 */
	@Override
	public void reset() throws Exception {
		if (this.engine != null) {
			execute();
		} else if (this.conn != null && this.query != null && !this.suppliedStatement) {
			executeOnSuppliedConnection();
		} else {
			throw new UnsupportedOperationException("This wrapper was made from a result set or a caller's "
					+ "prepared statement, so it has no query of its own to run again");
		}
	}

	/**
	 * This method allows me to perform the execution of a query on a given
	 * connection without having to go through a formal RDBMSNativeEngine construct
	 * i.e. the naked engine ;)
	 * 
	 * @param conn
	 * @param query
	 * @throws Exception
	 */
	public static RawRDBMSSelectWrapper directExecutionViaConnection(Connection conn, String query, boolean closeIfFail)
			throws Exception {
		RawRDBMSSelectWrapper wrapper = new RawRDBMSSelectWrapper();
		try {
			wrapper.query = query;
			wrapper.conn = conn;
			wrapper.stmt = wrapper.conn.createStatement();
			wrapper.rs = wrapper.stmt.executeQuery(query);
			wrapper.setVariables();
			return wrapper;
		} catch (Exception e) {
			classLogger.error("Failed to execute SQL on the supplied connection", e);
			if (closeIfFail) {
				ConnectionUtils.closeAllConnections(wrapper.conn, wrapper.stmt, wrapper.rs);
			} else {
				ConnectionUtils.closeAllConnections(null, wrapper.stmt, wrapper.rs);
			}
			throw e;
		}
	}

	/**
	 * Compiles a query through the engine's existing interpreter and executes its
	 * SQL on the supplied connection. This is the legacy Statement-based execution
	 * path.
	 *
	 * @param database    engine supplying the interpreter and query-tracking
	 *                    identity
	 * @param conn        existing connection on which to execute the rendered SQL
	 * @param qs          structured SELECT to render with the engine's interpreter
	 * @param closeIfFail whether execution failure should also close the supplied
	 *                    connection
	 * @return an executed wrapper exposing the result rows
	 * @throws Exception if compilation, execution, or result initialization fails
	 */
	public static RawRDBMSSelectWrapper directExecutionViaConnection(IRDBMSEngine database, Connection conn,
			SelectQueryStruct qs, boolean closeIfFail) throws Exception {
		String engineId = database.getEngineId();
		User user = ThreadStore.getUser();
		UserQueryTrackingThread queryT = new UserQueryTrackingThread(user, engineId);
		RawRDBMSSelectWrapper wrapper = new RawRDBMSSelectWrapper();
		try {
			IQueryInterpreter interpreter = database.getQueryInterpreter();
			interpreter.setQueryStruct(qs);
			wrapper.query = interpreter.composeQuery();
			// set the query used
			queryT.setQuery(wrapper.query);
			wrapper.conn = conn;
			wrapper.stmt = wrapper.conn.createStatement();
			// set the start time
			queryT.setStartTimeNow();
			wrapper.rs = wrapper.stmt.executeQuery(wrapper.query);
			wrapper.setVariables();
			// set the end time
			queryT.setEndTimeNow();

			return wrapper;
		} catch (Exception e) {
			queryT.setFailed();
			classLogger.error("Failed to execute structured query for database {} on the supplied connection", engineId,
					e);
			if (closeIfFail) {
				ConnectionUtils.closeAllConnections(wrapper.conn, wrapper.stmt, wrapper.rs);
			} else {
				ConnectionUtils.closeAllConnections(null, wrapper.stmt, wrapper.rs);
			}
			throw e;
		} finally {
			if (queryT != null) {
				Thread.ofVirtual().start(queryT);
			}
		}
	}

	/**
	 * This method allows me to perform the execution of a query on a given
	 * connection
	 * 
	 * @param database
	 * @param conn
	 * @param stmt
	 * @param query
	 * @param closeIfFail
	 * @return
	 * @throws Exception
	 */
	public static RawRDBMSSelectWrapper directExecutionPreparedStatement(IRDBMSEngine database, Connection conn,
			PreparedStatement stmt, String query, boolean closeIfFail) throws Exception {
		String engineId = database.getEngineId();
		User user = ThreadStore.getUser();
		UserQueryTrackingThread queryT = new UserQueryTrackingThread(user, engineId);
		RawRDBMSSelectWrapper wrapper = new RawRDBMSSelectWrapper();
		wrapper.suppliedStatement = true;
		try {
			wrapper.query = query;
			queryT.setQuery(wrapper.query);
			wrapper.conn = conn;
			wrapper.stmt = stmt;
			// set the start time
			queryT.setStartTimeNow();
			wrapper.rs = stmt.executeQuery();
			wrapper.setVariables();
			// set the end time
			queryT.setEndTimeNow();

			return wrapper;
		} catch (Exception e) {
			queryT.setFailed();
			classLogger.error("Failed to execute prepared statement for database {} on the supplied connection",
					engineId, e);
			if (closeIfFail) {
				ConnectionUtils.closeAllConnections(wrapper.conn, wrapper.stmt, wrapper.rs);
			} else {
				ConnectionUtils.closeAllConnections(null, wrapper.stmt, wrapper.rs);
			}
			throw e;
		} finally {
			if (queryT != null) {
				Thread.ofVirtual().start(queryT);
			}
		}
	}

	/**
	 * 
	 * @param rs
	 * @return
	 * @throws Exception
	 */
	public static RawRDBMSSelectWrapper flushRsToWrapper(ResultSet rs) throws Exception {
		RawRDBMSSelectWrapper wrapper = new RawRDBMSSelectWrapper();
		try {
			wrapper.rs = rs;
			wrapper.setVariables();
			return wrapper;
		} catch (Exception e) {
			classLogger.error("Failed to initialize RDBMS wrapper from the supplied result set", e);
			throw e;
		}
	}

	/**
	 * Wraps a result set someone else owns, so its rows convert as an engine
	 * query's do, dates in the database's time zone included. Closing the wrapper,
	 * or reading past its last row, closes only the result set.
	 *
	 * @param rs             the result set
	 * @param databaseZoneId the database's time zone, for its dates
	 * @return the wrapper
	 * @throws SQLException when the driver cannot describe the result set's columns
	 */
	public static RawRDBMSSelectWrapper wrapResultSet(ResultSet rs, ZoneId databaseZoneId) throws SQLException {
		RawRDBMSSelectWrapper wrapper = new RawRDBMSSelectWrapper();
		wrapper.rs = rs;
		wrapper.databaseZoneId = databaseZoneId;
		wrapper.readResultMetadata();
		return wrapper;
	}

	@Override
	public boolean flushable() {
		return false;
	}

	@Override
	public String flush() {
		return null;
	}
}
