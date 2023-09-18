package prerna.rdf.engine.wrappers;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
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
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.HeadersDataRow;
import prerna.om.ThreadStore;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.parsers.PraseSqlQueryForCount;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.usertracking.UserQueryTrackingThread;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class RawRDBMSSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private static final Logger logger = LogManager.getLogger(RawRDBMSSelectWrapper.class);

	protected HikariDataSource dataSource = null;
	protected Connection conn = null;
	protected Statement stmt = null;
	protected ResultSet rs = null;
	protected boolean closedConnection = false;

	protected int numColumns = 0;
	protected int[] colTypes = null;
	protected SemossDataType[] types;

	protected IHeadersDataRow currRow = null;

	// this is used so we do not close the engine connection
	protected boolean useEngineConnection = false;

	// use this if we want to close the connection once the iterator is done
	protected boolean closeConnectionAfterExecution = false;
	
	@Override
	public void execute() throws Exception {
		try {
			Map<String, Object> map = (Map<String, Object>) engine.execQuery(query);
			this.stmt = (Statement) map.get(RDBMSNativeEngine.STATEMENT_OBJECT);
			Object connObj = map.get(RDBMSNativeEngine.CONNECTION_OBJECT);
			if(connObj == null){
				this.useEngineConnection = true;
				connObj = map.get(RDBMSNativeEngine.ENGINE_CONNECTION_OBJECT);
			}
			this.conn = (Connection) connObj;
			this.rs = (ResultSet) map.get(RDBMSNativeEngine.RESULTSET_OBJECT);
			this.dataSource = (HikariDataSource) map.get(RDBMSNativeEngine.DATASOURCE_POOLING_OBJECT);
			// go through and collect the metadata around the query
			setVariables();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			if(this.useEngineConnection) {
				ConnectionUtils.closeAllConnections(null, stmt, rs);
			} else {
				ConnectionUtils.closeAllConnections(conn, stmt, rs);
			}
			throw e;
		}
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
		if(this.closedConnection) {
			return false;
		}
		try {
			// if it is null, try and get the next row
			// from the result set
			if(currRow == null) {
				currRow = getNextRow();
			}

			// if after attempting to get the next row it is 
			// still null, then there are no new returns within the rs
			if(currRow != null) {
				return true;
			}


		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			if(e.getMessage() != null && !e.getMessage().isEmpty()) {
				throw new IllegalArgumentException("Error occurred grabbing next row for query. Detailed message = " + e.getMessage());
			}
			throw new IllegalArgumentException("Error occurred grabbing next row for query");
		}

		return false;
	}

	private IHeadersDataRow getNextRow() throws SQLException {
		if(rs.next()) {
			Object[] row = new Object[numColumns];
			// iterate through all the columns to get the appropriate data types
			for(int colNum = 1; colNum <= numColumns; colNum++) {
				Object val = null;
				int type = colTypes[colNum-1];
				if(type == Types.INTEGER) {
					val = rs.getInt(colNum);
				} 
				else if (type == Types.BIGINT ) {
					val = rs.getLong(colNum);
				} 
				else if(type == Types.FLOAT || type == Types.DOUBLE || type == Types.NUMERIC || type == Types.DECIMAL || type == Types.REAL) {
					val = rs.getDouble(colNum);
				} 
				else if(type == Types.DATE) {
					try {
						Date dVal = rs.getDate(colNum);
						if(dVal == null) {
							val = null;
						} else {
							val = new SemossDate(dVal, "yyyy-MM-dd");
						}
					} catch(Exception e) {
						// some rdbms do not actually support dates
						// and just return a string
						// ex: SQLite
						try {
							String dateValStr = rs.getString(colNum);
							val = new SemossDate(dateValStr, "yyyy-MM-dd");
						} catch(Exception e2) {
							// out of luck...
							logger.error(Constants.STACKTRACE, e);
							logger.error(Constants.STACKTRACE, e2);
						}
					}
				} 
				else if(type == Types.TIMESTAMP) {
					try {
						Timestamp dVal = rs.getTimestamp(colNum);
						if(dVal == null) {
							val = null;
						} else {
							val = new SemossDate(dVal.getTime(), true);
						}
					} catch(Exception e) {
						// some rdbms do not actually support dates
						// and just return a string
						// ex: SQLite
						try {
							String dateValStr = rs.getString(colNum);
							val = new SemossDate(dateValStr, "yyyy-MM-dd HH:mm:ss");
						} catch(Exception e2) {
							// out of luck...
							logger.error(Constants.STACKTRACE, e);
							logger.error(Constants.STACKTRACE, e2);
						}
					}
				} 
				else if(type == Types.CLOB) {
					val = rs.getClob(colNum);
					try {
						val = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) val);
					} catch (Exception e) {
						logger.error(Constants.STACKTRACE, e);
						if(!rs.wasNull()) {
							val = rs.getString(colNum);
						}
					}
				} 
				else if(type == Types.BLOB) {
					val = rs.getBlob(colNum);
					try {
						val = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) val);
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					} catch (NullPointerException e) {
						if(!rs.wasNull()) {
							val = rs.getString(colNum);
						}
					}
				} 
				else if(type == Types.ARRAY) {
					Array arrVal = rs.getArray(colNum);
					if(arrVal != null) {
						val = arrVal.getArray();
					}
				} 
				else if(type == Types.VARBINARY) {
					byte[] bytes = rs.getBytes(colNum);
					if(bytes != null) {
						try {
							val = new String(bytes, "UTF-8");
						} catch (UnsupportedEncodingException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				}
				else if(type == Types.BOOLEAN || type == Types.BIT) {
					try {
						val = rs.getBoolean(colNum);
					} catch (SQLDataException e) {
						// sometimes, this is stored as an integer or string
						// as an example, opensearch
						try {
							val = rs.getInt(colNum);
							if(val != null) {
								if(((int) val) == 0) {
									val = false;
								} else {
									val = true;
								}
							}
						} catch (SQLDataException e2) {
							val = rs.getString(colNum);
							if(val != null) {
								if(Integer.parseInt(val + "") == 0) {
									val = false;
								} else {
									val = true;
								}
							}
						}
					}
				}
				// just grab the object and see what happens...
				else if(type == Types.OTHER) {
					try {
						val = rs.getObject(colNum);
					} catch(Exception e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
				else {
					val = rs.getString(colNum);
				}
				
				// need to account for null values
				if(rs.wasNull()) {
					val = null;
				}
				
				row[colNum-1] = val;
			}
			
			// return the header row
			return new HeadersDataRow(headers, rawHeaders, row, row);
		} else {
			try {
				close();
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}

		// no more results
		// return null
		return null;
	}


	protected void setVariables(){
		try {
			// get the result set metadata
			ResultSetMetaData rsmd = rs.getMetaData();
			numColumns = rsmd.getColumnCount();

			// create the arrays to store the column types,
			// the physical variable names and the display variable names
			colTypes = new int[numColumns];
			types = new SemossDataType[numColumns];
			rawHeaders = new String[numColumns];
			headers = new String[numColumns];

			for(int colIndex = 1; colIndex <= numColumns; colIndex++) {
				rawHeaders[colIndex-1] = rsmd.getColumnName(colIndex);
				headers[colIndex-1] = rsmd.getColumnLabel(colIndex);
				colTypes[colIndex-1] = rsmd.getColumnType(colIndex);
				types[colIndex-1] = SemossDataType.convertStringToDataType(rsmd.getColumnTypeName(colIndex));
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
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
	
	@Override
	public void close() throws IOException {
		if(this.closedConnection) {
			return;
		}
		// if using a datasource
		// we need to close the connection
		// to give it back to the pool
		if(this.dataSource != null || this.closeConnectionAfterExecution) {
			ConnectionUtils.closeAllConnections(this.conn, this.stmt, this.rs);
		} else {
			ConnectionUtils.closeAllConnections(null, this.stmt, this.rs);
		}
		
		this.closedConnection = true;
	}
	
	@Override
	public long getNumRows() {
		if(this.numRows == 0) {
			UserQueryTrackingThread queryT = null;
			// since we pass via the engine object
			if(this.engine != null) {
				User user = ThreadStore.getUser();
				queryT = new UserQueryTrackingThread(user, this.engine.getEngineId());
				
				// account for multi rdbms engine as well as base rdmbs engine
				IRDBMSEngine activeEngine = (IRDBMSEngine) this.engine;
				if(this.dataSource == null) {
					this.dataSource = activeEngine.getDataSource();
				}
				if(this.dataSource == null && this.conn == null) {
					try {
						this.conn = activeEngine.getConnection();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
			
			PraseSqlQueryForCount parser = new PraseSqlQueryForCount();
			String query;
			try {
				if(this.engine != null && ((IRDBMSEngine) this.engine).getDbType() == RdbmsTypeEnum.SQL_SERVER) {
					query = this.query;
				} else {
					query = parser.processQuery(this.query);
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				query = this.query;
			}
			if(query.endsWith(";")) {
				query = query.substring(0, query.length()-1);
			}
			
			query = "select count(*) from (" + query + ") t";
			Connection connection = null;
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				if(this.dataSource != null) {
					connection = this.dataSource.getConnection();
				} else {
					connection = this.conn;
				}
				if(connection == null) {
					throw new NullPointerException("The connection is not defined (null)");
				}
				statement = connection.createStatement();
				if(queryT != null) { queryT.setStartTimeNow(); };
				if(queryT != null) { queryT.setQuery(query); };
				resultSet = statement.executeQuery(query);
				if(queryT != null) { queryT.setEndTimeNow(); };
				if(resultSet.next()) {
					this.numRows = resultSet.getLong(1);
				}
			} catch (SQLException e) {
				if(queryT != null) { queryT.setFailed(); };
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(this.dataSource != null) {
					ConnectionUtils.closeAllConnections(null, statement, resultSet);
				} else {
					ConnectionUtils.closeAllConnections(connection, statement, resultSet);
				}
				if(queryT != null) { new Thread(queryT).start(); };
			}
		}
		return this.numRows;
	}
	
	@Override
	public long getNumRecords() {
		return getNumRows() * this.numColumns;
	}
	
	@Override
	public void reset() throws Exception {
		// close current stuff
		// but we shouldn't close the connection
		// so store whatever that boolean is as temp
		// and then reasign after we re-execute
		boolean temp = this.closeConnectionAfterExecution;
		this.closeConnectionAfterExecution = false;
		close();
		this.closeConnectionAfterExecution = temp;
		// execute again
		execute();
	}
	
	/**
	 * This method allows me to perform the execution of a query on a given connection
	 * without having to go through a formal RDBMSNativeEngine construct
	 * i.e. the naked engine ;)
	 * @param conn
	 * @param query
	 * @throws Exception 
	 */
	public static RawRDBMSSelectWrapper directExecutionViaConnection(Connection conn, String query, boolean closeIfFail) throws Exception {
		RawRDBMSSelectWrapper wrapper = new RawRDBMSSelectWrapper();
		try {
			wrapper.query = query;
			wrapper.conn = conn;
			wrapper.stmt = wrapper.conn.createStatement();
			wrapper.rs = wrapper.stmt.executeQuery(query);
			wrapper.setVariables();
			return wrapper;
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			if(closeIfFail) {
				ConnectionUtils.closeAllConnections(wrapper.conn, wrapper.stmt, wrapper.rs);
			} else {
				ConnectionUtils.closeAllConnections(null, wrapper.stmt, wrapper.rs);
			}
			throw e;
		}
	}
	
	/**
	 * This method allows me to perform the execution of a query on a given connection
	 * without having to go through a formal RDBMSNativeEngine construct
	 * i.e. the naked engine ;)
	 * @param conn
	 * @param query
	 * @throws Exception 
	 */
	public static RawRDBMSSelectWrapper directExecutionViaConnection(IRDBMSEngine database, Connection conn, SelectQueryStruct qs, boolean closeIfFail) throws Exception {
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
		} catch(Exception e) {
			queryT.setFailed();
			logger.error(Constants.STACKTRACE, e);
			if(closeIfFail) {
				ConnectionUtils.closeAllConnections(wrapper.conn, wrapper.stmt, wrapper.rs);
			} else {
				ConnectionUtils.closeAllConnections(null, wrapper.stmt, wrapper.rs);
			}
			throw e;
		} finally {
			if(queryT != null) {
				new Thread(queryT).start();
			}
		}
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
