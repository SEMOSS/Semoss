package prerna.rdf.engine.wrappers;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.HeadersDataRow;
import prerna.query.parsers.PraseSqlQueryForCount;
import prerna.util.ConnectionUtils;

// TODO >>>timb: so right now this is the only wrapper extending this class, will need RestEngine
public class RawRDBMSSelectWrapperREST extends AbstractRESTWrapper implements IRawSelectWrapper {

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
	public void localExecute() {
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

			// go through and collect the metadata around the query
			setVariables();
		} catch (Exception e){
			e.printStackTrace();
			if(this.useEngineConnection) {
				ConnectionUtils.closeAllConnections(null, rs, stmt);
			} else {
				ConnectionUtils.closeAllConnections(conn, rs, stmt);
			}
		}
	}

	@Override
	public IHeadersDataRow localNext() {
		if(currRow == null) {
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
	public boolean localHasNext() {
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
			e.printStackTrace();
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
				} else if(type == Types.FLOAT || type == Types.DOUBLE || type == Types.NUMERIC || type == Types.DECIMAL || type == Types.BIGINT) {
					val = rs.getDouble(colNum);
				} else if(type == Types.DATE) {
					Date dVal = rs.getDate(colNum);
					if(dVal == null) {
						val = null;
					} else {
						val = new SemossDate(dVal, "yyyy-MM-dd");
					}
				} else if(type == Types.TIMESTAMP) {
					Timestamp dVal = rs.getTimestamp(colNum);
					if(dVal == null) {
						val = null;
					} else {
						val = new SemossDate(dVal.getTime(), true);
					}
				} else if(type == Types.CLOB) {
					val = rs.getClob(colNum);
				} else if(type == Types.ARRAY) {
					Array arrVal = rs.getArray(colNum);
					if(arrVal != null) {
						val = arrVal.getArray();
					}
				} else if(type == Types.BOOLEAN) {
					val = rs.getBoolean(colNum);
				} else {
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
			cleanUp();
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
			e.printStackTrace();
		}
	}

	@Override
	public String[] localGetHeaders() {
		return headers;
	}

	@Override
	public SemossDataType[] localGetTypes() {
		return types;
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		return this.rs.getMetaData();
	}
	
	public void setCloseConenctionAfterExecution(boolean closeConnectionAfterExecution) {
		this.closeConnectionAfterExecution = closeConnectionAfterExecution;
	}
	
	@Override
	public void localCleanUp() {
		if(this.closedConnection) {
			return;
		}
		try {
			if(this.rs != null) {
				this.rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			if(this.stmt != null) {
				this.stmt.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if(this.closeConnectionAfterExecution) {
			try {
				if(this.conn != null) {
					this.conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		this.closedConnection = true;
	}
	
	@Override
	public long localGetNumRows() {
		if(this.numRows == 0) {
			PraseSqlQueryForCount parser = new PraseSqlQueryForCount();
			String query;
			try {
				query = parser.processQuery(this.query);
			} catch (Exception e) {
				e.printStackTrace();
				query = this.query;
			}
			query = "select count(*) from (" + query + ") t";
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = this.conn.createStatement();
				rs = stmt.executeQuery(query);
				if(rs.next()) {
					this.numRows = rs.getLong(1);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if(rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				if(stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return this.numRows;
	}
	
	@Override
	public long localGetNumRecords() {
		return getNumRows() * this.numColumns;
	}
	
	@Override
	public void localReset() {
		// close current stuff
		// but we shouldn't close the connection
		// so store whatever that boolean is as temp
		// and then reasign after we re-execute
		boolean temp = this.closeConnectionAfterExecution;
		this.closeConnectionAfterExecution = false;
		cleanUp();
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
	 */
	public void directExecutionViaConnection(Connection conn, String query, boolean closeIfFail) {
		try {
			this.query = query;
			this.conn = conn;
			this.stmt = this.conn.createStatement();
			this.rs = this.stmt.executeQuery(query);
			setVariables();
		} catch(Exception e) {
			e.printStackTrace();
			if(closeIfFail) {
				ConnectionUtils.closeAllConnections(conn, rs, stmt);
			}
			throw new IllegalArgumentException(e.getMessage());
		}
	}
}
