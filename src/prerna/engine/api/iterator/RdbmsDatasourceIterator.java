package prerna.engine.api.iterator;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.util.ConnectionUtils;

public class RdbmsDatasourceIterator extends AbstractDatasourceIterator {

	protected Connection conn = null;
	protected Statement stmt = null;
	protected ResultSet rs = null;
	
	protected int[] colTypes = null;
	protected IHeadersDataRow currRow = null;

	protected boolean closedConnection = false;
	
	// use this if we want to close the connection once the iterator is done
	// or if a failure occured
	protected boolean closeConnectionAfterExecution = false;
	
	public RdbmsDatasourceIterator(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void execute() {
		try {
			this.stmt = this.conn.createStatement();
			this.rs = this.stmt.executeQuery(this.query);

			// get the result set metadata
			ResultSetMetaData rsmd = rs.getMetaData();
			this.numColumns = rsmd.getColumnCount();

			// create the arrays to store the column types,
			// the physical variable names and the display variable names
			this.colTypes = new int[this.numColumns];
			this.types = new SemossDataType[this.numColumns];
			this.rawHeaders = new String[this.numColumns];
			this.headers = new String[this.numColumns];

			for(int colIndex = 1; colIndex <= this.numColumns; colIndex++) {
				this.rawHeaders[colIndex-1] = rsmd.getColumnName(colIndex);
				this.headers[colIndex-1] = rsmd.getColumnLabel(colIndex);
				this.colTypes[colIndex-1] = rsmd.getColumnType(colIndex);
				this.types[colIndex-1] = SemossDataType.convertStringToDataType(rsmd.getColumnTypeName(colIndex));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			if(this.closeConnectionAfterExecution) {
				ConnectionUtils.closeAllConnections(conn, rs, stmt);
			} else {
				ConnectionUtils.closeAllConnections(null, rs, stmt);
			}
		}
	}
	
	@Override
	public IHeadersDataRow next() {
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

				row[colNum-1] = val;
			}

			// return the header row
			return new HeadersDataRow(this.headers, this.rawHeaders, row, row);
		} else {
			cleanUp();
		}

		// no more results
		// return null
		return null;
	}

	@Override
	public void cleanUp() {
		if(this.closedConnection) {
			return;
		}
		if(this.rs != null) {
			try {
				this.rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(this.stmt != null) {
			try {
				this.stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(this.closeConnectionAfterExecution) {
			if(this.conn != null) {
				try {
					this.conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		this.closedConnection = true;
	}

	@Override
	public long getNumRecords() {
		String query = "select (count(*) * " + this.numColumns + ") from (" + this.query + ")";
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = this.conn.createStatement();
			rs = stmt.executeQuery(query);
			if(rs.next()) {
				return rs.getLong(1);
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
		
		return 0;
	}

	@Override
	public void reset() {
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
	
	public void setCloseConnectionAfterExecution(boolean closeConnectionAfterExecution) {
		this.closeConnectionAfterExecution = closeConnectionAfterExecution;
	}

}
