package prerna.rdf.engine.wrappers;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.util.ConnectionUtils;

public class RawImpalaSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {
	
	private SelectQueryStruct qs;
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	private boolean closedConnection = false;
	
	private int numColumns = 0;
	private String[] colTypeNames = null;
	private int[] colTypes = null;

	private IHeadersDataRow currRow = null;

	// this is used so we do not close the engine connection
	private boolean useEngineConnection = false;
	// use this if we want to close the connection once the iterator is done
	private boolean closeConnectionAfterExecution = false;
	
	public RawImpalaSelectWrapper () {
		
	}
	
	public RawImpalaSelectWrapper(SelectQueryStruct qs) {
		this.qs = qs;
	}

	@Override
	public void execute() {
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

				// get the column as the specific type
				// TODO: will need to expand this list... 
				int type = colTypes[colNum-1];
				if(type == Types.INTEGER) {
					val = rs.getInt(colNum);
				} else if(type == Types.FLOAT || type == Types.DOUBLE || type == Types.NUMERIC || type == Types.DECIMAL || type == Types.BIGINT) {
					val = rs.getDouble(colNum);
					//nulls are set to 0 unless there is a null check
					// if (rs.wasNull()) {
					// val = null;
					// }
				} else if(type == Types.DATE || type == Types.TIMESTAMP || type == Types.TIME) {
					val = rs.getDate(colNum);
				} 
				// TODO: we may want to not differentiate and just grab the String value of the CLOB
				else if(type == Types.CLOB) {
					val = rs.getClob(colNum);
				} else if(type == Types.ARRAY) {
					Array arrVal = rs.getArray(colNum);
					if(arrVal != null) {
						val = arrVal.getArray();
					}
				} else if(type == Types.BOOLEAN) {
					val = rs.getBoolean(colNum);
				}
				else {
					val = rs.getString(colNum);
				}

				row[colNum-1] = val;
			}

			// return the header row
			return new HeadersDataRow(displayVar, var, row, row);
		} else {
			cleanUp();
		}

		// no more results
		// return null
		return null;
	}


	private void setVariables(){
		try {
			// get the result set metadata
			ResultSetMetaData rsmd = rs.getMetaData();
			numColumns = rsmd.getColumnCount();

			// create the arrays to store the column types,
			// the physical variable names and the display variable names
			colTypes = new int[numColumns];
			colTypeNames = new String[numColumns];
			var = new String[numColumns];
			displayVar = new String[numColumns];

			for(int colIndex = 1; colIndex <= numColumns; colIndex++) {
				var[colIndex-1] = rsmd.getColumnName(colIndex);
				displayVar[colIndex-1] = rsmd.getColumnLabel(colIndex);
				//IMPALA EDITS
				//Remove the front appended math function and re-add it to address case issue due to impala returning lowercase only
				if(qs != null && !(qs instanceof HardSelectQueryStruct)) {
					if((qs.getSelectors().get(colIndex-1).getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION)){
						QueryFunctionSelector currentSelect= (QueryFunctionSelector) qs.getSelectors().get(colIndex-1);
						String aggregate = currentSelect.getFunction();
						var[colIndex-1]=var[colIndex-1].replaceFirst((aggregate.toLowerCase()), aggregate);
						displayVar[colIndex-1]=displayVar[colIndex-1].replaceFirst((aggregate.toLowerCase()), aggregate);
					}
				}
				colTypes[colIndex-1] = rsmd.getColumnType(colIndex);
				colTypeNames[colIndex-1] = rsmd.getColumnTypeName(colIndex);
			}
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String[] getDisplayVariables() {
		return displayVar;
	}

	@Override
	public String[] getPhysicalVariables() {
		return var;
	}
	
	@Override
	public String[] getTypes() {
		return colTypeNames;
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		return this.rs.getMetaData();
	}
	
	public void setCloseConenctionAfterExecution(boolean closeConnectionAfterExecution) {
		this.closeConnectionAfterExecution = closeConnectionAfterExecution;
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

	@Override
	public void cleanUp() {
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
}
