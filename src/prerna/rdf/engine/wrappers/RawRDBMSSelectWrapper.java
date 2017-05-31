package prerna.rdf.engine.wrappers;

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
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.util.ConnectionUtils;

public class RawRDBMSSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private Connection conn = null;
	private ResultSet rs = null;
	private Statement stmt = null;

	private int numColumns = 0;
	private int[] colTypes = null;

	private IHeadersDataRow currRow = null;

	// this is used so we do not close the engine connection
	private boolean useEngineConnection = false;

	public void directExecutionViaConnection(Connection conn, String query) {
		try {
			this.conn = conn;
			this.stmt = this.conn.createStatement();
			this.rs = this.stmt.executeQuery(query);
			setVariables();
		} catch(Exception e) {
			e.printStackTrace();
			ConnectionUtils.closeAllConnections(conn, rs, stmt);
			throw new IllegalArgumentException(e.getMessage());
		}
	}
	
	@Override
	public void execute() {
		try{
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
				if(type == Types.INTEGER || type == Types.FLOAT || type == Types.DOUBLE || type == Types.NUMERIC || type == Types.DECIMAL || type == Types.BIGINT) {
					val = rs.getDouble(colNum);
					//nulls are set to 0 unless there is a null check
					// if (rs.wasNull()) {
					// val = null;
					// }
				} else if(type == Types.DATE || type == Types.TIMESTAMP || type == Types.TIME) {
					val = rs.getDate(colNum);
				} else {
					val = rs.getString(colNum);
				}

				row[colNum-1] = val;
			}

			// return the header row
			return new HeadersDataRow(displayVar, row, row);
		} else {
			rs.close();
			stmt.close();
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
			var = new String[numColumns];
			displayVar = new String[numColumns];

			for(int colIndex = 1; colIndex <= numColumns; colIndex++) {
				var[colIndex-1] = rsmd.getColumnName(colIndex);
				displayVar[colIndex-1] = rsmd.getColumnLabel(colIndex);
				colTypes[colIndex-1] = rsmd.getColumnType(colIndex);
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

	public ResultSetMetaData getMetaData() throws SQLException {
		return this.rs.getMetaData();
	}
}
