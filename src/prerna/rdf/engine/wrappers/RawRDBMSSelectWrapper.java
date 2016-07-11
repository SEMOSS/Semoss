package prerna.rdf.engine.wrappers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Hashtable;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.rdf.util.SQLQueryParser;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.sql.SQLQueryUtil;

public class RawRDBMSSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private Connection conn = null;
	private ResultSet rs = null;
	private Statement stmt = null;

	private int numColumns = 0;
	private int[] colTypes = null;

	private IHeadersDataRow currRow = null;

	// this is used so we do not close the engine connection
	private boolean useEngineConnection = false;

	@Override
	public void execute() {
		try{
			Map<String, Object> map = (Map<String, Object>) engine.execQuery(query);
			stmt = (Statement) map.get(RDBMSNativeEngine.STATEMENT_OBJECT);
			Object connObj = map.get(RDBMSNativeEngine.CONNECTION_OBJECT);
			if(connObj == null){
				useEngineConnection = true;
				connObj = map.get(RDBMSNativeEngine.ENGINE_CONNECTION_OBJECT);
			}
			conn = (Connection) connObj;
			rs = (ResultSet) map.get(RDBMSNativeEngine.RESULTSET_OBJECT);

			// go through and collect the metadata around the query
			setVariables();
		} catch (Exception e){
			e.printStackTrace();
			if(useEngineConnection) {
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
				} else if(type == Types.DATE || type == Types.TIMESTAMP || type == Types.TIME) {
					val = rs.getDate(colNum);
				} else {
					val = rs.getString(colNum);
				}

				row[colNum-1] = val;
			}

			// return the header row
			return new HeadersDataRow(displayVar, row, row);
		}

		// no more results
		// return null
		return null;
	}


	private void setVariables(){
		try {
			// get the correct rdbms type
			// default to h2
			SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;
			String dbTypeString = engine.getProperty(Constants.RDBMS_TYPE);
			if (dbTypeString != null) {
				dbType = (SQLQueryUtil.DB_TYPE.valueOf(dbTypeString));
			}

			// get the result set metadata
			ResultSetMetaData rsmd = rs.getMetaData();
			numColumns = rsmd.getColumnCount();

			// create the arrays to store the column types,
			// the physical variable names and the display variable names
			colTypes = new int[numColumns];
			var = new String[numColumns];
			displayVar = new String[numColumns];

			// we parse through the query to get the display names
			// display names are the alias within the query for each return
			// as defined by the SQLQueryBuilder logic
			Hashtable<String, Hashtable<String, String>> parserResults = null;
			if(query.startsWith("SELECT")) {
				SQLQueryParser parser = new SQLQueryParser(query);
				parserResults = parser.getReturnVarsFromQuery(query);
			}


			// iterate through each column
			for(int colIndex = 1;colIndex <= numColumns;colIndex++)
			{
				String tableName = rsmd.getTableName(colIndex);
				String colName = rsmd.getColumnName(colIndex);
				String logName = colName;

				// if we found aliases defined within the query
				// we see if it matches and will use that as the display name
				if(parserResults != null && !parserResults.isEmpty()) {
					for(String tab : parserResults.keySet()) {
						if(tab.equalsIgnoreCase(tableName)) {
							for(String col : parserResults.get(tab).keySet()) {
								if(parserResults.get(tab).get(col).equalsIgnoreCase(colName)) {
									logName = col;
									break;
								}
							}
						}
					}
				}

				// set the physical variable name
				var[colIndex-1] = colName;
				// set the display name
				displayVar[colIndex-1] = logName;
				// set the column type
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

}
