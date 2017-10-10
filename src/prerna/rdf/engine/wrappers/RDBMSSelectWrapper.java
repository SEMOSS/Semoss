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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Hashtable;
import java.util.Map;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.util.SQLQueryParser;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;

public class RDBMSSelectWrapper extends AbstractWrapper implements ISelectWrapper {

	private ResultSet rs = null;
	private Connection conn = null;
	private Statement stmt = null;
	private Hashtable columnTypes = new Hashtable();
	private Hashtable columnTables = new Hashtable();
	private ISelectStatement curStmt = null;
	private boolean useEngineConnection = false;

	@Override
	public void execute() {
		try{
			Map<String, Object> map = (Map<String, Object>) engine.execQuery(query);
			stmt = (Statement) map.get(RDBMSNativeEngine.STATEMENT_OBJECT);
			Object connObj = map.get(RDBMSNativeEngine.CONNECTION_OBJECT);
			if(connObj==null){
				useEngineConnection = true;
				connObj = map.get(RDBMSNativeEngine.ENGINE_CONNECTION_OBJECT);
			}
			conn = (Connection) connObj;
			rs = (ResultSet) map.get(RDBMSNativeEngine.RESULTSET_OBJECT);

			setVariables(); //get the variables
		} catch (Exception e){
			e.printStackTrace();
			//in case query times out, close rs object..
			if(useEngineConnection)
				ConnectionUtils.closeAllConnections(null, rs, stmt);
			else
				ConnectionUtils.closeAllConnections(conn, rs, stmt);
		}
	}

	@Override
	public boolean hasNext() {
		boolean hasMore = false;
		curStmt = populateQueryResults();
		if(curStmt != null) {
			hasMore = true;
		} else {
			if(useEngineConnection)
				ConnectionUtils.closeAllConnections(null, rs, stmt);
			else
				ConnectionUtils.closeAllConnections(conn, rs, stmt);
		}
		return hasMore;
	}

	private ISelectStatement populateQueryResults(){
		ISelectStatement stmt = null;
		try {
			if(rs.next())
			{
				stmt = new SelectStatement();

				for(int colIndex = 0;colIndex < displayVar.length;colIndex++)
				{
					Object value = rs.getObject(displayVar[colIndex]);
					if(value == null) {
						value = "";
					}
					stmt.setVar(displayVar[colIndex], value);
					stmt.setRawVar(displayVar[colIndex], getRawValue(value, displayVar[colIndex]));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return stmt;
	}

	@Override
	public ISelectStatement next() {
		ISelectStatement stmt = curStmt;
		curStmt = null;
		return stmt;
	}

	private void setVariables(){
		try {
			//get rdbms type
			//			SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;
			//			String dbTypeString = engine.getProperty(Constants.RDBMS_TYPE);
			//			if (dbTypeString != null) {
			//				dbType = (SQLQueryUtil.DB_TYPE.valueOf(dbTypeString));
			//			}

			ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();

			var = new String[numColumns];
			displayVar = new String[numColumns];

			for(int colIndex = 1;colIndex <= numColumns;colIndex++)
			{
				String tableName = rsmd.getTableName(colIndex);
				String colName = rsmd.getColumnName(colIndex);
				String logName = colName;

				if(query.startsWith("SELECT")) {
					SQLQueryParser p = new SQLQueryParser(query);
					Hashtable<String, Hashtable<String, String>> h = p.getReturnVarsFromQuery(query);

					if(h != null && !h.isEmpty()) {
						for(String tab : h.keySet()) {
							if(tab.equalsIgnoreCase(tableName)) {
								for(String col : h.get(tab).keySet()) {
									if(h.get(tab).get(col).equalsIgnoreCase(colName)) {
										logName = col;
										break;
									}
								}
							}
						}
					}
				}
				// if(columnLabel.isEmpty() && dbType == SQLQueryUtil.DB_TYPE.SQL_Server){
				//		columnLabel = deriveTableName(columnLabel, columnLabel);
				// }

				var[colIndex-1] = colName;
				displayVar[colIndex-1] = logName;

				int type = rsmd.getColumnType(colIndex);
				columnTypes.put(displayVar[colIndex-1], type);

				if(logName != null && logName.length() != 0) // will use this to find what is the type to strap it together
				{
					columnTables.put(displayVar[colIndex-1], logName);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String[] getVariables() {
		// get the result set metadata to get the column names
		return displayVar;
	}

	@Override
	public String[] getDisplayVariables() {
		// get the result set metadata to get the column names
		return displayVar;
	}

	@Override
	public String[] getPhysicalVariables() {
		// get the result set metadata to get the column names
		return var;
	}

	private Object getRawValue(Object value, String header){
		if(value==null){
			return ""; //prevent null pointer exception.
		}

		int type = (int)columnTypes.get(header);
		Object tableObj = columnTables.get(header);
		String table = null;
		if(tableObj != null){
			table = tableObj + "";
		}

		String pk = "";
		if(header.contains("__")) {
			table = header.split("__")[0];
			pk = header.split("__")[1] + "/";
		}

		// there has to some way where I can say.. this is valid column type
		// we dont have this at this point.. for now I am just saying if this is 
		if(!value.toString().isEmpty() && (type == Types.LONGNVARCHAR || type == Types.VARCHAR || type == Types.CHAR || type == Types.LONGNVARCHAR || type == Types.NCHAR) && table!=null)
		{
			return Constants.CONCEPT_URI + pk + table + "/" + value;
		} else {
			return value;
		}
	}

	@Override
	public void cleanUp() {
		try {
			this.rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			this.stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
