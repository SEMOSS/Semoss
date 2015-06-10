/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.ConnectionUtils;

public class RDBMSSelectWrapper extends AbstractWrapper implements ISelectWrapper {

	private ArrayList<ISelectStatement> queryResults = new ArrayList();
	public static String uri = "http://semoss.org/ontologies/Concept";
	ResultSet rs = null;
	boolean hasMore = false;
	Hashtable columnTypes = new Hashtable();
	Hashtable columnTables = new Hashtable();
	private ISelectStatement curStmt = null;
		
	@Override
	public void execute() {
		// TODO Auto-generated method stub
		try{
			query = query.replaceAll("\\s+", " "); //normalize spaces

			rs = (ResultSet)engine.execQuery(query);
			setVariables(); //get the variables
		} catch (Exception e){
			e.printStackTrace();
			//in case query times out, close rs object..
			ConnectionUtils.closeResultSet(rs);
		}
	}

	@Override
	public boolean hasNext() {
		boolean hasMore = false;
		curStmt = populateQueryResults();
		if(curStmt != null) {
				hasMore = true;
		} else {
			ConnectionUtils.closeResultSet(rs);
		}
		return hasMore;
	}
	
	private ISelectStatement populateQueryResults(){
		ISelectStatement stmt = null;
		HashSet<String> dupCheck = new HashSet();
		boolean added = false;
		int rsCount = 0;
		try {
				rsCount++;
				if(rs.next())
				{
					stmt = new SelectStatement();
	
					for(int colIndex = 0;colIndex < var.length;colIndex++)
					{
						Object value = rs.getObject(var[colIndex]);
						
						if(value==null){
							value = ""; //prevent null pointer exception.
						}
						
						stmt.setVar(var[colIndex], value);
						//set the type and URI based on the type
						int type = (int)columnTypes.get(var[colIndex]);
						String qualifiedValue = null;
						
						
						// there has to some way where I can say.. this is valid column type
						// we dont have this at this point.. for now I am just saying if this is 
						if((type == Types.LONGNVARCHAR || type == Types.VARCHAR || type == Types.CHAR || type == Types.LONGNVARCHAR || type == Types.NCHAR) && columnTables.containsKey(var[colIndex]))
						{
							qualifiedValue = uri + "/" + toCamelCase(columnTables.get(var[colIndex]) + "") + "/" + value;
							stmt.setRawVar(var[colIndex], qualifiedValue);
						}
						else if (type == Types.BIGINT ){
							//this converts the big int object into double,
							//we had issues in the heatmapplaysheet with conversion of long to double
							Long valueLong = (long) value;
							double valueDouble = valueLong.doubleValue();
							stmt.setRawVar(var[colIndex], valueDouble); 
						} else if (type == Types.REAL){
							//this converts float to double as needed for the parcords play sheet
							Float valueFloat = (float) value;
							double valueDouble = valueFloat.doubleValue();
							stmt.setRawVar(var[colIndex], valueDouble);
						} else {
							stmt.setRawVar(var[colIndex], value);
						}
					}
				}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return stmt;
	}

	@Override
	public ISelectStatement next() {
		// TODO Auto-generated method stub
		ISelectStatement stmt = curStmt;
		curStmt = null;
		return stmt;
	}

	private void setVariables(){
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();
			
			var = new String[numColumns];
			
			for(int colIndex = 1;colIndex <= numColumns;colIndex++)
			{
				var[colIndex-1] = rsmd.getColumnLabel(colIndex);
				
				int type = rsmd.getColumnType(colIndex);
				columnTypes.put(var[colIndex-1], type);
				//columnTypes.put(toCamelCase(var[colIndex-1]), type);
				String tableName = rsmd.getTableName(colIndex);
				//columnTypes.put(var[colIndex-1], type);

				if(tableName != null && tableName.length() != 0) // will use this to find what is the type to strap it together
				{
					tableName = toCamelCase(tableName);
					columnTables.put(var[colIndex-1], tableName);
					//columnTables.put(toCamelCase(var[colIndex-1]), tableName);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public String[] getVariables() {
		// get the result set metadata to get the column names
		return var;
	}
	
	private Object typeConverter(Object input, int type)
	{
		Object retObject = input;
		switch(type)
		{
			
		// to be implemented later
		
		}
		
		return retObject;
	}
		
	public String toCamelCase(String input)
	{
		String output = input.substring(0,1).toUpperCase() + input.substring(1).toLowerCase();
		System.out.println("Output is " + output);
		return output;
	}

	
}
