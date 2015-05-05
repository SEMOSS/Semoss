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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Hashtable;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.ConnectionUtils;

public class RDBMSSelectWrapper extends AbstractWrapper implements ISelectWrapper {

	private ArrayList<ISelectStatement> queryResults = new ArrayList();
	public static String uri = "http://semoss.org/ontologies/concept";
	ResultSet rs = null;
	boolean hasMore = false;
	Hashtable columnTypes = new Hashtable();
	private int currentQueryIndex = 0;

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
		queryResults.clear(); //clear the query results arraylis
		rs = (ResultSet)engine.execQuery(query);
		setVariables(); //get the variables
		populateQueryResults();
		
		//close the result set
		ConnectionUtils.closeResultSet(rs);
	}

	@Override
	public boolean hasNext() {
		boolean hasMore = false;
		if(queryResults.size() > currentQueryIndex)
			hasMore = true;
		return hasMore;
	}
	
	private void populateQueryResults(){
		ISelectStatement stmt;
		try {
			while(rs.next()){
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
					if(type == Types.LONGNVARCHAR || type == Types.VARCHAR || type == Types.CHAR || type == Types.LONGNVARCHAR || type == Types.NCHAR)
					{
						qualifiedValue = uri + "/" + var[colIndex] + "/" + value;
						stmt.setRawVar(var[colIndex], qualifiedValue);
					}
					else if (type == Types.BIGINT){
						//a bit of a hack but this converts the big int object into double,
						//we had issues in the heatmapplaysheet with conversion of long to double
						Long valueLong = (long) value;
						double valueDouble = valueLong.doubleValue();
						stmt.setRawVar(var[colIndex], valueDouble); 
					} else
						stmt.setRawVar(var[colIndex], value);
				}
				queryResults.add(stmt);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public ISelectStatement next() {
		// TODO Auto-generated method stub
		ISelectStatement stmt = queryResults.get(currentQueryIndex);
		currentQueryIndex++;
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
}
