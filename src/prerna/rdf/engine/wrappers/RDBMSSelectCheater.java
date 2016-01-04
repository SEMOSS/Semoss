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
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.query.builder.SQLQueryTableBuilder;
import prerna.rdf.util.SQLQueryParser;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class RDBMSSelectCheater extends AbstractWrapper implements IConstructWrapper {

	private ArrayList<ISelectStatement> queryResults = new ArrayList();
	ResultSet rs = null;
	Connection conn = null;
	Statement stmt = null;
	boolean hasMore = false;
	Hashtable columnTypes = new Hashtable();
	private int currentQueryIndex = 0;
	IConstructStatement curStmt = null;
	Hashtable columnTables = new Hashtable();
	String subjectParent = null;
	String subject = null;
	String objectParent = null;
	String object = null;
	String predParent = null;
	String predicate = null;

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		try{
			curStmt = null;
			Map<String, Object> map = (Map<String, Object>) engine.execQuery(query);
			stmt = (Statement) map.get(RDBMSNativeEngine.STATEMENT_OBJECT);
			conn = (Connection) map.get(RDBMSNativeEngine.CONNECTION_OBJECT);
			rs = (ResultSet) map.get(RDBMSNativeEngine.RESULTSET_OBJECT);
			setVariables(); //get the variables
			//populateQueryResults();
			
			//close the result set
			//ConnectionUtils.closeResultSet(rs);
		} catch (Exception e){
			e.printStackTrace();
			ConnectionUtils.closeAllConnections(conn, rs, stmt);
		}
	}

	@Override
	public boolean hasNext() {
		boolean hasMore = false;
		curStmt = null;
		if(subjectParent == null && objectParent == null && predParent == null)
		{
			curStmt = populateQueryResults();
			if(curStmt != null)
				hasMore = true;
			else
				ConnectionUtils.closeAllConnections(conn, rs, stmt);
		}
		else
			hasMore = true;
		return hasMore;
	}
	
	private IConstructStatement populateQueryResults(){
		IConstructStatement stmt = null; // I know I need to run the magic of doing multiple indexes, but this is how we run it for now i.e. assumes 3 only
		try {
			if(rs.next()){
				stmt = new ConstructStatement();
				subject = rs.getObject(var[0]) + "" ;
				predicate = "" ;
				
				// var[2] might be empty String for SQL Server because of rs metadata error - if so, get the header by index instead
				if(!var[2].equals("")){
					object  = rs.getObject(var[2]) + "";
				} else {
					object = rs.getObject(3).toString();
				}
				if(rs.getObject(var[0]) != null && columnTables.contains(var[0].toUpperCase()))
				{
					String displayName = displayVar[0];
					subjectParent = engine.getTransformedNodeName(Constants.DISPLAY_URI + displayName, false);
					subject = Constants.CONCEPT_URI + displayName + "/"  + subject + ""; 
				}
				if(rs.getObject(var[2]) != null && columnTables.contains(var[2].toUpperCase()))
				{
					String displayName = displayVar[2];
					objectParent = engine.getTransformedNodeName(Constants.DISPLAY_URI + displayName, false);
					object = Constants.CONCEPT_URI + displayName + "/"  + object + ""; 
				}
				if(rs.getObject(var[1]) != null)
				{
					if(!var[1].equals("")){
						predicate = rs.getObject(var[1]) + "";
					} else {
						predicate = rs.getObject(2) + "";
					}
					predParent = Utility.getQualifiedClassName(predicate);
				}
				stmt.setSubject(subject);
				stmt.setObject(object);
				stmt.setPredicate(predicate);				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return stmt;
	}
	

	@Override
	public IConstructStatement next() {
		// TODO Auto-generated method stub
		IConstructStatement retSt = null;
		if(curStmt != null)
		{
			retSt = curStmt;
			curStmt = null;
		}
		else if(subject != null && subjectParent != null)
		{
			// give the subject superclass
			retSt = makeConstruct(subject, RDF.TYPE+"", subjectParent);
			subject = null;
			subjectParent = null;
		}
		else if(object != null && objectParent != null )
		{
			// give the subject superclass
			retSt = makeConstruct(object, RDF.TYPE+"", objectParent);
			object = null;
			objectParent = null;
		}
		else if(predicate != null && predParent != null)
		{
			// give the subject superclass
			retSt = makeConstruct(predicate, RDFS.SUBPROPERTYOF+"", predParent);
			predicate = null;
			predParent = null;
		}
		return retSt;
	}
	
	private IConstructStatement makeConstruct(String subject, String predicate, String object)
	{
		IConstructStatement retSt = new ConstructStatement();
		retSt = new ConstructStatement();
		retSt.setSubject(subject);
		retSt.setPredicate(predicate);
		retSt.setObject(object);
		return retSt;
		
	}
	
	/**
	 * setting variables to be used when querying for traverse freely/explore an instance of a concept.  FYI not using displaynames var 
	 * because we actuallly do the translation later when we pass through GraphDataModel.
	 */
	private void setVariables(){
		try {
			
			//get rdbms type
			SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;
			String dbTypeString = engine.getProperty(Constants.RDBMS_TYPE);
			if (dbTypeString != null) {
				dbType = (SQLQueryUtil.DB_TYPE.valueOf(dbTypeString));
			}
		
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
//				if(columnLabel.isEmpty() && dbType == SQLQueryUtil.DB_TYPE.SQL_Server){
//					columnLabel = deriveTableName(columnLabel, columnLabel);
//				}
				
				// weird thing that happens when we have T.Title -> rs.getObject expects Title, not T.Title
				if(logName.contains(".") && !logName.contains("http://semoss.org/ontologies") && !logName.contains(RDF.TYPE + "")
						&& !logName.contains(RDFS.SUBCLASSOF + "") && !logName.contains(RDFS.SUBPROPERTYOF + "")) {
					String[] logSplit = logName.split("\\.");
					logName = logSplit[1].toUpperCase();
				}
				
				var[colIndex-1] = logName;
				displayVar[colIndex-1] = logName;
				
				int type = rsmd.getColumnType(colIndex);
				columnTypes.put(displayVar[colIndex-1], type);
				
				if(logName != null && logName.length() != 0) // will use this to find what is the type to strap it together
				{
					columnTables.put(displayVar[colIndex-1], logName);
				}
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String deriveTableName(String tableName, String columnLabel){

		//Originally written for Maria db as it was having trouble getting the table name using the result set metadata method getTableName, 
		//this logic is for us to work around this issue (ie it used to get the alias and we would be able to get the table name from backtracking the alias
		boolean getTableNameFromColumn = tableName.equals("");
		if(!getTableNameFromColumn){
			//first see if this really is a table name, so if an alias exists then you can use this tableName
			String tableAlias = SQLQueryTableBuilder.getAlias(tableName);
			if(tableAlias.length()==0){
				//if no alias was returned, assume that maybe the value you got was an alias, 
				//so try to use the alias to get the tableName
				String tableNameFromAlias = SQLQueryTableBuilder.getTableNameByAlias(tableName);
				if(tableNameFromAlias.length()>0){
					tableName = tableNameFromAlias;
				} else {
					getTableNameFromColumn = true;//if you still have no tableName, try to use the columnLabel to get your tableName
				}
			}
		}
		
		//use columnName to derive table name
		if(getTableNameFromColumn){
			tableName = columnLabel;
			if(columnLabel.contains("__")){
				String[] splitColAndTable = tableName.split("__");
				tableName = splitColAndTable[0];
			}
		}
	return tableName;
		
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
