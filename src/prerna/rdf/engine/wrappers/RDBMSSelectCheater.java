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
import java.util.Hashtable;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.util.SQLQueryParser;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public class RDBMSSelectCheater extends AbstractWrapper implements IConstructWrapper {

	private static final Logger classLogger = LogManager.getLogger(RDBMSSelectCheater.class);

	private ResultSet rs = null;
	private Connection conn = null;
	private Statement stmt = null;
	private Hashtable columnTypes = new Hashtable();
	private IConstructStatement curStmt = null;
	private Hashtable columnTables = new Hashtable();
	private String subjectParent = null;
	private String subject = null;
	private String objectParent = null;
	private String object = null;
	private String predParent = null;
	private String predicate = null;

	@Override
	public void execute() {
		try{
			curStmt = null;
			Map<String, Object> map = (Map<String, Object>) engine.execQuery(query);
			stmt = (Statement) map.get(RDBMSNativeEngine.STATEMENT_OBJECT);
			conn = (Connection) map.get(RDBMSNativeEngine.CONNECTION_OBJECT);
			rs = (ResultSet) map.get(RDBMSNativeEngine.RESULTSET_OBJECT);
			setVariables(); //get the variables
		} catch (Exception e){
			classLogger.error(Constants.STACKTRACE, e);
			ConnectionUtils.closeAllConnections(conn, stmt, rs);
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
				ConnectionUtils.closeAllConnections(conn, stmt, rs);
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
				subject = rs.getObject(rawHeaders[0]) + "" ;
				predicate = "" ;

				// var[2] might be empty String for SQL Server because of rs metadata error - if so, get the header by index instead
				if(!rawHeaders[2].equals("")){
					object  = rs.getObject(rawHeaders[2]) + "";
				} else {
					object = rs.getObject(3).toString();
				}
				if(rs.getObject(rawHeaders[0]) != null && columnTables.contains(rawHeaders[0].toUpperCase()))
				{
					String displayName = headers[0];
					//					subjectParent = engine.getTransformedNodeName(Constants.DISPLAY_URI + displayName, false);
					subject = Constants.CONCEPT_URI + displayName + "/"  + subject + ""; 
				}
				if(rs.getObject(rawHeaders[2]) != null && columnTables.contains(rawHeaders[2].toUpperCase()))
				{
					String displayName = headers[2];
					//					objectParent = engine.getTransformedNodeName(Constants.DISPLAY_URI + displayName, false);
					object = Constants.CONCEPT_URI + displayName + "/"  + object + ""; 
				}
				if(rs.getObject(rawHeaders[1]) != null)
				{
					if(!rawHeaders[1].equals("")){
						predicate = rs.getObject(rawHeaders[1]) + "";
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
			classLogger.error(Constants.STACKTRACE, e);
		}
		return stmt;
	}


	@Override
	public IConstructStatement next() {
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
			//			SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;
			//			String dbTypeString = engine.getProperty(Constants.RDBMS_TYPE);
			//			if (dbTypeString != null) {
			//				dbType = (SQLQueryUtil.DB_TYPE.valueOf(dbTypeString));
			//			}

			ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();

			rawHeaders = new String[numColumns];
			headers = new String[numColumns];

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

				// weird thing that happens when we have T.Title -> rs.getObject expects Title, not T.Title
				if(logName.contains(".") && !logName.contains("http://semoss.org/ontologies") && !logName.contains(RDF.TYPE + "")
						&& !logName.contains(RDFS.SUBCLASSOF + "") && !logName.contains(RDFS.SUBPROPERTYOF + "")) {
					String[] logSplit = logName.split("\\.");
					logName = logSplit[1].toUpperCase();
				}

				rawHeaders[colIndex-1] = logName;
				headers[colIndex-1] = logName;

				int type = rsmd.getColumnType(colIndex);
				columnTypes.put(headers[colIndex-1], type);

				if(logName != null && logName.length() != 0) // will use this to find what is the type to strap it together
				{
					columnTables.put(headers[colIndex-1], logName);
				}
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	@Override
	public void close() {
		ConnectionUtils.closeAllConnections(null, stmt, rs);
	}
}
