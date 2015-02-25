/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.util.TriplePart;

public class SQLQueryTableBuilder extends AbstractQueryBuilder{
	static final Logger logger = LogManager.getLogger(SQLQueryTableBuilder.class.getName());
	IEngine engine = null;
	Hashtable <String,String> aliases = new Hashtable<String,String>();
	Hashtable <String, String> tableProcessed = new Hashtable<String, String>();
	ArrayList<String> totalVarList = new ArrayList<String>();
	ArrayList<Hashtable<String,String>> nodeV = new ArrayList<Hashtable<String,String>>();
	ArrayList<Hashtable<String,String>> predV = new ArrayList<Hashtable<String,String>>();
	ArrayList<Hashtable<String,String>> nodePropV = new ArrayList<Hashtable<String,String>>();
	String joins = "";
	String columns = "";
	String froms = "";
	public Hashtable<String, Object> allJSONHash = new Hashtable<String, Object>();


	public SQLQueryTableBuilder(IEngine engine)
	{
		this.engine = engine;
	}
	
	@Override
	public void buildQuery() 
	{
		parsePath();
		// we are assuming properties are passed in now based on user selection
//		parsePropertiesFromPath(); 
		configureQuery();	
		
		// now that this is done
		query = "SELECT " + columns + "  FROM  " + froms + "  WHERE " + joins;
	}
	
	
	protected void parsePath(){
		Hashtable<String, ArrayList> parsedPath = QueryBuilderHelper.parsePath(allJSONHash);
		totalVarList = parsedPath.get(QueryBuilderHelper.totalVarListKey);
		nodeV = parsedPath.get(QueryBuilderHelper.nodeVKey);
		predV = parsedPath.get(QueryBuilderHelper.predVKey);
	}

	protected void configureQuery()
	{
		/*
		 * 					predInfoHash.put("Subject", subjectURI);
					predInfoHash.put("SubjectVar", subjectName);
					predInfoHash.put("Pred", predURI);
					predInfoHash.put("Object", objectURI);
					predInfoHash.put("ObjectVar", objectName);
					predInfoHash.put(uriKey, predURI);
					predInfoHash.put(varKey, predName);

		 */
		// I need to pull each predicate
		// from the predicate identify what are the tables the user is querying
		// use alias for each one of these 
		// for each table I need to also get the column names in this table
		// for which I need to execute a query to get it
		// and then jam it completely in
		// 
		for(int predIndex = 0;predIndex < predV.size();predIndex++)
		{
			// get the predicate
			Hashtable <String,String> predInfoHash = predV.get(predIndex);
			// get the predicate out of it
			String predicate = predInfoHash.get("Pred");
			// split it in the dot
			// this is of the format
			// from_tablename.columnname.to_tablename.columnname
			String [] items = predicate.split(".");
			// this will yield 4 strings
			String fromTable = items[0];
			String fromColumn = items[1];
			String toTable = items[2];
			String toColumn = items[3];
			
			if(!tableProcessed.containsKey(fromTable))
			{
				getColumnString(fromTable);
				tableProcessed.put(fromTable, fromTable);
			}
			if(!tableProcessed.containsKey(toTable))
			{
				// create columns for this table
				getColumnString(toTable);
				tableProcessed.put(toTable, toTable);
			}
			
			String join = getAlias(fromTable) + "." + fromColumn + "=" + getAlias(toTable) + "." + toColumn;
			if(joins.length() > 0)
				joins = joins + " AND " + join;
			else
				joins = join;
		}		
	}
	
	private void getColumnString(String tableName)
	{
		String columnSubString = "";
		ArrayList tColumns = getColumnsFromTable(tableName);
		String alias = getAlias(tableName);
		for(int colIndex = 0;colIndex < tColumns.size();colIndex++)
		{
			if(colIndex == 0)
				columnSubString = alias + "." + tColumns.get(colIndex) + " AS " + tColumns.get(colIndex);
			else
				columnSubString = columnSubString + " , " + alias + "." + tColumns.get(colIndex) + " AS " + tColumns.get(colIndex);
		}
		// add it to the columns
		if(columns.length() > 0)
			columns = columns + " , " + columnSubString;
		else
			columns = columnSubString;
		
		// also add to the from
		// add it to the columns
		if(froms.length() > 0)
			froms = froms + " , " + tableName + "  " + alias;
		else
			columns = tableName + "  " + alias;
	}
	
	
	private String getAlias(String tableName)
	{
		String response = null;
		if(aliases.containsKey(tableName))
			response = aliases.get(tableName);
		else
		{
			boolean aliasComplete = false;
			int count = 0;
			String tryAlias = "";
			while(!aliasComplete)
			{
				tryAlias = tryAlias + tableName.charAt(count);
				aliasComplete = !aliases.containsKey(tryAlias);
			}
			response = tryAlias;
			aliases.put(tableName, tryAlias);
		}
		return response;
	}
	
	private ArrayList<String> getColumnsFromTable(String table)
	{
		String query = "SHOW COLUMNS from " + table;
		ISelectWrapper sWrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		ArrayList <String> columns = new ArrayList<String>();
		while(sWrapper.hasNext())
		{
			ISelectStatement stmt = sWrapper.next();
			columns.add(stmt.getVar("FIELD")+"");
		}
		return columns;
	}
}
