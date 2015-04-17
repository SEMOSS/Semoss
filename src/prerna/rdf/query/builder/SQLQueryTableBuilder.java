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
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Utility;

import com.google.gson.Gson;
import com.google.gson.internal.StringMap;

public class SQLQueryTableBuilder extends AbstractQueryBuilder{
	static final Logger logger = LogManager.getLogger(SQLQueryTableBuilder.class.getName());
	IEngine engine = null;
	
	Hashtable <String,String> aliases = new Hashtable<String,String>();
	Hashtable <String, String> tableProcessed = new Hashtable<String, String>();
	Hashtable <String, String> columnProcessed = new Hashtable<String,String>();
	ArrayList<String> totalVarList = new ArrayList<String>();
	ArrayList<Hashtable<String,String>> nodeV = new ArrayList<Hashtable<String,String>>();
	ArrayList<Hashtable<String,String>> predV = new ArrayList<Hashtable<String,String>>();
	ArrayList<Hashtable<String,String>> nodePropV = new ArrayList<Hashtable<String,String>>();
	String variableSequence = "";
	int limit = 500;
	int limitFilter = 10000;
	
	Hashtable <String,ArrayList<String>> tableHash = new Hashtable <String,ArrayList<String>>(); // contains the processed node name / table name and the properties
	String joins = "";
	String selectors = "";
	String froms = "";
	String filters = "";


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
		
		if(joins.length() > 0 && filters.length() > 0)
			joins = joins + " AND " + filters;
		else if(filters.length() > 0)
			joins = filters;
		
		// now that this is done
		if(joins.length() > 0)
			joins = " WHERE " + joins;
		query = "SELECT DISTINCT " + selectors + "  FROM  " + froms + joins + " LIMIT " + limit ;

	}
	
	
	protected void parsePath(){
		Hashtable<String, ArrayList> parsedPath = QueryBuilderHelper.parsePath(allJSONHash);
		totalVarList = parsedPath.get(QueryBuilderHelper.totalVarListKey);
		nodeV = parsedPath.get(QueryBuilderHelper.nodeVKey);
		predV = parsedPath.get(QueryBuilderHelper.predVKey);
	}
	
	@Override
	public void setJSONDataHash(Hashtable<String, Object> allJSONHash) {
		Gson gson = new Gson();
		this.allJSONHash = new Hashtable<String, Object>();
		this.allJSONHash.putAll((StringMap) allJSONHash.get("QueryData"));
		ArrayList<StringMap> list = (ArrayList<StringMap>) allJSONHash.get("SelectedNodeProps") ;
		this.nodePropV = new ArrayList<Hashtable<String, String>>();
		for(StringMap map : list){
			Hashtable hash = new Hashtable();
			hash.putAll(map);
			nodePropV.add(hash);
		}
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
		
		assimilateNodes();
		assimilateProperties();
		createSelectors();
		
		
		for(int predIndex = 0;predIndex < predV.size();predIndex++)
		{
			// get the predicate
			Hashtable <String,String> predInfoHash = predV.get(predIndex);
			// get the predicate out of it
			String predicate = predInfoHash.get("Pred");
			// split it in the dot
			// this is of the format
			// from_tablename.columnname.to_tablename.columnname
			predicate = Utility.getInstanceName(predicate);
			String [] items = predicate.split("\\.");
			// this will yield 4 strings
			String fromTable = items[0];
			String fromColumn = items[1];
			String toTable = items[2];
			String toColumn = items[3];
			
			//addSelfProp(fromTable);
			//addSelfProp(toTable);
			
			if(!tableProcessed.containsKey(fromTable))
			{
				tableProcessed.put(fromTable, fromTable);
			}
			if(!tableProcessed.containsKey(toTable))
			{
				// create columns for this table
				tableProcessed.put(toTable, toTable);
			}
			
			String join = getAlias(fromTable) + "." + fromColumn + "=" + getAlias(toTable) + "." + toColumn;

			
			//String join = "( " + getAlias(fromTable) + "." + fromColumn + "=" + getAlias(toTable) + "." + toColumn;
			//join += " OR " + getAlias(fromTable) + "." + fromColumn + " IS NULL ";
			//join += " OR " + getAlias(toTable) + "." + toColumn + " IS NULL ) ";

			if(joins.length() > 0)
				joins = joins + " AND " + join;
			else
				joins = join;
		}		
		
		// finalie the filters
		filterData();
		
	}
	
	
	private void filterData()
	{
		StringMap<ArrayList<Object>> filterResults = (StringMap<ArrayList<Object>>) allJSONHash.get(filterKey);
		
		/**
		 * {TITLE=[http://semoss.org/ontologies/concept/TITLE/127_Hours, http://semoss.org/ontologies/concept/TITLE/12_Years_a_Slave, http://semoss.org/ontologies/concept/TITLE/16_Blocks, http://semoss.org/ontologies/concept/TITLE/17_Again]}
		 * {TITLE=[http://semoss.org/ontologies/concept/TITLE/American Hustle]}
		 * 
		 */
		
		if(filterResults != null)
		{
		
			Iterator <String> keys = filterResults.keySet().iterator();
			for(int colIndex = 0;keys.hasNext();colIndex++) // process one column at a time. At this point my key is title on the above
			{
				String columnValue = keys.next(); // this gets me title above
				// need to split when there are underscores
				// for now keeping it simple
				String alias = getAlias(columnValue);
				// get the list
				ArrayList <Object> filterValues = (ArrayList<Object>)filterResults.get(columnValue);
				
				//transform the column value
				columnValue = alias + "." + columnValue;
				
				for(int filterIndex = 0;filterIndex < filterValues.size();filterIndex++)
				{
					String instance = Utility.getInstanceName(filterValues.get(filterIndex) + "");
					instance.replaceAll("'", "''");
					if(filters.length() > 0)
						filters = filters + " OR " + columnValue + " = '" + instance + "'";
					else
						filters = "(" + columnValue + " = '" + instance + "'";
				}
			}
			if(filters.length() > 0)
				filters = filters + ")";
		}
	}
	
	private void addSelfProp(String tableName)
	{
		ArrayList <String> propList = new ArrayList<String>();
		if(tableHash.containsKey(tableName))
			propList = tableHash.get(tableName);
		propList.add(tableName);
		tableHash.put(tableName, propList);
	}
	
	private void assimilateNodes()
	{
		if(nodeV != null)
		{
			for(int listIndex = 0;listIndex < nodeV.size();listIndex++)
			{
				Hashtable <String,String> node = nodeV.get(listIndex);
				String prop = (String) node.get("varKey");
				ArrayList <String> propList = new ArrayList<String>();
				if(tableHash.containsKey(prop))
					propList = tableHash.get(prop);
				propList.add(prop);
				tableHash.put(prop, propList);
				addToVariableSequence(prop);
			}
			
		}
	}
	
	private void addToVariableSequence(String asName)
	{
		if(variableSequence.length() > 0)
			variableSequence = variableSequence + ";" + asName.toUpperCase();
		else
			variableSequence = asName.toUpperCase();
	}
	
	private void assimilateProperties()
	{
		// the list has three properties
		// nodePropV is the list
		/**
		 * This is of this format
		 * SelectedNodeProps=[{SubjectVar=Title, uriKey=TITLE, varKey=Title__TITLE}]
		 */
		if(nodePropV != null)
		{
			for(int listIndex = 0;listIndex < nodePropV.size();listIndex++)
			{
				String key = (String) nodePropV.get(listIndex).get("SubjectVar");
				String prop = (String) nodePropV.get(listIndex).get("uriKey");
				ArrayList <String> propList = new ArrayList<String>();
				if(tableHash.containsKey(key))
					propList = tableHash.get(key);
				propList.add(prop);
				tableHash.put(key, propList);
				addToVariableSequence(key + "__" + prop);
			}
		}
		// now that I am done with the properties

	}
	
	private void createSelectors()
	{
		String columnSubString = "";
		//ArrayList <String> tColumns = getColumnsFromTable(tableName);
		// instead get this from what they sent
	
		String [] columns = variableSequence.split(";");
		
		for(int colIndex = 0;colIndex < columns.length;colIndex++)
		{
			String tableName = null;
			String colName = columns[colIndex];
			tableName = colName;
			
			if(colName.contains("__"))
			{
				tableName = colName.substring(0, colName.indexOf("__"));
				colName = colName.substring(colName.indexOf("__") + 2);
			}
			
			String alias = getAlias(tableName);
			String asName = colName;

			if(!tableName.equalsIgnoreCase(colName)) // this is a self reference dont worry about it something like title.title
				asName = tableName + "__" + colName;
			
			if(!columnProcessed.containsKey(asName.toUpperCase()))
			{
				// the as query needs to reflect how I am sending eventually on the var headers
				// the variable name is really
				// tableName__columnName <-- yes that is a double underscore
				
				if(selectors.length() == 0)
					selectors = alias + "." + colName + " AS " + asName;
				else
					selectors = selectors + " , " + alias + "." + colName + " AS " + asName;
				
				columnProcessed.put(asName.toUpperCase(), asName.toUpperCase());					
			}
			addFrom(tableName, alias);
		}
	}
	
	private void addFrom(String tableName, String alias)
	{
		if(!tableProcessed.containsKey(tableName.toUpperCase()))
		{
			tableProcessed.put(tableName.toUpperCase(),"true");
			if(froms.length() > 0)
				froms = froms + " , " + tableName + "  " + alias;
			else
				froms = tableName + "  " + alias;			
		}
	}
	
	
	private String getAlias(String tableName)
	{
		tableName = tableName.toUpperCase();
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
				aliasComplete = !aliases.containsValue(tryAlias);
				count++;
			}
			response = tryAlias;
			aliases.put(tableName, tryAlias);
		}
		return response;
	}
	
	public ArrayList<Hashtable<String,String>> getHeaderArray(){
		
		ArrayList<Hashtable<String,String>> retArray = new ArrayList<Hashtable<String,String>>();
		retArray.addAll(nodeV);
		retArray.addAll(nodePropV);
		
		Hashtable <String,Hashtable<String,String>> sequencer = new Hashtable <String, Hashtable<String,String>>();
		// add the filter queries
		// each list looks like this
		// Prop looks like this SelectedNodeProps=[{SubjectVar=Title, uriKey=TITLE, varKey=Title__TITLE}
		// node looks like a prop except it has no varKey I am told
		// i need a way to align this back to what I am spitting out upfront
		
		String tempjoins = joins;
		//if its already built, skip ahead
		if(!tempjoins.contains("WHERE")){
			if(joins.length() > 0 && filters.length() > 0)
				tempjoins = joins + " AND " + filters;
			else if(filters.length() > 0)
				tempjoins = filters;
			
			// now that this is done
			if(tempjoins.length() > 0)
				tempjoins = " WHERE " + tempjoins;
		}
		
		//we need to encode the equals signs, for when json sends the qry back over
		tempjoins = tempjoins.replaceAll("=", "%3D");
		
		for(Hashtable<String, String> headerHash : retArray){

			String varName = headerHash.get(QueryBuilderHelper.varKey); // title__rottenTomatoes
			String key = varName;
			// the var key needs to match the capitalization
			headerHash.put(QueryBuilderHelper.varKey, varName.toUpperCase());
			String tableName = (String) headerHash.get("uriKey"); //semoss.org/title.. 
			tableName = Utility.getInstanceName(tableName);
			String filterQuery = "";
			// need to modify the filter if there is an underscore to skip only to the last one
			if(varName.contains("__"))
			{
				tableName = varName.substring(0,varName.indexOf("__"));
				varName = varName.substring(varName.indexOf("__") + 2);
			}
			
			if(tempjoins.length() >0){
				//filter that query down even further, make sure you are showing the distinct values for that query.
				filterQuery = "SELECT DISTINCT " + varName + " FROM " + tableName + " WHERE " + varName + 
					" in (SELECT " + varName + "  FROM  " + froms + tempjoins +") ";
			} else {
				filterQuery = "SELECT DISTINCT " + varName + " FROM " + tableName ;
			}
			filterQuery += " ORDER BY 1 LIMIT " +  limitFilter;

			headerHash.put(QueryBuilderHelper.queryKey, filterQuery.toUpperCase());
			sequencer.put(key.toUpperCase(), headerHash);
			
			
			
		}
		
		retArray = new ArrayList<Hashtable<String,String>>();
		String [] vars = variableSequence.split(";");
		for(int varIndex = 0;varIndex < vars.length;varIndex++)
			retArray.add(sequencer.get(vars[varIndex].toUpperCase()));
		
		return retArray;
	}
	
	private ArrayList<String> getColumnsFromTable(String table)
	{
		String query = "SHOW COLUMNS from " + table;
		ISelectWrapper sWrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		ArrayList <String> columns = new ArrayList<String>();
		while(sWrapper.hasNext())
		{
			ISelectStatement stmt = sWrapper.next();
			columns.add(stmt.getVar("COLUMN_NAME")+"");
		}
		return columns;
	}
}
