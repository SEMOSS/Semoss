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
package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import prerna.rdf.query.util.ISPARQLReturnModifier;
import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;
import prerna.rdf.query.util.SPARQLAbstractReturnModifier;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public abstract class AbstractSpecificQueryBuilder {
	protected ArrayList<Hashtable<String, String>> parameters;
	protected SEMOSSQuery baseQuery;
	protected String query;
	Hashtable<String, String> aliases = new Hashtable<String, String>();
	Hashtable <String, String> tablesProcessed = new Hashtable<String, String>();
	String paramString = "";
	String tableString =  "";
	String joinString = "";
	String groupBy = "";
	ArrayList<String> joinsArr = new ArrayList();
	ArrayList<String> leftJoinsArr = new ArrayList();
	ArrayList<String> rightJoinsArr = new ArrayList();
	
	public AbstractSpecificQueryBuilder(ArrayList<Hashtable<String, String>> parameters, SEMOSSQuery baseQuery) {
		this.baseQuery = baseQuery;
		this.parameters = parameters;
	}
	
	protected void addParam(String clauseKey){
		SEMOSSQueryHelper.addParametersToQuery(parameters, baseQuery, clauseKey);
	}
	
	protected void createQuery() {
		baseQuery.createQuery();
		query = baseQuery.getQuery();
	}
	
	protected void createSQLQuery() {
		baseQuery.createSQLQuery(paramString , tableString, joinString, groupBy, joinsArr, leftJoinsArr, rightJoinsArr);
		query = baseQuery.getQuery();
	}
	
	protected void addReturnVariable(String colName, String varName, SEMOSSQuery semossQuery, String mathFunc){
		if(!mathFunc.isEmpty()){
			SEMOSSQueryHelper.addMathFuncToQuery(mathFunc, colName, semossQuery, varName); // add one to account for label
		}
		else if (colName.equals(varName)){
			SEMOSSQueryHelper.addSingleReturnVarToQuery(colName, semossQuery);
		}
		else { 
			ISPARQLReturnModifier mod = SEMOSSQueryHelper.createReturnModifier(colName, SPARQLAbstractReturnModifier.NONE);
			SEMOSSQueryHelper.addSingleReturnVarToQuery(varName, mod, semossQuery);
		}
	}
	
	protected ArrayList<String> uniqifyColNames(List<String> colNames){
		ArrayList<String> varNames = new ArrayList<String>();
		for (int i=0; i < colNames.size(); i++) {
			String nameInQuestion = colNames.get(i);
			int counter = 1;
			if(nameInQuestion != null){
				//check to see if same var is selected multiple times; then concatenate counter based on number of times dupe var is used
				for(int k=0; k < i; k++){
					String previousName = colNames.get(k);
					if(previousName != null){
						if(nameInQuestion.equals(previousName)) {
							counter++;
						}
					}
				}
			}
			if(counter>1)
				varNames.add(nameInQuestion + counter);
			else
				varNames.add(nameInQuestion);
		}
		return varNames;
	}
	
	protected String generateSelectorsString(ArrayList<String> labelList){
		ArrayList<String> varNames = uniqifyColNames(labelList);
		String selectorsString = "";
		for(int i=0; i < labelList.size(); i++){
			String columnName = labelList.get(i);
			String tableName = columnName;
			if(columnName.contains("__")){
				tableName = columnName.substring(0, columnName.indexOf("__"));
				columnName = columnName.substring(columnName.indexOf("__") + 2);
			}
			String alias = getAlias(tableName);
			String asName = columnName;

			if(!tableName.equalsIgnoreCase(columnName)) // this is a self reference dont worry about it something like title.title
				asName = tableName + "__" + columnName;
			
			if(selectorsString.length() > 0)  selectorsString += " , ";
			selectorsString +=  alias + "." + columnName + " AS " + asName;
			//logger.info("Adding variable: " + columnName);
			//addReturnVariable(varName, varNames.get(i), baseQuery, "");
		}
		return selectorsString;
	}
	
	protected String generateSelectorsSubString(String fullColName, String colAlias, boolean isGroupBy, String valueMathFunc){
		String returnParamStr = "";
		String tableName = fullColName;
		String colName = fullColName;
		
		//do the label column first
		//colName = label; //tableName = label;
		if(colName.contains("__"))
		{
			tableName = colName.substring(0, colName.indexOf("__"));
			colName = colName.substring(colName.indexOf("__") + 2);			
		}
		
		colName = getAlias(tableName) + "." + colName;
		
		//generate the group by statement first, before changing the colname for the aggregate column
		if(isGroupBy){
			if(groupBy.length() > 0) 
				groupBy += " , ";
			groupBy += colName;
		}
		
		//change the column name for the aggregate column if there is a function passed in.
		if(valueMathFunc.length() > 0){
			String sqlMathFunc = mapMath(valueMathFunc);
			if(sqlMathFunc != null)
				colName = sqlMathFunc + colName + ")";
		}
		
		returnParamStr = colName + " AS " + colAlias;

		return returnParamStr;
	}
	
	public String getQuery() {
		return query;
	}
	
	public abstract void buildQuery();

	public abstract void buildQueryR();
	
	protected String getAlias(String tableName)
	{
		return SQLQueryTableBuilder.getAlias(tableName);
	}
	
	//used for RDBMS functions
	protected String mapMath(String function)
	{
		String retString = null;
		
		if(function.equalsIgnoreCase("Average"))
			retString = "avg(";
		else if(function.equalsIgnoreCase("Count"))
			retString = "count(";
		else if(function.equalsIgnoreCase("Sum"))
			retString = "sum(";
		else if(function.equalsIgnoreCase("Max"))
			retString = "max(";
		else if(function.equalsIgnoreCase("Min"))
			retString = "min(";
		
		return retString;
	}
	
	public void addParameters()
	{
		// this is supposed to append to the where
		for(int paramIndex = 0;paramIndex < parameters.size();paramIndex++)
		{
			Hashtable<String, String> thisParam = parameters.get(paramIndex);
			// the key 
			// get the value
			Enumeration <String> keys = thisParam.keys();
			while(keys.hasMoreElements())
			{
				String colName = keys.nextElement();
				String originalColName = colName;
				String tableName = colName;
				if(colName.contains("__"))
				{
					tableName = colName.substring(0,colName.indexOf("__"));
					colName = colName.substring(colName.indexOf("__") + 2);
				}
				addToTableString(tableName);
				String qualifiedColName = getAlias(tableName) + "." + colName;
				String condition = qualifiedColName + " = '@" + colName + "-" + tableName + ":" + colName + "@'";
				
				if(joinString.length() > 0)
					joinString = joinString + " AND " + condition;
				else
					joinString = condition;
				joinsArr.add(condition);
			}
		}
	}
	
	
	// this gets the rel triples and adds the join to the query
	/**
	 * relTriples=[[http://semoss.org/ontologies/Concept/Title, http://semoss.org/ontologies/Relation/Title.Title.Nominated_Title.Title_FK, http://semoss.org/ontologies/Concept/Nominated_Title], [http://semoss.org/ontologies/Concept/Title, http://semoss.org/ontologies/Relation/Title.Title.Studio.Title_FK, http://semoss.org/ontologies/Concept/Studio]]
	 * @param relTriples
	 */
	public void addJoins(ArrayList<ArrayList<String>> relTriples)
	{
		for(int joinIndex = 0;joinIndex < relTriples.size();joinIndex++)
		{
			ArrayList <String> thisList = relTriples.get(joinIndex);
			if(thisList.size()==1){
				String tableNameURI = thisList.get(0);
				int lastSlash = tableNameURI.lastIndexOf("/");
				String tableName = tableNameURI.substring(lastSlash+1);
				addToTableString(tableName);
				continue;
			}
			String predicate = thisList.get(1);
			if(predicate.contains(":"))
				predicate = Utility.getInstanceName(predicate);
			String [] items = predicate.split("\\.");
			// this will yield 4 strings
			String fromTable = items[0];
			String fromColumn = items[1];
			String toTable = items[2];
			String toColumn = items[3];
			
			String join = getAlias(fromTable) + "." + fromColumn + "=" + getAlias(toTable) + "." + toColumn;
			
			joinsArr.add(join);
			if(joinString.length() > 0)
				joinString = joinString + " AND " + join;
			else
				joinString = join;
			
			addToTableString(fromTable);
			addToTableString(toTable);
		}		
	}
	
	
	protected void addToTableString(String tableName)
	{
		if(!tablesProcessed.containsKey(tableName.toUpperCase()))
		{
			SQLQueryUtil queryUtil = baseQuery.getQueryUtil();
			String alias = getAlias(tableName);
			String fromText = tableName + " " + alias;
			if(tableString.length() > 0){
				tableString = tableString + ", " + fromText;		
				rightJoinsArr.add(queryUtil.getDialectOuterJoinRight(fromText));
				leftJoinsArr.add(queryUtil.getDialectOuterJoinLeft(fromText));
			} else {
				tableString = fromText;
				rightJoinsArr.add(fromText);
				leftJoinsArr.add(fromText);
			}
			tablesProcessed.put(tableName.toUpperCase(), tableName.toUpperCase());
		}
	}
}
