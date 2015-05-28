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
package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.util.SEMOSSQuery;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class QueryBuilderHelper {

	static final Logger logger = LogManager.getLogger(QueryBuilderHelper.class.getName());
	static final int subIdx = 0;
	static final int predIdx = 1;
	static final int objIdx = 2;
	static final int propIdx = 0;
	public static final String uriKey = "uriKey";
	public static final String queryKey = "queryKey";
	public static final String varKey = "varKey";
	public static final String totalVarListKey = "totalVarList";
	public static final String nodeVKey = "nodeV";
	public static final String predVKey = "predV";
	static final String relArrayKey = "relTriples";
	
	public static Integer runCountQuery(int maxCount, SEMOSSQuery semossQuery, IEngine coreEngine ){
		Integer curLimit = semossQuery.getLimit();
		semossQuery.setLimit(maxCount);
		String q = semossQuery.getCountQuery(maxCount);
		logger.info("Count query generated : " + q);
		Vector<String> countV = Utility.getVectorOfReturn(q, coreEngine);
		int totalSize = 0;
		if(countV.size()>0)
		{
			System.out.println(countV.get(0));
			try 
			{
				totalSize = Integer.parseInt(countV.get(0));
			}catch (NumberFormatException e){
				logger.error(e);
			}
		}
		semossQuery.setLimit(curLimit); // reset the limit to whatever it was before
		return totalSize;
	}
	
	private static ArrayList<Hashtable<String,String>> parsePropertiesFromPath(IEngine coreEngine, ArrayList<Hashtable<String,String>> nodeV, ArrayList<String> totalVarList)
	{
		/**
		 * What really is in that structure of total Var List
		 * 					predInfoHash.put("Subject", subjectURI);
					predInfoHash.put("SubjectVar", subjectName);
					predInfoHash.put("Pred", predURI);
					predInfoHash.put("Object", objectURI);
					predInfoHash.put("ObjectVar", objectName);
					predInfoHash.put(uriKey, predURI);
					predInfoHash.put(varKey, predName);

		 * 
		 * 
		 */
		ArrayList<Hashtable<String,String>> nodePropV = new ArrayList<Hashtable<String,String>>();
		ArrayList<Hashtable<String, String>> relationVarList = new ArrayList<Hashtable<String, String>>();
		relationVarList.addAll(nodeV);
		// run through all of the variables to get properties
		for (int i=0;i<relationVarList.size();i++)
		{
			Hashtable<String, String> vHash = relationVarList.get(i);
			String varName = vHash.get(varKey);
			String varURI = vHash.get(uriKey);
			String nodePropQuery = "SELECT DISTINCT ?entity WHERE {{?source <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+varURI+">} {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?source ?entity ?prop }}";
			Vector<String> propVector = Utility.getVectorOfReturn(nodePropQuery, coreEngine);
			for (int propIdx=0 ; propIdx<propVector.size(); propIdx++)
			{
				String propURI = propVector.get(propIdx);
				String propName = varName + "__" + Utility.getInstanceName(propURI).replace("-",  "_");
				totalVarList.add(propName);
				//store node prop info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put("SubjectVar", varName);
				elementHash.put(varKey, propName);
				elementHash.put(uriKey, propURI);
				nodePropV.add(elementHash);
			}
		}
		return nodePropV;
	}

	private static ArrayList<Hashtable<String,String>> parsePropertiesFromPathR(IEngine coreEngine, ArrayList<Hashtable<String,String>> nodeV, ArrayList<String> totalVarList)
	{
		/**
		 * What really is in that structure of total Var List
		 * 					predInfoHash.put("Subject", subjectURI);
					predInfoHash.put("SubjectVar", subjectName);
					predInfoHash.put("Pred", predURI);
					predInfoHash.put("Object", objectURI);
					predInfoHash.put("ObjectVar", objectName);
					predInfoHash.put(uriKey, predURI);
					predInfoHash.put(varKey, predName);

		 * 
		 * 
		 */
		ArrayList<Hashtable<String,String>> nodePropV = new ArrayList<Hashtable<String,String>>();
		ArrayList<Hashtable<String, String>> relationVarList = new ArrayList<Hashtable<String, String>>();
		relationVarList.addAll(nodeV);
		// run through all of the variables to get properties
		for (int i=0;i<relationVarList.size();i++)
		{
			Hashtable<String, String> vHash = relationVarList.get(i);
			String varName = vHash.get(varKey);
			String varURI = vHash.get(uriKey);
			
			// for each one of these I need to process the columns
			// eliminate those ones that have the same name as table names
			
			ArrayList<String> propVector = 	getColumnsFromTable(coreEngine, varName);
			propVector.remove(varName.toUpperCase());
			for (int propIdx=0 ; propIdx<propVector.size(); propIdx++)
			{
				String propURI = propVector.get(propIdx);
				String propName = Utility.toCamelCase(varName) + "__" + Utility.toCamelCase(propURI.replace("-",  "_"));
				totalVarList.add(propName);
				//store node prop info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put("SubjectVar", Utility.toCamelCase(varName));
				elementHash.put(varKey, propName);
				// do I need the URI key ?
				elementHash.put(uriKey, Utility.toCamelCase(propURI));
				nodePropV.add(elementHash);
			}
		}
		return nodePropV;
	}
	
	private static ArrayList<String> getColumnsFromTable(IEngine engine, String table)
	{
		String engineName = engine.getEngineName();
		String query = "";
		
		String dbTypeString = prop.getProperty(Constants.RDBMS_TYPE);
		if (dbTypeString != null) {
			dbType = (SQLQueryUtil.DB_TYPE.valueOf(dbTypeString));
		}
		
		SQLQueryUtil queryUtil = SQLQueryUtil.initialize(dbType);

		query = queryUtil.getDialectAllColumns(table);
		ISelectWrapper sWrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		ArrayList <String> columns = new ArrayList<String>();
		while(sWrapper.hasNext())
		{
			ISelectStatement stmt = sWrapper.next();
			String var = queryUtil.getAllColumnsResultColumnName();
			String colName = stmt.getVar(var)+"";
			colName = colName.toUpperCase();
			if(!colName.endsWith("_FK"))
				columns.add(colName);
		}
		return columns;
	}

	
	
	public static Hashtable<String, ArrayList> parsePath(Hashtable allJSONHash)
	{
		ArrayList<String> totalVarList = new ArrayList<String>();
		ArrayList<Hashtable<String,String>> nodeV = new ArrayList<Hashtable<String,String>>();
		ArrayList<Hashtable<String,String>> predV = new ArrayList<Hashtable<String,String>>();
		ArrayList<ArrayList<String>> tripleArray = (ArrayList<ArrayList<String>>) allJSONHash.get(relArrayKey);
		for (int tripleIdx = 0; tripleIdx<tripleArray.size(); tripleIdx++)
		{
			ArrayList<String> thisTripleArray = tripleArray.get(tripleIdx);
			String subjectURI = thisTripleArray.get(subIdx);
			String subjectName = Utility.getInstanceName(subjectURI);
			// store node/rel info
			if (!totalVarList.contains(subjectName))
			{
				//store node info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put(varKey, subjectName);
				elementHash.put(uriKey, subjectURI);
				totalVarList.add(subjectName);
				nodeV.add(elementHash);
			}
			// if a full path has been selected and not just a single node, go through predicate and object
			if(thisTripleArray.size()>1)
			{
				String predURI = thisTripleArray.get(predIdx);
				String objectURI = thisTripleArray.get(objIdx);
				String objectName = Utility.getInstanceName(objectURI);
				String predName = subjectName + "_" +Utility.getInstanceName(predURI) + "_" + objectName;
				if (!totalVarList.contains(predName))
				{
					Hashtable<String, String> predInfoHash = new Hashtable<String,String>();
					predInfoHash.put("Subject", subjectURI);
					predInfoHash.put("SubjectVar", subjectName);
					predInfoHash.put("Pred", predURI);
					predInfoHash.put("Object", objectURI);
					predInfoHash.put("ObjectVar", objectName);
					predInfoHash.put(uriKey, predURI);
					predInfoHash.put(varKey, predName);

					totalVarList.add(predName);
					predV.add(predInfoHash);
				}
				if (!totalVarList.contains(objectName))
				{
					totalVarList.add(objectName);
					//store node info
					Hashtable<String, String> elementHash = new Hashtable<String, String>();
					elementHash.put(varKey, objectName);
					elementHash.put(uriKey, objectURI);
					nodeV.add(elementHash);
				}
			}
		}
		Hashtable<String, ArrayList> parsedPath = new Hashtable<String, ArrayList>();
		parsedPath.put(totalVarListKey, totalVarList);
		parsedPath.put(nodeVKey, nodeV);
		parsedPath.put(predVKey, predV);
		return parsedPath;
	}

	// if only we could write some F** comments on what this does.. it could be super cool
	// so what we need here is
	// an array list of hashtables
	// the hashtable has the following keys
	// subjectVar
	// uriKey - the URI of the item
	// varKey - the var Key which is a stripped version of uriKey
	
	public static Hashtable<String, ArrayList<Hashtable<String,String>>> getPropsFromPath(IEngine engine, Hashtable allJSONHash) 
	{
		Hashtable<String, ArrayList<Hashtable<String,String>>> propsHash = new Hashtable<String, ArrayList<Hashtable<String,String>>>();
			Hashtable<String, ArrayList> parsedPath = parsePath(allJSONHash);
			// revisit the property logic later
			if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || engine.getEngineType() == IEngine.ENGINE_TYPE.JENA || engine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
			{			
				ArrayList<Hashtable<String,String>> nodePropV = parsePropertiesFromPath(engine, parsedPath.get(nodeVKey), parsedPath.get(totalVarListKey));				
				propsHash.put("nodes", nodePropV);
			}
			else if (engine.getEngineType() == IEngine.ENGINE_TYPE.RDBMS)
			{
				/**
				 * NodePropV looks like this
				 * [{SubjectVar=Title, uriKey=http://semoss.org/ontologies/Relation/Contains/MovieBudget, varKey=Title__MovieBudget}, 
				 * {SubjectVar=Title, uriKey=http://semoss.org/ontologies/Relation/Contains/Revenue-Domestic, varKey=Title__Revenue_Domestic}, 
				 * {SubjectVar=Title, uriKey=http://semoss.org/ontologies/Relation/Contains/Revenue-International, varKey=Title__Revenue_International}, {SubjectVar=Title, uriKey=http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Audience, varKey=Title__RottenTomatoes_Audience}, {SubjectVar=Title, uriKey=http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Critics, varKey=Title__RottenTomatoes_Critics}]
				 */
				ArrayList<Hashtable<String,String>> nodePropV = parsePropertiesFromPathR(engine, parsedPath.get(nodeVKey), parsedPath.get(totalVarListKey));				
				propsHash.put("nodes", nodePropV);
			}
		return propsHash;
	}
}
