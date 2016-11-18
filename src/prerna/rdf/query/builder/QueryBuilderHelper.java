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
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.internal.StringMap;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.util.SEMOSSQuery;
import prerna.util.Constants;
import prerna.util.DIHelper;
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
	public static final String subjectVar = "SubjectVar";
	public static final String relArrayKey = "relTriples";
	
	public static Integer runCountQuery(int maxCount, SEMOSSQuery semossQuery, IEngine coreEngine ){
		Integer curLimit = semossQuery.getLimit();
		semossQuery.setLimit(maxCount);
		String q = semossQuery.getCountQuery(maxCount);
		logger.info("Count query generated : " + q);
		Vector<String> countV = Utility.getVectorOfReturn(q, coreEngine, true);
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
	
	private static ArrayList<Hashtable<String,String>> parsePropertiesFromPath(IEngine coreEngine, List<Hashtable<String,String>> nodeV, List<String> totalVarList)
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

		Vector<String> propVector = new Vector();
		
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
			propVector = Utility.getVectorOfReturn(nodePropQuery, coreEngine, true);
			
			for (int propIdx=0 ; propIdx<propVector.size(); propIdx++)
			{
				String propURI = propVector.get(propIdx);
				
//				String propURIDisplayName = coreEngine.getTransformedNodeName(propURI,true); //flip back to display name if there is one
//				String physicalVarName = Utility.getInstanceName(coreEngine.getTransformedNodeName(Constants.CONCEPT_URI + varName, true));
				String propURIDisplayName = propURI; //flip back to display name if there is one
				String physicalVarName = varName;
				String propName = Utility.getInstanceName(propURIDisplayName);
				totalVarList.add(propName);
				//store node prop info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put("SubjectVar", physicalVarName);//display
				elementHash.put(varKey, propName);//display
				elementHash.put(uriKey, propURIDisplayName);//display
				nodePropV.add(elementHash);
			}
		}
		return nodePropV;
	}

	private static ArrayList<Hashtable<String,String>> parsePropertiesFromPathR(IEngine coreEngine, List<Hashtable<String,String>> nodeV, List<String> totalVarList)
	{
		/**
		 * What really is in that structure of total Var List
		 * 			predInfoHash.put("Subject", subjectURI);
					predInfoHash.put("SubjectVar", subjectName);
					predInfoHash.put("Pred", predURI);
					predInfoHash.put("Object", objectURI);
					predInfoHash.put("ObjectVar", objectName);
					predInfoHash.put(uriKey, predURI);
					predInfoHash.put(varKey, predName);
		 * 
		 */
		ArrayList<Hashtable<String,String>> nodePropV = new ArrayList<Hashtable<String,String>>();
		ArrayList<Hashtable<String, String>> relationVarList = new ArrayList<Hashtable<String, String>>();
		relationVarList.addAll(nodeV);
		// run through all of the variables to get properties
		for (int i=0;i<relationVarList.size();i++)
		{
			Hashtable<String, String> vHash = relationVarList.get(i);
			String varURI = vHash.get(uriKey);
			String varName = vHash.get(varKey);
			
			String derivedVarName = Utility.getInstanceName(varURI); //
			
			// for each one of these I need to process the columns
			// eliminate those ones that have the same name as table names
			
			ArrayList<String> propVector = 	getColumnsFromTable(coreEngine,derivedVarName);
			propVector.remove(derivedVarName);
			String uriPrefix = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI) + "/" + Constants.DEFAULT_PROPERTY_CLASS + "/";
			for (int propIdx=0 ; propIdx<propVector.size(); propIdx++)
			{
				String propURI = propVector.get(propIdx);
				//since propURI isnt a URI, we need to convert it to one so that we can 
				
//				String propURIDisplayURI = coreEngine.getTransformedNodeName(uriPrefix + propURI,true); //flip back to display name if there is one
//				String propURIDisplayName = Utility.getInstanceName(propURIDisplayURI);
				String propURIDisplayURI = uriPrefix + propURI; 
				String propURIDisplayName = Utility.getInstanceName(propURIDisplayURI);

				
				totalVarList.add(propURIDisplayName); //EYI TBD
				//store node prop info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put("SubjectVar", varName); //Utility.toCamelCase(varName))
				elementHash.put(varKey, propURIDisplayName);//was propName
				// do I need the URI key ?
				propURIDisplayURI = propURIDisplayURI.replace(Utility.getInstanceName(propURIDisplayURI), propURIDisplayName);
				elementHash.put(uriKey, propURIDisplayURI);//display
				nodePropV.add(elementHash);
			}
		}
		return nodePropV;
	}
	
	private static ArrayList<String> getColumnsFromTable(IEngine engine, String table)
	{
		String engineName = engine.getEngineName();
		String query = "";
		
		SQLQueryUtil queryUtil = null;
		
		SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;
		String dbTypeString = engine.getProperty(Constants.RDBMS_TYPE);
		if (dbTypeString != null) {
			dbType = (SQLQueryUtil.DB_TYPE.valueOf(dbTypeString));
			
			if(!engine.getProperty(Constants.USERNAME).isEmpty() && !engine.getProperty(Constants.PASSWORD).isEmpty()) {
				queryUtil = SQLQueryUtil.initialize(dbType, engine.getProperty(Constants.CONNECTION_URL), engine.getProperty(Constants.USERNAME), engine.getProperty(Constants.PASSWORD));
			} else {
				queryUtil = SQLQueryUtil.initialize(dbType);
			}
		}
		
		query = queryUtil.getDialectAllColumns(table);
		ISelectWrapper sWrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		sWrapper.execute();
		ArrayList <String> columns = new ArrayList<String>();
		while(sWrapper.hasNext())
		{
			ISelectStatement stmt = sWrapper.next();
			String var = queryUtil.getAllColumnsResultColumnName();
			String colName = stmt.getVar(var)+"";
			if(!colName.endsWith("_FK"))
				columns.add(colName);
		}
		return columns;
	}

	
	
//	public static Hashtable<String, List> parsePath(Hashtable allJSONHash, IEngine engine)
	public static Hashtable<String, List> parsePath(QueryBuilderData builderData, IEngine engine)
	{
		List<String> totalVarList = new ArrayList<String>();
		List<Hashtable<String,String>> nodeV = new ArrayList<Hashtable<String,String>>();
		List<Hashtable<String,String>> predV = new ArrayList<Hashtable<String,String>>();
		List<List<String>> tripleArray = builderData.getRelTriples();
		for (int tripleIdx = 0; tripleIdx<tripleArray.size(); tripleIdx++)
		{
			List<String> thisTripleArray = tripleArray.get(tripleIdx);
			String subjectURI = thisTripleArray.remove(subIdx);
//			String subjectName = Utility.getInstanceName(engine.getTransformedNodeName(subjectURI, true));
//			subjectURI = engine.getTransformedNodeName(subjectURI, false);
			String subjectName = Utility.getInstanceName(subjectURI);
			thisTripleArray.add(subIdx, subjectURI);
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
				String objectURI = thisTripleArray.remove(objIdx);
//				objectURI = engine.getTransformedNodeName(objectURI, false);
				thisTripleArray.add(objIdx, objectURI);
//				String objectName = Utility.getInstanceName(engine.getTransformedNodeName(objectURI, true));
				String objectName = Utility.getInstanceName(objectURI);
				String predName = subjectName + "_" +Utility.getInstanceName(predURI) + "_" + objectName;
				if (!totalVarList.contains(predName))
				{
					Hashtable<String, String> predInfoHash = new Hashtable<String,String>();
					predInfoHash.put("Subject", subjectURI);		//Physical URI
					predInfoHash.put("SubjectVar", subjectName);	//Logical Name
					predInfoHash.put("Pred", predURI);				//Physical Pred URI
					predInfoHash.put("Object", objectURI);			//Physical URI
					predInfoHash.put("ObjectVar", objectName);		//Logical Name
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
		Hashtable<String, List> parsedPath = new Hashtable<String, List>();
		parsedPath.put(totalVarListKey, totalVarList);
		parsedPath.put(nodeVKey, nodeV);
		parsedPath.put(predVKey, predV);

		List<Map<String, String>> props = builderData.getNodeProps();
		if(props!=null){
			for(Map<String, String> prop: props){
				String subjectURI = prop.get(uriKey);
//				subjectURI = engine.getTransformedNodeName(subjectURI, false);
				prop.put(uriKey, subjectURI);
				String equivURI = prop.get("equivalentURI");
//				equivURI = engine.getTransformedNodeName(equivURI, false);
				prop.put("equivalentURI", equivURI);
			}
		}
		
		return parsedPath;
	}

	// so what we need here is
	// an array list of hashtables
	// the hashtable has the following keys
	// subjectVar
	// uriKey - the URI of the item
	// varKey - the var Key which is a stripped version of uriKey
	
	public static Hashtable<String, List<Hashtable<String,String>>> getPropsFromPath(IEngine engine, QueryBuilderData builderData) 
	{
		Hashtable<String, List<Hashtable<String,String>>> propsHash = new Hashtable<String, List<Hashtable<String,String>>>();
//		cleanBuilderData(engine, builderData);
		Hashtable<String, List> parsedPath = parsePath(builderData, engine);
		
		// revisit the property logic later
		if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || engine.getEngineType() == IEngine.ENGINE_TYPE.JENA || engine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{			
			List<Hashtable<String,String>> nodePropV = parsePropertiesFromPath(engine, parsedPath.get(nodeVKey), parsedPath.get(totalVarListKey));				
			propsHash.put("nodes", nodePropV);
		}
		else if (engine.getEngineType() == IEngine.ENGINE_TYPE.RDBMS)
		{
			/**
			 * NodePropV looks like this
			 * [{SubjectVar=Title, uriKey=http://semoss.org/ontologies/Relation/Contains/MovieBudget, varKey=MovieBudget}, 
			 * {SubjectVar=Title, uriKey=http://semoss.org/ontologies/Relation/Contains/Revenue-Domestic, varKey=Revenue_Domestic}, 
			 * {SubjectVar=Title, uriKey=http://semoss.org/ontologies/Relation/Contains/Revenue-International, varKey=Title__Revenue_International}, {SubjectVar=Title, uriKey=http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Audience, varKey=Title__RottenTomatoes_Audience}, {SubjectVar=Title, uriKey=http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Critics, varKey=Title__RottenTomatoes_Critics}]
			 */
			
			List<Hashtable<String,String>> nodePropV = parsePropertiesFromPathR(engine, parsedPath.get(nodeVKey), parsedPath.get(totalVarListKey));				
			propsHash.put("nodes", nodePropV);
		}
		return propsHash;
	}

	public static void cleanBuilderData(IEngine engine, QueryBuilderData builderData){
		// process all SelectedNodeProps
		List<Map<String, String>> list = builderData.getNodeProps();
		if(list!=null){
			for(int i = 0 ; i < list.size(); i++){
				Map<String, String> map = list.get(i);
				String physicalPropUri = (String) map.get("uriKey");

				//split uri
				String subjectVar = (String) map.get("SubjectVar");
//				String physicalSubjectVar = Utility.getInstanceName(engine.getTransformedNodeName(Constants.DISPLAY_URI + subjectVar, false));
				String physicalSubjectVar = subjectVar;
				String logicalPropName = (String) map.get("varKey");
				String physicalVarKey = "";
				if(logicalPropName.contains("__")){
					String[] splitColAndTable = logicalPropName.split("__");
					logicalPropName = splitColAndTable[1]; 
					physicalVarKey = logicalPropName;
				}
				//translate subject var.... and mod it
				if(!subjectVar.equals(physicalSubjectVar)||!logicalPropName.equals(physicalVarKey)){
					map.put("SubjectVar",physicalSubjectVar);
					map.put("varKey", physicalSubjectVar+"__"+ physicalVarKey);
					list.set(i, map);
				}
			}
		}
		
		//first clone relTriples, we want to keep a copy of the rawRelTriples
		List<List<String>> tripleArray = builderData.getRelTriples();
		if(tripleArray != null && tripleArray.size() >0){
			for (int tripleIdx = 0; tripleIdx<tripleArray.size(); tripleIdx++){
				List<String> thisTripleArray = tripleArray.get(tripleIdx);
				
//				String physicalURI = engine.getTransformedNodeName(thisTripleArray.get(subIdx), false);
				String physicalURI = thisTripleArray.get(subIdx);
				thisTripleArray.set(subIdx,physicalURI);
				if(thisTripleArray.size()>1){
//					String objectURI = engine.getTransformedNodeName(thisTripleArray.get(objIdx),false);
					String objectURI = thisTripleArray.get(objIdx);
					thisTripleArray.set(objIdx,objectURI);
				}
				tripleArray.set(tripleIdx, thisTripleArray);
			}	
		}

		//filterKey
		Map<String, List<Object>> filterResults = builderData.getFilterData();
		StringMap<List<Object>> filterResultsNew = new StringMap<List<Object>>();
		Iterator <String> filterKeys = null;
		
		if(filterResults != null ){
			filterKeys = filterResults.keySet().iterator();
			for(int colIndex = 0;filterKeys.hasNext();colIndex++){
				String colValue = filterKeys.next();
				int numberOfValues = filterResults.get(colValue).size();
				if(filterResults.get(colValue).size() > 0 ){
					List <Object> filterValues = (List<Object>)filterResults.get(colValue);
					String instanceBaseUri = "";
					for(int filterIndex = 0;filterIndex < filterValues.size();filterIndex++)
					{
						String instanceFullDisplayPath = filterValues.get(filterIndex).toString();
						int slashIdx = instanceFullDisplayPath.lastIndexOf("/");
						if(slashIdx!=-1){
							instanceBaseUri = instanceFullDisplayPath.substring(0,slashIdx);
//							instanceBaseUri = engine.getTransformedNodeName(instanceBaseUri, false);
							String instance = instanceFullDisplayPath.substring(slashIdx+1);
							if(instanceBaseUri.length() > 0 && !instanceBaseUri.endsWith("/"))
								instanceBaseUri+="/";
							String instanceFullPath = instanceBaseUri + instance;
							if(!instanceBaseUri.equals(instanceFullDisplayPath.substring(0,slashIdx))){
								filterValues.set(filterIndex, instanceFullPath);
							}
						}
					}
//						colValue = Utility.getInstanceName(instanceBaseUri); //use instanceBaseUri since it should have been the same for all of the values you translated...
					filterResultsNew.put(colValue, filterValues);
				}
			}
		}
	}
}
