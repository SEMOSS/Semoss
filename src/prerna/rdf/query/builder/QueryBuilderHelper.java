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

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.query.util.SEMOSSQuery;
import prerna.util.Utility;

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
		Vector<String> countV = coreEngine.getEntityOfType(q);
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
			Vector<String> propVector = coreEngine.getEntityOfType(nodePropQuery);
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

	public static Hashtable<String, ArrayList<Hashtable<String,String>>> getPropsFromPath(IEngine engine, Hashtable allJSONHash) 
	{
		Hashtable<String, ArrayList> parsedPath = parsePath(allJSONHash);
		ArrayList<Hashtable<String,String>> nodePropV = parsePropertiesFromPath(engine, parsedPath.get(nodeVKey), parsedPath.get(totalVarListKey));	
		Hashtable<String, ArrayList<Hashtable<String,String>>> propsHash = new Hashtable<String, ArrayList<Hashtable<String,String>>>();
		propsHash.put("nodes", nodePropV);
		return propsHash;
	}
}
