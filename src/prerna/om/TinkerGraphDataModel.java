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
package prerna.om;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.URI;

import prerna.ds.TinkerFrame;
import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Utility;

public class TinkerGraphDataModel {

	private static final Logger logger = LogManager.getLogger(TinkerGraphDataModel.class.getName());

	public void fillModel(String query, IDatabaseEngine engine, TinkerFrame tf){
		long start = System.currentTimeMillis();
		processData(query, engine, tf);
		long end = System.currentTimeMillis();
		
		logger.info(">>>>>> TOTAL TIME: " + (end-start) + " ms");
	}

	private void processData(String query, IDatabaseEngine engine, TinkerFrame tf) {
		// load the base filter hash
		// this will be used to ignore the triples
		// that are purely metdata oriented from the tinkerframe
		Hashtable<String, String> baseFilterHash = ((AbstractDatabaseEngine)engine).getBaseHash();

		String queryCap = query.toUpperCase().trim();
//		// this is just to remove the limit for some of the queries i have seen
//		// that i was using for testing purposes
//		if(queryCap.endsWith("LIMIT 1000")) {
//			queryCap = queryCap.replace("LIMIT 1000", "");
//		}
		logger.info("query executed is: " + queryCap);

		IConstructWrapper sjw = null;
		// it its a construct query, get a construct wrapper
		if(queryCap.startsWith("CONSTRUCT")) {
			sjw = WrapperManager.getInstance().getCWrapper(engine, query);
		} else {
			// this is actually a select query that we are discusing as a construct 
			sjw = WrapperManager.getInstance().getChWrapper(engine, query);
		}
		
		// the cardinality is used to define how the relationship is going to be added
		// since the wrapper always returns subject -> predicate -> object,
		// where we are always adding subject as the fromVertex and object as the toVertex
		// the cardinality is always set to be the same
		// so create it here and pass it in every relationship addition
		Map<Integer, Set<Integer>> cardinality = new HashMap<Integer, Set<Integer>>();
		Set<Integer> cardinalitySet = new HashSet<Integer>();
		cardinalitySet.add(1);
		cardinality.put(0, cardinalitySet);

		logger.info("Wrapper created, time to start iterating...");
		while(sjw.hasNext())
		{
			// grab the next response
			IConstructStatement sct = sjw.next();
			String predicateName = sct.getPredicate();//this.getDisplayName(sct.getPredicate());
			String subjectName = sct.getSubject();//this.getDisplayName(sct.getSubject());
			String objectName = sct.getObject()+"";//this.getDisplayName(sct.getObject()+"");

			// subjectName is always a URI so it will be our from vertex
			String vert1 = subjectName; 
			// we define the to vertex based on if it is a URI or a property
			String vert2 = "";
			
			// we need to ignore the metadata triples that get passed from the construct query
			if(!baseFilterHash.containsKey(subjectName) && !baseFilterHash.containsKey(predicateName) && !baseFilterHash.containsKey(objectName))
			{
				// need to account when we have a return that is just the node itself
				if(subjectName.equals(predicateName) && subjectName.equals(objectName)) {
					storeVertex(subjectName, tf);
				} else {
					// if we have a URI as the object
					// we just grab it and that is the to vertex
					if(sct.getObject() instanceof URI) 
					{
						vert2 = objectName;
					}
					else 
					{
						// if it is not a URI, we have a literal
						// given the way we store the triple
						// { <http://semoss.org/ontologies/Concept/Title/Avatar> <http://semoss.org/ontologies/Relation/Contains/MovieBudget> "1000"}
						// we construct the vertex to be the predicate + "/" + the literal value
						vert2 = predicateName + "/" + objectName;
					}
					
					// when we store edge
					// if the vertex does not yet exist
					// it will be added
					storeRelationship(vert1, vert2, tf, cardinality);
				}
			} 
		}
	}	
	
	private void storeVertex(String vert, TinkerFrame tf){
//		logger.info("storing vertex "  + vert);
		String type = Utility.getClassName(vert);
		
		Map<String, Object> clean = new HashMap<String, Object>();
		clean.put(type, Utility.getInstanceName(vert));

		// need to do the whole edge hash thing so we have headers
		Map<String, Set<String>> edgeHash = new Hashtable<String, Set<String>>();
		edgeHash.put(type, new HashSet<String>());
		Map<String, String> dataTypeMap = new Hashtable<String, String>();
		dataTypeMap.put(type, "STRING");
		//TODO: come back to this
		//TODO: come back to this
		//TODO: come back to this
		//TODO: come back to this
		//TODO: come back to this
//		tf.mergeEdgeHash(edgeHash, dataTypeMap);
		
		// need to pass in a map
		// this would be where we would take advantage of using display names
		Map<String, String> logicalToTypeMap = new HashMap<String, String>();
		logicalToTypeMap.put(type, type);
		
		// add relationship has a check to see there is no relationship
		// so it just adds a node
		// add an empty edge hash so it get that faster
		tf.addRelationship(clean, new Hashtable<String, Set<String>>(), logicalToTypeMap);
	}
	

	private void storeRelationship(String outVert, String inVert, TinkerFrame tf, Map<Integer, Set<Integer>> cardinality){
//		logger.info("storing edge "  + outVert + " and in " + inVert);
		String typeOut = Utility.getClassName(outVert);
		String typeIn = Utility.getClassName(inVert);
		
		// since we are always adding a subject -> object from a consturct wrapper
		// there is no way to know if the connection has been made on the meta level
		// so we need to add it here every time
		tf.getMetaData().addVertex(typeOut);
		tf.getMetaData().addVertex(typeIn);
		tf.getMetaData().addRelationship(typeOut, typeIn, "inner.join");

		String[] headers = {typeOut, typeIn};
		String[] cleanValues = {Utility.getInstanceName(outVert), Utility.getInstanceName(inVert)};

		// need to pass in a map
		// this would be where we would take advantage of using display names
		Map<String, String> logicalToTypeMap = new HashMap<String, String>();
		logicalToTypeMap.put(typeOut, typeOut);
		logicalToTypeMap.put(typeIn, typeIn);

		// add the relationship usign the cardinality
		// this allows us to have self-loops in the graph
		tf.addRelationship(headers, cleanValues, cardinality, logicalToTypeMap);
	}
	
	// this would be used if we are sending display names
//	private String getDisplayName(IDatabase coreEngine, String subKey){
//		return Utility.getTransformedNodeName(coreEngine, subKey, true);
//	}
	
}
