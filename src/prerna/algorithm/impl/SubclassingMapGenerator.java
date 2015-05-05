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
package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import edu.uci.ics.jung.graph.DelegateForest;

public class SubclassingMapGenerator {

	public SubclassingMapGenerator() {

	}

	private Set<String> parentsThatAreChildren;
	private Map<String, ArrayList<String>> subclassList;

	private Map<String, Map> subclassingMap;
	private Hashtable<String, SEMOSSVertex> vertStore;
	private Hashtable<String, SEMOSSEdge> edgeStore;
	
	private Hashtable<String, SEMOSSEdge> newEdgesAdded;
	
	public Map<String, ArrayList<String>> getSubclassList() {
		return subclassList;
	}

	public Hashtable<String, SEMOSSVertex> getVertStore() {
		return vertStore;
	}

	public Hashtable<String, SEMOSSEdge> getEdgeStore() {
		return edgeStore;
	}
	
	//engine is the baseDataEngine (OWL file)
	public Map<String, Map> processSubclassing(IEngine engine) {
		final String GET_SUBCLASSED_CONCEPTS = "SELECT DISTINCT ?parent ?child WHERE { FILTER(?parent != <http://semoss.org/ontologies/Concept>) FILTER(?child != <http://semoss.org/ontologies/Concept>) FILTER(?parent != ?child) {?parent <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?child <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?child <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?parent} }";		
		
		ISelectWrapper sjsw = Utility.processQuery(engine, GET_SUBCLASSED_CONCEPTS);
		String[] names = sjsw.getVariables();

		subclassList = new HashMap<String, ArrayList<String>>();
		Set<String> parentList = new HashSet<String>();
		Set<String> childList = new HashSet<String>();

		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String parent = sjss.getRawVar(names[0]).toString();
			String child = sjss.getRawVar(names[1]).toString();

			ArrayList<String> children;
			if(subclassList.containsKey(parent)) {
				children = subclassList.get(parent);
				children.add(child);
			} else {
				children = new ArrayList<String>();
				children.add(child);
				subclassList.put(parent, children);
			}

			parentList.add(parent);
			childList.add(child);
		}

		subclassingMap = new HashMap<String, Map>();

		if(subclassList.isEmpty()) {
			return subclassingMap;
		}

		parentsThatAreChildren = new HashSet<String>();
		for(String parent : parentList) {
			if(!childList.contains(parent)) {

				Map<String, Map> innerMap = new HashMap<String, Map>();
				for(String child: subclassList.get(parent)) {
					innerMap.put(child, new HashMap<String, Map>());
				}
				subclassingMap.put(parent, innerMap);
			} else {
				parentsThatAreChildren.add(parent);
			}
		}

		if(parentsThatAreChildren.isEmpty()) {
			return subclassingMap;
		}

		for(String key : subclassingMap.keySet()) {
			recursivelyBuildNDegreeSubclassing(subclassingMap.get(key));
		}
		return subclassingMap;
	}

	private void recursivelyBuildNDegreeSubclassing(Map<String, Map> rootMap) {
		for(Object parent : rootMap.keySet()) {
			if(parentsThatAreChildren.isEmpty()) {
				return;
			}
			//loop through until you find the position of the parent
			if(parentsThatAreChildren.contains(parent.toString())) {
				parentsThatAreChildren.remove(parent.toString());
				Map<String, Map> parentMap = rootMap.get(parent.toString());
				for(String child : subclassList.get(parent.toString())) {
					parentMap.put(child, new HashMap<String, Map>());
					if(parentsThatAreChildren.contains(child)) {
						recursivelyBuildNDegreeSubclassing(parentMap);
					}
				}
			}
		}
	}

	public void updateVertAndEdgeStoreForSubclassing(Hashtable<String, SEMOSSVertex> vertStore, Hashtable<String, SEMOSSEdge> edgeStore) {
		this.vertStore = vertStore;
		this.edgeStore = edgeStore;
		newEdgesAdded =  new Hashtable<String, SEMOSSEdge>();
		
		for(String rootParent : subclassingMap.keySet()) {
			Map<String, Map> subclasses = subclassingMap.get(rootParent);
			recursivelyUpdateVertAndEdgeStoreForSubclassing(subclasses, rootParent, vertStore, edgeStore);
		}
	}

	private void recursivelyUpdateVertAndEdgeStoreForSubclassing(Map<String, Map> subclasses, String rootParent, Hashtable<String, SEMOSSVertex> vertStore, Hashtable<String, SEMOSSEdge> edgeStore) {

		String semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		String relation = Constants.DEFAULT_RELATION_CLASS;

		String relURI = semossURI.concat("/").concat(relation).concat("/");

		for(String child : subclasses.keySet()) {
			SEMOSSVertex parentVertex = vertStore.get(rootParent);
			SEMOSSVertex childVertex = vertStore.get(child);
			String childName = childVertex.getProperty(Constants.VERTEX_NAME).toString();

			Vector<SEMOSSEdge> inParentEdges = parentVertex.getInEdges();
			for(SEMOSSEdge edge : inParentEdges) {
				//need to create new edge between child and target
				String nodeURI = edge.outVertex.propHash.get(Constants.URI).toString();
				String nodeName = edge.outVertex.propHash.get(Constants.VERTEX_NAME).toString();
				SEMOSSVertex nodeVertex = vertStore.get(nodeURI);
				
				String newEdgeURI = relURI.concat(nodeName).concat(":").concat(childName);
				SEMOSSEdge newEdge = new SEMOSSEdge(nodeVertex, childVertex, newEdgeURI);

				childVertex.getInEdges().add(newEdge);
				nodeVertex.getOutEdges().add(newEdge);
				edgeStore.put(newEdgeURI, newEdge);
				newEdgesAdded.put(newEdgeURI, newEdge);
			}
			Vector<SEMOSSEdge> outParentEdges = parentVertex.getOutEdges();
			for(SEMOSSEdge edge : outParentEdges) {
				//need to create new edge between child and target
				String nodeURI = edge.inVertex.propHash.get(Constants.URI).toString();
				String nodeName = edge.inVertex.propHash.get(Constants.VERTEX_NAME).toString();
				SEMOSSVertex nodeVertex = vertStore.get(nodeURI);
				
				String newEdgeURI = relURI.concat(childName).concat(":").concat(nodeName);
				SEMOSSEdge newEdge = new SEMOSSEdge(childVertex, nodeVertex, newEdgeURI);

				childVertex.getOutEdges().add(newEdge);
				nodeVertex.getInEdges().add(newEdge);
				edgeStore.put(newEdgeURI, newEdge);
				newEdgesAdded.put(newEdgeURI, newEdge);
			}

			Map<String, Map> childSubclasses = subclasses.get(child);
			if(!childSubclasses.isEmpty()) {
				//recursively add all child's edges
				recursivelyUpdateVertAndEdgeStoreForSubclassing(subclasses, child, vertStore, edgeStore);
			}
		}
	}

	public DelegateForest<SEMOSSVertex,SEMOSSEdge> updateForest(DelegateForest<SEMOSSVertex,SEMOSSEdge> forest) {
		for(String edgeKey : newEdgesAdded.keySet()) {
			SEMOSSEdge newEdge = newEdgesAdded.get(edgeKey);
			forest.addEdge(newEdge, newEdge.outVertex, newEdge.inVertex);			
		}
		
		return forest;
	}
	
	public HashMap<String, Integer> calculateEdgeCounts(RDFFileSesameEngine baseDataEngine) {
		final String GET_CONCEPT_EDGE_COUNT_QUERY = "SELECT DISTINCT ?entity ?direction ?node WHERE { { SELECT DISTINCT ?entity ?direction ?node WHERE { BIND('in' AS ?direction) FILTER(?inRel != <http://semoss.org/ontologies/Relation>) {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?inRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?node ?inRel ?entity} } } UNION { SELECT DISTINCT ?entity ?direction ?node WHERE { BIND('out' AS ?direction) FILTER(?outRel != <http://semoss.org/ontologies/Relation>) {?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?outRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?entity ?outRel ?node} } } }";

		HashMap<String, HashMap<String, Set<String>>> conceptEdgeHash = new HashMap<String, HashMap<String, Set<String>>>();
		ISelectWrapper sjsw = Utility.processQuery(baseDataEngine, GET_CONCEPT_EDGE_COUNT_QUERY);
		String[] names = sjsw.getVariables();
		String param1 = names[0];
		String param2 = names[1];
		String param3 = names[2];
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String concept = sjss.getRawVar(param1).toString();
			String direction = sjss.getVar(param2).toString();
			String relationshipNode = sjss.getRawVar(param3).toString();
			HashMap<String, Set<String>> innerHash;
			if(conceptEdgeHash.containsKey(concept)) {
				innerHash = conceptEdgeHash.get(concept);
				if(innerHash.containsKey(direction)) {
					innerHash.get(direction).add(relationshipNode);
				} else {
					HashSet<String> relationshipNodeSet = new HashSet<String>();
					relationshipNodeSet.add(relationshipNode);
					innerHash.put(direction, relationshipNodeSet);
				}
			} else {
				HashSet<String> relationshipNodeSet = new HashSet<String>();
				relationshipNodeSet.add(relationshipNode);
				innerHash = new HashMap<String, Set<String>>();
				innerHash.put(direction, relationshipNodeSet);
				conceptEdgeHash.put(concept, innerHash);
			}
		}

		HashMap<String, Integer> retHash = new HashMap<String, Integer>();
		//first add all edge counts
		for(String concept : conceptEdgeHash.keySet()) {
			HashMap<String, Set<String>> innerHash = conceptEdgeHash.get(concept);
			int numConnections = 0;
			for(String key : innerHash.keySet()) {
				numConnections += innerHash.get(key).size();
			}
			retHash.put(concept, numConnections);
		}

		//now fix the edge counts for those that are in the map
		for(String rootParent : subclassingMap.keySet()) {
			Map subclasses = subclassingMap.get(rootParent);
			recursivelyGetEdgeCounts(subclasses, conceptEdgeHash, retHash, rootParent);
		}

		return retHash;
	}

	private void recursivelyGetEdgeCounts(Map<String, Map> rootMap, HashMap<String, HashMap<String, Set<String>>> conceptEdgeHash, HashMap<String, Integer> edgeCountsForConcepts, String parent) {
		for(String child : rootMap.keySet()) {
			HashMap<String, Set<String>> parentInnerHash = conceptEdgeHash.get(parent);
			HashMap<String, Set<String>> childInnerHash = conceptEdgeHash.get(child);
			//to avoid null point errors, just set it to an empty map
			if(childInnerHash == null) {
				childInnerHash = new HashMap<String, Set<String>>();
			}

			int numUnqiueEdges = 0;
			Set<String> allInnerHashKeySet = new HashSet<String>();
			allInnerHashKeySet.addAll(parentInnerHash.keySet());
			allInnerHashKeySet.addAll(childInnerHash.keySet());

			for(String innerKey : allInnerHashKeySet) {
				if(parentInnerHash.containsKey(innerKey)) {
					if(childInnerHash.containsKey(innerKey)) {
						// compare to get unique edges and update child's list so future recursion takes it into consideration
						Set<String> childList = childInnerHash.get(innerKey);
						childList.addAll(parentInnerHash.get(innerKey));
						numUnqiueEdges += childList.size();
					} else {
						// compare to get unique edges and update child's list so future recursion takes it into consideration
						numUnqiueEdges += parentInnerHash.get(innerKey).size();
						childInnerHash.put(innerKey, parentInnerHash.get(innerKey));
					}
				} else {
					numUnqiueEdges += childInnerHash.get(innerKey).size();
				}
			}

			edgeCountsForConcepts.put(child, numUnqiueEdges);

			Map<String, Map> childSubclasses = rootMap.get(child);
			if(!childSubclasses.isEmpty()) {
				//recursively calculate all child's edge counts
				recursivelyGetEdgeCounts(childSubclasses, conceptEdgeHash, edgeCountsForConcepts, child);
			}
		}
	}

}
