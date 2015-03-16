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
package prerna.nameserver;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.util.DIHelper;
import prerna.util.Utility;

public final class MasterDBHelper {

	private MasterDBHelper() {
		
	}
	
	public static void removeInsightStatementToMasterDBs(IEngine masterEngine, String selectedEngineName, String sub, String pred, String obj, Boolean concept) {
		masterEngine.removeStatement(sub, pred, obj, concept);
		if(pred.equals("PARAM:TYPE")) {
			if(concept) {
				String keyword = obj.substring(obj.lastIndexOf("/")+1);
				removeRelationship(masterEngine, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + keyword, obj, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + keyword + ":" + keyword);
				removeRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + selectedEngineName, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + keyword, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + selectedEngineName + ":" + keyword);
			}
		}
	}
	
	public static void addInsightStatementToMasterDBs(IEngine masterEngine, String selectedEngineName, String sub, String pred, String obj, Boolean concept) {
		masterEngine.addStatement(sub, pred, obj, concept);
		if(pred.equals("PARAM:TYPE")) {
			if(concept) {
				String keyword = obj.substring(obj.lastIndexOf("/")+1);
				addRelationship(masterEngine, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + keyword, obj, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + keyword + ":" + keyword);
				addRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + selectedEngineName, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + keyword, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + selectedEngineName + ":" + keyword);
			}
		}
	}
	
	/**
	 * Adds a node and the necessary triples given its instance URI
	 * @param nodeURI	String representing the URI for the node type. e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @throws EngineException
	 */
	public static void addNode(IEngine masterEngine, String nodeURI) {
		int index = nodeURI.lastIndexOf("/");
		String baseURI = nodeURI.substring(0,index);
		String instance = nodeURI.substring(index+1);
		masterEngine.addStatement(nodeURI, RDF.TYPE.stringValue(), baseURI, true);
		masterEngine.addStatement(baseURI, RDFS.SUBCLASSOF.stringValue(), MasterDatabaseURIs.SEMOSS_CONCEPT_URI, true);
		masterEngine.addStatement(nodeURI, RDFS.LABEL.stringValue(), instance, false);
	}

	/**
	 * Adds just the relationship given the URIs for the two nodes and the URI of the relation
	 * @param node1URI	String representing the full URI of node 1 URI e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @param node2URI	String representing the full URI of node 2 URI e.g. http://semoss.org/ontologies/Concept/Keyword/Dog
	 * @param relationURI	String representing the full URI of the relationship http://semoss.org/ontologies/Relation/Has/Dog:Dog
	 * @throws EngineException
	 */
	public static void addRelationship(IEngine masterEngine, String node1URI, String node2URI, String relationURI) {
		int relIndex = relationURI.lastIndexOf("/");
		String relBaseURI = relationURI.substring(0,relIndex);
		String relInst = relationURI.substring(relIndex+1);

		masterEngine.addStatement(relationURI, RDFS.SUBPROPERTYOF.stringValue(), relBaseURI,true);
		masterEngine.addStatement(relBaseURI, RDFS.SUBPROPERTYOF.stringValue(), MasterDatabaseURIs.SEMOSS_RELATION_URI,true);
		masterEngine.addStatement(relationURI, RDFS.LABEL.stringValue(), relInst,false);
		masterEngine.addStatement(node1URI, relationURI, node2URI,true);
	}


	/**
	 * Method to add property on an instance.
	 * @param nodeURI	String containing the node or relationship URI to add the property to e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @param propURI	String representing the URI of the property relation e.g. http://semoss.org/ontologies/Relation/Contains/Weight
	 * @param value	Value to add as the property e.g. 1.0
	 * @throws EngineException	Thrown if statement cannot be added to the engine
	 */
	public static void addProperty(IEngine masterEngine, String nodeURI, String propURI, Object value,Boolean isConcept) {
		masterEngine.addStatement(nodeURI, propURI, value, isConcept);
	}
	
	/**
	 * Removes a node and the necessary triples given its instance URI
	 * @param nodeURI	String representing the URI for the node type. e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @throws EngineException
	 */
	public static void removeNode(IEngine masterEngine, String nodeURI) {
		int index = nodeURI.lastIndexOf("/");
		String baseURI = nodeURI.substring(0,index);
		String instance = nodeURI.substring(index+1);
		masterEngine.removeStatement(nodeURI, RDF.TYPE.stringValue(), baseURI, true);
		masterEngine.removeStatement(baseURI, RDFS.SUBCLASSOF.stringValue(), MasterDatabaseURIs.SEMOSS_CONCEPT_URI, true);
		masterEngine.removeStatement(nodeURI, RDFS.LABEL.stringValue(), instance, false);
	}

	/**
	 * Removes just the relationship given the URIs for the two nodes and the URI of the relation
	 * @param node1URI	String representing the full URI of node 1 URI e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @param node2URI	String representing the full URI of node 2 URI e.g. http://semoss.org/ontologies/Concept/Keyword/Dog
	 * @param relationURI	String representing the full URI of the relationship http://semoss.org/ontologies/Relation/Has/Dog:Dog
	 * @throws EngineException
	 */
	public static void removeRelationship(IEngine masterEngine, String node1URI, String node2URI, String relationURI) {
		int relIndex = relationURI.lastIndexOf("/");
		String relBaseURI = relationURI.substring(0,relIndex);
		String relInst = relationURI.substring(relIndex+1);

		masterEngine.removeStatement(relationURI, RDFS.SUBPROPERTYOF.stringValue(), relBaseURI,true);
		masterEngine.removeStatement(relBaseURI, RDFS.SUBPROPERTYOF.stringValue(), MasterDatabaseURIs.SEMOSS_RELATION_URI,true);
		masterEngine.removeStatement(relationURI, RDFS.LABEL.stringValue(), relInst,false);
		masterEngine.removeStatement(node1URI, relationURI, node2URI,true);
	}


	/**
	 * Method to remove property on an instance.
	 * @param nodeURI	String containing the node or relationship URI to add the property to e.g. http://semoss.org/ontologies/Concept/MasterConcept/Dog
	 * @param propURI	String representing the URI of the property relation e.g. http://semoss.org/ontologies/Relation/Contains/Weight
	 * @param value	Value to remove as the property e.g. 1.0
	 * @throws EngineException	Thrown if statement cannot be added to the engine
	 */
	public static void removeProperty(IEngine masterEngine, String nodeURI, String propURI, Object value,Boolean isConcept) {
		masterEngine.removeStatement(nodeURI, propURI, value, isConcept);
	}
	
	/**
	 * Adds the list of enignes that are in the master db
	 * @param masterEngine		The engine to query
	 * @param engineList		The set to add the engine list to
	 */
	public static void fillEnglishList(IEngine masterEngine, Set<String> engineList) {
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.ENGINE_LIST_QUERY);
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext()) {
			ISelectStatement sjss = wrapper.next();
			engineList.add((String) sjss.getVar(names[0]));
		}
	}
	
	/**
	 * Add the engine URLs to a hashtable
	 * @param masterEngine		The engine to query
	 * @param engineURLHash		The hashtable to add all the engine URLs to
	 */
	public static void fillAPIHash(IEngine masterEngine, Hashtable<String, String> engineURLHash){
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.ENGINE_API_QUERY);
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			ISelectStatement sjss = wrapper.next();
			String engine = (String)sjss.getVar(names[0]);
			String baseURI = (sjss.getRawVar(names[1])).toString();
			engineURLHash.put(engine,baseURI);
		}
	}
	
	/**
	 * Based on the keywordURI, find related keywords and their respective nouns and engines
	 * @param masterEngine			The engine to query
	 * @param keywordSet			The Set of keywords to find related to
	 * @param relatedKeywordSet		The Set connected keywords to the keywordSet passed in
	 * @param engineKeywordMap		The Map containing the engine keyword and the value Set of keywords contained in that engine
	 */
	public static void findRelatedKeywordsToSetStrings(IEngine masterEngine, Set<String> keywordSet, Set<String> connectedKeywordSet, Map<String, Set<String>> engineKeywordMap){
		// find all related keywords to the inputed data type
		String bindingsStr = "";
		Iterator<String> keywordsIt = keywordSet.iterator();
		while(keywordsIt.hasNext()) {
			bindingsStr = bindingsStr.concat("(<").concat(MasterDatabaseURIs.KEYWORD_BASE_URI).concat("/").concat(keywordsIt.next()).concat(">)");
		}
		
		String query = MasterDatabaseQueries.GET_RELATED_KEYWORDS_TO_SET_AND_THEIR_NOUNS.replace("@BINDINGS@", bindingsStr);
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, query);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String engine = sjss.getVar(names[0]).toString();
			String keyword = sjss.getRawVar(names[1]).toString();
			
			connectedKeywordSet.add(keyword);
			
			Set<String> keywordForEngineList;
			if(engineKeywordMap.containsKey(engine)) {
				keywordForEngineList = engineKeywordMap.get(engine);
				keywordForEngineList.add(keyword);
			} else {
				keywordForEngineList = new HashSet<String>();
				keywordForEngineList.add(keyword);
				engineKeywordMap.put(engine, keywordForEngineList);
			}
		}
		
		//TODO: remove once error checking is done
//		System.err.println(">>>>>>>>>>>>>>>>>FOUND RELATED KEYWORDS TO SET: " + keywordSet);
//		System.err.println(">>>>>>>>>>>>>>>>>LIST IS: " + connectedKeywordSet);
	}
	
	
	/**
	 * Based on the keywordURI, find related keywords and their respective nouns and engines
	 * @param masterEngine			The engine to query
	 * @param keywordURI			The keywordURI to find related keywords
	 * @param keywordSet			The Set containing all the keywords connected to the keywordURI
	 * @param engineKeywordMap		The Map containing the engine keyword and the value Set of keywords contained in that engine
	 */
	public static void findRelatedKeywordsToSpecificURI(IEngine masterEngine, String keywordURI, Set<String> keywordSet, Map<String, Set<String>> engineKeywordMap){
		// find all related keywords to the inputed data type
		String query = MasterDatabaseQueries.GET_RELATED_KEYWORDS_AND_THEIR_NOUNS.replace("@KEYWORD@", keywordURI);
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, query);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String engine = sjss.getVar(names[0]).toString();
			String keyword = sjss.getRawVar(names[1]).toString();
			
			keywordSet.add(keyword);
			
			Set<String> keywordForEngineList;
			if(engineKeywordMap.containsKey(engine)) {
				keywordForEngineList = engineKeywordMap.get(engine);
				keywordForEngineList.add(keyword);
			} else {
				keywordForEngineList = new HashSet<String>();
				keywordForEngineList.add(keyword);
				engineKeywordMap.put(engine, keywordForEngineList);
			}
		}
		
		//TODO: remove once error checking is done
//		System.err.println(">>>>>>>>>>>>>>>>>FOUND RELATED KEYWORDS " + Utility.getInstanceName(keywordURI));
//		System.err.println(">>>>>>>>>>>>>>>>>LIST IS: " + keywordSet);
	}
	
	public static Map<String, Set<String>> getMCValueMappingTree(IEngine masterEngine) {
		// fill the concept-concept tree 
		MasterDatabaseForest<String> forest = new MasterDatabaseForest<String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			ISelectStatement sjss = wrapper.next();
			// add parent child relationships to value mapping
			TreeNode<String> parentNode = new TreeNode<String>(sjss.getVar(names[0]).toString());
			parentNode.addChild(sjss.getVar(names[1]).toString());
			forest.addNodes(parentNode);
		}
		return forest.getValueMapping();
	}
	
	public static Map<String, Set<String>> getMCToKeywordValueMapping(IEngine masterEngine) {
		// fill the keyword-concept graph
		MasterDatabaseBipartiteGraph<String> keywordConceptBipartiteGraph = new MasterDatabaseBipartiteGraph<String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.KEYWORD_NOUN_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			ISelectStatement sjss = wrapper.next();
			// add parent child relationships to value mapping
			BipartiteNode<String> node = new BipartiteNode<String>(sjss.getVar(names[0]).toString());
			node.addChild(sjss.getVar(names[1]).toString());
			keywordConceptBipartiteGraph.addToKeywordSet(node);
		}
		return keywordConceptBipartiteGraph.getMcMapping();
	}
	
	public static Map<String, Set<String>> getMCValueMappingTree(String masterEngineName) {
		BigDataEngine masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterEngineName);
		// fill the concept-concept tree 
		MasterDatabaseForest<String> forest = new MasterDatabaseForest<String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			ISelectStatement sjss = wrapper.next();
			// add parent child relationships to value mapping
			TreeNode<String> parentNode = new TreeNode<String>(sjss.getVar(names[0]).toString());
			parentNode.addChild(sjss.getVar(names[1]).toString());
			forest.addNodes(parentNode);
		}
		return forest.getValueMapping();
	}
	
	public static Map<String, Set<String>> getMCToKeywordValueMapping(String masterEngineName) {
		BigDataEngine masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterEngineName);
		// fill the keyword-concept graph
		MasterDatabaseBipartiteGraph<String> keywordConceptBipartiteGraph = new MasterDatabaseBipartiteGraph<String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.KEYWORD_NOUN_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			ISelectStatement sjss = wrapper.next();
			// add parent child relationships to value mapping
			BipartiteNode<String> node = new BipartiteNode<String>(sjss.getVar(names[0]).toString());
			node.addChild(sjss.getVar(names[1]).toString());
			keywordConceptBipartiteGraph.addToKeywordSet(node);
		}
		return keywordConceptBipartiteGraph.getMcMapping();
	}
}
