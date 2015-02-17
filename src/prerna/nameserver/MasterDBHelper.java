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

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.util.DIHelper;
import prerna.util.Utility;

public final class MasterDBHelper implements IMasterDatabaseQueries, IMasterDatabaseURIs{

	private MasterDBHelper() {
		
	}
	
	/**
	 * Adds the list of enignes that are in the master db
	 * @param masterEngine		The engine to query
	 * @param engineList		The set to add the engine list to
	 */
	public static void fillEnglishList(IEngine masterEngine, Set<String> engineList) {
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, ENGINE_LIST_QUERY);
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
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, ENGINE_API_QUERY);
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
	 * @param keywordURI			The keywordURI to find related keywords
	 * @param keywordNounMap		The Map containing the key keyword and the value Set of nouns that comprise the keyword
	 * @param engineKeywordMap		The Map containing the engine keyword and the value Set of keywords contained in that engine
	 */
	public static void findRelatedKeywordsToSetStrings(IEngine masterEngine, Set<String> keywordSet, Map<String, Set<String>> keywordNounMap, Map<String, Set<String>> engineKeywordMap){
		// find all related keywords to the inputed data type
		String bindingsStr = "";
		Iterator<String> keywordsIt = keywordSet.iterator();
		while(keywordsIt.hasNext()) {
			bindingsStr = bindingsStr.concat("(<").concat(KEYWORD_BASE_URI).concat("/").concat(keywordsIt.next()).concat(">)");
		}
		
		String query = GET_RELATED_KEYWORDS_TO_SET_AND_THEIR_NOUNS.replace("@BINDINGS@", bindingsStr);
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, query);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String engine = sjss.getVar(names[0]).toString();
			String keyword = sjss.getRawVar(names[1]).toString();
			String noun = sjss.getRawVar(names[2]).toString();
			
			Set<String> nounList;
			if(keywordNounMap.containsKey(keyword)) {
				nounList = keywordNounMap.get(keyword);
				nounList.add(noun);
			} else {
				nounList = new HashSet<String>();
				nounList.add(noun);
				keywordNounMap.put(keyword, nounList);
			}
			
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
		System.err.println(">>>>>>>>>>>>>>>>>FOUND RELATED KEYWORDS TO SET: " + keywordSet);
		System.err.println(">>>>>>>>>>>>>>>>>LIST IS: " + keywordNounMap.keySet());
	}
	
	
	/**
	 * Based on the keywordURI, find related keywords and their respective nouns and engines
	 * @param masterEngine			The engine to query
	 * @param keywordURI			The keywordURI to find related keywords
	 * @param keywordNounMap		The Map containing the key keyword and the value Set of nouns that comprise the keyword
	 * @param engineKeywordMap		The Map containing the engine keyword and the value Set of keywords contained in that engine
	 */
	public static void findRelatedKeywordsToSpecificURI(IEngine masterEngine, String keywordURI, Map<String, Set<String>> keywordNounMap, Map<String, Set<String>> engineKeywordMap){
		// find all related keywords to the inputed data type
		String query = GET_RELATED_KEYWORDS_AND_THEIR_NOUNS.replace("@KEYWORD@", keywordURI);
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, query);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String engine = sjss.getVar(names[0]).toString();
			String keyword = sjss.getRawVar(names[1]).toString();
			String noun = sjss.getRawVar(names[2]).toString();
			
			Set<String> nounList;
			if(keywordNounMap.containsKey(keyword)) {
				nounList = keywordNounMap.get(keyword);
				nounList.add(noun);
			} else {
				nounList = new HashSet<String>();
				nounList.add(noun);
				keywordNounMap.put(keyword, nounList);
			}
			
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
		System.err.println(">>>>>>>>>>>>>>>>>FOUND RELATED KEYWORDS " + Utility.getInstanceName(keywordURI));
		System.err.println(">>>>>>>>>>>>>>>>>LIST IS: " + keywordNounMap.keySet());
	}
	
	public static Map<String, Set<String>> getMCValueMappingTree(IEngine masterEngine) {
		// fill the concept-concept tree 
		MasterDatabaseForest<String> forest = new MasterDatabaseForest<String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MC_PARENT_CHILD_QUERY);
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
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, KEYWORD_NOUN_QUERY);
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
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MC_PARENT_CHILD_QUERY);
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
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, KEYWORD_NOUN_QUERY);
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
