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

import java.util.Map;
import java.util.Set;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.util.DIHelper;
import prerna.util.Utility;

public final class MasterDBHelper implements IMasterDatabaseQueries{

	private MasterDBHelper() {
		
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
