package prerna.nameserver;

import java.util.Map;
import java.util.Set;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;
import prerna.util.Utility;

public final class MasterDBHelper implements IMasterDatabase{

	private MasterDBHelper() {
		
	}
	
	public static Map<String, Set<String>> getMCValueMappingTree(IEngine masterEngine) {
		// fill the concept-concept tree 
		MasterDatabaseForest<String> forest = new MasterDatabaseForest<String>();
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine, MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
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
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine, KEYWORD_NOUN_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
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
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine, MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
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
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine, KEYWORD_NOUN_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
			// add parent child relationships to value mapping
			BipartiteNode<String> node = new BipartiteNode<String>(sjss.getVar(names[0]).toString());
			node.addChild(sjss.getVar(names[1]).toString());
			keywordConceptBipartiteGraph.addToKeywordSet(node);
		}
		return keywordConceptBipartiteGraph.getMcMapping();
	}
}
