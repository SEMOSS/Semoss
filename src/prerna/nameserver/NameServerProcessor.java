package prerna.nameserver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IEngine;
import prerna.util.Utility;

public class NameServerProcessor extends AbstractNameServer {

	/**
	 * Constructor for the class, using master database as defined in hosting
	 */
	public NameServerProcessor() {
		super();
	}
	
	/**
	 * Constructor for the class, using defined master database
	 * Defines the wordnet library
	 * Defines the stanford nlp library
	 */
	public NameServerProcessor(String wordNetDir, String lpDir) {
		super(wordNetDir, lpDir);
	}
	
	/**
	 * Constructor for the class, using input database as master database
	 * Sometimes we do not need to use wordnet or stanford nlp library so avoid long loading time
	 * Modifies the name to match the input name for the master database
	 * @param masterEngine
	 */
	public NameServerProcessor(IEngine masterEngine) {
		super(masterEngine);
	}
	
	/**
	 * Constructor for the class, using input database as master database
	 * Defines the engine for the name server
	 * Defines the wordnet library
	 * Defines the stanford nlp library
	 */
	public NameServerProcessor(IEngine masterEngine, String wordNetDir, String lpDir) {
		super(masterEngine, wordNetDir, lpDir);
	}
	
	@Override
	public boolean indexQuestion(String questionURL, List<String> paramURLs, List<String> tags, Map<String, Object> filterOptions) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean unIndexQuestion(String quesitonURL) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean indexEngine(String engineName, IEngine owlEngine) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean unIndexEngine(String engineName) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<INameServerTag> getTags(String questionURL) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean addTags(String questionURL, List<INameServerTag> tags) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteTags(String questionURL, List<INameServerTag> tags) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<String> searchTags(List<INameServerTag> tags) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Map<String, Set<String>>> searchConnectedConcepts(String concept) {
		//TODO: need to stop using the keyword as a parameter in master db
		/*
		 * Logic
		 * 1) Find list of engines and tags related to input concept
		 * 2) For each engine, find all upstream and downstream relationships for the keywords
		 * 3) Done 
		 */
		
		Map<String, Map<String, Set<String>>> retHash = new Hashtable<String, Map<String, Set<String>>>();
		
		String originalKeywordURI = MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + Utility.getInstanceName(concept);
		Set<String> keywordSet = new HashSet<String>();
		Map<String, Set<String>> engineKeywordMap = new HashMap<String, Set<String>>();
		if(masterEngine != null)
		{
			MasterDBHelper.findRelatedKeywordsToSpecificURI(masterEngine, originalKeywordURI, keywordSet, engineKeywordMap);
			for(String engine : engineKeywordMap.keySet()) {
				String engineURL = MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engine;
				Set<String> keywordList = engineKeywordMap.get(engine);
				for(String keyword : keywordList) {
					// make sure the keyword connected is within similarity range
					if(!wnComp.isSimilar(originalKeywordURI, keyword)) {
						continue;
					}
					// need to change this since it is returning 
					keyword = keyword.replace("/Keyword", "");
					Map<String, Set<String>> connections = MasterDBHelper.getRelationshipsForConcept(masterEngine, keyword, engineURL);
					
					// if engine is not there, just add to retHash
					if(!retHash.containsKey(engine)) {
						if(!connections.isEmpty()) {
							retHash.put(engine, connections);
						}
					} else {
						Map<String, Set<String>> innerHash = retHash.get(engine);
						// check if previous upstream/downstream values have been found
						for(String key : connections.keySet()) {
							if(innerHash.containsKey(key)) {
								// if found, add to existing set of possible connections
								innerHash.get(key).addAll(connections.get(key));
							} else {
								// if not found, just add upstream/downstream and values
								innerHash.put(key, connections.get(key));
							}
						}
					}
				}
			}
		}
		
		return retHash;
	}

	@Override
	public Map<String, Map<String, Set<String>>> searchDirectlyConnectedConcepts(String concept) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> searchData(List<String> instanceURIs) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> searchQuestion(String searchString) {
		// TODO Auto-generated method stub
		return null;
	}

}
