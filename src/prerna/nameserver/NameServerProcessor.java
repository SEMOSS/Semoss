package prerna.nameserver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class NameServerProcessor extends AbstractNameServer {

	/**
	 * Constructor for the class, using master database as defined in hosting
	 */
	public NameServerProcessor() {
		super();
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

	public ConnectedConcepts searchConnectedConcepts(String concept) {
		//TODO: need to stop using the keyword as a parameter in master db
		/*
		 * Logic
		 * 1) find all keywords related to concept
		 * 2) for each keyword related, find upstream and downstream relationships (and store relationship name)
		 */
		ConnectedConcepts combineResults = new ConnectedConcepts();
		
		String originalKeywordURI = MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + AddToMasterDB.removeConceptUri(concept);
		Set<String> keywordSet = new HashSet<String>();
		Map<String, Set<String>> engineKeywordMap = new HashMap<String, Set<String>>();
		Map<String, String> engineURLHash = new HashMap<String, String>();
		if(masterEngine != null)
		{
			MasterDBHelper.findRelatedKeywordsToSpecificURI(masterEngine, originalKeywordURI, keywordSet, engineKeywordMap);
			MasterDBHelper.fillAPIHash(masterEngine, engineURLHash);
			for(String engineName : engineKeywordMap.keySet()) {
				IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
				String engineURL = MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName;
				Set<String> keywordList = engineKeywordMap.get(engineName);
				for(String keyword : keywordList) {
					// make sure the keyword connected is within similarity range
					double simScore = wnComp.compareKeywords(originalKeywordURI, keyword);
					if(!wnComp.isSimilar(simScore)) {
						continue;
					}
					// need to change this since it is returning 
					keyword = keyword.replace("/Keyword", "");
					Map<String, Map<String, Set<String>>> connections = MasterDBHelper.getRelationshipsForConcept(masterEngine, keyword, engineURL);
					
					if(!connections.isEmpty()) {
						//iterate over the connections map and translate/transform all nodes
						for(Map.Entry<String, Map<String, Set<String>>> eachMap: connections.entrySet()){
							Map<String, Set<String>> newMap = new HashMap<String, Set<String>>();
							for(String eachRelationship: eachMap.getValue().keySet()){
								Set<String> conceptsURI= eachMap.getValue().get(eachRelationship);
								Set<String> newConceptsURI = new HashSet<String>();
								for(String singleURI: conceptsURI){
									singleURI = engine.getTransformedNodeName(singleURI, true);
									newConceptsURI.add(singleURI);
								}
								newMap.put(eachRelationship, newConceptsURI);
							}
							if(newMap.size()>0){
								connections.put(eachMap.getKey(), newMap);
							}
						}
						
						String datakeyword = engine.getTransformedNodeName(keyword, true); //get the keywords display name
						combineResults.addData(engineName, datakeyword, connections);
						combineResults.addSimilarity(engineName, datakeyword, 1-simScore);
						if(engineURLHash.containsKey(engineName)) {
							combineResults.addAPI(engineName, engineURLHash.get(engineName));
						}
					}
				}
			}
		}
		
		return combineResults;
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
	
//	public boolean addUserInsightCount(String userId, Insight insight) {
//		AddToMasterDB masterDB = new AddToMasterDB(DIHelper.getInstance().getProperty(Constants.LOCAL_MASTER_DB_NAME));
//		return masterDB.processInsightExecutionForUser(userId, insight);
//	}
	
	public HashMap<String, Object> getTopInsights(String engine, String limit) {
		SearchMasterDB masterDB = new SearchMasterDB(Constants.LOCAL_MASTER_DB_NAME);
		return masterDB.getTopInsights(engine, limit);
	}
	
//	public boolean publishInsightToFeed(String userId, Insight insight, String visibility) {
//		AddToMasterDB masterDB = new AddToMasterDB(Constants.LOCAL_MASTER_DB_NAME);
//		return masterDB.publishInsightToFeed(userId, insight, visibility);
//	}
	
	public HashMap<String, Object> getFeedInsights(String userId, String visibility, String limit) {
		SearchMasterDB masterDB = new SearchMasterDB(Constants.LOCAL_MASTER_DB_NAME);
		return masterDB.getFeedInsights(userId, visibility, limit);
	}
	
	public HashMap<String, Object> getAllInsights(String groupBy, String orderBy) {
		SearchMasterDB masterDB = new SearchMasterDB(Constants.LOCAL_MASTER_DB_NAME);
		return masterDB.getAllInsights(groupBy, orderBy);
	}
	
	public HashMap<String, Object> getInsightDetails(String insight, String user) {
		SearchMasterDB masterDB = new SearchMasterDB(Constants.LOCAL_MASTER_DB_NAME);
		return masterDB.getInsightDetails(insight, user);
	}
}
