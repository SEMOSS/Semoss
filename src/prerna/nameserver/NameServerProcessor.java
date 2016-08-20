package prerna.nameserver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import prerna.engine.api.IEngine;
import prerna.util.DIHelper;
import prerna.util.Utility;

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
					
					Map<String, Object> cleanedConnections = new TreeMap<String, Object>();
					if(!connections.isEmpty()) {
						//iterate over the connections map and translate/transform all nodes
						for(Map.Entry<String, Map<String, Set<String>>> eachMap: connections.entrySet()){
							Map<String, Map<String, Map<String, String>>> newMap = new TreeMap<String, Map<String, Map<String, String>>>();
							for(String eachRelationship: eachMap.getValue().keySet()){
								Set<String> conceptsURI= eachMap.getValue().get(eachRelationship);
								Map<String, Map<String, String>> newConceptsURI = new TreeMap<String, Map<String, String>>();
								for(String singleURI: conceptsURI){
									String logicalURI = engine.getTransformedNodeName(singleURI, true);
//									String node = singleURI.replaceAll(".*/Concept/", "");
									String node = Utility.getInstanceName(logicalURI);
									String parent = null;
									if(node.contains("/")) {
										// this is for properties that are also concepts
										String propName = node.substring(0, node.lastIndexOf("/"));
										String conceptName = node.substring(node.lastIndexOf("/") + 1, node.length());
										if(!propName.equals(conceptName)) {
											node = propName;
											parent = conceptName;
										} else {
											node = conceptName;
										}
									}
									Map<String, String> nodeMap = new Hashtable<String, String>();
									nodeMap.put("physicalName", node);
									if(parent != null) {
										nodeMap.put("parent", parent);
									}
									
									newConceptsURI.put(logicalURI, nodeMap);
								}
								newMap.put(eachRelationship, newConceptsURI);
							}
							if(newMap.size()>0){
								cleanedConnections.put(eachMap.getKey(), newMap);
							}
						}
						
						String datakeyword = engine.getTransformedNodeName(keyword, true); //get the keywords display name
//						String node = keyword.replaceAll(".*/Concept/", "");
						String node = Utility.getInstanceName(keyword);
						String parent = null;
						if(node.contains("/")) {
							// this is for properties that are also concepts
							String propName = node.substring(0, node.lastIndexOf("/"));
							String conceptName = node.substring(node.lastIndexOf("/") + 1, node.length());
							if(!propName.equals(conceptName)) {
								node = propName;
								parent = conceptName;
							} else {
								node = conceptName;
							}
						}
						combineResults.addData(engineName, datakeyword, node, parent, cleanedConnections);
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
//	
//	public HashMap<String, Object> getTopInsights(String engine, String limit) {
//		SearchMasterDB masterDB = new SearchMasterDB(Constants.LOCAL_MASTER_DB_NAME);
//		return masterDB.getTopInsights(engine, limit);
//	}
//	
//	public boolean publishInsightToFeed(String userId, Insight insight, String visibility) {
//		AddToMasterDB masterDB = new AddToMasterDB(Constants.LOCAL_MASTER_DB_NAME);
//		return masterDB.publishInsightToFeed(userId, insight, visibility);
//	}
//	
//	public HashMap<String, Object> getFeedInsights(String userId, String visibility, String limit) {
//		SearchMasterDB masterDB = new SearchMasterDB(Constants.LOCAL_MASTER_DB_NAME);
//		return masterDB.getFeedInsights(userId, visibility, limit);
//	}
//	
//	public HashMap<String, Object> getAllInsights(String groupBy, String orderBy) {
//		SearchMasterDB masterDB = new SearchMasterDB(Constants.LOCAL_MASTER_DB_NAME);
//		return masterDB.getAllInsights(groupBy, orderBy);
//	}
//	
//	public HashMap<String, Object> getInsightDetails(String insight, String user) {
//		SearchMasterDB masterDB = new SearchMasterDB(Constants.LOCAL_MASTER_DB_NAME);
//		return masterDB.getInsightDetails(insight, user);
//	}

	@Override
	public String findMostSimilarKeyword(String word) {
		Set<String> keywordURIs = MasterDBHelper.getExistingKeywords(masterEngine);
		/*
		for(String uri : keywordURIs) {
			String instance = uri.replaceAll(".Keyword/", "");
			if(instance.contains("/")) {
				// this is for properties that are also concepts
				instance = instance.substring(0, instance.lastIndexOf("/"));
			}
			if(instance.equalsIgnoreCase(word)) {
				return uri;
			}
		}*/
		
		// no match, use wordnet comparison
		// not sure if we need this yet
		
		String mostRelatedURI = null;
		double mostSimVal = HypernymListGenerator.SIMILARITY_CUTOFF;
		for(String uri : keywordURIs) {
			//String instance = uri.replaceAll(".Keyword/", "");
			String instance = uri;
			if(instance.contains("/")) {
				// this is for properties that are also concepts
				instance = instance.substring(0, instance.lastIndexOf("/"));
			}
			double simVal = wnComp.compareKeywords(instance, word);
			if(simVal < mostSimVal) {
				mostSimVal = simVal;
				mostRelatedURI = uri;
			}
		}
		
		return mostRelatedURI;
	}
}
