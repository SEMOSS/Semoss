package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.BooleanProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SearchMasterDB {
	private static final Logger logger = LogManager.getLogger(SearchMasterDB.class.getName());
	
	//engine variables
	String masterDBName = "MasterDatabase";
	IEngine masterEngine;
	
	protected final static String semossURI = "http://semoss.org/ontologies";
	protected final static String keywordBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS+"/Keyword";
	protected final static String engineBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS+"/Engine";
	protected final static String masterConceptBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS+"/MasterConcept";
	
	protected final static String getPossibleKeywordsQuery = "SELECT DISTINCT ?Keyword WHERE {BIND(<http://semoss.org/ontologies/Concept/Keyword/@KEYWORD@> AS ?SubgraphKeyword) BIND(<http://semoss.org/ontologies/Concept/Engine/@ENGINE@> AS ?Engine) {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?Keyword} {?Engine <http://semoss.org/ontologies/Relation/Has> ?Keyword}}";
	protected final static String instanceExistsQuery = "ASK WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@KEYWORD@> ;}FILTER( regex (str(?s),'@KEYWORDINSTANCE@$'))}";
	protected final static String masterConceptsQuery = "SELECT DISTINCT ?SubgraphKeyword ?MasterConcept WHERE {{?SubgraphKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword}}";
	protected final static String similarKeywordsQuery = "SELECT DISTINCT ?Engine ?SubgraphKeyword ?MasterKeyword WHERE {{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?SubgraphKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} @FILTER@ OPTIONAL{{?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}  {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeyword} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeyword}}}";
	protected final static String similarMasterConceptsQuery = "SELECT DISTINCT ?Engine ?SubgraphKeyword ?MasterConcept WHERE {{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?SubgraphKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}  {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeyword} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeyword} @FILTER@}";
	protected final static String similarEdgesQuery = "SELECT DISTINCT ?Engine ?MasterConceptConnection ?MasterConceptFrom ?MasterConceptTo ?MasterKeywordFrom ?MasterKeywordTo WHERE {{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConceptFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConceptTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?MasterConceptFrom} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?MasterConceptTo} {?MasterConceptFrom <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?MasterConceptTo <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo} @FILTER@}";
	protected final static String similarMasterConceptConnectionsQuery = "SELECT DISTINCT ?Engine ?MasterConceptConnection ?MasterConceptFrom ?MasterConceptTo WHERE {{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConceptFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConceptTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?MasterConceptFrom} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?MasterConceptTo} {?MasterConceptFrom <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?MasterConceptTo <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo} @FILTER@}";
	protected final static String similarMasterConceptConnectionsOutsideQuery = "SELECT DISTINCT ?Engine ?MasterConceptConnection (IF(BOUND(?MasterConceptFrom),?MasterConceptFrom,?BoundMasterConcept) AS ?MCFrom) (IF(BOUND(?MasterConceptTo),?MasterConceptTo,?BoundMasterConcept) AS ?MCTo) WHERE {{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?BoundMasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConceptTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}  {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?BoundMasterConcept} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?MasterConceptTo} {?BoundMasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?MasterConceptTo <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo}}UNION{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?MasterConceptFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?BoundMasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}  {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?MasterConceptFrom} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?BoundMasterConcept} {?MasterConceptFrom <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?BoundMasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo}}@FILTER@}";
	protected final static String relatedQuestionsQuery = "SELECT DISTINCT ?Engine ?InsightLabel WHERE {{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?SubgraphKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeyword} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeyword} {?MasterKeyword <http://semoss.org/ontologies/Relation/Keyword:Insight> ?Insight} {?Engine <http://semoss.org/ontologies/Relation/Engine:Insight> ?Insight}{?Insight <http://semoss.org/ontologies/Relation/Contains/Label> ?InsightLabel} @FILTER@}";

	//list of keyword and edge types in the subgraph
	ArrayList<String> keywordList;
	ArrayList<String> edgeVertInList;
	ArrayList<String> edgeVertOutList;
	//list of master concepts associated with the keywords
	ArrayList<String> masterConceptsList;
	
	//list for instances
	ArrayList<String> keywordForInstanceList;
	ArrayList<String> instanceList;
	
	public String[] headers;
	boolean count=false;
	boolean includeInstance = false;
	
	public void setCountBoolean(boolean count) {
		this.count=count;
	}
		
	public void setMasterDBName(String masterDBName) {
		this.masterDBName=masterDBName;
	}
	
	/**
	 * Method to set KeywordList and EdgeLists if just given these lists.
	 * Will likely be used later as alternative to the below method.
	 */
	public void setKeywordAndEdgeList(ArrayList<String> keywordList,ArrayList<String> edgeVertInList,ArrayList<String> edgeVertOutList) {
		this.keywordList = keywordList;
		this.edgeVertInList = edgeVertInList;
		this.edgeVertOutList = edgeVertOutList;
	}
	
	/**
	 * Method to set InstanceLists
	 * @param keywordForInstanceList
	 * @param instanceList
	 */
	public void setInstanceList(ArrayList<String> keywordForInstanceList,ArrayList<String> instanceList) {
		includeInstance = true;
		this.keywordForInstanceList = keywordForInstanceList;
		this.instanceList = instanceList;
	}
	
	/**
	 * Method to set KeywordList and EdgeList if given a full subgraph.
	 * @param vertStore
	 * @param edgeStore
	 * @param metamodelSubgraph True if the subgraph is from a metamodel, false if includes instances
	 */
	public void setKeywordAndEdgeList(Hashtable<String,SEMOSSVertex> vertStore,Hashtable<String,SEMOSSEdge> edgeStore,boolean metamodelSubgraph) {
		keywordList = new ArrayList<String>();
		Iterator<SEMOSSVertex> vertItr = vertStore.values().iterator();
		while(vertItr.hasNext()) {
			SEMOSSVertex vert = vertItr.next();
			String keyword;
			if(metamodelSubgraph)
				keyword = (String)vert.getProperty(Constants.VERTEX_NAME);
			else
				keyword = (String)vert.getProperty(Constants.VERTEX_TYPE);
			if(!keywordList.contains(keyword))
				keywordList.add(keyword);
		}
		
		edgeVertInList = new ArrayList<String>();
		edgeVertOutList = new ArrayList<String>();
		Iterator<SEMOSSEdge> edgeItr = edgeStore.values().iterator();
		while(edgeItr.hasNext()) {
			SEMOSSEdge edge = edgeItr.next();
			String edgeIn;
			String edgeOut;
			if(metamodelSubgraph) {
				edgeIn = (String)edge.inVertex.getProperty(Constants.VERTEX_NAME);
				edgeOut = (String)edge.outVertex.getProperty(Constants.VERTEX_NAME);
			} else {
				edgeIn = (String)edge.inVertex.getProperty(Constants.VERTEX_TYPE);
				edgeOut = (String)edge.outVertex.getProperty(Constants.VERTEX_TYPE);
			}
			if(!edgeListContains(edgeIn,edgeOut)) {
				edgeVertInList.add(edgeIn);
				edgeVertOutList.add(edgeOut);
			}
		}
	}
	
	private Boolean edgeListContains(String edgeIn, String edgeOut) {
		for(int inIndex = 0;inIndex<edgeVertInList.size();inIndex++) {
			if(edgeVertInList.get(inIndex).equals(edgeIn)&&edgeVertOutList.get(inIndex).equals(edgeOut))
				return true;
		}
		return false;
	}
	
	/**
	 * Determines the similarity of a given subgraph to other databases in the Master database.
	 * Each database, that has any overlap with the subgraph.
	 * Deletes the old master database if necessary.
	 */
	public ArrayList<Object[]> searchDB() {

		masterEngine = (BigDataEngine)DIHelper.getInstance().getLocalProp(masterDBName);

		String databaseFilter="";
		if(includeInstance) {
			ArrayList<String> databaseList = filterDatabaseList();
			if(databaseList.isEmpty()) {
				logger.info("No databases fit the criteria.");
				return new ArrayList<Object []>();
			}
			databaseFilter = createDatabaseFilter(databaseList);
		}

		String mcQuery = addBindings(masterConceptsQuery,"SubgraphKeyword",keywordBaseURI,keywordList);
		ArrayList<Object []> keywordMasterConceptsList = processQuery(mcQuery);
		masterConceptsList = processMasterConceptsList(keywordList,keywordMasterConceptsList);
		
		ArrayList<Object[]> list = new ArrayList<Object[]>();

		//to look at keyword level and get all possible combinations of keywords for master concepts
		if(!count){
			String simKeywords = addBindings(similarKeywordsQuery,"SubgraphKeyword",keywordBaseURI,keywordList);
			simKeywords = addDatabaseFilter(simKeywords,databaseFilter);
			ArrayList<Object []> similarKeywordsResults = processQuery(simKeywords);
			similarKeywordsResults = addColumn(similarKeywordsResults,"Node");
			list.addAll(similarKeywordsResults);
			
			String simEdges = addBindings(similarEdgesQuery,"MasterConceptFrom",masterConceptBaseURI,masterConceptsList);
			simEdges = addDatabaseFilter(simEdges,databaseFilter);
			ArrayList<Object []> similarEdgesList = processQuery(simEdges);
			ArrayList<Object []> processedSimilarEdgesList = processSimilarEdgesList(edgeVertOutList,edgeVertInList,similarEdgesList);
			processedSimilarEdgesList = addColumn(processedSimilarEdgesList,"Edge");
			list.addAll(processedSimilarEdgesList);
			
			headers = new String[4];
			headers[0] = "Engine";
			headers[1] = "Node or Edge In Subgraph";
			headers[2] = "Mapped Node or Edge In Master";
			headers[3] = "Node or Edge";
		} else {//to find the score of each database, only considering master concepts and getting unique relationships
			String simMCQuery = addBindings(similarMasterConceptsQuery,"SubgraphKeyword",keywordBaseURI,keywordList);
			simMCQuery = addDatabaseFilter(simMCQuery,databaseFilter);
			ArrayList<Object []> similarMasterConceptsResults = processQuery(simMCQuery);
			
			String simMCC = addBindings(similarMasterConceptConnectionsQuery,"MasterConceptFrom",masterConceptBaseURI,masterConceptsList);
			simMCC = addDatabaseFilter(simMCC,databaseFilter);
			ArrayList<Object []> similarEdgesResults = processQuery(simMCC);
			similarEdgesResults = processSimilarEdgesList(edgeVertOutList,edgeVertInList,similarEdgesResults);
			
			String allMCC = addBindings(similarMasterConceptConnectionsOutsideQuery,"BoundMasterConcept",masterConceptBaseURI,masterConceptsList);
			allMCC = addDatabaseFilter(allMCC,databaseFilter);			
			ArrayList<Object []> allEdgesList = processQuery(allMCC);
			
			similarMasterConceptsResults = countDatabaseRows(similarMasterConceptsResults);
			similarEdgesResults = countDatabaseRows(similarEdgesResults);
			allEdgesList = countDatabaseRows(allEdgesList);
			
			list = aggregateScores(similarMasterConceptsResults,similarEdgesResults,allEdgesList,keywordList.size()+edgeVertInList.size());
			list = normalize(list,2);
			
			String questions;
			if(includeInstance) {
				questions = addBindings(relatedQuestionsQuery,"SubgraphKeyword",keywordBaseURI,keywordForInstanceList);				
			} else {
				questions = addBindings(relatedQuestionsQuery,"SubgraphKeyword",keywordBaseURI,keywordList);
			}
			questions = addDatabaseFilter(questions,databaseFilter);
			ArrayList<Object []> relatedQuestionsResults = processQuery(questions);
			
			list = addQuestions(list,relatedQuestionsResults);
			
			headers = new String[4];
			headers[0] = "Engine";
			headers[1] = "SubgraphScore";
			headers[2] = "SubgraphAndOutsideEdgeScore";
			headers[3] = "Related Questions";
		}
		return list;
	}
	
	
	private ArrayList<String> filterDatabaseList() {
		String databaseQuery = "SELECT DISTINCT ?Engine WHERE {{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>}}";
		ArrayList<String> databaseList = processListQuery(databaseQuery);
		
		for(int instanceInd = 0;instanceInd<keywordForInstanceList.size();instanceInd++) {
			String subgraphKeyword = keywordForInstanceList.get(instanceInd);
			String instance = instanceList.get(instanceInd);
			
			int databaseInd = 0;
			while(databaseInd<databaseList.size()) {
				//for this database, check to see if it contains the instance. if not, remove it and continue
				String databaseName = databaseList.get(databaseInd);
				IEngine engine = (BigDataEngine)DIHelper.getInstance().getLocalProp(databaseName);
				if(engine == null || !databaseContainsInstance(engine,subgraphKeyword,instance) ) {
					databaseList.remove(databaseInd);
				}else {
					databaseInd++;
				}
			}
		}
		return databaseList;
	}
	
	private String createDatabaseFilter(ArrayList<String> databaseList) {
		String databaseFilter ="";
		for(String db : databaseList) {
			databaseFilter += "<"+engineBaseURI + "/"+db+">"+", ";
		}
		return databaseFilter.substring(0,databaseFilter.length()-2);
	}
	
	private Boolean databaseContainsInstance(IEngine engine, String subgraphKeyword, String instance) {
		String getPossibleKeywordsQueryFilled = getPossibleKeywordsQuery.replaceAll("@KEYWORD@",subgraphKeyword).replaceAll("@ENGINE@", engine.getEngineName());
		ArrayList<String> possibleKeywordList = processListQuery(getPossibleKeywordsQueryFilled);
		if(possibleKeywordList.isEmpty())
			return false;
		for(int i=0;i<possibleKeywordList.size();i++) {
			String possibleKeyword = possibleKeywordList.get(i);
			String instanceExistsQueryFilled = instanceExistsQuery.replaceAll("@KEYWORD@", possibleKeyword).replaceAll("@KEYWORDINSTANCE@",possibleKeyword+"/"+instance);
			
			BooleanProcessor proc = new BooleanProcessor();
			proc.setEngine(engine);
			proc.setQuery(instanceExistsQueryFilled);
			if(proc.processQuery())
				return true;
		}
		
		return false;
	}
	private ArrayList<String> processMasterConceptsList(ArrayList<String> keywordList, ArrayList<Object []> keywordMasterConceptsList) {
		ArrayList<String> masterConceptsList = new ArrayList<String>();
		masterConceptsList.addAll(keywordList);
		for(int i=0;i<keywordMasterConceptsList.size();i++) {
			Object[] keywordMCRelation = keywordMasterConceptsList.get(i);
			String keyword = (String)keywordMCRelation[0];
			String concept = (String)keywordMCRelation[1];
			int index = keywordList.indexOf(keyword);
			masterConceptsList.set(index,concept);
		}
		return masterConceptsList;
	}
	
	private ArrayList<Object []> processSimilarEdgesList(ArrayList<String> edgeVertOutList,ArrayList<String> edgeVertInList,ArrayList<Object []> similarEdgesList) {
		ArrayList<Object []> processedSimilarEdgesList = new ArrayList<Object []>();
		ArrayList<String> databaseMccList = new ArrayList<String>();
		//method 1: go through all edges in subgraph
		//iterate through the edges in the query results to see if any match
		//if they do, write them
		//if there are no results for a edge, should we print?
		for(int subgraphInd=0;subgraphInd<edgeVertOutList.size();subgraphInd++) {
			String keywordFrom = edgeVertOutList.get(subgraphInd);
			String keywordTo = edgeVertInList.get(subgraphInd);
			String mcFrom = getMasterConcept(keywordFrom);
			String mcTo = getMasterConcept(keywordTo);
			if(!mcFrom.equals("")&&!mcTo.equals("")) {
				for(int queryInd=0;queryInd<similarEdgesList.size();queryInd++) {
					//?Database ?MasterConceptConnection ?MasterConceptFrom ?MasterConceptTo ?MasterKeywordFrom ?MasterKeywordTo
					Object[] possibleEdge = similarEdgesList.get(queryInd);
					String queryMCFrom = (String) possibleEdge[2];
					String queryMCTo = (String) possibleEdge[3];
					if((mcFrom.equals(queryMCFrom)&&mcTo.equals(queryMCTo))||(mcFrom.equals(queryMCTo)&&mcTo.equals(queryMCFrom))) {
						Object [] edge = new Object[3];
						String database = (String) possibleEdge[0];
						String mcc = (String) possibleEdge[1];
						edge[0] = (String) possibleEdge[0];
						edge[1] = keywordFrom + Constants.RELATION_URI_CONCATENATOR + keywordTo;
						if(!count)
							edge[2] = (String) possibleEdge[4] + Constants.RELATION_URI_CONCATENATOR + (String) possibleEdge[5];
						else
							edge[2] = queryMCFrom + Constants.RELATION_URI_CONCATENATOR + queryMCTo;
						//could put this second edge as the specific verticies going to/from
						String databaseMCC = database + "-" + mcc;
						if(!count || !databaseMccList.contains(databaseMCC)){
							processedSimilarEdgesList.add(edge);
							databaseMccList.add(databaseMCC);
						}
					}
				}
			}
		}

		return processedSimilarEdgesList;
	}
	private String getMasterConcept(String keyword) {
		int keywordInd = keywordList.indexOf(keyword);
		if(keywordInd>-1){
			return masterConceptsList.get(keywordInd);
		}
		else{
			logger.error("No Master Concept for Keyword "+keyword);
			return "";
		}
	}
	/**
	 * Adds the bindings for the Keyword variable based on the verticies in the vertStore.
	 * @param variable	String for the name of the variable used in the query to bind the values to.
	 */
	private String addBindings(String query,String variable,String baseURI,ArrayList<String> vals) {
		String bindings = "BINDINGS ?"+variable+" {";
		for(int i=0;i<vals.size();i++){
			bindings+="(<"+baseURI+"/"+vals.get(i)+">)";
		}
		bindings+="}";
		return query + bindings;
	}
	
	private String addDatabaseFilter(String query,String databaseFilter) {
		if(!databaseFilter.equals(""))
			return query.replace("@FILTER@","FILTER (?Engine in ("+databaseFilter+"))");
		else
			return query.replace("@FILTER@","");
	}
	
	/**
	 * Executes a query and stores the results.
	 * @param query String to run.
	 * @return ArrayList<String> that contains the results of the query.
	 */
	private ArrayList<String> processListQuery(String query) {
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,query);
		ArrayList<String> list = new ArrayList<String>();
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();				
			Object value = getVariable(names[0], sjss);
			list.add((String)value);
		}
		return list;
	}
	
	/**
	 * Executes a query and stores the results.
	 * @param query String to run.
	 * @return ArrayList<Object []> that contains the results of the query.
	 */
	private ArrayList<Object []> processQuery(String query) {
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,query);
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();				
			Object [] values = new Object[names.length];
			for(int colIndex = 0;colIndex < names.length;colIndex++)
				values[colIndex] = getVariable(names[colIndex], sjss);
			list.add(values);
		}
		return list;
	}
	
	/**
	 * Method getVariable. Gets the variable names from the query results.
	 * @param varName String - the variable name.
	 * @param sjss SesameJenaSelectStatement - the associated sesame jena select statement.
	 * @return Object - results.*/
	private Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getVar(varName);
	}

	/**
	 * Adds a column in the list with the value specified.
	 * @param list	ArrayList of Object[] where each Object[]
	 * @param fillVal
	 * @return
	 */
	private ArrayList<Object []> addColumn(ArrayList<Object []> list,String fillVal) {
		for(int i=0;i<list.size();i++) {
			Object [] currRow = list.get(i);
			Object [] newRow = new Object[currRow.length+1];
			for(int col=0;col<currRow.length;col++)
				newRow[col] = currRow[col];
			newRow[newRow.length-1] = fillVal;
			list.set(i,newRow);
		}
		return list;
	}
	
	/**
	 * Adds a column in the list with the value specified.
	 * @param list	ArrayList of Object[] where each Object[]
	 * @param fillVal
	 * @return
	 */
	private ArrayList<Object []> addQuestions(ArrayList<Object []> list,ArrayList<Object []> engineQuestionList) {

		for(int i=0;i<list.size();i++) {
			Object [] currRow = list.get(i);
			Object [] newRow = new Object[currRow.length+1];
			for(int col=0;col<currRow.length;col++) {
				newRow[col] = currRow[col];
			}
			newRow[currRow.length] = "";
			list.set(i, newRow);
		}
		for(int i=0;i<engineQuestionList.size();i++) {
			Object [] eqRow = engineQuestionList.get(i);
			String engine = (String)eqRow[0];
			String question = (String)eqRow[1];
			int j=0;
			while(j<list.size()){
				Object [] currRow = list.get(j);
				String currEngine = (String) currRow[0];
				if(currEngine.equals(engine)) {
					if(((String)currRow[currRow.length-1]).equals("")) {
						currRow[currRow.length-1] = question;
						j=list.size();
					}
					else {
						Object[] newRow = currRow.clone();
						newRow[newRow.length-1] = question;
						list.add(j,newRow);
						j=list.size();
					}
				}
				else
					j++;
			}
		}
		return list;
	}
	
	/**
	 * Counts the number of rows in a list that are related to each database.
	 * @param databaseRowList	ArrayList of Object[] where each Object[] is a database and an entry to be counted
	 * @return					New ArrayList of Object[] where each Object[] is a database and its count of rows
	 */
	private ArrayList<Object []> countDatabaseRows(ArrayList<Object []> databaseRowList){
		ArrayList<Object []> databaseCountList = new ArrayList<Object []>();
		
		for(int i=0;i<databaseRowList.size();i++) {
			Object[] row = databaseRowList.get(i);
			String database = (String) row[0];
			int databaseRowInd = getIndex(databaseCountList,database);
			if(databaseRowInd>-1) {
				Object[] databaseRow = databaseCountList.get(databaseRowInd);
				databaseRow[1] = (Integer) databaseRow[1]+1;
				databaseCountList.set(databaseRowInd, databaseRow);
			} else {
				Object[] databaseRow = new Object[2];
				databaseRow[0] = database;
				databaseRow[1] = 1;
				databaseCountList.add(databaseRow);
			}
		}
		return databaseCountList;
	}
	
	/**
	 * Calculates preliminary, unnormalized scores for each database.
	 * @param similarMasterConceptsResults	ArrayList of Object[] where each Object[] is a database and the number of master concepts in the subgraph that it contains
	 * @param similarEdgesResults			ArrayList of Object[] where each Object[] is a database and the number of edges going between the master concepts in the subgraph
	 * @param allEdgeResults				ArrayList of Object[] where each Object[] is a database and the number of edges going from/to the master concepts in the subgraph
	 * @param n								Integer representing the total number of nodes and edges in the subgraph
	 * @return								ArrayList of Object[] where each Object[] is a database and its two possible score
	 */
	private ArrayList<Object []> aggregateScores(ArrayList<Object []> similarMasterConceptsResults,ArrayList<Object []> similarEdgesResults,ArrayList<Object []> allEdgeResults,int n) {
		ArrayList<Object []> scoreList = new ArrayList<Object []>();
		for(int i=0;i<similarMasterConceptsResults.size();i++) {
			Object[] databaseRow = similarMasterConceptsResults.get(i);
			String database = (String)databaseRow[0];
			Double p_n = ((Integer)databaseRow[1])*1.0;
			
			Double p_e = 0.0;
			int similarEdgeRowInd = getIndex(similarEdgesResults,database);
			if(similarEdgeRowInd>-1) {
				Object[] similarEdgeRow = similarEdgesResults.get(similarEdgeRowInd);
				p_e = ((Integer)similarEdgeRow[1])*1.0;
			} else {
				logger.info("Engine "+database + " does not have any similar or inside edges.");
			}
			
			Double q = 0.0;
			int allEdgeRowInd = getIndex(allEdgeResults,database);
			if(allEdgeRowInd>-1) {
				Object[] allEdgeRow = allEdgeResults.get(allEdgeRowInd);
				q = ((Integer)allEdgeRow[1])*1.0;
			} else {
				logger.error("Engine "+database + " does not have any edges.");
			}
			
			Object[] newDatabaseRow = new Object[3];
			newDatabaseRow[0] = database;
			newDatabaseRow[1] = Utility.round((p_e+p_n)/n,2);
			newDatabaseRow[2] = Utility.round((p_n-p_e+2*q)/n * (p_n+p_e)/n,2);
			
			scoreList.add(i,newDatabaseRow);
		}
		return scoreList;
	}
	
	/**
	 * Goes through the list and normalizes the scores in the col specified.
	 * @param scoreList	ArrayList of Object[] to search
	 * @param col		Integer representing the column to normalize
	 * @return			Normalized List
	 */
	private ArrayList<Object []> normalize(ArrayList<Object []> scoreList,int col) {
		double maxScore = 0.0;
		for(int i=0;i<scoreList.size();i++) {
			Object[] databaseRow = scoreList.get(i);
			Double currScore = (Double)databaseRow[col];
			if(currScore>maxScore)
				maxScore=currScore;
		}
		for(int i=0;i<scoreList.size();i++) {
			Object[] databaseRow = scoreList.get(i);
			databaseRow[col] = Utility.round((Double)databaseRow[col] / maxScore,2);
		}
		return scoreList;
	}
	
	/**
	 * Finds the row in the list that pertains to a specific value.
	 * If multiple rows, only returns the first one.
	 * @param list 		ArrayList of Object[] to search
	 * @param valToFind	String representing the value to look for
	 * @return			Index of the value. -1 if it does not exist.
	 */
	private Integer getIndex(ArrayList<Object []> list,String valToFind){
		for(int i=0;i<list.size();i++) {
			Object[] row = list.get(i);
			String currVal = (String) row[0];
			if(currVal.equals(valToFind))
				return i;
		}
		return -1;
	}

}
