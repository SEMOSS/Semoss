package prerna.algorithm.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import com.bigdata.rdf.model.BigdataURIImpl;

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
	
	private final String dbKey = "database";
	private final String scoreKey = "similarityScore";
	private final String questionKey = "question";
	private final String keywordKey = "keyword";
	private final String perspectiveKey = "perspective";
	private final String instanceKey = "instances";
	private final String vizTypeKey = "viz";
	
	
	//engine variables
	String masterDBName = "MasterDatabase_8DBs";
	IEngine masterEngine = (BigDataEngine)DIHelper.getInstance().getLocalProp(masterDBName);

	protected final static String semossURI = "http://semoss.org/ontologies";
	protected final static String conceptBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
	protected final static String engineBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS+"/Engine";
	protected final static String masterConceptBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS+"/MasterConcept";
	
	//for the instance's master concepts, find all the possible related keywords for each engine.
	protected final static String getPossibleKeywordsQuery = "SELECT DISTINCT ?MasterConcept ?Engine ?Keyword WHERE {{?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}{?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?Keyword} {?Engine <http://semoss.org/ontologies/Relation/Has> ?Keyword}}";
	//for a given instance, check to see if a given engine contains under the keyword type.
	protected final static String instanceExistsQuery = "SELECT DISTINCT ?instanceURI WHERE { {?instanceURI <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@KEYWORDURI@> ;}FILTER( regex (str(?instanceURI),'@INSTANCE@$'))}";
	//for a given list of keywords, find their master concepts.
	protected final static String masterConceptsForSubgraphKeywordsQuery = "SELECT DISTINCT ?Keyword ?MasterConcept WHERE {{?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?Keyword}}";
	protected final static String allMasterConceptsQuery = "SELECT DISTINCT ?MasterConcept WHERE {{?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}}";
	protected final static String similarKeywordsQuery = "SELECT DISTINCT ?Engine ?SubgraphKeyword ?MasterKeyword WHERE {{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?SubgraphKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} @FILTER@ OPTIONAL{{?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}  {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeyword} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeyword}}}";
	protected final static String similarMasterConceptsQuery = "SELECT DISTINCT ?Engine ?MasterConcept WHERE {{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeyword} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeyword} @FILTER@}";
	protected final static String similarEdgesQuery = "SELECT DISTINCT ?Engine ?MasterConceptConnection ?MasterConceptFrom ?MasterConceptTo ?MasterKeywordFrom ?MasterKeywordTo WHERE {{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConceptFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConceptTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?MasterConceptFrom} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?MasterConceptTo} {?MasterConceptFrom <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?MasterConceptTo <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo} @FILTER@}";
	protected final static String similarMasterConceptConnectionsQuery = "SELECT DISTINCT ?Engine ?MasterConceptConnection ?MasterConceptFrom ?MasterConceptTo WHERE {{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConceptFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConceptTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?MasterConceptFrom} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?MasterConceptTo} {?MasterConceptFrom <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?MasterConceptTo <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo} @FILTER@}";
	protected final static String similarMasterConceptConnectionsOutsideQuery = "SELECT DISTINCT ?Engine ?MasterConceptConnection (IF(BOUND(?MasterConceptFrom),?MasterConceptFrom,?BoundMasterConcept) AS ?MCFrom) (IF(BOUND(?MasterConceptTo),?MasterConceptTo,?BoundMasterConcept) AS ?MCTo) WHERE {{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?BoundMasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConceptTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}  {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?BoundMasterConcept} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?MasterConceptTo} {?BoundMasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?MasterConceptTo <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo}}UNION{{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?MasterConceptFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?BoundMasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}  {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?MasterConceptFrom} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?BoundMasterConcept} {?MasterConceptFrom <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?BoundMasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo}}@FILTER@}";
	protected final static String relatedQuestionsQuery = "SELECT DISTINCT ?Engine ?InsightLabel ?MasterKeyword ?PerspectiveLabel ?MasterConcept ?Viz WHERE {{?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Insight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Insight>} {?Perspective <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Perspective>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeyword} {?Engine <http://semoss.org/ontologies/Relation/Has> ?MasterKeyword} {?Insight <INSIGHT:PARAM> ?Param}{?Param <PARAM:TYPE> ?MasterKeyword} {?Engine <http://semoss.org/ontologies/Relation/Engine:Perspective> ?Perspective} {?Perspective <http://semoss.org/ontologies/Relation/Perspective:Insight> ?Insight} {?Engine <http://semoss.org/ontologies/Relation/Engine:Insight> ?Insight}{?Insight <http://semoss.org/ontologies/Relation/Contains/Label> ?InsightLabel} {?Perspective <http://semoss.org/ontologies/Relation/Contains/Label> ?PerspectiveLabel} {?Insight <http://semoss.org/ontologies/Relation/Contains/Layout> ?Viz} @FILTER@}";

	double similarityThresh = 2.7;
	
	//list of keyword and edge types in the subgraph
	ArrayList<String> keywordList = new ArrayList<String>();
	ArrayList<String> edgeVertInList = new ArrayList<String>();
	ArrayList<String> edgeVertOutList = new ArrayList<String>();
	
	//list of master concepts associated with each of the keywords
	ArrayList<String> masterConceptsList;
	
	//list for instances, their keywords, and their master concepts
	private boolean includeInstance = false;
	ArrayList<String> instanceList;
	ArrayList<String> keywordForInstanceList;
	ArrayList<String> mcForInstanceList;
	//stores the list of possible URIs for each MC_Engine combination.
	Hashtable<String,ArrayList<String>> urisForMCEng = new Hashtable<String,ArrayList<String>>();
	
	private boolean debugging=false;
	
	public void isDebugging(boolean debugging) {
		this.debugging=debugging;
	}
		
	public void setMasterDBName(String masterDBName) {
		this.masterDBName = masterDBName;
		this.masterEngine = (BigDataEngine)DIHelper.getInstance().getLocalProp(masterDBName);
	}
	
	/**
	 * Specify a list of instances and their keywords
	 * Also finds the master concepts for each of the keywords and sets them.
	 * Assumes all keywords provided are in the master database, if not, then the master concept is just the keyword
	 * @param keywordForInstanceList
	 * @param instanceList
	 */
	public void setInstanceList(ArrayList<SEMOSSVertex> instanceVertList) {
		includeInstance = true;
		this.instanceList = new ArrayList<String>();
		this.keywordForInstanceList = new ArrayList<String>();
		//process through instance list, add instance and type for each uri
		for(SEMOSSVertex vert : instanceVertList) {
			String type = (String)vert.getProperty(Constants.VERTEX_TYPE);
			String instance = (String)vert.getProperty(Constants.VERTEX_NAME);
			instanceList.add(instance);
			keywordForInstanceList.add(type);
			if(!keywordList.contains(type)) {
				keywordList.add(type);
			}
		}
		//find the master concept for each instance
		mcForInstanceList = getMCsForKeywords(keywordForInstanceList);
	}
	
	/**
	 * Method to set KeywordList and EdgeLists if just given these lists.
	 * KeywordList should have unique elements
	 * Edge vert out and edge vert in should match up with one another without duplicate relationship
	 */
	public void setKeywordAndEdgeList(ArrayList<String> keywordList,ArrayList<String> edgeVertOutList,ArrayList<String> edgeVertInList) {
		this.keywordList = keywordList;
		this.edgeVertOutList = edgeVertOutList;
		this.edgeVertInList = edgeVertInList;
	}
	
	/**
	 * Method to set KeywordList and EdgeList if given a full subgraph.
	 * @param vertStore
	 * @param edgeStore
	 * @param metamodelSubgraph True if the subgraph is from a metamodel, false if includes instances
	 */
	public void setKeywordAndEdgeList(Hashtable<String,SEMOSSVertex> vertStore,Hashtable<String,SEMOSSEdge> edgeStore,boolean metamodelSubgraph) {

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
	
	/**
	 * Checks to see if an edge exists in the edgeList.
	 * True if does, false if does not.
	 * @param edgeIn
	 * @param edgeOut
	 * @return
	 */
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
	public ArrayList<Hashtable<String,Object>> searchDB() {

		if(keywordList == null || keywordList.isEmpty()) {
			logger.info("No keywords were given to search for.");
			return new ArrayList<Hashtable<String, Object>>();
		}
		
		//if including instances, then only include databases that have those instances
		//by creating a filter string of allowed databases to use in the queries
		//empty filter if no instances to be included
		String databaseFilter="";
		if(includeInstance) {
			//TODO can a database still be relevant even if it only contains 1/2 of the instances of interest?
			ArrayList<String> databaseList = filterDatabaseList();
			if(databaseList.isEmpty()) {
				logger.error("No databases include the instances given.");
				Utility.showError("No databases include the instances given.");
				return new ArrayList<Hashtable<String, Object>>();
			}
			databaseFilter = createDatabaseFilter(databaseList);
		}
		
		//determine the master concepts for each keyword.
		//if the keywords are in the subgraph, then just query
		//if the keywords are not in the subgraph (i.e. come from NLP),
		//use word net to find a suitable MC
		//if MCs still not found, then the entry will be the "keyword"
		masterConceptsList = getMCsForKeywords(keywordList);
		int numVals = 0;
		for(int i=0;i<masterConceptsList.size();i++) {
			if(!masterConceptsList.get(i).equals(""))
				numVals++;
		}
		if(numVals==0) {
			logger.error("No master concepts found for any keywords entered.");
			Utility.showError("No master concepts found for any keywords entered.");
			return new ArrayList<Hashtable<String, Object>>();
		}

		ArrayList<Hashtable<String, Object>> list = new ArrayList<Hashtable<String, Object>>();

		//to look at keyword level and get all possible combinations of keywords for master concepts
		//this is really only used for testing and debugging purposes. Otherwise, count should always be true.
		if(debugging){
			String simKeywords = addBindings(similarKeywordsQuery,"SubgraphKeyword",conceptBaseURI,keywordList);
			simKeywords = addDatabaseFilter(simKeywords,databaseFilter);
			ArrayList<Hashtable<String, Object>> similarKeywordsResults = processHashQuery(simKeywords);
			similarKeywordsResults = addColumn(similarKeywordsResults,"Node", "Type");
			list.addAll(similarKeywordsResults);
			
//			String simEdges = addBindings(similarEdgesQuery,"MasterConceptFrom",masterConceptBaseURI,masterConceptsList);
//			simEdges = addDatabaseFilter(simEdges,databaseFilter);
//			ArrayList<Hashtable<String, Object>> similarEdgesList = processHashQuery(simEdges);
//			ArrayList<Hashtable<String, Object>> processedSimilarEdgesList = processSimilarEdgesList(edgeVertOutList,edgeVertInList,similarEdgesList);
//			processedSimilarEdgesList = addColumn(processedSimilarEdgesList,"Edge", "Type");
//			list.addAll(processedSimilarEdgesList);
			
		} else {//to find the score of each database, only considering master concepts and getting unique relationships
			String simMCQuery = addBindings(similarMasterConceptsQuery,"MasterConcept",masterConceptBaseURI,masterConceptsList);
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
			
			Hashtable<String, Double> engineScores = new Hashtable<String, Double>();
			engineScores = aggregateScores(similarMasterConceptsResults,similarEdgesResults,allEdgesList,keywordList.size()+edgeVertInList.size());
			engineScores = normalize(engineScores);
			
			String questions;
			if(includeInstance) {
				questions = addBindings(relatedQuestionsQuery,"MasterConcept",masterConceptBaseURI,mcForInstanceList);				
			} else {
				questions = addBindings(relatedQuestionsQuery,"MasterConcept",masterConceptBaseURI,masterConceptsList);
			}
			questions = addDatabaseFilter(questions,databaseFilter);
			//ArrayList<Object []> relatedQuestionsResults = processQuery(questions);
			
			list = addQuestions(questions,engineScores);
			
		}
		return list;
	}

	//TODO currently only including engines that have all the instances. should this be including engines even if there is only one instance?
	private ArrayList<String> filterDatabaseList() {
		//for each instance's master concept, find all engines keywords associated to it
		//making a hashtable that links MC_Engine-->keywordlist
		//also make a first cut at the engine list to then filter through
		ArrayList<String> engines = new ArrayList<String>();
		Hashtable<String,ArrayList<String>> keywordsForEngines = new Hashtable<String,ArrayList<String>>();
		//only process each master concept once.
		
		String getPossibleKeywordsQueryFilled = addBindings(getPossibleKeywordsQuery,"MasterConcept",masterConceptBaseURI,mcForInstanceList);
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,getPossibleKeywordsQueryFilled);
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
			String mc = (String)sjss.getVar(names[0]);
			String engine = (String)sjss.getVar(names[1]);
			BigdataURIImpl keywordURI = (BigdataURIImpl)sjss.getRawVar(names[2]);
			
			String mcEngineKey = mc+"_"+engine;
			ArrayList<String> keywords = new ArrayList<String>();
			if(keywordsForEngines.containsKey(mcEngineKey)) {
				keywords = keywordsForEngines.get(mcEngineKey);
			}
			keywords.add(keywordURI.stringValue());
			keywordsForEngines.put(mcEngineKey,keywords);
			if(!engines.contains(engine))
				engines.add(engine);
		}
	
		//go through the engine list
		//for each instance, check all keywords to see if instance exists
		//return all the keywords that have that instance
		ArrayList<String> filteredEngines = new ArrayList<String>();
		for(int eInd=0;eInd<engines.size();eInd++) {
			String engineName = engines.get(eInd);
			IEngine engine = (BigDataEngine)DIHelper.getInstance().getLocalProp(engineName);
			if(engineContainsAllInstances(engine,keywordsForEngines))
				filteredEngines.add(engineName);
		}
		
		return filteredEngines;
	}
		
	private Boolean engineContainsAllInstances(IEngine engine,Hashtable<String,ArrayList<String>> keywordURIsForEngines) {
		//if there is any instance that an engine does not have, then return false
		//for each instance, get the possible keywords. If doesn't exist return false
		//check all keywords, add all that have the instance to the list.
		//make a list of all instanceURIs for each
		Hashtable<String,ArrayList<String>> thisEngineURIs = new Hashtable<String,ArrayList<String>>();
		for(int iInd=0;iInd<mcForInstanceList.size();iInd++) {
			String mc = mcForInstanceList.get(iInd);
			String instance = instanceList.get(iInd);
			String mcEngineKey = mc+"_"+engine.getEngineName();
			
			if(!keywordURIsForEngines.containsKey(mcEngineKey))
				return false;
			
			ArrayList<String> possibleKeywordURIs = keywordURIsForEngines.get(mcEngineKey);

			Boolean foundResults = false;
			for(int i=0;i<possibleKeywordURIs.size();i++) {
				String possibleKeywordURI = possibleKeywordURIs.get(i);
				String instanceExistsQueryFilled = instanceExistsQuery.replaceAll("@KEYWORDURI@", possibleKeywordURI).replaceAll("@INSTANCE@","/"+instance);
				ArrayList<String> instanceURIs = processListQuery(engine,instanceExistsQueryFilled,true);
				String engineMCKeywordKey = engine.getEngineName()+"_"+mc+"_"+possibleKeywordURI;
				if(!instanceURIs.isEmpty()) {
					if(thisEngineURIs.containsKey(engineMCKeywordKey))
						instanceURIs.addAll(thisEngineURIs.get(engineMCKeywordKey));
					thisEngineURIs.put(engineMCKeywordKey,instanceURIs);
					foundResults = true;
				}
			}
			
			if(!foundResults)
				return false;
		}
		//if we made it to this point, we have all of the isntances and should all all to the master uris
		urisForMCEng.putAll(thisEngineURIs);
		return true;
	}
//	
//	private Boolean databaseContainsInstance(IEngine engine, String mc, String instance, int instanceInd) {
//		String getPossibleKeywordsQueryFilled = getPossibleKeywordsQuery.replaceAll("@MASTERCONCEPT@",mc).replaceAll("@ENGINE@", engine.getEngineName());
//		ArrayList<String> possibleKeywordList = processListQuery(masterEngine,getPossibleKeywordsQueryFilled);
//		Boolean retval=false;
//		if(possibleKeywordList.isEmpty())
//			return false;
//		for(int i=0;i<possibleKeywordList.size();i++) {
//			String possibleKeyword = possibleKeywordList.get(i);
//			String instanceExistsQueryFilled = instanceExistsQuery.replaceAll("@KEYWORD@", possibleKeyword).replaceAll("@KEYWORDINSTANCE@","/"+instance);
//			
//			BooleanProcessor proc = new BooleanProcessor();
//			proc.setEngine(engine);
//			proc.setQuery(instanceExistsQueryFilled);
//			if(proc.processQuery()) {
//				Hashtable<String,ArrayList<String>> engineKeywordHash = engineKeywordForInstanceList.get(instanceInd);
//				ArrayList<String> enginesKeywords = new ArrayList<String>();
//				if(engineKeywordHash.containsKey(engine.getEngineName()))
//					enginesKeywords = engineKeywordHash.get(engine.getEngineName());
//				if(!enginesKeywords.contains(possibleKeyword))
//					enginesKeywords.add(possibleKeyword);
//				engineKeywordHash.put(engine.getEngineName(),enginesKeywords);
//				engineKeywordForInstanceList.set(instanceInd, engineKeywordHash);
//				retval=true;
//			}
//		}
//		
//		return retval;
//	}
	
	private String createDatabaseFilter(ArrayList<String> databaseList) {
		String databaseFilter ="";
		for(String db : databaseList) {
			databaseFilter += "<"+engineBaseURI + "/"+db+">"+", ";
		}
		return databaseFilter.substring(0,databaseFilter.length()-2);
	}
	
	private ArrayList<String> getMCsForKeywords(ArrayList<String> keywordList) {
		ArrayList<String> masterConceptsList = new ArrayList<String>(keywordList.size());
		for(int i=0;i<keywordList.size();i++)
			masterConceptsList.add("");
		String mcQuery = addBindings(masterConceptsForSubgraphKeywordsQuery,"Keyword",conceptBaseURI,keywordList);
		ArrayList<Object []> keywordMasterConceptsList = processQuery(mcQuery);
		//if all the keywords have a master concept, then link them	
		//if there are keywords without masterconcepts, use word net to see if there are related.
		//this primarily will happen when input comes from NLP processing.
		if(keywordList.size()<=keywordMasterConceptsList.size()) {
			for(int i=0;i<keywordMasterConceptsList.size();i++) {
				Object[] keywordMCRelation = keywordMasterConceptsList.get(i);
				String keyword = (String)keywordMCRelation[0];
				String concept = (String)keywordMCRelation[1];
				int index = keywordList.indexOf(keyword);
				masterConceptsList.set(index,concept);
			}
		}
		else{
			ArrayList<String> allMCs = processListQuery(masterEngine,allMasterConceptsQuery,false);
			double[][] simScores;
			try {
				simScores = IntakePortal.WordNetMappingFunction(deepCopy(keywordList),deepCopy(allMCs));
				for(int keyInd=0;keyInd<keywordList.size();keyInd++) {
					String keyword = keywordList.get(keyInd);
					keywordList.set(keyInd, Utility.cleanString(keyword,true));
				}
				for(int keyInd=0;keyInd<keywordList.size();keyInd++) {
					int mcInd = 0;
					while(mcInd<allMCs.size()) {
						if(simScores[keyInd][mcInd]>similarityThresh) {
							masterConceptsList.set(keyInd,allMCs.get(mcInd));
							logger.info("Master Concept for the NLP search term "+keywordList.get(keyInd) + " is " + allMCs.get(mcInd));
							mcInd=allMCs.size();
						}
						mcInd++;
					}
					if(masterConceptsList.get(keyInd).equals("")) {
						logger.error("No Master Concept was found for the NLP search term "+keywordList.get(keyInd));
					}
				}
			} catch (InvalidFormatException e) {
				logger.error("Invalid Format Exception: could not do word net mapping for keywords and MCs");
				return new ArrayList<String>();
			} catch (IOException e) {
				logger.error("IO Exception: could not do word net mapping for keywords and MCs");
				return new ArrayList<String>();
			}
		}
		return masterConceptsList;
	}
	
	private ArrayList<String> deepCopy(ArrayList<String> list) {
		ArrayList<String> copy = new ArrayList<String>();
		for(String entry : list) {
			copy.add(entry);
		}
		return copy;
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
						if(debugging)
							edge[2] = (String) possibleEdge[4] + Constants.RELATION_URI_CONCATENATOR + (String) possibleEdge[5];
						else
							edge[2] = queryMCFrom + Constants.RELATION_URI_CONCATENATOR + queryMCTo;
						//could put this second edge as the specific verticies going to/from
						String databaseMCC = database + "-" + mcc;
						if(debugging || !databaseMccList.contains(databaseMCC)){
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
			String val = vals.get(i);
			if(!val.equals(""))
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
	private ArrayList<String> processListQuery(IEngine engine,String query,Boolean getURI) {
		SesameJenaSelectWrapper wrapper = Utility.processQuery(engine,query);
		ArrayList<String> list = new ArrayList<String>();
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
			if(getURI) {
				String value = ((BigdataURIImpl)sjss.getRawVar(names[0])).stringValue();
				list.add(value);
			}else {
				Object value = sjss.getVar(names[0]);
				list.add((String)value);
			}
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
				values[colIndex] = sjss.getVar(names[colIndex]);
			list.add(values);
		}
		return list;
	}
	
	private ArrayList<Hashtable<String, Object>> processHashQuery(String query) {
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,query);
		ArrayList<Hashtable<String, Object>> list = new ArrayList<Hashtable<String, Object>>();
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();				
			Hashtable<String, Object> values = new Hashtable<String, Object>();
			for(int colIndex = 0;colIndex < names.length;colIndex++)
				values.put(names[colIndex], sjss.getVar(names[colIndex]));
			list.add(values);
		}
		return list;
	}

	/**
	 * Adds a column in the list with the value specified.
	 * @param list	ArrayList of Object[] where each Object[]
	 * @param fillVal
	 * @return
	 */
	private ArrayList<Hashtable<String, Object>> addColumn(ArrayList<Hashtable<String, Object>> list,String fillVal, String type) {
		for(int i=0;i<list.size();i++) {
			Hashtable<String, Object> currRow = list.get(i);
			currRow.put(type, fillVal);
		}
		return list;
	}
	
	/**
	 * Adds a column in the list with the value specified.
	 * @param list	ArrayList of Object[] where each Object[]
	 * @param fillVal
	 * @return
	 */
	private ArrayList<Hashtable<String, Object>> addQuestions(String questionQuery,Hashtable<String, Double> engineScoreList) {
		ArrayList<Hashtable<String, Object>> returnArray = new ArrayList<Hashtable<String, Object>>();
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,questionQuery);
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();				
			String engine = (String)sjss.getVar(names[0]);
			String question = (String)sjss.getVar(names[1]);
			String keyword =((BigdataURIImpl)sjss.getRawVar(names[2])).stringValue();
			String perspective = (String)sjss.getVar(names[3]);
			String masterConcept = (String)sjss.getVar(names[4]);
			String viz = (String)sjss.getVar(names[5]);
			Double score = engineScoreList.get(engine);

			Hashtable<String, Object> insightHash = new Hashtable<String, Object>();
			insightHash.put(this.dbKey, engine);
			insightHash.put(this.questionKey, question);
			insightHash.put(this.keywordKey, keyword);
			insightHash.put(this.perspectiveKey, perspective);
			insightHash.put(this.scoreKey, score);
			insightHash.put(this.vizTypeKey,viz);
			ArrayList<String> instances =  new ArrayList<String>();
			insightHash.put(this.instanceKey, instances);
			
			//if we are not including instances, add the hashtable automatically
			if(!includeInstance)
				returnArray.add(insightHash);
			else {
				//for this mc/engine
				String mcEngineKey = engine+"_"+masterConcept + "_"+keyword;
				if(urisForMCEng.containsKey(mcEngineKey)) {
					ArrayList<String> instanceURIs = urisForMCEng.get(mcEngineKey);
					instances.addAll(instanceURIs);
					returnArray.add(insightHash);
				}
			}
		}
		return returnArray;
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
	private Hashtable<String, Double> aggregateScores(ArrayList<Object []> similarMasterConceptsResults,ArrayList<Object []> similarEdgesResults,ArrayList<Object []> allEdgeResults,int n) {
		Hashtable<String, Double> scoreList = new Hashtable<String, Double>();
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
			
			scoreList.put(database ,  Utility.round((p_n-p_e+2*q)/n * (p_n+p_e)/n,2));
			
//			Object[] newDatabaseRow = new Object[3];
//			newDatabaseRow[0] = database;
//			newDatabaseRow[1] = Utility.round((p_e+p_n)/n,2);
//			newDatabaseRow[2] = Utility.round((p_n-p_e+2*q)/n * (p_n+p_e)/n,2);
//			
//			scoreList.add(i,newDatabaseRow);
		}
		return scoreList;
	}
	
	/**
	 * Goes through the list and normalizes the scores in the col specified.
	 * @param scoreList	ArrayList of Object[] to search
	 * @param col		Integer representing the column to normalize
	 * @return			Normalized List
	 */
	private Hashtable<String, Double> normalize(Hashtable<String, Double> scoreList) {
		double maxScore = 0.0;
		for(Double val : scoreList.values()) {
			if(val>maxScore)
				maxScore=val;
		}
		for(String key: scoreList.keySet()) {
			scoreList.put(key, Utility.round((Double)scoreList.get(key) / maxScore,2));
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
