package prerna.algorithm.impl;

import java.text.DecimalFormat;
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

public class SearchMasterDB {
	private static final Logger logger = LogManager.getLogger(SearchMasterDB.class.getName());
	
	//engine variables
	String masterDBName = "MasterDatabase";
	IEngine masterEngine;
	
	protected final static String semossURI = "http://semoss.org/ontologies";
	protected final static String keywordBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS+"/Keyword";
	protected final static String databaseBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS+"/Database";
	protected final static String masterConceptBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS+"/MasterConcept";
	
	protected final static String getPossibleKeywordsQuery = "SELECT DISTINCT ?Keyword WHERE {BIND(<http://semoss.org/ontologies/Concept/Keyword/@KEYWORD@> AS ?SubgraphKeyword) BIND(<http://semoss.org/ontologies/Concept/Database/@DATABASE@> AS ?Database) {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?Keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?Keyword} {?Database <http://semoss.org/ontologies/Relation/Has> ?Keyword}}";
	protected final static String instanceExistsQuery = "ASK WHERE { {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@KEYWORD@> ;}FILTER( regex (str(?s),'@KEYWORDINSTANCE@$'))}";
	String masterConceptsQuery = "SELECT DISTINCT ?SubgraphKeyword ?MasterConcept WHERE {{?SubgraphKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword}}";
	String similarKeywordsQuery = "SELECT DISTINCT ?Database ?SubgraphKeyword ?MasterKeyword WHERE {{?Database <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Database>} {?SubgraphKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} @FILTER@ OPTIONAL{{?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}  {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeyword} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeyword}}}";
	String similarMasterConceptsQuery = "SELECT DISTINCT ?Database ?SubgraphKeyword ?MasterConcept WHERE {{?Database <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Database>} {?SubgraphKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}  {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeyword} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeyword} @FILTER@}";
	String similarEdgesQuery = "SELECT DISTINCT ?Database ?MasterConceptConnection ?MasterConceptFrom ?MasterConceptTo ?MasterKeywordFrom ?MasterKeywordTo WHERE {{?Database <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Database>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConceptFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConceptTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?MasterConceptFrom} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?MasterConceptTo} {?MasterConceptFrom <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?MasterConceptTo <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo} @FILTER@}";
	String similarMasterConceptConnectionsQuery = "SELECT DISTINCT ?Database ?MasterConceptConnection ?MasterConceptFrom ?MasterConceptTo WHERE {{?Database <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Database>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConceptFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConceptTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?MasterConceptFrom} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?MasterConceptTo} {?MasterConceptFrom <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?MasterConceptTo <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo} @FILTER@}";

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
	
//	/**
//	 * Method to set KeywordList and EdgeLists if just given these lists.
//	 * Will likely be used later as alternative to the below method.
//	 */
//	public void setKeywordAndEdgeList(ArrayList<String> keywordList,ArrayList<String> edgeVertInList,ArrayList<String> edgeVertOutList) {
//		this.keywordList = keywordList;
//		this.edgeVertInList = edgeVertInList;
//		this.edgeVertOutList = edgeVertOutList;
//	}
	
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

		masterConceptsQuery = addBindings(masterConceptsQuery,"SubgraphKeyword",keywordBaseURI,keywordList);
		ArrayList<Object []> keywordMasterConceptsList = processQuery(masterConceptsQuery);
		masterConceptsList = processMasterConceptsList(keywordList,keywordMasterConceptsList);
		
		ArrayList<Object[]> list = new ArrayList<Object[]>();

		//to look at keyword level and get all possible combinations of keywords for master concepts
		if(!count){
			similarKeywordsQuery = addBindings(similarKeywordsQuery,"SubgraphKeyword",keywordBaseURI,keywordList);
			similarKeywordsQuery = addDatabaseFilter(similarKeywordsQuery,databaseFilter);
			ArrayList<Object []> similarKeywordsResults = processQuery(similarKeywordsQuery);
			similarKeywordsResults = addColumn(similarKeywordsResults,"Node");
			list.addAll(similarKeywordsResults);
			
			similarEdgesQuery = addBindings(similarEdgesQuery,"MasterConceptFrom",masterConceptBaseURI,masterConceptsList);
			similarEdgesQuery = addDatabaseFilter(similarEdgesQuery,databaseFilter);
			ArrayList<Object []> similarEdgesList = processQuery(similarEdgesQuery);
			ArrayList<Object []> processedSimilarEdgesList = processSimilarEdgesList(edgeVertOutList,edgeVertInList,similarEdgesList);
			processedSimilarEdgesList = addColumn(processedSimilarEdgesList,"Edge");
			list.addAll(processedSimilarEdgesList);
			
			headers = new String[4];
			headers[0] = "Database";
			headers[1] = "Node_or_Edge_In_Subgraph";
			headers[2] = "Mapped_Node_or_Edge_In_Master";
			headers[3] = "Node or Edge";
		} else {//to find the score of each database, only considering master concepts and getting unique relationships
			similarMasterConceptsQuery = addBindings(similarMasterConceptsQuery,"SubgraphKeyword",keywordBaseURI,keywordList);
			similarMasterConceptsQuery = addDatabaseFilter(similarMasterConceptsQuery,databaseFilter);
			ArrayList<Object []> similarMasterConceptsResults = processQuery(similarMasterConceptsQuery);
			
			similarMasterConceptConnectionsQuery = addBindings(similarMasterConceptConnectionsQuery,"MasterConceptFrom",masterConceptBaseURI,masterConceptsList);
			similarMasterConceptConnectionsQuery = addDatabaseFilter(similarMasterConceptConnectionsQuery,databaseFilter);
			ArrayList<Object []> similarEdgesList = processQuery(similarMasterConceptConnectionsQuery);
			ArrayList<Object []> processedSimilarEdgesList = processSimilarEdgesList(edgeVertOutList,edgeVertInList,similarEdgesList);
			
			ArrayList<Object[]> fullResultsList = new ArrayList<Object[]>();
			fullResultsList.addAll(similarMasterConceptsResults);
			fullResultsList.addAll(processedSimilarEdgesList);
			list = createScoreList(fullResultsList,keywordList.size()+edgeVertInList.size());
			
			headers = new String[2];
			headers[0] = "Database";
			headers[1] = "Score";
		}

		return list;
	}
	
	
	private ArrayList<String> filterDatabaseList() {
		String databaseQuery = "SELECT DISTINCT ?Database WHERE {{?Database <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Database>}}";
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
			databaseFilter += "<"+databaseBaseURI + "/"+db+">"+", ";
		}
		return databaseFilter.substring(0,databaseFilter.length()-2);
	}
	
	private Boolean databaseContainsInstance(IEngine engine, String subgraphKeyword, String instance) {
		String getPossibleKeywordsQueryFilled = getPossibleKeywordsQuery.replaceAll("@KEYWORD@",subgraphKeyword).replaceAll("@DATABASE@", engine.getEngineName());
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
			return query.replace("@FILTER@","FILTER (?Database in ("+databaseFilter+"))");
		else
			return query.replace("@FILTER@","");
	}
	
	/**
	 * Executes a query and stores the results.
	 * @param query String to run.
	 * @return ArrayList<String> that contains the results of the query.
	 */
	private ArrayList<String> processListQuery(String query) {
		SesameJenaSelectWrapper wrapper = executeQuery(query);
		ArrayList<String> list = new ArrayList<String>();
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();				
				Object value = getVariable(names[0], sjss);
				list.add((String)value);
			}
		} catch (RuntimeException e) {
			logger.error("Could not store results for query: "+query);
		}
		return list;
	}
	
	/**
	 * Executes a query and stores the results.
	 * @param query String to run.
	 * @return ArrayList<Object []> that contains the results of the query.
	 */
	private ArrayList<Object []> processQuery(String query) {
		SesameJenaSelectWrapper wrapper = executeQuery(query);
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();				
				Object [] values = new Object[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++)
					values[colIndex] = getVariable(names[colIndex], sjss);
				list.add(values);
			}
		} catch (RuntimeException e) {
			logger.error("Could not store results for query: "+query);
		}
		return list;
	}
	private SesameJenaSelectWrapper executeQuery(String query){
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(masterEngine);
		try{
			wrapper.executeQuery();	
		} catch (RuntimeException e){
			logger.error("Could not execute query: "+query);
		}
		return wrapper;
	}
	/**
	 * Method getVariable. Gets the variable names from the query results.
	 * @param varName String - the variable name.
	 * @param sjss SesameJenaSelectStatement - the associated sesame jena select statement.
	 * @return Object - results.*/
	private Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getVar(varName);
	}

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
	
	private ArrayList<Object []> createScoreList(ArrayList<Object []> fullResultslist, int totalNum){
		ArrayList<Object []> finalScoreList = new ArrayList<Object []>();
		
		for(int i=0;i<fullResultslist.size();i++) {
			Object[] row = fullResultslist.get(i);
			String database = (String) row[0];
			int databaseRowInd = getIndOfDatabase(finalScoreList,database);
			if(databaseRowInd>-1) {
				Object[] databaseRow = finalScoreList.get(databaseRowInd);
				databaseRow[1] = (Integer) databaseRow[1]+1;
				finalScoreList.set(databaseRowInd, databaseRow);
			} else {
				Object[] databaseRow = new Object[2];
				databaseRow[0] = database;
				databaseRow[1] = 1;
				finalScoreList.add(databaseRow);
			}
		}
		for(int i=0;i<finalScoreList.size();i++) {
			Object[] databaseRow = finalScoreList.get(i);
			Double score = ((Integer)databaseRow[1])*1.0 / totalNum;
			DecimalFormat formatter = new DecimalFormat("#.##");
			databaseRow[1] = formatter.format(score);
			finalScoreList.set(i,databaseRow);
		}
		return finalScoreList;
	}
	
	private Integer getIndOfDatabase(ArrayList<Object []> finalScoreList,String database){
		for(int i=0;i<finalScoreList.size();i++) {
			Object[] row = finalScoreList.get(i);
			String currDatabase = (String) row[0];
			if(currDatabase.equals(database))
				return i;
		}
		return -1;
	}

}
