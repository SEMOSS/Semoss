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
import prerna.util.Constants;
import prerna.util.DIHelper;

public class SearchMasterDB {
	private static final Logger logger = LogManager.getLogger(SearchMasterDB.class.getName());

	//hashtable of verticies and edges in the subgraph to search for
	Hashtable<String, SEMOSSVertex> vertStore;
	Hashtable<String, SEMOSSEdge> edgeStore;
	
	//variables for creating the db
	String dbName = "MasterDatabase";
	IEngine engine;
	
	protected final static String semossURI = "http://semoss.org/ontologies";
	protected final static String keywordBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS+"/Keyword";
	protected final static String masterConceptBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS+"/MasterConcept";
	
	String masterConceptsQuery = "SELECT DISTINCT ?SubgraphKeyword ?MasterConcept WHERE {{?SubgraphKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword}}";
	String similarKeywordsQuery = "SELECT DISTINCT ?Database ?SubgraphKeyword ?MasterKeyword WHERE {{?Database <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Database>} {?SubgraphKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} OPTIONAL{{?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}  {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeyword} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeyword}}}";
	String similarMasterConceptsQuery = "SELECT DISTINCT ?Database ?SubgraphKeyword ?MasterConcept WHERE {{?Database <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Database>} {?SubgraphKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterKeyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}  {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?SubgraphKeyword} {?MasterConcept <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeyword} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeyword}}";
	String similarEdgesQuery = "SELECT DISTINCT ?Database ?MasterConceptConnection ?MasterConceptFrom ?MasterConceptTo ?MasterKeywordFrom ?MasterKeywordTo WHERE {{?Database <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Database>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConceptFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConceptTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?MasterConceptFrom} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?MasterConceptTo} {?MasterConceptFrom <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?MasterConceptTo <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo}}";
	String similarMasterConceptConnectionsQuery = "SELECT DISTINCT ?Database ?MasterConceptConnection ?MasterConceptFrom ?MasterConceptTo WHERE {{?Database <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Database>} {?MasterConceptConnection <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConceptConnection>} {?MasterConceptFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?MasterConceptTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>} {?KeywordFrom <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?MasterKeywordTo <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterConceptConnection} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/From> ?MasterConceptFrom} {?MasterConceptConnection <http://semoss.org/ontologies/Relation/To> ?MasterConceptTo} {?MasterConceptFrom <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordFrom} {?MasterConceptTo <http://semoss.org/ontologies/Relation/ConsistsOf> ?MasterKeywordTo} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordFrom} {?Database <http://semoss.org/ontologies/Relation/Has> ?MasterKeywordTo}}";

	
	ArrayList<String> keywordList;
	ArrayList<String> masterConceptsList;

	public String[] headers;
	boolean count=false;
	//TODO methods to set engines, verticies, edges
	
	public void setCountBoolean(boolean count) {
		this.count=count;
	}
	/**
	 * Determines the similarity of a given subgraph to other databases in the Master database.
	 * Each database, that has any overlap with the subgraph.
	 * Deletes the old master database if necessary.
	 */
	public ArrayList<Object[]> searchDB() {

		engine = (BigDataEngine)DIHelper.getInstance().getLocalProp(dbName);

		//create vert store, edgestore for testing.
		//will ultimately be replaced with a way to input the metamodels.
		createTestingData();

		keywordList = new ArrayList<String>();
		Iterator<SEMOSSVertex> vertItr = vertStore.values().iterator();
		while(vertItr.hasNext()) {
			SEMOSSVertex vert = vertItr.next();
			keywordList.add((String)vert.getProperty(Constants.VERTEX_NAME));
		}
		
		ArrayList<String> edgeVertInList = new ArrayList<String>();
		ArrayList<String> edgeVertOutList = new ArrayList<String>();
		Iterator<SEMOSSEdge> edgeItr = edgeStore.values().iterator();
		while(edgeItr.hasNext()) {
			SEMOSSEdge edge = edgeItr.next();
			edgeVertInList.add((String)edge.inVertex.getProperty(Constants.VERTEX_NAME));
			edgeVertOutList.add((String)edge.outVertex.getProperty(Constants.VERTEX_NAME));
		}
				
		masterConceptsQuery = addBindings(masterConceptsQuery,"SubgraphKeyword",keywordBaseURI,keywordList);
		ArrayList<Object []> keywordMasterConceptsList = executeQuery(masterConceptsQuery);
		masterConceptsList = processMasterConceptsList(keywordList,keywordMasterConceptsList);
		
		ArrayList<Object []> list = new ArrayList<Object []>();

		//to look at keyword level and get all possible combinations of keywords for master concepts
		if(!count){
			similarKeywordsQuery = addBindings(similarKeywordsQuery,"SubgraphKeyword",keywordBaseURI,keywordList);
			ArrayList<Object []> similarKeywordsResults = executeQuery(similarKeywordsQuery);
			similarKeywordsResults = addColumn(similarKeywordsResults,"Node");
			list.addAll(similarKeywordsResults);
			
			similarEdgesQuery = addBindings(similarEdgesQuery,"MasterConceptFrom",masterConceptBaseURI,masterConceptsList);
			ArrayList<Object []> similarEdgesList = executeQuery(similarEdgesQuery);
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
			ArrayList<Object []> similarMasterConceptsResults = executeQuery(similarMasterConceptsQuery);
			
			similarMasterConceptConnectionsQuery = addBindings(similarMasterConceptConnectionsQuery,"MasterConceptFrom",masterConceptBaseURI,masterConceptsList);
			ArrayList<Object []> similarEdgesList = executeQuery(similarMasterConceptConnectionsQuery);
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
	
	private void createTestingData() {
		vertStore = new Hashtable<String, SEMOSSVertex>();
		edgeStore = new Hashtable<String, SEMOSSEdge>();
		
		//TODO fill verticies and edges store for testing. eventually will be deleted and replaced with setters
		SEMOSSVertex icd = new SEMOSSVertex("http://semoss.org/ontologies/Concept/InterfaceControlDocument");
		SEMOSSVertex data = new SEMOSSVertex("http://semoss.org/ontologies/Concept/DataObject");
		SEMOSSVertex service = new SEMOSSVertex("http://semoss.org/ontologies/Concept/Service");
		SEMOSSVertex system = new SEMOSSVertex("http://semoss.org/ontologies/Concept/System");
		SEMOSSVertex dataElement = new SEMOSSVertex("http://semoss.org/ontologies/Concept/DataElement");
		vertStore.put(icd.uri,icd);
		vertStore.put(data.uri,data);
		vertStore.put(service.uri,service);
		vertStore.put(system.uri,system);
		vertStore.put(dataElement.uri,dataElement);
		
		SEMOSSEdge icdData = new SEMOSSEdge(icd,data,"http://semoss.org/ontologies/Relation/Payload/InterfaceControlDocument:DataObject");
		SEMOSSEdge serviceICD = new SEMOSSEdge(service,icd,"http://semoss.org/ontologies/Relation/Payload/Service:InterfaceControlDocument");
		SEMOSSEdge systemData = new SEMOSSEdge(system,data,"http://semoss.org/ontologies/Relation/Payload/System:DataObject");
		SEMOSSEdge icdSystem = new SEMOSSEdge(icd,system,"http://semoss.org/ontologies/Relation/Payload/InterfaceControlDocument:System");
		SEMOSSEdge systemICD = new SEMOSSEdge(system,icd,"http://semoss.org/ontologies/Relation/Payload/System:InterfaceControlDocument");
		SEMOSSEdge serviceData = new SEMOSSEdge(service,data,"http://semoss.org/ontologies/Relation/Payload/Service:DataObject");
		SEMOSSEdge dataEleData = new SEMOSSEdge(dataElement,data,"http://semoss.org/ontologies/Relation/Payload/DataElement:DataObject");
		edgeStore.put(icdData.getURI(), icdData);
		edgeStore.put(serviceICD.getURI(), serviceICD);
		edgeStore.put(systemData.getURI(), systemData);
		edgeStore.put(icdSystem.getURI(), icdSystem);
		edgeStore.put(systemICD.getURI(), systemICD);
		edgeStore.put(serviceData.getURI(), serviceData);
		edgeStore.put(dataEleData.getURI(), dataEleData);
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
	
	/**
	 * Executes a query and stores the results.
	 * @param query String to run.
	 * @return ArrayList<Object []> that contains the results of the query.
	 */
	private ArrayList<Object []> executeQuery(String query) {
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		try{
			wrapper.executeQuery();	
		} catch (RuntimeException e){
			logger.error("Could not execute query: "+query);
		}

		// get the bindings from it
		String[] names = wrapper.getVariables();
		int count = 0;
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				
				Object [] values = new Object[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					values[colIndex] = getVariable(names[colIndex], sjss);
					logger.debug("Binding Name " + names[colIndex]);
					logger.debug("Binding Value " + values[colIndex]);
				}
				logger.debug("Creating new Value " + values);
				list.add(count, values);
				count++;
			}
		} catch (RuntimeException e) {
			logger.error("Could not store results for query: "+query);
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
			databaseRow[1] = ((Integer)databaseRow[1])*1.0 / totalNum;
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
