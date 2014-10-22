package prerna.algorithm.impl;

import java.io.File;
import java.io.IOException;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;
import org.openrdf.sail.memory.model.MemURI;

import prerna.algorithm.nlp.IntakePortal;
import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.poi.main.PropFileWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.engine.impl.SesameJenaUpdateWrapper;
import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.model.BigdataURIImpl;

public class CreateMasterDB extends ModifyMasterDB{
	//ArrayList of database names
	ArrayList<String> engineList;
	//ArrayList of vertstores (one for each database)
	ArrayList<Hashtable<String, SEMOSSVertex>> vertStoreList;
	//ArrayList of edgestores (one for each database)
	ArrayList<Hashtable<String, SEMOSSEdge>> edgeStoreList;
	
	//setting for similarity threshold
	double similarityThresh = 0.7022;
	double maxSimilarity = 1;
	
	private final static String userInsightQuery = "ASK WHERE {BIND(<http://semoss.org/ontologies/Concept/User/@USER@> AS ?User) BIND(<http://semoss.org/ontologies/Concept/Insight/@INSIGHT@> AS ?Insight) {?UserInsight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/UserInsight> ;} {?User <http://semoss.org/ontologies/Relation/PartOf> ?UserInsight} {?Insight <http://semoss.org/ontologies/Relation/PartOf> ?UserInsight}}";
	private final static String userInsightCountQuery = "SELECT ?UserInsight ?TimesClicked WHERE {BIND(<http://semoss.org/ontologies/Concept/User/@USER@> AS ?User) BIND(<http://semoss.org/ontologies/Concept/Insight/@INSIGHT@> AS ?Insight) {?UserInsight <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/UserInsight> ;} {?User <http://semoss.org/ontologies/Relation/PartOf> ?UserInsight} {?Insight <http://semoss.org/ontologies/Relation/PartOf> ?UserInsight} {?UserInsight <http://semoss.org/ontologies/Relation/Contains/TimesClicked> ?TimesClicked}}";
	private final static String serverExistsQuery = "SELECT DISTINCT ?Server WHERE {BIND('@BASEURI@' AS ?BaseURI){?Server <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Server>} {?Server <http://semoss.org/ontologies/Relation/Contains/BaseURI> ?BaseURI}} LIMIT 1";
	private final static String numServersQuery = "SELECT DISTINCT (COUNT(?Server) AS ?NumServers) WHERE {{?Server <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Server>}}";
	private final static String mcQuery = "SELECT DISTINCT ?mc ?keyword WHERE {{?mc <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}{?keyword <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Keyword>}{?mc <http://semoss.org/ontologies/Relation/ConsistsOf> ?keyword}}";
	
	//ArrayList of master concepts in master database
	ArrayList<String> masterConceptList = new ArrayList<String>();
	//ArrayList of keywordsLists (one for each master concept)
	ArrayList<ArrayList<String>> masterConceptKeyWordsList = new ArrayList<ArrayList<String>>();
	
	/**
	 * Creates a new master database from the engines and metamodels provided.
	 * Deletes the old master database if necessary.
	 */
	public void createDB() {
		Boolean newEngine = false;
		//check to see if the master engine is already running. If so, delete all exisiting triples.
		//If not running, create a new engine with SMSS file and open.
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		PropFileWriter propWriter = new PropFileWriter();
		if(masterEngine==null) {
			newEngine = true;
			masterEngine = new BigDataEngine();
			
			propWriter.setBaseDir(baseDirectory);
			try {
				propWriter.runWriter(masterDBName, "", "", "");
			} catch (FileReaderException e) {
				logger.error("File reader exception when trying to write propfile for new master engine");
			} catch (EngineException e) {
				logger.error("Engine exception when trying to write propfile for new master engine");
			}
	//		createSMSS();
			String smssFileName = baseDirectory + fileSeparator + "db" + fileSeparator + masterDBName + ".temp";
			masterEngine.openDB(smssFileName);
		}
		else {
			deleteTriplesInDB();
		}
		
		//create enginelist, vert store, edgestore for testing.
		//will ultimately be replaced with a way to input the metamodels.
		createTestingData();

		//process through the relations and add them to the db
		processConceptAndKeywords();
		processRelations();
		addInsights();
		
		try {
			createBaseRelations();
		} catch (EngineException e) {
			logger.error("Could not add base relation triples.");
		}
		
		masterEngine.commit();
		masterEngine.infer();
		
		//if creating a new engine, close it and allow File watcher to add it
		if(newEngine) {
			try{
				masterEngine.closeDB();
				logger.info("DB Closed");
			}catch(IllegalMonitorStateException e) {
				logger.info("Can't close DB");
			}
		}
		
		//if creating a new engine, recreate the temp file as smss.
		if(newEngine) {
			File propFile = new File(propWriter.propFileName);
			File newProp = new File(propWriter.propFileName.replace("temp", "smss"));
			try {
				FileUtils.copyFile(propFile, newProp);
			} catch (IOException e) {
				logger.error("Could not create .smss file for new database");
			}
			propFile.delete();
		}		

		logger.info("Finished creating database");
	}
	
	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * @param userName	String representing the name of the user to add
	 */
	public void addUser(String userName) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		try {
			addNode(userBaseURI,userName);
		} catch (EngineException e) {
			logger.error("Could not add user to master database ::: " + userName);
		}
		masterEngine.commit();
		masterEngine.infer();
		logger.info("Finished adding new user "+userName);
	}
	
	/**
	 * Increase the count of the number of times a user has selected on an insight.
	 * Assuming that the insight is the instance of the uri, so is in the following format:
	 * Database:Perspective:QuestionKey
	 * e.g. TAP_Core_Data:System-Perspective:SysP1
	 * @param userName	String representing the user
	 * @param insight	String representing the insight clicked on
	 */
	public void increaseInsightCount(String userName, String insight) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		//if the UserInsight concat node is not in the database, add it and the relationships.
		//set the count on the UserInsight concat node to be one.
		//if the node exists, get the count. delete the old count. replace it with count+1.
		String userInsightExistsFilled = userInsightQuery.replaceAll("@USER@",userName).replaceAll("@INSIGHT@", insight);
		BooleanProcessor proc = new BooleanProcessor();
		proc.setQuery(userInsightExistsFilled);
		boolean concatExists = proc.processQuery();
		if(concatExists){ 
			String userInsightCountFilled = userInsightCountQuery.replaceAll("@USER@",userName).replaceAll("@INSIGHT@", insight);
			SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,userInsightCountFilled);
			String[] names = wrapper.getVariables();
			while(wrapper.hasNext())
			{
				//get count
				SesameJenaSelectStatement sjss = wrapper.next();
				Object userIn = sjss.getRawVar(names[0]);
				Double count = (Double)sjss.getVar(names[1]);
				//delete old count and add new count + 1.
				if(userIn instanceof BigdataURIImpl) {
					BigdataURIImpl userInsight = ((BigdataURIImpl)userIn);
					try {
						masterEngine.sc.removeStatements(userInsight, masterEngine.vf.createURI(timesClickedPropURI), masterEngine.vf.createLiteral(count));
						count++;
						masterEngine.sc.addStatement(userInsight, masterEngine.vf.createURI(timesClickedPropURI), masterEngine.vf.createLiteral(count));
						logger.debug("Count increased for user insight ::: " + userIn.toString() + " ::: " + count);
					} catch (SailException e) {
						logger.error("Could not increase count for user insight ::: " + userIn.toString());
					} 
				}
			}
		}else {
			String userInsightInstance = userName + Constants.RELATION_URI_CONCATENATOR + insight;
			try {
				String userInsightURI = userInsightBaseURI + "/" + userInsightInstance;
				addNode(userInsightBaseURI,userInsightInstance);
				addRelationship(userBaseURI + "/" + userName,userInsightURI,userUserInsightBaseURI,userName + Constants.RELATION_URI_CONCATENATOR + userInsightInstance);
				addRelationship(insightBaseURI + "/" + insight,userInsightURI,insightUserInsightBaseURI,insight + Constants.RELATION_URI_CONCATENATOR + userInsightInstance);
				addProperty(userInsightURI,timesClickedPropURI,1.0);
			} catch (EngineException e) {
				logger.error("Could not create UserInsight node with count of 1 for user insight ::: " + userInsightInstance);
			}
		}
		masterEngine.commit();
		masterEngine.infer();
	}
	
	public void addEngine(String engineName) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName + "");
		
		GraphPlaySheet playSheet0 = createMetamodel(((AbstractEngine)engine).getBaseDataEngine().getRC());
		Hashtable<String, SEMOSSVertex> vertStore  = playSheet0.getGraphData().getVertStore();
		Hashtable<String, SEMOSSEdge> edgeStore = playSheet0.getGraphData().getEdgeStore();
		//fill masterConceptList with everything that is already in the database
		if(masterConceptList.isEmpty()) {
			SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,mcQuery);
			String[] names = wrapper.getVariables();
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				String mc = (String)sjss.getVar(names[0]);
				String keyword = (String)sjss.getVar(names[1]);
				int mcInd = masterConceptList.indexOf(mc);
				if(mcInd>-1) {
					ArrayList<String> keywords = masterConceptKeyWordsList.get(mcInd);
					keywords.add(keyword);
					masterConceptKeyWordsList.set(mcInd, keywords);
				} else {
					ArrayList<String> keywords = new ArrayList<String>();
					keywords.add(keyword);
					masterConceptList.add(mc);
					masterConceptKeyWordsList.add(keywords);
				}
			}
		}
		processConceptAndKeywords(engineName,vertStore);
		processRelations(engineName,edgeStore);
		
		RepositoryConnection rc = engine.getInsightDB();
		addInsights(rc);

		masterEngine.commit();
		masterEngine.infer();
		
		logger.info("Finished adding new engine "+engineName);
	}
	
	/**
	 * TODO change how we make the server name?
	 * @return
	 */
	private String createServerName() {
		String countQuery = numServersQuery;
		SesameJenaSelectWrapper wrapper2 = Utility.processQuery(masterEngine,countQuery);

		String[] names2 = wrapper2.getVariables();
		while(wrapper2.hasNext()) {
			SesameJenaSelectStatement sjss = wrapper2.next();
			Double count = (Double)sjss.getVar(names2[0]);
			return (count + 1) + "";
		}
		return 1+"";
	}
	
	private void addEngineToServer(String baseURI,String engineName) throws EngineException{
		//check to see if the base URI already exists in the master db.
		//if it does, get the corresponding server name
		//if not, make up a server name (incremement 1)
		//add the engine to the corresponding server		
		String existsQuery = serverExistsQuery.replaceAll("@BASEURI@", baseURI);
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,existsQuery);

		String server = "";
		String serverNodeURI = "";
		try {
			String[] names = wrapper.getVariables();
			while(wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				server = (String)sjss.getVar(names[0]);
				serverNodeURI = serverBaseURI + "/" + server;
			}
			if(server=="") {

				server = createServerName();
				//add server type triple. add server label
				serverNodeURI = serverBaseURI + "/" + server;
				masterEngine.addStatement(serverNodeURI, RDF.TYPE.stringValue(), serverBaseURI, true);
				masterEngine.addStatement(serverNodeURI, RDFS.LABEL.stringValue(), server, false);
				//add base uri as property to server
				try {
					masterEngine.sc.addStatement(masterEngine.vf.createURI(serverNodeURI), masterEngine.vf.createURI(baseURIPropURI), masterEngine.vf.createLiteral(baseURI));
				} catch (SailException e) {
					logger.error("Could not add baseURI for engine"+engineName);
				}
			}
			//add server to engine triple
			addNodeAndRelationship(engineBaseURI,engineName,serverBaseURI,server,engineServerBaseURI);
		} catch (EngineException e) {
			logger.error("Could not add engine to server relations for engine" + engineName);
			throw new EngineException();
		}
	}
	
	public String registerEngineAPI(String baseURI, String engineName) {
		//make sure the masterEngine has been set.
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		
		try {
			//TODO different servers cannot have the same engine name correct? If they can, this will be an issue...
			// assuming that the engine name does not already exist for this server TODO do we check for this previously?
			addEngineToServer(baseURI,engineName);
			String engineAPI = baseURI + "/s-"+engineName;
			String owl = Utility.retrieveResult(engineAPI + "/getOWLDefinition", null);
			RepositoryConnection owlRC = getNewRepository();
			owlRC.add(new StringBufferInputStream(owl), "http://semoss.org", RDFFormat.RDFXML);
			
			GraphPlaySheet playSheet0 = createMetamodel(owlRC);
			Hashtable<String, SEMOSSVertex> vertStore  = playSheet0.getGraphData().getVertStore();
			Hashtable<String, SEMOSSEdge> edgeStore = playSheet0.getGraphData().getEdgeStore();
			//fill masterConceptList with everything that is already in the database
			if(masterConceptList.isEmpty()) {
				SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,mcQuery);
				String[] names = wrapper.getVariables();
				while(wrapper.hasNext())
				{
					SesameJenaSelectStatement sjss = wrapper.next();
					String mc = (String)sjss.getVar(names[0]);
					String keyword = (String)sjss.getVar(names[1]);
					int mcInd = masterConceptList.indexOf(mc);
					if(mcInd>-1) {
						ArrayList<String> keywords = masterConceptKeyWordsList.get(mcInd);
						keywords.add(keyword);
						masterConceptKeyWordsList.set(mcInd, keywords);
					} else {
						ArrayList<String> keywords = new ArrayList<String>();
						keywords.add(keyword);
						masterConceptList.add(mc);
						masterConceptKeyWordsList.add(keywords);
					}
				}
			}
			processConceptAndKeywords(engineName,vertStore);
			processRelations(engineName,edgeStore);
			
			String insights = Utility.retrieveResult(engineAPI + "/getInsightDefinition", null);
			RepositoryConnection insightsRC = getNewRepository();
			insightsRC.add(new StringBufferInputStream(insights), "http://semoss.org", RDFFormat.RDFXML);
			
			addInsights(insightsRC);
			
			addUser("Anonymous");

			masterEngine.commit();
			masterEngine.infer();
			
			logger.info("Finished adding new engine "+engineName);
			
		} catch (RDFParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (EngineException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return "success";

	}
	
	public RepositoryConnection getNewRepository() {
		try {
			RepositoryConnection rc = null;
			Repository myRepository = new SailRepository(
					new ForwardChainingRDFSInferencer(new MemoryStore()));
			myRepository.initialize();
			rc = myRepository.getConnection();
			return rc;
		} catch (RepositoryException e) {
			logger.error("Could not get a new repository");
		}
		return null;
	}

	private void createTestingData() {
		//create enginelist, vert store, edgestore for testing;
		engineList = new ArrayList<String>();
		engineList.add("TAP_Core_Data");
		engineList.add("TAP_Site_Data");
		engineList.add("TAP_Cost_Data");
		engineList.add("TAP_Functional_Data");
		engineList.add("HR_Core");
		engineList.add("TAP_Services_Data");
		engineList.add("TAP_Portfolio");
		engineList.add("VendorSelection");
		
		
		vertStoreList = new ArrayList<Hashtable<String, SEMOSSVertex>>();
		edgeStoreList = new ArrayList<Hashtable<String, SEMOSSEdge>>();
		
		for(int i=0;i<engineList.size();i++) {
			String engineName = engineList.get(i);
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName + "");
			GraphPlaySheet playSheet0 = createMetamodel(((AbstractEngine)engine).getBaseDataEngine().getRC());
			vertStoreList.add(playSheet0.getGraphData().getVertStore());
			edgeStoreList.add(playSheet0.getGraphData().getEdgeStore());
		}
	}
	
	/**
	 * Creates the GraphPlaySheet for a database that shows the metamodel.
	 * @param engine IEngine to create the metamodel from
	 * @return GraphPlaySheet that displays the metamodel
	 */
	private GraphPlaySheet createMetamodel(RepositoryConnection rc){
		ExecuteQueryProcessor exQueryProcessor = new ExecuteQueryProcessor();
		//hard code playsheet attributes since no insight exists for this
		String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
		String playSheetName = "prerna.ui.components.playsheets.GraphPlaySheet";
		RDFFileSesameEngine sesameEngine = new RDFFileSesameEngine();
		sesameEngine.setRC(rc);
		sesameEngine.setEngineName("Metamodel Engine");
		
		sesameEngine.setBaseData(sesameEngine);
		Hashtable<String, String> filterHash = new Hashtable<String, String>();
		filterHash.put("http://semoss.org/ontologies/Relation", "http://semoss.org/ontologies/Relation");
		sesameEngine.setBaseHash(filterHash);
		
		exQueryProcessor.prepareQueryOutputPlaySheet(sesameEngine, sparql, playSheetName, "", "");

		GraphPlaySheet playSheet= (GraphPlaySheet) exQueryProcessor.getPlaySheet();
		playSheet.getGraphData().setSubclassCreate(true);//this makes the base queries use subclass instead of type--necessary for the metamodel query
		playSheet.createData();
		playSheet.runAnalytics();

		return playSheet;
	}
	
	private void processConceptAndKeywords() {
		for(int engineInd = 0;engineInd < engineList.size();engineInd++) {
			String engineName = engineList.get(engineInd);
			Hashtable<String, SEMOSSVertex> vertStore = vertStoreList.get(engineInd);
			processConceptAndKeywords(engineName,vertStore);
		}
	}

	/**
	 * Creates and adds the master concepts, keywords, and databases nodes to the master database.
	 * Loops through every database's keywords and checks to see if there is a similar master concept already in the master database.
	 * If there is, add triples for the keyword and database to that master concept.
	 * If there is not, add the master concept and then add triples for the keyword and database.
	 */
	private void processConceptAndKeywords(String engineName,Hashtable<String, SEMOSSVertex> vertStore) {
		Iterator<SEMOSSVertex> vertItr = vertStore.values().iterator();
		while(vertItr.hasNext()) {
			SEMOSSVertex vert = vertItr.next();
			String vertName = (String)vert.getProperty(Constants.VERTEX_NAME);
			String vertURI = (String)vert.getURI();
			boolean found = false;
			int mcInd=0;
			Double similarityScore = 0.0;
			String masterConcept = "";
			while(mcInd<masterConceptList.size()&&found==false) {
				masterConcept = masterConceptList.get(mcInd);
				similarityScore = IntakePortal.DBRecSimScore(vertName,masterConcept);
				if(similarityScore>similarityThresh)
					found=true;
				mcInd++;
			}
			ArrayList<String> keywordsList;
			if(found) {
				keywordsList = masterConceptKeyWordsList.get(mcInd-1);
				keywordsList.add(vertName);
				masterConceptKeyWordsList.set(mcInd-1,keywordsList);
			} else {
				masterConcept = vertName;
				masterConceptList.add(masterConcept);
				keywordsList = new ArrayList<String>();
				keywordsList.add(vertName);
				masterConceptKeyWordsList.add(keywordsList);
				similarityScore = maxSimilarity; //Set for initial mapping of MasterConcept
			}
			
			try {
				addNodeAndRelationship(mcBaseURI,masterConcept,mcBaseURI + "/" + masterConcept,keywordBaseURI,vertName,vertURI,mcKeywordBaseURI);
				addNodeAndRelationship(engineBaseURI,engineName,engineBaseURI + "/" + engineName,keywordBaseURI,vertName,vertURI,engineKeywordBaseURI);
				addProperty(mcKeywordBaseURI +"/" + masterConcept + Constants.RELATION_URI_CONCATENATOR + vertName, similarityPropURI, similarityScore);
			} catch(EngineException e) {
				logger.error("Triple was not added");
			}
		}
	}

	private void processRelations() {
		for(int engineInd = 0;engineInd < engineList.size();engineInd++) {
			String engineName = engineList.get(engineInd);
			Hashtable<String, SEMOSSEdge> edgeStore = edgeStoreList.get(engineInd);
			processRelations(engineName,edgeStore);
		}
	}
	
	/**
	 * Creates and adds relationships between master concepts. For each relationship that exists in individual databases,
	 * a relationship is added between the master concepts that correspond to the nodes in the individual database.
	 * The relationships are tagged by the individual database they correspond to.
	 * Loops through every database's edges. Finds the master concepts for each of the nodes.
	 * Add triples for a new relationship between the master concepts in the master database.
	 * Add triples to relate the new relationship to the database.
	 */
	private void processRelations(String engineName,Hashtable<String, SEMOSSEdge> edgeStore) {		
		Iterator<SEMOSSEdge> edgeItr = edgeStore.values().iterator();
		while(edgeItr.hasNext()) {
			SEMOSSEdge edge = edgeItr.next();
//				String edgeName = (String)edge.getProperty(Constants.EDGE_NAME);
			String vertIn = (String)edge.inVertex.getProperty(Constants.VERTEX_NAME);
			String vertOut = (String)edge.outVertex.getProperty(Constants.VERTEX_NAME);
			String masterConceptIn = findMasterConceptForKeyWord(vertIn);
			String masterConceptOut = findMasterConceptForKeyWord(vertOut);
			if(masterConceptIn.equals("") || masterConceptOut.equals("")) {
				logger.error("Cannot add edge for database "+engineName+" between "+masterConceptIn+" and "+masterConceptOut);
			}else {
				try {
					String connectionInstanceURI = masterConceptOut + Constants.RELATION_URI_CONCATENATOR + masterConceptIn;
					addNodeAndRelationship(mccBaseURI,connectionInstanceURI,mcBaseURI,masterConceptOut,mccFromMCBaseURI);
					addNodeAndRelationship(mccBaseURI,connectionInstanceURI,mcBaseURI,masterConceptIn,mccToMCBaseURI);
					addNodeAndRelationship(engineBaseURI,engineName,mccBaseURI,connectionInstanceURI,engineMCCBaseURI);
				} catch (EngineException e) {
					logger.error("Triple was not added");
				}
			}
		}
	}
	
	private void addInsights() {
		for(int engineInd=0;engineInd<engineList.size();engineInd++) {
			String engineName = engineList.get(engineInd);
			BigDataEngine engine =  (BigDataEngine) DIHelper.getInstance().getLocalProp(engineName + "");
			RepositoryConnection rc = engine.getInsightDB();
			addInsights(rc);
		}
	}
	
	private void addInsights(RepositoryConnection rc) {
		try {
			RepositoryResult<Statement> results = rc.getStatements(null, null, null, true);
			while(results.hasNext()) {
				Statement s = results.next();
				this.masterEngine.sc.addStatement(s.getSubject(),s.getPredicate(),s.getObject(),s.getContext());
				if(s.getPredicate().toString().equals("PARAM:TYPE")) {
					if(s.getObject() instanceof MemURI) {
						MemURI keyword = (MemURI) s.getObject();
						this.masterEngine.sc.addStatement(keyword, RDF.TYPE, masterEngine.vf.createURI(keywordBaseURI));
					}
					else {
						logger.info("error adding param to keyword relationship for "+s.getSubject().stringValue()+">>>"+s.getPredicate().stringValue()+">>>"+s.getObject().stringValue());
					}
				}
			}
		} catch (RepositoryException e) {
			logger.info("Repository Error adding insights");
		} catch (SailException e) {
			e.printStackTrace();
			logger.info("Sail Error adding insights");
		}
	}
	
	/**
	 * Method to add both nodes and the relationship to the MasterDatabase.
	 * @param subjectBaseURI	String representing the base URI for the subject
	 * @param subjectInstance	String representing the subject's instance name
	 * @param objectBaseURI	String representing the base URI for the object
	 * @param objectInstance	String representing the object's instance name
	 * @param relationBaseURI	String representing the base URI for the relationship between the subject and object
	 * @throws EngineException	Thrown if statement cannot be added to the engine
	 */
	private void addNodeAndRelationship(String subjectBaseURI, String subjectInstance,String objectBaseURI,String objectInstance, String relationBaseURI) throws EngineException {
		String subjectNodeURI = subjectBaseURI + "/" + subjectInstance;
		String objectNodeURI = objectBaseURI + "/" + objectInstance;
		addNodeAndRelationship(subjectBaseURI,subjectInstance,subjectNodeURI,objectBaseURI,objectInstance,objectNodeURI,relationBaseURI);
	}
	
	/**
	 * Method to add both nodes and the relationship to the MasterDatabase.
	 * @param subjectBaseURI	String representing the base URI for the subject
	 * @param subjectInstance	String representing the subject's instance name
	 * @param subjectURI	String representing the subject's full URI
	 * @param objectBaseURI	String representing the base URI for the object
	 * @param objectInstance	String representing the object's instance name
	 * @param objectURI	String representing the object's full URI
	 * @param relationBaseURI	String representing the base URI for the relationship between the subject and object
	 * @throws EngineException	Thrown if statement cannot be added to the engine
	 */
	private void addNodeAndRelationship(String subjectBaseURI, String subjectInstance,String subjectURI,String objectBaseURI,String objectInstance, String objectURI, String relationBaseURI) throws EngineException {
		String relationInstance = subjectInstance + Constants.RELATION_URI_CONCATENATOR + objectInstance;
		String relationURI = relationBaseURI +"/" + relationInstance;
		addNodeAndRelationship(subjectBaseURI,subjectInstance,subjectURI,objectBaseURI,objectInstance,objectURI,relationBaseURI,relationInstance,relationURI);
	}
	
	/**
	 * Method to add both nodes and the relationship to the MasterDatabase.
	 * @param subjectBaseURI	String representing the base URI for the subject
	 * @param subjectInstance	String representing the subject's instance name
	 * @param objectBaseURI	String representing the base URI for the object
	 * @param objectInstance	String representing the object's instance name
	 * @param relationBaseURI	String representing the base URI for the relationship between the subject and object
	 * @param relationBaseURI	String representing the instance name for the relationship between the subject and object
	 * @param relationURI	String representing the URI for the relationship between the subject and object
	 * @throws EngineException	Thrown if statement cannot be added to the engine
	 */
	private void addNodeAndRelationship(String subjectBaseURI, String subjectInstance,String subjectURI,String objectBaseURI,String objectInstance, String objectURI, String relationBaseURI, String relationInstance, String relationURI) throws EngineException {
		addNode(subjectBaseURI,subjectInstance,subjectURI);
		addNode(objectBaseURI,objectInstance,objectURI);
		addRelationship(subjectURI,objectURI,relationBaseURI,relationInstance,relationURI);
	}
		
	/**
	 * Adds just the relationship given a full subject URI and the instance name, full object URI and instance name, and relation base uri
	 * @param subjectURI	String representing the full subject URI e.g. http://semoss.org/ontologies/Concept/User/ksmart
	 * @param objectURI	String representing the full object URI i.e. http://semoss.org/ontologies/Concept/Engine/TAP_Core_Data
	 * @param relationBaseURI	String representing the base URI of the relationship http://semoss.org/ontologies/Relation/Creates
	 * @param relationInstance	String representing the instance of the relationship ksmart:TAP_Core_Data
	 * @throws EngineException
	 */
	private void addRelationship(String subjectURI, String objectURI, String relationBaseURI,String relationInstance) throws EngineException{
		String relationURI = relationBaseURI + "/" + relationInstance;
		addRelationship(subjectURI,objectURI,relationBaseURI,relationInstance,relationURI);
	}

	/**
	 * Adds just the relationship given a full subject URI and the instance name, full object URI and instance name, and relation base uri
	 * @param subjectURI	String representing the full subject URI e.g. http://semoss.org/ontologies/Concept/User/ksmart
	 * @param objectURI	String representing the full object URI i.e. http://semoss.org/ontologies/Concept/Engine/TAP_Core_Data
	 * @param relationBaseURI	String representing the base URI of the relationship http://semoss.org/ontologies/Relation/Creates
	 * @param relationInstance	String representing the instance of the relationship ksmart:TAP_Core_Data
	 * @param relationURI	String representing the full URI of the relationship http://semoss.org/ontologies/Relation/Creates/ksmart:TAP_Core_Data
	 * @throws EngineException
	 */
	private void addRelationship(String subjectURI, String objectURI, String relationBaseURI, String relationInstance, String relationURI) throws EngineException{
		masterEngine.addStatement(relationURI, RDFS.SUBPROPERTYOF.stringValue(), relationBaseURI,true);
		masterEngine.addStatement(relationURI, RDFS.LABEL.stringValue(), relationInstance,false);
		masterEngine.addStatement(subjectURI, relationURI, objectURI,true);
		logger.info("Added relationship: "+subjectURI+" >>> " + relationURI + " >>>" + objectURI);
	}
	
	/**
	 * Adds a node given a baseURI and the instance name.
	 * Defaults the URI to be the concatenation of baseURI / instanceName
	 * @param baseURI	String representing the URI for the node type. e.g. http://semoss.org/ontologies/Concept/Engine
	 * @param instance	String representing the name of the instance to add e.g. TAP_Core_Data
	 * @throws EngineException
	 */
	private void addNode(String baseURI, String instance) throws EngineException{
		String nodeURI = baseURI + "/" + instance;
		addNode(baseURI,instance,nodeURI);
	}
	
	/**
	 * Adds a node given a baseURI, instance name, and the URI.
	 * @param baseURI	String representing the URI for the node type. e.g. http://semoss.org/ontologies/Concept/Engine
	 * @param instance	String representing the name of the instance to add e.g. TAP_Core_Data
	 * @param nodeURI	String representing the URI for the node e.g. http://semoss.org/ontologies/Concept/Engine/TAP_Core_Data
	 * @throws EngineException
	 */
	private void addNode(String baseURI, String instance, String nodeURI) throws EngineException{
		masterEngine.addStatement(nodeURI, RDF.TYPE.stringValue(), baseURI, true);
		masterEngine.addStatement(nodeURI, RDFS.LABEL.stringValue(), instance, false);
	}
	
	/**
	 * Method to add property on an instance.
	 * @param instanceURI	String containing the node or relationship URI to add the property to
	 * @param propBaseURI	String representing the URI of the property relation
	 * @param value	Value to add as the property
	 * @throws EngineException	Thrown if statement cannot be added to the engine
	 */
	public void addProperty(String instanceURI, String propBaseURI, Double value) throws EngineException {
		masterEngine.addStatement(instanceURI, propBaseURI, value, false);
		logger.info("Added property: "+instanceURI+" >>> " + propBaseURI + " >>>" + value);
	}
	
	/**
	 * Finds and returns the master concept for any keyword
	 * @param keyWord	String representing the keyword in a database
	 * @return String representing the master concept the keyword is similar to in the master database.
	 */
	private String findMasterConceptForKeyWord(String keyWord) {
		for(int mcInd = 0;mcInd<masterConceptList.size();mcInd++) {
			String masterConcept = masterConceptList.get(mcInd);
			ArrayList<String> keyWordsList = masterConceptKeyWordsList.get(mcInd);
			if(keyWordsList.contains(keyWord)) {
				return masterConcept;
			}
		}
		logger.error("Master Concept not found for keyword "+keyWord);
		return "";
	}
	
	private void createBaseRelations() throws EngineException{
		//Concept is a type of Class
		String typePred = RDF.TYPE.stringValue();
		String classURI = Constants.CLASS_URI;
		String propURI = Constants.DEFAULT_PROPERTY_URI;
		masterEngine.addStatement(semossConceptURI, typePred, classURI, true);
		masterEngine.addStatement(semossRelationURI, typePred, propURI, true);
		
		//add node subclass triples
		String subclassPredicate = Constants.SUBCLASS_URI;
		masterEngine.addStatement(mcBaseURI, subclassPredicate, semossConceptURI, true);
		masterEngine.addStatement(mccBaseURI, subclassPredicate, semossConceptURI, true);
		masterEngine.addStatement(keywordBaseURI, subclassPredicate, semossConceptURI, true);
		masterEngine.addStatement(engineBaseURI, subclassPredicate, semossConceptURI, true);
		masterEngine.addStatement(serverBaseURI, subclassPredicate, semossConceptURI, true);
		masterEngine.addStatement(userBaseURI, subclassPredicate, semossConceptURI, true);
		masterEngine.addStatement(userInsightBaseURI, subclassPredicate, semossConceptURI, true);

		//add relation subproperty triples
		String subpropertypredicate = Constants.SUBPROPERTY_URI;
		masterEngine.addStatement(mcKeywordBaseURI, subpropertypredicate, semossRelationURI, true);
		masterEngine.addStatement(engineKeywordBaseURI, subpropertypredicate, semossRelationURI, true);
		masterEngine.addStatement(engineMCCBaseURI, subpropertypredicate, semossRelationURI, true);
		masterEngine.addStatement(mccFromMCBaseURI, subpropertypredicate, semossRelationURI, true);
		masterEngine.addStatement(mccToMCBaseURI, subpropertypredicate, semossRelationURI, true);
		masterEngine.addStatement(engineServerBaseURI, subpropertypredicate, semossRelationURI, true);
		masterEngine.addStatement(userUserInsightBaseURI, subpropertypredicate, semossRelationURI, true);
		masterEngine.addStatement(insightUserInsightBaseURI, subpropertypredicate, semossRelationURI, true);

		//add property triples
		masterEngine.addStatement(similarityPropURI, RDF.TYPE.stringValue(), propURI, true);	
		masterEngine.addStatement(baseURIPropURI, RDF.TYPE.stringValue(), propURI, true);
		masterEngine.addStatement(timesClickedPropURI, RDF.TYPE.stringValue(), propURI, true);
	}
	
	/**
	 * Deletes the old master database
	 */
	public void deleteTriplesInDB() {
		logger.info("Deleting all triples in Master Database.");
		String delQuery = "DELETE {?s ?p ?o} WHERE { {?s ?p ?o}}";
		
		//create the update wrapper, set the variables, and let it run
		SesameJenaUpdateWrapper wrapper = new SesameJenaUpdateWrapper();
		wrapper.setEngine(masterEngine);
		wrapper.setQuery(delQuery);
		wrapper.execute();
		masterEngine.commit();
		logger.info("All triples in Master Database deleted.");
	}	
}
