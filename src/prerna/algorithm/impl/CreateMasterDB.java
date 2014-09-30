package prerna.algorithm.impl;

import java.io.File;
import java.io.IOException;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
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
import prerna.rdf.engine.impl.InMemorySesameEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaUpdateWrapper;
import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class CreateMasterDB {
	private static final Logger logger = LogManager.getLogger(CreateMasterDB.class.getName());

	//ArrayList of database names
	ArrayList<String> engineList;
	//ArrayList of vertstores (one for each database)
	ArrayList<Hashtable<String, SEMOSSVertex>> vertStoreList;
	//ArrayList of edgestores (one for each database)
	ArrayList<Hashtable<String, SEMOSSEdge>> edgeStoreList;
	
	//setting for similarity threshold
	double similarityThresh = 0.7022;
	double maxSimilarity = 1;
	
	//variables for creating the db
	static final String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	static final String fileSeparator = System.getProperty("file.separator");
	String masterDBName = "MasterDatabase";
	String masterDBFileName="db" + fileSeparator + masterDBName;
	BigDataEngine masterEngine;

	//uri variables
	protected final static String semossURI = "http://semoss.org/ontologies";
	protected final static String semossConceptURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
	protected final static String semossRelationURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
	protected final static String masterConceptBaseURI = semossConceptURI+"/MasterConcept";
	protected final static String masterConceptConnectionBaseURI = semossConceptURI+"/MasterConceptConnection";
	protected final static String keywordBaseURI = semossConceptURI+"/Keyword";
	protected final static String engineBaseURI = semossConceptURI+"/Engine";
	protected final static String consistsOfRelationURI = semossRelationURI+"/ConsistsOf";
	protected final static String hasRelationURI = semossRelationURI+"/Has";
	protected final static String fromRelationURI = semossRelationURI+"/From";
	protected final static String toRelationURI = semossRelationURI+"/To";
	protected static final String keywordInsightBaseURI = semossRelationURI+"/Keyword:Insight";
	//for testing, including similarity as a property
	protected final static String propURI = semossRelationURI + "/" + "Contains";
	protected final static String similarityPropURI = propURI + "/" + "SimilarityScore";
	//protected final static String typeBaseURI = propURI + "/" + "Type";
			
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

		logger.info("Finished");
	}
	
	public void addEngine(String engineName) {
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName + "");
		
		GraphPlaySheet playSheet0 = createMetamodel(((AbstractEngine)engine).getBaseDataEngine().getRC());
		Hashtable<String, SEMOSSVertex> vertStore  = playSheet0.getGraphData().getVertStore();
		Hashtable<String, SEMOSSEdge> edgeStore = playSheet0.getGraphData().getEdgeStore();
		
		processConceptAndKeywords(engineName,vertStore);
		processRelations(engineName,edgeStore);
		
		RepositoryConnection rc = engine.getInsightDB();
		addInsights(rc);

		masterEngine.commit();
		masterEngine.infer();
		
		logger.info("Finished adding new engine "+engineName);
	}
	
	public void registerEngineAPI(String engineAPI) {
		//make sure the masterEngine has been set.
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		
		//need to get owl from from engineAPI
		try {
			Hashtable<String,String> engineHash = new Hashtable<String,String>();
			engineHash.put("key","ENGINE");
			String engineName = Utility.retrieveResult(engineAPI + "/getProperty", engineHash);
			
			String owl = Utility.retrieveResult(engineAPI + "/getOWLDefinition", null);
			RepositoryConnection owlRC = getNewRepository();
			owlRC.add(new StringBufferInputStream(owl), "http://semoss.org", RDFFormat.RDFXML);
			
			GraphPlaySheet playSheet0 = createMetamodel(owlRC);
			Hashtable<String, SEMOSSVertex> vertStore  = playSheet0.getGraphData().getVertStore();
			Hashtable<String, SEMOSSEdge> edgeStore = playSheet0.getGraphData().getEdgeStore();
			
			processConceptAndKeywords(engineName,vertStore);
			processRelations(engineName,edgeStore);
			
			String insights = Utility.retrieveResult(engineAPI + "/getInsightDefinition", null);
			RepositoryConnection insightsRC = getNewRepository();
			insightsRC.add(new StringBufferInputStream(insights), "http://semoss.org", RDFFormat.RDFXML);
			
			addWebInsights(engineName, engineAPI, insightsRC);

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
		}


		
		

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
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		AbstractEngine sesameEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)sesameEngine).setRepositoryConnection(rc);
		
		sesameEngine.setBaseData((RDFFileSesameEngine) sesameEngine);
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
				addRelationship(masterConceptBaseURI,masterConcept,keywordBaseURI,vertName,vertURI,consistsOfRelationURI);
				addRelationship(engineBaseURI,engineName,keywordBaseURI,vertName,vertURI,hasRelationURI);
				addProperty(consistsOfRelationURI +"/" + masterConcept + Constants.RELATION_URI_CONCATENATOR + vertName, similarityScore);
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
					addRelationship(masterConceptConnectionBaseURI,connectionInstanceURI,masterConceptBaseURI,masterConceptOut,fromRelationURI);
					addRelationship(masterConceptConnectionBaseURI,connectionInstanceURI,masterConceptBaseURI,masterConceptIn,toRelationURI);
					addRelationship(engineBaseURI,engineName,masterConceptConnectionBaseURI,connectionInstanceURI,hasRelationURI);
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
	
	private void addWebInsights(String engineName, String engineAPI, RepositoryConnection rc) {
		addInsights(rc);
		String engineURI = engineBaseURI + "/" + engineName;
		try {
			this.masterEngine.sc.addStatement( masterEngine.vf.createURI(engineURI), masterEngine.vf.createURI(propURI+"/EngineAPI"), masterEngine.vf.createLiteral(engineAPI));
		} catch (SailException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	
	private void addRelationship(String subjectBaseURI, String subjectInstanceName,String objectBaseURI,String objectInstanceName, String relationURI) throws EngineException {
		String objectNodeURI = objectBaseURI + "/" + objectInstanceName;
		addRelationship(subjectBaseURI,subjectInstanceName,objectBaseURI,objectInstanceName,objectNodeURI,relationURI);
	}
	
	/**
	 * Method to add a relationship to the MasterDatabase.
	 * @param subjectBaseURI	String representing the base URI for the subject
	 * @param subjectInstanceName	String representing the subject's instance name
	 * @param objectBaseURI	String representing the base URI for the object
	 * @param objectInstanceName	String representing the object's instance name
	 * @param relationURI	String representing the base URI for the relationship between the subject and object
	 * @throws EngineException	Thrown if statement cannot be added to the engine
	 */
	private void addRelationship(String subjectBaseURI, String subjectInstanceName,String objectBaseURI,String objectInstanceName, String objectNodeURI, String relationURI) throws EngineException {
		String subjectNodeURI = subjectBaseURI + "/" + subjectInstanceName;
		createStatement(masterEngine.vf.createURI(subjectNodeURI), RDF.TYPE, masterEngine.vf.createURI(subjectBaseURI));
		createStatement(masterEngine.vf.createURI(subjectNodeURI), RDFS.LABEL, masterEngine.vf.createLiteral(subjectInstanceName));
	
		createStatement(masterEngine.vf.createURI(objectNodeURI), RDF.TYPE, masterEngine.vf.createURI(objectBaseURI));
		createStatement(masterEngine.vf.createURI(objectNodeURI), RDFS.LABEL, masterEngine.vf.createLiteral(objectInstanceName));
		
		String relationInstanceURI = relationURI +"/" + subjectInstanceName + Constants.RELATION_URI_CONCATENATOR + objectInstanceName;
		createStatement(masterEngine.vf.createURI(relationInstanceURI), RDFS.SUBPROPERTYOF, masterEngine.vf.createURI(relationURI));
		createStatement(masterEngine.vf.createURI(relationInstanceURI), RDFS.LABEL, masterEngine.vf.createLiteral(subjectInstanceName + Constants.RELATION_URI_CONCATENATOR + objectInstanceName));
		createStatement(masterEngine.vf.createURI(subjectNodeURI), masterEngine.vf.createURI(relationInstanceURI), masterEngine.vf.createURI(objectNodeURI));
		logger.info("Added relationship: "+subjectInstanceName+" >>> " + relationURI + " >>>" + objectInstanceName);
	}
	
	/**
	 * Method to add similarity score property on the MasterConcept to Keyword relationship.
	 * For testing purposes, will likely be removed in final version.
	 * @param instanceURI	String containing the relationship to add the property to
	 * @param similarityScore	Similarity between the MasterConcept and Keyword
	 * @throws EngineException	Thrown if statement cannot be added to the engine
	 */
	public void addProperty(String instanceURI, Double similarityScore) throws EngineException {
		logger.info("Processing Similarity Score of " + similarityScore + " for " + instanceURI);
		createStatement(masterEngine.vf.createURI(similarityPropURI), RDF.TYPE, masterEngine.vf.createURI(propURI));	
		createStatement(masterEngine.vf.createURI(instanceURI), masterEngine.vf.createURI(similarityPropURI), masterEngine.vf.createLiteral(similarityScore));
		logger.info("Added relationship: "+instanceURI+" >>> " + similarityPropURI + " >>>" + similarityScore);
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

	/**
	 * Creates and adds the triple into the repository connection
	 * @param subject		URI for the subject of the triple
	 * @param predicate		URI for the predicate of the triple
	 * @param object		Value for the object of the triple, this param is not a URI since objects can be literals and literals do not have URIs
	 * @throws EngineException  
	 */
	private void createStatement(URI subject, URI predicate, Value object) throws EngineException {
		URI newSub;
		URI newPred;
		Value newObj;
		String subString;
		String predString;
		String objString;
		String sub = subject.stringValue().trim();
		String pred = predicate.stringValue().trim();

		subString = Utility.cleanString(sub, false);
		newSub = masterEngine.vf.createURI(subString);

		predString = Utility.cleanString(pred, false);
		newPred = masterEngine.vf.createURI(predString);

		if(object instanceof Literal) 
			newObj = object;
		else {
			objString = Utility.cleanString(object.stringValue(), false);
			newObj = masterEngine.vf.createURI(objString);
		}
		try {
			masterEngine.sc.addStatement(newSub, newPred, newObj);
		} catch (SailException e) {
			e.printStackTrace();
			throw new EngineException("Error adding triple {<" + newSub + "> <" + newPred + "> <" + newObj + ">}");
		}
	}
	
	private void createBaseRelations() throws EngineException{
		//Concept is a type of Class
		String typePred = RDF.TYPE.stringValue();
		String classURI = Constants.CLASS_URI;
		String propURI = Constants.DEFAULT_PROPERTY_URI;
		try {
			createStatement(masterEngine.vf.createURI(semossConceptURI), masterEngine.vf.createURI(typePred), masterEngine.vf.createURI(classURI));
			createStatement(masterEngine.vf.createURI(semossRelationURI), masterEngine.vf.createURI(typePred), masterEngine.vf.createURI(propURI));
		} catch (EngineException e) {
			logger.error("Could not add Concept is type of class triple and Relation is type of property triple");
			throw new EngineException("Error adding base relation triples");
		}
		
		//add node subclass triples
		String subclassPredicate = Constants.SUBCLASS_URI;
		createStatement(masterEngine.vf.createURI(masterConceptBaseURI), masterEngine.vf.createURI(subclassPredicate), masterEngine.vf.createURI(semossConceptURI));
		createStatement(masterEngine.vf.createURI(masterConceptConnectionBaseURI), masterEngine.vf.createURI(subclassPredicate), masterEngine.vf.createURI(semossConceptURI));
		createStatement(masterEngine.vf.createURI(keywordBaseURI), masterEngine.vf.createURI(subclassPredicate), masterEngine.vf.createURI(semossConceptURI));
		createStatement(masterEngine.vf.createURI(engineBaseURI), masterEngine.vf.createURI(subclassPredicate), masterEngine.vf.createURI(semossConceptURI));
		
		//add relation subproperty triples
		String subpropertypredicate = Constants.SUBPROPERTY_URI;
		createStatement(masterEngine.vf.createURI(consistsOfRelationURI), masterEngine.vf.createURI(subpropertypredicate), masterEngine.vf.createURI(semossRelationURI));
		createStatement(masterEngine.vf.createURI(hasRelationURI), masterEngine.vf.createURI(subpropertypredicate), masterEngine.vf.createURI(semossRelationURI));
		createStatement(masterEngine.vf.createURI(fromRelationURI), masterEngine.vf.createURI(subpropertypredicate), masterEngine.vf.createURI(semossRelationURI));
		createStatement(masterEngine.vf.createURI(toRelationURI), masterEngine.vf.createURI(subpropertypredicate), masterEngine.vf.createURI(semossRelationURI));
		createStatement(masterEngine.vf.createURI(keywordInsightBaseURI), masterEngine.vf.createURI(subpropertypredicate), masterEngine.vf.createURI(semossRelationURI));
		
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
