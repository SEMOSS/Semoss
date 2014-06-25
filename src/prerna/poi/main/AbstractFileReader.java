package prerna.poi.main;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.memory.MemoryStore;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

public abstract class AbstractFileReader {

	static String propFile; // the file that serves as the property file
	Hashtable<String, String> rdfMap = new Hashtable<String, String>();
	String bdPropFile;
	Properties bdProp = new Properties(); // properties for big data
	Sail bdSail;
	ValueFactory vf;
	public SailConnection sc;public String semossURI;
	String customBaseURI = "";
	public static String CONTAINS = "Contains";
	public Hashtable<String,String> baseConceptURIHash = new Hashtable<String,String>(); 
	public Hashtable<String,String> conceptURIHash = new Hashtable<String,String>();
	public Hashtable<String,String> baseRelationURIHash = new Hashtable<String,String>(); 
	public Hashtable<String,String> relationURIHash = new Hashtable<String,String>();
	public Hashtable<String,String> basePropURIHash = new Hashtable<String,String>();
	Hashtable<String, String[]> baseRelations = new Hashtable<String, String[]>();
	public String basePropURI= "";
	Logger logger = Logger.getLogger(getClass());

	// OWL variables
	RepositoryConnection rcOWL;
	ValueFactory vfOWL;
	SailConnection scOWL;
	String owlFile = "";
	
	//reload base data
	RDFFileSesameEngine baseDataEngine;
	Hashtable baseDataHash = new Hashtable();
	
	/**
	 * Creates and adds the triple into the repository connection
	 * @param subject		URI for the subject of the triple
	 * @param predicate		URI for the predicate of the triple
	 * @param object		Value for the object of the triple, this param is not a URI since objects can be literals and literals do not have URIs
	 */
	protected void createStatement(URI subject, URI predicate, Value object) throws Exception
	{
		URI newSub;
		URI newPred;
		Value newObj;
		String subString;
		String predString;
		String objString;
		String sub = subject.stringValue().trim();
		String pred = predicate.stringValue().trim();

		subString = Utility.cleanString(sub, false);
		newSub = vf.createURI(subString);

		predString = Utility.cleanString(pred, false);
		newPred = vf.createURI(predString);

		if(object instanceof Literal) 
			newObj = object;
		else {
			objString = Utility.cleanString(object.stringValue(), false);
			newObj = vf.createURI(objString);
		}
		sc.addStatement(newSub, newPred, newObj);
	}

	/**
	 * Creates the database based on the engine properties 
	 */
	private void openDB() throws Exception {
		// create database based on engine properties
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String fileName = baseFolder + "/" + bdProp.getProperty("com.bigdata.journal.AbstractJournal.file");
		bdProp.put("com.bigdata.journal.AbstractJournal.file", fileName);
		bdSail = new BigdataSail(bdProp);
		Repository repo = new BigdataSailRepository((BigdataSail) bdSail);
		repo.initialize();
		SailRepositoryConnection src = (SailRepositoryConnection) repo.getConnection();
		sc = src.getSailConnection();
		vf = bdSail.getValueFactory();
	}

	/**
	 * Loading engine properties in order to create the database 
	 * @param fileName String containing the fileName of the temp file that contains the information of the smss file
	 */
	private void loadBDProperties(String fileName) throws Exception
	{
		InputStream fis = new FileInputStream(fileName);
		bdProp.load(fis);
		fis.close();
	}

	/**
	 * Loads the prop file for the CSV file
	 * @param fileName	Absolute path to the prop file specified in the last column of the CSV file
	 */
	protected void openProp(String fileName) throws Exception
	{
		Properties rdfPropMap = new Properties();
		rdfPropMap.load(new FileInputStream(fileName));
		for(String name: rdfPropMap.stringPropertyNames()){
			rdfMap.put(name, rdfPropMap.getProperty(name).toString());
		}
	}

	/**
	 * Close the database engine
	 */
	public void closeDB() throws Exception
	{
		logger.warn("Closing....");
		commitDB();
		sc.close();
		bdSail.shutDown();
	}	
	
	protected void commitDB() throws SailException{
		sc.commit();
		InferenceEngine ie = ((BigdataSail)bdSail).getInferenceEngine();
		ie.computeClosure(null);
		sc.commit();
	}

	/**
	 * Creates a repository connection to be put all the base relationship data to create the OWL file
	 */
	private void openOWLWithOutConnection() throws RepositoryException
	{
		Repository myRepository = new SailRepository(new MemoryStore());
		myRepository.initialize();
		rcOWL = myRepository.getConnection();
		scOWL = ((SailRepositoryConnection) rcOWL).getSailConnection();
		vfOWL = rcOWL.getValueFactory();
	}

	/**
	 * Creates a repository connection and puts all the existing base relationships to create an updated OWL file
	 * @param engine	The database engine used to get all the existing base relationships
	 */
	private void openOWLWithConnection(IEngine engine) throws RepositoryException
	{
		Repository myRepository = new SailRepository(new MemoryStore());
		myRepository.initialize();
		rcOWL = myRepository.getConnection();
		scOWL = ((SailRepositoryConnection) rcOWL).getSailConnection();
		vfOWL = rcOWL.getValueFactory();

		baseDataEngine = ((AbstractEngine)engine).getBaseDataEngine();
		baseDataHash = ((AbstractEngine)engine).getBaseHash();
		
		RepositoryConnection existingRC = ((RDFFileSesameEngine) baseDataEngine).getRc();
		// load pre-existing base data
		RepositoryResult<Statement> rcBase = existingRC.getStatements(null, null, null, false);
		List<Statement> rcBaseList = rcBase.asList();
		Iterator<Statement> iterator = rcBaseList.iterator();
		while(iterator.hasNext()){
			logger.info(iterator.next());
		}
		rcOWL.add(rcBaseList);
	}

	/**
	 * Close the OWL engine
	 */
	protected void closeOWL() throws Exception {
		scOWL.close();
		rcOWL.close();
	}
	
	protected void storeBaseStatement(String sub, String pred, String obj){
		try {
			scOWL.addStatement(vf.createURI(sub), vf.createURI(pred), vf.createURI(obj));
			if(baseDataEngine != null && baseDataHash != null)
			{
				baseDataEngine.addStatement(sub, pred, obj, true);
				baseDataHash.put(sub, sub);
				baseDataHash.put(pred, pred);
				baseDataHash.put(obj,obj);
			}
		} catch (SailException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates all base relationships in the metamodel to add into the database and creates the OWL file
	 */
	protected void createBaseRelations() throws Exception{

		// necessary triple saying Concept is a type of Class
		String sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
		String pred = RDF.TYPE.stringValue();
		String obj = Constants.CLASS_URI;
		createStatement(vf.createURI(sub), vf.createURI(pred), vf.createURI(obj));
		storeBaseStatement(sub, pred, obj);
		// necessary triple saying Relation is a type of Property
		sub =  semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		pred = RDF.TYPE.stringValue();
		obj = Constants.DEFAULT_PROPERTY_URI;
		createStatement(vf.createURI(sub), vf.createURI(pred), vf.createURI(obj));
		storeBaseStatement(sub, pred, obj);

		if(basePropURI.equals("")){
			basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
		}
		storeBaseStatement(basePropURI, Constants.SUBPROPERTY_URI, basePropURI);

		Iterator<String> baseHashIt = baseConceptURIHash.keySet().iterator();
		//now add all of the base relations that have been stored in the hash.
		while(baseHashIt.hasNext()){
			String subjectInstance = baseHashIt.next() +"";
			String predicate = Constants.SUBCLASS_URI;
			//convert instances to URIs
			String subject = baseConceptURIHash.get(subjectInstance) +"";
			String object = semossURI + "/Concept";
			// create the statement now
			createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			// add base relations URIs to OWL
			storeBaseStatement(subject, predicate, object);
		}
		baseHashIt = baseRelationURIHash.keySet().iterator();
		while(baseHashIt.hasNext()){
			String subjectInstance = baseHashIt.next() +"";
			String predicate = Constants.SUBPROPERTY_URI;
			//convert instances to URIs
			String subject = baseRelationURIHash.get(subjectInstance) +"";
			String object = semossURI + "/Relation";
			// create the statement now
			createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			// add base relationship URIs to OWL
			storeBaseStatement(subject, predicate, object);
		}
		for(String[] relArray : baseRelations.values()){
			String subject = relArray[0];
			String predicate = relArray[1];
			String object = relArray[2];

//			createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			storeBaseStatement(subject, predicate, object);
			logger.info("RELATION TRIPLE:::: " + subject +" "+ predicate +" "+ object);
		}
		
		scOWL.commit();
		if(baseDataEngine != null) {
			baseDataEngine.commit();
		}
		// create the OWL File
		FileWriter fWrite = new FileWriter(owlFile);
		RDFXMLPrettyWriter owlWriter  = new RDFXMLPrettyWriter(fWrite); 
		rcOWL.export(owlWriter);
		fWrite.close();
		owlWriter.close();

		closeOWL();
	}
	
	protected String[] prepareReader(String fileNames, String customBase, String owlFile){
		String[] files = fileNames.split(";");
		semossURI = (String) DIHelper.getInstance().getLocalProp(Constants.SEMOSS_URI);
		//make location of the owl file in the dbname folder
		this.owlFile = owlFile; 
		semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		if(!customBase.equals("")) customBaseURI = customBase;
		
		return files;
	}
	
	protected void openEngineWithoutConnection(String dbName){
		String bdPropFile = dbName;
		try {
			loadBDProperties(bdPropFile);
			openDB();
			openOWLWithOutConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected void openEngineWithConnection(String engineName){
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
		BigDataEngine bigEngine = (BigDataEngine) engine;
		bdSail = bigEngine.bdSail;
		sc = bigEngine.sc;
		vf = bigEngine.vf;
		try {
			openOWLWithConnection(engine);
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getBaseURI(String nodeType){
		// Generate URI for subject node at the instance and base level
		String semossBaseURI = baseConceptURIHash.get(nodeType+Constants.CLASS);
		// check to see if user specified URI in custom map file
		if(semossBaseURI == null)
		{
			if(rdfMap.containsKey(nodeType+Constants.CLASS))
			{
				semossBaseURI = rdfMap.get(nodeType+Constants.CLASS);
			}
			// if no user specified URI, use generic SEMOSS URI
			else
			{
				semossBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ nodeType;
			}
			baseConceptURIHash.put(nodeType+Constants.CLASS, semossBaseURI);
		}
		return semossBaseURI;
	}

	public String getInstanceURI(String nodeType){

		String instanceBaseURI = conceptURIHash.get(nodeType);
		// check to see if user specified URI in custom map file
		if(instanceBaseURI == null)
		{
			if(rdfMap.containsKey(nodeType))
			{
				instanceBaseURI = rdfMap.get(nodeType);
			}
			// if no user specified URI, use generic customBaseURI
			else 
			{
				instanceBaseURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ nodeType;
			}
			conceptURIHash.put(nodeType, instanceBaseURI);
		}
		return instanceBaseURI;
	}
	
	/**
	 * Create and add all triples associated with relationship tabs
	 * @param subjectNodeType 			String containing the subject node type
	 * @param objectNodeType 			String containing the object node type
	 * @param instanceSubjectName 		String containing the name of the subject instance
	 * @param instanceObjectName	 	String containing the name of the object instance
	 * @param relName 					String containing the name of the relationship between the subject and object
	 * @param propHash 					Hashtable that contains all properties
	 */
	public void createRelationship(String subjectNodeType, String objectNodeType, String instanceSubjectName, String instanceObjectName, String relName, Hashtable<String, Object> propHash) throws Exception{
		// cellCounter used to determine which column currently processing
		instanceSubjectName = Utility.cleanString(instanceSubjectName, true);
		instanceObjectName = Utility.cleanString(instanceObjectName, true);

		// get base URIs for subject node at instance and semoss level
		String subjectSemossBaseURI = getBaseURI(subjectNodeType);
		String subjectInstanceBaseURI = getInstanceURI(subjectNodeType);

		// get base URIs for object node at instance and semoss level
		String objectSemossBaseURI = getBaseURI(objectNodeType);
		String objectInstanceBaseURI = getInstanceURI(objectNodeType);

		// create the full URI for the subject instance
		// add type and label triples to database
		String subjectNodeURI = subjectInstanceBaseURI + "/" + instanceSubjectName;
		createStatement(vf.createURI(subjectNodeURI), RDF.TYPE, vf.createURI(subjectSemossBaseURI));
		createStatement(vf.createURI(subjectNodeURI), RDFS.LABEL, vf.createLiteral(instanceSubjectName));
		
		// create the full URI for the object instance
		// add type and label triples to database
		String objectNodeURI = objectInstanceBaseURI + "/" + instanceObjectName;
		createStatement(vf.createURI(objectNodeURI), RDF.TYPE, vf.createURI(objectSemossBaseURI));
		createStatement(vf.createURI(objectNodeURI), RDFS.LABEL, vf.createLiteral(instanceObjectName));
		
		// generate URIs for the relationship
		relName = Utility.cleanString(relName, true);
		String relSemossBaseURI = baseRelationURIHash.get(subjectNodeType + "_"+ relName + "_" + objectNodeType+Constants.CLASS);
		// check to see if user specified URI in custom map file
		if(relSemossBaseURI == null){
			if(rdfMap.containsKey(subjectNodeType + "_"+ relName + "_" + objectNodeType+Constants.CLASS)) 
			{
				relSemossBaseURI = rdfMap.get(subjectNodeType + "_"+ relName + "_" + objectNodeType+Constants.CLASS);
			}
			// if no user specified URI, use generic SEMOSS URI
			else
			{
				relSemossBaseURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + relName;
			}
			baseRelationURIHash.put(subjectNodeType + "_"+ relName + "_" + objectNodeType +Constants.CLASS, relSemossBaseURI);
		}
		
		String relInstanceBaseURI = relationURIHash.get(subjectNodeType + "_"+ relName + "_" + objectNodeType);
		// check to see if user specified URI in custom map file
		if(relInstanceBaseURI == null){
			if(rdfMap.containsKey(subjectNodeType + "_"+ relName + "_" + objectNodeType)) 
			{
				relInstanceBaseURI = rdfMap.get(subjectNodeType + "_"+ relName + "_" + objectNodeType);
			}
			// if no user specified URI, use generic customBaseURI
			else 
			{
				relInstanceBaseURI = customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + relName;
			}
			relationURIHash.put(subjectNodeType + "_"+ relName + "_" + objectNodeType, relInstanceBaseURI);
		}

		String relArrayKey = subjectSemossBaseURI+relSemossBaseURI+objectSemossBaseURI;
		if(!baseRelations.contains(relArrayKey))
			baseRelations.put(relArrayKey, new String[]{subjectSemossBaseURI, relSemossBaseURI, objectSemossBaseURI});

		// create instance value of relationship and add instance relationship, subproperty, and label triples
		String instanceRelURI = relInstanceBaseURI + "/" + instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName;
		logger.info("Adding Relationship " +subjectNodeType +" " + instanceSubjectName + " ... " + relName + " ... " + objectNodeType +" " + instanceObjectName); 
		createStatement(vf.createURI(instanceRelURI), RDFS.SUBPROPERTYOF, vf.createURI(relSemossBaseURI));
		createStatement(vf.createURI(instanceRelURI), RDFS.LABEL, vf.createLiteral(instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName));
		createStatement(vf.createURI(subjectNodeURI), vf.createURI(instanceRelURI), vf.createURI(objectNodeURI));
		
		addProperties(instanceRelURI, propHash);
	}
	
	public void addNodeProperties(String nodeType, String instanceName, Hashtable<String, Object> propHash) throws Exception{
		//create the node in case its not in a relationship
		instanceName = Utility.cleanString(instanceName, true);
		String semossBaseURI = getBaseURI(nodeType);
		String instanceBaseURI = getInstanceURI(nodeType);
		String subjectNodeURI = instanceBaseURI + "/" + instanceName;
		createStatement(vf.createURI(subjectNodeURI), RDF.TYPE, vf.createURI(semossBaseURI));
		createStatement(vf.createURI(subjectNodeURI), RDFS.LABEL, vf.createLiteral(instanceName));
		
		//add whatever properties associated with the node
		addProperties(subjectNodeURI, propHash);
	}
	
	public void addProperties(String instanceURI, Hashtable<String, Object> propHash) throws Exception{
		
		// add all properties
		Enumeration<String> propKeys = propHash.keys();
		if(basePropURI.equals(""))
		{
			basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
		}
		// add property triple based on data type of property
		while(propKeys.hasMoreElements()) 
		{
			String key = propKeys.nextElement().toString();
			String propURI = basePropURI + "/" + key;
			logger.info("Processing Property " + key + " for " + instanceURI);
			createStatement(vf.createURI(propURI), RDF.TYPE, vf.createURI(basePropURI));	
			if(propHash.get(key).getClass() == new Double(1).getClass())
			{
				Double value = (Double) propHash.get(key);
				logger.info("Processing Double value " + value); 
				createStatement(vf.createURI(instanceURI), vf.createURI(propURI), vf.createLiteral(value.doubleValue()));
			}
			else if(propHash.get(key).getClass() == new Date(1).getClass())
			{
				Date value = (Date)propHash.get(key);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String date = df.format(value);
				URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
				logger.info("Processing Date value " + date); 
				createStatement(vf.createURI(instanceURI), vf.createURI(propURI), vf.createLiteral(date, datatype));
			}
			else if(propHash.get(key).getClass() == new Boolean(true).getClass())
			{
				Boolean value = (Boolean) propHash.get(key);
				logger.info("Processing Boolean value " + value); 
				createStatement(vf.createURI(instanceURI), vf.createURI(propURI), vf.createLiteral(value.booleanValue()));
			}
			else
			{
				String value = propHash.get(key).toString();
				if(value.equals(Constants.PROCESS_CURRENT_DATE)){
					logger.info("Processing Current Date Property"); 
					insertCurrentDate(propURI, basePropURI, instanceURI);
				}
				else if(value.equals(Constants.PROCESS_CURRENT_USER)){
					logger.info("Processing Current User Property"); 
					insertCurrentUser(propURI, basePropURI, instanceURI);
				}
				else{
					String cleanValue = Utility.cleanString(value, true);
					logger.info("Processing String value " + cleanValue); 
					createStatement(vf.createURI(instanceURI), vf.createURI(propURI), vf.createLiteral(cleanValue));
				}
			}
		}
	}
	
	/**
	 * Insert the current user as a property onto a node if property is "PROCESS_CURRENT_USER"
	 * @param propURI 			String containing the URI of the property at the instance level
	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
	 */
	private void insertCurrentUser(String propURI, String basePropURI, String subjectNodeURI){
		String cleanValue = System.getProperty("user.name");
		try{
			createStatement(vf.createURI(propURI), RDF.TYPE, vf.createURI(basePropURI));				
			createStatement(vf.createURI(subjectNodeURI), vf.createURI(propURI), vf.createLiteral(cleanValue));
		} catch (Exception e) {
			logger.error(e);
		}	
	}

	/**
	 * Insert the current date as a property onto a node if property is "PROCESS_CURRENT_DATE"
	 * @param propURI 			String containing the URI of the property at the instance level
	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
	 */
	private void insertCurrentDate(String propInstanceURI, String basePropURI, String subjectNodeURI){
		Date dValue = new Date();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String date = df.format(dValue);
		try {
			createStatement(vf.createURI(propInstanceURI), RDF.TYPE, vf.createURI(basePropURI));
			URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
			createStatement(vf.createURI(subjectNodeURI), vf.createURI(propInstanceURI), vf.createLiteral(date, datatype));
		} catch (Exception e) {
			logger.error(e);
		}	
	}

}
