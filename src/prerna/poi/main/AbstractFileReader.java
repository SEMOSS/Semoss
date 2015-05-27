/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.poi.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.error.FileWriterException;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.hp.hpl.jena.vocabulary.OWL;

public abstract class AbstractFileReader {

	protected Hashtable<String, String> rdfMap = new Hashtable<String, String>();
	protected String bdPropFile;
	protected Properties bdProp = new Properties(); // properties for big data
	protected IEngine engine;
	
	protected String customBaseURI = "";
	public String basePropURI= "";

	protected String semossURI;
	protected final static String CONTAINS = "Contains";
	
	public Hashtable<String,String> baseConceptURIHash = new Hashtable<String,String>(); 
	public Hashtable<String,String> conceptURIHash = new Hashtable<String,String>();
	public Hashtable<String,String> baseRelationURIHash = new Hashtable<String,String>(); 
	public Hashtable<String,String> relationURIHash = new Hashtable<String,String>();
	public Hashtable<String,String> basePropURIHash = new Hashtable<String,String>();
	public Hashtable<String,String> basePropRelations = new Hashtable<String,String>();
	
	protected Hashtable<String, String[]> baseRelations = new Hashtable<String, String[]>();

	private static final Logger logger = LogManager.getLogger(AbstractFileReader.class.getName());

	// OWL variables
	protected RepositoryConnection rcOWL;
	protected String owlFile = "";

	//reload base data
	protected RDFFileSesameEngine baseDataEngine;
	protected Hashtable<String, String> baseDataHash = new Hashtable<String, String>();

	/**
	 * Loads the prop file for the CSV file
	 * @param fileName	Absolute path to the prop file specified in the last column of the CSV file
	 * @throws FileReaderException 
	 */
	protected void openProp(String fileName) throws FileReaderException {
		Properties rdfPropMap = new Properties();
		FileInputStream fileIn = null;
		try {
			fileIn = new FileInputStream(fileName);
			rdfPropMap.load(fileIn);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not find user-specified prop file with CSV metamodel data located at: " + fileName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not read user-specified prop file with CSV metamodel data located at: " + fileName);
		} finally{
			try{
				if(fileIn!=null)
					fileIn.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		for(String name: rdfPropMap.stringPropertyNames()){
			rdfMap.put(name, rdfPropMap.getProperty(name).toString());
		}
	}

	/**
	 * Close the database engine
	 * @throws EngineException 
	 */
	public void closeDB() throws EngineException {
		logger.warn("Closing....");
		commitDB();
		engine.closeDB();
		//TODO: why do we do this?
//		try {
//			sc.close();
//			bdSail.shutDown();
//		} catch (SailException e) {
//			e.printStackTrace();
//			throw new EngineException("Could not close database connection");
//		}
	}	

	protected void commitDB() throws EngineException {
		logger.warn("Committing....");
		engine.commit();
		
		//TODO: how to call .infer() ?
		if(engine!=null && engine instanceof BigDataEngine){
			((BigDataEngine)engine).infer();
		}
		else{
			if(engine!=null && engine instanceof RDFFileSesameEngine){
				try {
					((RDFFileSesameEngine)engine).exportDB();
				} catch (RepositoryException | RDFHandlerException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Creates a repository connection to be put all the base relationship data to create the OWL file
	 * @throws EngineException 
	 */
	private void openOWLWithOutConnection() throws EngineException {
		baseDataEngine = new RDFFileSesameEngine();
		baseDataEngine.openDB(null);
		baseDataEngine.setFileName(owlFile);
	}

	/**
	 * Creates a repository connection and puts all the existing base relationships to create an updated OWL file
	 * @param engine	The database engine used to get all the existing base relationships
	 * @throws EngineException 
	 */
	@SuppressWarnings("unchecked")
	private void openOWLWithConnection(IEngine engine) throws EngineException {
		baseDataEngine = ((AbstractEngine)engine).getBaseDataEngine();
		baseDataHash = ((AbstractEngine)engine).getBaseHash();
		baseDataEngine.setFileName(owlFile);
	}

	/**
	 * Close the OWL engine
	 * @throws EngineException 
	 */
	protected void closeOWL() throws EngineException {
		baseDataEngine.commit();
	}

	protected void storeBaseStatement(String sub, String pred, String obj) throws EngineException {
		String cleanSub = Utility.cleanString(sub, false);
		String cleanPred = Utility.cleanString(pred, false);
		String cleanObj = Utility.cleanString(obj, false);
		baseDataEngine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{cleanSub, cleanPred, cleanObj, true});
		if(baseDataEngine != null && baseDataHash != null)
		{
			baseDataEngine.addStatement(new Object[]{cleanSub, cleanPred, cleanObj, true});
			baseDataHash.put(cleanSub, cleanSub);
			baseDataHash.put(cleanPred, cleanPred);
			baseDataHash.put(cleanObj,cleanObj);//
		}
	}

	/**
	 * Creates all base relationships in the metamodel to add into the database and creates the OWL file
	 * @throws EngineException 
	 * @throws FileWriterException 
	 */
	protected void createBaseRelations() throws EngineException, FileWriterException {
		// necessary triple saying Concept is a type of Class
		String sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
		String pred = RDF.TYPE.stringValue();
		String obj = Constants.CLASS_URI;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, pred, obj, true});
		storeBaseStatement(sub, pred, obj);
		// necessary triple saying Relation is a type of Property
		sub =  semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		pred = RDF.TYPE.stringValue();
		obj = Constants.DEFAULT_PROPERTY_URI;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, pred, obj, true});
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
			String subject = Utility.cleanString(baseConceptURIHash.get(subjectInstance) +"", false);
			String object = semossURI + "/Concept";
			// create the statement now
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subject, predicate, object, true});
			// add base relations URIs to OWL
			storeBaseStatement(subject, predicate, object);
		}
		baseHashIt = baseRelationURIHash.keySet().iterator();
		while(baseHashIt.hasNext()){
			String subjectInstance = baseHashIt.next() +"";
			String predicate = Constants.SUBPROPERTY_URI;
			//convert instances to URIs
			String subject = Utility.cleanString(baseRelationURIHash.get(subjectInstance) +"", false);
			String object = semossURI + "/Relation";
			// create the statement now
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subject, predicate, object, true});
			// add base relationship URIs to OWL//
			storeBaseStatement(subject, predicate, object);
		}
		for(String[] relArray : baseRelations.values()){
			String subject = relArray[0];
			String predicate = relArray[1];
			String object = relArray[2];
			storeBaseStatement(subject, predicate, object);
//			logger.info("RELATION TRIPLE:::: " + subject +" "+ predicate +" "+ object);
		}

		// I need to write one now for creating properties as well
		// this is where I will do properties
		// add the base relation first
		storeBaseStatement(semossURI + "/" + Constants.DEFAULT_PROPERTY_CLASS, RDF.TYPE+"", semossURI + "/" + Constants.DEFAULT_RELATION_CLASS);
		
		baseHashIt = basePropURIHash.keySet().iterator();
		while(baseHashIt.hasNext()){
			String subjectInstance = baseHashIt.next() +"";
			String predicate = RDF.TYPE +"";
			//convert instances to URIs
			String subject = subjectInstance;
			String object = semossURI + "/" + Constants.DEFAULT_PROPERTY_CLASS;
			// create the statement now
			// base property uri is like
			// Relation/Contains/MovieBudget RDFS:SUBCLASSOF /Relation/Contains
			storeBaseStatement(subject, predicate, object);
		}
		
		// now write the actual relations
		// relation instances go next
		for(String relArray : basePropRelations.keySet()){
			String property = relArray;
			String parent = basePropRelations.get(property);
			//createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
			storeBaseStatement(parent, OWL.DatatypeProperty+"", property);
//			logger.info("RELATION TRIPLE:::: " + subject +" "+ predicate +" "+ object);
		}

		baseDataEngine.commit();
		try {
			baseDataEngine.exportDB();
		} catch (RepositoryException ex) {
			ex.printStackTrace();
		} catch (RDFHandlerException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		closeOWL();
	}

	protected String[] prepareReader(String fileNames, String customBase, String owlFile, String bdPropFile){
		String[] files = fileNames.split(";");
		//make location of the owl file in the dbname folder
		this.owlFile = owlFile; 
		// location of bdPropFile
		this.bdPropFile = bdPropFile;
		semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		if(!customBase.equals("")) {
			customBaseURI = customBase;
		}

		return files;
	}

	protected void openEngineWithoutConnection(String dbName) throws FileReaderException, EngineException {
		createNewEngine();
		openOWLWithOutConnection();
	}
	
	private void createNewEngine() {
		engine = new BigDataEngine();
		engine.openDB(bdPropFile);
	}

	protected void openEngineWithConnection(String engineName) throws EngineException {
		engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
		openOWLWithConnection(engine);
	}

	public String getBaseURI(String nodeType) {
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

	public String getInstanceURI(String nodeType) {

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
	 * @throws EngineException 
	 */
	public void createRelationship(String subjectNodeType, String objectNodeType, String instanceSubjectName, String instanceObjectName, String relName, Hashtable<String, Object> propHash) throws EngineException {
		subjectNodeType = Utility.cleanString(subjectNodeType, true);
		objectNodeType = Utility.cleanString(objectNodeType, true);
		
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
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, RDF.TYPE, subjectSemossBaseURI, true});
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, RDFS.LABEL, instanceSubjectName, false});
//		createStatement(vf.createURI(subjectNodeURI), RDF.TYPE, vf.createURI(subjectSemossBaseURI));
//		createStatement(vf.createURI(subjectNodeURI), RDFS.LABEL, vf.createLiteral(instanceSubjectName));

		// create the full URI for the object instance
		// add type and label triples to database
		String objectNodeURI = objectInstanceBaseURI + "/" + instanceObjectName;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{objectNodeURI, RDF.TYPE, objectSemossBaseURI, true});
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{objectNodeURI, RDFS.LABEL, instanceObjectName, false});
//		createStatement(vf.createURI(objectNodeURI), RDF.TYPE, vf.createURI(objectSemossBaseURI));
//		createStatement(vf.createURI(objectNodeURI), RDFS.LABEL, vf.createLiteral(instanceObjectName));

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
//		logger.info("Adding Relationship " +subjectNodeType +" " + instanceSubjectName + " ... " + relName + " ... " + objectNodeType +" " + instanceObjectName); 
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceRelURI, RDFS.SUBPROPERTYOF, relSemossBaseURI, true});
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceRelURI, RDFS.LABEL, instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName, false});
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, instanceRelURI, objectNodeURI, true});

//		createStatement(vf.createURI(instanceRelURI), RDFS.SUBPROPERTYOF, vf.createURI(relSemossBaseURI));
//		createStatement(vf.createURI(instanceRelURI), RDFS.LABEL, vf.createLiteral(instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName));
//		createStatement(vf.createURI(subjectNodeURI), vf.createURI(instanceRelURI), vf.createURI(objectNodeURI));

		addProperties(instanceRelURI, propHash);
	}

	public void addNodeProperties(String nodeType, String instanceName, Hashtable<String, Object> propHash) throws EngineException {
		//create the node in case its not in a relationship
		instanceName = Utility.cleanString(instanceName, true);
		String semossBaseURI = getBaseURI(nodeType);
		String instanceBaseURI = getInstanceURI(nodeType);
		String subjectNodeURI = instanceBaseURI + "/" + instanceName;
//		createStatement(vf.createURI(subjectNodeURI), RDF.TYPE, vf.createURI(semossBaseURI));
//		createStatement(vf.createURI(subjectNodeURI), RDFS.LABEL, vf.createLiteral(instanceName));
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, RDF.TYPE, semossBaseURI, true});
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, RDFS.LABEL, instanceName, false});

		addProperties(subjectNodeURI, propHash);
		// adding node properties to owl
		if(basePropURI.equals(""))
		{
			basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
		}
		for(String propName : propHash.keySet()) {
			String propURI = basePropURI + "/" + propName;
			basePropRelations.put(propURI, semossBaseURI);
			basePropURIHash.put(propURI, propURI);
		}
	}

	public void addProperties(String instanceURI, Hashtable<String, Object> propHash) throws EngineException {

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
//			logger.info("Processing Property " + key + " for " + instanceURI);
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propURI, RDF.TYPE, basePropURI, true});
			if(propHash.get(key).getClass() == new Double(1).getClass())
			{
				Double value = (Double) propHash.get(key);
//				logger.info("Processing Double value " + value); 
				engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceURI, propURI, value.doubleValue(), false});
			}
			else if(propHash.get(key).getClass() == new Date(1).getClass())
			{
				Date value = (Date)propHash.get(key);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String date = df.format(value);
				Date dateFormatted;
				try {
					dateFormatted = df.parse(date);
				} catch (ParseException e) {
					logger.error("ERROR: could not parse date: " + date);
					continue;
				}
				engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceURI, propURI, dateFormatted, false});
//				URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
//				logger.info("Processing Date value " + date); 
//				createStatement(vf.createURI(instanceURI), vf.createURI(propURI), vf.createLiteral(date, datatype));
			}
			else if(propHash.get(key).getClass() == new Boolean(true).getClass())
			{
				Boolean value = (Boolean) propHash.get(key);
//				logger.info("Processing Boolean value " + value); 
				engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceURI, propURI, value.booleanValue(), false});
			}
			else
			{
				String value = propHash.get(key).toString();
				if(value.equals(Constants.PROCESS_CURRENT_DATE)){
//					logger.info("Processing Current Date Property"); 
					insertCurrentDate(propURI, basePropURI, instanceURI);
				}
				else if(value.equals(Constants.PROCESS_CURRENT_USER)){
//					logger.info("Processing Current User Property"); 
					insertCurrentUser(propURI, basePropURI, instanceURI);
				}
				else{
					String cleanValue = Utility.cleanString(value, true);
//					logger.info("Processing String value " + cleanValue); 
					engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{instanceURI, propURI, cleanValue, false});
//					createStatement(vf.createURI(instanceURI), vf.createURI(propURI), vf.createLiteral(cleanValue));
				}
			}
		}
	}

	/**
	 * Insert the current user as a property onto a node if property is "PROCESS_CURRENT_USER"
	 * @param propURI 			String containing the URI of the property at the instance level
	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
	 * @throws EngineException 
	 */
	private void insertCurrentUser(String propURI, String basePropURI, String subjectNodeURI) throws EngineException {
		String cleanValue = System.getProperty("user.name");
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propURI, RDF.TYPE, basePropURI, true});
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, propURI, cleanValue, false});
//		createStatement(vf.createURI(propURI), RDF.TYPE, vf.createURI(basePropURI));				
//		createStatement(vf.createURI(subjectNodeURI), vf.createURI(propURI), vf.createLiteral(cleanValue));	
	}

	/**
	 * Insert the current date as a property onto a node if property is "PROCESS_CURRENT_DATE"
	 * @param propURI 			String containing the URI of the property at the instance level
	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
	 * @throws EngineException 
	 */
	private void insertCurrentDate(String propInstanceURI, String basePropURI, String subjectNodeURI) throws EngineException {
		Date dValue = new Date();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String date = df.format(dValue);
		Date dateFormatted;
		try {
			dateFormatted = df.parse(date);
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propInstanceURI, RDF.TYPE, basePropURI, true});
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, propInstanceURI, dateFormatted, false});
		} catch (ParseException e) {
			logger.error("ERROR: could not parse date: " + date);
		}
//		createStatement(vf.createURI(propInstanceURI), RDF.TYPE, vf.createURI(basePropURI));
//		URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
//		createStatement(vf.createURI(subjectNodeURI), vf.createURI(propInstanceURI), vf.createLiteral(date, datatype));
	}

}
