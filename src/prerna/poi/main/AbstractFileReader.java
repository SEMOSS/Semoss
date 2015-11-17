/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.sail.SailException;

import com.hp.hpl.jena.vocabulary.OWL;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

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
	private Hashtable<String,String> basePropURIHash = new Hashtable<String,String>();
	private Hashtable<String,String> basePropRelations = new Hashtable<String,String>();
	protected Hashtable<String,String> displayNamesHash = new Hashtable<String,String>();
	
	protected Hashtable<String, String[]> baseRelations = new Hashtable<String, String[]>();

	private static final Logger logger = LogManager.getLogger(AbstractFileReader.class.getName());

	// OWL variables
	protected String owlFile = "";
	protected BaseDatabaseCreator baseEngCreator;

	/**
	 * Loads the prop file for the CSV file
	 * @param fileName	Absolute path to the prop file specified in the last column of the CSV file
	 * @throws IOException 
	 */
	protected void openProp(String fileName) throws IOException {
		Properties rdfPropMap = new Properties();
		FileInputStream fileIn = null;
		try {
			fileIn = new FileInputStream(fileName);
			rdfPropMap.load(fileIn);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new FileNotFoundException("Could not find user-specified prop file located in header row in cell: " + fileName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Could not read user-specified prop file located in header row in cell: " + fileName);
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
	 * @throws IOException 
	 */
	public void closeDB() throws IOException {
		logger.warn("Closing....");
		if(engine != null) {
			commitDB();
			engine.closeDB();
		}
		//TODO: why do we do this?
//		try {
//			sc.close();
//			bdSail.shutDown();
//		} catch (SailException e) {
//			e.printStackTrace();
//			throw new IOException("Could not close database connection");
//		}
	}	

	protected void commitDB() throws IOException {
		logger.warn("Committing....");
		engine.commit();
		
		//TODO: how to call .infer() ?
		if(engine!=null && engine instanceof BigDataEngine){
			((BigDataEngine)engine).infer();
		} else if(engine!=null && engine instanceof RDFFileSesameEngine){
			try {
				((RDFFileSesameEngine)engine).exportDB();
			} catch (RepositoryException | RDFHandlerException | IOException e) {
				e.printStackTrace();
				throw new IOException("Unable to commit data from file into database");
			}
		}
	}

	/**
	 * Creates a repository connection to be put all the base relationship data
	 * to create the OWL file
	 * 
	 * @throws EngineException
	 */
	private void openOWLWithOutConnection(String owlFile) {
		baseEngCreator = new BaseDatabaseCreator(owlFile);
	}

	/**
	 * Creates a repository connection and puts all the existing base
	 * relationships to create an updated OWL file
	 * 
	 * @param engine
	 *            The database engine used to get all the existing base
	 *            relationships
	 * @throws EngineException
	 */
	private void openOWLWithConnection(IEngine engine, String owlFile) {
		baseEngCreator = new BaseDatabaseCreator(engine, owlFile);
	}

	/**
	 * Close the OWL engine
	 * @throws EngineException
	 */
	protected void closeOWL() {
		baseEngCreator.closeBaseEng();
	}
	
	protected void storeBaseStatement(String sub, String pred, String obj) {
		storeBaseStatement(sub,pred,obj,true);
	}
	
	protected void storeBaseStatement(String sub, String pred, String obj, boolean concept) {
		baseEngCreator.addToBaseEngine(new Object[]{sub, pred, obj, concept});
	}

	/**
	 * Creates all base relationships in the metamodel to add into the database
	 * and creates the OWL file
	 * 
	 * @throws EngineException
	 * @throws FileWriterException
	 */
	protected void createBaseRelations() {
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
			String subjectKey = baseHashIt.next() +"";
			String subjectInstance = basePropURIHash.get(subjectKey);//String subjectInstance = baseHashIt.next() +"";
			String predicate = RDF.TYPE +"";
			//convert instances to URIs
			String subject = subjectInstance;
			String object = semossURI + "/" + Constants.DEFAULT_PROPERTY_CLASS;
			// create the statement now
			// base property uri is like
			// Relation/Contains/MovieBudget RDFS:SUBCLASSOF /Relation/Contains
			storeBaseStatement(subject, predicate, object);
			String parent = basePropRelations.get(subjectKey);
		}
		
		// now write the actual relations
		// relation instances go next
		for(String relArray : basePropRelations.keySet()){
			String propertyKey = relArray;
			String parent = basePropRelations.get(propertyKey);
			String parentURI = baseConceptURIHash.get(parent);
			String propertyURI = basePropURIHash.get(propertyKey);
			storeBaseStatement(parentURI, OWL.DatatypeProperty+"", propertyURI);
//			logger.info("RELATION TRIPLE:::: " + subject +" "+ predicate +" "+ object);
		}
		
		//process logic for display naming
		if(displayNamesHash.size()>0){
			DisplayNamesProcessor.addDisplayNamesToOWL(displayNamesHash, basePropURIHash, baseConceptURIHash, baseEngCreator, engine);
		}

		baseEngCreator.commit();
		try {
			baseEngCreator.exportBaseEng(true);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	protected void processDisplayNames(){
		displayNamesHash = DisplayNamesProcessor.generateDisplayNameMap(rdfMap, false);
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

	protected void openEngineWithoutConnection(String dbName) {
		createNewEngine(dbName);
		openOWLWithOutConnection(owlFile);
	}
	
	private void createNewEngine(String dbName) {
		engine = new BigDataEngine();
		engine.setEngineName(dbName);
		engine.openDB(bdPropFile);
	}

	protected void openEngineWithConnection(String engineName) {
		engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
		openOWLWithConnection(engine, owlFile);
	}

	public String getBaseURI(String nodeType) {
		nodeType = Utility.cleanString(nodeType, true);
		// Generate URI for subject node at the instance and base level
		String semossBaseURI = baseConceptURIHash.get(nodeType);
		// check to see if user specified URI in custom map file
		if(semossBaseURI == null){
			if(rdfMap.containsKey(nodeType+Constants.CLASS)){
				semossBaseURI = rdfMap.get(nodeType+Constants.CLASS);
			}
			// if no user specified URI, use generic SEMOSS URI
			else {
				semossBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ nodeType;
			}
			baseConceptURIHash.put(nodeType, semossBaseURI);
		}
		return semossBaseURI;
	}

	public String getInstanceURI(String nodeType) {

		String instanceBaseURI = conceptURIHash.get(nodeType);
		// check to see if user specified URI in custom map file
		if(instanceBaseURI == null){
			if(rdfMap.containsKey(nodeType)){
				instanceBaseURI = rdfMap.get(nodeType);
			}
			// if no user specified URI, use generic customBaseURI
			else {
				instanceBaseURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ nodeType;
			}
			conceptURIHash.put(nodeType, instanceBaseURI);
		}
		return instanceBaseURI;
	}

	/**
	 * Create and add all triples associated with relationship tabs
	 * @param subjectNodeType					String containing the subject node type
	 * @param objectNodeType					String containing the object node type
	 * @param instanceSubjectName				String containing the name of the subject instance
	 * @param instanceObjectName				String containing the name of the object instance
	 * @param relName							String containing the name of the relationship between the subject and object
	 * @param propHash							Hashtable that contains all properties
	 */
	public void createRelationship(String subjectNodeType, String objectNodeType, String instanceSubjectName,
			String instanceObjectName, String relName, Hashtable<String, Object> propHash) {
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
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT,
				new Object[] { subjectNodeURI, RDF.TYPE, subjectSemossBaseURI, true });
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT,
				new Object[] { subjectNodeURI, RDFS.LABEL, instanceSubjectName, false });

		// create the full URI for the object instance
		// add type and label triples to database
		String objectNodeURI = objectInstanceBaseURI + "/" + instanceObjectName;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT,
				new Object[] { objectNodeURI, RDF.TYPE, objectSemossBaseURI, true });
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT,
				new Object[] { objectNodeURI, RDFS.LABEL, instanceObjectName, false });

		// generate URIs for the relationship
		relName = Utility.cleanPredicateString(relName);

		String relSemossBaseURI = baseRelationURIHash
				.get(subjectNodeType + "_" + relName + "_" + objectNodeType);
		// check to see if user specified URI in custom map file
		if (relSemossBaseURI == null) {
			if (rdfMap.containsKey(subjectNodeType + "_" + relName + "_" + objectNodeType + Constants.CLASS)) {
				relSemossBaseURI = rdfMap.get(subjectNodeType + "_" + relName + "_" + objectNodeType + Constants.CLASS);
			}
			// if no user specified URI, use generic SEMOSS URI
			else {
				relSemossBaseURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + relName;
			}
			baseRelationURIHash.put(subjectNodeType + "_" + relName + "_" + objectNodeType,
					relSemossBaseURI);
		}

		String relInstanceBaseURI = relationURIHash.get(subjectNodeType + "_" + relName + "_" + objectNodeType);
		// check to see if user specified URI in custom map file
		if (relInstanceBaseURI == null) {
			if (rdfMap.containsKey(subjectNodeType + "_" + relName + "_" + objectNodeType)) {
				relInstanceBaseURI = rdfMap.get(subjectNodeType + "_" + relName + "_" + objectNodeType);
			}
			// if no user specified URI, use generic customBaseURI
			else {
				relInstanceBaseURI = customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + relName;
			}
			relationURIHash.put(subjectNodeType + "_" + relName + "_" + objectNodeType, relInstanceBaseURI);
		}

		String relArrayKey = subjectSemossBaseURI + relSemossBaseURI + objectSemossBaseURI;
		if (!baseRelations.contains(relArrayKey))
			baseRelations.put(relArrayKey, new String[] { subjectSemossBaseURI, relSemossBaseURI, objectSemossBaseURI });

		// create instance value of relationship and add instance relationship,
		// subproperty, and label triples
		String instanceRelURI = relInstanceBaseURI + "/" + instanceSubjectName + Constants.RELATION_URI_CONCATENATOR
				+ instanceObjectName;
		// logger.info("Adding Relationship " +subjectNodeType +" " +
		// instanceSubjectName + " ... " + relName + " ... " + objectNodeType +"
		// " + instanceObjectName);
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT,
				new Object[] { instanceRelURI, RDFS.SUBPROPERTYOF, relSemossBaseURI, true });
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceRelURI, RDFS.LABEL,
				instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName, false });
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT,
				new Object[] { subjectNodeURI, instanceRelURI, objectNodeURI, true });

		addProperties(instanceRelURI, propHash);
	}

	public void addNodeProperties(String nodeType, String instanceName, Hashtable<String, Object> propHash) {
		//create the node in case its not in a relationship
		instanceName = Utility.cleanString(instanceName, true);
		String semossBaseURI = getBaseURI(nodeType);
		String instanceBaseURI = getInstanceURI(nodeType);
		String subjectNodeURI = instanceBaseURI + "/" + instanceName;
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
			basePropRelations.put(nodeType + "%" + propName,nodeType);//basePropRelations.put(propURI, semossBaseURI);
			basePropURIHash.put(nodeType + "%" + propName, propURI);//basePropURIHash.put(propURI, propURI);
		}
	}

	public void addProperties(String instanceURI, Hashtable<String, Object> propHash) {

		// add all properties
		Enumeration<String> propKeys = propHash.keys();
		if (basePropURI.equals("")) {
			basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
		}
		// add property triple based on data type of property
		while (propKeys.hasMoreElements()) {
			String key = propKeys.nextElement().toString();
			String propURI = basePropURI + "/" + key;
			// logger.info("Processing Property " + key + " for " +
			// instanceURI);
			engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { propURI, RDF.TYPE, basePropURI, true });
			if (propHash.get(key).getClass() == new Double(1).getClass()) {
				Double value = (Double) propHash.get(key);
				// logger.info("Processing Double value " + value);
				engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, value.doubleValue(), false });
			} else if (propHash.get(key).getClass() == new Date(1).getClass()) {
				Date value = (Date) propHash.get(key);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String date = df.format(value);
				Date dateFormatted;
				try {
					dateFormatted = df.parse(date);
				} catch (ParseException e) {
					logger.error("ERROR: could not parse date: " + date);
					continue;
				}
				engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, dateFormatted, false });
				// logger.info("Processing Date value " + dateFormatted);
			} else if (propHash.get(key).getClass() == new Boolean(true).getClass()) {
				Boolean value = (Boolean) propHash.get(key);
				// logger.info("Processing Boolean value " + value);
				engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, value.booleanValue(), false });
			} else {
				String value = propHash.get(key).toString();
				if (value.equals(Constants.PROCESS_CURRENT_DATE)) {
					// logger.info("Processing Current Date Property");
					insertCurrentDate(propURI, basePropURI, instanceURI);
				} else if (value.equals(Constants.PROCESS_CURRENT_USER)) {
					// logger.info("Processing Current User Property");
					insertCurrentUser(propURI, basePropURI, instanceURI);
				} else {
					String cleanValue = Utility.cleanString(value, true);
					// logger.info("Processing String value " + cleanValue);
					engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, cleanValue, false });
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
	private void insertCurrentUser(String propURI, String basePropURI, String subjectNodeURI) {
		String cleanValue = System.getProperty("user.name");
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propURI, RDF.TYPE, basePropURI, true});
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, propURI, cleanValue, false});
	}

	/**
	 * Insert the current date as a property onto a node if property is "PROCESS_CURRENT_DATE"
	 * @param propURI 			String containing the URI of the property at the instance level
	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
	 */
	private void insertCurrentDate(String propInstanceURI, String basePropURI, String subjectNodeURI) {
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
	}

}
