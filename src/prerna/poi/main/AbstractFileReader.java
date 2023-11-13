///*******************************************************************************
// * Copyright 2015 Defense Health Agency (DHA)
// *
// * If your use of this software does not include any GPLv2 components:
// * 	Licensed under the Apache License, Version 2.0 (the "License");
// * 	you may not use this file except in compliance with the License.
// * 	You may obtain a copy of the License at
// *
// * 	  http://www.apache.org/licenses/LICENSE-2.0
// *
// * 	Unless required by applicable law or agreed to in writing, software
// * 	distributed under the License is distributed on an "AS IS" BASIS,
// * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * 	See the License for the specific language governing permissions and
// * 	limitations under the License.
// * ----------------------------------------------------------------------------
// * If your use of this software includes any GPLv2 components:
// * 	This program is free software; you can redistribute it and/or
// * 	modify it under the terms of the GNU General Public License
// * 	as published by the Free Software Foundation; either version 2
// * 	of the License, or (at your option) any later version.
// *
// * 	This program is distributed in the hope that it will be useful,
// * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
// * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * 	GNU General Public License for more details.
// *******************************************************************************/
//package prerna.poi.main;
//
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.text.DateFormat;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.Enumeration;
//import java.util.HashMap;
//import java.util.Hashtable;
//import java.util.Map;
//import java.util.Properties;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.openrdf.model.vocabulary.RDF;
//import org.openrdf.model.vocabulary.RDFS;
//
//import prerna.engine.api.IDatabaseEngine;
//import prerna.engine.api.impl.util.AbstractOwler;
//import prerna.poi.main.helper.ImportOptions;
//import prerna.util.Constants;
//import prerna.util.Utility;
//
//public abstract class AbstractFileReader extends AbstractEngineCreator {
//
//	private static final Logger logger = LogManager.getLogger(AbstractFileReader.class.getName());
//
//	// RDBMSReader will look in this map for objects that are specified in the prop file,
//	// but do not exist as headers in a CSV
//	protected Map<String, String> objectValueMap = new HashMap<String, String>();
//	protected Map<String, String> objectTypeMap = new HashMap<String, String>();
//	protected String rowKey; // What to put in a prop file to grab the current row number
//	
//	// If true, RDBMSReader will create indexes when cleaning up tables  
//	// Default to true
//	protected boolean createIndexes = true;
//	
//	// path to the set of propFiles for automated
//	protected String[] propFiles;
//	// the path to the prop file being used to load the engine
//	protected String propFile;
//		
//	// stores a list of rdf maps created from the FE
//	protected Hashtable<String, String>[] rdfMapArr;
//	// the specific rdf map for a file during data loading
//	protected Hashtable<String, String> rdfMap = new Hashtable<String, String>();
//
//	// boolean to determine if we are looking for a prop file or assuming user defined metamodel via ui
//	protected boolean propFileExist = true;
//	// boolean to differentiate when prop file is defined within csv file
//	protected boolean propFileDefinedInsideCsv = true;
//	
//	// the base URI for properties when loading rdf engines
//	protected String basePropURI= "";
//	protected final static String CONTAINS = "Contains";
//	
//	// use this boolean to determine if we db should be turned off 
//	// after creation and loaded back through smss watcher
//	// or if process will handle load that portion
//	protected boolean autoLoad = true;
//	protected Hashtable<String, String[]> baseRelations = new Hashtable<String, String[]>();
//
//	public abstract IDatabaseEngine importFileWithOutConnection(ImportOptions options) throws Exception;
//	
//	public abstract void importFileWithConnection(ImportOptions options) throws Exception;
//	/**
//	 * Loads the prop file for the CSV file
//	 * @param fileName	Absolute path to the prop file specified in the last column of the CSV file
//	 * @throws IOException 
//	 */
//	protected void openProp(String fileName) throws IOException {
//		Properties rdfPropMap = new Properties();
//		FileInputStream fileIn = null;
//		try {
//			fileIn = new FileInputStream(Utility.normalizePath(fileName));
//			rdfPropMap.load(fileIn);
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//			throw new FileNotFoundException("Could not find user-specified prop file located in header row in cell: " + fileName);
//		} catch (IOException e) {
//			e.printStackTrace();
//			throw new IOException("Could not read user-specified prop file located in header row in cell: " + fileName);
//		} finally{
//			try{
//				if(fileIn!=null)
//					fileIn.close();
//			}catch(IOException e) {
//				e.printStackTrace();
//			}
//		}
//		for(String name: rdfPropMap.stringPropertyNames()){
//			rdfMap.put(name, rdfPropMap.getProperty(name).toString());
//		}
//	}
//
//	protected void loadMetadataIntoEngine() {
//		Hashtable<String, String> hash = owler.getConceptHash();
//		String object = AbstractOwler.SEMOSS_URI_PREFIX + AbstractOwler.DEFAULT_NODE_CLASS;
//		for(String concept : hash.keySet()) {
//			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{hash.get(concept), RDFS.SUBCLASSOF + "", object, true});
//		}
//		hash = owler.getRelationHash();
//		object = AbstractOwler.SEMOSS_URI_PREFIX + AbstractOwler.DEFAULT_RELATION_CLASS;
//		for(String relation : hash.keySet()) {
//			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{hash.get(relation), RDFS.SUBPROPERTYOF + "", object, true});
//		}
//		hash = owler.getPropHash();
//		object = AbstractOwler.SEMOSS_URI_PREFIX + AbstractOwler.DEFAULT_PROP_CLASS;
//		for(String prop : hash.keySet()) {
//			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{hash.get(prop), RDF.TYPE + "", object, true});
//		}
//	}
//	
////	protected void processDisplayNames(){
////		displayNamesHash = DisplayNamesProcessor.generateDisplayNameMap(rdfMap, false);
////	}
//	
//	protected String[] prepareReader(String fileNames, String customBase, String owlFile, String bdPropFile){
//		String[] files = fileNames.trim().split(";");
//		prepEngineCreator(customBase, owlFile, bdPropFile);
//		return files;
//	} 
//
//	public String getInstanceURI(String nodeType) {
//		return customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ nodeType;
//	}
//
//	/**
//	 * Create and add all triples associated with relationship tabs
//	 * @param subjectNodeType					String containing the subject node type
//	 * @param objectNodeType					String containing the object node type
//	 * @param instanceSubjectName				String containing the name of the subject instance
//	 * @param instanceObjectName				String containing the name of the object instance
//	 * @param relName							String containing the name of the relationship between the subject and object
//	 * @param propHash							Hashtable that contains all properties
//	 */
//	public void createRelationship(String subjectNodeType, String objectNodeType, String instanceSubjectName,
//			String instanceObjectName, String relName, Hashtable<String, Object> propHash) {
//		subjectNodeType = Utility.cleanString(subjectNodeType, true);
//		objectNodeType = Utility.cleanString(objectNodeType, true);
//
//		instanceSubjectName = Utility.cleanString(instanceSubjectName, true);
//		instanceObjectName = Utility.cleanString(instanceObjectName, true);
//
//		// get base URIs for subject node at instance and semoss level
//		String subjectSemossBaseURI = owler.addConcept(subjectNodeType);
//		String subjectInstanceBaseURI = getInstanceURI(subjectNodeType);
//
//		// get base URIs for object node at instance and semoss level
//		String objectSemossBaseURI = owler.addConcept(objectNodeType);
//		String objectInstanceBaseURI = getInstanceURI(objectNodeType);
//
//		// create the full URI for the subject instance
//		// add type and label triples to database
//		String subjectNodeURI = subjectInstanceBaseURI + "/" + instanceSubjectName; 
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { subjectNodeURI, RDF.TYPE, subjectSemossBaseURI, true });
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { subjectNodeURI, RDFS.LABEL, instanceSubjectName, false });
//
//		// create the full URI for the object instance
//		// add type and label triples to database
//		String objectNodeURI = objectInstanceBaseURI + "/" + instanceObjectName; 
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { objectNodeURI, RDF.TYPE, objectSemossBaseURI, true });
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT,new Object[] { objectNodeURI, RDFS.LABEL, instanceObjectName, false });
//
//		// generate URIs for the relationship
//		relName = Utility.cleanPredicateString(relName);
//		String relSemossBaseURI = owler.addRelation(subjectNodeType, objectNodeType, relName);
//		String relInstanceBaseURI = customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + relName;
//
//		// create instance value of relationship and add instance relationship,
//		// subproperty, and label triples
//		String instanceRelURI = relInstanceBaseURI + "/" + instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName;
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceRelURI, RDFS.SUBPROPERTYOF, relSemossBaseURI, true });
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceRelURI, RDFS.LABEL, 
//				instanceSubjectName + Constants.RELATION_URI_CONCATENATOR + instanceObjectName, false });
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { subjectNodeURI, instanceRelURI, objectNodeURI, true });
//
//		addProperties("", instanceRelURI, propHash);
//	}
//
//	public void addNodeProperties(String nodeType, String instanceName, Hashtable<String, Object> propHash) {
//		//create the node in case its not in a relationship
//		instanceName = Utility.cleanString(instanceName, true);
//		nodeType = Utility.cleanString(nodeType, true); 
//		String semossBaseURI = owler.addConcept(nodeType);
//		String instanceBaseURI = getInstanceURI(nodeType);
//		String subjectNodeURI = instanceBaseURI + "/" + instanceName;
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, RDF.TYPE, semossBaseURI, true});
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, RDFS.LABEL, instanceName, false});
//
//		addProperties(nodeType, subjectNodeURI, propHash);
//	}
//
//	public void addProperties(String subjectNodeType, String instanceURI, Hashtable<String, Object> propHash) {
//
//		// add all properties
//		Enumeration<String> propKeys = propHash.keys();
//		if (basePropURI.equals("")) {
//			basePropURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
//		}
//		// add property triple based on data type of property
//		while (propKeys.hasMoreElements()) {
//			String key = propKeys.nextElement().toString();
//			String propURI = basePropURI + "/" + Utility.cleanString(key, true);
//			// logger.info("Processing Property " + key + " for " + instanceURI);
//			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { propURI, RDF.TYPE, basePropURI, true });
//			if (propHash.get(key) instanceof Number) {
//				Double value = ((Number) propHash.get(key)).doubleValue();
//				// logger.info("Processing Double value " + value);
//				engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, value.doubleValue(), false });
//				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
//					owler.addProp(subjectNodeType, key, "DOUBLE");
//				}
//			} else if (propHash.get(key) instanceof Date) {
//				Date value = (Date) propHash.get(key);
//				// logger.info("Processing Date value " + value);
//				engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, value, false });
//				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
//					owler.addProp(subjectNodeType, key, "DATE");
//				}
//			} else if (propHash.get(key) instanceof Boolean) {
//				Boolean value = (Boolean) propHash.get(key);
//				// logger.info("Processing Boolean value " + value);
//				engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, value.booleanValue(), false });
//				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
//					owler.addProp(subjectNodeType, key, "BOOLEAN");
//				}
//			} else {
//				String value = propHash.get(key).toString();
//				if (value.equals(Constants.PROCESS_CURRENT_DATE)) {
//					// logger.info("Processing Current Date Property");
//					insertCurrentDate(propURI, basePropURI, instanceURI);
//				} else if (value.equals(Constants.PROCESS_CURRENT_USER)) {
//					// logger.info("Processing Current User Property");
//					insertCurrentUser(propURI, basePropURI, instanceURI);
//				} else {
//					String cleanValue = Utility.cleanString(value, true, false, true);
//					// logger.info("Processing String value " + cleanValue);
//					engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[] { instanceURI, propURI, cleanValue, false });
//				}
//				if(subjectNodeType != null && !subjectNodeType.isEmpty()) {
//					owler.addProp(subjectNodeType, key, "STRING");
//				}
//			}
//		}
//	}
//
//	/**
//	 * Insert the current user as a property onto a node if property is "PROCESS_CURRENT_USER"
//	 * @param propURI 			String containing the URI of the property at the instance level
//	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
//	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
//	 */
//	private void insertCurrentUser(String propURI, String basePropURI, String subjectNodeURI) {
//		String cleanValue = System.getProperty("user.name");
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propURI, RDF.TYPE, basePropURI, true});
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, propURI, cleanValue, false});
//	}
//
//	/**
//	 * Insert the current date as a property onto a node if property is "PROCESS_CURRENT_DATE"
//	 * @param propURI 			String containing the URI of the property at the instance level
//	 * @param basePropURI 		String containing the base URI of the property at SEMOSS level
//	 * @param subjectNodeURI 	String containing the URI of the subject at the instance level
//	 */
//	private void insertCurrentDate(String propInstanceURI, String basePropURI, String subjectNodeURI) {
//		Date dValue = new Date();
//		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//		String date = df.format(dValue);
//		Date dateFormatted;
//		try {
//			dateFormatted = df.parse(date);
//			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{propInstanceURI, RDF.TYPE, basePropURI, true});
//			engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{subjectNodeURI, propInstanceURI, dateFormatted, false});
//		} catch (ParseException e) {
//			logger.error("ERROR: could not parse date: " + date);
//		}
//	}
//	
//	public boolean isAutoLoad() {
//		return autoLoad;
//	}
//
//	public void setAutoLoad(boolean autoLoad) {
//		this.autoLoad = autoLoad;
//	}
//	
//	/**
//	 * Setter to store the metamodel created by user as a Hashtable
//	 * @param data	Hashtable<String, String> containing all the information in a properties file
//	 */
//	public void setRdfMapArr(Hashtable<String, String>[] rdfMapArr) {
//		this.rdfMapArr = rdfMapArr;
//		this.propFileExist = false;
//	}
//	
//	/**
//	 * Set the prop file location
//	 * @param propFileLocation
//	 */
//	public void setPropFile(String propFileLocation) {
//		this.propFile = propFileLocation;
//		this.propFileExist = true;
//		this.propFileDefinedInsideCsv = false;
//	}
//	
//	public void setObjectValueMap(Map<String, String> objectValueMap) {
//		this.objectValueMap = objectValueMap;
//	}
//	
//	public void setObjectTypeMap(Map<String, String> objectTypeMap) {
//		this.objectTypeMap = objectTypeMap;
//	}
//
//	public void setRowKey(String rowKey) {
//		this.rowKey = rowKey;
//	}
//	
//	public void setCreateIndexes(boolean createIndexes) {
//		this.createIndexes = createIndexes;
//	}
//	
//}
