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
package prerna.nameserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hp.hpl.jena.vocabulary.RDFS;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.MetaHelper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PersistentHash;
import prerna.util.Utility;

public class AddToMasterDB extends ModifyMasterDB {

	// http://semoss.org/ontologies/meta/engine
	private String semossEngine = Constants.BASE_URI + "meta/engine";
	// http://semoss.org/ontologies/Relation/presentin
	private String presentIn = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/presentin";
	// http://semoss.org/ontologies/Relation/conceptual
	private String conceptual = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/conceptual";
	// http://semoss.org/ontologies/Relation/logical
	private String logical = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/logical";
	// http://semoss.org/ontologies/Relation/modified
	private String modified = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/modified";
	// http://semoss.org/ontologies/Relation/engineType
	private String engineType = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/engineType";

	// For testing, change to your own local directories
	private static final String WS_DIRECTORY = "C:/Users/pkapaleeswaran/Workspacej3";
	private static final String DB_DIRECTORY = WS_DIRECTORY + "/SemossWeb/db";
	
	Connection conn = null;
	
	private PersistentHash conceptIdHash = new PersistentHash();
	
	/*
	 *  a.	Need multiple primary keys
		b.	Need a way to specify property with same name across the multiple concepts
		c.	Need for multiple foreign keys
		d.	Being able to handle loop elegantly
		e.	Being able to link based on multiple keys i.e. I should be able to query the database through 2 keys instead of one
		f.	Same as e. but also being able to do it across tables on create
		g.	Need a way to tag instance level data so it can be compared and new recommendations can be made i.e. being able to send the data. Need some way of doing a PKI so the actual data is never sent
	 * 
	 * 
	 */
	
	
	public AddToMasterDB(String localMasterDbName) {
		super(localMasterDbName);
	}
	
	public AddToMasterDB() {
		super();
	}

	public boolean registerEngineLocal(Properties prop) {
		// registers this engine
		// I am not sure I need the engine here
		// all that I need is the OWL file
		// Pick up the concepts
		// Generate the hypernyms based on the concept
		// Pick up the properties
		// Generate hypernyms for the properties
		// Insert this into local master in the following fashion

		// ConceptName typeOf Concept
		// ConceptName composedOf Hypernym
		// ConceptName_PhysicalName_Engine has Engine
		// ConceptName_PhysicalName_Engine has ConceptName
		// ConceptName_PhysicalName_Engine has PhysicalName
		// ConceptName_PhysicalName_Engine related_to ConceptName_PhysicalName_Engine
		// This Stuff is new below i.e. adding property
		// PropertyName typeof Property
		// PropertyName composedOf Hypernym
		// PropertyName_PhysicalName_Engine has Engine
		// PropertyName_PhysicalName_Engine has PropertyName
		// PropertyName_PhysicalName_Engine has PhysicalName
		// PropertyName_PhysicalName_Engine belongsTo ConceptName_PhysicalName_Engine

		// grab the local master engine
		IEngine localMaster = Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		
		getConnection(localMaster);
		

		// get the base folder
		String baseFolder = null;
		try {
			baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		} catch (Exception ignored) {
			// just set to default location
			// used for testing if DIHelper not loaded
			baseFolder = "C:/workspace/Semoss_Dev";
		}

		// we want to load in the OWL for the engine we want to synchronize into the
		// the local master
		// get the owl relative path from the base folder to get the full path
		String owlFile = baseFolder + "/" + prop.getProperty(Constants.OWL);
		
		// owl is stored as RDF/XML file
		RDFFileSesameEngine rfse = new RDFFileSesameEngine();
		rfse.openFile(owlFile, null, null);
		
		// we create the meta helper to facilitate querying the engine OWL
		MetaHelper helper = new MetaHelper(rfse, null, null);

		// also get the last modified date of the OWL file to store
		// into the local master
		File file = new File(owlFile);
		Date modDate = new Date(file.lastModified());

		// insert the engine first
		// engine is a type of engine
		// keep the engine URI
		String engineName = prop.getProperty(Constants.ENGINE);
		LOGGER.info("Starting to synchronize engine ::: " + engineName);
		
		// create the engine URI
		// http://semoss.org/ontologies/meta/engine/ENGINE_NAME
		String engineUri = Constants.BASE_URI +"" + "meta/engine/" + engineName;

		/*
		 * TRIPLE INSERTION EXAMPLE
		 * 
		 * {	
		 * 		<http://semoss.org/ontologies/meta/engine/ENGINE_NAME> 
		 *  	<http://semoss.org/ontologies/Relation/modified>
		 *  	DATE_LITERAL 
		 * }
		 */
		/*
		 * Modification 
		 * Engine | Created Date | Modification Date | Type
		 * 
		 * 
		 */
		addData(engineUri, modified, modDate, false, localMaster);

		/*
		 * TRIPLE INSERTION EXAMPLE
		 * 
		 * {	
		 * 		<http://semoss.org/ontologies/meta/engine/ENGINE_NAME> 
		 *  	<RDF.TYPE>
		 *  	<http://semoss.org/ontologies/meta/engine>
		 * }
		 * 
		 */
		
		/**
		 * Not needed - covered in the type above
		 * 
		 */
		
		addData(engineUri, RDF.TYPE + "", semossEngine, true, localMaster);

		// grab the engine type 
		// if it is RDBMS vs RDF
		IEngine.ENGINE_TYPE engineType = null;
		String engineTypeString = null;
		if(prop.getProperty("ENGINE_TYPE").contains("RDBMS")) {
			engineType = IEngine.ENGINE_TYPE.RDBMS;
			engineTypeString = "TYPE:RDBMS";
		} else if(prop.getProperty("ENGINE_TYPE").contains("Tinker")) {
			engineType = IEngine.ENGINE_TYPE.TINKER;
			engineTypeString = "TYPE:TINKER";
		} else if(prop.getProperty("ENGINE_TYPE").contains("Solr")) {
			engineType = IEngine.ENGINE_TYPE.SOLR;
			engineTypeString = "TYPE:SOLR";
		} else 	if(prop.getProperty("ENGINE_TYPE").contains("RNative")) {
			engineType = IEngine.ENGINE_TYPE.R; // process it as a flat file I bet 
			engineTypeString = "TYPE:R";
		}else {
			engineType = IEngine.ENGINE_TYPE.SESAME;
			engineTypeString = "TYPE:RDF";
		}
		
		String uniqueId = UUID.randomUUID().toString();
		conceptIdHash.put(engineName+"_ENGINE", uniqueId);
		String [] colNames = {"ID", "EngineName", "ModifiedDate", "Type"};
		String [] types = {"varchar(800)", "varchar(800)", "timestamp", "varchar(800)"};
		Object [] engineData = {uniqueId, engineName, new java.sql.Timestamp(modDate.getTime()), engineTypeString, "true"};
		makeQuery("Engine", colNames, types, engineData);
		
		

		/*
		 * TRIPLE INSERTION EXAMPLE
		 * 
		 * {	
		 * 		<http://semoss.org/ontologies/meta/engine/ENGINE_NAME> 
		 *  	<RDF.TYPE>
		 *  	<TYPE:RDF> or <TYPE:RDBMS>
		 * }
		 */
		addData(engineUri, this.engineType, engineTypeString, true, localMaster);

		// get the list of all the physical names
		// false denotes getting the physical names
		Vector <String> concepts = helper.getConcepts(false);
		LOGGER.info("For engine " + engineName + " : Total Concepts Found = " + concepts.size());

		// maps from the concept name to the physical composite
		Hashtable <String, String> physicalComposite = new Hashtable <String, String>();

		// iterate through all the concepts to insert into the local master
		for(int conceptIndex = 0; conceptIndex< concepts.size(); conceptIndex++) {
			String conceptToInsert = concepts.get(conceptIndex);
			LOGGER.debug("Processing concept ::: " + conceptToInsert);
			masterConcept(conceptToInsert, engineUri, physicalComposite, localMaster, helper, engineType);
		}
		
		return true;
	}
	
	private void makeQuery(String tableName, String [] colNames, String [] types, Object [] data)
	{
		//System.out.println("------------------------------------------------");
		//System.out.println(data[0]);
		/*for(int dataIndex = 1;dataIndex < data.length;dataIndex++)
			System.out.print("<" + data[dataIndex] + ">");
		System.out.println();
		*/
		
		String createString = makeCreate(tableName, colNames, types);
		String insertString = makeInsert(tableName, colNames, types, data);
		try
		{
			conn.createStatement().execute(createString);
			conn.createStatement().execute(insertString);
		}catch (Exception ex)
		{
			ex.printStackTrace();
		}
		
	}
	
	private String makeCreate(String tableName, String [] colNames, String [] types)
	{
		StringBuilder retString = new StringBuilder("CREATE TABLE IF NOT EXISTS "+ tableName + " (" + colNames[0] + " " + types[0]);
		for(int colIndex = 1;colIndex < colNames.length;colIndex++)
			retString = retString.append(" , " + colNames[colIndex] + "  " + types[colIndex]);
		retString = retString.append(")");
		
		//System.out.println("CREATE >>> " + retString);
		
		return retString.toString();
		
	}

	
	private String makeInsert(String tableName, String [] colNames, String [] types, Object [] data)
	{
		StringBuilder retString = new StringBuilder("INSERT INTO "+ tableName + " (" + colNames[0]);
		for(int colIndex = 1;colIndex < colNames.length;colIndex++)
			retString = retString.append(" , " + colNames[colIndex]);

		String prefix = "'";
		
		retString = retString.append(") VALUES (" + prefix + data[0] + prefix);
		for(int colIndex = 1;colIndex < colNames.length;colIndex++)
		{
			if(types[colIndex].contains("varchar") || types[colIndex].toLowerCase().contains("timestamp") || types[colIndex].toLowerCase().contains("date"))
				prefix = "'";
			else
				prefix = "";
			retString = retString.append(" , " + prefix + data[colIndex] + prefix);
		}
		retString = retString.append(")");
		
		//System.out.println("INSERT >>> " + retString);
		
		return retString.toString();
	}
	

	
	private Connection getConnection(IEngine localMaster)
	{
		if(conn == null)
		{
	    	try {
	    		conn = ((RDBMSNativeEngine)localMaster).makeConnection();
	    		conceptIdHash = ((RDBMSNativeEngine)localMaster).getConceptIdHash();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return conn;
	}
	
	private void masterConcept(String physicalConceptUri, 
			String engineUri, 
			Hashtable <String, String> previousConcepts, 
			IEngine engine, 
			MetaHelper helper, 
			IEngine.ENGINE_TYPE engineType)
	{
		// get the conceptual URI for the concept
		// http://semoss.org/ontologies/Concept/CLEAN_CONCEPT_NAME
		// clean concept name above is PKQL acceptable (i.e. alpha-numeric-underscore characters only)
		String conceptualUri = helper.getConceptualUriFromPhysicalUri(physicalConceptUri);
		
		// get the concept instance
		// in rdf, this just returns the instance name
		// in rdbms, this returns Table_TABLE_NAME + Column_COLUMN_NAME 
		// ('+' is not actual present, but there is no space in between the actual table name and the column tag)
		String conceptInstance = Utility.getInstanceName(physicalConceptUri, engineType);
		
		// as a note
		// for RDBMS, this will be the table name, not the primary key in the table
		String physicalInstance = Utility.getInstanceName(physicalConceptUri); 
		
		// get the instance of engine
		String engineInstance = Utility.getInstanceName(engineUri);

		// get the engine composite
		// http://semoss.org/ontologies/Concept/ENGINE_NAME_PHYSICAL_NAME
		String engineComposite = Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS + "/" + engineInstance + "_" + conceptInstance;


		// get the logical concept name
		// as a note
		// this is the same as the physical concept URI in RDF
		// but it is different for RDBMS as it does not contain the primary key
		String logicalConcept = Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS + "/" + physicalInstance;

		// base SEMOSS concept
		String semossConcept = Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS;

		/*
		 * Add the physical concept as a subclass of concept
		 * 
		 * TRIPLE INSERTION EXAMPLE
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/Title> 
		 *  	<RDFS.subClassOf>
		 *  	<http://semoss.org/ontologies/Concept>
		 * }
		 * or 
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/TABLE_NAME/COLUMN_NAME> 
		 *  	<RDFS.subClassOf>
		 *  	<http://semoss.org/ontologies/Concept>
		 * }
		 */
		//>>addData(physicalConceptUri, RDFS.subClassOf+ "", semossConcept, true, engine);

		/*
		 * Add the engine composite as a subclass of concept
		 * Note, for RDBMS, the value after the engine name is the table name
		 * 
		 * TRIPLE INSERTION EXAMPLE
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/Movie_Title> 
		 *  	<RDFS.subClassOf>
		 *  	<http://semoss.org/ontologies/Concept>
		 * }
		 */
		//>>addData(engineComposite, RDFS.subClassOf+ "", semossConcept, true, engine);

		/*
		 * Add engine composite to be present in the engine
		 * Note, for RDBMS, the value after the engine name is the table name
		 * 
		 * TRIPLE INSERTION EXAMPLE
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/Movie_Title> 
		 *  	<http://semoss.org/ontologies/Relation/presentin>
		 *  	<http://semoss.org/ontologies/meta/engine/ENGINE_NAME>
		 * }
		 */
		//>>addData(engineComposite, presentIn, engineUri, true, engine);

		/*
		 * Add engine composite as a type of the physical URI
		 * Note, for RDBMS, the value after the engine name is the table name
		 * 
		 * TRIPLE INSERTION EXAMPLE
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/Movie_Title> 
		 *  	<RDF.TYPE>		
		 * 		<http://semoss.org/ontologies/Concept/Title> 
		 * }
		 * or 
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/Movie_Title> 
		 *  	<RDF.TYPE>
		 * 		<http://semoss.org/ontologies/Concept/TABLE_NAME/COLUMN_NAME> 
		 * }
		 */
		//>>addData(engineComposite, RDF.TYPE+"", physicalConceptUri, true, engine);

		/*
		 * Add physical uri to conceptual uri
		 * TRIPLE INSERTION EXAMPLE
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/Movie_Title> 
		 *  	<http://semoss.org/ontologies/Relation/conceptual>
		 *  	<http://semoss.org/ontologies/Concept/Title>
		 * }
		 * or 
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/Movie_TABLE_NAME> 
		 *  	<http://semoss.org/ontologies/Relation/conceptual>
		 *  	<http://semoss.org/ontologies/Concept/TABLE_NAME>
		 * }
		 */
		//>>addData(engineComposite, conceptual, conceptualUri, true, engine);

		/*
		 * TODO: Come back to this to add multiple logical names
		 * We make one logical which is the physical URI
		 * Except for RDBMS, since it does not contain the primary key
		 * TRIPLE INSERTION EXAMPLE
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/Movie_Title> 
		 *  	<http://semoss.org/ontologies/Relation/logical>
		 *  	<http://semoss.org/ontologies/Concept/Title>
		 * }
		 * or 
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/Movie_TABLE_NAME> 
		 *  	<http://semoss.org/ontologies/Relation/logical>
		 *  	<http://semoss.org/ontologies/Concept/TABLE_NAME>
		 * } 
		 */
		//>>addData(engineComposite, logical, logicalConcept, true, engine);
		
		/*
		 * Also add the conceputal name as a logical name
		 * TRIPLE INSERTION EXAMPLE
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/Movie_Title> 
		 *  	<http://semoss.org/ontologies/Relation/logical>
		 *  	<http://semoss.org/ontologies/Concept/Title>
		 * }
		 * or 
		 * {	
		 * 		<http://semoss.org/ontologies/Concept/Movie_TABLE_NAME> 
		 *  	<http://semoss.org/ontologies/Relation/logical>
		 *  	<http://semoss.org/ontologies/Concept/TABLE_NAME>
		 * } 
		 */
		//>>addData(engineComposite, logical, conceptualUri, true, engine);
		
		addConcept(engineInstance, physicalInstance, physicalInstance, helper, physicalConceptUri);
		
		// now get all the keys for the concept
		List<String> keys = helper.getKeys4Concept(physicalConceptUri, false);
		
		/*
		 * Add all the keys for the concept
		 * TRIPLE INSERTION EXAMPLE
		 * {
		 * 		<http://semoss.org/ontologies/Concept/Movie_Title>
		 * 		<URI:KEY>
		 * 		<http://semoss.org/ontologies/Relation/Contains/Movie_Title/Movie_Title>
		 * }
		 * or
		 * {
		 * 		<http://semoss.org/ontologies/Concept/Movie_TABLE_NAME>
		 * 		<URI:KEY>
		 * 		<http://semoss.org/ontologies/Relation/Contains/Movie_Title/Movie_TABLE_NAME>
		 * }
		 */
		// iterate through and add all the keys		
		for (String physicalKeyUri : keys) {
			String iKey = Utility.getInstanceName(physicalKeyUri, engineType);
			String engineKeyComposite = Constants.BASE_URI + Constants.DEFAULT_PROPERTY_CLASS + "/" + engineInstance + "_" + iKey;
			//>>addData(engineComposite, Constants.META_KEY, engineKeyComposite, true, engine);
		}
		
		// adding the physical URI to the engine composite
		previousConcepts.put(physicalConceptUri, engineComposite);

		// now get all the properties for the concept
		// false will return the physical URI for the concepts
		List<String> properties = helper.getProperties4Concept(physicalConceptUri, false);

		// iterate through and add all the properties
		for(int propIndex = 0;propIndex < properties.size(); propIndex++) {
			String physicalPropUri = properties.get(propIndex);
			LOGGER.debug("For concept = " + physicalConceptUri + " adding property ::: " + physicalPropUri);
			addProperty(engineComposite, physicalPropUri, engineUri, engine, helper, engineType, conceptInstance, physicalInstance); 
		}
		
		// only need to process relationships in one direction
		
		Vector <String[]> otherConcepts = helper.getFromNeighborsWithRelation(physicalConceptUri, 0);		
		
		masterOtherConcepts(engine, otherConcepts, previousConcepts, engineInstance, conceptInstance, engineComposite, true, engineType, physicalInstance, helper);
		
		// need to introduce another class called get composite neighbors
		// with the relation /Relation/Composite - where it is also a subclass of relation ?
		// the composite relation will contain all the composite relationship in a single string
		// Where the compositions will be separated by a :	
		// /Relation/Composite/Title.Title.Studio.Title_FK:Title.Title.Nominated.Title_FK

	}
	
	private void addConcept(String engineInstance, String physicalInstance, String mainInstance, MetaHelper helper, String Uri)
	{
		/**
		 * All Concepts are of the form
		 *  Concept | Conceptual Name | Logical Name | DomainArea | ID
		 *  Need to figure out domain area
		 * In the beginning - everything is just physical
		 */

		String uniqueId = null;
		String [] colNames;
		String [] types;
		String [] conceptData;
		// need to make the domain also to be an ID
		if(conceptIdHash.containsKey(physicalInstance+"_CONCEPTUAL"))
		{
			uniqueId = conceptIdHash.get(physicalInstance+"_CONCEPTUAL");
		}
		else
		{
			uniqueId = UUID.randomUUID().toString();
			conceptIdHash.put(physicalInstance+"_CONCEPTUAL", uniqueId);
			colNames = new String[]{"LocalConceptID", "ConceptualName", "LogicalName", "DomainName", "GlobalID"};
			types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
			// making the logical name to be to upper case
			conceptData = new String[]{uniqueId, physicalInstance, physicalInstance, "NewDomain", ""};
			String tableName = "Concept";
			makeQuery(tableName, colNames, types, conceptData);
		}
		/**
		 * Engine Specific Concept Data
		 *  Engine | Physical Concept | Main Physical Concept (Same as Physical for concept different for property) | ID | Concept ID (refers to the id of the concept in ConceptTable | Primary Key? 
		 *  Primary Key - may be useful in terms of getting to the concept
		 *  i.e. the table should be the same for property ?
		 *  We need to make sure that the concept in previous step doesn't always insert but gives the id as well
		 *  Do we need the main physical Concept - ok.. so this could be the table in the case of RDBMS without which you cannot bring it up
		 *  but there could be many of these - in which case we should show the user about it ? or qualify it with the table name ?
		 * 
		 */
		
		String dataType = "";
		String originalType = "";
		if(helper != null) {
			dataType = helper.getDataTypes(Uri);
			if(dataType == null) {
				originalType = "STRING";
				dataType = "STRING";
			} else {
				originalType = dataType;
				dataType = dataType.replace("TYPE:", "");
			}
		}

		if(dataType.equalsIgnoreCase("STRING") || dataType.toUpperCase().contains("VARCHAR"))
			dataType = "STRING";
		else if(dataType.equalsIgnoreCase("DOUBLE") || dataType.toUpperCase().contains("FLOAT"))
			dataType = "DOUBLE";
		else if(dataType.equalsIgnoreCase("DATE") || dataType.toUpperCase().contains("TIMESTAMP"))
			dataType = "DATE";

		if(!conceptIdHash.containsKey(physicalInstance+  "_" + engineInstance + "_PHYSICAL"))
		{
			String conceptId = uniqueId;
			uniqueId = UUID.randomUUID().toString();
			String engineId = conceptIdHash.get(engineInstance + "_ENGINE");
			conceptIdHash.put(physicalInstance + "_" + engineInstance + "_PHYSICAL", uniqueId);
			colNames = new String[]{"Engine", "PhysicalName", "ParentPhysicalID", "PhysicalNameID", "LocalConceptID", "PK", "Property", "Original_Type", "Property_Type"};
			types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "boolean", "boolean", "varchar(800)","varchar(800)"};
			String [] conceptInstanceData = {engineId, physicalInstance, uniqueId, uniqueId, conceptId, "TRUE", "FALSE", originalType, dataType};
			makeQuery("EngineConcept", colNames, types, conceptInstanceData);
		}
	}
	
	
	
	private void masterOtherConcepts(IEngine engine, 
			Vector <String[]> otherConcepts, 
			Hashtable <String, String> previousConcepts, 
			String engineInstance, 
			String conceptInstance, 
			String engineComposite, 
			boolean from, 
			IEngine.ENGINE_TYPE engineType, String mainConceptInstance, MetaHelper helper)
	{
		for(int otherIndex = 0;otherIndex < otherConcepts.size();otherIndex++)
		{
			String otherConcept = otherConcepts.get(otherIndex)[0];
			String otherRelation = otherConcepts.get(otherIndex)[1];
			String iOtherConcept = Utility.getInstanceName(otherConcept, engineType);
			String iOtherRelation = Utility.getInstanceName(otherRelation);
			String otherEngineConceptComposite = previousConcepts.get(otherConcept);
			
			
			if(previousConcepts.containsKey(otherConcept))
			{
				otherEngineConceptComposite = previousConcepts.get(otherConcept);
			}
			else
			{
				otherEngineConceptComposite = Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS + "/" + engineInstance + "_" + iOtherConcept;
				previousConcepts.put(otherConcept, otherEngineConceptComposite);
				addConcept(engineInstance, Utility.getInstanceName(otherConcept), Utility.getInstanceName(otherConcept), helper, otherConcept);
			}
			
			String relationCompositeName = null;
			
			/*
			// we only need to process in one direction!
//			if(from)
//			{
				relationCompositeName = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/" + engineInstance + "_" + iOtherConcept + "_" + conceptInstance ;
				addData(otherEngineConceptComposite, relationCompositeName, engineComposite, true, engine);
//			}
//			else
//			{					
//				relationCompositeName = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/" + engineInstance + "_" + conceptInstance + "_" + iOtherConcept ;
//				addData(engineComposite, relationCompositeName, otherEngineConceptComposite, true, engine);
//			}
			LOGGER.debug("Added Relation.... " + relationCompositeName);
			
			addData(relationCompositeName, RDFS.subPropertyOf + "", Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS, true, engine);
			*/
			
			/**
			 * Create a conceptual relationship and then the actual relationship
			 * first piece is conceptual
			 * ID, Source Conceptual ID, Target Conceptual ID, GLOBAL ID   
			 */
			String otherConceptInstance = Utility.getInstanceName(otherConcept);
			
			String [] colNames = {"ID", "SourceID", "TargetID", "GlobalID"};
			String [] types = {"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
			String relId = null;
			if(!conceptIdHash.containsKey(mainConceptInstance + "_" + otherConceptInstance + "_RELATION"))
			{
				relId = UUID.randomUUID().toString();
				String conceptConceptualId = conceptIdHash.get(mainConceptInstance + "_CONCEPTUAL");
				String otherConceptualId = conceptIdHash.get(otherConceptInstance + "_CONCEPTUAL");
				String [] relData = {relId, conceptConceptualId, otherConceptualId, ""};
				makeQuery("Relation", colNames, types, relData);
				// I need to keep the relation name as well
				conceptIdHash.put(mainConceptInstance + "_" + otherConceptInstance + "_RELATION", relId);
			}
			else
				relId = conceptIdHash.get(mainConceptInstance + "_" + otherConceptInstance + "_RELATION");
			
			/**
			 * Relationships are kept only at the physical level - sorry that did not come out right.. but..
			 * need to accomodate for multiple foreign keys as well
			 *  
			 * Engine | Rel_ID| InstanceRelation ID | From Concept ID | To Concept ID | From Property ID | To Property ID 	
			 * 
			 * In the case of RDBMS the property is the same as concept ?
			 * In the case 
			 * 
			 */
			//System.out.println(conceptIdHash.thisHash);
			// for now I am not keeping ID.. but merely trying to get the property
			// need to make sure we balance for multiple foregin keys
			// need to get the IDs for the concepts
			if(!conceptIdHash.containsKey(engineInstance + "_" + mainConceptInstance + "_" + Utility.getInstanceName(otherConcept)+"_PHYSICAL"))
			{
				colNames = new String []{"Engine", "RelationID", "InstanceRelationID", "SourceConceptID", "TargetConceptID", "SourceProperty", "TargetProperty", "RelationName"}; //"DomainName"};
				types = new String[]{"varchar(800)", "varchar(800)","varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
				String conceptId = conceptIdHash.get(mainConceptInstance + "_" + engineInstance +"_PHYSICAL");
				String otherConceptId = conceptIdHash.get(Utility.getInstanceName(otherConcept) + "_" + engineInstance +"_PHYSICAL");
				String engineId = conceptIdHash.get(engineInstance + "_ENGINE");
				String uniqueId = UUID.randomUUID().toString();
				String [] conceptData = {engineId, relId, uniqueId, conceptId, otherConceptId, mainConceptInstance, Utility.getInstanceName(otherConcept), iOtherRelation};
				conceptIdHash.put(engineInstance + "_" + mainConceptInstance + "_" + Utility.getInstanceName(otherConcept)+"_PHYSICAL", uniqueId);
				makeQuery("EngineRelation", colNames, types, conceptData);
			}
		}
	}
	
	private void addProperty(
			String engineConceptComposite, 
			String physicalPropUri, 
			String engineUri, 
			IEngine engine,
			MetaHelper helper, 
			IEngine.ENGINE_TYPE engineType, 
			String conceptInstance,
			String physicalInstance)
	{
		String conceptualPropertyUri = helper.getConceptualUriFromPhysicalUri(physicalPropUri);
		
		String iProperty = Utility.getInstanceName(physicalPropUri, engineType);
		String engineName = Utility.getInstanceName(engineUri);
		String enginePropertyComposite = Constants.BASE_URI + Constants.DEFAULT_PROPERTY_CLASS + "/" + engineName + "_" + iProperty;
		
		// so I might need to do a couple of checks here
		// basically i also need to add a logical name
		// the logical name is purely just the last name
		// need to do this the messy way for now
		String lProperty = null;
		if(engineType == IEngine.ENGINE_TYPE.RDBMS || engineType == IEngine.ENGINE_TYPE.R)
			lProperty = Utility.getClassName(physicalPropUri);
		if(lProperty == null || lProperty.equalsIgnoreCase("Contains"))
			lProperty = Utility.getInstanceName(physicalPropUri);

		String logicalPropertyUri = Constants.BASE_URI + Constants.DEFAULT_PROPERTY_CLASS + "/" +lProperty;
	
		// I need to say this property in this engine is a type of property
		// and also specify this belongs to this engineConcept
		/*addData(enginePropertyComposite, RDF.TYPE + "", Constants.BASE_URI + Constants.DEFAULT_PROPERTY_CLASS, true, engine);
		addData(enginePropertyComposite, RDF.TYPE + "", physicalPropUri, true, engine);
		
		// present in the engine
		addData(enginePropertyComposite, presentIn, engineUri, true, engine);
		
		// engine concept has this engine property
		addData(engineConceptComposite, OWL.DATATYPEPROPERTY + "", enginePropertyComposite, true, engine);		
		
		// also add the logical name
		addData(enginePropertyComposite, logical, logicalPropertyUri, true, engine);
		
		// also add the conceptual as a logical name
		addData(enginePropertyComposite, logical, conceptualPropertyUri, true, engine);
		
		// add in conceptual name
		addData(enginePropertyComposite, conceptual, conceptualPropertyUri, true, engine);
		*/
		/**
		 * All Concepts are of the form
		 *  Concept | Conceptual Name | Logical Name | DomainArea | ID
		 *  Need to figure out domain area
		 * 
		 */
		String dataType = "";
		String originalType = "";
		if(helper != null) {
			dataType = helper.getDataTypes(physicalPropUri);
			originalType = dataType;
			dataType = dataType.replace("TYPE:", "");
		}
		if(dataType.equalsIgnoreCase("STRING") || dataType.toUpperCase().contains("VARCHAR"))
			dataType = "STRING";
		else if(dataType.equalsIgnoreCase("DOUBLE") || dataType.toUpperCase().contains("FLOAT"))
			dataType = "DOUBLE";
		else if(dataType.equalsIgnoreCase("DATE") || dataType.toUpperCase().contains("TIMESTAMP"))
			dataType = "DATE";


		String [] colNames = {"LocalConceptID", "ConceptualName", "LogicalName", "DomainName", "GlobalID"};
		String [] types = {"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
		if(!conceptIdHash.containsKey(lProperty))
		{
			// how do we handle if there is a concept and property with the same name ?
			// it is treated the same

			String uniqueId = UUID.randomUUID().toString();
			conceptIdHash.put(lProperty, uniqueId);
			String [] conceptData = {uniqueId, lProperty, lProperty, "New Domain",""};
			makeQuery("Concept", colNames, types, conceptData);
		}
		
		/**
		 * Need a similar structure for properties as concept
		 * Should we just promote the properties to just concept ?
		 * Engine Specific Concept Data
		 *  Engine ID | Physical Concept | Main Physical Concept ID (Filled only when it is a property) | ID | Concept ID (refers to the id of the concept in ConceptTable | Primary Key? 
		 *  We need to make sure that the concept in previous step doesn't always insert but gives the id as well
		 * 

		 * 
		 */
		colNames = new String[]{"Engine", "PhysicalName", "ParentPhysicalID", "PhysicalNameID", "LocalConceptID", "PK", "Property", "Original_Type", "Property_Type"};
		types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "boolean", "boolean", "varchar(800)","varchar(800)"};

		String conceptId = conceptIdHash.get(lProperty);
		String uniqueId = UUID.randomUUID().toString();
		String engineId = conceptIdHash.get(engineName + "_ENGINE");
		String mainConceptId = conceptIdHash.get(physicalInstance + "_" + engineName + "_PHYSICAL");
		String [] conceptInstanceData = {engineId, lProperty, mainConceptId, uniqueId, conceptId, "FALSE", "TRUE", originalType, dataType};
		makeQuery("EngineConcept", colNames, types, conceptInstanceData);

	}
	
	/**
	 * Insert the triple into the local master database
	 * @param subject				The subject URI
	 * @param predicate				The predicate URI
	 * @param object				The object (either URI or Literal)
	 * @param concept				Boolean true if object is concept and false is object is literal
	 * @param engine				The local master engine to insert into
	 */
	private void addData(String subject, String predicate, Object object, boolean concept, IEngine engine)
	{
		Object [] statement = new Object[4];
		statement[0] = subject;
		statement[1] = predicate;
		statement[2] = object;
		statement[3] = new Boolean(concept);
		
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, statement);
	}
	
	// LOTS OF TESTING CODE BELOW...
	
	public void testMaster(IEngine engine)
	{
		
		/*
		// write the engine's meta data to a file
		File engineMetadataFile = new File(WS_DIRECTORY + "/testEngineMetadata.xml");
		writeEngine(engine, engineMetadataFile);
		
		// the meta data is in rdf/xml format
		RDFFileSesameEngine rfse = new RDFFileSesameEngine();
		rfse.openFile(engineMetadataFile.getAbsolutePath(), "RDF/XML", OWLER.BASE_URI);
		
		tryQueries(rfse);
		*/
		
		tryQueries(engine);
							
		/*
		String engineName = "Mov4";
		
		// first get rid of concept with properties
		String deleteQuery = "DELETE "
				+ "{"
				+ "?concept ?prop ?conceptProp."
				+ "?conceptProp ?type ?semossProp. "
				+ "?concept ?type ?semossConcept."
				+ "?concept ?subclass ?rdfConcept."
				+ "} "
				+ "WHERE"
				+ "{"
				+ "{?concept <http://semoss.org/ontologies/Relation/presentin>  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?concept ?in ?engine}"
				+ "{?concept ?prop ?conceptProp}"
				+ "{?concept ?type ?semossConcept}"
				+ "{?concept ?subclass ?rdfConcept}"
				+ "{?conceptProp ?type ?semossProp}"
				+ "FILTER(?in = <http://semoss.org/ontologies/Relation/presentin> &&"
				+ "		  ?prop = <http://www.w3.org/2002/07/owl#DatatypeProperty> &&"
				+ "		  ?type = <" + RDF.TYPE + "> &&"
				+ "		  ?subclass = <" + RDFS.subClassOf + ">"
				+ ")"
				+"}";

		engine.insertData(deleteQuery);

		// cool now without
		deleteQuery = "DELETE "
				+ "{"
				+ "?concept ?type ?semossConcept."
				+ "?concept ?subclass ?rdfConcept."
				+ "} "
				+ "WHERE"
				+ "{"
				+ "{?concept <http://semoss.org/ontologies/Relation/presentin>  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?concept ?in ?engine}"
				+ "{?concept ?type ?semossConcept}"
				+ "{?concept ?subclass ?rdfConcept}"
				+ "FILTER(?in = <http://semoss.org/ontologies/Relation/presentin> &&"
				+ "		  ?type = <" + RDF.TYPE + "> &&"
				+ "		  ?subclass = <" + RDFS.subClassOf + ">"
				+ ")"
				+"}";

		engine.insertData(deleteQuery);
		
		// relationships
		deleteQuery = "DELETE "
				+ "{"
				+ "?concept ?rel ?anotherConcept."
				+ "?rel ?subprop ?semRel."
				+ "} "
				+ "WHERE"
				+ "{"
				+ "{?concept <http://semoss.org/ontologies/Relation/presentin>  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?anotherConcept <http://semoss.org/ontologies/Relation/presentin>  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?rel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
				+ "{?rel ?subprop ?semRel}"
				+ "FILTER(?subprop = <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> &&"
				+ "		  ?semRel = <http://semoss.org/ontologies/Relation>"
				+ ")"
				+"}";
			 
		engine.insertData(deleteQuery);
		
		// last delete everything else
		deleteQuery = "DELETE "
				+ "{"
				+ "?concept ?in ?engine;"
				+ "} "
				+ "WHERE"
				+ "{"
				+ "{?concept ?in  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?concept ?in ?engine}"
				+ "FILTER(?in = <http://semoss.org/ontologies/Relation/presentin>"
				+ ")"
				+"}";

		engine.insertData(deleteQuery);
			
		// last delete modified
		deleteQuery = "DELETE {?engine ?anyPred ?anyObj} WHERE {"
				+ "{<http://semoss.org/ontologies/meta/engine/" + engineName + "> ?anyPred ?anyObj}"
				+ "{?engine ?anyPred ?anyObj}"
				+ "}";

		engine.insertData(deleteQuery);
		
		writeEngine(engine, new File("C:/users/pkapaleeswaran/workspacej3/SemossWeb/db/LocalMasterDatabase/NFileLocalMaster.xml"));
		*/
	}
	
	private void tryQueries(IEngine engine)
	{
		// show all the engines
		String engineQuery = "SELECT DISTINCT ?engine WHERE {"
				+ "{?engine <" + RDF.TYPE + "> <http://semoss.org/ontologies/meta/engine>}"
				+ "}";
		
		System.out.println("Engines... ");
		printQueryResult(engine, engineQuery, new String[] {"engine"});
		
		// show all the concepts
		String conceptQuery = "SELECT DISTINCT ?concept WHERE {"
				+ "{?concept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "FILTER(?concept != <http://semoss.org/ontologies/Concept>)"
				+ "}";
		
		System.out.println("Concepts... ");
		printQueryResult(engine, conceptQuery, new String[] {"concept"});
	
		// show all the concepts for a given engine
		String engineURI = "http://semoss.org/ontologies/meta/engine/Movie_DB";
		String conceptEngineQuery = "SELECT DISTINCT ?concept WHERE {"
				+ "{?concept <http://semoss.org/ontologies/Relation/presentin> <" + engineURI + ">}"
				+ "}";
		
		System.out.println("Concepts for " + engineURI + "... ");
		printQueryResult(engine, conceptEngineQuery, new String[] {"concept"});
		
		// show all the properties for a given concept
		String physicalConceptURI = "http://semoss.org/ontologies/Concept/Title/Title";
		String propQuery = "SELECT DISTINCT ?conceptProp ?engine WHERE {"
				+ "{?conceptComposite <" + RDF.TYPE + "> <" + physicalConceptURI + ">}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?concept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?conceptComposite <" + OWL.DATATYPEPROPERTY + "> ?propComposite}"
				+ "{?propComposite <http://semoss.org/ontologies/Relation/presentin> ?engine}"
				+ "{?propComposite <" + RDF.TYPE + "> ?conceptProp}"
				+ "FILTER(?concept != <http://semoss.org/ontologies/Concept>)"
				+"}";
		
		System.out.println("Properties for " + physicalConceptURI + "... ");
		printQueryResult(engine, propQuery, new String[] {"conceptProp", "engine"});
		
		/*
		 * How the engines are related
		 * so I have some physical concept present in a engine
		 * which is of a particular type
		 * and now this type needs to have a a different physical concept in a different engine
		 */
		String engineRelationsQuery = "SELECT DISTINCT ?someEngine ?fromConcept ?anotherEngine WHERE {"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?anotherConceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?anotherConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?anotherEngine}"
				+ "FILTER("
				+ "?someEngine != ?anotherEngine "
				+ "&& ?conceptComposite != ?anotherConceptComposite"
				+ ")}";
		
		System.out.println("How the engines are related... ");
		printQueryResult(engine, engineRelationsQuery, new String[] {"someEngine", "fromConcept", "anotherEngine"});

		// base query to get engine and concepts
		/*
		String baseQuery = "SELECT ?engine ?conceptLogical ?concept WHERE {{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?engine}"
				//+ "{?conceptComposite ?rel2 <" + Constants.CONCEPT_URI + ">}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
				+ "{?concept <http://semoss.org/ontologies/Relation/conceptual> ?conceptLogical}"
				+ "}";
		
		printQueryResult(engine, baseQuery, new String[] {"engine", "conceptLogical", "concept"});
		*/
		
		// show conceptual names if present
		String conceptualQuery = "SELECT DISTINCT (COALESCE(?conceptual, ?concept) AS ?retConcept) WHERE { "
				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "OPTIONAL {"
					+ "{?concept <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual }"
				+ "}" // end optional for conceptual names if present
				+ "}";
		
		System.out.println("Conceptual names... ");
		printQueryResult(engine, conceptualQuery, new String[] {"retConcept"});
				
		// show all the keys for a concept
		// note that this is an engine composite
		// one with mutiple
		String multipleKeyURI = "http://semoss.org/ontologies/Concept/Songs_Table_Song_COMPOSITE_ArtistColumn_Song_COMPOSITE_Artist";
		String keyQueryMultiple = "SELECT DISTINCT ?key WHERE { "
				+ "{<" + multipleKeyURI + "> <" + Constants.META_KEY + "> ?key}"
				+ "}";
		
		System.out.println("Keys for " + multipleKeyURI + "... ");
		printQueryResult(engine, keyQueryMultiple, new String[] {"key"});
		
		// one with a single key
		String singleKeyURI = "http://semoss.org/ontologies/Concept/Movie_DB_Table_TitleColumn_Title";
		String keyQuerySingle = "SELECT DISTINCT ?key WHERE { "
				+ "{<" + singleKeyURI + "> <" + Constants.META_KEY + "> ?key}"
				+ "}";
		
		System.out.println("Keys for " + singleKeyURI + "... ");
		printQueryResult(engine, keyQuerySingle, new String[] {"key"});
				
		// the concept to use from movies
		String conceptURI = "http://semoss.org/ontologies/Concept/Title";
		
		// downstream
		String downstreamQuery = "SELECT DISTINCT ?someEngine ?fromConcept ?fromLogical WHERE {"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?conceptComposite <" + logical + "> ?fromLogical}"
				+ "{?toConceptComposite <"+ logical + "> <" + conceptURI + ">}" // logical
				+ "{?conceptComposite ?someRel ?toConceptComposite}"
				+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
				+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
				+ "&& ?toConcept != ?someEngine)}";
		
		System.out.println("Downstream... ");
		printQueryResult(engine, downstreamQuery, new String[] {"someEngine", "fromConcept", "fromLogical"});
		
		// upstream
		String upstreamQuery = "SELECT DISTINCT ?someEngine ?fromConcept ?fromLogical WHERE {"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/logical> ?fromLogical}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/logical> <" + conceptURI + ">}" // change this back to logical
				+ "{?toConceptComposite ?someRel ?conceptComposite}"
				+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
				+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
				+ "&& ?toConcept != ?someEngine"
				//+ "&& ?fromLogical != <" + conceptURI + ">"
				+")}";
		
		System.out.println("Upstream... ");
		printQueryResult(engine, upstreamQuery, new String[] {"someEngine", "fromConcept", "fromLogical"});
		
		// get vertex and edges in a given engine
		String engineName = "Movie_DB";
		
		// gets the concepts and properties
		Hashtable <String, Hashtable> edgeAndVertex = new Hashtable<String, Hashtable>();
		String verticesQuery = "SELECT DISTINCT ?concept (COALESCE(?prop, ?noprop) as ?conceptProp) (COALESCE(?propLogical, ?noprop) as ?propLogicalF) ?conceptLogical WHERE "
				+ "{BIND(<http://semoss.org/ontologies/Relation/contains/noprop> AS ?noprop)"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?conceptComposite <" + logical + "> ?conceptLogical}"
				+ "OPTIONAL{"
				+ "{?conceptComposite <" + OWL.DATATYPEPROPERTY + "> ?propComposite}"
				+ "{?propComposite <" + RDF.TYPE + "> ?prop}"
				+ "{?propComposite <" + logical + "> ?propLogical}"
				+ "}"
				+ "}";
		
		makeVertices(engine, verticesQuery, edgeAndVertex);
		
		// get all the relationships
		// in a given database
		String relationshipsQuery = "SELECT DISTINCT ?fromConcept ?someRel ?toConcept WHERE {"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?toConceptComposite <"+ RDF.TYPE + "> ?toConcept}"
				+ "{?conceptComposite ?someRel ?toConceptComposite}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "}";
		
		System.out.println("Relationships... ");
		printQueryResult(engine, relationshipsQuery, new String[] {"fromConcept", "someRel", "toConcept"});

		// all concepts with no database
		String edgesQuery = "SELECT DISTINCT ?fromConcept ?someRel ?toConcept WHERE {"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?toConceptComposite <"+ RDF.TYPE + "> ?toConcept}"
				+ "{?conceptComposite ?someRel ?toConceptComposite}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
				+ "&& ?someRel != <http://semoss.org/ontologies/Relation>)"
				+ "}";
		
		// make the edges
		makeEdges(engine, edgesQuery, edgeAndVertex);
		
		// get everything linked to a keyword
		// so I dont have a logical concept
		// I cant do this
		Object[] vertArray = (Object[])edgeAndVertex.get("nodes").values().toArray();
		Object[] edgeArray = (Object[])edgeAndVertex.get("edges").values().toArray();
		Hashtable<String, Object[]> finalArray = new Hashtable<String, Object[]>();
		finalArray.put("nodes", vertArray);
		finalArray.put("edges", edgeArray);
		Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
        String output = gson.toJson(finalArray);
		System.out.println("Output is..++++++++++++++++++++");
		System.out.println(output);
		System.out.println("Output is..++++++++++++++++++++");
	}
	
	private void printQueryResult(IEngine engine, String query, String[] bindings) {
		System.out.println("Running Query " + query);
		TupleQueryResult tqr = (TupleQueryResult)engine.execQuery(query);
		try {
			while(tqr.hasNext())
			{
				BindingSet bs = tqr.next();
				for (String binding : bindings) {
					System.out.println(bs.getBinding(binding));
				}
				System.out.println();
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
	}
	
	// need to migrate to a different method eventually
	private void makeVertices(IEngine engine, String query, Hashtable<String, Hashtable> edgesAndVertices)
	{		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		Hashtable nodes = new Hashtable();
		if(edgesAndVertices.containsKey("nodes"))
			nodes = (Hashtable)edgesAndVertices.get("nodes");
		while(wrapper.hasNext())
		{
			ISelectStatement stmt = wrapper.next();
			String concept = stmt.getRawVar("concept") + "";
			String conceptProp = stmt.getRawVar("conceptProp") + "";
			String conceptLogical = stmt.getRawVar("conceptLogical") + "";
			String propLogical = stmt.getRawVar("propLogicalF") + "";
			
			// forcing RDBMS to make sure I get properties
			String physicalName = Utility.getInstanceName(concept, IEngine.ENGINE_TYPE.RDBMS);
			
			// forcing RDBMS to make sure I get properties in proper format
			String propName = Utility.getInstanceName(conceptProp, IEngine.ENGINE_TYPE.RDBMS);
			SEMOSSVertex thisVert = null;
			if(nodes.containsKey(concept))
				thisVert = (SEMOSSVertex)nodes.get(concept);
			else
			{
				thisVert = new SEMOSSVertex(concept);
				thisVert.propHash.put("physicalName", physicalName);
				thisVert.propHash.put("logicalName", Utility.getInstanceName(conceptLogical));
			}
			thisVert.setProperty(conceptProp, propName);
			Hashtable <String, String> propUriHash = (Hashtable<String, String>) thisVert.propHash.get("propUriHash");
			propUriHash.put(propName+"_LOGICAL", propLogical);
			nodes.put(concept, thisVert);
		}
		edgesAndVertices.put("nodes", nodes);
	}
	
	private void makeEdges(IEngine engine, String query, Hashtable<String, Hashtable> edgesAndVertices)
	{
		Hashtable nodes = new Hashtable();
		Hashtable edges = new Hashtable();
		if(edgesAndVertices.containsKey("nodes"))
			nodes = (Hashtable)edgesAndVertices.get("nodes");
		
		if(edgesAndVertices.containsKey("edges"))
			edges = (Hashtable)edgesAndVertices.get("edges");
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		while(wrapper.hasNext())
		{
			ISelectStatement stmt = wrapper.next();
			String fromConcept = stmt.getRawVar("fromConcept") + "";
			String toConcept = stmt.getRawVar("toConcept") + "";
			String relName = stmt.getRawVar("someRel") + "";
			
			SEMOSSVertex outVertex = (SEMOSSVertex)nodes.get(fromConcept);
			SEMOSSVertex inVertex = (SEMOSSVertex)nodes.get(toConcept);
			
			SEMOSSEdge edge = new SEMOSSEdge(outVertex, inVertex, relName);
			edges.put(relName, edge);
		}
		edgesAndVertices.put("edges", edges);
	}
	
	private static void writeEngine(IEngine engine, File file)
	{
		try {
			RDFFileSesameEngine rfse;
			if (engine instanceof BigDataEngine) {
				rfse = ((BigDataEngine) engine).getBaseDataEngine();
			} else if (engine instanceof RDBMSNativeEngine) {
				rfse = ((RDBMSNativeEngine) engine).getBaseDataEngine();
			} else {
				rfse = (RDFFileSesameEngine) engine;
			}
			RDFXMLWriter writer = new RDFXMLWriter(new FileWriter(file));
			rfse.writeData(writer);
		} catch (RepositoryException | RDFHandlerException | IOException e) {
			e.printStackTrace();
		}

	}
	
	private static Properties loadEngineProp(String engineName) throws IOException {
		try (FileInputStream fis = new FileInputStream(new File(determineSmssPath(engineName)))) {
			Properties prop = new Properties();
			prop.load(fis);
			return prop;
		}
	}
	
	private static String determineSmssPath(String engineName) {
		return DB_DIRECTORY + "/" + engineName + ".smss";
	}
	
	public void close(IEngine localMaster)
	{
		try {
    		((RDBMSNativeEngine)localMaster).commitRDBMS();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void main(String [] args) throws IOException
	{
		// load the RDF map for testing purposes
		String rdfMapDir = "C:/Users/pkapaleeswaran/Workspacej3/SemossDev";
		//System.getProperty("user.dir") 
		DIHelper.getInstance().loadCoreProp(rdfMapDir + "/RDF_Map.prop");
				
		// load the local master database
		Properties localMasterProp = loadEngineProp(Constants.LOCAL_MASTER_DB_NAME);
		IEngine localMaster = Utility.loadEngine(determineSmssPath(Constants.LOCAL_MASTER_DB_NAME), localMasterProp);	
		
		// test loading in a new engine to the master database
		
		// get the new engine
		String engineName = "Mv1";
		Properties engineProp = loadEngineProp(engineName);
		Utility.loadEngine(determineSmssPath(engineName), engineProp);
		
		// delete the engine from the master db so that we can re-add it fresh for testing purposes
		DeleteFromMasterDB deleter = new DeleteFromMasterDB();
		deleter.deleteEngineRDBMS(engineName);

		
		String engineName2 = "actor";
		Properties engineProp2 = loadEngineProp(engineName2);
		Utility.loadEngine(determineSmssPath(engineName), engineProp);
		
		// delete the engine from the master db so that we can re-add it fresh for testing purposes
		deleter = new DeleteFromMasterDB();
		deleter.deleteEngineRDBMS(engineName);

		// test registering the engine
		AddToMasterDB adder = new AddToMasterDB();
		adder.registerEngineLocal(engineProp);
		adder.registerEngineLocal(engineProp2);
		
		//adder.close();
		
		// test the master db
		
		//adder.testMaster(localMaster);
	}

	public Date getEngineDate(String engineName) {
		// TODO Auto-generated method stub
		java.util.Date retDate = null;
		Connection conn = ((RDBMSNativeEngine)this.masterEngine).makeConnection();
		if(((RDBMSNativeEngine)this.masterEngine).getTableCount() > 0)
		{
			try
			{
				String query = "select modifieddate from engine e "
							+ "where "
							+ "e.enginename = '" + engineName + "'";
				
				ResultSet rs = conn.createStatement().executeQuery(query);
				while(rs.next())
				{
					java.sql.Timestamp modDate = rs.getTimestamp(1);
					retDate = new java.util.Date(modDate.getTime());
				
				}
			}catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
		return retDate;
	}
	/**
	 * Creates a new table xrayconfigs
	 * inserts filesName and config file string
	 * 
	 * @param config
	 * @param fileName
	 */
	public void addXrayConfig(String config, String fileName) {
		// make statements
		// create table to local master
		String tableName = "xrayconfigs";
		String[] colNames = new String[] { "filename", "config" };
		String[] types = new String[] { "varchar(800)", "varchar(20000)" };
		
		String createNew = makeCreate("XRAYCONFIGS", colNames, types) + ";";
		// check if fileName exists


		IEngine localMaster = Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		getConnection(localMaster);
		try {
			conn.createStatement().execute(createNew);
			String configFile = MasterDatabaseUtility.getXrayConfigFile(fileName);
			if (configFile.length() > 0) {
				//create update statement
				String update = "UPDATE xrayconfigs SET config = '"+config+"' WHERE fileName = '"+fileName+"';";
				int updateCount = conn.createStatement().executeUpdate(update);

			} else {
				//make new insert
				String insertString = makeInsert(tableName, colNames, types,
						new Object[] { fileName, config });
				insertString += ";";
				conn.createStatement().execute(insertString);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
}
