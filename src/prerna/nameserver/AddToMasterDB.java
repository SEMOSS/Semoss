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
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.impl.MetaHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PersistentHash;
import prerna.util.Utility;

public class AddToMasterDB {

	private static final Logger LOGGER = LogManager.getLogger(AddToMasterDB.class.getName());

	// For testing, change to your own local directories
	private static final String WS_DIRECTORY = "C:/Users/pkapaleeswaran/Workspacej3";
	private static final String DB_DIRECTORY = WS_DIRECTORY + "/SemossWeb/db";
		
	private Connection conn = null;
	private PersistentHash conceptIdHash = null;
	
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
	
	public boolean registerEngineLocal(Properties prop) {
		String engineUniqueId = prop.getProperty(Constants.ENGINE);
		if(engineUniqueId == null) {
			engineUniqueId = UUID.randomUUID().toString();
		}
		return registerEngineLocal(prop, engineUniqueId);
	}
	
	public boolean registerEngineLocal(Properties prop, String engineUniqueId) {
		// grab the local master engine
		IEngine localMaster = Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		// establish the connection
		getConnection(localMaster);
		
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		
		// we want to load in the OWL for the engine we want to synchronize into the
		// the local master
		// get the owl relative path from the base folder to get the full path
		String owlFile = SmssUtilities.getOwlFile(prop).getAbsolutePath();

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
		LOGGER.info("Starting to synchronize engine ::: " + engineName);
		
		// grab the engine type 
		// if it is RDBMS vs RDF
		IEngine.ENGINE_TYPE engineType = null;
		String engineTypeString = null;
		String propEngType = prop.getProperty("ENGINE_TYPE");
		if(propEngType.contains("RDBMS") || propEngType.contains("Impala")) {
			engineType = IEngine.ENGINE_TYPE.RDBMS;
			engineTypeString = "TYPE:RDBMS";
		} else if(propEngType.contains("Tinker")) {
			engineType = IEngine.ENGINE_TYPE.TINKER;
			engineTypeString = "TYPE:TINKER";
		} else 	if(propEngType.contains("RNative")) {
			engineType = IEngine.ENGINE_TYPE.R; // process it as a flat file I bet 
			engineTypeString = "TYPE:R";
		} else {
			engineType = IEngine.ENGINE_TYPE.SESAME;
			engineTypeString = "TYPE:RDF";
		}
		
		this.conceptIdHash.put(engineName+"_ENGINE", engineUniqueId);
		String [] colNames = {"ID", "EngineName", "ModifiedDate", "Type"};
		String [] types = {"varchar(800)", "varchar(800)", "timestamp", "varchar(800)"};
		Object [] engineData = {engineUniqueId, engineName, new java.sql.Timestamp(modDate.getTime()), engineTypeString, "true"};
		insertQuery("Engine", colNames, types, engineData);
		
		// get the list of all the physical names
		// false denotes getting the physical names
		Vector<String> concepts = helper.getConcepts(false);
		Vector<String[]> relationships = helper.getRelationships(false);
		LOGGER.info("For engine " + engineName + " : Total Concepts Found = " + concepts.size());
		LOGGER.info("For engine " + engineName + " : Total Relationships Found = " + relationships.size());

		// iterate through all the concepts to insert into the local master
		for(int conceptIndex = 0; conceptIndex < concepts.size(); conceptIndex++) {
			String conceptPhysicalUri = concepts.get(conceptIndex);
			LOGGER.debug("Processing concept ::: " + conceptPhysicalUri);
			masterConcept(engineName, conceptPhysicalUri, helper, engineType);
		}
		
		for(int relIndex = 0; relIndex < relationships.size(); relIndex++) {
			String[] relationshipToInsert = relationships.get(relIndex);
			LOGGER.debug("Processing relationship ::: " + Arrays.toString(relationshipToInsert));
			masterRelationship(engineName, relationshipToInsert, helper);
		}
		
		return true;
	}
	
	
	/**
	 * Will add a concept and all of its properties into the local master
	 * This will add the following information:
	 * 		The concept into the CONCEPT TABLE if it does not already exist
	 * 		The properties of the concept into the CONCEPT TABLE if it does not already exist
	 * 		The concept into the ENGINECONCEPT TABLE
	 * 		The properties of the concept into the ENGINECONCEPT TABLE
	 * Since we are adding a new engine, we do not need to check if the concept/properties exist in
	 * the ENGINECONEPT TABLE
	 * @param engineName
	 * @param conceptPhysicalUri
	 * @param helper
	 * @param engineType
	 */
	private void masterConcept(String engineName, String conceptPhysicalUri, MetaHelper helper, IEngine.ENGINE_TYPE engineType) {
		String[] colNames = null;
		String[] types = null;
		String[] insertValues = null;
		
		// I need to add the concept into the CONCEPT table
		// The CONCEPT table is engine agnostic
		// So if I have Movie_RDBMS and Movie_RDF, and both have Title
		// it will only be added once into this table
		
		// so grab the conceptual name
		String conceptConceptualUri = helper.getConceptualUriFromPhysicalUri(conceptPhysicalUri);
		String conceptualName = Utility.getInstanceName(conceptConceptualUri);
		// grab the logical names
		Set<String> logicals = helper.getLogicalNames(conceptPhysicalUri);
		// and check if it is already there or not
		String conceptGuid = null;
		if(this.conceptIdHash.containsKey(conceptualName + "_CONCEPTUAL")) {
			// this concept already exists
			// so we will just grab the ID
			conceptGuid = this.conceptIdHash.get(conceptualName + "_CONCEPTUAL");
			
			Set<String> curLogicals = MasterDatabaseUtility.getAllLogicalNamesFromConceptualRDBMS(conceptualName);
			// add new logicals
			if(!logicals.isEmpty()) {
				colNames = new String[]{"LocalConceptID", "ConceptualName", "LogicalName", "DomainName", "GlobalID"};
				types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
			
				for(String logical : logicals) {
					if(!curLogicals.contains(logical)) {
						insertValues = new String[]{conceptGuid, conceptualName, logical, "NewDomain", ""};
						insertQuery("Concept", colNames, types, insertValues);
					}
				}
			}
		} else {
			// we need to create a new one
			conceptGuid = UUID.randomUUID().toString();
			// store it in the hash
			this.conceptIdHash.put(conceptualName+"_CONCEPTUAL", conceptGuid);
			
			// now insert it into the table
			colNames = new String[]{"LocalConceptID", "ConceptualName", "LogicalName", "DomainName", "GlobalID"};
			types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
			// TODO: we need to also store multiple logical names at some point
			// right now, default is to add the conceptual name as a logical name
			insertValues = new String[]{conceptGuid, conceptualName, conceptualName, "NewDomain", ""};
			insertQuery("Concept", colNames, types, insertValues);
			
			// also add all the logical names
			for(String logical : logicals) {
				insertValues = new String[]{conceptGuid, conceptualName, logical, "NewDomain", ""};
				insertQuery("Concept", colNames, types, insertValues);
			}
		}
		
		// now that we have either retrieved an existing concept id or made a new one
		// we can add this to the ENGINECONCEPT table
		// but we need to grab some additional information

		// generate a new id for the concept
		String engineConceptGuid = UUID.randomUUID().toString();
		// grab the data type of the concept
		String[] dataTypes = getDataType(conceptPhysicalUri, helper);
		// grab the data type of the concept
		String adtlDataType = getAdtlDataType(conceptPhysicalUri, helper);
		// get the physical name
		String conceptPhysicalInstance = Utility.getInstanceName(conceptPhysicalUri); 
		// get the engine id
		String engineId = this.conceptIdHash.get(engineName + "_ENGINE");
		
		colNames = new String[]{"Engine", "PhysicalName", "ParentPhysicalID", "PhysicalNameID", "LocalConceptID", "PK", "Property", "Original_Type", "Property_Type", "Additional_Type"};
		types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "boolean", "boolean", "varchar(800)","varchar(800)","varchar(800)"};
		String [] conceptInstanceData = {engineId, conceptPhysicalInstance, engineConceptGuid, engineConceptGuid, conceptGuid, "TRUE", "FALSE", dataTypes[0], dataTypes[1], adtlDataType};
		insertQuery("EngineConcept", colNames, types, conceptInstanceData);
		
		// store it in the hash, we will need this for the engine relationships
		this.conceptIdHash.put(engineName + "_" + conceptPhysicalInstance + "_PHYSICAL", engineConceptGuid);
		
		{
			// add the logical to the physical name id
			if(!logicals.isEmpty()) {
				colNames = new String[]{"PhysicalNameID", "ConceptualName", "LogicalName", "DomainName", "GlobalID"};
				types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
			
				for(String logical : logicals) {
					insertValues = new String[]{engineConceptGuid, "logical", logical};
					insertQuery("CONCEPTMETADATA", colNames, types, insertValues);
				}
			}
			// add the description to the physical name id
			String desc = helper.getDescription(conceptPhysicalUri);
			if(desc != null && !desc.trim().isEmpty()) {
				colNames = new String[]{"PhysicalNameID", "Key", "Value"};
				types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
				desc = desc.trim();
				if(desc.length() > 20_000) {
					desc = desc.substring(0, 19_996) + "...";
				}
				insertValues = new String[]{engineConceptGuid, "description", desc};
				insertQuery("CONCEPTMETADATA", colNames, types, insertValues);
			}
		}
		
		// now we need to add the properties for this concept + engine
		List<String> properties = helper.getProperties4Concept(conceptPhysicalUri, false);
		for(int propIndex = 0; propIndex < properties.size(); propIndex++) {
			String propertyPhysicalUri = properties.get(propIndex);
			LOGGER.debug("For concept = " + conceptPhysicalUri + ", adding property ::: " + propertyPhysicalUri);
			masterProperty(engineName, propertyPhysicalUri, engineConceptGuid, helper, engineType); 
		}
	}
	
	/**
	 * Will add a property for a given concept into the local master
	 * This will add the following information:
	 * 		The property into the CONCEPT TABLE if it does not already exist
	 * 		The property into the ENGINECONCEPT TABLE
	 * Since we are adding a new engine, we do not need to check if the properties exist in the ENGINECONEPT TABLE
	 * @param engineName
	 * @param propertyPhysicalUri
	 * @param parentEngineConceptGuid
	 * @param helper
	 * @param engineType
	 */
	private void masterProperty(String engineName, String propertyPhysicalUri, String parentEngineConceptGuid, MetaHelper helper, IEngine.ENGINE_TYPE engineType) {
		String[] colNames = null;
		String[] types = null;
		String[] insertValues = null;
		
		// I need to add the property into the CONCEPT table
		// The CONCEPT table is engine agnostic
		// So if I have Movie_RDBMS and Movie_RDF, and both have Title with property Movie_Budget
		// the property it will only be added once into this table
		
		// so grab the conceptual name
		String propertyConceptualUri = helper.getConceptualUriFromPhysicalUri(propertyPhysicalUri);
		// property conceptual uris are always /Column/Table
		String propertyConceptualName = Utility.getClassName(propertyConceptualUri);
		// grab the logical names 
		Set<String> logicals = helper.getLogicalNames(propertyPhysicalUri);

		// and check if it is already there or not
		String propertyGuid = null;
		if(this.conceptIdHash.containsKey(propertyConceptualName)) {
			// this concept already exists
			// so we will just grab the ID
			propertyGuid = this.conceptIdHash.get(propertyConceptualName);
			
			Set<String> curLogicals = MasterDatabaseUtility.getAllLogicalNamesFromConceptualRDBMS(propertyConceptualName);
			// add new logicals
			if(!logicals.isEmpty()) {
				colNames = new String[]{"LocalConceptID", "ConceptualName", "LogicalName", "DomainName", "GlobalID"};
				types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
			
				for(String logical : logicals) {
					if(!curLogicals.contains(logical)) {
						insertValues = new String[]{propertyGuid, propertyConceptualName, logical, "NewDomain", ""};
						insertQuery("Concept", colNames, types, insertValues);
					}
				}
			}
		} else {
			// we need to create a new one
			propertyGuid = UUID.randomUUID().toString();
			// store it in the hash
			this.conceptIdHash.put(propertyConceptualName, propertyGuid);
			
			// now insert it into the table
			colNames = new String[]{"LocalConceptID", "ConceptualName", "LogicalName", "DomainName", "GlobalID"};
			types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
			// TODO: we need to also store multiple logical names at some point
			// right now, default is to add the conceptual name as a logical name
			insertValues = new String[]{propertyGuid, propertyConceptualName, propertyConceptualName, "NewDomain", ""};
			insertQuery("Concept", colNames, types, insertValues);
			
			// also add all the logical names
			for(String logical : logicals) {
				insertValues = new String[]{propertyGuid, propertyConceptualName, logical, "NewDomain", ""};
				insertQuery("Concept", colNames, types, insertValues);
			}
		}
		
		// now that we have either retrieved an existing property id or made a new one
		// we can add this to the ENGINECONCEPT table
		// but we need to grab some additional information

		// generate a new id for the concept
		String enginePropertyGuid = UUID.randomUUID().toString();
		// grab the data type of the concept
		String[] dataTypes = getDataType(propertyPhysicalUri, helper);
		// grab the data type of the concept
		String adtlDataType = getAdtlDataType(propertyPhysicalUri, helper);
		// get the physical name
		// need to account for differences in how this is stored between
		// rdbms vs. graph databases
		String propertyPhysicalInstance = null;
		if(engineType == IEngine.ENGINE_TYPE.RDBMS || engineType == IEngine.ENGINE_TYPE.R) {
			propertyPhysicalInstance = Utility.getClassName(propertyPhysicalUri);
		}
		if(propertyPhysicalInstance == null || propertyPhysicalInstance.equalsIgnoreCase("Contains")) {
			propertyPhysicalInstance = Utility.getInstanceName(propertyPhysicalUri);
		}
		// get the engine id
		String engineId = this.conceptIdHash.get(engineName + "_ENGINE");
		
		colNames = new String[]{"Engine", "PhysicalName", "ParentPhysicalID", "PhysicalNameID", "LocalConceptID", "PK", "Property", "Original_Type", "Property_Type","Additional_Type"};
		types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "boolean", "boolean", "varchar(800)","varchar(800)","varchar(800)"};
		String [] conceptInstanceData = {engineId, propertyPhysicalInstance, parentEngineConceptGuid, enginePropertyGuid, propertyGuid, "FALSE", "TRUE", dataTypes[0], dataTypes[1], adtlDataType};
		insertQuery("EngineConcept", colNames, types, conceptInstanceData);
		
		{
			// add the logical to the physical name id
			if(!logicals.isEmpty()) {
				colNames = new String[]{"PhysicalNameID", "ConceptualName", "LogicalName", "DomainName", "GlobalID"};
				types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
			
				for(String logical : logicals) {
					insertValues = new String[]{enginePropertyGuid, "logical", logical};
					insertQuery("CONCEPTMETADATA", colNames, types, insertValues);
				}
			}
			// add the description to the physical name id
			String desc = helper.getDescription(propertyPhysicalUri);
			if(desc != null && !desc.trim().isEmpty()) {
				colNames = new String[]{"PhysicalNameID", "Key", "Value"};
				types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
				desc = desc.trim();
				if(desc.length() > 20_000) {
					desc = desc.substring(0, 19_996) + "...";
				}
				insertValues = new String[]{enginePropertyGuid, "description", desc};
				insertQuery("CONCEPTMETADATA", colNames, types, insertValues);
			}
		}
	}
	
	/**
	 * Get the original and high-level datatype for a concept or property
	 * @param physicalUri
	 * @param helper
	 * @return
	 */
	private String[] getDataType(String physicalUri, MetaHelper helper) {
		String dataType = "";
		String originalType = "";
		if(helper != null) {
			dataType = helper.getDataTypes(physicalUri);
			if(dataType == null) {
				originalType = "STRING";
				dataType = "STRING";
			} else {
				originalType = dataType;
				dataType = dataType.replace("TYPE:", "");
			}
		}

		if(Utility.isIntegerType(dataType)) {
			dataType = "INT";
		} else if(Utility.isDoubleType(dataType)) {
			dataType = "DOUBLE";
		} else if(Utility.isDateType(dataType)){
			dataType = "DATE";
		} else if(Utility.isTimeStamp(dataType)){
			dataType = "TIMESTAMP";
		} else {
			dataType = "STRING";
		}
		
		return new String[]{originalType, dataType};
	}
	
	private String getAdtlDataType(String physicalUri, MetaHelper helper) {
		if(helper != null) {
			return helper.getAdtlDataTypes(physicalUri);
		}
		return null;
	}
	
	/**
	 * Master a relationship into the local master
	 * Relationship array is [startNodePhysicalUri, endNodePhysicalUri, relationshipUri]
	 * @param engineName
	 * @param relationship
	 * @param helper
	 */
	private void masterRelationship(String engineName, String[] relationship, MetaHelper helper) {
		String[] colNames = null;
		String[] types = null;
		String[] insertValues = null;
		
		String startNodePhysicalUri = relationship[0];
		String endNodePhysicalUri = relationship[1];
		String relationshipUri = relationship[2];

		// note, we have already looped through all the different nodes within the engine
		// so there is nothing to check with regards to seeing if a concept id is not already there
				
		// grab the conceptual names
		// start node
		String startNodePhysicalInstance = Utility.getInstanceName(startNodePhysicalUri); 
		String conceptualStartNodeUri = helper.getConceptualUriFromPhysicalUri(startNodePhysicalUri);
		String conceptualStartNodeName = Utility.getInstanceName(conceptualStartNodeUri);
		// end node
		String endNodePhysicalInstance = Utility.getInstanceName(endNodePhysicalUri); 
		String conceptualEndNodeUri = helper.getConceptualUriFromPhysicalUri(endNodePhysicalUri);
		String conceptualEndNodeName = Utility.getInstanceName(conceptualEndNodeUri);
		
		String relationGuid = null;
		// The RELATION TABLE is engine agnostic
		// So we need to check and see if this relationship has already been added or not
		if(this.conceptIdHash.containsKey(conceptualStartNodeName + "_" + conceptualEndNodeName + "_RELATION")) {
			relationGuid = this.conceptIdHash.get(conceptualStartNodeName + "_" + conceptualEndNodeName + "_RELATION");
		} else {
			// we need to create it
			relationGuid = UUID.randomUUID().toString();
			
			// store it in the hash
			this.conceptIdHash.put(conceptualStartNodeName + "_" + conceptualEndNodeName + "_RELATION", relationGuid);
			
			colNames = new String[]{"ID", "SourceID", "TargetID", "GlobalID"};
			types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
			
			String startConceptualGuid = this.conceptIdHash.get(conceptualStartNodeName + "_CONCEPTUAL");
			String endConceptualGuid = this.conceptIdHash.get(conceptualEndNodeName + "_CONCEPTUAL");
			insertValues = new String[]{relationGuid, startConceptualGuid, endConceptualGuid, ""};
			insertQuery("Relation", colNames, types, insertValues);
		}
		
		// since we are adding a new engine
		// there is no check needed
		// just add the engine
		colNames = new String []{"Engine", "RelationID", "InstanceRelationID", "SourceConceptID", "TargetConceptID", "SourceProperty", "TargetProperty", "RelationName"};
		types = new String[]{"varchar(800)", "varchar(800)","varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
		
		String startConceptGuid = this.conceptIdHash.get(engineName + "_" + startNodePhysicalInstance +"_PHYSICAL");
		String endConceptGuid = this.conceptIdHash.get(engineName + "_" + endNodePhysicalInstance +"_PHYSICAL");
		String engineId = this.conceptIdHash.get(engineName + "_ENGINE");
		String engineRelationGuid = UUID.randomUUID().toString();
		insertValues = new String[]{engineId, relationGuid, engineRelationGuid, startConceptGuid, endConceptGuid, 
				startNodePhysicalInstance, endNodePhysicalInstance, Utility.getInstanceName(relationshipUri)};
		insertQuery("EngineRelation", colNames, types, insertValues);
	}
	
	/**
	 * Executes a query
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @param data
	 */
	private void insertQuery(String tableName, String [] colNames, String [] types, Object [] data) {
		String insertString = RdbmsQueryBuilder.makeInsert(tableName, colNames, types, data);
		executeSql(conn, insertString);
	}
	
	public void commit(IEngine localMaster) {
		try {
    		((RDBMSNativeEngine)localMaster).commitRDBMS();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Get the local master RDBMS connection
	 * @param localMaster
	 * @return
	 */
	private Connection getConnection(IEngine localMaster) {
    	try {
    		conn = ((RDBMSNativeEngine) localMaster).makeConnection();
    		conceptIdHash = ((RDBMSNativeEngine) localMaster).getConceptIdHash();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	
	
	/**
	 * Get the date for a given engine
	 * @param engineId
	 * @return
	 */
	public Date getEngineDate(String engineId) {
		java.util.Date retDate = null;
		RDBMSNativeEngine localMaster = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = getConnection(localMaster);
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String query = "select modifieddate from engine e where e.id = '" + engineId + "'";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				java.sql.Timestamp modDate = rs.getTimestamp(1);
				retDate = new java.util.Date(modDate.getTime());
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
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
		
		// check if fileName exists

		IEngine localMaster = Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		getConnection(localMaster);
		try {
			String configFile = MasterDatabaseUtility.getXrayConfigFile(fileName);
			if (configFile.length() > 0) {
				//create update statement
				String update = "UPDATE xrayconfigs SET config = '"+config+"' WHERE fileName = '"+fileName+"';";
				int updateCount = conn.createStatement().executeUpdate(update);

			} else {
				//make new insert
				String insertString = RdbmsQueryBuilder.makeInsert(tableName, colNames, types,
						new Object[] { fileName, config });
				insertString += ";";
				conn.createStatement().execute(insertString);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	/**
	 * Adds row to metadata table
	 * ex localConceptID, key, value
	 * 
	 * @param engineId
	 * @param concept
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean addMetadata(String engineId, String concept, String key, String value) {
		boolean valid = false;
		String tableName = Constants.CONCEPT_METADATA_TABLE;
		String[] colNames = new String[] { Constants.PHYSICAL_NAME_ID, Constants.KEY, Constants.VALUE };
		String[] types = new String[] { "varchar(800)", "varchar(800)", "varchar(20000)" };

		String localConceptID = MasterDatabaseUtility.getPhysicalConceptId(engineId, concept);
		try {
			IEngine localMaster = Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
			getConnection(localMaster);
			// check if key exists
			String duplicateCheck = MasterDatabaseUtility.getMetadataValue(engineId, concept, key);
			if (duplicateCheck == null) {
				String insertString = RdbmsQueryBuilder.makeInsert(tableName, colNames, types, new Object[] { localConceptID, key, value });
				int validInsert = conn.createStatement().executeUpdate(insertString + ";");
				if (validInsert > 0) {
					valid = true;
				}
			} // update
			else {
				String update = "UPDATE " + Constants.CONCEPT_METADATA_TABLE + " SET " + Constants.VALUE + " = \'"
						+ value + "\' WHERE " + Constants.PHYSICAL_NAME_ID + " = \'" + localConceptID + "\' and "
						+ Constants.KEY + " = \'" + key + "\'";
				int validInsert = conn.createStatement().executeUpdate(update + ";");
				if (validInsert > 0) {
					valid = true;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return valid;
	}
	
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////

	private static void executeSql(Connection conn, String sql) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	
	
	
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////

	
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
	
}
