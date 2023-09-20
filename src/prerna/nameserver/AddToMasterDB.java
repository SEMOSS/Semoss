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
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.impl.util.MetadataUtility;
import prerna.engine.impl.MetaHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.reactor.masterdatabase.util.GenerateMetamodelUtility;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PersistentHash;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AddToMasterDB {

    private static final Logger classLogger = LogManager.getLogger(AddToMasterDB.class);

    private PersistentHash conceptIdHash = null;

	/*
	 *
	 * 
	    a.	Need multiple primary keys
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
        String engineId = prop.getProperty(Constants.ENGINE);
        if (engineId == null) {
            engineId = UUID.randomUUID().toString();
        }
        return registerEngineLocal(prop, engineId);
    }

    public boolean registerEngineLocal(Properties prop, String engineId) {
        // grab the local master engine
    	IRDBMSEngine localMaster = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB);
        // establish the connection
        Connection conn = null;
        PreparedStatement conceptPs = null;
        PreparedStatement engineConceptPs = null;
        PreparedStatement conceptMetaDataPs = null;
        PreparedStatement relationPs = null;
        PreparedStatement engineRelationPs = null;
        try {
            conn = localMaster.makeConnection();
            conceptIdHash = ((RDBMSNativeEngine) localMaster).getConceptIdHash();
            
            String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
            if(engineName == null) {
            	engineName = engineId;
            }
            
            // we want to load in the OWL for the engine we want to synchronize into the
            // the local master
            // get the owl relative path from the base folder to get the full path
            String owlFile = SmssUtilities.getOwlFile(prop).getAbsolutePath();

            // owl is stored as RDF/XML file
            RDFFileSesameEngine rfse = new RDFFileSesameEngine();
            rfse.setEngineId(engineId + "_" + Constants.OWL_ENGINE_SUFFIX);
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
    		classLogger.info("Starting to synchronize engine ::: " + Utility.cleanLogString(engineName));

            // grab the engine type
            // if it is RDBMS vs RDF
            IDatabaseEngine.DATABASE_TYPE dbType = null;
            String engineTypeString = null;
            String propEngType = prop.getProperty("ENGINE_TYPE");
            if (propEngType.contains("RDBMS") || propEngType.contains("H2EmbeddedServerEngine") || propEngType.contains("Impala")) {
                dbType = IDatabaseEngine.DATABASE_TYPE.RDBMS;
                engineTypeString = "TYPE:RDBMS";
            } else if (propEngType.contains("Tinker")) {
                dbType = IDatabaseEngine.DATABASE_TYPE.TINKER;
                engineTypeString = "TYPE:TINKER";
            } else if (propEngType.contains("RNative")) {
                dbType = IDatabaseEngine.DATABASE_TYPE.R; // process it as a flat file I bet
                engineTypeString = "TYPE:R";
            } else if (propEngType.contains("Janus")) {
                dbType = IDatabaseEngine.DATABASE_TYPE.JANUS_GRAPH;
                engineTypeString = "TYPE:JANUS_GRAPH";
            } else {
                dbType = IDatabaseEngine.DATABASE_TYPE.SESAME;
                engineTypeString = "TYPE:RDF";
            }

            this.conceptIdHash.put(engineName + "_ENGINE", engineId);
            String enginePsQuery = "INSERT INTO ENGINE (ID, ENGINENAME, MODIFIEDDATE, TYPE) VALUES (?,?,?,?)";
            try(PreparedStatement ps = conn.prepareStatement(enginePsQuery)) {
            	int parameterIndex = 1;
				ps.setString(parameterIndex++, engineId);
				if(engineName == null) {
					ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(parameterIndex++, engineName);
				}
				ps.setTimestamp(parameterIndex++, new java.sql.Timestamp(modDate.getTime()));
				ps.setString(parameterIndex++, engineTypeString);
				ps.execute();
            }

            // get the list of all the physical names
            // false denotes getting the physical names
            List<String> concepts = helper.getPhysicalConcepts();
            List<String[]> relationships = helper.getPhysicalRelationships();
    		classLogger.info("For engine " + Utility.cleanLogString(engineName) + " : Total Concepts Found = " + concepts.size());
    		classLogger.info("For engine " + Utility.cleanLogString(engineName) + " : Total Relationships Found = " + relationships.size());

            String conceptPsQuery = "INSERT INTO CONCEPT (LOCALCONCEPTID, CONCEPTUALNAME, LOGICALNAME, DOMAINNAME, GLOBALID) VALUES (?,?,?,?,?)";
            String engineConceptPsQuery = "INSERT INTO ENGINECONCEPT (ENGINE, PARENTSEMOSSNAME, SEMOSSNAME, PARENTPHYSICALNAME, "
            		+ "PARENTPHYSICALNAMEID, PARENTLOCALCONCEPTID, PHYSICALNAME, PHYSICALNAMEID, LOCALCONCEPTID, "
            		+ "IGNORE_DATA, PK, ORIGINAL_TYPE, PROPERTY_TYPE, ADDITIONAL_TYPE) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String conceptMetadataPsQuery = "INSERT INTO CONCEPTMETADATA ("+Constants.LM_PHYSICAL_NAME_ID
            		+", "+Constants.LM_META_KEY+", "+Constants.LM_META_VALUE + ") VALUES (?,?,?)";
    		
            conceptPs = conn.prepareStatement(conceptPsQuery);
            engineConceptPs = conn.prepareStatement(engineConceptPsQuery);
            conceptMetaDataPs = conn.prepareStatement(conceptMetadataPsQuery);

            // iterate through all the concepts to insert into the local master
            for (int conceptIndex = 0; conceptIndex < concepts.size(); conceptIndex++) {
                String conceptPhysicalUri = concepts.get(conceptIndex);
                classLogger.debug("Processing concept ::: " + conceptPhysicalUri);
                masterConcept(conceptPs, engineConceptPs, conceptMetaDataPs, engineName, conceptPhysicalUri, helper, dbType);
            }

            String relationPsQuery = "INSERT INTO RELATION (ID, SOURCEID, TARGETID, GLOBALID) VALUES (?,?,?,?)";
            String engineRelationPsQuery = "INSERT INTO ENGINERELATION (Engine, RelationID, InstanceRelationID, SourceConceptID, "
            		+ "TargetConceptID, SourceProperty, TargetProperty, RelationName) VALUES (?,?,?,?,?,?,?,?)";
            
            relationPs = conn.prepareStatement(relationPsQuery);
            engineRelationPs = conn.prepareStatement(engineRelationPsQuery);
            
            for (int relIndex = 0; relIndex < relationships.size(); relIndex++) {
                String[] relationshipToInsert = relationships.get(relIndex);
                classLogger.debug("Processing relationship ::: " + Arrays.toString(relationshipToInsert));
                masterRelationship(relationPs, engineRelationPs, engineName, relationshipToInsert, helper);
            }
            
            // sync metamodel position
            Map<String, Object> positions = GenerateMetamodelUtility.getOwlMetamodelPositions(engineId);
            if (positions.size() > 0) {
            	MasterDatabaseUtility.saveMetamodelPositions(engineId, positions, conn);
            }
            
            // execute all of the inserts
            conceptPs.executeBatch();
            engineConceptPs.executeBatch();
            conceptMetaDataPs.executeBatch();
            relationPs.executeBatch();
            engineRelationPs.executeBatch();
            if(!conn.getAutoCommit()) {
            	conn.commit();
            }
            return true;
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw new IllegalArgumentException("An error occurred establishing a connection to the local master database");
        } finally {
        	ConnectionUtils.closeStatement(conceptPs);
        	ConnectionUtils.closeStatement(engineConceptPs);
        	ConnectionUtils.closeStatement(conceptMetaDataPs);
        	ConnectionUtils.closeStatement(relationPs);
        	ConnectionUtils.closeStatement(engineRelationPs);
        	ConnectionUtils.closeAllConnectionsIfPooling(localMaster, conn, null, null);
        }
    }

    /**
     * Will add a concept and all of its properties into the local master This will
     * add the following information: The concept into the CONCEPT TABLE if it does
     * not already exist The properties of the concept into the CONCEPT TABLE if it
     * does not already exist The concept into the ENGINECONCEPT TABLE The
     * properties of the concept into the ENGINECONCEPT TABLE Since we are adding a
     * new engine, we do not need to check if the concept/properties exist in the
     * ENGINECONEPT TABLE
     *
     * @param engineName
     * @param conceptPhysicalUri
     * @param helper
     * @param dbType
     * @throws SQLException 
     */
    private void masterConcept(PreparedStatement conceptPs, PreparedStatement engineConceptPs, PreparedStatement conceptMetaDataPs, 
    		String engineName, String conceptPhysicalUri, MetaHelper helper, IDatabaseEngine.DATABASE_TYPE dbType) throws SQLException {
    	int parameterIndex = 1;
    	
        // I need to add the concept into the CONCEPT table
        // The CONCEPT table is engine agnostic
        // So if I have Movie_RDBMS and Movie_RDF, and both have Title
        // it will only be added once into this table

        // so grab the conceptual name
        String conceptPixelUri = helper.getConceptPixelUriFromPhysicalUri(conceptPhysicalUri);
        String semossName = Utility.getInstanceName(conceptPixelUri);

        // grab the conceptual name
        // grab the logical names
        String conceptualName = helper.getConceptualName(conceptPhysicalUri);
        Set<String> logicals = helper.getLogicalNames(conceptPhysicalUri);
        // and check if it is already there or not
        String conceptGuid = null;
        if (this.conceptIdHash.containsKey(conceptualName + "_CONCEPTUAL")) {
            // this concept already exists
            // so we will just grab the ID
            conceptGuid = this.conceptIdHash.get(conceptualName + "_CONCEPTUAL");

            Collection<String> curLogicals = MasterDatabaseUtility.getAllLogicalNamesFromConceptualRDBMS(conceptualName);
            // add new logicals
            if (!logicals.isEmpty()) {
                for (String logical : logicals) {
                    if (!curLogicals.contains(logical)) {
                    	parameterIndex = 1;
        				conceptPs.setString(parameterIndex++, conceptGuid);
        				conceptPs.setString(parameterIndex++, conceptualName);
        				conceptPs.setString(parameterIndex++, logical.toLowerCase());
        				conceptPs.setString(parameterIndex++, "NewDomain");
        				conceptPs.setString(parameterIndex++, "");
        				conceptPs.addBatch();
                    }
                }
            }
        } else {
            // we need to create a new one
            conceptGuid = UUID.randomUUID().toString();
            // store it in the hash
            this.conceptIdHash.put(conceptualName + "_CONCEPTUAL", conceptGuid);
            // now insert it into the table
            // TODO: we need to also store multiple logical names at some point
            // right now, default is to add the conceptual name as a logical name
            parameterIndex = 1;
			conceptPs.setString(parameterIndex++, conceptGuid);
			conceptPs.setString(parameterIndex++, conceptualName);
			conceptPs.setString(parameterIndex++, conceptualName.toLowerCase());
			conceptPs.setString(parameterIndex++, "NewDomain");
			conceptPs.setString(parameterIndex++, "");
			conceptPs.addBatch();
            
            // also add all the logical names
            for (String logical : logicals) {
            	parameterIndex = 1;
				conceptPs.setString(parameterIndex++, conceptGuid);
				conceptPs.setString(parameterIndex++, conceptualName);
				conceptPs.setString(parameterIndex++, logical.toLowerCase());
				conceptPs.setString(parameterIndex++, "NewDomain");
				conceptPs.setString(parameterIndex++, "");
				conceptPs.addBatch();
            }
        }

        // now that we have either retrieved an existing concept id or made a new one
        // we can add this to the ENGINECONCEPT table
        // but we need to grab some additional information

        // a concept (or table) in RDBMS/R has no meaning - the data is in the
        // properties (columns)
        boolean ignoreData = MetadataUtility.ignoreConceptData(dbType);

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

        // this is a concept
        // and it has no parent
        parameterIndex = 1;
        engineConceptPs.setString(parameterIndex++, engineId);
        engineConceptPs.setNull(parameterIndex++, java.sql.Types.VARCHAR);
        engineConceptPs.setString(parameterIndex++, semossName);
        engineConceptPs.setNull(parameterIndex++, java.sql.Types.VARCHAR);
        engineConceptPs.setNull(parameterIndex++, java.sql.Types.VARCHAR);
        engineConceptPs.setNull(parameterIndex++, java.sql.Types.VARCHAR);
        engineConceptPs.setString(parameterIndex++, conceptPhysicalInstance);
        engineConceptPs.setString(parameterIndex++, engineConceptGuid);
        engineConceptPs.setString(parameterIndex++, conceptGuid);
        engineConceptPs.setBoolean(parameterIndex++, ignoreData);
        engineConceptPs.setBoolean(parameterIndex++, true);
        engineConceptPs.setString(parameterIndex++, dataTypes[0]);
        engineConceptPs.setString(parameterIndex++, dataTypes[1]);
        engineConceptPs.setString(parameterIndex++, adtlDataType);
		engineConceptPs.addBatch();
        
        // store it in the hash, we will need this for the engine relationships
        this.conceptIdHash.put(engineName + "_" + conceptPhysicalInstance + "_PHYSICAL", engineConceptGuid);

        {
            // add the conceptual as a logical name to the physical name id
            parameterIndex = 1;
			conceptMetaDataPs.setString(parameterIndex++, engineConceptGuid);
			conceptMetaDataPs.setString(parameterIndex++, "logical");
			conceptMetaDataPs.setString(parameterIndex++, conceptualName.toLowerCase());
			conceptMetaDataPs.addBatch();
			
            if (!logicals.isEmpty()) {
                for (String logical : logicals) {
                    parameterIndex = 1;
    				conceptMetaDataPs.setString(parameterIndex++, engineConceptGuid);
    				conceptMetaDataPs.setString(parameterIndex++, "logical");
    				conceptMetaDataPs.setString(parameterIndex++, logical.toLowerCase());
    				conceptMetaDataPs.addBatch();
                }
            }
            // add the description to the physical name id
            String desc = helper.getDescription(conceptPhysicalUri);
            if (desc != null && !desc.trim().isEmpty()) {
            	desc = desc.trim();
                if (desc.length() > 20_000) {
                    desc = desc.substring(0, 19_996) + "...";
                }
                parameterIndex = 1;
				conceptMetaDataPs.setString(parameterIndex++, engineConceptGuid);
				conceptMetaDataPs.setString(parameterIndex++, "description");
				conceptMetaDataPs.setString(parameterIndex++, desc);
				conceptMetaDataPs.addBatch();
            }
        }

        // now we need to add the properties for this concept + engine
        List<String> properties = helper.getPropertyUris4PhysicalUri(conceptPhysicalUri);
        for (int propIndex = 0; propIndex < properties.size(); propIndex++) {
            String propertyPhysicalUri = properties.get(propIndex);
            classLogger.debug("For concept = " + conceptPhysicalUri + ", adding property ::: " + propertyPhysicalUri);
            masterProperty(conceptPs, engineConceptPs, conceptMetaDataPs, engineName, 
            		conceptPhysicalUri, propertyPhysicalUri, engineConceptGuid,
                    conceptPhysicalInstance, conceptGuid, semossName, helper, dbType);
        }
    }

    /**
     * Will add a property for a given concept into the local master This will add
     * the following information: The property into the CONCEPT TABLE if it does not
     * already exist The property into the ENGINECONCEPT TABLE Since we are adding a
     * new engine, we do not need to check if the properties exist in the
     * ENGINECONEPT TABLE
     *
     * @param engineName
     * @param propertyPhysicalUri
     * @param parentEngineConceptGuid
     * @param helper
     * @param dbType
     * @throws SQLException 
     */
    private void masterProperty(PreparedStatement conceptPs, PreparedStatement engineConceptPs, PreparedStatement conceptMetaDataPs, 
    		String engineName, String conceptPhysicalUri, String propertyPhysicalUri,
            String parentEngineConceptGuid, String parentPhysicalName, String parentConceptGuid,
            String parentSemossName, MetaHelper helper, IDatabaseEngine.DATABASE_TYPE dbType) throws SQLException {
    	int parameterIndex = 1;
        // I need to add the property into the CONCEPT table
        // The CONCEPT table is engine agnostic
        // So if I have Movie_RDBMS and Movie_RDF, and both have Title with property
        // Movie_Budget
        // the property it will only be added once into this table

        // so grab the conceptual name
        String propertyPixelUri = helper.getPropertyPixelUriFromPhysicalUri(conceptPhysicalUri, propertyPhysicalUri);
        // pixel URI is always column/table
        String propertySemossName = Utility.getClassName(propertyPixelUri);

        // grab the conceptual name
        // grab the logical names
        String propertyConceptualName = helper.getConceptualName(propertyPhysicalUri);
        Set<String> logicals = helper.getLogicalNames(propertyPhysicalUri);

        // and check if it is already there or not
        String propertyGuid = null;
        if (this.conceptIdHash.containsKey(propertyConceptualName)) {
            // this concept already exists
            // so we will just grab the ID
            propertyGuid = this.conceptIdHash.get(propertyConceptualName);

            Collection<String> curLogicals = MasterDatabaseUtility.getAllLogicalNamesFromConceptualRDBMS(propertyConceptualName);
            // add new logicals
            if (!logicals.isEmpty()) {
                for (String logical : logicals) {
                    if (!curLogicals.contains(logical)) {
                        parameterIndex = 1;
            			conceptPs.setString(parameterIndex++, propertyGuid);
            			conceptPs.setString(parameterIndex++, propertyConceptualName);
            			conceptPs.setString(parameterIndex++, logical.toLowerCase());
            			conceptPs.setString(parameterIndex++, "NewDomain");
            			conceptPs.setString(parameterIndex++, "");
            			conceptPs.addBatch();
                    }
                }
            }
        } else {
            // we need to create a new one
            propertyGuid = UUID.randomUUID().toString();
            // store it in the hash
            this.conceptIdHash.put(propertyConceptualName, propertyGuid);

            // now insert it into the table
            // TODO: we need to also store multiple logical names at some point
            // right now, default is to add the conceptual name as a logical name
            parameterIndex = 1;
			conceptPs.setString(parameterIndex++, propertyGuid);
			conceptPs.setString(parameterIndex++, propertyConceptualName);
			conceptPs.setString(parameterIndex++, propertyConceptualName.toLowerCase());
			conceptPs.setString(parameterIndex++, "NewDomain");
			conceptPs.setString(parameterIndex++, "");
			conceptPs.addBatch();
            
            // also add all the logical names
            for (String logical : logicals) {
                parameterIndex = 1;
    			conceptPs.setString(parameterIndex++, propertyGuid);
    			conceptPs.setString(parameterIndex++, propertyConceptualName);
    			conceptPs.setString(parameterIndex++, logical.toLowerCase());
    			conceptPs.setString(parameterIndex++, "NewDomain");
    			conceptPs.setString(parameterIndex++, "");
    			conceptPs.addBatch();
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
        if (dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS || dbType == IDatabaseEngine.DATABASE_TYPE.R) {
            propertyPhysicalInstance = Utility.getClassName(propertyPhysicalUri);
        }
        if (propertyPhysicalInstance == null || propertyPhysicalInstance.equalsIgnoreCase("Contains")) {
            propertyPhysicalInstance = Utility.getInstanceName(propertyPhysicalUri);
        }
        // get the engine id
        String engineId = this.conceptIdHash.get(engineName + "_ENGINE");

        parameterIndex = 1;
        engineConceptPs.setString(parameterIndex++, engineId);
        engineConceptPs.setString(parameterIndex++, parentSemossName);
        engineConceptPs.setString(parameterIndex++, propertySemossName);
        engineConceptPs.setString(parameterIndex++, parentPhysicalName);
        engineConceptPs.setString(parameterIndex++, parentEngineConceptGuid);
        engineConceptPs.setString(parameterIndex++, parentConceptGuid);
        engineConceptPs.setString(parameterIndex++, propertyPhysicalInstance);
        engineConceptPs.setString(parameterIndex++, enginePropertyGuid);
        engineConceptPs.setString(parameterIndex++, propertyGuid);
        engineConceptPs.setBoolean(parameterIndex++, false);
        engineConceptPs.setBoolean(parameterIndex++, false);
        engineConceptPs.setString(parameterIndex++, dataTypes[0]);
        engineConceptPs.setString(parameterIndex++, dataTypes[1]);
        engineConceptPs.setString(parameterIndex++, adtlDataType);
		engineConceptPs.addBatch();
        
        {
            // add the conceptual as a logical name to the physical name id
            parameterIndex = 1;
			conceptMetaDataPs.setString(parameterIndex++, enginePropertyGuid);
			conceptMetaDataPs.setString(parameterIndex++, "logical");
			conceptMetaDataPs.setString(parameterIndex++, propertyConceptualName.toLowerCase());
			conceptMetaDataPs.addBatch();
            
            // add the logical to the physical name id
            if (!logicals.isEmpty()) {
                for (String logical : logicals) {
                    parameterIndex = 1;
        			conceptMetaDataPs.setString(parameterIndex++, enginePropertyGuid);
        			conceptMetaDataPs.setString(parameterIndex++, "logical");
        			conceptMetaDataPs.setString(parameterIndex++, logical.toLowerCase());
        			conceptMetaDataPs.addBatch();
                }
            }
            // add the description to the physical name id
            String desc = helper.getDescription(propertyPhysicalUri);
            if (desc != null && !desc.trim().isEmpty()) {
            	desc = desc.trim();
                if (desc.length() > 20_000) {
                    desc = desc.substring(0, 19_996) + "...";
                }
                parameterIndex = 1;
    			conceptMetaDataPs.setString(parameterIndex++, enginePropertyGuid);
    			conceptMetaDataPs.setString(parameterIndex++, "description");
    			conceptMetaDataPs.setString(parameterIndex++, desc);
    			conceptMetaDataPs.addBatch();
            }
        }
    }

    /**
     * Get the original and high-level datatype for a concept or property
     *
     * @param physicalUri
     * @param helper
     * @return
     */
    private String[] getDataType(String physicalUri, MetaHelper helper) {
        String dataType = "";
        String originalType = "";
        if (helper != null) {
            dataType = helper.getDataTypes(physicalUri);
            if (dataType == null) {
                // this is the case for a table in RDBMS
                // or the fake root name in JSON
                return new String[]{null, null};
            } else {
                originalType = dataType;
                dataType = dataType.replace("TYPE:", "");
            }
        }

        if (Utility.isIntegerType(dataType)) {
            dataType = "INT";
        } else if (Utility.isDoubleType(dataType)) {
            dataType = "DOUBLE";
        } else if (Utility.isDateType(dataType)) {
            dataType = "DATE";
        } else if (Utility.isTimeStamp(dataType)) {
            dataType = "TIMESTAMP";
        } else {
            dataType = "STRING";
        }

        return new String[]{originalType, dataType};
    }

    private String getAdtlDataType(String physicalUri, MetaHelper helper) {
        if (helper != null) {
            return helper.getAdtlDataTypes(physicalUri);
        }
        return null;
    }

    /**
     * Master a relationship into the local master Relationship array is
     * [startNodePhysicalUri, endNodePhysicalUri, relationshipUri]
     *
     * @param engineName
     * @param relationship
     * @param helper
     * @throws SQLException 
     */
    private void masterRelationship(PreparedStatement relationPs, PreparedStatement engineRelationPs, 
    		String engineName, String[] relationship, MetaHelper helper) throws SQLException {
    	int parameterIndex = 1;

        String startNodePhysicalUri = relationship[0];
        String endNodePhysicalUri = relationship[1];
        String relationshipUri = relationship[2];

        // note, we have already looped through all the different nodes within the
        // engine
        // so there is nothing to check with regards to seeing if a concept id is not
        // already there

        // grab the conceptual names
        // start node
        String startNodePhysicalInstance = Utility.getInstanceName(startNodePhysicalUri);
        String pixelStartNodeUri = helper.getConceptPixelUriFromPhysicalUri(startNodePhysicalUri);
        String pixelStartNodeName = Utility.getInstanceName(pixelStartNodeUri);
        // end node
        String endNodePhysicalInstance = Utility.getInstanceName(endNodePhysicalUri);
        String pixelEndNodeUri = helper.getConceptPixelUriFromPhysicalUri(endNodePhysicalUri);
        String pixelEndNodeName = Utility.getInstanceName(pixelEndNodeUri);

        String relationGuid = null;
        // The RELATION TABLE is engine agnostic
        // So we need to check and see if this relationship has already been added or
        // not
        if (this.conceptIdHash.containsKey(pixelStartNodeName + "_" + pixelEndNodeName + "_RELATION")) {
            relationGuid = this.conceptIdHash.get(pixelStartNodeName + "_" + pixelEndNodeName + "_RELATION");
        } else {
            // we need to create it
            relationGuid = UUID.randomUUID().toString();

            // store it in the hash
            this.conceptIdHash.put(pixelStartNodeName + "_" + pixelEndNodeName + "_RELATION", relationGuid);
            String startConceptualGuid = this.conceptIdHash.get(pixelStartNodeName + "_CONCEPTUAL");
            String endConceptualGuid = this.conceptIdHash.get(pixelEndNodeName + "_CONCEPTUAL");

            parameterIndex = 1;
            relationPs.setString(parameterIndex++, relationGuid);
            relationPs.setString(parameterIndex++, startConceptualGuid);
            relationPs.setString(parameterIndex++, endConceptualGuid);
            relationPs.setString(parameterIndex++, "");
            relationPs.addBatch();
        }

        // since we are adding a new engine
        // there is no check needed
        // just add the engine
        String startConceptGuid = this.conceptIdHash.get(engineName + "_" + startNodePhysicalInstance + "_PHYSICAL");
        String endConceptGuid = this.conceptIdHash.get(engineName + "_" + endNodePhysicalInstance + "_PHYSICAL");
        String engineId = this.conceptIdHash.get(engineName + "_ENGINE");
        String engineRelationGuid = UUID.randomUUID().toString();
        
        parameterIndex = 1;
        engineRelationPs.setString(parameterIndex++, engineId);
        engineRelationPs.setString(parameterIndex++, relationGuid);
        engineRelationPs.setString(parameterIndex++, engineRelationGuid);
        engineRelationPs.setString(parameterIndex++, startConceptGuid);
        engineRelationPs.setString(parameterIndex++, endConceptGuid);
        engineRelationPs.setString(parameterIndex++, pixelStartNodeName);
        engineRelationPs.setString(parameterIndex++, pixelEndNodeName);
        engineRelationPs.setString(parameterIndex++, Utility.getInstanceName(relationshipUri));
        engineRelationPs.addBatch();
    }

//    /**
//     * Executes a query
//     *
//     * @param tableName
//     * @param colNames
//     * @param types
//     * @param data
//     */
//    private void insertQuery(Connection conn, String tableName, String[] colNames, String[] types, Object[] data) {
//        String insertString = RdbmsQueryBuilder.makeInsert(tableName, colNames, types, data);
//        executeSql(conn, insertString);
//    }

    
    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////


    /**
     * Creates a new table xrayconfigs inserts filesName and config file string
     *
     * @param config
     * @param fileName
     */
    public void addXrayConfig(String config, String fileName) {
        // make statements
        // create table to local master
        // check if fileName exists
        IRDBMSEngine localMaster = (IRDBMSEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB);
        Connection conn = null;
        PreparedStatement ps = null;
        try {
        	conn = localMaster.makeConnection();
            String configFile = MasterDatabaseUtility.getXrayConfigFile(fileName);
            if (configFile.length() > 0) {
                // create update statement
                ps = conn.prepareStatement("UPDATE xrayconfigs SET config=? WHERE filename=?");
                ps.setString(1, config);
                ps.setString(2, fileName);
                ps.execute();
            } else {
                // make new insert
				ps = conn.prepareStatement("INSERT INTO xrayconfig (filename, config) VALUES(?, ?)");
				ps.setString(1, fileName);
				ps.setString(2, config);
                ps.execute();
            }
            if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        } finally {
        	ConnectionUtils.closeAllConnectionsIfPooling(localMaster, conn, ps, null);
        }
    }

	/**
	 * Adds row to metadata table ex localConceptID, key, value
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
		String[] colNames = new String[]{Constants.LM_PHYSICAL_NAME_ID, Constants.LM_META_KEY, Constants.LM_META_VALUE};
		String localConceptID = MasterDatabaseUtility.getPhysicalConceptId(engineId, concept);
		
		IRDBMSEngine localMaster = (IRDBMSEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB);
		AbstractSqlQueryUtil queryUtil = localMaster.getQueryUtil();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
        	conn = localMaster.makeConnection();
			// check if key exists
			String duplicateCheck = MasterDatabaseUtility.getMetadataValue(engineId, concept, key);
			if (duplicateCheck == null) {
				stmt = conn.prepareStatement(queryUtil.createInsertPreparedStatementString(tableName, colNames));
				stmt.setString(1, localConceptID);
				stmt.setString(2, key);
				queryUtil.handleInsertionOfClob(conn, stmt, value, 3, new Gson());
				valid = stmt.execute();
			} 
			// update
			else {
				String updateSql = "UPDATE " + Constants.CONCEPT_METADATA_TABLE + " SET " + Constants.LM_META_VALUE + " = ? "
						+ " WHERE " + Constants.LM_PHYSICAL_NAME_ID + " = ? " + " AND " + Constants.LM_META_KEY
						+ " = ? ";
				stmt = conn.prepareStatement(updateSql);
				queryUtil.handleInsertionOfClob(conn, stmt, value, 1, new Gson());
				stmt.setString(2, localConceptID);
				stmt.setString(3, key);
				int validInsert = stmt.executeUpdate();
				if (validInsert > 0) {
					valid = true;
				}
			}
        } catch (SQLException e) {
        	classLogger.error(Constants.STACKTRACE, e);
        } catch (UnsupportedEncodingException e) {
        	classLogger.error(Constants.STACKTRACE, e);
		} finally {
        	ConnectionUtils.closeAllConnectionsIfPooling(localMaster, conn, stmt, null);
        }
		return valid;
	}

    //////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////

    public static void main(String[] args) throws IOException {
        // load the RDF map for testing purposes
        String rdfMapDir = "C:/Users/pkapaleeswaran/Workspacej3/SemossDev";
        // System.getProperty("user.dir")
        DIHelper.getInstance().loadCoreProp(rdfMapDir + "/RDF_Map.prop");

        final String WS_DIRECTORY = "C:/Users/pkapaleeswaran/Workspacej3";
        final String DB_DIRECTORY = WS_DIRECTORY + "/SemossWeb/db";

        // load the local master database
        Properties localMasterProp = loadEngineProp(DB_DIRECTORY, Constants.LOCAL_MASTER_DB);
        IDatabaseEngine localMaster = Utility.loadDatabase(determineSmssPath(DB_DIRECTORY, Constants.LOCAL_MASTER_DB), localMasterProp);

        // test loading in a new engine to the master database

        // get the new engine
        String engineName = "Mv1";
        Properties engineProp = loadEngineProp(DB_DIRECTORY, engineName);
        Utility.loadDatabase(determineSmssPath(DB_DIRECTORY, engineName), engineProp);

        // delete the engine from the master db so that we can re-add it fresh for
        // testing purposes
        DeleteFromMasterDB deleter = new DeleteFromMasterDB();
        deleter.deleteEngineRDBMS(engineName);

        String engineName2 = "actor";
        Properties engineProp2 = loadEngineProp(DB_DIRECTORY, engineName2);
        Utility.loadDatabase(determineSmssPath(DB_DIRECTORY, engineName), engineProp);

        // delete the engine from the master db so that we can re-add it fresh for
        // testing purposes
        deleter = new DeleteFromMasterDB();
        deleter.deleteEngineRDBMS(engineName);

        // test registering the engine
        AddToMasterDB adder = new AddToMasterDB();
        adder.registerEngineLocal(engineProp);
        adder.registerEngineLocal(engineProp2);
    }

    private static Properties loadEngineProp(final String DB_DIRECTORY, String engineName) throws IOException {
        try (FileInputStream fis = new FileInputStream(new File(determineSmssPath(DB_DIRECTORY, engineName)))) {
            Properties prop = new Properties();
            prop.load(fis);
            return prop;
        }
    }

    private static String determineSmssPath(final String DB_DIRECTORY, String engineName) {
        return DB_DIRECTORY + "/" + engineName + ".smss";
    }

}
