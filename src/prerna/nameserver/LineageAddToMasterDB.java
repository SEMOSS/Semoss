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
import java.util.Arrays;
import java.util.Properties;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLERLineage;
import prerna.util.Utility;

public class LineageAddToMasterDB extends ModifyMasterDB {

	/*
	 * The purpose of this class is to ingest any lineage data
	 * present within the owl file of an engine into the local master
	 * 
	 * NOTE: This class will not add the required triples that are required
	 * within AddToMasterDB class.  This class assumes the registerEngineLocal
	 * method inside AddToMasterDB has already been executed.
	 * 
	 */
	
	public LineageAddToMasterDB(String localMasterDbName) {
		super(localMasterDbName);
	}
	
	public LineageAddToMasterDB() {
		super();
	}

	public boolean registerEngineLineageLocal(Properties prop) {
		// grab the local master engine
		IEngine localMaster = Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);

		// we want to load in the OWL for the engine we want to synchronize into the
		// the local master

		// get the base folder
		String baseFolder = null;
		try {
			baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		} catch (Exception ignored) {
			// just set to default location
			// used for testing if DIHelper not loaded
			baseFolder = "C:/workspace/Semoss_Dev";
		}

		// get the owl relative path from the base folder to get the full path
		String owlFile = baseFolder + "/" + prop.getProperty(Constants.OWL);
		// owl is stored as RDF/XML file
		RDFFileSesameEngine rfse = new RDFFileSesameEngine();
		rfse.openFile(owlFile, null, null);

		// insert the engine first
		// engine is a type of engine
		// keep the engine URI
		String engineName = prop.getProperty(Constants.ENGINE);
		LOGGER.info("Starting to synchronize engine ::: " + engineName);

		// grab the engine type 
		// if it is RDBMS vs NoSQL
		IEngine.ENGINE_TYPE engineType = null;

		if(prop.getProperty("ENGINE_TYPE").contains("RDBMS")) {
			engineType = IEngine.ENGINE_TYPE.RDBMS;
		} else if(prop.getProperty("ENGINE_TYPE").contains("Tinker")) {
			engineType = IEngine.ENGINE_TYPE.TINKER;
		}
		else {
			engineType = IEngine.ENGINE_TYPE.SESAME;
		}

		masterLineage(rfse, engineName, engineType, localMaster);
		
		// commit the lineage information
		localMaster.commit();
		
		return true;
	}
	
	private void masterLineage(IEngine owlFile, String engineName, IEngine.ENGINE_TYPE engineType, IEngine localMaster) {
		String baseLineageURI = Constants.BASE_URI + OWLERLineage.DEFAULT_LINEAGE_CLASS;

		String query = "SELECT ?SOURCE ?REL ?TARGET ?PROP ?VALUE WHERE { "
				+ "{?SOURCE ?REL ?TARGET} "
				+ "{?REL <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Lineage>} "
				+ "OPTIONAL { {?REL ?PROP ?VALUE}"
					+ "{?PROP <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <LINEAGE:PROPERTY>} "
					+ "}"
				+ "}";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(owlFile, query);
		while(wrapper.hasNext()) {
			Object[] rawValues = wrapper.next().getRawValues();
			
			String sourceUri = rawValues[0].toString();
			String relationshipUri = rawValues[1].toString();
			String targetUri = rawValues[2].toString();
			LOGGER.debug("Merging lineage information for >>> " + sourceUri + " , " + relationshipUri + " , " + targetUri);
			
			// keep the below as objects since it could be null
			Object relPropUri = rawValues[3];
			Object propValueUri = rawValues[4];
			
			// get the engine composite uri for the source
			String sourceInstanceName = Utility.getInstanceName(sourceUri, engineType);
			String sourceCompositeUri = Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS + "/" + engineName + "_" + sourceInstanceName;
			
			// get the engine composite uri for the target
			String targetInstanceName = Utility.getInstanceName(targetUri, engineType);
			String targetCompositeUri = Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS + "/" + engineName + "_" + targetInstanceName;
			
			// create a unique lineage relationship between the source and target
			String relationCompositeUri = baseLineageURI + "/" + engineName + "_" + sourceInstanceName + "_" + targetInstanceName ;
			
			// add the lineage triple
			addData(sourceCompositeUri, relationCompositeUri, targetCompositeUri, true, localMaster);
			// add the relationship as a type of lineage relationship for querying
			addData(relationCompositeUri, RDFS.SUBPROPERTYOF.stringValue(), baseLineageURI, true, localMaster);
			
			// if relationship has additional information
			if(relPropUri != null && propValueUri != null) {
				// add the info to the relationship
				// and then define it as a LINEAGE:PROPERTY
				addData(relationCompositeUri, relPropUri.toString(), propValueUri.toString(), true, localMaster);
				addData(relPropUri.toString(), RDF.TYPE.stringValue(), "LINEAGE:PROPERTY", true, localMaster);
			}
		}
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
	
	
	public static void main(String [] args) throws IOException  {
		// test with hard coded engine that has 
		// lineage data added into its owl
		
		TestUtilityMethods.loadDIHelper();
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new BigDataEngine();
		coreEngine.setEngineName(Constants.LOCAL_MASTER_DB_NAME);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);
		
		// get an SMSS file and try to generate this
		String smssFile = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		Properties prop = new Properties();
		FileInputStream fis  = null;
		fis = new FileInputStream(new File(smssFile));
		prop.load(fis);
		prop.put("fis", fis);
		
		LineageAddToMasterDB adder = new LineageAddToMasterDB();
		
		// run the routine
		adder.registerEngineLineageLocal(prop);
		
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		String query = "SELECT ?SOURCE ?REL ?TARGET ?PROP ?VALUE WHERE { "
				+ "{?SOURCE ?REL ?TARGET} "
				+ "{?REL <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Lineage>} "
				+ "OPTIONAL { {?REL ?PROP ?VALUE}"
					+ "{?PROP <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <LINEAGE:PROPERTY>} "
					+ "}"
				+ "}";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(coreEngine, query);
		while(wrapper.hasNext()) {
			System.out.println(Arrays.toString(wrapper.next().getRawValues()));
		}
	}
	
}
