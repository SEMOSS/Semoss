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
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
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
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
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
		addData(engineUri, modified, modDate, false, localMaster);

		/*
		 * TRIPLE INSERTION EXAMPLE
		 * 
		 * {	
		 * 		<http://semoss.org/ontologies/meta/engine/ENGINE_NAME> 
		 *  	<RDF.TYPE>
		 *  	<http://semoss.org/ontologies/meta/engine>
		 * }
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
		}
		else {
			engineType = IEngine.ENGINE_TYPE.SESAME;
			engineTypeString = "TYPE:RDF";
		}

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

		//		FileInputStream fis = null;
		//		try {
		//			fis = (FileInputStream)prop.get("fis");
		//		} finally {
		//			if(fis != null) {
		//				try {
		//					fis.close();
		//				} catch (IOException e) {
		//					e.printStackTrace();
		//				}
		//			}
		//		}
		// write the engine to a file
		//		tryQueries(rfse);

		//testMaster(rfse);

		return true;
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
		addData(physicalConceptUri, RDFS.subClassOf+ "", semossConcept, true, engine);
		// add composite as a concept


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
		addData(engineComposite, RDFS.subClassOf+ "", semossConcept, true, engine);


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
		addData(engineComposite, presentIn, engineUri, true, engine);

		/*
		 * Add engine composite as a type of hte physical URI
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
		addData(engineComposite, RDF.TYPE+"", physicalConceptUri, true, engine);


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
		addData(engineComposite, conceptual, conceptualUri, true, engine);

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
		addData(engineComposite, logical, logicalConcept, true, engine);
		
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
		addData(engineComposite, logical, conceptualUri, true, engine);
		
		// adding the physical URI to the engine composite
		previousConcepts.put(physicalConceptUri, engineComposite);

		// now get all the properties for the concept
		// false will return the physical URI for the concepts
		List<String> properties = helper.getProperties4Concept(physicalConceptUri, false);

		// iterate through and add all the properties
		for(int propIndex = 0;propIndex < properties.size(); propIndex++) {
			String physicalPropUri = properties.get(propIndex);
			LOGGER.debug("For concept = " + physicalConceptUri + " adding property ::: " + physicalPropUri);
			addProperty(engineComposite, physicalPropUri, engineUri, engine, helper, engineType); 
		}
		
		// only need to process relationships in one direction
		Vector <String> otherConcepts = helper.getFromNeighbors(physicalConceptUri, 0);		
		masterOtherConcepts(engine, otherConcepts, previousConcepts, engineInstance, conceptInstance, engineComposite, true, engineType);
	}
	
	private void masterOtherConcepts(IEngine engine, 
			Vector <String> otherConcepts, 
			Hashtable <String, String> previousConcepts, 
			String engineInstance, 
			String conceptInstance, 
			String engineComposite, 
			boolean from, 
			IEngine.ENGINE_TYPE engineType)
	{
		for(int otherIndex = 0;otherIndex < otherConcepts.size();otherIndex++)
		{
			String otherConcept = otherConcepts.get(otherIndex);
			String iOtherConcept = Utility.getInstanceName(otherConcept, engineType);
			String otherEngineConceptComposite = previousConcepts.get(otherConcept);
			
			
			if(previousConcepts.containsKey(otherConcept))
			{
				otherEngineConceptComposite = previousConcepts.get(otherConcept);
			}
			else
			{
				otherEngineConceptComposite = Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS + "/" + engineInstance + "_" + iOtherConcept;
				previousConcepts.put(otherConcept, otherEngineConceptComposite);
			}
			
			String relationCompositeName = null;
			
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
		}
	}
	
	private void addProperty(
			String engineConceptComposite, 
			String physicalPropUri, 
			String engineUri, 
			IEngine engine,
			MetaHelper helper, 
			IEngine.ENGINE_TYPE engineType)
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
		if(engineType == IEngine.ENGINE_TYPE.RDBMS)
			lProperty = Utility.getClassName(physicalPropUri);
		if(lProperty == null || lProperty.equalsIgnoreCase("Contains"))
			lProperty = Utility.getInstanceName(physicalPropUri);

		String logicalPropertyUri = Constants.BASE_URI + Constants.DEFAULT_PROPERTY_CLASS + "/" +lProperty;
	
		// I need to say this property in this engine is a type of property
		// and also specify this belongs to this engineConcept

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
	
	public void testMaster(IEngine inputEngine)
	{
		IEngine engine = inputEngine;
		try {
			if(engine != null)
				((RDFFileSesameEngine)engine).writeData(new RDFXMLWriter(new FileWriter("C:/users/pkapaleeswaran/workspacej3/SemossWeb/db/SubLocalMaster.xml")));
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(inputEngine == null)
		{
			RDFFileSesameEngine rfse = new RDFFileSesameEngine();
			String file = "C:/users/pkapaleeswaran/workspacej3/SemossWeb/db/SubLocalMaster.xml";
			file = "C:/users/pkapaleeswaran/workspacej3/SemossWeb/db/LocalMasterDatabase/FileLocalMaster.xml";
			file = "C:/users/pkapaleeswaran/workspacej3/SemossWeb/localMaster.xml";
			rfse.openFile(file, null, null);
			
			engine = (IEngine)rfse;
		}
		
		
		String conceptURI = "http://semoss.org/ontologies/Concept/Studio";
		// try connected
		String downstreamQuery = "SELECT DISTINCT ?someEngine ?fromConcept ?fromLogical WHERE {"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/logical> ?fromLogical}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/logical> <" + conceptURI + ">}" // logical
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
		
		System.out.println("Downstream.. ");
		printQueryResult(engine,downstreamQuery);
		
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
				+ "&& ?fromLogical != <" + conceptURI + ">"
				+")}";

			System.out.println("Upstream... ");
			printQueryResult(engine,upstreamQuery);
			

			String propQuery = "SELECT DISTINCT ?conceptProp ?engine WHERE {"
				+ "{?conceptComposite <" + RDF.TYPE + "> <" + conceptURI + ">}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?concept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?conceptComposite <" + OWL.DATATYPEPROPERTY + "> ?propComposite}"
				+ "{?propComposite <http://semoss.org/ontologies/Relation/presentin> ?engine}"
				+ "{?propComposite <" + RDF.TYPE + "> ?conceptProp}"
				+ "FILTER(?concept != <http://semoss.org/ontologies/Concept>)"
				+"}";

			
			/*
			tryQueries(null);
		
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
		//printQueryResult(engine, propQuery);
		 */
		
		// query to get all the engines
//		String engineQuery = "SELECT DISTINCT ?engine WHERE {?engine <" + RDF.TYPE + "> <http://semoss.org/ontologies/meta/engine>}";
		//printQueryResult(engine, engineQuery);
		
		//tryQueries(null);
		
	}
	
	private void tryQueries(IEngine engine)
	{
		String fileName = "C:/workspace/Semoss/localmaster.xml";
		File file = new File(fileName);

		writeEngine(engine, file);
		
		engine = new RDFFileSesameEngine();
		((RDFFileSesameEngine)engine).openFile("C:/workspace/Semoss/localmaster.xml", null, null);

		//fileName = "C:/users/pkapaleeswaran/workspacej3/SemossWeb/localMaster.xml";
		//((RDFFileSesameEngine)engine).openFile(fileName, null, null);

		// first write it to a file 
		
		

		/**
		 * How the engines are related
		 * so I have some physical concept present in a engine
		 * which is of a particular type
		 * and now this type needs to have a a different physical concept in a different engine
		 * 
		 * 
		 */
		String engineQuery = "SELECT DISTINCT ?someEngine ?fromConcept ?anotherEngine WHERE {"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?anotherConceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?anotherConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?anotherEngine}"
				+ "FILTER("
				+ "?someEngine != ?anotherEngine "
				+ "&& ?conceptComposite != ?anotherConceptComposite"
				+ ")}";
		
		//printQueryResult(engine, engineQuery);
		
		
		
		String query = query = "SELECT DISTINCT (COALESCE(?conceptual, ?concept) AS ?retConcept) WHERE { "
				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "OPTIONAL {"
					+ "{?concept <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual }"
				+ "}" // end optional for conceputal names if present
			+ "}";
		
			printQueryResult(engine, query);
		
		String conceptURI = "http://semoss.org/ontologies/Concept/Title";
		
		// try connected
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
		
		System.out.println("Downstream.. ");
		printQueryResult(engine,downstreamQuery);
		
		
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
			printQueryResult(engine,upstreamQuery);
			
			System.out.println("Hello");
		
		
		
		
		
		// base query to get engine and concepts
		/*String query = "SELECT ?engine ?conceptLogical ?concept WHERE {{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?engine}"
				//+ "{?conceptComposite ?rel2 <" + Constants.CONCEPT_URI + ">}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
				+ "{?concept <http://semoss.org/ontologies/Relation/conceptual> ?conceptLogical}"
				+ "}";
		*/
		//printQueryResult(engine, query);
		 
		// get vertex and edges in a given engine
		// engine comes in
		String engineName = "Mov2";
		
		// gets the concepts and properties
		
		Hashtable <String, Hashtable> edgeAndVertex = new Hashtable<String, Hashtable>();
		//String query = null;
		query = "SELECT DISTINCT ?concept (COALESCE(?prop, ?noprop) as ?conceptProp) (COALESCE(?propLogical, ?noprop) as ?propLogicalF) ?conceptLogical WHERE "
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
		
		makeVertices(engine, query, edgeAndVertex);
		
		// get all the relationships
		// in a given database
		query = "SELECT DISTINCT ?fromConcept ?someRel ?toConcept WHERE {"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?toConceptComposite <"+ RDF.TYPE + "> ?toConcept}"
				+ "{?conceptComposite ?someRel ?toConceptComposite}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "}";
		
		// all concepts with no database
		query = "SELECT DISTINCT ?fromConcept ?someRel ?toConcept WHERE {"
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
		makeEdges(engine, query, edgeAndVertex);
		// get everything linked to a keyword
		// so I dont have a logical concept
		// I cant do this
		
		Object [] vertArray = (Object[])edgeAndVertex.get("nodes").values().toArray();
		Object [] edgeArray = (Object[])edgeAndVertex.get("edges").values().toArray();
		Hashtable finalArray = new Hashtable();
		finalArray.put("nodes", vertArray);
		finalArray.put("edges", edgeArray);
		
        Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
        String output = gson.toJson(finalArray);
		System.out.println("Output is..++++++++++++++++++++");
		System.out.println(output);
		System.out.println("Output is..++++++++++++++++++++");
		
	}
	
	private void printQueryResult(IEngine engine, String query)
	{
		System.out.println("Running Query " + query);
		TupleQueryResult tqr = (TupleQueryResult)engine.execQuery(query);
		
		try {
			while(tqr.hasNext())
			{
				BindingSet bs = tqr.next();
				//System.out.println("Engine " + bs.getBinding("engine") + "<>" + bs.getBinding("from") + "<>" + bs.getBinding("fromLogical"));
				//System.out.println("Engine " + bs.getBinding("conceptProp") + "<>" + bs.getBinding("concept"));
				System.out.println("Engine " + bs.getBinding("someEngine") + bs.getBinding("fromConcept") + "<>" + bs.getBinding("fromLogical"));
				//System.out.println("Engine " + bs.getBinding("fromConcept") + "<>" + bs.getBinding("someEngine") + bs.getBinding("physical"));
				//System.out.println("Engine " + bs.getBinding("conceptProp") + "<>" + bs.getBinding("engine") + bs.getBinding("physical"));
				//System.out.println("Engine " + bs.getBinding("someEngine") + bs.getBinding("fromConcept") + bs.getBinding("anotherEngine"));

			}
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	// need to migrate to a different method eventually
	private void makeVertices(IEngine engine, String query, Hashtable <String, Hashtable>edgesAndVertices)
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
	
	
	private void makeEdges(IEngine engine, String query, Hashtable <String, Hashtable> edgesAndVertices)
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
	
	private void writeEngine(IEngine engine, File file)
	{
		try {
			RDFFileSesameEngine rfse = (RDFFileSesameEngine)engine;
			RDFXMLWriter writer = new RDFXMLWriter(new FileWriter(file));
			rfse.writeData(writer);
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	
	
	public static void main(String [] args)
	{
		// get an SMSS file and try to generate this
		String smssFile = "C:/workspace/Semoss_Dev/db/TAP_Core_Data.smss";
		//String smssFile2 = "C:/users/pkapaleeswaran/workspacej3/SemossWeb/db/Churn.smss";
		Properties prop = new Properties();
		
		FileInputStream fis  = null;
		try {
			fis = new FileInputStream(new File(smssFile));
			prop.load(fis);
			prop.put("fis", fis);
			
			AddToMasterDB atm = new AddToMasterDB();

			atm.registerEngineLocal(prop);

			//atm.testMaster(null);
			//atm.tryQueries(null);
			//fis = new FileInputStream(new File(smssFile2));
			//prop.load(fis);
			//atm.registerEngineLocal2(prop);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
