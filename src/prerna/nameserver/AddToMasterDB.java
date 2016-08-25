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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.hp.hpl.jena.vocabulary.RDFS;

import prerna.algorithm.impl.CentralityCalculator;
import prerna.algorithm.nlp.TextHelper;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.MetaHelper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AddToMasterDB extends ModifyMasterDB {
	
	

	private HypernymListGenerator hypernymGenerator;
	String semossEngine = Constants.BASE_URI + "meta/engine";
	String presentIn = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/presentin";
	String conceptual = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/conceptual";
	String logical = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/logical";
	String modified = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/modified";
	String engineType = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/engineType";

	public AddToMasterDB(String localMasterDbName) {
		super(localMasterDbName);
	}
	public AddToMasterDB() {
		super();
	}

	public boolean registerEngineLocal(String engineName) {
		boolean success = false;
		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		Map<String, String> parentChildMapping = new HashMap<String, String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			// add parent child relationships to value mapping
			ISelectStatement sjss = wrapper.next();
			parentChildMapping.put(sjss.getVar(names[0]).toString(), sjss.getVar(names[1]).toString());
		}
		hypernymGenerator = new HypernymListGenerator();
		hypernymGenerator.addMappings(parentChildMapping);

		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
		GraphPlaySheet gps = CentralityCalculator.createMetamodel(engine, sparql, true);
		Hashtable<String, SEMOSSVertex> vertStore  = gps.getVertStore();
		Hashtable<String, SEMOSSEdge> edgeStore = gps.getEdgeStore();

		addNewDBConcepts(engineName, vertStore, edgeStore, parentChildMapping);
//		RepositoryConnection rc = engine.getInsightDB();
//		addInsights(rc);

		logger.info("Finished adding new engine " + engineName);
		success = true;

		masterEngine.commit();
		masterEngine.infer();

		return success;
	}

	public boolean registerEngineLocal2(Properties prop) {
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
		
		String baseFolder = null;
		
		try {
			baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		} catch (Exception ignored) {
			// TODO Auto-generated catch block
			
		}
		if(baseFolder == null)
			baseFolder = "C:/users/pkapaleeswaran/workspacej3/SemossWeb";
		// load the engine OWL File
		String owlFile = baseFolder + "/" + prop.getProperty(Constants.OWL);
		
		
		IEngine localMaster = (IEngine)DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
		
		
		RDFFileSesameEngine rfse = new RDFFileSesameEngine();
		rfse.openFile(owlFile, null, null);
		
		if(localMaster == null)
			localMaster = rfse;
		
		// also get the time on this filee
		File file = new File(owlFile);
		Date modDate = new Date(file.lastModified());

		MetaHelper helper = new MetaHelper(rfse, null, null);
				
		Vector <String> concepts = helper.getConcepts2(false); // gets all the PHYSICAL Concepts - true gets all the conceptual names
		//Vector <String> concepts = helper.getConcepts(); // gets all the PHYSICAL Concepts

		// just to make this easier
		// I will create an output file as well
		
		RDFFileSesameEngine imse = new RDFFileSesameEngine();
		imse.openFile(null, null, null);
		try {
		
		Object [] args = new Object[4];
		// insert the engine first
		// engine is a type of engine
		// keep the engine uri
		String engineName = prop.getProperty(Constants.ENGINE);
		System.out.println("Engine >> " + engineName);
		String engineURI = Constants.BASE_URI +"" + "meta/engine/" + engineName;
		addData(engineURI, modified, modDate, false, localMaster);
		addData(engineURI, RDF.TYPE + "", semossEngine, true, localMaster);
		
		
		IEngine.ENGINE_TYPE engineType = null;
		String engineTypeString = null;
		
		if(prop.getProperty("ENGINE_TYPE").contains("RDBMS"))
		{
			engineType = IEngine.ENGINE_TYPE.RDBMS;
			engineTypeString = "TYPE:RDBMS";
		}
		else
		{
			engineType = IEngine.ENGINE_TYPE.SESAME;
			engineTypeString = "TYPE:RDF";
		}	
		addData(engineURI, this.engineType, engineTypeString, true, localMaster);
		
		System.err.println("Processing Engine" + engineName + "Total Concepts Found " + concepts.size());
		// maps from the concept name to the physical composite
		Hashtable <String, String> physicalComposite = new Hashtable <String, String>();
		Hashtable <String, String> relationHash = new Hashtable <String, String>();
		
		// now I need to insert this into new world
		for(int conceptIndex = 0;conceptIndex< concepts.size();conceptIndex++)
		{
			masterConcept(concepts.get(conceptIndex), engineURI, physicalComposite, relationHash, localMaster, helper, engineType);
		}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		//tryQueries(rfse);
		
		//testMaster(rfse);
		
		return true;
	}
	
	
	private void masterConcept(String physicalConcept, String engineName, Hashtable <String, String> previousConcepts,Hashtable <String, String> relationHash, IEngine engine, MetaHelper helper, IEngine.ENGINE_TYPE engineType)
	{
		// get the conceptual name for this concept
		// eventually from the engine
		// this is the default display name
		String conceptualName = helper.findTransformedNodeName(physicalConcept, false);
		
		String conceptInstance = Utility.getInstanceName(physicalConcept, engineType); // This should primarily be just the name i.e. the instance name
		
		// get the logical names for this concept
		// there would be a default display name
		Vector <String> logicalNames = new Vector<String>(); // need to get this later
		// while I am getting the class it is out of whack because this accomodates for engine being RDBMS
		
		String lConcept = null; 
		
		//if(engineType == IEngine.ENGINE_TYPE.RDBMS)
		//	lConcept = Utility.getClassName(physicalConcept);
		//else
		lConcept = Utility.getInstanceName(physicalConcept); // << - should be the physical name to accomodate for table name. Table name is the physical concept
		
		// get the instance of engine
		String engineInstance = Utility.getInstanceName(engineName);
		
		String engineComposite = Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS + "/" + engineInstance + "_" + conceptInstance;
		
		String logicalConcept = Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS + "/" + lConcept;
		
		String semossConcept = Constants.BASE_URI + Constants.DEFAULT_NODE_CLASS;
		// basics of concept
		
		// add it as a concept
		addData(physicalConcept, RDFS.subClassOf+ "", semossConcept, true, engine);
		// add composite as a concept
		addData(engineComposite, RDFS.subClassOf+ "", semossConcept, true, engine);
		// make composite to be in engine
		addData(engineComposite, presentIn, engineName, true, engine);
		// make composite a type of physical
		addData(engineComposite, RDF.TYPE+"", physicalConcept, true, engine);
		// make physical to have a conceptual as conceptual
		addData(physicalConcept,  conceptual, conceptualName, true, engine);

		// need to find a way to get all the logical
		//***** Logical Block ******
		// for now I will put the keyword on this one
		addData(engineComposite, logical, logicalConcept, true, engine);
		
		
		//***** Logical Block ******
		
		// now add all the property
		// true gets conceptual name properties
		// false gets others
		List <String> logPropVector = helper.getProperties4Concept2(physicalConcept, false);
		// temporary fix
		if(logPropVector.size() == 0)
			logPropVector = helper.getProperties4Concept(physicalConcept, false);
		//List <String> logPropVector = helper.getProperties4Concept(physicalConcept, false);
		
		previousConcepts.put(physicalConcept, engineComposite);
		
		for(int propIndex = 0;propIndex < logPropVector.size();propIndex++)
			addProperty(engineComposite, logPropVector.get(propIndex), engineName, engine, engineType); // ending property for a concept
		
		// change this to from and to neighbors
		Vector <String> otherConcepts = helper.getFromNeighbors(physicalConcept, 0);		
		masterOtherConcepts(engine, otherConcepts, previousConcepts, relationHash, engineInstance, conceptInstance, engineComposite, true, engineType);
		otherConcepts = helper.getToNeighbors(physicalConcept, 0);		
		masterOtherConcepts(engine, otherConcepts, previousConcepts, relationHash, engineInstance, conceptInstance, engineComposite, false, engineType);
		// process all related concepts
		// first time I create title. I dont have genre in previousConcepts
		// next time when I create genre.. it will get title as a relationship
		// at which point I will insert this into the localmaster
	}
	
	private void masterOtherConcepts(IEngine engine, Vector <String> otherConcepts, Hashtable <String, String> previousConcepts, Hashtable <String, String> previousRelations, String engineInstance, String conceptInstance, String engineComposite, boolean from, IEngine.ENGINE_TYPE engineType)
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
			
			String to = conceptInstance + "_" + iOtherConcept;
			String fro = iOtherConcept + "_" + conceptInstance;
			
			if(!previousRelations.containsKey(to) && !previousRelations.containsKey(fro))
			{
				
				// yes, I know I dont have the relationname and I am just insterting a random relation
				// this needs to be done at an instance level
				// I should get the composite for the other one
				// I also need to keep that I have done this relation already so not do it
				previousRelations.put(to, to);
				previousRelations.put(fro, fro);
				
				String relationCompositeName = null;
				
				if(from)
				{
					relationCompositeName = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/" + engineInstance + "_" + iOtherConcept + "_" + conceptInstance ;
					addData(otherEngineConceptComposite, relationCompositeName, engineComposite, true, engine);
				}
				else
				{					
					relationCompositeName = Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS + "/" + engineInstance + "_" + conceptInstance + "_" + iOtherConcept ;
					addData(engineComposite, relationCompositeName, otherEngineConceptComposite, true, engine);
				}
				System.out.println("Added Relation.... " + relationCompositeName);
				
				addData(relationCompositeName, RDFS.subPropertyOf + "", Constants.BASE_URI + Constants.DEFAULT_RELATION_CLASS, true, engine);
			}
		}
	}
	
	private void addProperty(String engineConceptComposite, String property, String engineName, IEngine engine, IEngine.ENGINE_TYPE engineType)
	{
		String iProperty = Utility.getInstanceName(property, engineType);
		String iEngine = Utility.getInstanceName(engineName);
		String enginePropertyComposite = Constants.BASE_URI + Constants.DEFAULT_PROPERTY_CLASS + "/" + iEngine + "_" + iProperty;
		
		// so I might need to do a couple of checks here
		// basically i also need to add a logical name
		// the logical name is purely just the last name
		// need to do this the messy way for now
		String lProperty = null;
		if(engineType == IEngine.ENGINE_TYPE.RDBMS)
			lProperty = Utility.getClassName(property);
		if(lProperty == null || lProperty.equalsIgnoreCase("Contains"))
			lProperty = Utility.getInstanceName(property);

		String logicalPropertyName = Constants.BASE_URI + Constants.DEFAULT_PROPERTY_CLASS + "/" +lProperty;
		
	
		// I need to say this property in this engine is a type of property
		// and also specify this belongs to this engineConcept

		addData(enginePropertyComposite, RDF.TYPE + "", property, true, engine);
		
		// present in the engine
		addData(enginePropertyComposite, presentIn, engineName, true, engine);
		
		// engine concept has this engine property
		addData(engineConceptComposite, OWL.DATATYPEPROPERTY + "", enginePropertyComposite, true, engine);		
		
		// also add the logical name
		addData(enginePropertyComposite, logical, logicalPropertyName, true, engine);
	}
	
	private void addData(String subject, String predicate, Object object, boolean concept, IEngine engine)
	{
		Object [] statement = new Object[4];
		statement[0] = subject;
		statement[1] = predicate;
		statement[2] = object;
		statement[3] = new Boolean(concept);
		
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, statement);
	}
	
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
				+ "{?conceptComposite <" + logical + "> ?fromLogical}"
				+ "{?toConceptComposite <"+ logical + "> <" + conceptURI + ">}" // change this back to logical
				+ "{?toConceptComposite ?someRel ?conceptComposite}"
				//+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
				+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
				+ "&& ?toConcept != ?someEngine)}";

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
		String engineQuery = "SELECT DISTINCT ?engine WHERE {?engine <" + RDF.TYPE + "> <http://semoss.org/ontologies/meta/engine>}";
		//printQueryResult(engine, engineQuery);
		
		//tryQueries(null);
		
	}
	
	private void tryQueries(IEngine engine)
	{
		String fileName = "C:/users/pkapaleeswaran/workspacej3/SemossWeb/localmaster.xml";
		File file = new File(fileName);

		writeEngine(engine, file);
		
		engine = new RDFFileSesameEngine();
		((RDFFileSesameEngine)engine).openFile("C:/users/pkapaleeswaran/workspacej3/SemossWeb/db/LocalMasterDatabase/NFileLocalMaster.xml", null, null);

		fileName = "C:/users/pkapaleeswaran/workspacej3/SemossWeb/localMaster.xml";
		((RDFFileSesameEngine)engine).openFile(fileName, null, null);

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
		
		
		/*
		String query = query = "SELECT DISTINCT (COALESCE(?conceptual, ?concept) AS ?retConcept) WHERE { "
				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "OPTIONAL {"
					+ "{?concept <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual }"
				+ "}" // end optional for conceputal names if present
			+ "}";
		
			printQueryResult(engine, query);
		
		String conceptURI = "http://semoss.org/ontologies/Concept/System";
		
		String downstreamQuery = "SELECT DISTINCT ?someEngine ?fromConcept ?physical WHERE {"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
				+ "{?toConceptComposite <"+ RDF.TYPE + "> <" + conceptURI + ">}"
				+ "{?conceptComposite ?someRel ?toConceptComposite}"
				//+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
				+ "{<" + conceptURI + "> <http://semoss.org/ontologies/Relation/conceptual> ?physical}"				
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
				+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
				+ "&& ?toConcept != ?someEngine)}";
		System.out.println("Downstream.... ");
		printQueryResult(engine,downstreamQuery);
		
		
		String upstreamQuery = "SELECT DISTINCT ?someEngine ?toConcept ?physical WHERE {"
				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
				+ "{?conceptComposite <" + RDF.TYPE + "> <" + conceptURI + ">}"
				+ "{?toConceptComposite <"+ RDF.TYPE + "> ?toConcept}"
				+ "{?conceptComposite ?someRel ?toConceptComposite}"
				+ "{<" + conceptURI + "> <http://semoss.org/ontologies/Relation/Conceptual> ?physical}"				
				//+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
				//+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
				+ ")}";

			System.out.println("Upstream.... ");
			printQueryResult(engine,upstreamQuery);
			
			System.out.println("Hello");
		*/
		
		
		
		
		// base query to get engine and concepts
		String query = "SELECT ?engine ?conceptLogical ?concept WHERE {{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?engine}"
				//+ "{?conceptComposite ?rel2 <" + Constants.CONCEPT_URI + ">}"
				+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
				+ "{?concept <http://semoss.org/ontologies/Relation/conceptual> ?conceptLogical}"
				+ "}";
		
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
				//System.out.println("Engine " + bs.getBinding("engine") + "<>" + bs.getBinding("conceptLogical") + "<>" + bs.getBinding("concept"));
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
		String smssFile = "C:/users/pkapaleeswaran/workspacej3/SemossWeb/db/Mov2.smss";
		//String smssFile2 = "C:/users/pkapaleeswaran/workspacej3/SemossWeb/db/Churn.smss";
		Properties prop = new Properties();
		
		FileInputStream fis  = null;
		try {
			fis = new FileInputStream(new File(smssFile));
			prop.load(fis);
			prop.put("fis", fis);
			
			AddToMasterDB atm = new AddToMasterDB();

			atm.registerEngineLocal2(prop);

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
	
	public boolean registerEngineLocal(IEngine engine) {
		boolean success = false;
		String engineName = engine.getEngineName();

		//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		// for a given physical -- allt he hypernymslogical
		// physical -> hypernym
		// hypernym -> hypernym
		Map<String, String> parentChildMapping = new HashMap<String, String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			// add parent child relationships to value mapping
			ISelectStatement sjss = wrapper.next();
			parentChildMapping.put(sjss.getVar(names[0]).toString(), sjss.getVar(names[1]).toString());
		}

		hypernymGenerator = new HypernymListGenerator();
		hypernymGenerator.addMappings(parentChildMapping);

		String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
		GraphPlaySheet gps = CentralityCalculator.createMetamodel(engine, sparql, true);
		Hashtable<String, SEMOSSVertex> vertStore  = gps.getVertStore();
		Hashtable<String, SEMOSSEdge> edgeStore = gps.getEdgeStore();

		addNewDBConcepts(engineName, vertStore, edgeStore, parentChildMapping);
//		RepositoryConnection rc = engine.getInsightDB();
//		addInsights(rc);

		logger.info("Finished adding new engine " + engineName);
		success = true;

		masterEngine.commit();
		masterEngine.infer();

		return success;
	}

	public Hashtable<String, Boolean> registerEngineLocal(ArrayList<String> dbArray) {
		Hashtable<String, Boolean> successHash = new Hashtable<String, Boolean>();
		//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		Map<String, String> parentChildMapping = new HashMap<String, String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			// add parent child relationships to value mapping
			ISelectStatement sjss = wrapper.next();
			parentChildMapping.put(sjss.getVar(names[0]).toString(), sjss.getVar(names[1]).toString());
		}

		hypernymGenerator = new HypernymListGenerator();
		hypernymGenerator.addMappings(parentChildMapping);

		for(String engineName : dbArray) {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName + "");
			String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
			GraphPlaySheet gps = CentralityCalculator.createMetamodel(engine, sparql, true);
			Hashtable<String, SEMOSSVertex> vertStore  = gps.getVertStore();
			Hashtable<String, SEMOSSEdge> edgeStore = gps.getEdgeStore();

			addNewDBConcepts(engineName, vertStore, edgeStore, parentChildMapping);
//			RepositoryConnection rc = engine.getInsightDB();
//			addInsights(rc);

			logger.info("Finished adding new engine " + engineName);
			successHash.put(engineName, true);
		}

		//		for(String parent : parentChildMapping.keySet()) {
		//			logger.info("Parent: " + parent + ". Child: " + parentChildMapping.get(parent));
		//		}

		masterEngine.commit();
		masterEngine.infer();
		
		return successHash;
	}

	public Hashtable<String, Boolean> registerEngineAPI(String baseURL, ArrayList<String> dbArray) throws RDFParseException, RepositoryException, IOException {

		Hashtable<String, Boolean> successHash = new Hashtable<String, Boolean>();

		//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);

		Map<String, String> parentChildMapping = new HashMap<String, String>();
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, MasterDatabaseQueries.MC_PARENT_CHILD_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			// add parent child relationships to value mapping
			ISelectStatement sjss = wrapper.next();
			parentChildMapping.put(sjss.getVar(names[0]).toString(), sjss.getVar(names[1]).toString());
		}

		hypernymGenerator = new HypernymListGenerator();
		hypernymGenerator.addMappings(parentChildMapping);

		Gson gson = new Gson();
		for(String engineName : dbArray) {
			String engineAPI = baseURL + "/s-"+engineName;
			String stringResults = Utility.retrieveResult(engineAPI + "/metamodel", null);
//			RepositoryConnection owlRC = getNewRepository();
//			owlRC.add(new ByteArrayInputStream(owl.getBytes("UTF-8")), "http://semoss.org", RDFFormat.RDFXML);
//
//			String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
//			GraphPlaySheet gps = CentralityCalculator.createMetamodel(owlRC, sparql);
			Map<String, Object> results = gson.fromJson(stringResults, new TypeToken<HashMap<String, Object>>() {}.getType());
//			Hashtable<String, SEMOSSVertex> vertStore  = gps.getDataMaker().getVertStore();
//			Hashtable<String, SEMOSSEdge> edgeStore = gps.getDataMaker().getEdgeStore();
			Hashtable<String, SEMOSSVertex> vertStore  = (Hashtable<String, SEMOSSVertex>) results.get("nodes");
			Hashtable<String, SEMOSSEdge> edgeStore = (Hashtable<String, SEMOSSEdge>) results.get("edges");
			
			addNewDBConcepts(engineName, vertStore, edgeStore, parentChildMapping);
			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, MasterDatabaseURIs.PROP_URI + "/" + "API",baseURL,true);

//			String insights = Utility.retrieveResult(engineAPI + "/getInsightDefinition", null);
//			RepositoryConnection insightsRC = getNewRepository();
//			insightsRC.add(new ByteArrayInputStream(insights.getBytes("UTF-8")), "http://semoss.org", RDFFormat.RDFXML);
//			addInsights(insightsRC);

			logger.info("Finished adding new engine " + engineName);
			successHash.put(engineName, true);
		}

		//		for(String parent : parentChildMapping.keySet()) {
		//			logger.info("Parent: " + parent + ". Child: " + parentChildMapping.get(parent));
		//		}

		masterEngine.commit();
		masterEngine.infer();

		return successHash;
	}	

	private void addNewDBConcepts(String engineName, Hashtable<String, SEMOSSVertex> vertStore, Hashtable<String, SEMOSSEdge> edgeStore, Map<String, String> parentChildMapping) {
		MasterDatabaseForest<String> forest = new MasterDatabaseForest<String>();
		MasterDatabaseBipartiteGraph<String> keywordConceptBipartiteGraph = new MasterDatabaseBipartiteGraph<String>();

		Iterator<SEMOSSVertex> vertItr = vertStore.values().iterator();
		while(vertItr.hasNext()) {
			SEMOSSVertex vert = vertItr.next();
			String vertName = vert.getProperty(Constants.VERTEX_NAME).toString();
			if(!vertName.equals("Concept")) {
				// update the concept-concept tree and the keyword-concept graph
				String typeURI = vert.getURI();//full URI of this keyword
				String keyWordVertName = removeConceptUri(Utility.cleanString(typeURI, false));
				
				// give the uri http://semoss.org/ontologies/Concept/Year/Title 
				// the vertName above will give you Title, but we need Year
				// check below is to correct this
				if(keyWordVertName.contains("/")) {
					String[] keyWordVertNameSplit = keyWordVertName.split("/");
					vertName = keyWordVertNameSplit[0];
				}
				
				String cleanVertName = Utility.cleanString(vertName, false);
				MasterDBHelper.addKeywordNode(masterEngine, typeURI);
				MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + keyWordVertName, typeURI, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + cleanVertName + ":" + cleanVertName);

				String[] nouns = TextHelper.breakCompoundText(vertName);
				BipartiteNode<String> biNode = new BipartiteNode<String>(keyWordVertName);
				int i = 0;
				int numNouns = nouns.length;
				for(; i < numNouns; i++) {
					String noun = nouns[i].toLowerCase();
					biNode.addChild(noun);
					List<String> hypernymList = hypernymGenerator.getHypernymList(noun);
					TreeNode<String> node = hypernymGenerator.getHypernymTree(hypernymList);
					forest.addNodes(node);
					String topHypernym = Utility.cleanString(hypernymList.get(hypernymList.size()-1), false);
					String cleanNoun = Utility.cleanString(noun, false);
					MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanNoun, MasterDatabaseURIs.MC_BASE_URI + "/" + topHypernym, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/HasTopHypernym/" + cleanNoun + ":" + topHypernym);
				}
				keywordConceptBipartiteGraph.addToKeywordSet(biNode);
			}
		}

		// add mc to mc information to db
		Map<String, Set<String>> mcValueMapping = forest.getValueMapping();
		for(String parentMC : mcValueMapping.keySet()) {
			Set<String> childrenMC = mcValueMapping.get(parentMC);
			String cleanParentMC = Utility.cleanString(parentMC, false);
			// null when node doesn't have a parent
			if(childrenMC != null && !childrenMC.isEmpty()) {
				for(String childMC : childrenMC) {
					String cleanChildMC = Utility.cleanString(childMC, false);
					MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanParentMC);
					MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanChildMC);
					MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanParentMC, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanChildMC, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/ParentOf/" + cleanParentMC + ":" + cleanChildMC);
				}
			}
		}
		
		MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName);
		// add keyword to mc information to db
		Map<String, Set<String>> keywordMapping = keywordConceptBipartiteGraph.getKeywordMapping();
		for(String keyword : keywordMapping.keySet()) {
			String cleanKeyword = keyword;
			if(keyword.contains("/")) {
				cleanKeyword = Utility.getInstanceName(keyword);
			}
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + keyword, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + engineName + ":" + cleanKeyword);
			Set<String> mcList = keywordMapping.get(keyword);
			for(String mc : mcList) {
				String cleanMC = Utility.cleanString(mc, false);
				// note that keywords have already been added
				MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanMC);
				MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + keyword, MasterDatabaseURIs.MC_BASE_URI + "/" + cleanMC, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/ComposedOf/" + cleanKeyword + ":" + cleanMC);
			}
		}

		Iterator<SEMOSSEdge> edgeItr = edgeStore.values().iterator();
		while(edgeItr.hasNext()) {
			SEMOSSEdge edge = edgeItr.next();
			String edgeName = edge.getProperty(Constants.EDGE_TYPE).toString();
			String firstVertURI = edge.outVertex.getURI();
			String firstVertName = edge.outVertex.getProperty(Constants.VERTEX_NAME).toString();
			String secondVertURI = edge.inVertex.getURI();
			String secondVertName = edge.inVertex.getProperty(Constants.VERTEX_NAME).toString();

			String edgeConcat = engineName + ":" + firstVertName + ":" + edgeName + ":" + secondVertName;

			//add a node representing the engine-relation
			//add property on the engine relation node with the name
			//add a relation connecting the engine to the engine-relation node
			//add a relation connecting the in vert to the engine-relation node
			//add a relation connecting the engine-relation node to the out vert

			MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.ENGINE_RELATION_BASE_URI + "/" + edgeConcat);
			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.ENGINE_RELATION_BASE_URI + "/" + edgeConcat, MasterDatabaseURIs.PROP_URI + "/Name", edgeName, false);
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINE_BASE_URI + "/" + engineName, MasterDatabaseURIs.ENGINE_RELATION_BASE_URI + "/" + edgeConcat, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Has/" + engineName + ":" + edgeConcat);
			MasterDBHelper.addRelationship(masterEngine, firstVertURI, MasterDatabaseURIs.ENGINE_RELATION_BASE_URI + "/" + edgeConcat, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Provides/" + firstVertName + ":" + edgeConcat);
			MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.ENGINE_RELATION_BASE_URI + "/" + edgeConcat, secondVertURI, MasterDatabaseURIs.SEMOSS_RELATION_URI + "/Consumes/" + edgeConcat + ":" + secondVertName);
		}
	}

//	private RepositoryConnection getNewRepository() {
//		try {
//			RepositoryConnection rc = null;
//			Repository myRepository = new SailRepository(new ForwardChainingRDFSInferencer(new MemoryStore()));
//			myRepository.initialize();
//			rc = myRepository.getConnection();
//			return rc;
//		} catch (RepositoryException e) {
//			logger.error("Could not get a new repository");
//		}
//		return null;
//	}

//	private void addInsights(RepositoryConnection rc) {
//		try {
//			RepositoryResult<Statement> results = rc.getStatements(null, null, null, true);
//			while(results.hasNext()) {
//				Statement s = results.next();
//				boolean concept = true;
//				Object obj = s.getObject();
//				if(s.getObject() instanceof Literal) {
//					concept = false;
//					obj = ( (Value)obj).stringValue();
//				}
//				this.masterEngine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{s.getSubject(), s.getPredicate(), obj, concept});
//			}
//		} catch (RepositoryException e) {
//			logger.info("Repository Error adding insights");
//		}
//	}

//	public void createUserInsight(String userId, String insightId) {
//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
//		String userInsight = userId + "-" + insightId;
//
//		MasterDBHelper.addNode(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight);
//		MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.USERINSIGHT_EXECUTION_COUNT_PROP_URI, new Double(1), false);
//		MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.USERINSIGHT_LAST_EXECUTED_DATE_PROP_URI, new Date(), false);
//		MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.USER_BASE_URI + "/" + userId, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.USER_USERINSIGHT_REL_URI);
//		MasterDBHelper.addRelationship(masterEngine, MasterDatabaseURIs.INSIGHT_BASE_URI + "/" + insightId, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.INSIGHT_USERINSIGHT_REL_URI);
//	}

//	public boolean processInsightExecutionForUser(String userId, Insight insight) {
//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
//		String userInsight = userId + "-" + insight.getInsightID();
//
//		ISelectWrapper sjsw = Utility.processQuery(masterEngine, MasterDatabaseQueries.GET_USER_INSIGHT.replace("@USERINSIGHT@", userInsight));
//		if(!sjsw.hasNext()) {
//			createUserInsight(userId, insight.getInsightID());
//		} else {
//			Double count = 1.0;
//			Date oldDate = new Date();
//			sjsw = Utility.processQuery(masterEngine, MasterDatabaseQueries.GET_USER_INSIGHT_EXECUTED_COUNT.replace("@USERINSIGHT@", userInsight));
//			if(sjsw.hasNext()) {
//				String[] names = sjsw.getVariables();
//				ISelectStatement iss = sjsw.next();
//				count = Double.parseDouble(iss.getVar(names[0]).toString());
//				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
//				try {
//					oldDate = df.parse(iss.getVar(names[1]).toString());
//				} catch (ParseException e) {
//					e.printStackTrace();
//				}
//			}
//			MasterDBHelper.removeProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userId + "-" + insight.getInsightID(), MasterDatabaseURIs.USERINSIGHT_EXECUTION_COUNT_PROP_URI, count, false);
//			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userId + "-" + insight.getInsightID(), MasterDatabaseURIs.USERINSIGHT_EXECUTION_COUNT_PROP_URI, ++count, false);
//			MasterDBHelper.removeProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userId + "-" + insight.getInsightID(), MasterDatabaseURIs.USERINSIGHT_LAST_EXECUTED_DATE_PROP_URI, oldDate, false);
//			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userId + "-" + insight.getInsightID(), MasterDatabaseURIs.USERINSIGHT_LAST_EXECUTED_DATE_PROP_URI, new Date(), false);
//		}
//
//		masterEngine.commit();
//		masterEngine.infer();
//
//		return true;
//	}

//	public boolean publishInsightToFeed(String userId, Insight insight, String newVisibility) {
//		masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
//		final String userInsight = userId + "-" + insight.getInsightID();
//
//		String oldVisibility = "";
//		String pubDate = "";
//		ISelectWrapper sjsw = Utility.processQuery(masterEngine, MasterDatabaseQueries.GET_VISIBILITY_FOR_USERINSIGHT.replace("@USERINSIGHT@", userInsight));
//		if(!sjsw.hasNext()) {
//			createUserInsight(userId, insight.getInsightID());
//		} else {
//			String[] names = sjsw.getVariables();
//			ISelectStatement iss = sjsw.next();
//			oldVisibility = iss.getVar(names[0]).toString();
//			pubDate = iss.getVar(names[1]).toString();
//			MasterDBHelper.removeProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.USERINSIGHT_PUBLISH_VISIBILITY_PROP_URI, oldVisibility, false);
//		}
//
//		MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.USERINSIGHT_PUBLISH_VISIBILITY_PROP_URI, newVisibility, false);
//
//		if(oldVisibility.isEmpty() || (oldVisibility.equals("me") && !newVisibility.equals("me"))) {
//			MasterDBHelper.removeProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.USERINSIGHT_PUBLISH_DATE_PROP_URI, pubDate, false);
//			MasterDBHelper.addProperty(masterEngine, MasterDatabaseURIs.USERINSIGHT_URI + "/" + userInsight, MasterDatabaseURIs.USERINSIGHT_PUBLISH_DATE_PROP_URI, new Date(), false);
//		}
//
//		masterEngine.commit();
//		masterEngine.infer();
//
//		return true;
//	}

	public static String removeConceptUri(String s) {
		return s.replaceAll(".*/Concept/", "");
	}
	
	public static String removeRelationUri(String s) {
		return s.replaceAll(".*/Relation/", "");
	}
}
