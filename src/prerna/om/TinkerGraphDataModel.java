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
package prerna.om;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.ds.TinkerFrame;
import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.InMemorySesameEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.RDFEngineHelper;
import prerna.util.JenaSesameUtils;
import prerna.util.Utility;

public class TinkerGraphDataModel {
	/*
	 * This contains all data that is fundamental to a SEMOSS Graph
	 * This data mainly consists of the edgeStore and vertStore as well as models/repository connections
	 */
	private static final Logger logger = LogManager.getLogger(TinkerGraphDataModel.class.getName());

	private RepositoryConnection rc = null;
	private RDFFileSesameEngine baseRelEngine = null;
	private Hashtable baseFilterHash = new Hashtable();
	String containsRelation;
	
	// subjects and objects below is used as a bindings later on within queries	
	private StringBuffer subjects = new StringBuffer("");
	private StringBuffer objects = new StringBuffer("");

	private boolean isPhysicalMetamodel = false;
	
	boolean subclassCreate = false; //this is used for our metamodel graphs. Need to be modify the base graph base and base concepts queries to use subclass of concept rather than type
	
	IEngine coreEngine;
	
	public void fillModel(String query, IEngine engine, TinkerFrame tf){
		processData(query, engine, tf);
		fillStoresFromModel(engine, tf);
	}

	//this function requires the rc to be completely full
	//it will use the rc to create edge and node properties
	//and then nodes and edges
	private void fillStoresFromModel(IEngine engine, TinkerFrame tf){
		if(rc!=null){
			if(containsRelation == null)
				containsRelation = findContainsRelation();
			if(containsRelation == null)
				containsRelation = "<http://semoss.org/ontologies/Relation/Contains>";

			String baseConceptSelectQuery = null;
			baseConceptSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
					"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
					"BIND(\"\" AS ?Predicate)" + // these are only used so that I can use select cheater...
					"BIND(\"\" AS ?Object)" +
					"}";
			if(subclassCreate) //this is used for our metamodel graphs. Need to be subclass of concept rather than type
				baseConceptSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
						"{?Subject " + "<http://www.w3.org/2000/01/rdf-schema#subClassOf>  " +  " <http://semoss.org/ontologies/Concept>;}" +
						"BIND(\"\" AS ?Predicate)" + // these are only used so that I can use select cheater...
						"BIND(\"\" AS ?Object)" +
						"}";
			genBaseConcepts(baseConceptSelectQuery, tf);
			logger.info("Loaded Orphans");


			String predicateSelectQuery = null;
			predicateSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
					"{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
					"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
					"{?Subject ?Predicate ?Object}" +
					"}";
			if(subclassCreate) //this is used for our metamodel graphs. Need to be subclass of concept rather than type
				predicateSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
						"{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
						"{?Subject " + "<http://www.w3.org/2000/01/rdf-schema#subClassOf>  " +  " <http://semoss.org/ontologies/Concept>;}" +
						"{?Subject ?Predicate ?Object}" +
						"}";
			genBaseGraph(predicateSelectQuery, tf);//subjects2, predicates2, subjects2);
			logger.info("Loaded Graph");
		}
	}

	private void processData(String query, IEngine engine, TinkerFrame tf) {
		this.coreEngine = engine; // right now we need this for logical names... really shouldn't be needed in this class at all

		// open up the engine
		String queryCap = query.toUpperCase();

		IConstructWrapper sjw = null;

		if(queryCap.startsWith("CONSTRUCT"))
			sjw = WrapperManager.getInstance().getCWrapper(engine, query);
		else
			sjw = WrapperManager.getInstance().getChWrapper(engine, query);

		logger.info("Executed the query");
		logger.debug("creating the in memory jena model");

		try {
			// predicates is used later on in a binding string in a query
			StringBuffer predicates = new StringBuffer("");
			while(sjw.hasNext())
			{
				// read the subject predicate object
				// add it to the in memory jena model
				// get the properties
				// add it to the in memory jena model
				IConstructStatement st = sjw.next();
				Object obj = st.getObject();
				logger.debug(st.getSubject() + "<<>>" + st.getPredicate() + "<<>>" + st.getObject());

				if(subjects.indexOf("(<" + st.getSubject() + ">)") < 0) {
					if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || engine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE || engine.getEngineType() == IEngine.ENGINE_TYPE.RDBMS) // RDBMS because the the in memory is RDBMS
						subjects.append("(<").append(st.getSubject()).append(">)");
					else if(engine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
						subjects.append("<").append(st.getSubject()).append(">");
				}

				if(predicates.indexOf("(<" + st.getPredicate() +">)") < 0) {
					if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || engine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE || engine.getEngineType() == IEngine.ENGINE_TYPE.RDBMS)
						predicates.append("(<").append(st.getPredicate()).append(">)");
					else if(engine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
						predicates.append("<").append(st.getPredicate()).append(">");
				}
				
				if(obj instanceof URI && !(obj instanceof com.hp.hpl.jena.rdf.model.Literal)) {			
					if(objects.indexOf("(<" + obj +">)") < 0) {
						if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || engine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE || engine.getEngineType() == IEngine.ENGINE_TYPE.RDBMS)
							objects.append("(<" + obj +">)");
						else if(engine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
							objects.append("<" + obj +">");
					}
				}
				addToSesame(st, false, false); // and this will work fine because I am just giving out URIs
			}						

			loadBaseData(engine); // this method will work fine too.. although I have no use for it for later
			// load the concept linkages
			// the concept linkages are a combination of the base relationships and what is on the file
			boolean isRDF = (engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || engine.getEngineType() == IEngine.ENGINE_TYPE.JENA || 
					engine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE);
			boolean loadHierarchy = !(subjects.length()==0 && predicates.length()==0 && objects.length()==0) && isRDF; // Load Hierarchy if and only if this is a RDF Engine - else dont worry about it
			if(loadHierarchy) {
				try {
					loadConceptHierarchy(engine, subjects.toString(), objects.toString());
					logger.debug("Loaded Concept");
					loadRelationHierarchy(engine, predicates.toString());
					logger.debug("Loaded Relation");
				} catch(RuntimeException ex) {
					ex.printStackTrace();
				}
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		}	
	}

	/**
	 * Method addToSesame.
	 * @param st SesameJenaConstructStatement
	 * @param overrideURI boolean
	 * @param add2Base boolean
	 */
	private void addToSesame(IConstructStatement st, boolean overrideURI, boolean add2Base) {
		try {
			// initialization routine...
			if(rc == null)
			{
				Repository myRepository = new SailRepository(
						new ForwardChainingRDFSInferencer(
								new MemoryStore()));
				myRepository.initialize();

				rc = myRepository.getConnection();	
				rc.setAutoCommit(false);
			}

			// Create the subject and predicate
			org.openrdf.model.Resource subject = new URIImpl(st.getSubject());
			org.openrdf.model.URI predicate = new URIImpl(st.getPredicate());

			// figure out if this is an object later
			//TODO: Need a way to figure out if obj from RDBMS is URI or Literal
			Object obj = st.getObject();
			if((overrideURI || obj instanceof URI || obj.toString().startsWith("http://")) && !(obj instanceof com.hp.hpl.jena.rdf.model.Literal))
			{
				org.openrdf.model.Resource object = null;

				if(obj instanceof org.openrdf.model.Resource)
					object = (org.openrdf.model.Resource) obj;
				else 
					object = new URIImpl(st.getObject()+"");

				if(add2Base) {
					baseRelEngine.addStatement(new Object[]{st.getSubject(), st.getPredicate(), st.getObject(), true});
				}
				rc.add(subject,predicate,object);
			}
			// the else basically means a couple of things
			// this is not a URI would the primary
			else if(obj instanceof Literal) // all the sesame routine goes here
			{
				if(add2Base) {
					baseRelEngine.addStatement(new Object[]{st.getSubject(), st.getPredicate(), st.getObject(), false});
				}
				rc.add(subject, predicate, (Literal)obj);
			}
			else if(obj instanceof com.hp.hpl.jena.rdf.model.Literal || !obj.toString().startsWith("http://"))
			{
				// I need to figure out a way to convert this into sesame literal
				Literal newObj = JenaSesameUtils.asSesameLiteral((com.hp.hpl.jena.rdf.model.Literal)obj);
				System.err.println("Adding to sesame " + subject + predicate + rc.getValueFactory().createLiteral(obj+""));
				if(add2Base) {
					baseRelEngine.addStatement(new Object[]{st.getSubject(), st.getPredicate(), st.getObject(), false});
				}
				rc.add(subject, predicate, (Literal)newObj);
			}
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Method addNodeProperty.
	 * @param subject String
	 * @param object Object
	 * @param predicate String
	 */
	private void addNodeProperty(String subject, Object object, String predicate) {
		logger.debug("Creating property for a vertex" );
		
//		String displayNameSubject = this.getDisplayName(subject);
//		SEMOSSVertex vert1 = vertStore.get(displayNameSubject);
//		if (vert1 == null) {
//			vert1 = new SEMOSSVertex(displayNameSubject);
//		}
//		//only set property and store vertex if the property does not already exist on the node
//		String propName = this.getDisplayName(predicate); //Utility.getInstanceName(this.getDisplayName(subjectInstance +"%"+ predicate));
//		if (vert1.getProperty(propName)==null) {
//			vert1.setProperty(propName, object);
//			storeVert(vert1);
//		}
	}

	private void storeVert(String uri, Object value, TinkerFrame tf){
		logger.info("storing vert "  + uri + " and value " + value);
		
		String type = Utility.getClassName(uri);
		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
		edgeHash.put(type, new HashSet<String>());
		tf.mergeEdgeHash(edgeHash, null);
//		tf.connectTypes(type, null, null);
		Map<String, Object> clean = new HashMap<String, Object>();
		clean.put(type, value);
		
		Map<String, Object> raw = new HashMap<String, Object>();
		raw.put(type, uri);
		
		tf.addRelationship(clean, raw);
	}

	private void storeVert(String uri, TinkerFrame tf){
		storeVert(uri, Utility.getInstanceName(uri), tf);
	}

	private void storeEdge(String outVert, String inVert, String edgeName, TinkerFrame tf, Map<Integer, Set<Integer>> cardinality){
		logger.info("storing edge "  + outVert + " and in " + inVert);
		String typeOut = Utility.getClassName(outVert);
		String typeIn = Utility.getClassName(inVert);
		tf.connectTypes(typeOut, typeIn, null);

//		Map<String, Object> clean = new HashMap<String, Object>();
//		clean.put(typeOut, outVert);
//		clean.put(typeIn, inVert);
//		Map<String, Object> raw = new HashMap<String, Object>();
//		raw.put(typeOut, outVert);
//		raw.put(typeIn, inVert);

		String[] headers = {typeOut, typeIn};
		String[] cleanValues = {Utility.getInstanceName(outVert), Utility.getInstanceName(inVert)};
		String[] rawValues = {outVert, inVert};
		
		Map<String, String> logicalToTypeMap = new HashMap<String, String>();
		logicalToTypeMap.put(typeOut, typeOut);
		logicalToTypeMap.put(typeIn, typeIn);

		tf.addRelationship(headers, cleanValues, rawValues, cardinality, logicalToTypeMap);
	}

	/**
	 * Method findContainsRelation.
	 * @return String
	 */
	private String findContainsRelation()
	{
		String query2 = "SELECT DISTINCT ?Subject ?subProp ?contains WHERE { BIND( <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subProp) BIND( <http://semoss.org/ontologies/Relation/Contains> AS ?contains) {?Subject ?subProp  ?contains}}";

		IEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);
		IConstructWrapper sjsc = WrapperManager.getInstance().getChWrapper(jenaEngine, query2);
		
		if(sjsc.hasNext())
		{
			IConstructStatement st = sjsc.next();
			return "<" + st.getSubject() + ">";
		}

		return null;
	}	

	/**
	 * Method genBaseConcepts.
	 */
	private void genBaseConcepts(String conceptSelectQuery, TinkerFrame tf)
	{
		// create all the relationships now
		logger.info("ConceptSelectQuery query " + conceptSelectQuery);

		IEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);
		
		logger.debug(conceptSelectQuery);
		IConstructWrapper sjsc = WrapperManager.getInstance().getChWrapper(jenaEngine, conceptSelectQuery);

		try {
			logger.debug("Execute complete");

			while(sjsc.hasNext())
			{
				IConstructStatement sct = sjsc.next();
				String subject = sct.getSubject();//this.getDisplayName(sct.getSubject());
				
				if(!baseFilterHash.containsKey(subject))// && !baseFilterHash.containsKey(sct.getPredicate()) && !baseFilterHash.containsKey(sct.getObject()+""))
				{
					storeVert(subject, tf);
				}
			}
		}catch(RuntimeException ex)
		{
			ex.printStackTrace();
		}
	}
	
	// executes the first SPARQL query and generates the graphs
	/**
	 * Method genBaseGraph.
	 */
	private void genBaseGraph(String predicateSelectQuery, TinkerFrame tf)
	{
		IEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);

		IConstructWrapper sjsc = WrapperManager.getInstance().getChWrapper(jenaEngine, predicateSelectQuery);
				
		logger.debug(predicateSelectQuery);
		
		Map<Integer, Set<Integer>> cardinality = new HashMap<Integer, Set<Integer>>();
		Set<Integer> cardinalitySet = new HashSet<Integer>();
		cardinalitySet.add(1);
		cardinality.put(0, cardinalitySet);
		
		try {
			logger.warn("Execute compelete");

			while(sjsc.hasNext())
			{
				IConstructStatement sct = sjsc.next();
				String predicateName = sct.getPredicate();//this.getDisplayName(sct.getPredicate());
				String subjectName = sct.getSubject();//this.getDisplayName(sct.getSubject());
				String objectName = sct.getObject()+"";//this.getDisplayName(sct.getObject()+"");

				String vert1Name = "";
				String vert2 = "";
				if(!baseFilterHash.containsKey(subjectName) && !baseFilterHash.containsKey(predicateName) && !baseFilterHash.containsKey(objectName))
				{
					// get the subject, predicate and object
					// look for the appropriate vertices etc and paint it
//					storeVert(subjectName, tf);
					vert1Name = Utility.getInstanceName(subjectName);
					if(sct.getObject() instanceof URI){
//						storeVert(objectName, tf);
						vert2 = objectName;
					}
					else { // ok this is a literal
//						storeVert(predicateName, sct.getObject(), tf);
						vert2 = predicateName+"";
					}
					// check to see if this is another type of edge			
					if(!predicateName.contains(vert1Name + ":" + Utility.getInstanceName(vert2))) {
						// need to not add this check when we make concepts into edges like data network
						if(!predicateName.contains("/ontologies/Concept/")) {
							predicateName = predicateName + "/" + vert1Name + ":" + Utility.getInstanceName(vert2);
						}
					}
					// when we store edge
					// if the vert does not yet exist
					// it will be added
					storeEdge(subjectName, vert2, predicateName, tf, cardinality);
				}
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		}	
	}
	
	private String getDisplayName(String subKey){
		if(isPhysicalMetamodel){
			return subKey;
		} else {
			return Utility.getTransformedNodeName(coreEngine, subKey, true);
		}
	}

	public void setSubclassCreate(boolean subclassCreate){
		this.subclassCreate = subclassCreate;
	}
	
	public void setIsPhysicalMetamodel(boolean physicalMetamodel){
		this.isPhysicalMetamodel = physicalMetamodel;
	}
	
	private void loadBaseData(IEngine engine){
		// now add the base relationships to the metamodel
		// this links the hierarchy that tool needs to the metamodel being queried
		// eventually this could be a SPIN
		// need to get the engine name and jam it - Done Baby
		if(this.baseRelEngine == null){
			this.baseRelEngine = ((AbstractEngine)engine).getBaseDataEngine();
		} else {
			RDFEngineHelper.addAllData(((AbstractEngine)engine).getBaseDataEngine(), this.baseRelEngine.getRc());
		}
		this.baseFilterHash.putAll(((AbstractEngine)engine).getBaseHash());
		RDFEngineHelper.addAllData(baseRelEngine, rc);
		logger.info("BaseQuery");
	}

	/**
	 * Add results from a query on an engine to the respository connection.
	 * @param fromEngine 	Engine where data is stored.
	 * @param query 		Query to be run.
	 * @param ps 			Graph playsheet where sesame construct statement is stored.
	 */
	private void addResultsToRC(IEngine fromEngine, String query) {
		
		IConstructWrapper sjsc = WrapperManager.getInstance().getCWrapper(fromEngine, query);
		while(sjsc.hasNext())
		{
			IConstructStatement st = sjsc.next();
			addToSesame(st, false, false);
		}
	}
	
	private void loadConceptHierarchy(IEngine fromEngine, String subjects, String objects)
	{
		String conceptHierarchyForSubject = "" ;

		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{			
			conceptHierarchyForSubject = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
					"{" +
					"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Object}" +
					"{?Subject ?Predicate ?Object}" + 
					"} BINDINGS ?Subject { " + subjects + objects + " } " +
					"";
		}
		else if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			conceptHierarchyForSubject = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
					"{VALUES ?Subject {" + subjects + objects + "}" +
					"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Object}" +
					"{?Subject ?Predicate ?Object}" + 
					"}";
		}
		
		addResultsToRC(fromEngine, conceptHierarchyForSubject);
	}
	
	private void loadRelationHierarchy(IEngine fromEngine, String predicates)
	{
		// same concept as the subject, but only for relations
		String relationHierarchy = "";

		if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || fromEngine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
		{
			relationHierarchy = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
					"{" +
					"{?Subject ?Predicate ?Object}" + 
					"{?Subject <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?Object}" +
					"} BINDINGS ?Subject { " + predicates + " } " +
					"";// relation hierarchy		
		}
		else if(fromEngine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			relationHierarchy = "CONSTRUCT { ?Subject ?Predicate ?Object} WHERE " +
					"{ VALUES ?Subject {" + predicates + "}" + 
					"{?Subject ?Predicate ?Object}" + 
					"{?Subject <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?Object}" +
					"}";// relation hierarchy					
		}
		
		addResultsToRC(fromEngine, relationHierarchy);

	}	
}
