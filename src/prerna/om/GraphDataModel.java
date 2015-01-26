/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.om;

import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jgrapht.graph.SimpleGraph;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.InMemoryJenaEngine;
import prerna.rdf.engine.impl.InMemorySesameEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaConstructWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectCheater;
import prerna.rdf.engine.impl.SesameJenaUpdateWrapper;
import prerna.ui.components.GraphOWLHelper;
import prerna.ui.components.PropertySpecData;
import prerna.ui.components.RDFEngineHelper;
import prerna.ui.components.VertexColorShapeData;
import prerna.ui.components.VertexFilterData;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.JenaSesameUtils;
import prerna.util.Utility;

public class GraphDataModel {
	/*
	 * This contains all data that is fundamental to a SEMOSS Graph
	 * This data mainly consists of the edgeStore and vertStore as well as models/repository connections
	 */
	private static final Logger logger = LogManager.getLogger(GraphDataModel.class.getName());
	public enum CREATION_METHOD {CREATE_NEW, OVERLAY, UNDO};
	CREATION_METHOD method;
	
	Hashtable <String, String> loadedOWLS = new Hashtable<String, String>();

	Properties rdfMap = null;
	String RELATION_URI = null;
	String PROP_URI = null;
	public RepositoryConnection rc = null;
	public RepositoryConnection curRC = null;
	public RDFFileSesameEngine baseRelEngine = null;
	public Hashtable baseFilterHash = new Hashtable();
	Model jenaModel = null;
	Model curModel = null;
	public int modelCounter = 0;
	public Vector <Model> modelStore = new Vector<Model>();
	String containsRelation;
	public Vector <RepositoryConnection> rcStore = new Vector<RepositoryConnection>();
	public PropertySpecData predData = new PropertySpecData();
	
	boolean search, prop, sudowl;
	
	boolean subclassCreate = false; //this is used for our metamodel graphs. Need to be modify the base graph base and base concepts queries to use subclass of concept rather than type
	
	Hashtable<String, SEMOSSVertex> vertStore = null;
	Hashtable<String, SEMOSSEdge> edgeStore = null;

	//these are used for keeping track of only what was added or subtracted and will only be populated when overlay is true
	Hashtable<String, SEMOSSVertex> incrementalVertStore = null;
	Hashtable<String, SEMOSSVertex> incrementalVertPropStore = null;
	Hashtable<String, SEMOSSEdge> incrementalEdgeStore = null;
	
	public GraphDataModel(){
		vertStore = new Hashtable<String, SEMOSSVertex>();
		edgeStore =new Hashtable<String, SEMOSSEdge>();
		rdfMap = DIHelper.getInstance().getRdfMap();
		createBaseURIs();
	}
	
	public void overlayData(String query, IEngine engine){
		curModel = null;
		curRC = null;
		incrementalVertStore = new Hashtable<String, SEMOSSVertex>();
		incrementalVertPropStore = new Hashtable<String, SEMOSSVertex>();
		incrementalEdgeStore = new Hashtable<String, SEMOSSEdge>();
		
		logger.info("Creating the new model");
		try {
				Repository myRepository2 = new SailRepository(
			            new ForwardChainingRDFSInferencer(
			            new MemoryStore()));
					myRepository2.initialize();
				
				curRC = myRepository2.getConnection();
				curModel = ModelFactory.createDefaultModel();
				
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		
		processData(query, engine);
		processTraverseCourse();
	}
	
	public void createModel(String query, IEngine engine){
		if(method == CREATION_METHOD.OVERLAY){
			overlayData(query, engine);
		}
		else 
			processData(query, engine);
	}
	
	//this function requires the rc to be completely full
	//it will use the rc to create edge and node properties
	//and then nodes and edges
	boolean test = true;
	public void fillStoresFromModel(){
		if(rc!=null){
			if(containsRelation == null)
				containsRelation = findContainsRelation();
			if(containsRelation == null)
				containsRelation = "<http://semoss.org/ontologies/Relation/Contains>";
			RDFEngineHelper.genNodePropertiesLocal(rc, containsRelation, this);
			RDFEngineHelper.genEdgePropertiesLocal(rc, containsRelation, this);
			genBaseConcepts();
			logger.info("Loaded Orphans");
			genBaseGraph();//subjects2, predicates2, subjects2);
			logger.info("Loaded Graph");
		}
	}
	
	public void processData(String query, IEngine engine) {
		// open up the engine
		String queryCap = query.toUpperCase();
		
		SesameJenaConstructWrapper sjw = null;
		if(queryCap.startsWith("CONSTRUCT"))
			sjw = new SesameJenaConstructWrapper();
		else
			sjw = new SesameJenaSelectCheater();

		logger.debug("Query is " + query);
		sjw.setEngine(engine);
		sjw.setQuery(query);
		
		try{
			sjw.execute();	
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
		
		logger.info("Executed the query");
		// need to take the base information from the base query and insert it into the jena model
		// this is based on EXTERNAL ontology
		// then take the ontology and insert it into the jena model
		// (may be eventually we can run this through a reasoner too)
		// Now insert our base model into the same ontology
		// Now query the model for 
		// Relations - Paint the basic graph
		// Now find a way to get all the predicate properties from them
		// Hopefully the property is done using subproperty of
		// predicates - Pick all the predicates but for the properties
		// paint them
		// properties
		// and then paint it appropriately
		logger.debug("creating the in memory jena model");
		
		// replacing the current logic with SPARQLParse
		
		// I am going to use the same standard query
		/*String thisquery = "SELECT ?System1 ?Upstream ?ICD ?Downstream ?System2 ?carries ?Data1 ?contains2 ?prop2 ?System3 ?Upstream2 ?ICD2 ?contains1 ?prop ?Downstream2 ?carries2 ?Data2 ?Provide ?BLU" +
		" WHERE { {?System1  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System1){{?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Data1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}{?System1 ?Upstream ?ICD ;}{?ICD ?Downstream ?System2 ;} {?ICD ?carries ?Data1;}{?carries ?contains2 ?prop2} {?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Upstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?Downstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}  {?ICD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Data2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?carries2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?System3 ?Upstream2 ?ICD2 ;}{?ICD2 ?Downstream2 ?System1 ;} {?ICD2 ?carries2 ?Data2;} {?carries2 ?contains1 ?prop} {?contains1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System1 ?Provide ?BLU}}}";
		SPARQLParse parse = new SPARQLParse();
		parse.createRepository();
		parse.parseIt(thisquery);
		parse.executeQuery(thisquery, engine);
		parse.loadBaseDB(engine.getProperty(Constants.OWL));
		this.rc = parse.rc;
		*/
		
		// this is where the block goes
		//figure out if we need to index jena for search and process for SUDOWL
		
		try {
//			boolean isError = false;
			
			StringBuffer subjects = new StringBuffer("");
			StringBuffer predicates = new StringBuffer("");
			StringBuffer objects = new StringBuffer("");
			//if(!sjw.hasNext())
			//{
			//	logger.info("Came into not having ANY data"); 
			//	return;
			//}
			while(sjw.hasNext())
			{
				// read the subject predicate object
				// add it to the in memory jena model
				// get the properties
				// add it to the in memory jena model
				SesameJenaConstructStatement st = sjw.next();
				Object obj = st.getObject();
				logger.debug(st.getSubject() + "<<>>" + st.getPredicate() + "<<>>" + st.getObject());
				//predData.addPredicate2(st.getPredicate());
				//predData.addConceptAvailable(st.getSubject());//, st.getSubject());
				//predData.addPredicateAvailable(st.getPredicate());//, st.getPredicate());

				if(subjects.indexOf("(<" + st.getSubject() + ">)") < 0) {
					if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || engine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
						subjects.append("(<").append(st.getSubject()).append(">)");
					else
						subjects.append("<").append(st.getSubject()).append(">");
				}
				
				if(predicates.indexOf("(<" + st.getPredicate() +">)") < 0) {
					if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || engine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
						predicates.append("(<").append(st.getPredicate()).append(">)");
					else
						predicates.append("<").append(st.getPredicate()).append(">");
				}
				
				//TODO: need to find a way to do this for jena too
				if(obj instanceof URI && !(obj instanceof com.hp.hpl.jena.rdf.model.Literal)) {			
					if(objects.indexOf("(<" + obj +">)") < 0) {
						if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || engine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE)
							objects.append("(<" + obj +">)");
						else
							objects.append("<" + obj +">");
					}
				}
				//addToJenaModel(st);
				addToSesame(st, false, false);
				if (search) addToJenaModel3(st);
			}			
			logger.debug("Subjects >>> " + subjects);
			logger.debug("Predicatss >>>> " + predicates);
			
			loadBaseData(engine);
			// load the concept linkages
			// the concept linkages are a combination of the base relationships and what is on the file
			boolean loadHierarchy = !(subjects.length()==0 && predicates.length()==0 && objects.length()==0); 
			if(loadHierarchy) {
				try {
					RDFEngineHelper.loadConceptHierarchy(engine, subjects.toString(), objects.toString(), this);
					logger.debug("Loaded Concept");
					RDFEngineHelper.loadRelationHierarchy(engine, predicates.toString(), this);
					logger.debug("Loaded Relation");
				} catch(RuntimeException ex) {
					ex.printStackTrace();
				}
			}
			containsRelation = findContainsRelation();
			if(containsRelation == null)
				containsRelation = "<http://semoss.org/ontologies/Relation/Contains>";

			if(sudowl) {
				logger.info("Starting to load SUDOWL");
				GraphOWLHelper.loadConceptHierarchy(rc, subjects.toString(), objects.toString(), this);
				GraphOWLHelper.loadRelationHierarchy(rc, predicates.toString(), this);
				GraphOWLHelper.loadPropertyHierarchy(rc,predicates.toString(), containsRelation, this);
				logger.info("Finished loading SUDOWL");
			}
			if(prop) {
				logger.info("Starting to load properties");
				// load local property hierarchy
				try
				{
					//loadPropertyHierarchy(predicates, containsRelation);
					RDFEngineHelper.loadPropertyHierarchy(engine,predicates.toString(), containsRelation, this);
					// now that this is done, we can query for concepts						
					//genPropertiesRemote(propertyQuery + "BINDINGS ?Subject { " + subjects + " " + predicates + " " + objects+ " } ");
					RDFEngineHelper.genPropertiesRemote(engine, subjects.toString(), objects.toString(), predicates.toString(), containsRelation, this);
					logger.info("Loaded Properties");
				} catch(RuntimeException ex) {
					ex.printStackTrace();
				}
				//genProperties(propertyQuery + predicates + " } ");
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		}	
		modelCounter++;
	}

	/**
	 * Method addToSesame.
	 * @param st SesameJenaConstructStatement
	 * @param overrideURI boolean
	 * @param add2Base boolean
	 */
	public void addToSesame(SesameJenaConstructStatement st, boolean overrideURI, boolean add2Base) {
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
				
				if(method == CREATION_METHOD.OVERLAY) {
					if (!rc.hasStatement(subject,predicate,object, true)) {
						curRC.add(subject,predicate,object);
					} else {
						return;
					}
				}
				if(add2Base) {
					baseRelEngine.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), true);
				}
				rc.add(subject,predicate,object);
			}
			// the else basically means a couple of things
			// this is not a URI would the primary
			else if(obj instanceof Literal) // all the sesame routine goes here
			{
				/*if(obj instanceof com.bigdata.rdf.model.BigdataValueImpl){
				rc.add(subject, predicate, (com.bigdata.rdf.model.BigdataValueImpl) obj);
				if(extend || overlay)
				{
					//logger.info("Adding to the new model");
					curRC.add(subject,predicate,rc.getValueFactory().createLiteral(obj+""));
				}
				if(add2Base)
				{
					baseRelEngine.addStatement(st.getSubject(), st.getPredicate(), obj, false);
				}*/
				
				if(method == CREATION_METHOD.OVERLAY) {
					//logger.info("Adding to the new model");
					if (!rc.hasStatement(subject,predicate,(Literal)obj, true)) {
						curRC.add(subject,predicate,(Literal)obj);
					} else {
						return;
					}
				}
				if(add2Base) {
					baseRelEngine.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), false);
				}
				rc.add(subject, predicate, (Literal)obj);
			}
			else if(obj instanceof com.hp.hpl.jena.rdf.model.Literal || !obj.toString().startsWith("http://"))
			{
				// I need to figure out a way to convert this into sesame literal
				Literal newObj = JenaSesameUtils.asSesameLiteral((com.hp.hpl.jena.rdf.model.Literal)obj);
				System.err.println("Adding to sesame " + subject + predicate + rc.getValueFactory().createLiteral(obj+""));
				
				if(method == CREATION_METHOD.OVERLAY) {
					if (!rc.hasStatement(subject,predicate,newObj, true)) {
						curRC.add(subject,predicate,(Literal)newObj);
					} else {
						return;
					}
				}
				if(add2Base) {
					baseRelEngine.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), false);
				}
				rc.add(subject, predicate, (Literal)newObj);
			}
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		/*jenaModel.add(jenaSt);*/
		// just so that we can remove it later
	}

	/**
	 * Method addToJenaModel3.
	 * @param st SesameJenaConstructStatement
	 */
	public void addToJenaModel3(SesameJenaConstructStatement st) {
		// if the jena model is not null
		// then add to the new jenaModel and the old one
		if(jenaModel == null)
		{
			//jenaModel = ModelFactory.createDefaultModel(ReificationStyle.Standard);
			//Model baseModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_MICRO_RULE_INF);
			//Model baseModel = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM);
			jenaModel = ModelFactory.createDefaultModel();
		}
		Resource subject = jenaModel.createResource(st.getSubject());
		Property prop = jenaModel.createProperty(st.getPredicate());
		Resource object = jenaModel.createResource(st.getObject()+"");
		com.hp.hpl.jena.rdf.model.Statement jenaSt = null;
		//logger.warn("Adding Statement " + subject + "<>" + prop + "<>" + object);

		jenaSt = jenaModel.createStatement(subject, prop, object);
		/*
		if ((st.getObject()+"").contains("double"))
		{
			Double val = new Double(((Literal)st.getObject()).doubleValue());
			com.hp.hpl.jena.rdf.model.Literal l = ModelFactory.createDefaultModel().createTypedLiteral(val);
			jenaSt = jenaModel.createLiteralStatement(subject, prop, l);
			jenaModel.add(jenaSt);
		}
		else
		{
			jenaModel.add(jenaSt);
		}
		*/
		if (!jenaModel.contains(jenaSt))
		{
			jenaModel.add(jenaSt);
			if(method == CREATION_METHOD.OVERLAY)
			{
				curModel.add(jenaSt);
			}
		}
		//jenaModel.add(jenaSt);
		// just so that we can remove it later
	}

	/**
	 * Method addNodeProperty.
	 * @param subject String
	 * @param object Object
	 * @param predicate String
	 */
	public void addNodeProperty(String subject, Object object, String predicate) {
			logger.debug("Creating property for a vertex" );
			SEMOSSVertex vert1 = vertStore.get(subject);
			if (vert1 == null) {
				vert1 = new SEMOSSVertex(subject);
			}
			//only set property and store vertex if the property does not already exist on the node
			String propName = Utility.getInstanceName(predicate);
			if (vert1.getProperty(propName)==null) {
				vert1.setProperty(propName, object);
				storeVert(vert1);
			}
//			genControlData(vert1);
			//controlData.addProperty(vert1.getProperty(Constants.VERTEX_TYPE)+"", Utility.getClassName(predicate));
	}
	
	private void storeVert(SEMOSSVertex vert){
		if(method == CREATION_METHOD.OVERLAY && incrementalVertStore != null){
			if(!vertStore.containsKey(vert.getProperty(Constants.URI) + "")){
				incrementalVertStore.put(vert.getProperty(Constants.URI) + "", vert);
			}
			else{
				incrementalVertPropStore.put(vert.getProperty(Constants.URI) + "", vert);
			}
		}
		else if(method == CREATION_METHOD.UNDO && incrementalVertStore != null)
			incrementalVertStore.remove(vert.getProperty(Constants.URI) + "");
		vertStore.put(vert.getProperty(Constants.URI) + "", vert);
	}

	private void storeEdge(SEMOSSEdge edge){
		if(method == CREATION_METHOD.OVERLAY && incrementalEdgeStore != null && !edgeStore.containsKey(edge.getProperty(Constants.URI) + ""))
			incrementalEdgeStore.put(edge.getProperty(Constants.URI) + "", edge);
		if(method == CREATION_METHOD.UNDO && incrementalEdgeStore != null)
			incrementalEdgeStore.remove(edge.getProperty(Constants.URI) + "");
		edgeStore.put(edge.getProperty(Constants.URI) + "", edge);
	}
			
	/**
	 * Method addEdgeProperty.
	 * @param subject String
	 * @param object Object
	 * @param predicate String
	 */
	public void addEdgeProperty(String edgeName, Object value, String propName, String outNode, String inNode) {
		logger.debug("Creating property for an edge");
		SEMOSSEdge edge = edgeStore.get(edgeName);

		if(edge == null)
		{
			SEMOSSVertex vert1 = vertStore.get(outNode);
			if (vert1 == null) {
				vert1 = new SEMOSSVertex(outNode);
				storeVert(vert1);
			}
			SEMOSSVertex vert2 = vertStore.get(inNode);
			if (vert2 == null) {
				vert2 = new SEMOSSVertex(inNode + "");
				storeVert(vert2);
			}
			 edge = new SEMOSSEdge(vert1, vert2, edgeName);
		}
		//only set property and store edge if the property does not already exist on the edge
		String propNameInstance = Utility.getInstanceName(propName);
		if (edge.getProperty(propNameInstance)==null)
		{
			edge.setProperty(propNameInstance, value);
			storeEdge(edge);
		}
//			genControlData(edge);
		//controlData.addProperty(edge.getProperty(Constants.EDGE_TYPE)+"", Utility.getClassName(predicate));
	}

	/**
	 * Method findContainsRelation.
	 * @return String
	 */
	public String findContainsRelation()
	{
		String query2 = "SELECT DISTINCT ?Subject ?subProp ?contains WHERE { BIND( <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subProp) BIND( <http://semoss.org/ontologies/Relation/Contains> AS ?contains) {?Subject ?subProp  ?contains}}";

		String containsString = null;
		
		SesameJenaConstructWrapper sjsc = new SesameJenaConstructWrapper();
		
		//IEngine jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);

		
		if(query2.toUpperCase().contains("CONSTRUCT"))
			sjsc = new SesameJenaConstructWrapper();
		else
			sjsc = new SesameJenaSelectCheater();

		// = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);
		sjsc.setQuery(query2);
		sjsc.execute();
		
		// eventually - I will not need the count
		int count = 0;
		while(sjsc.hasNext() && count < 1)
		{
			SesameJenaConstructStatement st = sjsc.next();
			containsString = "<" + st.getSubject() + ">";
			count++;
		}
		
		return containsString;
	}	
	
	/**
	 * Method createBaseURIs.
	 */
	private void createBaseURIs()
	{
		RELATION_URI = DIHelper.getInstance().getProperty(
				Constants.PREDICATE_URI);
		PROP_URI = DIHelper.getInstance()
				.getProperty(Constants.PROP_URI);
	}
	
	public void undoData(){
		System.out.println("rcStore  " + rcStore.toString());
		RepositoryConnection lastRC = rcStore.elementAt(modelCounter-2);
		Model lastModel = modelStore.elementAt(modelCounter-2);
		// remove undo model from repository connection
		try {
			logger.info("Number of undo statements " + lastRC.size());
			logger.info("Number of statements in the old model " + rc.size());
			logger.info("rcStore size              " + rcStore.size());
			logger.info("modelCounter              " + modelCounter);
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		IEngine sesameEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)sesameEngine).setRepositoryConnection(lastRC);
		RDFEngineHelper.removeAllData(sesameEngine, rc);
		//jenaModel.remove(lastModel);
		modelCounter--;
		
		incrementalVertStore.clear();
		incrementalVertStore.putAll(vertStore);
		incrementalEdgeStore.clear();
		incrementalEdgeStore.putAll(edgeStore);
		vertStore.clear();
		edgeStore.clear();
	}
	
	public void redoData(){
		RepositoryConnection newRC = rcStore.elementAt(modelCounter-1);
        Model newModel = modelStore.elementAt(modelCounter-1);
        //add redo model from repository connection
        
        IEngine sesameEngine = new InMemorySesameEngine();
        ((InMemorySesameEngine)sesameEngine).setRepositoryConnection(newRC);
        RDFEngineHelper.addAllData(sesameEngine, rc);
        //jenaModel.add(newModel);
        modelCounter++;

		incrementalVertStore.clear();
		incrementalVertPropStore.clear();
		incrementalEdgeStore.clear();
	}
	
	/**
	 * Method removeFromJenaModel.
	 * @param st SesameJenaConstructStatement
	 */
	protected void removeFromJenaModel(SesameJenaConstructStatement st) {
		Resource subject = jenaModel.createResource(st.getSubject());
		Property prop = jenaModel.createProperty(st.getPredicate());
		Resource object = jenaModel.createResource(st.getObject()+"");
		com.hp.hpl.jena.rdf.model.Statement jenaSt = null;

		logger.warn("Removing Statement " + subject + "<>" + prop + "<>" + object);
		jenaSt = jenaModel.createStatement(subject, prop, object);
		jenaModel.remove(jenaSt);
	}

	/**
	 * Method genBaseConcepts.
	 */
	public void genBaseConcepts()
	{
		// create all the relationships now
		String conceptSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
				  //"{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
				  "{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
//				  "{?Subject ?Predicate ?Object.}" +
				  "BIND(\"\" AS ?Predicate)" + // these are only used so that I can use select cheater...
				  "BIND(\"\" AS ?Object)" +
				  "}";
		if(subclassCreate) //this is used for our metamodel graphs. Need to be subclass of concept rather than type
			conceptSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
				  "{?Subject " + "<http://www.w3.org/2000/01/rdf-schema#subClassOf>  " +  " <http://semoss.org/ontologies/Concept>;}" +
//				  "{?Subject ?Predicate ?Object.}" +
				  "BIND(\"\" AS ?Predicate)" + // these are only used so that I can use select cheater...
				  "BIND(\"\" AS ?Object)" +
				  "}";
		
		logger.info("ConceptSelectQuery query " + conceptSelectQuery);
		
		//IEngine jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);
				
		logger.debug(conceptSelectQuery);
		
		try {
			sjsc.setQuery(conceptSelectQuery);
			sjsc.execute();
			logger.debug("Execute complete");

			int count = 0;
			while(sjsc.hasNext())
			{
				//logger.debug("Iterating " + count);
				count++;

				SesameJenaConstructStatement sct = sjsc.next();

				if(!baseFilterHash.containsKey(sct.getSubject()))// && !baseFilterHash.containsKey(sct.getPredicate()) && !baseFilterHash.containsKey(sct.getObject()+""))
				{
						SEMOSSVertex vert1 = vertStore.get(sct.getSubject()+"");
						if(vert1 == null)
						{
							vert1 = new SEMOSSVertex(sct.getSubject());
							storeVert(vert1);
						}
						// add my friend
//						if(filteredNodes == null || (filteredNodes != null && !filteredNodes.containsKey(sct.getSubject()+"")))
//							this.forest.addVertex(vertStore.get(sct.getSubject()));
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
	public void genBaseGraph()
	{
		// create all the relationships now
		String predicateSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
									  //"VALUES ?Subject {"  + subjects + "}"+
									  //"VALUES ?Object {"  + subjects + "}"+
									  //"VALUES ?Object {"  + objects + "}" +
									  //"VALUES ?Predicate {"  + predicates + "}" +
									  "{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
									  "{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
//									  "{?Object " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
									  "{?Subject ?Predicate ?Object}" +
									  "}";
		if(subclassCreate) //this is used for our metamodel graphs. Need to be subclass of concept rather than type
			predicateSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
									  "{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
									  "{?Subject " + "<http://www.w3.org/2000/01/rdf-schema#subClassOf>  " +  " <http://semoss.org/ontologies/Concept>;}" +
									  "{?Subject ?Predicate ?Object}" +
									  "}";
		
		
		//IEngine jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);
				
		logger.debug(predicateSelectQuery);
		
		try {
			sjsc.setQuery(predicateSelectQuery);
			sjsc.execute();
			logger.warn("Execute compelete");

			int count = 0;
			while(sjsc.hasNext())
			{
				//logger.warn("Iterating " + count);
				count++;

				SesameJenaConstructStatement sct = sjsc.next();
				String predicateName = sct.getPredicate();
				
				if(!baseFilterHash.containsKey(sct.getSubject()) && !baseFilterHash.containsKey(sct.getPredicate()) && !baseFilterHash.containsKey(sct.getObject()+""))
				{
					// get the subject, predicate and object
					// look for the appropriate vertices etc and paint it
					SEMOSSVertex vert1 = vertStore.get(sct.getSubject()+"");
					if(vert1 == null)
					{
						vert1 = new SEMOSSVertex(sct.getSubject());
						storeVert(vert1);
					}
					SEMOSSVertex vert2 = vertStore.get(sct.getObject()+"");
					if(vert2 == null )//|| forest.getInEdges(vert2).size()>=1)
					{
						if(sct.getObject() instanceof URI)
							vert2 = new SEMOSSVertex(sct.getObject()+"");
						else // ok this is a literal
							vert2 = new SEMOSSVertex(sct.getPredicate(), sct.getObject());
						storeVert(vert2);
					}
					// create the edge now
					SEMOSSEdge edge = edgeStore.get(sct.getPredicate()+"");
					// check to see if this is another type of edge
					if(!sct.getPredicate().contains(vert1.getProperty(Constants.VERTEX_NAME).toString()) &&  !sct.getPredicate().contains(vert2.getProperty(Constants.VERTEX_NAME).toString()))
						predicateName = sct.getPredicate() + "/" + vert1.getProperty(Constants.VERTEX_NAME) + ":" + vert2.getProperty(Constants.VERTEX_NAME);
					if(edge == null)
						edge = edgeStore.get(predicateName);
					if(edge == null)
					{
						// need to create the predicate at runtime I think
						/*edge = new DBCMEdge(vert1, vert2, sct.getPredicate());
						System.err.println("Predicate plugged is " + predicateName);
						edgeStore.put(sct.getPredicate()+"", edge);*/
	
						// the logic works only when the predicates dont have the vertices on it.. 
						edge = new SEMOSSEdge(vert1, vert2, predicateName);
						storeEdge(edge);
					}
					//logger.warn("Found Edge " + edge.getURI() + "<<>>" + vert1.getURI() + "<<>>" + vert2.getURI());
	
					
					// add the edge now if the edge does not exist
					// need to handle the duplicate issue again
//					try
//					{	
//						// try to see if the predicate here is a property
//						// if so then add it as a property
//						this.forest.addEdge(edge, vertStore.get(sct.getSubject()+""),
//							vertStore.get(sct.getObject()+""));
//					}catch (Exception ex)
//					{
//						ex.printStackTrace();
//						logger.warn("Missing Edge " + edge.getURI() + "<<>>" + vert1.getURI() + "<<>>" + vert2.getURI());
//						// ok.. I am going to ignore for now that this is a duplicate edge
//					}
				}
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		}	
	}

	//update all internal models associated with this playsheet with the query passed in
	/**
	 * Method updateAllModels.
	 * @param query String
	 */
	public void updateAllModels(String query){
		logger.debug(query);
		
		// run query on rc
		try{
			rc.commit();
		}catch(RepositoryException e){
			logger.debug(e);
		}
		InMemorySesameEngine rcSesameEngine = new InMemorySesameEngine();
		rcSesameEngine.setRepositoryConnection(rc);
		SesameJenaUpdateWrapper sjuw = new SesameJenaUpdateWrapper();
		sjuw.setEngine(rcSesameEngine);
		sjuw.setQuery(query);
		sjuw.execute();
		logger.info("Ran update against rc");

		// run query on curRc
		if(curRC != null){
			InMemorySesameEngine curRcSesameEngine = new InMemorySesameEngine();
			curRcSesameEngine.setRepositoryConnection(curRC);
			sjuw.setEngine(curRcSesameEngine);
			sjuw.setQuery(query);
			sjuw.execute();
			logger.info("Ran update against curRC");
		}

		// run query on jenaModel
		InMemoryJenaEngine modelJenaEngine = new InMemoryJenaEngine();
		modelJenaEngine.setModel(jenaModel);
		sjuw.setEngine(modelJenaEngine);
		sjuw.setQuery(query);
		sjuw.execute();
		logger.info("Ran update against jenaModel");

		// run query on jenaModel
		if (curModel!=null){
			InMemoryJenaEngine curModelJenaEngine = new InMemoryJenaEngine();
			curModelJenaEngine.setModel(curModel);
			sjuw.setEngine(curModelJenaEngine);
			sjuw.setQuery(query);
			sjuw.execute();
			logger.info("Ran update against curModel");
		}
	}
	
	public void setOverlay(boolean overlay){
		if(overlay)
			this.method = CREATION_METHOD.OVERLAY;
		else
			this.method = CREATION_METHOD.CREATE_NEW;
	}

	public void setUndo(boolean undo){
		if(undo)
			this.method = CREATION_METHOD.UNDO;
		else
			this.method = CREATION_METHOD.CREATE_NEW;
	}

	public void setPropSudowlSearch(boolean prop, boolean sudowl, boolean search){
		this.prop = prop;
		this.sudowl = sudowl;
		this.search = search;
	}
	
	public void setSubclassCreate(boolean subclassCreate){
		this.subclassCreate = subclassCreate;
	}

	/**
	 * Method processTraverseCourse.
	 */
	public void processTraverseCourse()
	{
		//if you're at a spot where you have forward models, extensions will reset the future, thus we need to remove all future models
		//modelCounter already added by the time it gets here so you need to -1 to modelCounter
		if (rcStore.size()>=modelCounter-1)
		{
			//have to start removing from teh back of the model to avoid the rcstore from resizing
			//
			for (int modelIdx=rcStore.size()-1;modelIdx>=modelCounter-2;modelIdx--)
			{
				modelStore.remove(modelIdx);
				rcStore.remove(modelIdx);
			}
		}
		modelStore.addElement(curModel);
		rcStore.addElement(curRC);
		logger.debug("Extend : Total Models added = " + modelStore.size());
	}
	
	public Hashtable<String, SEMOSSVertex> getVertStore(){
		return this.vertStore;
	}

	public Hashtable<String, SEMOSSEdge> getEdgeStore(){
		return this.edgeStore;
	}

	public Hashtable<String, SEMOSSVertex> getIncrementalVertStore(){
		return this.incrementalVertStore;
	}

	public Hashtable<String, SEMOSSVertex> getIncrementalVertPropStore(){
		return this.incrementalVertPropStore;
	}

	public Hashtable<String, SEMOSSEdge> getIncrementalEdgeStore(){
		return this.incrementalEdgeStore;
	}
	
	public Model getJenaModel(){
		return jenaModel;
	}
	
	public void removeView(String query, IEngine engine){
		// this will extend it
		// i.e. Checks to see if the node is available
		// if the node is not already there then this predicate wont be added

		SesameJenaConstructWrapper sjw = null;
		String queryCap = query.toUpperCase();
		if(queryCap.startsWith("CONSTRUCT"))
			sjw = new SesameJenaConstructWrapper();
		else
			sjw = new SesameJenaSelectCheater();
		sjw.setEngine(engine);
		sjw.setQuery(query);
		sjw.execute();

		Model curModel = ModelFactory.createDefaultModel();
		
		while (sjw.hasNext()) {
			SesameJenaConstructStatement st = sjw.next();
			org.openrdf.model.Resource subject = new URIImpl(st.getSubject());
			org.openrdf.model.URI predicate = new URIImpl(st.getPredicate());
			String delQuery = "DELETE DATA {";
			// figure out if this is an object later
			Object obj = st.getObject();
			delQuery=delQuery+"<"+subject+"><"+predicate+">";
	
			if((obj instanceof com.hp.hpl.jena.rdf.model.Literal) || (obj instanceof Literal))
			{
	
				delQuery=delQuery+obj+".";
			}
			else 
			{
				delQuery=delQuery+"<"+obj+">";
			}
			//delQuery = "DELETE DATA {<http://health.mil/ontologies/Concept/System/CHCS><http://semoss.org/ontologies/Relation/Provide><http://health.mil/ontologies/Concept/InterfaceControlDocument/CHCS-ABTS-Order_Information>}";
			delQuery = delQuery+"}";
			Update up;
			try {
				up = rc.prepareUpdate(QueryLanguage.SPARQL, delQuery);
				rc.setAutoCommit(false);
				up.execute();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (MalformedQueryException e) {
				e.printStackTrace();
			} catch (UpdateExecutionException e) {
				e.printStackTrace();
			}
			delQuery = delQuery+".";
			//count++;
			logger.debug(delQuery);
		}
		//need to reset this
		vertStore = new Hashtable<String, SEMOSSVertex>();
		edgeStore = new Hashtable<String, SEMOSSEdge>();
		
	}

	/**
	 * Method getPredicateData.
	 * @return PropertySpecData
	 */
	public PropertySpecData getPredicateData() {
		return predData;
	}
	
	public void loadBaseData(IEngine engine){
		// now add the base relationships to the metamodel
		// this links the hierarchy that tool needs to the metamodel being queried
		// eventually this could be a SPIN
		// need to get the engine name and jam it - Done Baby
		if(!loadedOWLS.containsKey(engine.getEngineName()) && engine instanceof AbstractEngine) {
			if(this.baseRelEngine == null){
				this.baseRelEngine = ((AbstractEngine)engine).getBaseDataEngine();
			} else {
				RDFEngineHelper.addAllData(((AbstractEngine)engine).getBaseDataEngine(), this.baseRelEngine.getRC());
			}

			this.baseFilterHash.putAll(((AbstractEngine)engine).getBaseHash());
			
			RDFEngineHelper.addAllData(baseRelEngine, rc);
			loadedOWLS.put(engine.getEngineName(), engine.getEngineName());
		}
		logger.info("BaseQuery");
	}
}
