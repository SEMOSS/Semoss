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

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
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

import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.rdf.InMemoryJenaEngine;
import prerna.engine.impl.rdf.InMemorySesameEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.engine.impl.rdf.SesameJenaConstructStatement;
import prerna.engine.impl.rdf.SesameJenaSelectCheater;
import prerna.engine.impl.rdf.SesameJenaUpdateWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.GraphOWLHelper;
import prerna.ui.components.PropertySpecData;
import prerna.ui.components.RDFEngineHelper;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.Constants;
import prerna.util.JenaSesameUtils;
import prerna.util.Utility;

public class GraphDataModel implements IDataMaker {
	/*
	 * This contains all data that is fundamental to a SEMOSS Graph
	 * This data mainly consists of the edgeStore and vertStore as well as models/repository connections
	 */
	private static final Logger logger = LogManager.getLogger(GraphDataModel.class.getName());
	public enum CREATION_METHOD {CREATE_NEW, OVERLAY, UNDO};
	CREATION_METHOD method;

	Hashtable <String, String> loadedOWLS = new Hashtable<String, String>();

	String RELATION_URI = null;
	String PROP_URI = null;
	public RepositoryConnection rc = null;
	public RepositoryConnection curRC = null;
	public RDFFileSesameEngine baseRelEngine = null;
	public Hashtable baseFilterHash = new Hashtable();
	Model jenaModel = null;
	public Model curModel = null;
	public int modelCounter = 0;
	public Vector <Model> modelStore = new Vector<Model>();
	String containsRelation;
	public Vector <RepositoryConnection> rcStore = new Vector<RepositoryConnection>();
	public PropertySpecData predData = new PropertySpecData();
	public StringBuffer subjects = new StringBuffer("");
	public StringBuffer objects = new StringBuffer("");

	private boolean isPhysicalMetamodel = false;
	private static final String PHYSICAL_NAME = "PhysicalName";
	
	boolean search, prop, sudowl;

	boolean subclassCreate = false; //this is used for our metamodel graphs. Need to be modify the base graph base and base concepts queries to use subclass of concept rather than type

	Hashtable<String, SEMOSSVertex> vertStore = null;
	Hashtable<String, SEMOSSEdge> edgeStore = null;

	//these are used for keeping track of only what was added or subtracted and will only be populated when overlay is true
	Hashtable<String, SEMOSSVertex> incrementalVertStore = null;
	Hashtable<String, SEMOSSVertex> incrementalVertPropStore = null;
	Hashtable<String, SEMOSSEdge> incrementalEdgeStore = null;

	IDatabaseEngine coreEngine;
	String coreQuery;
	
	
	// the user id of the user who executed to create this frame
	// not really important for gdm... not important for h2 frame
	protected String userId;
	// used to determine if the data id has been altered
	// this is only being updated when logic goes through pkql
	protected BigInteger dataId = new BigInteger("0");


	public GraphDataModel(){
		vertStore = new Hashtable<String, SEMOSSVertex>();
		edgeStore =new Hashtable<String, SEMOSSEdge>();
		createBaseURIs();

		sudowl = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.GPSSudowl));
		prop = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.GPSProp));
		search = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.GPSSearch));
	}

	public boolean getSudowl(){
		return this.sudowl;
	}
	public boolean getProp(){
		return this.prop;
	}
	public boolean getSearch(){
		return this.search;
	}

	public void overlayData(String query, IDatabaseEngine engine){

		subjects = new StringBuffer(""); // remove from the old one
		objects = new StringBuffer(""); // remove fromt he old one

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
	
	public void overlayData(Iterator<IConstructStatement> it, IDatabaseEngine engine){

		subjects = new StringBuffer(""); // remove from the old one
		objects = new StringBuffer(""); // remove fromt he old one

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

		processData(it, engine);
		processTraverseCourse();
	}

	public void createModel(String query, IDatabaseEngine engine){
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
	public void fillStoresFromModel(IDatabaseEngine engine){
		if(rc!=null){
			if(containsRelation == null)
				containsRelation = findContainsRelation();
			if(containsRelation == null)
				containsRelation = "<http://semoss.org/ontologies/Relation/Contains>";
			RDFEngineHelper.genNodePropertiesLocal(rc, containsRelation, this, subclassCreate);
			RDFEngineHelper.genEdgePropertiesLocal(rc, containsRelation, this);

			boolean isRDF = (engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.SESAME || engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.JENA || 
					engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.SEMOSS_SESAME_REMOTE);

			String baseConceptSelectQuery = null;
			// the queries below needs to be instrumented later to come from engine
			isRDF = true;
			if(isRDF)
			{
				baseConceptSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
						//"{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
						"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
						//					  "{?Subject ?Predicate ?Object.}" +
						"BIND(\"\" AS ?Predicate)" + // these are only used so that I can use select cheater...
						"BIND(\"\" AS ?Object)" +
						"}";
				if(subclassCreate) //this is used for our metamodel graphs. Need to be subclass of concept rather than type
					baseConceptSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
							"{?Subject " + "<http://www.w3.org/2000/01/rdf-schema#subClassOf>  " +  " <http://semoss.org/ontologies/Concept>;}" +
							//					  "{?Subject ?Predicate ?Object.}" +
							"BIND(\"\" AS ?Predicate)" + // these are only used so that I can use select cheater...
							"BIND(\"\" AS ?Object)" +
							"}";
			}
			else if(engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.RDBMS)
			{
				// change the query here
				baseConceptSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {"
						+ "{?Subject ?Predicate ?Object} "
						+ "} BINDINGS ?Subject {" + subjects + "}";
			}
			try {
				genBaseConcepts(baseConceptSelectQuery);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("Loaded Orphans");


			String predicateSelectQuery = null;
			isRDF = true;
			if(isRDF)
			{
				predicateSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
						//"VALUES ?Subject {"  + subjects + "}"+
						//"VALUES ?Object {"  + subjects + "}"+
						//"VALUES ?Object {"  + objects + "}" +
						//"VALUES ?Predicate {"  + predicates + "}" +
						"{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
						"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
						//  "{?Object " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
						"{?Subject ?Predicate ?Object}" +
						"}";
				if(subclassCreate) //this is used for our metamodel graphs. Need to be subclass of concept rather than type
					predicateSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
							"{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
							"{?Subject " + "<http://www.w3.org/2000/01/rdf-schema#subClassOf>  " +  " <http://semoss.org/ontologies/Concept>;}" +
							"{?Subject ?Predicate ?Object}" +
							"}";





			}
			else if(engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.RDBMS)
			{
				predicateSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {"
						+ "{?Subject ?Predicate ?Object} "
						+ "} BINDINGS ?Subject {" + subjects + "}";
			}


			try {
				genBaseGraph(predicateSelectQuery);//subjects2, predicates2, subjects2);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("Loaded Graph");
		}
	}
	
	public void processData(Iterator<IConstructStatement> sjw, IDatabaseEngine engine) {
		/*logger.debug("Query is " + query);
		sjw.setEngine(engine);
		sjw.setQuery(query);

		try{
			sjw.execute();	
		} catch (RuntimeException e) {
			e.printStackTrace();
		}*/

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
		" WHERE { {?System1  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System1){{?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Data1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}{?System1 ?Upstream ?ICD ;}{?ICD ?Downstream ?System2 ;} {?ICD ?carries ?Data1;}{?carries ?contains2 ?prop2} {?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Upstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?Downstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}  {?ICD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;}{?Data2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?carries2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?System3 ?Upstream2 ?ICD2 ;}{?ICD2 ?Downstream2 ?System1 ;} {?ICD2 ?carries2 ?Data2;} {?carries2 ?contains1 ?prop} {?contains1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System1 ?Provide ?BLU}}}";
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

			//StringBuffer subjects = new StringBuffer(""); - Moved to class scope
			StringBuffer predicates = new StringBuffer("");
			// StringBuffer objects = new StringBuffer(""); - moved to class scope
			//if(!sjw.hasNext())
			//{
			//	logger.info("Came into not having ANY data"); 
			//	return;
			//}			
			logger.info("starting with processing main query");
			while(sjw.hasNext())
			{
				// read the subject predicate object
				// add it to the in memory jena model
				// get the properties
				// add it to the in memory jena model
				IConstructStatement st = sjw.next();
				Object obj = st.getObject();
				logger.debug(st.getSubject() + "<<>>" + st.getPredicate() + "<<>>" + st.getObject());
				//predData.addPredicate2(st.getPredicate());
				//predData.addConceptAvailable(st.getSubject());//, st.getSubject());
				//predData.addPredicateAvailable(st.getPredicate());//, st.getPredicate());

				if(subjects.indexOf("(<" + st.getSubject() + ">)") < 0) {
					if(engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.SESAME || engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.SEMOSS_SESAME_REMOTE || engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.RDBMS) // RDBMS because the the in memory is RDBMS
						subjects.append("(<").append(st.getSubject()).append(">)");
					else if(engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.JENA)
						subjects.append("<").append(st.getSubject()).append(">");
					// do the block for RDBMS - For RDBMS - What I really need are the instances - actually I dont need anything
				}

				if(predicates.indexOf("(<" + st.getPredicate() +">)") < 0) {
					if(engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.SESAME || engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.SEMOSS_SESAME_REMOTE || engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.RDBMS)
						predicates.append("(<").append(st.getPredicate()).append(">)");
					else if(engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.JENA)
						predicates.append("<").append(st.getPredicate()).append(">");
				}

				//TODO: need to find a way to do this for jena too
				if(obj instanceof URI && !(obj instanceof org.apache.jena.rdf.model.Literal)) {			
					if(objects.indexOf("(<" + obj +">)") < 0) {
						if(engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.SESAME || engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.SEMOSS_SESAME_REMOTE || engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.RDBMS)
							objects.append("(<" + obj +">)");
						else if(engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.JENA)
							objects.append("<" + obj +">");
					}
				}
				//addToJenaModel(st);
				addToSesame(st, false, false); // and this will work fine because I am just giving out URIs
				if (search) addToJenaModel3(st);
			}		
			try {
				logger.info("done with processing main query" + rc.size());
			} catch (RepositoryException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			logger.debug("Subjects >>> " + subjects);
			logger.debug("Predicatss >>>> " + predicates);

			if(rc!=null){ // if rc is null here, that means our original query didn't return anything. 

				logger.info("starting with processing base data");
				loadBaseData(engine); // this method will work fine too.. although I have no use for it for later
				logger.info("done with processing base data");
				// load the concept linkages
				// the concept linkages are a combination of the base relationships and what is on the file
				boolean isRDF = (engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.SESAME || engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.JENA || 
						engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.SEMOSS_SESAME_REMOTE);
				boolean loadHierarchy = !(subjects.length()==0 && predicates.length()==0 && objects.length()==0) && isRDF; // Load Hierarchy if and only if this is a RDF Engine - else dont worry about it
				if(loadHierarchy) {
					try {
						logger.info("starting with processing hierarchy");
						RDFEngineHelper.loadConceptHierarchy(engine, subjects.toString(), objects.toString(), this);
						logger.debug("Loaded Concept");
						RDFEngineHelper.loadRelationHierarchy(engine, predicates.toString(), this);
						logger.debug("Loaded Relation");
						try {
							logger.info("done with processing hierarchy"+ rc.size());
						} catch (RepositoryException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} catch(RuntimeException ex) {
						logger.error(Constants.STACKTRACE, ex);
					}
				}

				logger.info("finding contains...");
				containsRelation = findContainsRelation();
				if(containsRelation == null)
					containsRelation = "<http://semoss.org/ontologies/Relation/Contains>";
	
				if(sudowl && isRDF) { // I wont worry about this for now
					logger.info("Starting to load SUDOWL");
					try {
						GraphOWLHelper.loadConceptHierarchy(rc, subjects.toString(), objects.toString(), this);
						GraphOWLHelper.loadRelationHierarchy(rc, predicates.toString(), this);
						GraphOWLHelper.loadPropertyHierarchy(rc,predicates.toString(), containsRelation, this);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					logger.info("Finished loading SUDOWL");
				}
				if(prop && isRDF) { // hmm this is something i need wor worry about but I will work it with an RDF plug right now
					logger.info("Starting to load properties");
					// load local property hierarchy
					try
					{
						//loadPropertyHierarchy(predicates, containsRelation);
						RDFEngineHelper.loadPropertyHierarchy(engine,predicates.toString(), containsRelation, this);
						// now that this is done, we can query for concepts						
						//genPropertiesRemote(propertyQuery + "BINDINGS ?Subject { " + subjects + " " + predicates + " " + objects+ " } ");
						RDFEngineHelper.genPropertiesRemote(engine, subjects.toString(), objects.toString(), predicates.toString(), containsRelation, this);
						try {
							logger.info("DONE:: Loaded Properties" + rc.size());
						} catch (RepositoryException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} catch(RuntimeException ex) {
						logger.error(Constants.STACKTRACE, ex);
					}
					//genProperties(propertyQuery + predicates + " } ");
				}
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		}	
		modelCounter++;
	}

	public void processData(String query, IDatabaseEngine engine) {
		this.coreEngine = engine; // right now we need this for logical names... really shouldn't be needed in this class at all
		this.coreQuery = query;

		// open up the engine
		String queryCap = query.toUpperCase();

		//SesameJenaConstructWrapper sjw = null;
		IConstructWrapper sjw = null;

		if(queryCap.startsWith("CONSTRUCT"))
			sjw = WrapperManager.getInstance().getCWrapper(engine, query);
		else
			sjw = WrapperManager.getInstance().getChWrapper(engine, query);

		processData(sjw, engine);
	}

	/**
	 * Method addToSesame.
	 * @param st SesameJenaConstructStatement
	 * @param overrideURI boolean
	 * @param add2Base boolean
	 */
	public void addToSesame(IConstructStatement st, boolean overrideURI, boolean add2Base) {
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
			if((overrideURI || obj instanceof URI || obj.toString().startsWith("http://")) && !(obj instanceof org.apache.jena.rdf.model.Literal))
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
					baseRelEngine.addStatement(new Object[]{st.getSubject(), st.getPredicate(), st.getObject(), true});
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
					baseRelEngine.addStatement(new Object[]{st.getSubject(), st.getPredicate(), st.getObject(), false});
				}
				rc.add(subject, predicate, (Literal)obj);
			}
			else if(obj instanceof org.apache.jena.rdf.model.Literal || !obj.toString().startsWith("http://"))
			{
				// I need to figure out a way to convert this into sesame literal
				Literal newObj = JenaSesameUtils.asSesameLiteral((org.apache.jena.rdf.model.Literal)obj);
				System.err.println("Adding to sesame " + subject + predicate + rc.getValueFactory().createLiteral(obj+""));

				if(method == CREATION_METHOD.OVERLAY) {
					if (!rc.hasStatement(subject,predicate,newObj, true)) {
						curRC.add(subject,predicate,(Literal)newObj);
					} else {
						return;
					}
				}
				if(add2Base) {
					baseRelEngine.addStatement(new Object[]{st.getSubject(), st.getPredicate(), st.getObject(), false});
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
	public void addToJenaModel3(IConstructStatement st) {
		// if the jena model is not null
		// then add to the new jenaModel and the old one
		if(jenaModel == null)
		{
			//jenaModel = ModelFactory.createDefaultModel(ReificationStyle.Standard);
			//Model baseModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_MICRO_RULE_INF);
			//Model baseModel = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM);
			jenaModel = ModelFactory.createDefaultModel();
		}
		Resource subject = jenaModel.createResource(getDisplayName(st.getSubject()));
		Property prop = jenaModel.createProperty(getDisplayName(st.getPredicate()));
		Resource object = jenaModel.createResource(getDisplayName(st.getObject()+""));
		org.apache.jena.rdf.model.Statement jenaSt = null;
		//logger.warn("Adding Statement " + subject + "<>" + prop + "<>" + object);

		jenaSt = jenaModel.createStatement(subject, prop, object);
		/*
		if ((st.getObject()+"").contains("double"))
		{
			Double val = new Double(((Literal)st.getObject()).doubleValue());
			org.apache.jena.rdf.model.Literal l = ModelFactory.createDefaultModel().createTypedLiteral(val);
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
			
			String displayNameSubject = this.getDisplayName(subject);
			SEMOSSVertex vert1 = vertStore.get(displayNameSubject);
			if (vert1 == null) {
				vert1 = new SEMOSSVertex(displayNameSubject);
				vert1.putProperty(this.PHYSICAL_NAME, Utility.getInstanceName(subject));
			}
			//only set property and store vertex if the property does not already exist on the node
			String propName = this.getDisplayName(predicate); //Utility.getInstanceName(this.getDisplayName(subjectInstance +"%"+ predicate));
			if (vert1.getProperty(propName)==null) {
				if(this.subclassCreate && object instanceof LiteralImpl && ((LiteralImpl)object).getLabel().isEmpty()){
					object = Utility.getInstanceName(this.getDisplayName(predicate));
				}
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

			String displayNameSubject = this.getDisplayName(outNode);
			SEMOSSVertex vert1 = vertStore.get(displayNameSubject);
			if (vert1 == null) {
				vert1 = new SEMOSSVertex(displayNameSubject);
				vert1.putProperty(this.PHYSICAL_NAME, Utility.getInstanceName(outNode));
				storeVert(vert1);
			}
			String displayNameObject = this.getDisplayName(inNode);
			SEMOSSVertex vert2 = vertStore.get(displayNameObject);
			if (vert2 == null) {
				vert2 = new SEMOSSVertex(displayNameObject + "");
				vert2.putProperty(PHYSICAL_NAME, Utility.getInstanceName(outNode));
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

		//SesameJenaConstructWrapper sjsc = new SesameJenaConstructWrapper();
		IConstructWrapper sjsc = null;

		//IDatabase jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IDatabaseEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);


		if(query2.toUpperCase().contains("CONSTRUCT"))
			sjsc = WrapperManager.getInstance().getCWrapper(jenaEngine, query2);
		else
			sjsc = WrapperManager.getInstance().getChWrapper(jenaEngine, query2);

		/*// = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);
		sjsc.setQuery(query2);
		sjsc.execute();
		 */

		// eventually - I will not need the count
		int count = 0;
		while(sjsc.hasNext() && count < 1)
		{
			IConstructStatement st = sjsc.next();
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
		RELATION_URI = Utility.getDIHelperProperty(Constants.PREDICATE_URI);
		PROP_URI = Utility.getDIHelperProperty(Constants.PROP_URI);
	}

	public void undoData(){
		System.out.println("rcStore  " + rcStore.toString());
		RepositoryConnection lastRC = rcStore.elementAt(modelCounter-2);
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
		IDatabaseEngine sesameEngine = new InMemorySesameEngine();
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
		//add redo model from repository connection

		IDatabaseEngine sesameEngine = new InMemorySesameEngine();
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
		org.apache.jena.rdf.model.Statement jenaSt = null;

		logger.warn("Removing Statement " + subject + "<>" + prop + "<>" + object);
		jenaSt = jenaModel.createStatement(subject, prop, object);
		jenaModel.remove(jenaSt);
	}

	/**
	 * Method genBaseConcepts.
	 * @throws Exception 
	 */
	public void genBaseConcepts(String conceptSelectQuery) throws Exception
	{
		// create all the relationships now
		logger.info("ConceptSelectQuery query " + conceptSelectQuery);

		//IDatabase jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IDatabaseEngine jenaEngine = new InMemorySesameEngine();
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
				String physName = sct.getSubject();
				String subject = this.getDisplayName(physName);
				
				if(!baseFilterHash.containsKey(subject))// && !baseFilterHash.containsKey(sct.getPredicate()) && !baseFilterHash.containsKey(sct.getObject()+""))
				{
					SEMOSSVertex vert1 = vertStore.get(subject);
					if(vert1 == null)
					{
						vert1 = new SEMOSSVertex(subject);
						vert1.putProperty(this.PHYSICAL_NAME, Utility.getInstanceName(physName));
						storeVert(vert1);
					}
					// add my friend
					//						if(filteredNodes == null || (filteredNodes != null && !filteredNodes.containsKey(sct.getSubject()+"")))
					//							this.forest.addVertex(vertStore.get(sct.getSubject()));
				}
			}
		}catch(RuntimeException ex)
		{
			logger.error(Constants.STACKTRACE, ex);
		}
	}
	
	// executes the first SPARQL query and generates the graphs
	/**
	 * Method genBaseGraph.
	 * @throws Exception 
	 */
	public void genBaseGraph(String predicateSelectQuery) throws Exception
	{
		// create all the relationships now
		//IDatabase jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IDatabaseEngine jenaEngine = new InMemorySesameEngine();
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
				String predicateName = this.getDisplayName(sct.getPredicate());
				String subPhys = sct.getSubject();
				String subjectName = this.getDisplayName(subPhys);
				String objPhys = sct.getObject()+"";
				String objectName = this.getDisplayName(objPhys);
				
				if(!baseFilterHash.containsKey(subjectName) && !baseFilterHash.containsKey(predicateName) && !baseFilterHash.containsKey(objectName))
				{
					// get the subject, predicate and object
					// look for the appropriate vertices etc and paint it
					SEMOSSVertex vert1 = vertStore.get(subjectName+"");
					if(vert1 == null)
					{
						vert1 = new SEMOSSVertex(subjectName);
						vert1.putProperty(this.PHYSICAL_NAME, Utility.getInstanceName(subPhys));
						storeVert(vert1);
					}
					SEMOSSVertex vert2 = vertStore.get(objectName);
					if(vert2 == null )//|| forest.getInEdges(vert2).size()>=1)
					{
						if(sct.getObject() instanceof URI)
							vert2 = new SEMOSSVertex(objectName);
						else // ok this is a literal
							vert2 = new SEMOSSVertex(predicateName, sct.getObject());
						vert2.putProperty(this.PHYSICAL_NAME, Utility.getInstanceName(objPhys));
						storeVert(vert2);
					}
					// create the edge now
					SEMOSSEdge edge = edgeStore.get(predicateName+"");
					// check to see if this is another type of edge			
					//if(!predicateName.contains(vert1.getProperty(Constants.VERTEX_NAME).toString()) &&  !predicateName.contains(vert2.getProperty(Constants.VERTEX_NAME).toString()))
					if(!predicateName.contains(vert1.getProperty(Constants.VERTEX_NAME) + ":" + vert2.getProperty(Constants.VERTEX_NAME))) {
						// need to not add this check when we make concepts into edges like data network
						if(!predicateName.contains("/ontologies/Concept/")) {
							predicateName = predicateName + "/" + vert1.getProperty(Constants.VERTEX_NAME) + ":" + vert2.getProperty(Constants.VERTEX_NAME);
						}
					}
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
	
	public String getDisplayName(String subKey){
//		if(isPhysicalMetamodel){
			return subKey;
//		} else {
//			return Utility.getTransformedNodeName(coreEngine, subKey, true);
//		}
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
	
	public void setIsPhysicalMetamodel(boolean physicalMetamodel){
		this.isPhysicalMetamodel = physicalMetamodel;
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

	public void setVertStore(Hashtable<String, SEMOSSVertex> vs){
		this.vertStore = vs;
	}

	public void setEdgeStore(Hashtable<String, SEMOSSEdge> es){
		this.edgeStore = es;
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

	public void removeView(String query, IDatabaseEngine engine){
		// this will extend it
		// i.e. Checks to see if the node is available
		// if the node is not already there then this predicate wont be added

		//SesameJenaConstructWrapper sjw = null;
		IConstructWrapper sjw = null;
		String queryCap = query.toUpperCase();
		if(queryCap.startsWith("CONSTRUCT"))
			sjw = WrapperManager.getInstance().getCWrapper(engine, query);
		else
			sjw = WrapperManager.getInstance().getChWrapper(engine, query);
		/*
		sjw.setEngine(engine);
		sjw.setQuery(query);
		sjw.execute();
		 */
		Model curModel = ModelFactory.createDefaultModel();

		while (sjw.hasNext()) {
			IConstructStatement st = sjw.next();
			org.openrdf.model.Resource subject = new URIImpl(st.getSubject());
			org.openrdf.model.URI predicate = new URIImpl(st.getPredicate());
			String delQuery = "DELETE DATA {";
			// figure out if this is an object later
			Object obj = st.getObject();
			delQuery=delQuery+"<"+subject+"><"+predicate+">";

			if((obj instanceof org.apache.jena.rdf.model.Literal) || (obj instanceof Literal))
			{

				delQuery=delQuery+obj+".";
			}
			else 
			{
				delQuery=delQuery+"<"+obj+">";
			}
			//delQuery = "DELETE DATA {<http://health.mil/ontologies/Concept/System/CHCS><http://semoss.org/ontologies/Relation/Provide><http://health.mil/ontologies/Concept/SystemInterface/CHCS-ABTS-Order_Information>}";
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

	public void loadBaseData(IDatabaseEngine engine){
		// now add the base relationships to the metamodel
		// this links the hierarchy that tool needs to the metamodel being queried
		// eventually this could be a SPIN
		// need to get the engine name and jam it - Done Baby
		if(!loadedOWLS.containsKey(engine.getEngineId()) && engine instanceof AbstractDatabaseEngine) {
			if(this.baseRelEngine == null){
				this.baseRelEngine = ((AbstractDatabaseEngine)engine).getBaseDataEngine();
			} else {
				RDFEngineHelper.addAllData(((AbstractDatabaseEngine)engine).getBaseDataEngine(), this.baseRelEngine.getRc());
			}

			this.baseFilterHash.putAll(((AbstractDatabaseEngine)engine).getBaseHash());

			RDFEngineHelper.addAllData(baseRelEngine, rc);
			loadedOWLS.put(engine.getEngineId(), engine.getEngineId());
		}
		logger.info("BaseQuery");
	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		
		processPreTransformations(component, component.getPreTrans());

		//		setPropSudowlSearch();
		this.coreQuery = component.fillQuery();
		this.coreEngine = component.getEngine();

		if(!vertStore.isEmpty()){ // if this is not empty, it means it is a traversal
			setOverlay(true);
		}

		createModel(this.coreQuery, coreEngine);  // instrumented this call

		logger.info("Creating the base Graph");
		fillStoresFromModel(coreEngine); // < This is where the gen base graph models

		//empty incremental stores indicate no new data is available, which means the traversal is invalid. this will set the traversal back one step and remove the invalid traversal from rc and model stores.
		if(getIncrementalVertStore() != null && getIncrementalEdgeStore() != null && getIncrementalVertStore().isEmpty() && getIncrementalEdgeStore().isEmpty() && getIncrementalVertPropStore() != null && getIncrementalVertPropStore().isEmpty() ) {
			setUndo(true);
			undoData();
			fillStoresFromModel(coreEngine);
			rcStore.remove(rcStore.size()-1);
			modelStore.remove(modelStore.size()-1);
		}

	}

	public boolean getOverlay(){
		if(CREATION_METHOD.OVERLAY.equals(this.method)){
			return true;
		}
		return false;
	}

	public IDatabaseEngine getEngine(){
		return this.coreEngine;
	}

	/**
	 * Method getPredicateData.
	 * @return PropertySpecData
	 */
	public PropertySpecData getPredicateData() {
		return predData;
	}

	public String getQuery(){
		return this.coreQuery;
	}

	@Override
	public void processPostTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms, IDataMaker... dataFrame) {
		for(ISEMOSSTransformation transform : transforms){
			transform.setDataMakers(this);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
		}
	}

	@Override
	public void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms){
		for(ISEMOSSTransformation transform : transforms){
			transform.setDataMakers(this);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
		}
	}

	@Override
	public Map getDataMakerOutput(String... selectors) {
		Hashtable retHash = new Hashtable();
//		if(this.getOverlay()){ // removing this for now to keep aligned with tinker frame
//			Map<String, SEMOSSVertex> props = this.getIncrementalVertPropStore();
//			Map<String, SEMOSSVertex> nodes = this.getIncrementalVertStore();
//			props.keySet().removeAll(nodes.keySet());
//			retHash .put("nodes", nodes);
//			retHash.put("nodeProperties", props);
//			retHash.put("edges", this.getIncrementalEdgeStore().values());
//		} else {
			retHash.put("nodes", this.getVertStore());
			retHash.put("edges", this.getEdgeStore().values());
//		}
		return retHash;
	}

	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = new HashMap<String, String>();
//		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
//		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
//		reactorNames.put(PKQLReactor.MATH_FUN.toString(), "prerna.sablecc.MathReactor");
//		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.TinkerColAddReactor");
//		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
//		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
//		reactorNames.put(PKQLEnum.API, "prerna.sablecc.QueryApiReactor");
//		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
//		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
//		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
//		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.ColAddReactor");
//		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.GDMImportDataReactor");
//		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
//		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
//		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
//		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
//		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
//		reactorNames.put(PKQLReactor.VAR.toString(), "prerna.sablecc.VarReactor");
//		reactorNames.put(PKQLReactor.INPUT.toString(), "prerna.sablecc.InputReactor");
//		
//		reactorNames.put(PKQLEnum.QUERY_API, "prerna.sablecc.QueryApiReactor");
//		switch(reactorType) {
//			case IMPORT_DATA : return new GDMImportDataReactor();
//			case COL_ADD : return new ColAddReactor();
//		}
		
		return reactorNames;
	}

	@Override
	/**
	 * Set the user id for the user who created this frame instance
	 */
	public void setUserId(String userId) {
		this.userId = userId;
	}
	
	@Override
	/**
	 * Return the user id for the user who created this frame instance
	 */
	public String getUserId() {
		return this.userId;
	}

	@Override
	/**
	 * Used to update the data id when data has changed within the frame
	 */
	public void updateDataId() {
		this.dataId = this.dataId.add(BigInteger.valueOf(1));
	}
	
	@Override
	/**
	 * Returns the current data id
	 */
	public int getDataId() {
		return this.dataId.intValue();
	}
	
	public void resetDataId() {
		this.dataId = BigInteger.valueOf(0);
	}

	@Override
	public String getDataMakerName() {
		return "GraphDataModel";
	}
}
