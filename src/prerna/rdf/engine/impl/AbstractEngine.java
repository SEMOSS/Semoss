/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.rdf.engine.impl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.RDFEngineHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.ibm.icu.util.StringTokenizer;

/**
 * An Abstract Engine that sets up the base constructs needed to create an
 * engine.
 */
public abstract class AbstractEngine implements IEngine {

	static final Logger logger = LogManager.getLogger(AbstractEngine.class.getName());

	String engineName = null;
	String propFile = null;
	Properties prop = null;
	Properties dreamerProp = null;
	Properties ontoProp = null;
	RDFFileSesameEngine baseDataEngine;
	RepositoryConnection insightBase = null;
	ValueFactory insightVF = null;
	Resource engineURI2 = null;
	Hashtable baseDataHash;
	String map = null;
	
	public static final String perspectives = "SELECT ?perspective WHERE {{<@engine@> <"
			+ Constants.PERSPECTIVE
			+ ":"
			+ Constants.PERSPECTIVE
			+ "> ?perspectiveURI.} {?perspectiveURI <"
			+ Constants.PERSPECTIVE
			+ ":" + Constants.LABEL + ">  ?perspective.}" + "}";

	public static final String insights = "SELECT ?insight WHERE {{?perspectiveURI <"
			+ Constants.PERSPECTIVE
			+ ":"
			+ Constants.LABEL
			+ "> ?perspective .}"
			+ "{?perspectiveURI <"
			+ Constants.PERSPECTIVE
			+ ":"
			+ Constants.ID
			+ "> ?insightURI.}"
			+ "{?insightURI <"
			+ Constants.INSIGHT
			+ ":"
			+ Constants.LABEL
			+ "> ?insight.}"
			+ "FILTER (regex (?perspective, \"@perspective@\" ,\"i\"))" + "}";

	public static final String insightsOutputs = "SELECT ?insight ?output WHERE {"
			+ "{?insightURI <"
			+ Constants.INSIGHT
			+ ":"
			+ Constants.LABEL
			+ "> ?insight.}"
			+ "{?insightURI <"
			+ Constants.INSIGHT
			+ ":"
			+ Constants.OUTPUT
			+ "> ?output.}"
			+ "}"
			+ "BINDINGS ?insight {@insightsBindings@}";


	public static final String fromSparql = "SELECT DISTINCT ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel  ?y} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
					+ "}";

	public static final String toSparql = "SELECT DISTINCT ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel ?y} "
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
					+ "}";

	public static final String fromSparqlWithVerbs = "SELECT DISTINCT ?rel ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel  ?y} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
					+ "}";

	public static final String toSparqlWithVerbs = "SELECT DISTINCT ?rel ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel ?y} "
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
					+ "}";
	
	public static final String typeSparql = "SELECT ?insight WHERE {"
			+ "{<@type@> <" + Constants.INSIGHT + ":" + Constants.TYPE
			+ "> ?insightURI;}" + "{?insightURI <" + Constants.INSIGHT + ":"
			+ Constants.LABEL + "> ?insight.}" + "}";

	public static final String insight4TagSparql = "SELECT ?insight WHERE {{?insightURI <"
			+ Constants.INSIGHT
			+ ":"
			+ Constants.TAG
			+ "> ?tag;}"
			+ ""
			+ "{?insightURI <"
			+ Constants.INSIGHT
			+ ":"
			+ Constants.LABEL
			+ "> ?insight.}"
			+ ""
			+ "FILTER (regex (?tag, \"@tag@\" ,\"i\"))"
			+ "}";

	public static final String tag4InsightSparql = "SELECT ?insight WHERE {"
			+ "{?insightURI <" + Constants.INSIGHT + ":" + Constants.TAG
			+ "> ?tag;}" + "" + "{?insightURI <" + Constants.INSIGHT + ":"
			+ Constants.LABEL + "> ?insight.}" + ""
			+ "FILTER (regex (?insight, \"@insight@\" ,\"i\"))" + "}";


	// some indices for easy retrieval
	// for a given perspective // all the various questions
	// Hashtable<String, Vector> perspectiveInsightHash = new Hashtable<String,
	// Vector>();

	// label to id hash
	// Hashtable<String, Insight> labelIdHash2 = new Hashtable<String,
	// Insight>();

	// entity to label hash
	// Hashtable<String, Vector> typeLabelHash2 = new Hashtable<String,
	// Vector>();

	private String dreamer;

	private String ontology;

	private String owl;

	/**
	 * Opens a database as defined by its properties file. What is included in
	 * the properties file is dependent on the type of engine that is being
	 * initiated. This is the function that first initializes an engine with the
	 * property file at the very least defining the data store.
	 * 
	 * @param propFile
	 *            contains all information regarding the data store and how the
	 *            engine should be instantiated. Dependent on what type of
	 *            engine is being instantiated.
	 */
	@Override
	public void openDB(String propFile) {
		try {
			if (propFile != null) {
				this.propFile = propFile;
				String baseFolder = DIHelper.getInstance().getProperty(
						"BaseFolder");
				String fileName = baseFolder + "/" + propFile;
				System.err.println("Loading file ENGINE " + fileName);
				prop = loadProp(propFile);
				// in here I should also load the questions and insights and
				// everything else
				// get the questions sheet
				// get to the working dir and load it up
				String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
				String questionPropFile = prop.getProperty(Constants.DREAMER);
				if (questionPropFile != null) {
					createInsightBase();
					dreamerProp = loadProp(baseFolder + "/" + questionPropFile);
					loadAllPerspectives(engineURI2);
				}
				String ontoFile = prop.getProperty(Constants.ONTOLOGY);
				String owlFile = prop.getProperty(Constants.OWL);
				if (owlFile != null)
					setOWL(baseFolder + "/" + owlFile);
				System.err.println("Ontology is " + ontoFile);
				if (ontoFile != null)
					setOntology(baseFolder + "/" + ontoFile);

			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getProperty(String key) {

		String retProp = dreamerProp.getProperty(key);
		if (retProp == null && ontoProp != null && ontoProp.containsKey(key))
			retProp = ontoProp.getProperty(key);
		if (retProp == null && prop != null && prop.containsKey(key))
			retProp = prop.getProperty(key);
		return retProp;
	}

	private void createInsightBase() {
		try {
			Repository myRepository = new SailRepository(
					new ForwardChainingRDFSInferencer(new MemoryStore()));
			myRepository.initialize();
			insightBase = myRepository.getConnection();
			insightVF = insightBase.getValueFactory();
			engineURI2 = insightVF.createURI("database:" + engineName);

		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Load the perspectives for a specific engine.
	 * 
	 * @param List
	 *            of properties
	 */
	public void loadAllPerspectives(Resource engineURI) {
		try {
			// this should load the properties from the specified as opposed to
			// loading from core prop
			// lastly the localprop needs to set up so that it can be swapped
			String perspectives = (String) dreamerProp
					.get(Constants.PERSPECTIVE);
			logger.fatal("Perspectives " + perspectives);
			StringTokenizer tokens = new StringTokenizer(perspectives, ";");
			Hashtable perspectiveHash = new Hashtable();
			while (tokens.hasMoreTokens()) {
				String perspective = tokens.nextToken();
				perspectiveHash.put(perspective, perspective); // the value will
																// be replaced
																// later with
																// another hash
				// engineLocalProp.put(Constants.PERSPECTIVE, perspectiveHash);
				// add it to insight base
				// engineName has perspective
				URI perspectiveURI = insightVF.createURI(engineName + ":"
						+ Constants.PERSPECTIVE + ":" + perspective);
				URI perspectivePred = insightVF.createURI(Constants.PERSPECTIVE
						+ ":" + Constants.PERSPECTIVE);
				insightBase.add(engineURI, perspectivePred, perspectiveURI);
				insightBase.add(
						perspectiveURI,
						insightVF.createURI(Constants.PERSPECTIVE + ":"
								+ Constants.LABEL),
						insightVF.createLiteral(perspective));
				loadQuestions(perspective, perspectiveURI, engineURI);

			}
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Loads questions for a particular perspective.
	 * 
	 * @param List
	 *            of properties
	 * @param String
	 *            with the name of the perspective
	 * @param Hashtables
	 *            containing local properties of a specific engine
	 */
	public void loadQuestions(String perspective, URI perspectiveURI,
			Resource engineURI) {
		try {
			String key = perspective;
			String qsList = dreamerProp.getProperty(key); // get the ; delimited
			// questions

			Hashtable qsHash = new Hashtable();
			Hashtable layoutHash = new Hashtable();
			if (qsList != null) {
				int count = 1;
				StringTokenizer qsTokens = new StringTokenizer(qsList, ";");
				while (qsTokens.hasMoreElements()) {
					// get the question
					String qsKey = qsTokens.nextToken();
					
					String qsDescr = dreamerProp.getProperty(qsKey);
					String layoutName = dreamerProp.getProperty(qsKey + "_"
							+ Constants.LAYOUT);
					qsDescr = count + ". " + qsDescr;

					String sparql = dreamerProp.getProperty(qsKey + "_"
							+ Constants.QUERY);

					String description = dreamerProp.getProperty(qsKey + "_"
							+ Constants.DESCR);

					// load it with the entity keys
					Hashtable paramHash = Utility.getParams(sparql);
					// the paramhash keys contains
					// VariableName-VariableType
					// we are only interested in variable type
					// the algorithmic complexity of this is just plain AWESOME
					/*
					 * Insight insight = new Insight();
					 * 
					 * insight.setId(qsKey); insight.setLabel(qsDescr);
					 * insight.setSparql(sparql); insight.setOutput(layoutName);
					 * insight.setDatabaseID(engineName);
					 */
					// add it to insight base
					// engineName has perspective
					// add everything related to the question

					URI qURI = insightVF.createURI(engineName + ":"
							+ perspective + ":" + qsKey);
					URI qPred = insightVF.createURI(Constants.PERSPECTIVE + ":"
							+ Constants.ID);
					URI ePred = insightVF.createURI(Constants.ENGINE + ":"
							+ Constants.ID);
					URI descriptionPred = insightVF.createURI(Constants.INSIGHT + ":"
							+ Constants.DESCR);

					// add the question to the engine
					insightBase.add(engineURI, ePred, qURI);

					// add question to perspective
					// perspective INSIGHT:ID id_of_question(ID)
					insightBase.add(perspectiveURI, qPred, qURI);
					// add description to insight
					// insight INSIGHT:DESCRIPTION description
					if(description != null)
						insightBase.add(qURI, descriptionPred, insightVF.createLiteral(description));
					// perspective INSIGHT:INSIGHT label_of_question - I bet I
					// never use this.. :D
					insightBase.add(
							perspectiveURI,
							insightVF.createURI(Constants.INSIGHT + ":"
									+ Constants.INSIGHT),
							insightVF.createLiteral(qsDescr));

					// add other properties about the question
					// ID insight:label label
					insightBase.add(
							qURI,
							insightVF.createURI(Constants.INSIGHT + ":"
									+ Constants.LABEL),
							insightVF.createLiteral(qsDescr));
					// ID insight:sparql sparql
					insightBase.add(
							qURI,
							insightVF.createURI(Constants.INSIGHT + ":"
									+ Constants.SPARQL),
							insightVF.createLiteral(sparql));
					// ID insight:output output
					insightBase.add(
							qURI,
							insightVF.createURI(Constants.INSIGHT + ":"
									+ Constants.OUTPUT),
							insightVF.createLiteral(layoutName));

					URI perspectivePred = insightVF
							.createURI(Constants.PERSPECTIVE + ":"
									+ Constants.PERSPECTIVE);
					// engine perspective:perspective perspectiveURI
					insightBase.add(engineURI, perspectivePred, perspectiveURI);

					Enumeration<String> paramKeys = paramHash.keys();
					// need to find a way to handle multiple param types
					while (paramKeys.hasMoreElements()) {
						String param = paramKeys.nextElement();
						String paramKey = param.substring(0, param.indexOf("-"));
						String type = param
								.substring(param.indexOf("-") + 1);
						
						String qsParamKey = engineName + ":" + perspective + ":" + qsKey + ":" + paramKey;
						
						// add this parameter to the quri
						insightBase.add(qURI, insightVF.createURI("INSIGHT:PARAM"), insightVF.createURI(qsParamKey));
						insightBase.add(insightVF.createURI(qsParamKey), insightVF.createURI("PARAM:TYPE"), insightVF.createLiteral(type));
						insightBase.add(insightVF.createURI(qsParamKey), insightVF.createURI("INSIGHT:PARAM:LABEL"), insightVF.createLiteral(paramKey));
						
						// see if the param key has a query associated with it
						// usually it is of the form qsKey + _ + paramKey + _ + Query
						String result = DIHelper.getInstance().getProperty(
								"TYPE" + "_" + Constants.QUERY);
						if(dreamerProp.containsKey(qsKey + "_" + paramKey +"_" + Constants.QUERY))
							// record this
							// qskey_paramKey - Entity:Query - result
							result = dreamerProp.getProperty(qsKey + "_" + paramKey +"_" + Constants.QUERY);
							insightBase.add(insightVF.createURI(qsParamKey), insightVF.createURI("PARAM:QUERY"), insightVF.createLiteral(result));							
						
						// see if there is dependency
						// dependency is of the form qsKey + _ + paramKey + _ + Depend
						if(dreamerProp.containsKey(qsKey + "_" + paramKey +"_" + Constants.DEPEND))
						{
							// record this
							// qsKey_paramkey  - qsKey:Depends - result
							result = dreamerProp.getProperty(qsKey + "_" + paramKey +"_" + Constants.DEPEND);
							StringTokenizer depTokens = new StringTokenizer(result, ";");
							insightBase.add(insightVF.createURI(qsParamKey), insightVF.createURI("HAS:PARAM:DEPEND"), insightVF.createLiteral("true"));
							while(depTokens.hasMoreElements())
							{
								String depToken = depTokens.nextToken();
								//String resKey = engineName + ":" + perspective + ":" + qsKey + ":" + depToken;
								insightBase.add(insightVF.createURI(qsParamKey), insightVF.createURI("PARAM:DEPEND"), insightVF.createLiteral(depToken));
							}
						}						
						else
						{
							insightBase.add(insightVF.createURI(qsParamKey), insightVF.createURI("HAS:PARAM:DEPEND"), insightVF.createLiteral("false"));
							insightBase.add(insightVF.createURI(qsParamKey), insightVF.createURI("PARAM:DEPEND"), insightVF.createLiteral("None"));
						}

						// insight.setEntityType(type);

						// add to entity types
						// Vector<String> labels = new Vector<String>();
						// if (typeLabelHash.containsKey(type))
						// labels = typeLabelHash.get(type);
						// labels.addElement(qsDescr);
						// typeLabelHash.put(type, labels);

						// add it to insight base
						// engineName has perspective
						if (type.contains(":")) {
							URI typeURI = insightVF.createURI(type);
							// type INSIGHT:TYPE id_of_the_question
							insightBase.add(
									typeURI,
									insightVF.createURI(Constants.INSIGHT + ":"
											+ Constants.TYPE), qURI);
						}

					}
					// add to list of perspectives
					// Vector<Insight> vec = new Vector<Insight>();
					// if (perspectiveInsightHash.containsKey(key))
					// vec = (Vector) perspectiveInsightHash.get(key);
					// vec.addElement(insight);
					// perspectiveInsightHash.put(key, vec);

					// finally add the translation
					// labelIdHash.put(insight.getLabel(), insight);
					count++;
				}
				logger.info("Loaded Perspective " + key);
			}
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Closes the data base associated with the engine. This will prevent
	 * further changes from being made in the data store and safely ends the
	 * active transactions and closes the engine.
	 */
	@Override
	public void closeDB() {
	}

	/**
	 * Method loadProp. Loads the database properties from a specifed properties
	 * file.
	 * 
	 * @param fileName
	 *            String - The name of the properties file to be loaded.
	 * 
	 * @return Properties - The properties imported from the prop file.
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public Properties loadProp(String fileName) throws FileNotFoundException, IOException {
		Properties retProp = new Properties();
		if(fileName != null)
			retProp.load(new FileInputStream(fileName));
		logger.info("Properties >>>>>>>>" + prop);
		return retProp;
	}

	/**
	 * Runs the passed string query against the engine and returns graph query
	 * results. The query passed must be in the structure of a CONSTRUCT SPARQL
	 * query. The exact format of the results will be dependent on the type of
	 * the engine, but regardless the results are able to be graphed.
	 * 
	 * @param query
	 *            the string version of the query to be run against the engine
	 * 
	 * @return the graph query results
	 */
	@Override
	public Object execGraphQuery(String query) {
		// TODO: Don't return null
		return null;
	}

	/**
	 * Runs the passed string query against the engine as a SELECT query. The
	 * query passed must be in the structure of a SELECT SPARQL query and the
	 * result format will depend on the engine type.
	 * 
	 * @param query
	 *            the string version of the SELECT query to be run against the
	 *            engine
	 * 
	 * @return triple query results that can be displayed as a grid
	 */
	@Override
	public Object execSelectQuery(String query) {
		// TODO: Don't return null
		return null;
	}

	/**
	 * Runs the passed string query against the engine as an INSERT query. The
	 * query passed must be in the structure of an INSERT SPARQL query or an
	 * INSERT DATA SPARQL query and there are no returned results. The query
	 * will result in the specified triples getting added to the data store.
	 * 
	 * @param query
	 *            the INSERT or INSERT DATA SPARQL query to be run against the
	 *            engine
	 */
	@Override
	public void execInsertQuery(String query) throws SailException,
			UpdateExecutionException, RepositoryException,
			MalformedQueryException {

	}

	/**
	 * Gets the type of the engine. The engine type is often used to determine
	 * what API to use while running queries agains the engine.
	 * 
	 * @return the type of the engine
	 */
	@Override
	public ENGINE_TYPE getEngineType() {
		// TODO: Don't return null
		return null;
	}

	/**
	 * Processes a SELECT query just like {@link #execSelectQuery(String)} but
	 * then parses the results to get only their instance names. These instance
	 * names are then returned as the Vector of Strings.
	 * 
	 * @param sparqlQuery
	 *            the SELECT SPARQL query to be run against the engine
	 * 
	 * @return the Vector of Strings representing the instance names of all of
	 *         the query results
	 */
	@Override
	public Vector<String> getEntityOfType(String sparqlQuery) {
		// TODO: Don't return null
		return null;
	}

	/**
	 * Returns whether or not an engine is currently connected to the data
	 * store. The connection becomes true when {@link #openDB(String)} is called
	 * and the connection becomes false when {@link #closeDB()} is called.
	 * 
	 * @return true if the engine is connected to its data store and false if it
	 *         is not
	 */
	@Override
	public boolean isConnected() {
		return false;
	}

	/**
	 * Processes the passed ASK SPARQL query against the engine. The query must
	 * be in the structure of an ASK query and the result will be a boolean
	 * indicating whether or not the data store connected to the engine has
	 * triples matching the pattern of the ASK query.
	 * 
	 * @param query
	 *            the ASK SPARQL query to be run against the engine
	 * 
	 * @return true if the data store connected to the engine contains triples
	 *         that match the pattern of the query and false if it does not.
	 */
	@Override
	public Boolean execAskQuery(String query) {
		// TODO: Don't return null
		return null;
	}

	/**
	 * Sets the name of the engine. This may be a lot of times the same as the
	 * Repository Name
	 * 
	 * @param engineName
	 *            - Name of the engine that this is being set to
	 */
	@Override
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	/**
	 * Gets the engine name for this engine
	 * 
	 * @return Name of the engine it is being set to
	 */
	@Override
	public String getEngineName() {
		return engineName;
	}

	/**
	 * Method addStatement. Processes a given subject, predicate, object triple
	 * and adds the statement to the SailConnection.
	 * 
	 * @param subject
	 *            String - RDF Subject for the triple
	 * @param predicate
	 *            String - RDF Predicate for the triple
	 * @param object
	 *            Object - RDF Object for the triple
	 * @param concept
	 *            boolean - True if the statement is a concept
	 */
	@Override
	public void addStatement(String subject, String predicate, Object object,
			boolean concept) {

	}
	
	/**
	 * Method removeStatement. Processes a given subject, predicate, object triple and removes the statement to the SailConnection.
	 * @param subject		String - RDF Subject for the triple
	 * @param predicate		String - RDF Predicate for the triple
	 * @param object		Object - RDF Object for the triple
	 * @param concept		boolean - True if the statement is a concept
	 */
	@Override
	public void removeStatement(String subject, String predicate, Object object, boolean concept) 
	{

	}

	/**
	 * Commit the database. Commits the active transaction. This operation ends
	 * the active transaction.
	 */
	public void commit() {

	}

	/**
	 * Writes the database back with updated properties if necessary
	 */
	public void saveConfiguration() {
		FileOutputStream fileOut = null;
		try {
			System.err.println("Writing to file " + propFile);
			fileOut = new FileOutputStream(propFile);
			prop.store(fileOut, null);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(fileOut!=null)
					fileOut.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Adds a new property to the properties list.
	 * 
	 * @param name
	 *            String - The name of the property.
	 * @param value
	 *            String - The value of the property.
	 */
	public void addConfiguration(String name, String value) {
		prop.put(name, value);
	}

	/**
	 * Sets the base data engine.
	 * 
	 * @param eng
	 *            - The base data engine that this is being set to
	 */
	public void setBaseData(RDFFileSesameEngine eng) {
		this.baseDataEngine = eng;
	}

	/**
	 * Gets the base data engine.
	 * 
	 * @return RDFFileSesameEngine - the base data engine
	 */
	public RDFFileSesameEngine getBaseDataEngine() {
		return this.baseDataEngine;
	}

	/**
	 * Sets the base data hash
	 * 
	 * @param h
	 *            Hashtable - The base data hash that this is being set to
	 */
	public void setBaseHash(Hashtable h) {
		System.err.println(this.engineName + " Set the Base Data Hash ");
		this.baseDataHash = h;
	}

	/**
	 * Gets the base data hash
	 * 
	 * @return Hashtable - The base data hash.
	 */
	public Hashtable getBaseHash() {
		return this.baseDataHash;
	}

	/**
	 * 
	 */
	public Vector<String> getPerspectives() {
		return getPerspectives(engineURI2 + "");
	}

	public Vector<String> getPerspectives(String engine) {
		Vector<String> retString = new Vector<String>();
		// using SPARQL to do the same thing
		Hashtable paramHash = new Hashtable();
		paramHash.put("engine", engine + "");
		String query = Utility.fillParam(perspectives, paramHash);
		System.err.println("Query is " + query);
		return getSelect(query, insightBase, "perspective");
	}

	public Vector<Object> getInsights(String perspective) {

		return getInsights(perspective, engineURI2 + "");
	}

	public Vector getInsights(String perspective, String engine) {

		if (perspective != null) {
			Hashtable paramHash = new Hashtable();
			paramHash.put("perspective", perspective);
			String query = Utility.fillParam(insights, paramHash);
			System.err.println("Query " + query);
			return getSelect(query, insightBase, "insight");
		}
		return null;
	}

	public Vector<Hashtable<String, String>> getOutputs4Insights(Vector<String> insights) {

		Vector<Hashtable<String, String>> retV = new Vector<Hashtable<String, String>>();
		if (insights != null) {
			//prepare bindings set
			String bindingsSet = "";
			for (String insight : insights){
				bindingsSet = bindingsSet + "(\"" + insight + "\")";
			}
			
			Hashtable paramHash = new Hashtable();
			paramHash.put("insightsBindings", bindingsSet);
			String unfilledQuery = this.insightsOutputs;
			String query = Utility.fillParam(unfilledQuery, paramHash);
			System.err.println("Query " + query);
			retV = getSelectObject(query, insightBase);
		}
		return retV;
	}

	public Vector<String> getInsights() {
		URI perspectivePred = insightVF.createURI(Constants.PERSPECTIVE + ":"
				+ Constants.PERSPECTIVE);
		URI qPred = insightVF.createURI(Constants.PERSPECTIVE + ":" + Constants.ID);
		URI insightPred = insightVF.createURI(Constants.INSIGHT + ":"
				+ Constants.LABEL);

		String insights = "SELECT ?insight WHERE {" + "{<" + engineURI2 + "><"
				+ qPred + "> ?insightURI.}" + "{?insightURI <" + insightPred
				+ "> ?insight.}" + "}";

		System.err.println("Query " + insights);

		return getSelect(insights, insightBase, "insight");
	}

	public Vector<String> getInsight4Type(String type) {

		Vector<String> retString = new Vector<String>();
		Hashtable paramHash = new Hashtable();
		paramHash.put("type", type);
		// replacing with SPARQL
		return getSelect(Utility.fillParam(typeSparql, paramHash), insightBase,
				"insight");
	}

	public Vector<String> getInsight4Tag(String tag) {

		Vector<String> retString = new Vector<String>();
		Hashtable paramHash = new Hashtable();
		paramHash.put("tag", tag);
		return getSelect(Utility.fillParam(insight4TagSparql, paramHash),
				insightBase, "insight");
	}

	public Vector<String> getTag4Insight(String insight) {

		Vector<String> retString = new Vector<String>();
		// replacing with SPARQL
		Hashtable paramHash = new Hashtable();
		paramHash.put("insight", insight);
		return getSelect(Utility.fillParam(tag4InsightSparql, paramHash),
				insightBase, "tag");
	}
	
	public Vector getParams(String label)
	{
		Vector <SEMOSSParam> retParam = new Vector<SEMOSSParam>();
		try {
			URI insightPred = insightVF.createURI(Constants.INSIGHT + ":"
					+ Constants.LABEL);
			URI paramPred = insightVF.createURI("INSIGHT:PARAM");
			URI paramPredLabel = insightVF.createURI("INSIGHT:PARAM:LABEL");
			URI queryPred = insightVF.createURI("PARAM:QUERY");
			URI hasDependPred = insightVF.createURI("HAS:PARAM:DEPEND");
			URI dependPred = insightVF.createURI("PARAM:DEPEND");
			URI typePred = insightVF.createURI("PARAM:TYPE");

			String paramSparql = "SELECT ?paramLabel ?query ?depend ?dependVar ?paramType WHERE {"
				+"BIND(\"" + label + "\" AS ?insight)"
				+ "{?insightURI <"
				+ insightPred
				+ "> ?insight}"
				+ "{?insightURI <" + paramPred + "> ?param } "
				+ "{?param <" + paramPredLabel + "> ?paramLabel } "
				+ "{?param <" + typePred + "> ?paramType } "
				+ "{?param <" + queryPred + "> ?query } "
				+ "{?param <" + hasDependPred + "> ?depend } " 
				+ "{?param <" + dependPred + "> ?dependVar } "
				+ "}";
			TupleQuery query = insightBase.prepareTupleQuery(
					QueryLanguage.SPARQL, paramSparql);
			
			System.err.println("SPARQL " + paramSparql);
			TupleQueryResult res = query.evaluate();
			
			
			while(res.hasNext())
			{
				BindingSet bs = res.next();
				SEMOSSParam param = new SEMOSSParam();
				param.setName(bs.getBinding("paramLabel").getValue() + "");
				if(bs.getBinding("query") != null)
					param.setQuery(bs.getBinding("query").getValue() + "");
				if(bs.getBinding("depend") != null)
					param.setDepends(bs.getBinding("depend").getValue() +"");
				if(bs.getBinding("paramType") != null)
					param.setType(bs.getBinding("paramType").getValue() +"");
				if(bs.getBinding("dependVar") != null)
					param.addDependVar(bs.getBinding("dependVar").getValue() +"");
				
				
				retParam.addElement(param);
				
				System.out.println(param.getName() + param.getQuery() + param.isDepends() + param.getType());
				
				
			}
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retParam;
	}
	

	
	public Vector<Insight> getInsight2(String... labels) {
		// replace this with the query
		String bindingsSet = "";
		for (String insight : labels){
			bindingsSet = bindingsSet + "(\"" + insight + "\")";
		}
		
		Vector<Insight> insightV = new Vector<Insight>();
		try {
			URI insightPred = insightVF.createURI(Constants.INSIGHT + ":"
					+ Constants.LABEL);

			String insightSparql = "SELECT DISTINCT ?insightURI ?insight ?sparql ?output ?engine ?description WHERE {"
					+ "{?insightURI <"
					+ insightPred
					+ "> ?insight.}"
					+ "{?insightURI <" 
					+ Constants.INSIGHT + ":" + Constants.SPARQL
					+ "> ?sparql.}"
					+ "{?insightURI <"
					+ Constants.INSIGHT + ":" + Constants.OUTPUT
					+ "> ?output.}"
					+ "{?engine <"
					+ Constants.ENGINE + ":" + Constants.ID
					+ "> ?insightURI.}"
					+ "OPTIONAL {?insightURI <"
					+ Constants.INSIGHT + ":" + Constants.DESCR
					+ "> ?description.}"
					+ "}"
					+ "BINDINGS ?insight {"+ bindingsSet + "}";
			System.err.println("Insighter... " + insightSparql + labels);
			System.err.println("Lable is " + labels);
			TupleQuery query = insightBase.prepareTupleQuery(
					QueryLanguage.SPARQL, insightSparql);
			TupleQueryResult res = query.evaluate();

			while (res.hasNext()) {
				Insight in = new Insight();
				BindingSet bs = res.next();
				in.setId(bs.getBinding("insightURI").getValue() + "");
				String sparql = bs.getBinding("sparql").getValue() + "";
				sparql = sparql.replace("\"", "");
				in.setSparql(sparql);
				String label = bs.getBinding("insight").getValue().stringValue();
				in.setLabel(label);
				String output = bs.getBinding("output").getValue() + "";
				output = output.replace("\"", "");
				in.setOutput(output);
				String engine = bs.getBinding("engine").getValue() + "";
				engine = engine.replace("\"", "");
				in.setEngine(engine);
				if(bs.getBinding("description") != null){
					String description = bs.getBinding("description").getValue() + "";
					description = description.replace("\"", "");
					in.setDescription(description);
				}
				insightV.add(in);
				System.err.println(in.toString());
			}
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (insightV.isEmpty()) {
			// in = labelIdHash.get(label);
			Insight in = new Insight();
			in = new Insight();
			in.setLabel("");
			in.setOutput("Unknown");
			in.setId("DN");
			in.setSparql("This will not work");
			insightV.add(in);
			System.err.println("Using Label ID Hash ");
		}
		return insightV;
		// return labelIdHash.get(label);
	}
	
	public Insight getInsight(String label) {
		// replace this with the query
		Insight in = new Insight();
		try {
			URI insightPred = insightVF.createURI(Constants.INSIGHT + ":"
					+ Constants.LABEL);

			String insightSparql = "SELECT ?insightURI ?insight ?sparql ?output WHERE {"
					+"BIND(\"" + label + "\" AS ?insight)"
					+ "{?insightURI <"
					+ insightPred
					+ "> ?insight.}"
					+ "{?insightURI <"
					+ insightVF.createURI(Constants.INSIGHT + ":"
							+ Constants.SPARQL)
					+ "> ?sparql.}"
					+ "{?insightURI <"
					+ insightVF.createURI(Constants.INSIGHT + ":"
							+ Constants.OUTPUT)
					+ "> ?output.}"
//					+ "FILTER (regex (?insight, \""
//					+ label
//					+ "\" ,\"i\"))"
					+ "}";
			System.err.println("Insighter... " + insightSparql + label);
			System.err.println("Lable is " + label);
			TupleQuery query = insightBase.prepareTupleQuery(
					QueryLanguage.SPARQL, insightSparql);
			TupleQueryResult res = query.evaluate();

			if (!res.hasNext())
				in = null;
			while (res.hasNext()) {
				BindingSet bs = res.next();
				in.setId(bs.getBinding("insightURI").getValue() + "");
				String sparql = bs.getBinding("sparql").getValue() + "";
				sparql = sparql.replace("\"", "");
				String label2 = bs.getBinding("insight").getValue() + "";
				// label2 = label2.replace("\"", "");
				System.err.println("Came in here " + sparql + label2);
				System.err.println("Came in here " + label2);
				in.setSparql(sparql);
				String output = bs.getBinding("output").getValue() + "";
				output = output.replace("\"", "");
				in.setOutput(output);
				System.err.println(in);
			}
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (in == null) {
			// in = labelIdHash.get(label);
			in = new Insight();
			in.setLabel(label);
			in.setOutput("Unknown");
			in.setId("DN");
			in.setSparql("This will not work");
			System.err.println("Using Label ID Hash ");
		}
		return in;
		// return labelIdHash.get(label);
	}

	// sets the dreamer
	public void setDreamer(String dreamer) {
		this.dreamer = dreamer;
	}

	// sets the dreamer
	public void setOntology(String ontology) {
		System.err.println("Ontology file is " + ontology);
		this.ontology = ontology;

		if (ontoProp == null) {
			ontoProp = new Properties();
			try {
				ontoProp = loadProp(ontology);
				createBaseRelationEngine();

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void setOWL(String owl) {
		this.owl = owl;
	}

	// need to complete this
	/**
	 * Checks for an OWL and adds it to the engine. Sets the base data hash from
	 * the engine properties, commits the database, and creates the base
	 * relation engine.
	 * 
	 * @param List
	 *            of properties for a specific engine
	 * @param Engine
	 *            to set
	 */
	public void createBaseRelationEngine() {
		RDFFileSesameEngine baseRelEngine = new RDFFileSesameEngine();
		Hashtable baseHash = new Hashtable();
		// If OWL file doesn't exist, go the old way and create the base
		// relation engine
		// String owlFileName =
		// (String)DIHelper.getInstance().getCoreProp().get(engine.getEngineName()
		// + "_" + Constants.OWL);
		if (owl == null) {
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			owl = baseFolder + "/db/" + getEngineName() + "/" + getEngineName()	+ "_OWL.OWL";
		}
		if(ontoProp != null)
		{
			if (ontoProp.containsKey("BaseData")) {
				// TODO: Need to find a way to write this into the prop file
				try {
					System.err.println("Executed this block");
					baseHash = createBaseRelations(ontoProp, baseRelEngine);
				} catch (Exception e) {
					// TODO: Specify exception
					e.printStackTrace();
				}
			}
		}
		baseRelEngine.fileName = owl;
		baseRelEngine.openDB(null);
		if(prop != null) {
			addConfiguration(Constants.OWL, owl);
		}
		
		try {
			baseHash.putAll(RDFEngineHelper.loadBaseRelationsFromOWL(owl));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		setBaseHash(baseHash);
		
		baseRelEngine.commit();
		setBaseData(baseRelEngine);
	}
	
	/**
	 * Creates base relations for a specific engine and RDF Map. Splits RDF map
	 * values into tokens in order to obtain the subject/predicate/object
	 * triple. Put these values into the base filter hash and then add the
	 * triple into the base relation engine.
	 * 
	 * @param RDF
	 *            map
	 * @param The
	 *            base sesame engine for the RDF map
	 * 
	 * @return Hashtable containing base triple relations
	 */
	private Hashtable createBaseRelations(Properties rdfMap, RDFFileSesameEngine baseRelEngine) {
		String relationName = "BaseData";
		Hashtable baseFilterHash = new Hashtable();
		if (rdfMap.containsKey(relationName)) { // load using what is on the map
			String value = rdfMap.getProperty(relationName);
			// System.out.println(" Relations are " + value);
			StringTokenizer relTokens = new StringTokenizer(value, ";");
			while (relTokens.hasMoreTokens()) {
				String rel = relTokens.nextToken();
				String relNames = rdfMap.getProperty(rel);
				StringTokenizer rdfTokens = new StringTokenizer(relNames, ";");

				while (rdfTokens.hasMoreTokens()) {
					String nextToken = rdfTokens.nextToken();
					// System.err.println(" Next token ... " + nextToken);
					StringTokenizer stmtTokens = new StringTokenizer(nextToken, "+");
					String subject = stmtTokens.nextToken();
					String predicate = stmtTokens.nextToken();
					String object = stmtTokens.nextToken();
					baseFilterHash.put(subject, subject);
					baseFilterHash.put(object, object);
					baseFilterHash.put(predicate, predicate);
					// create the statement now
					baseRelEngine.addStatement(subject, predicate, object, true);
				}// statement while
			}// relationship while
		}// if using map
		return baseFilterHash;
	}

	// gets the from neighborhood for a given node
	public Vector<String> getFromNeighbors(String nodeType, int neighborHood) {
		// this is where this node is the from node
		Hashtable paramHash = new Hashtable();
		paramHash.put("nodeType", nodeType);
		return baseDataEngine.getEntityOfType(Utility.fillParam(fromSparql, paramHash));
	}

	// gets the to nodes
	public Vector<String> getToNeighbors(String nodeType, int neighborHood) {
		// this is where this node is the to node
		Hashtable paramHash = new Hashtable();
		paramHash.put("nodeType", nodeType);
		return baseDataEngine.getEntityOfType(Utility.fillParam(toSparql, paramHash));
	}

	// gets the from neighborhood for a given node
	public Hashtable<String, Vector<String>> getFromNeighborsWithVerbs(String nodeType, int neighborHood) {
		// this is where this node is the from node
		Hashtable paramHash = new Hashtable();
		paramHash.put("nodeType", nodeType);
		String query = Utility.fillParam(fromSparqlWithVerbs, paramHash);
		
		return fillNeighborsWithVerbsHash(query, baseDataEngine);
	}

	// gets the to nodes
	public Hashtable<String, Vector<String>> getToNeighborsWithVerbs(String nodeType, int neighborHood) {
		// this is where this node is the to node
		Hashtable paramHash = new Hashtable();
		paramHash.put("nodeType", nodeType);
		String query = Utility.fillParam(toSparqlWithVerbs, paramHash);
		
		return fillNeighborsWithVerbsHash(query, baseDataEngine);
	}
	
	private Hashtable<String, Vector<String>> fillNeighborsWithVerbsHash(String query, IEngine engine){
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();
		String[] var = sjsw.getVariables();
		Hashtable<String, Vector<String>> retHash = new Hashtable<String, Vector<String>>();
		while(sjsw.hasNext()){
			SesameJenaSelectStatement sjss = sjsw.next();
			String verb = sjss.getRawVar(var[0]) + "";
			String node = sjss.getRawVar(var[1]) + "";
			Vector<String> verbVect = new Vector<String>();
			if(retHash.containsKey(verb))
				verbVect = retHash.get(verb);
			verbVect.add(node);
			retHash.put(verb, verbVect);
		}
		return retHash;
	}
	
	// gets the from and to nodes
	public Vector<String> getNeighbors(String nodeType, int neighborHood) {
		Vector<String> from = getFromNeighbors(nodeType, 0);
		Vector<String> to = getToNeighbors(nodeType, 0);
		from.addAll(to);
		return from;
	}

	private Vector<String> getSelect(String sparql, RepositoryConnection rc,
			String variable) {
		Vector<String> retString = new Vector<String>();

		try {
			TupleQuery query = rc.prepareTupleQuery(QueryLanguage.SPARQL,
					sparql);
			TupleQueryResult res = query.evaluate();

			if (!res.hasNext())
				retString = null;

			while (res.hasNext()) {
				String tag = res.next().getBinding(variable).getValue() + "";
				tag = tag.replace("\"", "");
				retString.addElement(tag);
			}
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retString;

	}
	
	private Vector<Hashtable<String, String>> getSelectObject(String sparql, RepositoryConnection rc) {
		Vector<Hashtable<String, String>> retString = new Vector<Hashtable<String, String>>();

		try {
			TupleQuery query = rc.prepareTupleQuery(QueryLanguage.SPARQL,
					sparql);
			TupleQueryResult res = query.evaluate();
			List<String> names = res.getBindingNames();

			if (!res.hasNext())
				retString = null;

			while (res.hasNext()) {
				Hashtable rowHash = new Hashtable();
				BindingSet bs = res.next();
				for(int colIdx = 0; colIdx < names.size(); colIdx ++){
					String variable = names.get(colIdx);
					Object value = ((Value)bs.getBinding(variable).getValue()).stringValue();
					System.out.println("Variable :: " + variable);
					System.out.println("Value :: " + value);
					rowHash.put(variable, value);
				}
				retString.addElement(rowHash);
			}
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retString;

	}

	public Vector<String> getParamValues(String name, String type, String insightId) 
	{
		String query = DIHelper.getInstance().getProperty(
				"TYPE" + "_" + Constants.QUERY);
		return getParamValues(name, type, insightId, query);
	}
	// gets the param values for a parameter
	public Vector<String> getParamValues(String name, String type, String insightId, String query) {
		// TODO
		// try to see if this type is available with direct values
		Vector<String> uris = new Vector<String>();
		String options = dreamerProp.getProperty(type + "_" + Constants.OPTION);
		String customQuery = query ;//dreamerProp.getProperty(insightId.substring(insightId.lastIndexOf(":")+1) + "_" + name + "_" + Constants.QUERY);
		if (options != null) {
			StringTokenizer tokens = new StringTokenizer(options, ";");
			// sorry for the cryptic crap below
			int tknIndex = 0;
			for (; tokens.hasMoreTokens(); tknIndex++) {
//				Node node = new Node();
				String token = tokens.nextToken();
//				node.setLabel(token);
//				node.setURI(token);
//				node.setType(type);
				uris.addElement(token);
			}
		} else {
			// this needs to be retrieved through SPARQL
			// need to use custom query if it has been specified on the dreamer
			// otherwise use generic fill query
			String sparqlQuery = "";
			if(customQuery != null){
				sparqlQuery = customQuery;
			}else { 
				sparqlQuery = DIHelper.getInstance().getProperty(
					"TYPE" + "_" + Constants.QUERY);
			}

			Hashtable paramTable = new Hashtable();
			paramTable.put(Constants.ENTITY, type);
			sparqlQuery = Utility.fillParam(sparqlQuery, paramTable);
			Vector <String> entities = getEntityOfType(sparqlQuery);
			for(int entityIndex = 0;entityIndex < entities.size();entityIndex++)
			{
				String entity = entities.elementAt(entityIndex);
//				Node node = new Node();
//				node.setLabel(Utility.getInstanceName(entity));
//				node.setType(type);
//				node.setURI(entity);
				uris.addElement(entity);
			}
		}
		return uris;
		// return null;
	}
	
	// gets the OWL engine
	// this needs to change later
	public RepositoryConnection getOWL()
	{
		return baseDataEngine.rc;
	}
	
	public RepositoryConnection getInsightDB()
	{
		return this.insightBase;
	}
	
	public void setMap(String map) {
		this.map = map;
	}	
	
	public String getInsightDefinition()
	{
		StringWriter output = new StringWriter();
		try {
			insightBase.export(new RDFXMLPrettyWriter(output));
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return output.toString();
	}

	public String getOWLDefinition()
	{
		StringWriter output = new StringWriter();
		try {
			baseDataEngine.rc.export(new RDFXMLPrettyWriter(output));
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return output.toString();
	}

	
}
