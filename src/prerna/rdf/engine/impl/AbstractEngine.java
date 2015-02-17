/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.rdf.engine.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
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
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.query.builder.IQueryBuilder;
import prerna.rdf.query.builder.SPARQLQueryTableBuilder;
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
	Properties generalEngineProp = null;
	Properties ontoProp = null;
	RDFFileSesameEngine baseDataEngine;
	protected RDFFileSesameEngine insightBaseXML;

	ValueFactory insightVF = null;
	String engineURI2 = null;
	Hashtable baseDataHash;
	String map = null;
	protected static final String semossURI = "http://semoss.org/ontologies/";
	protected static final String engineBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Engine";
	protected static final String perspectiveBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Perspective";
	protected static final String insightBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Insight";
	protected static final String paramBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Param";
	
	protected static final String enginePerspectiveBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Engine:Perspective";
	protected static final String perspectiveInsightBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Perspective:Insight";
	protected static final String engineInsightBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Engine:Insight";
	protected static final String containsBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Contains";
	protected static final String orderBaseURI = containsBaseURI + "/Order";
	protected static final String labelBaseURI = containsBaseURI + "/Label";
	protected static final String layoutBaseURI = containsBaseURI + "/Layout";
	protected static final String sparqlBaseURI = containsBaseURI + "/SPARQL";
	protected static final String tagBaseURI = containsBaseURI + "/Tag";
	protected static final String descriptionBaseURI = containsBaseURI + "/Description";
	
	protected static final String perspectives = "SELECT DISTINCT ?perspective WHERE {"
			+ "{?enginePerspective <"+Constants.SUBPROPERTY_URI +"> <"+enginePerspectiveBaseURI+"> }"
			+ "{<@engine@> ?enginePerspective ?perspectiveURI.}" 
			+ "{?perspectiveURI <" + labelBaseURI + ">  ?perspective.}" + "}";
	
	protected static final String perspectivesURI = "SELECT DISTINCT ?perspectiveURI WHERE {"
			+ "{?enginePerspective <"+Constants.SUBPROPERTY_URI +"> <"+enginePerspectiveBaseURI+"> }"
			+ "{<@engine@> ?enginePerspective ?perspectiveURI.}" + "}";

	protected static final String insights = "SELECT DISTINCT ?insight WHERE {"
			+ "{?perspectiveURI <"+ labelBaseURI + "> ?perspective .}"
			+ "{?perspectiveInsight <"+Constants.SUBPROPERTY_URI +"> <"+perspectiveInsightBaseURI+"> }"
			+ "{?perspectiveURI ?perspectiveInsight ?insightURI.}"
			+ "{?insightURI <" + labelBaseURI + "> ?insight.}"
			+ "FILTER (regex (?perspective, \"@perspective@\" ,\"i\"))" + "}";
	
	protected static final String insightsURI = "SELECT DISTINCT ?insightURI WHERE {"
			+ "BIND(<@perspective@> AS ?perspectiveURI)"
			+ "{?perspectiveInsight <"+Constants.SUBPROPERTY_URI +"> <"+perspectiveInsightBaseURI+"> }"
			+ "{?perspectiveURI ?perspectiveInsight ?insightURI.}"
			+ "}";
	
	protected static final String orderedInsightsURI = "SELECT DISTINCT ?insightURI WHERE {"
			+ "BIND(<@perspective@> AS ?perspectiveURI)"
			+ "{?perspectiveInsight <"+Constants.SUBPROPERTY_URI +"> <"+perspectiveInsightBaseURI+"> }"
			+ "{?perspectiveURI ?perspectiveInsight ?insightURI.} "
			+ "{?insightURI <" + orderBaseURI + "> ?order } BIND(xsd:decimal(?order) AS ?orderNum)"
			+ "} ORDER BY ?orderNum";

	protected static final String insightsOutputs = "SELECT ?insight ?output WHERE {"
			+ "{?insightURI <" + labelBaseURI + "> ?insight.}"
			+ "{?insightURI <" + layoutBaseURI + "> ?output.}"
			+ "}" + "BINDINGS ?insight {@insightsBindings@}";


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
	
	protected static final String typeSparql = "SELECT ?insight WHERE {"
			+ "{?param <" + "PARAM:TYPE" + "> <@type@>}"
			+ "{?insight <" + "INSIGHT:PARAM" + "> ?param;}"
			+ "{?insightURI <" + labelBaseURI + "> ?insight.}" + "}";

	protected static final String insight4TagSparql = "SELECT ?insight WHERE {"
			+ "{?insightURI <" + tagBaseURI + "> ?tag;}"
			+ "{?insightURI <" + labelBaseURI + "> ?insight.}"
			+ "FILTER (regex (?tag, \"@tag@\" ,\"i\"))"
			+ "}";

	protected static final String tag4InsightSparql = "SELECT ?insight WHERE {"
			+ "{?insightURI <" + tagBaseURI + "> ?tag;}"
			+ "{?insightURI <" + labelBaseURI + "> ?insight.}"
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

	protected String questionXMLFile;
	
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
				
				//loads the questionxmlfile if there is one, if not, get the question sheet and create an xml file and load to engine
				questionXMLFile = prop.getProperty(Constants.INSIGHTS);
				createInsightBase();
				
				if(questionXMLFile != null) {
					createBaseRelationXMLEngine(questionXMLFile);
				}
				else {
					//need to add questionXML to the smss file
					String questionPropFile = prop.getProperty(Constants.DREAMER);
					questionXMLFile = "db/" + getEngineName() + "/" + getEngineName()	+ "_Questions.XML";
					//addConfiguration(Constants.XML, questionXMLFile);
					//saveConfiguration();
					addPropToFile(propFile, Constants.INSIGHTS, questionXMLFile, "ENGINE_TYPE");
					
					if(questionPropFile != null){
						dreamerProp = loadProp(baseFolder + "/" + questionPropFile);
						loadAllPerspectives(engineURI2);
						createInsightBaseRelations();
						createQuestionXMLFile(questionXMLFile, baseFolder);
					}
				}
				/*String questionPropFile = prop.getProperty(Constants.DREAMER);
				if (questionPropFile != null) {
					createInsightBase();
					dreamerProp = loadProp(baseFolder + "/" + questionPropFile);
					loadAllPerspectives(engineURI2);
				}*/
				String ontoFile = prop.getProperty(Constants.ONTOLOGY);
				String owlFile = prop.getProperty(Constants.OWL);
				String genEngPropFile = prop.getProperty(Constants.ENGINE_PROPERTIES);
				if (owlFile != null)
					setOWL(baseFolder + "/" + owlFile);
				System.err.println("Ontology is " + ontoFile);
				if (ontoFile != null)
					setOntology(baseFolder + "/" + ontoFile);
				if (genEngPropFile != null)
					generalEngineProp = loadProp(baseFolder + "/" + genEngPropFile);

			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void addPropToFile(String propFile, String key, String value, String lineLocation){
		
		FileOutputStream fileOut = null;
		File file = new File(propFile);
		ArrayList<String> content = new ArrayList<String>();
		
		BufferedReader reader = null;
		FileReader fr = null;
		
		try{
			fr = new FileReader(file);
			reader = new BufferedReader(fr);
			String line;
			while((line = reader.readLine()) != null){
				content.add(line);
			}
			
			fileOut = new FileOutputStream(file);
			for(int i=0; i<content.size(); i++){
				byte[] contentInBytes = content.get(i).getBytes();
				fileOut.write(contentInBytes);
				fileOut.write("\n".getBytes());
				
				if(content.get(i).contains(lineLocation)){
					String newProp = key + "\t" + value;
					fileOut.write(newProp.getBytes());
					fileOut.write("\n".getBytes());
				}
			}
			
		} catch(IOException e){
			e.printStackTrace();
		} finally{
			try{
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			try{
				fileOut.close();
			} catch (IOException e){
				e.printStackTrace();
			}
		}
	}

	public void createQuestionXMLFile(String questionXMLFile, String baseFolder){
		FileWriter fWrite = null;
		RDFXMLWriter questionXMLWriter = null;
		
		try {
			String xmlFileName = baseFolder + "/" +questionXMLFile;

			fWrite = new FileWriter(xmlFileName);
			questionXMLWriter  = new RDFXMLWriter(fWrite);
			//System.err.println(insightBaseXML.rc);
			insightBaseXML.rc.export(questionXMLWriter);
			
			System.err.println("Created XML Question Sheet at: " + xmlFileName);
		} catch (IOException | RDFHandlerException | RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			try{
				if(fWrite!=null){
					fWrite.close();
				} 
			} catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	
	public void createBaseRelationXMLEngine(String questionXMLFile){
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		insightBaseXML.fileName = baseFolder + "/" + questionXMLFile;
		insightBaseXML.openDB(null);
	}
	
	public String getProperty(String key) {
		String retProp = null;

		System.err.println("Property is " + key + "]");
		if (generalEngineProp != null && generalEngineProp.containsKey(key))
			retProp = generalEngineProp.getProperty(key);
		if (retProp == null && ontoProp != null && ontoProp.containsKey(key))
			retProp = ontoProp.getProperty(key);
		if (retProp == null && prop != null && prop.containsKey(key))
			retProp = prop.getProperty(key);
		return retProp;
	}

	public void createInsightBase() {
		try {
			insightBaseXML = new RDFFileSesameEngine();
			Repository myRepository = new SailRepository(
					new ForwardChainingRDFSInferencer(new MemoryStore()));
			myRepository.initialize();
			//insightBase = myRepository.getConnection();
			insightBaseXML.rc = myRepository.getConnection();
			insightBaseXML.vf = insightBaseXML.rc.getValueFactory();
			insightVF = insightBaseXML.vf;//insightBaseXML.rc.getValueFactory();
			insightBaseXML.sc = ((SailRepositoryConnection) insightBaseXML.rc).getSailConnection();
			engineURI2 = engineBaseURI + "/" + engineName;
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void createInsightBaseRelations() {
		String typePred = RDF.TYPE.stringValue();
		String classURI = Constants.CLASS_URI;
		String propURI = Constants.DEFAULT_PROPERTY_URI;
		String subclassPredicate = Constants.SUBCLASS_URI;
		String subpropertyPredicate = Constants.SUBPROPERTY_URI;
		final String semossConceptURI = semossURI + Constants.DEFAULT_NODE_CLASS;
		final String semossRelationURI = semossURI + Constants.DEFAULT_RELATION_CLASS;
		
		insightBaseXML.addStatement(semossConceptURI, typePred, classURI, true);
		insightBaseXML.addStatement(semossRelationURI, typePred, propURI, true);
		insightBaseXML.addStatement(engineBaseURI, subclassPredicate, semossConceptURI, true);
		insightBaseXML.addStatement(perspectiveBaseURI, subclassPredicate, semossConceptURI, true);
		insightBaseXML.addStatement(insightBaseURI, subclassPredicate, semossConceptURI, true);
		insightBaseXML.addStatement(paramBaseURI, subclassPredicate, semossConceptURI, true);
		insightBaseXML.addStatement(enginePerspectiveBaseURI, subpropertyPredicate, semossRelationURI, true);
		insightBaseXML.addStatement(perspectiveInsightBaseURI, subpropertyPredicate, semossRelationURI, true);
		insightBaseXML.addStatement(engineInsightBaseURI, subpropertyPredicate, semossRelationURI, true);
		insightBaseXML.addStatement(containsBaseURI, subpropertyPredicate, semossRelationURI, true);
		insightBaseXML.addStatement(orderBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true);
		insightBaseXML.addStatement(labelBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true);
		insightBaseXML.addStatement(layoutBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true);
		insightBaseXML.addStatement(sparqlBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true);
		//insightBase.add(insightVF.createURI(typeBaseURI), RDF.TYPE, insightVF.createURI(containsBaseURI));
		insightBaseXML.addStatement(tagBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true);
		insightBaseXML.addStatement(descriptionBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true);
	}

	/**
	 * Load the perspectives for a specific engine.
	 * 
	 * @param List
	 *            of properties
	 */
	public void loadAllPerspectives(String engineURI) {
		
		// this should load the properties from the specified as opposed to
		// loading from core prop
		// lastly the localprop needs to set up so that it can be swapped
		QuestionAdministrator.selectedEngine = engineName;
		QuestionAdministrator questionAdmin = new QuestionAdministrator(this);
		String perspectives = (String) dreamerProp
				.get(Constants.PERSPECTIVE);
		logger.fatal("Perspectives " + perspectives);
		StringTokenizer tokens = new StringTokenizer(perspectives, ";");
		Hashtable perspectiveHash = new Hashtable();
		while (tokens.hasMoreTokens()) {
			String perspective = tokens.nextToken();
			perspectiveHash.put(perspective, perspective);
			String perspectiveInstanceURI = perspectiveBaseURI+"/"+engineName + ":" + perspective;
			String perspectivePred = enginePerspectiveBaseURI +"/" + engineName
					+ Constants.RELATION_URI_CONCATENATOR +engineName + ":" + perspective;

			String key = perspective;
			String qsList = dreamerProp.getProperty(key); // get the ; delimited
			// questions

			Hashtable qsHash = new Hashtable();
			Hashtable layoutHash = new Hashtable();
			if (qsList != null) {
				int count = 1;
				StringTokenizer qsTokens = new StringTokenizer(qsList, ";");
				while (qsTokens.hasMoreElements()) {
					Vector<String> parameterDependList = new Vector<String>();
					Vector<String> parameterQueryList = new Vector<String>();
					Vector<String> parameterOptionList = new Vector<String>();
					// get the question
					String qsKey = qsTokens.nextToken();
					
					String qsDescr = dreamerProp.getProperty(qsKey);
					String layoutName = dreamerProp.getProperty(qsKey + "_"
							+ Constants.LAYOUT);
					
					String qsOrder = count + "";
					
					//qsDescr = count + ". " + qsDescr;

					String sparql = dreamerProp.getProperty(qsKey + "_"
							+ Constants.QUERY);

					String description = dreamerProp.getProperty(qsKey + "_"
							+ Constants.DESCR);

					Hashtable paramHash = Utility.getParams(sparql);

					Enumeration<String> paramKeys = paramHash.keys();

					/*if(qsKey.equals("SysP15")){
						int x = 0;
					}*/
					// loops through to get all param dependencies, queries and options
					while (paramKeys.hasMoreElements()) {
						String param = paramKeys.nextElement();
						String paramKey = param.substring(0, param.indexOf("-"));
						String type = param.substring(param.indexOf("-") + 1);
						
						String qsParamKey = engineName + ":" + perspective + ":" + qsKey + ":" + paramKey;
						
						// see if the param key has a query associated with it
						// usually it is of the form qsKey + _ + paramKey + _ + Query
						String parameterQueryKey = qsKey + "_" + paramKey + "_" + Constants.QUERY;
						if(dreamerProp.containsKey(parameterQueryKey))
						{
							String parameterQueryValue = dreamerProp.getProperty(parameterQueryKey);
							parameterQueryKey = paramKey + "_" + Constants.QUERY;
							parameterQueryList.add(parameterQueryKey+"_-_"+parameterQueryValue);
						}
						// see if there is dependency
						// dependency is of the form qsKey + _ + paramKey + _ + Depend
						String parameterDependKey = qsKey + "_" + paramKey + "_" + Constants.DEPEND;
						if(dreamerProp.containsKey(parameterDependKey))
						{
							// record this
							// qsKey_paramkey  - qsKey:Depends - result
							String parameterDependValue = dreamerProp.getProperty(parameterDependKey);
							parameterDependKey = paramKey + "_" + Constants.DEPEND;
							StringTokenizer depTokens = new StringTokenizer(parameterDependValue, ";");
							
							//one parameter may have multiple dependencies separated by ;
							while(depTokens.hasMoreElements())
							{
								String depToken = depTokens.nextToken();
								parameterDependList.add(parameterDependKey+"_-_"+depToken);
							}
						}
						//see if there is option
						// dependency is of the form qsKey + _ + paramKey + _ + Depend
						/////////String parameterOptionKey = qsKey + "_" +paramKey + "_" + Constants.OPTION;  ------ this is what it should be... oh well
						String parameterOptionKey = type + "_" + Constants.OPTION;
//						System.out.println("CHecking : " + parameterOptionKey);
						/*if (qsKey.contains("T1")){
							int x = 0;
						}*/
						if(dreamerProp.containsKey(parameterOptionKey))
						{
//							System.out.println("TRUE");
							String parameterOptionValue = dreamerProp.getProperty(parameterOptionKey);
							parameterOptionList.add(parameterOptionKey + "_-_" + parameterOptionValue);
						}
					}
					questionAdmin.addQuestion(perspective, qsKey, qsOrder, qsDescr, sparql, layoutName, description, parameterDependList, parameterQueryList, parameterOptionList);
					count++;
				}
				logger.info("Loaded Perspective " + key);
			}				
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
		{
			FileInputStream fis = new FileInputStream(fileName);
			retProp.load(fis);
			fis.close();
		}
		logger.info("Properties >>>>>>>>" + fileName);
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
	 * Sets the name of the engine. This may be a lot of times the same as the
	 * Repository Name
	 * 
	 * @param engineName
	 *            - Name of the engine that this is being set to
	 */

	public void setEngineURI2Name(String engineURI2) {
		this.engineURI2 = engineURI2;
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
		return getSelect(query, insightBaseXML.rc, "perspective");
	}
	public Vector<String> getPerspectivesURI() {
		return getPerspectivesURI(engineURI2 + "");
	}

	public Vector<String> getPerspectivesURI(String engine) {
		Vector<String> retString = new Vector<String>();
		// using SPARQL to do the same thing
		Hashtable paramHash = new Hashtable();
		paramHash.put("engine", engine + "");
		String query = Utility.fillParam(perspectivesURI, paramHash);
		System.err.println("Query is " + query);
		return getSelect(query, insightBaseXML.rc, "perspectiveURI");
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
			return getSelect(query, insightBaseXML.rc, "insight");
		}
		return null;
	}

	public Vector<String> getInsightsURI(String perspective) {

		return getInsightsURI(perspective, engineURI2 + "");
	}

	public Vector<String> getOrderedInsightsURI(String perspective) {
		if (perspective != null) {
			Hashtable paramHash = new Hashtable();
			paramHash.put("perspective", perspective);
			String query = Utility.fillParam(orderedInsightsURI, paramHash);
			System.err.println("Query " + query);
			return getSelect(query, insightBaseXML.rc, "insightURI");
		}
		return null;
	}

	public Vector<String> getInsightsURI(String perspective, String engine) {

		if (perspective != null) {
			Hashtable paramHash = new Hashtable();
			paramHash.put("perspective", perspective);
			String query = Utility.fillParam(insightsURI, paramHash);
			System.err.println("Query " + query);
			return getSelect(query, insightBaseXML.rc, "insightURI");
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
			retV = getSelectObject(query, insightBaseXML.rc);
		}
		return retV;
	}

	public Vector<String> getInsights() {
		//URI perspectivePred = insightVF.createURI(Constants.PERSPECTIVE + ":"
		//		+ Constants.PERSPECTIVE);
		URI qPred = insightVF.createURI(Constants.PERSPECTIVE + ":" + Constants.ID);
		//URI insightPred = labelBaseURI;

		String insights = "SELECT ?insight WHERE {" + "{<" + engineURI2 + "><"
				+ qPred + "> ?insightURI.}" + "{?insightURI <" + labelBaseURI
				+ "> ?insight.}" + "}";

		System.err.println("Query " + insights);

		return getSelect(insights, insightBaseXML.rc, "insight");
	}

	public Vector<String> getInsight4Type(String type) {

		Vector<String> retString = new Vector<String>();
		Hashtable paramHash = new Hashtable();
		paramHash.put("type", type);
		// replacing with SPARQL
		return getSelect(Utility.fillParam(typeSparql, paramHash), insightBaseXML.rc,
				"insight");
	}

	public Vector<String> getInsight4Tag(String tag) {

		Vector<String> retString = new Vector<String>();
		Hashtable paramHash = new Hashtable();
		paramHash.put("tag", tag);
		return getSelect(Utility.fillParam(insight4TagSparql, paramHash),
				insightBaseXML.rc, "insight");
	}

	public Vector<String> getTag4Insight(String insight) {

		Vector<String> retString = new Vector<String>();
		// replacing with SPARQL
		Hashtable paramHash = new Hashtable();
		paramHash.put("insight", insight);
		return getSelect(Utility.fillParam(tag4InsightSparql, paramHash),
				insightBaseXML.rc, "tag");
	}
	
	private Vector<SEMOSSParam> addSEMOSSParams(Vector<SEMOSSParam> retParam,String paramSparql) {
		try {
			TupleQuery query = insightBaseXML.rc.prepareTupleQuery(QueryLanguage.SPARQL, paramSparql);

			System.err.println("SPARQL " + paramSparql);
			TupleQueryResult res = query.evaluate();

			while(res.hasNext())
			{
				BindingSet bs = res.next();
				SEMOSSParam param = new SEMOSSParam();
				param.setName(bs.getBinding("paramLabel").getValue() + "");
				if(bs.getBinding("paramType") != null)
					param.setType(bs.getBinding("paramType").getValue() +"");
				if(bs.getBinding("option") != null)
					param.setOptions(bs.getBinding("option").getValue() + "");
				if(bs.getBinding("query") != null)
					param.setQuery(bs.getBinding("query").getValue() + "");
				if(bs.getBinding("depend") != null)
					param.setDepends(bs.getBinding("depend").getValue() +"");
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
	
	public Vector getParams(String label)
	{
		Vector <SEMOSSParam> retParam = new Vector<SEMOSSParam>();

		URI paramPred = insightVF.createURI("INSIGHT:PARAM");
		URI paramPredLabel = insightVF.createURI("PARAM:LABEL");
		URI queryPred = insightVF.createURI("PARAM:QUERY");
		URI hasDependPred = insightVF.createURI("PARAM:HAS:DEPEND");
		URI dependPred = insightVF.createURI("PARAM:DEPEND");
		URI typePred = insightVF.createURI("PARAM:TYPE");

		String queryParamSparql = "SELECT DISTINCT ?paramLabel ?query ?depend ?dependVar ?paramType WHERE {"
			+"BIND(\"" + label + "\" AS ?insight)"
			+ "{?insightURI <" + labelBaseURI + "> ?insight}"
			+ "{?insightURI <" + paramPred + "> ?param } "
			+ "{?param <" + paramPredLabel + "> ?paramLabel } "
			+ "{?param <" + typePred + "> ?paramType } "
			+ "{?param <" + queryPred + "> ?query } "
			+ "{?param <" + hasDependPred + "> ?depend } " 
			+ "{?param <" + dependPred + "> ?dependVar } "
			+ "}";
			
		
		retParam = addSEMOSSParams(retParam,queryParamSparql);
		
		// check if its empty... if it is, it might be options... otherwise it really has no param
//		if(retParam.isEmpty()){
			URI optionPred = insightVF.createURI("PARAM:OPTION");
			String optionParamSparql = "SELECT DISTINCT ?paramLabel ?option ?paramType WHERE {"
					+"BIND(\"" + label + "\" AS ?insight)"
					+ "{?insightURI <" + labelBaseURI + "> ?insight}"
					+ "{?insightURI <" + paramPred + "> ?param } "
					+ "{?param <" + paramPredLabel + "> ?paramLabel } "
					+ "{?param <" + typePred + "> ?paramType } "
					+ "{?param <" + optionPred + "> ?option } "
					+ "}";	
			retParam = addSEMOSSParams(retParam,optionParamSparql);
//		}

		return retParam;
	}
	
	public Vector getParamsURI(String label)
	{
		Vector <SEMOSSParam> retParam = new Vector<SEMOSSParam>();

		URI paramPred = insightVF.createURI("INSIGHT:PARAM");
		URI paramPredLabel = insightVF.createURI("PARAM:LABEL");
		URI queryPred = insightVF.createURI("PARAM:QUERY");
		URI hasDependPred = insightVF.createURI("PARAM:HAS:DEPEND");
		URI dependPred = insightVF.createURI("PARAM:DEPEND");
		URI typePred = insightVF.createURI("PARAM:TYPE");

		String queryParamSparql = "SELECT ?paramLabel ?query ?depend ?dependVar ?paramType WHERE {"
			+"BIND(<" + label + "> AS ?insightURI)"
			+ "{?insightURI <" + paramPred + "> ?param } "
			+ "{?param <" + paramPredLabel + "> ?paramLabel } "
			+ "{?param <" + typePred + "> ?paramType } "
			+ "{?param <" + queryPred + "> ?query } "
			+ "{?param <" + hasDependPred + "> ?depend } " 
			+ "{?param <" + dependPred + "> ?dependVar } "
			+ "}";
			
		
		retParam = addSEMOSSParams(retParam,queryParamSparql);
		
		// check if its empty... if it is, it might be options... otherwise it really has no param
//		if(retParam.isEmpty()){
			URI optionPred = insightVF.createURI("PARAM:OPTION");
			String optionParamSparql = "SELECT ?paramLabel ?option ?paramType WHERE {"
					+"BIND(<" + label + "> AS ?insightURI)"
					+ "{?insightURI <" + paramPred + "> ?param } "
					+ "{?param <" + paramPredLabel + "> ?paramLabel } "
					+ "{?param <" + typePred + "> ?paramType } "
					+ "{?param <" + optionPred + "> ?option } "
					+ "}";	
			retParam = addSEMOSSParams(retParam,optionParamSparql);
//		}

		return retParam;
	}
	public Vector<Insight> getInsight2URI(String... labels) {
		String bindingsSet = "";
		for (String insight : labels){
			bindingsSet = bindingsSet + "(<" + insight + ">)";
		}
		String insightSparql = "SELECT DISTINCT ?insightURI ?order ?insight ?sparql ?output ?engine ?description WHERE {"
					+ "{?insightURI <" + labelBaseURI + "> ?insight.}"
					+ "{?insightURI <" + sparqlBaseURI + "> ?sparql.}"
					+ "{?insightURI <" + layoutBaseURI + "> ?output.}"
					+ "OPTIONAL {?insightURI <" + orderBaseURI + "> ?order.}"
					+ "OPTIONAL {?insightURI <" + descriptionBaseURI + "> ?description.}" + "}"
					+ "BINDINGS ?insightURI {"+ bindingsSet + "}";
		return processInsight2(insightSparql,labels);
	}
	public Vector<Insight> getInsight2(String... labels) {
		// replace this with the query
		String bindingsSet = "";
		for (String insight : labels){
			bindingsSet = bindingsSet + "(\"" + insight + "\")";
		}
		String insightSparql = "SELECT DISTINCT ?insightURI ?order ?insight ?sparql ?output ?engine ?description WHERE {"
					+ "{?insightURI <" + labelBaseURI + "> ?insight.}"
					+ "{?insightURI <" + sparqlBaseURI + "> ?sparql.}"
					+ "{?insightURI <" + layoutBaseURI + "> ?output.}"
					+ "{?engineInsight <"+Constants.SUBPROPERTY_URI+"> <"+engineInsightBaseURI+">}"
					+ "{?engine ?engineInsight ?insightURI.}"
					+ "OPTIONAL {?insightURI <" + orderBaseURI + "> ?order.}"
					+ "OPTIONAL {?insightURI <" + descriptionBaseURI + "> ?description.}"
					+ "}"
					+ "BINDINGS ?insight {"+ bindingsSet + "}";
		return processInsight2(insightSparql,labels);
	}
	
	public Vector<Insight> processInsight2(String insightSparql,String... labels)
	{
		Vector<Insight> insightV = new Vector<Insight>();
		try{
			System.err.println("Insighter... " + insightSparql + labels);
			System.err.println("Label is " + labels);
			TupleQuery query = insightBaseXML.rc.prepareTupleQuery(
					QueryLanguage.SPARQL, insightSparql);
			TupleQueryResult res = query.evaluate();
	
			while (res.hasNext()) {
				Insight in = new Insight();
				BindingSet bs = res.next();
				in.setId( Utility.getInstanceName(bs.getBinding("insightURI").getValue() + ""));
				
				Literal lit = (Literal)bs.getValue("sparql");
				String sparql = lit.getLabel();
				in.setSparql(sparql);
				
				if(bs.getBinding("order") != null){
					String order = bs.getBinding("order").getValue().stringValue();
					in.setOrder(order);
				}
				String label = bs.getBinding("insight").getValue().stringValue();
				in.setLabel(label);
	
				Literal outputlit = (Literal)bs.getValue("output");
				String output = outputlit.getLabel();
				in.setOutput(output);
	
				String engine = bs.getValue("engine") + "";
				in.setEngine(engine);
				
				if(bs.getBinding("description") != null){
					Literal descriptionlit = (Literal)bs.getValue("description");
					String description = descriptionlit.getLabel();
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
	}
	
	public Insight getInsight(String label) {
		// replace this with the query
		Insight in = new Insight();
		try {
	
			String insightSparql = "SELECT ?insightURI ?order ?insight ?sparql ?output WHERE {"
					+"BIND(\"" + label + "\" AS ?insight)"
					+ "{?insightURI <" + labelBaseURI + "> ?insight.}"
					+ "{?insightURI <" + sparqlBaseURI + "> ?sparql.}"
					+ "{?insightURI <" + layoutBaseURI + "> ?output.}"
					+ "OPTIONAL {?insightURI <" + orderBaseURI + "> ?order.}"
//					+ "FILTER (regex (?insight, \""
//					+ label
//					+ "\" ,\"i\"))"
					+ "}";
			System.err.println("Insighter... " + insightSparql + label);
			System.err.println("Label is " + label);
			TupleQuery query = insightBaseXML.rc.prepareTupleQuery(
					QueryLanguage.SPARQL, insightSparql);
			TupleQueryResult res = query.evaluate();

			if (!res.hasNext())
				in = null;
			while (res.hasNext()) {
				BindingSet bs = res.next();
				in.setId(bs.getBinding("insightURI").getValue() + "");
				String sparql = bs.getBinding("sparql").getValue() + "";
				sparql = sparql.replace("\"", "");
				if(bs.getBinding("order") != null){
					String order = bs.getBinding("order").getValue() + "";
					in.setOrder(order);
				}
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
			in.setOrder("Unknown");
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
	
	private Hashtable<String, Vector<String>> fillNeighborsWithVerbsHash(String query, IEngine engine) {
		Hashtable<String, Vector<String>> retHash = new Hashtable<String, Vector<String>>();
		
		try {
			SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
			sjsw.setEngine(engine);
			sjsw.setQuery(query);
			sjsw.executeQuery();
			String[] var = sjsw.getVariables();
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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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

			if (!res.hasNext()) {
				retString = null;
			}
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
		// I have my insightId, query for the params and options or query making sure that the
		
		String insightLabelSparql = "SELECT DISTINCT ?insightURI ?order ?insight ?sparql ?output ?engine ?description WHERE {"
				+ "{?insightURI <" + labelBaseURI + "> ?insight.}"
				+ "{?insightURI <" + sparqlBaseURI + "> ?sparql.}"
				+ "{?insightURI <" + layoutBaseURI + "> ?output.}"
				+ "{?engineInsight <"+Constants.SUBPROPERTY_URI+"> <"+engineInsightBaseURI+">}"
				+ "{?engine ?engineInsight ?insightURI.}"
				+ "OPTIONAL {?insightURI <" + orderBaseURI + "> ?order.}"
				+ "OPTIONAL {?insightURI <" + descriptionBaseURI + "> ?description.}"
				+ "}"
				+ "BINDINGS ?insightURI {(<"+ insightBaseURI + "/" + insightId + ">)}";
		
		String question = "";

		try{
			TupleQuery labelQuery = insightBaseXML.rc.prepareTupleQuery(
					QueryLanguage.SPARQL, insightLabelSparql);
			TupleQueryResult result = labelQuery.evaluate();
			while (result.hasNext()) {
				BindingSet bs = result.next();
				question = bs.getBinding("insight").getValue() + "";
				System.out.println("Question is: " + question);
			}
		}catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// try to see if this type is available with direct values
		Vector<String> uris = new Vector<String>();

		Vector<SEMOSSParam> paramInfoVector = getParams(question.replace("\"", ""));
		String optionsConcat = null;

		if(!paramInfoVector.isEmpty()){
			for(int i = 0; i < paramInfoVector.size(); i++){
				String test = paramInfoVector.get(i).getName();
				if ((paramInfoVector.get(i).getType()).equals(type) && paramInfoVector.get(i).getOptions() != null
						&& !paramInfoVector.get(i).getOptions().isEmpty()) {
					Vector options = paramInfoVector.get(i).getOptions();
					optionsConcat = "";
					for (int j = 0; j < options.size(); j++) {
						optionsConcat += options.get(j);
						if(j!=options.size()-1){
							optionsConcat += ";";
						}
					}
				}
			}
		}
		
		String options = optionsConcat;//dreamerProp.getProperty(type + "_" + Constants.OPTION);
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
		}
		if(uris.isEmpty()){
			// this needs to be retrieved through SPARQL
			// need to use custom query if it has been specified on the dreamer
			// otherwise use generic fill query
			String sparqlQuery = "";
			if(query != null){
				sparqlQuery = query;
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
		return this.insightBaseXML.rc;
	}
	
	public void setMap(String map) {
		this.map = map;
	}	
	
	public String getInsightDefinition()
	{
		StringWriter output = new StringWriter();
		try {
			insightBaseXML.rc.export(new RDFXMLWriter(output));
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
			baseDataEngine.rc.export(new RDFXMLWriter(output));
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return output.toString();
	}

	public RDFFileSesameEngine getInsightBaseXML() {
		return insightBaseXML;
	}
	
	public String getSMSS(){
		return this.propFile;
	}
	
	public IQueryBuilder getQueryBuilder(){
		return new SPARQLQueryTableBuilder();
	}
	
}
