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
package prerna.engine.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.Binding;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.openrdf.sail.memory.model.MemLiteral;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.wrappers.WrapperManager;
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

	private static final Logger logger = LogManager.getLogger(AbstractEngine.class.getName());

	protected String engineName = null;
	private String propFile = null;
	protected Properties prop = null;
	private Properties dreamerProp = null;
	private Properties generalEngineProp = null;
	private Properties ontoProp = null;
	protected RDFFileSesameEngine baseDataEngine;
	protected RDFFileSesameEngine insightBaseXML;

	protected String engineURI2 = null;
	private Hashtable<String, String> baseDataHash;
	private static final String semossURI = "http://semoss.org/ontologies/";
	protected static final String engineBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Engine";
	private static final String perspectiveBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Perspective";
	private static final String insightBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Insight";
	private static final String paramBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Param";
	
	private static final String enginePerspectiveBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Engine:Perspective";
	private static final String perspectiveInsightBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Perspective:Insight";
	private static final String engineInsightBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Engine:Insight";
	private static final String containsBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Contains";
	private static final String orderBaseURI = containsBaseURI + "/Order";
	private static final String labelBaseURI = containsBaseURI + "/Label";
	private static final String layoutBaseURI = containsBaseURI + "/Layout";
	private static final String sparqlBaseURI = containsBaseURI + "/SPARQL";
	private static final String tagBaseURI = containsBaseURI + "/Tag";
	private static final String descriptionBaseURI = containsBaseURI + "/Description";
	private static final String TIME_STAMP_URI = containsBaseURI + "/TimeStamp";

	private static final String perspectives = "SELECT DISTINCT ?perspective WHERE {"
			+ "{?enginePerspective <"+Constants.SUBPROPERTY_URI +"> <"+enginePerspectiveBaseURI+"> }"
			+ "{<@engine@> ?enginePerspective ?perspectiveURI.}" 
			+ "{?perspectiveURI <" + labelBaseURI + ">  ?perspective.}" + "}";
	
	private static final String perspectivesURI = "SELECT DISTINCT ?perspectiveURI WHERE {"
			+ "{?enginePerspective <"+Constants.SUBPROPERTY_URI +"> <"+enginePerspectiveBaseURI+"> }"
			+ "{<@engine@> ?enginePerspective ?perspectiveURI.}" + "}";

	private static final String insights = "SELECT DISTINCT ?insight WHERE {"
			+ "{?perspectiveURI <"+ labelBaseURI + "> ?perspective .}"
			+ "{?perspectiveInsight <"+Constants.SUBPROPERTY_URI +"> <"+perspectiveInsightBaseURI+"> }"
			+ "{?perspectiveURI ?perspectiveInsight ?insightURI.}"
			+ "{?insightURI <" + labelBaseURI + "> ?insight.}"
			+ "FILTER (regex (?perspective, \"^@perspective@$\" ,\"i\"))" + "}";
	
	private static final String insightsURI = "SELECT DISTINCT ?insightURI WHERE {"
			+ "BIND(<@perspective@> AS ?perspectiveURI)"
			+ "{?perspectiveInsight <"+Constants.SUBPROPERTY_URI +"> <"+perspectiveInsightBaseURI+"> }"
			+ "{?perspectiveURI ?perspectiveInsight ?insightURI.}"
			+ "}";
	
	private static final String orderedInsightsURI = "SELECT DISTINCT ?insightURI WHERE {"
			+ "BIND(<@perspective@> AS ?perspectiveURI)"
			+ "{?perspectiveInsight <"+Constants.SUBPROPERTY_URI +"> <"+perspectiveInsightBaseURI+"> }"
			+ "{?perspectiveURI ?perspectiveInsight ?insightURI.} "
			+ "{?insightURI <" + orderBaseURI + "> ?order } BIND(xsd:decimal(?order) AS ?orderNum)"
			+ "} ORDER BY ?orderNum";

	private static final String fromSparql = "SELECT DISTINCT ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel  ?y} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
					+ "}";

	private static final String toSparql = "SELECT DISTINCT ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel ?y} "
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
					+ "}";
	
	private static final String typeSparql = "SELECT ?insight WHERE {"
			+ "{?param <" + "PARAM:TYPE" + "> <@type@>}"
			+ "{?insightUri <" + "INSIGHT:PARAM" + "> ?param;}"
			+ "{?insightUri <" + labelBaseURI + "> ?insight.}" + "}";

	private static final String insight4TagSparql = "SELECT ?insight WHERE {"
			+ "{?insightURI <" + tagBaseURI + "> ?tag;}"
			+ "{?insightURI <" + labelBaseURI + "> ?insight.}"
			+ "FILTER (regex (?tag, \"@tag@\" ,\"i\"))"
			+ "}";

	private static final String tag4InsightSparql = "SELECT ?insight WHERE {"
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

	private String questionXMLFile;
	

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
				
				//loads the questionxmlfile if there is one, if not, get the question sheet and create an xml file and load to engine
				questionXMLFile = prop.getProperty(Constants.INSIGHTS);

				engineURI2 = engineBaseURI + "/" + engineName;
				if(questionXMLFile != null) {
					createBaseRelationXMLEngine(questionXMLFile);
				}
				//TODO: delete once xml files are stable and prop file is no longer needed
				else {
					createInsightBase();
					//need to add questionXML to the smss file
					String questionPropFile = prop.getProperty(Constants.DREAMER);
					questionXMLFile = "db" + System.getProperty("file.separator") +  getEngineName() + System.getProperty("file.separator") + getEngineName()	+ "_Questions.XML";
					//addProperty(Constants.XML, questionXMLFile);
					//saveConfiguration();
					
					if(questionPropFile != null){
						dreamerProp = loadProp(baseFolder + "/" + questionPropFile);
						loadAllPerspectives(engineURI2);
						createInsightBaseRelations();
						createQuestionXMLFile(questionXMLFile, baseFolder);
						addPropToFile(propFile, Constants.INSIGHTS, questionXMLFile, "ENGINE_TYPE");
						if(!prop.contains(Constants.INSIGHTS))
							prop.put(Constants.INSIGHTS, questionXMLFile);
					}
				}
				
				//TODO: delete once all XML files have a timeStamp
				checkAndAddTimeStampToXML();
				
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

				// need to add the logic for entityQuery
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	//TODO: delete once all insight XML files have a timestamp
	private void checkAndAddTimeStampToXML() {
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String checkTimeStamp = "SELECT DISTINCT ?Time WHERE { {<http://semoss.org/ontologies/Concept/Engine/" + engineName + "> <http://semoss.org/ontologies/Relation/Contains/TimeStamp> ?Time}}";
		ISelectWrapper sjsw = Utility.processQuery(insightBaseXML, checkTimeStamp);
		String[] names = sjsw.getVariables();
		boolean hasTime = false;
		List<Date> dateList = new ArrayList<Date>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			Date d;
			try {
				d = df.parse(sjss.getVar(names[0]).toString());
				dateList.add(d);
				hasTime = true;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		//to fix error when multiple dates were added
		if(dateList.size() > 1) {
			// remove all old dates
			for(Date d : dateList) {
				insightBaseXML.removeStatement(new Object[]{engineBaseURI + "/" + engineName, TIME_STAMP_URI, d, false});
			}
			// add new date
			Date currentTime = Utility.getCurrentTime();
			insightBaseXML.addStatement(new Object[]{engineBaseURI + "/" + engineName, TIME_STAMP_URI, currentTime, false});
		} else if(!hasTime) {
			Date currentTime = Utility.getCurrentTime();
			insightBaseXML.addStatement(new Object[]{engineBaseURI + "/" + engineName, TIME_STAMP_URI, currentTime, false});
		}
		createQuestionXMLFile(questionXMLFile, workingDir);
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
			insightBaseXML.getRc().export(questionXMLWriter);
			
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
		insightBaseXML = new RDFFileSesameEngine();
		insightBaseXML.setFileName(baseFolder + "/" + questionXMLFile);
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
		insightBaseXML = new RDFFileSesameEngine();
		insightBaseXML.openDB(null);
	}
	
	private void createInsightBaseRelations() {
		String typePred = RDF.TYPE.stringValue();
		String classURI = Constants.CLASS_URI;
		String propURI = Constants.DEFAULT_PROPERTY_URI;
		String subclassPredicate = Constants.SUBCLASS_URI;
		String subpropertyPredicate = Constants.SUBPROPERTY_URI;
		final String semossConceptURI = semossURI + Constants.DEFAULT_NODE_CLASS;
		final String semossRelationURI = semossURI + Constants.DEFAULT_RELATION_CLASS;
		
		insightBaseXML.addStatement(new Object[]{semossConceptURI, typePred, classURI, true});
		insightBaseXML.addStatement(new Object[]{semossRelationURI, typePred, propURI, true});
		insightBaseXML.addStatement(new Object[]{engineBaseURI, subclassPredicate, semossConceptURI, true});
		insightBaseXML.addStatement(new Object[]{perspectiveBaseURI, subclassPredicate, semossConceptURI, true});
		insightBaseXML.addStatement(new Object[]{insightBaseURI, subclassPredicate, semossConceptURI, true});
		insightBaseXML.addStatement(new Object[]{paramBaseURI, subclassPredicate, semossConceptURI, true});
		insightBaseXML.addStatement(new Object[]{enginePerspectiveBaseURI, subpropertyPredicate, semossRelationURI, true});
		insightBaseXML.addStatement(new Object[]{perspectiveInsightBaseURI, subpropertyPredicate, semossRelationURI, true});
		insightBaseXML.addStatement(new Object[]{engineInsightBaseURI, subpropertyPredicate, semossRelationURI, true});
		insightBaseXML.addStatement(new Object[]{containsBaseURI, subpropertyPredicate, semossRelationURI, true});
		insightBaseXML.addStatement(new Object[]{orderBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true});
		insightBaseXML.addStatement(new Object[]{labelBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true});
		insightBaseXML.addStatement(new Object[]{layoutBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true});
		insightBaseXML.addStatement(new Object[]{sparqlBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true});
		//insightBase.add(insightVF.createURI(typeBaseURI), RDF.TYPE, insightVF.createURI(containsBaseURI));
		insightBaseXML.addStatement(new Object[]{tagBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true});
		insightBaseXML.addStatement(new Object[]{descriptionBaseURI, RDF.TYPE.stringValue(), containsBaseURI, true});
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
		QuestionAdministrator questionAdmin = new QuestionAdministrator(this, false);
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
					questionAdmin.cleanAddQuestion(perspective, qsKey, qsOrder, qsDescr, sparql, layoutName, description, parameterDependList, parameterQueryList, parameterOptionList);
					count++;
				}
				logger.info("Loaded Perspective " + key);
			}				
		}
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
	public void addProperty(String name, String value) {
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
		return Utility.getVectorOfReturn(query, insightBaseXML);
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
		return Utility.getVectorOfReturn(query, insightBaseXML);
	}
	public Vector<String> getInsights(String perspective) {

		return getInsights(perspective, engineURI2 + "");
	}

	public Vector<String> getInsights(String perspective, String engine) {

		if (perspective != null) {
			Hashtable paramHash = new Hashtable();
			paramHash.put("perspective", perspective);
			String query = Utility.fillParam(insights, paramHash);
			System.err.println("Query " + query);
			return Utility.getVectorOfReturn(query, insightBaseXML);
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
			return Utility.getVectorOfReturn(query, insightBaseXML);
		}
		return null;
	}

	public Vector<String> getInsightsURI(String perspective, String engine) {

		if (perspective != null) {
			Hashtable paramHash = new Hashtable();
			paramHash.put("perspective", perspective);
			String query = Utility.fillParam(insightsURI, paramHash);
			System.err.println("Query " + query);
			return Utility.getVectorOfReturn(query, insightBaseXML);
		}
		return null;
	}

	public Vector<String> getInsights() {
		//URI perspectivePred = insightVF.createURI(Constants.PERSPECTIVE + ":"
		//		+ Constants.PERSPECTIVE);
		String qPred = Constants.PERSPECTIVE + ":" + Constants.ID;
		//URI insightPred = labelBaseURI;

		String insights = "SELECT ?insight WHERE {" + "{<" + engineURI2 + "><"
				+ qPred + "> ?insightURI.}" + "{?insightURI <" + labelBaseURI
				+ "> ?insight.}" + "}";

		System.err.println("Query " + insights);

		return Utility.getVectorOfReturn(insights, insightBaseXML);
	}

	public Vector<String> getInsight4Type(String type) {

		Vector<String> retString = new Vector<String>();
		Hashtable paramHash = new Hashtable();
		paramHash.put("type", type);
		// replacing with SPARQL
		return Utility.getVectorOfReturn(Utility.fillParam(typeSparql, paramHash), insightBaseXML);
	}

	public Vector<String> getInsight4Tag(String tag) {

		Vector<String> retString = new Vector<String>();
		Hashtable paramHash = new Hashtable();
		paramHash.put("tag", tag);
		return Utility.getVectorOfReturn(Utility.fillParam(insight4TagSparql, paramHash),
				insightBaseXML);
	}

	public Vector<String> getTag4Insight(String insight) {

		Vector<String> retString = new Vector<String>();
		// replacing with SPARQL
		Hashtable paramHash = new Hashtable();
		paramHash.put("insight", insight);
		return Utility.getVectorOfReturn(Utility.fillParam(tag4InsightSparql, paramHash),
				insightBaseXML);
	}
	
	private Vector<SEMOSSParam> addSEMOSSParams(Vector<SEMOSSParam> retParam,String paramSparql) {
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightBaseXML, paramSparql);
		wrap.execute();
		// want only unique params. if a question name is duplicated, its possible to have multiple returned here
		// really should switch this be insight ID based
		Vector<String> usedLabels = new Vector<String>();
			while(wrap.hasNext())
			{
				ISelectStatement bs = wrap.next();
				String label = bs.getRawVar("paramLabel") + "";
				if(!usedLabels.contains(label)){
					usedLabels.add(label);
					SEMOSSParam param = new SEMOSSParam();
					param.setName(label);
					
					if(bs.getRawVar("paramType") != null)
						param.setType(bs.getRawVar("paramType") +"");
					if(bs.getRawVar("option") != null)
						param.setOptions(bs.getRawVar("option") + "");
					if(bs.getRawVar("query") != null)
						param.setQuery(bs.getRawVar("query") + "");
					if(bs.getRawVar("depend") != null)
						param.setDepends(bs.getRawVar("depend") +"");
					if(bs.getRawVar("dependVar") != null)
						param.addDependVar(bs.getRawVar("dependVar") +"");
					if(bs.getRawVar("param") != null)
						param.setUri(bs.getRawVar("param") +"");
	
					retParam.addElement(param);
					System.out.println(param.getName() + param.getQuery() + param.isDepends() + param.getType());
				}
				
			}
			
		return retParam;
	}
	
	public Vector<SEMOSSParam> getParams(String label)
	{
		Vector <SEMOSSParam> retParam = new Vector<SEMOSSParam>();

		String paramPred = "INSIGHT:PARAM";
		String paramPredLabel = "PARAM:LABEL";
		String queryPred = "PARAM:QUERY";
		String hasDependPred = "PARAM:HAS:DEPEND";
		String dependPred = "PARAM:DEPEND";
		String typePred = "PARAM:TYPE";
		String optionPred = "PARAM:OPTION";

		String queryParamSparql = "SELECT DISTINCT ?paramLabel ?query ?option ?depend ?dependVar ?paramType ?param WHERE {"
			+ "{?insightURI <" + labelBaseURI + "> ?insight}"
			+ "{?insightURI <" + paramPred + "> ?param } "
			+ "{?param <" + paramPredLabel + "> ?paramLabel } "
			+ "{?param <" + typePred + "> ?paramType } "
			+ "OPTIONAL {?param <" + queryPred + "> ?query } "
			+ "OPTIONAL {?param <" + optionPred + "> ?option } "
			+ "{?param <" + hasDependPred + "> ?depend } " 
			+ "{?param <" + dependPred + "> ?dependVar } "
			+ "} BINDINGS ?insight {(\""+label+"\")}";
		
		retParam = addSEMOSSParams(retParam,queryParamSparql);
		
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

		System.err.println("Insighter... " + insightSparql + labels);
		System.err.println("Label is " + labels);
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightBaseXML, insightSparql);
		wrap.execute();

		while (wrap.hasNext()) {
			Insight in = new Insight();
			ISelectStatement bs = wrap.next();
			in.setId( Utility.getInstanceName(bs.getRawVar("insightURI") + ""));
			in.setURI( bs.getRawVar("insightURI") + "");
			
			Literal lit = (Literal)bs.getRawVar("sparql");
			String sparql = lit.getLabel();
			in.setSparql(sparql);
			
			if(bs.getRawVar("order") != null){
				String order = ((MemLiteral)bs.getRawVar("order")).stringValue();
				in.setOrder(order);
			}
			String label = ((LiteralImpl)bs.getRawVar("insight")).getLabel();
			in.setLabel(label);

			Literal outputlit = (Literal)bs.getRawVar("output");
			String output = outputlit.getLabel();
			in.setOutput(output);

			String engine = bs.getRawVar("engine") + "";
			in.setEngine(engine);
			
			if(bs.getRawVar("description") != null){
				Literal descriptionlit = (Literal)bs.getRawVar("description");
				String description = descriptionlit.getLabel();
				in.setDescription(description);
			}
			insightV.add(in);
			System.err.println(in.toString());
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
		
		String insightSparql = "SELECT ?insightURI ?order ?insight ?sparql ?output WHERE {"
				+"BIND(\"" + label + "\" AS ?insight)"
				+ "{?insightURI <" + labelBaseURI + "> ?insight.}"
				+ "{?insightURI <" + sparqlBaseURI + "> ?sparql.}"
				+ "{?insightURI <" + layoutBaseURI + "> ?output.}"
				+ "OPTIONAL {?insightURI <" + orderBaseURI + "> ?order.}"
				+ "}";
		System.err.println("Insighter... " + insightSparql + label);
		System.err.println("Label is " + label);
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightBaseXML, insightSparql);
		wrap.execute();

		if (!wrap.hasNext())
			in = null;
		while (wrap.hasNext()) {
			ISelectStatement bs = wrap.next();
			in.setId(bs.getRawVar("insightURI") + "");
			String sparql = bs.getRawVar("sparql") + "";
			sparql = sparql.replace("\"", "");
			if(bs.getRawVar("order") != null){
				String order = ((MemLiteral)bs.getRawVar("order")).stringValue();
				in.setOrder(order);
			}
			String label2 = bs.getRawVar("insight") + "";
			logger.info("Getting insight... sparql for "+label2 + " is " + sparql);
			in.setSparql(sparql);
			String output = bs.getRawVar("output") + "";
			output = output.replace("\"", "");
			in.setOutput(output);
		}
		logger.info("final filled insight is  "+ in);
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
		baseRelEngine.setFileName(owl);
		baseRelEngine.openDB(null);
		if(prop != null) {
			addProperty(Constants.OWL, owl);
		}
		
		try {
			baseHash.putAll(RDFEngineHelper.createBaseFilterHash(baseRelEngine.getRc()));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		setBaseHash(baseHash);
		
		baseRelEngine.commit();
		setBaseData(baseRelEngine);
	}

	// gets the from neighborhood for a given node
	public Vector<String> getFromNeighbors(String nodeType, int neighborHood) {
		// this is where this node is the from node
		Hashtable paramHash = new Hashtable();
		paramHash.put("nodeType", nodeType);
		return Utility.getVectorOfReturn(Utility.fillParam(fromSparql, paramHash), baseDataEngine);
	}

	// gets the to nodes
	public Vector<String> getToNeighbors(String nodeType, int neighborHood) {
		// this is where this node is the to node
		Hashtable paramHash = new Hashtable();
		paramHash.put("nodeType", nodeType);
		return Utility.getVectorOfReturn(Utility.fillParam(toSparql, paramHash), baseDataEngine);
	}
	
	// gets the from and to nodes
	public Vector<String> getNeighbors(String nodeType, int neighborHood) {
		Vector<String> from = getFromNeighbors(nodeType, 0);
		Vector<String> to = getToNeighbors(nodeType, 0);
		from.addAll(to);
		return from;
	}

	// gets the OWL engine
	// this needs to change later
	// needed for cluster engine
	public RepositoryConnection getOWL()
	{
		return baseDataEngine.getRc();
	}
	
	public RepositoryConnection getInsightDB()
	{
		return this.insightBaseXML.getRc();
	}
	
	public String getInsightDefinition()
	{
		StringWriter output = new StringWriter();
		try {
			insightBaseXML.getRc().export(new RDFXMLWriter(output));
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
			baseDataEngine.getRc().export(new RDFXMLWriter(output));
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
	
	public IQueryBuilder getQueryBuilder(){
		return new SPARQLQueryTableBuilder();
	}
	
	/**
	 * Commits the base data engine
	 */
	public void commitOWL() {
		logger.info("Committing base data engine of " + this.engineName);
		this.baseDataEngine.commit();
	}

	/**
	 * Uses the uri of a parameter to get the values to show for that parameter
	 * 
	 */
	public Vector<String> getParamOptions(String parameterURI) {
		// query to get param definition based on uri.
		Vector<SEMOSSParam> paramRet = new Vector<SEMOSSParam>();
		String paramPredLabel = "PARAM:LABEL";
		String queryPred = "PARAM:QUERY";
		String hasDependPred = "PARAM:HAS:DEPEND";
		String dependPred = "PARAM:DEPEND";
		String typePred = "PARAM:TYPE";
		String optionPred = "PARAM:OPTION";

		String queryParamSparql = "SELECT DISTINCT ?paramLabel ?query ?option ?depend ?dependVar ?paramType ?param WHERE {"
			+"BIND(<" + parameterURI + "> AS ?param)"
			+ "{?param <" + paramPredLabel + "> ?paramLabel } "
			+ "{?param <" + typePred + "> ?paramType } "
			+ "OPTIONAL {?param <" + queryPred + "> ?query } "
			+ "OPTIONAL {?param <" + optionPred + "> ?option } "
			+ "{?param <" + hasDependPred + "> ?depend } " 
			+ "{?param <" + dependPred + "> ?dependVar } "
			+ "}";
		
		paramRet = addSEMOSSParams(paramRet,queryParamSparql);
		
		Vector<String> uris = new Vector<String>();
		if(!paramRet.isEmpty()){
			SEMOSSParam ourParam = paramRet.get(0); // there should only be one as we are getting the param from a specific param URI
			//if the param has options defined, we are all set
			//grab the options and we are good to go
			Vector<String> options = ourParam.getOptions();
			if (options != null && !options.isEmpty()) {
				uris.addAll(options);
			}
			
			// if options are not defined, need to get uris either from custom sparql or type
			// need to use custom query if it has been specified in the xml
			// otherwise use generic fill query
			String paramQuery = ourParam.getQuery();
			String type = ourParam.getType();
			// RDBMS right now does type:type... need to get just the second type. This will be fixed once insights don't store generic query
			// TODO: fix this logic. need to decide how to store param type for rdbms
			if(this.getEngineType().equals(IEngine.ENGINE_TYPE.RDBMS)){
				if (type.contains(":")) {
					String[] typeArray = type.split(":");
					String table = typeArray[0];
					type = typeArray[1];
					if(paramQuery != null)
						paramQuery = paramQuery.substring(0, paramQuery.lastIndexOf("@entity@")) + table;
				}
			}
			if(paramQuery != null){
				Hashtable<String, Object> paramTable = new Hashtable<String, Object>();
				paramTable.put(Constants.ENTITY, type);
				paramQuery = Utility.fillParam(paramQuery, paramTable);
				uris = this.getCleanSelect(paramQuery);
			}else { 
				uris = this.getEntityOfType(type);
			}
		}
		return uris;
	}
	
	protected abstract Vector<String> getCleanSelect(String query);

	public Vector<String> getConcepts() {
		String query = "SELECT ?concept WHERE {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }";
		return Utility.getVectorOfReturn(query, baseDataEngine);
	}

	public Vector<String> getProperties4Concept(String concept) {
		String query = "SELECT ?property WHERE { {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?concept  <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property}"
				+ "{?property a <" + containsBaseURI + ">}}"
						+ "BINDINGS ?concept {(<"+concept+">)}";
		return Utility.getVectorOfReturn(query, baseDataEngine);
	}

	/**
	 * Runs a select query on the base data engine of this engine
	 */
	public Object execOntoSelectQuery(String query) {
		logger.info("Running select query on base data engine of " + this.engineName);
		logger.info("Query is " + query);
		return this.baseDataEngine.execQuery(query);
	}

	/**
	 * Runs insert query on base data engine of this engine
	 */
	public void ontoInsertData(String query) {
		logger.info("Running insert query on base data engine of " + this.engineName);
		logger.info("Query is " + query);
		baseDataEngine.insertData(query);
	}

	/**
	 * This method runs an update query on the base data engine which contains all owl and metamodel information
	 */
	public void ontoRemoveData(String query) {
		logger.info("Running update query on base data engine of " + this.engineName);
		logger.info("Query is " + query);
		baseDataEngine.removeData(query);
	}
	
	public String getMethodName(IEngine.ACTION_TYPE actionType){
		String retString = "";
		switch(actionType)
			{
			case ADD_STATEMENT: {
				retString = "addStatement";
				break;
			}
			case REMOVE_STATEMENT: {
				retString = "removeStatement";
				break;
			}
			default: {
				
			}
		}
		return retString;
	}

	
	public Object doAction(IEngine.ACTION_TYPE actionType, Object[] args){

		// Iterate through methods on the engine -- do this on startup
		// Find the method on the engine that matches the action type passed in
		// pass the arguments and let it run
		// 
		// if the method does not exist on the engine
		// look at the smss for the method (?)
		String methodName = this.getMethodName(actionType);
//		logger.info("Trying to run method: " + methodName + " on database: " + this.getEngineName() + " with arguments: " + args.toString());
		
		Object[] params = {args};
    	java.lang.reflect.Method method = null;
    	Object ret = null;
    	try {
    		method = this.getClass().getMethod(methodName, args.getClass());
  			ret = method.invoke(this, params);
    	} catch (SecurityException e) {
    		logger.error(e);
    	} catch (NoSuchMethodException e) {
    		logger.error(e);
    	} catch (IllegalArgumentException e) {
    		logger.error(e);
    	} catch (IllegalAccessException e) {
    		logger.error(e);
    	} catch (InvocationTargetException e) {
    		logger.error(e);
    	}
    	return ret;
		
//		IDBAction action = null;
//		switch(actionType)
//			{
//			case ADD_STATEMENT: {
//				action = new AddStatementWrapper();
//				break;
//			}
//			default: {
//				
//			}
//		}
//		
//		Object returnObj = action.performAction(args, this);
//		
//		return returnObj;
	}

	public void deleteDB() {
		System.out.println("closing " + this.engineName);
		this.closeDB();
		System.out.println("db closed");
		System.out.println("deleting folder");
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String insightLoc = baseFolder + "/" + this.getProperty(Constants.INSIGHTS);
		System.out.println("insight file is  " + insightLoc);
		File insightFile = new File(insightLoc);
		File engineFolder = new File(insightFile.getParent());
		String folderName = engineFolder.getName();
		try {
			System.out.println("checking folder " + folderName + " against db " + this.engineName);//this check is to ensure we are deleting the right folder
			if(folderName.equals(this.engineName))
			{
				System.out.println("folder getting deleted is " + engineFolder.getAbsolutePath());
				FileUtils.deleteDirectory(engineFolder);
			}
			else{
				logger.error("Cannot delete database folder as folder name does not line up with engine name");
				//try deleting each file individually
				System.out.println("Deleting insight file " + insightLoc);
				insightFile.delete();
	
				String ontoLoc = baseFolder + "/" + this.getProperty(Constants.ONTOLOGY);
				if(ontoLoc != null){
					System.out.println("Deleting onto file " + ontoLoc);
					File ontoFile = new File(ontoLoc);
					ontoFile.delete();
				}
	
				String owlLoc = baseFolder + "/" + this.getProperty(Constants.OWL);
				if(owlLoc != null){
					System.out.println("Deleting owl file " + owlLoc);
					File owlFile = new File(owlLoc);
					owlFile.delete();
				}
			}
			System.out.println("Deleting smss " + this.propFile);
			File smssFile = new File(this.propFile);
			smssFile.delete();
	
			//NEED TO REMOVE FROM SESSION IF FROM WEB
			
			//remove from dihelper...
			String engineNames = (String)DIHelper.getInstance().getLocalProp(Constants.ENGINES);
			engineNames = engineNames.replace(";" + engineName, "");
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String [] args) throws Exception
	{
		DIHelper.getInstance().loadCoreProp("C:\\Users\\bisutton\\workspace\\SEMOSSDev\\RDF_Map.prop");
		FileInputStream fileIn = null;
		try{
			Properties prop = new Properties();
			String fileName = "C:\\Users\\bisutton\\workspace\\SEMOSSDev\\db\\Movie_Test.smss";
			fileIn = new FileInputStream(fileName);
			prop.load(fileIn);
			System.err.println("Loading DB " + fileName);
			Utility.loadEngine(fileName, prop);
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			try{
				if(fileIn!=null)
					fileIn.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		IEngine eng = (IEngine) DIHelper.getInstance().getLocalProp("Movie_Test");
		Vector<String> props = eng.getProperties4Concept("http://semoss.org/ontologies/Concept/Title");
		while(!props.isEmpty()){
			System.out.println(props.remove(0));
		}
	}
}
