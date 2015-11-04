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
package prerna.engine.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.jdbc.JdbcClob;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryBuilder;
import prerna.rdf.query.builder.SPARQLQueryTableBuilder;
import prerna.rdf.util.AbstractQueryParser;
import prerna.rdf.util.SPARQLQueryParser;
import prerna.ui.components.RDFEngineHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

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
	protected RDBMSNativeEngine insightRDBMS;
	protected String insightDriver = "org.h2.Driver";
	protected String insightRDBMSType = "H2_DB";
	protected String connectionURLStart = "jdbc:h2:";
	protected String connectionURLEnd = ";query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
	protected String insightUsername = "sa";

	private String dreamer;
	private String ontology;
	private String owl;
	private String insightDatabaseLoc;

	private Hashtable<String, String> baseDataHash;

	private static final String SEMOSS_URI = "http://semoss.org/ontologies/";

	private static final String CONTAINS_BASE_URI = SEMOSS_URI + Constants.DEFAULT_RELATION_CLASS + "/Contains";
	private static final String GET_ALL_INSIGHTS_QUERY = "SELECT DISTINCT QUESTION_ID, QUESTION_ORDER FROM QUESTION_ID ORDER BY QUESTION_ID";
	private static final String GET_ALL_INSIGHTS_WITH_METADATA_QUERY = "SELECT DISTINCT QUESTION_ID, QUESTION_NAME, QUESTION_PERSPECTIVE, QUESTION_LAYOUT, ID FROM QUESTION_ID ORDER BY ID";

	private static final String GET_ALL_PERSPECTIVES_QUERY = "SELECT DISTINCT QUESTION_PERSPECTIVE FROM QUESTION_ID ORDER BY QUESTION_PERSPECTIVE";

	private static final String QUESTION_PARAM_KEY = "@QUESTION_VALUE@";
	private static final String GET_INSIGHT_INFO_QUERY = "SELECT DISTINCT QUESTION_ID, QUESTION_NAME, QUESTION_MAKEUP, QUESTION_PERSPECTIVE, QUESTION_LAYOUT, QUESTION_ORDER, DATA_TABLE_ALIGN, QUESTION_DATA_MAKER FROM QUESTION_ID WHERE QUESTION_ID IN (" + QUESTION_PARAM_KEY + ") ORDER BY QUESTION_ORDER";

	private static final String PERSPECTIVE_PARAM_KEY = "@PERSPECTIVE_VALUE@";
	private static final String GET_ALL_INSIGHTS_IN_PERSPECTIVE = "SELECT DISTINCT QUESTION_ID, QUESTION_ORDER FROM QUESTION_ID WHERE QUESTION_PERSPECTIVE = '" + PERSPECTIVE_PARAM_KEY + "' ORDER BY QUESTION_ORDER";

	private static final String QUESTION_ID_FK_PARAM_KEY = "@QUESTION_ID_FK_VALUES@";
	private static final String GET_ALL_PARAMS_FOR_QUESTION_ID = "SELECT DISTINCT PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_OPTIONS, PARAMETER_QUERY, PARAMETER_DEPENDENCY, PARAMETER_IS_DB_QUERY, PARAMETER_MULTI_SELECT, PARAMETER_COMPONENT_FILTER_ID, PARAMETER_ID FROM PARAMETER_ID WHERE QUESTION_ID_FK = '" + QUESTION_ID_FK_PARAM_KEY + "'";

	private static final String PARAMETER_ID_PARAM_KEY = "@PARAMETER_ID";
	private static final String GET_INFO_FOR_PARAM = "SELECT DISTINCT PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_OPTIONS, PARAMETER_QUERY, PARAMETER_DEPENDENCY, PARAMETER_IS_DB_QUERY, PARAMETER_MULTI_SELECT, PARAMETER_COMPONENT_FILTER_ID FROM PARAMETER_ID WHERE PARAMETER_ID = '" + PARAMETER_ID_PARAM_KEY + "'";
	private static final String GET_INFO_FOR_PARAMS = "SELECT DISTINCT PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_OPTIONS, PARAMETER_QUERY, PARAMETER_DEPENDENCY, PARAMETER_IS_DB_QUERY, PARAMETER_MULTI_SELECT, PARAMETER_COMPONENT_FILTER_ID FROM PARAMETER_ID WHERE PARAMETER_ID IN (" + PARAMETER_ID_PARAM_KEY + ")";
	
	private static final String FROM_SPARQL = "SELECT DISTINCT ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel  ?y} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
			+ "}";

	private static final String TO_SPARQL = "SELECT DISTINCT ?entity WHERE { "
			+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} "
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
			+ "{?x ?rel ?y} "
			+ "{<@nodeType@> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?x}"
			+ "{?entity <http://www.w3.org/2000/01/rdf-schema#subClassOf>* ?y}"
			+ "}";

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
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			if(propFile != null) {
				this.propFile = propFile;
				logger.info("Opening DB - " + engineName);
				prop = loadProp(propFile);
			}
			if(prop != null) {
				// load the rdbms insights db
				insightDatabaseLoc = prop.getProperty(Constants.RDBMS_INSIGHTS);
				if(insightDatabaseLoc != null) {
					logger.info("Loading insight rdbms database...");
					insightRDBMS = new RDBMSNativeEngine();
					Properties prop = new Properties();
					prop.put(Constants.DRIVER, insightDriver);
					prop.put(Constants.RDBMS_TYPE, insightRDBMSType);
					String connURL = connectionURLStart + baseFolder + "/" + insightDatabaseLoc + connectionURLEnd;
					prop.put(Constants.CONNECTION_URL, connURL);
					prop.put(Constants.USERNAME, insightUsername);
					insightRDBMS.setProperties(prop);
					insightRDBMS.openDB(null);
				} 
				else { // RDBMS Question engine has not been made. Must do conversion
					// set the necessary fun stuff
					InsightsConverter converter = new InsightsConverter();
					this.insightRDBMS = converter.generateNewInsightsRDBMS(this.engineName);
					converter.setEngine(this);
					converter.setEngineName(prop.getProperty(Constants.ENGINE));
					converter.setSMSSLocation(propFile);
					
					// Use the xml if the prop file has that defined
					if (prop.containsKey(Constants.INSIGHTS)){
						logger.info("LOADING XML QUESTIONS FOR DB ::: " + this.getEngineName());
						String xmlFilePath = prop.getProperty(Constants.INSIGHTS);
						converter.loadQuestionsFromXML(baseFolder + "\\" + xmlFilePath); // this does questions and parameters now
					}
					// else we will use the prop file
					else if (prop.containsKey(Constants.DREAMER)){
						String dreamerLoc = baseFolder + "/" + prop.getProperty(Constants.DREAMER);
						logger.info("LOADING PROP FILE QUESTIONS FOR DB ::: " + this.getEngineName());
						logger.info("question prop file loc is " + dreamerLoc);
						Properties dreamerProps = loadProp(dreamerLoc);
						converter.loadQuestionsFromPropFile(dreamerProps);
					}
					else {
						logger.fatal("NO QUESTION SHEET DEFINED ON SMSS");
						logger.fatal("cannot start " + this.getEngineName() + " without question file");
					}
					//update smss location
					converter.updateSMSSFile();
				}
				// load the rdf owl db
				String owlFile = prop.getProperty(Constants.OWL);
				if (owlFile != null) {
					logger.info("Loading OWL: " + owlFile);
					setOWL(baseFolder + "/" + owlFile);
				}
				String ontoFile = prop.getProperty(Constants.ONTOLOGY);
				if (ontoFile != null) {
					logger.info("Loading Ontology: " + ontoFile);
					setOntology(baseFolder + "/" + ontoFile);
				}
				// load properties object for db
				String genEngPropFile = prop.getProperty(Constants.ENGINE_PROPERTIES);
				if (genEngPropFile != null) {
					generalEngineProp = loadProp(baseFolder + "/" + genEngPropFile);
				}
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void closeDB() {
		if(this.baseDataEngine != null) {
			logger.debug("closing its owl engine ");
			this.baseDataEngine.closeDB();
		}
		if(this.insightRDBMS != null) {
			logger.debug("closing its insight engine ");
			this.insightRDBMS.closeDB();
		}
	}
	
	@Override
	public String getProperty(String key) {
		String retProp = null;

		logger.debug("Property is " + key + "]");
		if (generalEngineProp != null && generalEngineProp.containsKey(key))
			retProp = generalEngineProp.getProperty(key);
		if (retProp == null && ontoProp != null && ontoProp.containsKey(key))
			retProp = ontoProp.getProperty(key);
		if (retProp == null && prop != null && prop.containsKey(key))
			retProp = prop.getProperty(key);
		return retProp;
	}

	/**
	 * Method loadProp. Loads the database properties from a specifed properties
	 * file.
	 * @param fileName			String of the name of the properties file to be loaded.
	 * @return Properties		The properties imported from the prop file.
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public Properties loadProp(String fileName) throws FileNotFoundException, IOException {
		Properties retProp = new Properties();
		if(fileName != null) {
			FileInputStream fis = new FileInputStream(fileName);
			retProp.load(fis);
			fis.close();
		}
		logger.debug("Properties >>>>>>>>" + fileName);
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
	 * @param engineName	name of the engine that this is being set to
	 */
	@Override
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	/**
	 * Gets the engine name for this engine
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
			logger.debug("Writing to file " + propFile);
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
	 * @param name		String - The name of the property.
	 * @param value		String - The value of the property.
	 */
	public void addProperty(String name, String value) {
		prop.put(name, value);
	}

	/**
	 * Sets the base data engine.
	 * 
	 * @param eng 	The base data engine that this is being set to
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
	 * @param h		Hashtable - The base data hash that this is being set to
	 */
	public void setBaseHash(Hashtable h) {
		logger.debug(this.engineName + " Set the Base Data Hash ");
		this.baseDataHash = h;
	}

	/**
	 * Gets the base data hash
	 * @return Hashtable - The base data hash.
	 */
	public Hashtable getBaseHash() {
		return this.baseDataHash;
	}

	// sets the dreamer
	public void setDreamer(String dreamer) {
		this.dreamer = dreamer;
	}

	// sets the dreamer
	public void setOntology(String ontology) {
		logger.debug("Ontology file is " + ontology);
		this.ontology = ontology;

		if (ontoProp == null) {
			ontoProp = new Properties();
			try {
				ontoProp = loadProp(ontology);
				createBaseRelationEngine();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void setOWL(String owl) {
		this.owl = owl;
	}

	public void setProperties(Properties prop) {
		this.prop = prop;
	}
	
	/**
	 * Checks for an OWL and adds it to the engine. Sets the base data hash from
	 * the engine properties, commits the database, and creates the base
	 * relation engine.
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
		Map<String, List<Object>> paramHash = new Hashtable<String, List<Object>>();
		List<Object> nodeArr = new Vector<Object>();
		nodeArr.add(nodeType);
		paramHash.put("nodeType", nodeArr);
		return Utility.getVectorOfReturn(Utility.fillParam(FROM_SPARQL, paramHash), baseDataEngine, true);
	}

	// gets the to nodes
	public Vector<String> getToNeighbors(String nodeType, int neighborHood) {
		// this is where this node is the to node
		Map<String, List<Object>> paramHash = new Hashtable<String, List<Object>>();
		List<Object> nodeArr = new Vector<Object>();
		nodeArr.add(nodeType);
		paramHash.put("nodeType", nodeArr);
		return Utility.getVectorOfReturn(Utility.fillParam(TO_SPARQL, paramHash), baseDataEngine, true);
	}

	// gets the from and to nodes
	public Vector<String> getNeighbors(String nodeType, int neighborHood) {
		Vector<String> from = getFromNeighbors(nodeType, 0);
		Vector<String> to = getToNeighbors(nodeType, 0);
		from.addAll(to);
		return from;
	}

	public String getOWL() {
		return this.owl;
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

	public IQueryBuilder getQueryBuilder(){
		return new SPARQLQueryTableBuilder();
	}

	/**
	 * Commits the base data engine
	 */
	public void commitOWL() {
		logger.debug("Committing base data engine of " + this.engineName);
		this.baseDataEngine.commit();
	}

	protected abstract Vector<Object> getCleanSelect(String query);

	public Vector<String> getConcepts() {
		String query = "SELECT ?concept WHERE {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	public Vector<String> getProperties4Concept(String concept) {
		String query = "SELECT ?property WHERE { {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?concept  <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property}"
				+ "{?property a <" + CONTAINS_BASE_URI + ">}}"
				+ "BINDINGS ?concept {(<"+concept+">)}";
		return Utility.getVectorOfReturn(query, baseDataEngine, true);
	}

	/**
	 * Runs a select query on the base data engine of this engine
	 */
	public Object execOntoSelectQuery(String query) {
		logger.debug("Running select query on base data engine of " + this.engineName);
		logger.debug("Query is " + query);
		return this.baseDataEngine.execQuery(query);
	}

	/**
	 * Runs insert query on base data engine of this engine
	 */
	public void ontoInsertData(String query) {
		logger.debug("Running insert query on base data engine of " + this.engineName);
		logger.debug("Query is " + query);
		baseDataEngine.insertData(query);
	}

	/**
	 * This method runs an update query on the base data engine which contains all owl and metamodel information
	 */
	public void ontoRemoveData(String query) {
		logger.debug("Running update query on base data engine of " + this.engineName);
		logger.debug("Query is " + query);
		baseDataEngine.removeData(query);
	}

	public String getMethodName(IEngine.ACTION_TYPE actionType){
		String retString = "";
		switch(actionType) {
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

		// if the method does not exist on the engine
		// look at the smss for the method (?)
		String methodName = this.getMethodName(actionType);

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
	}

	public void deleteDB() {
		logger.debug("closing " + this.engineName);
		this.closeDB();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String insightLoc = baseFolder + "/" + this.getProperty(Constants.RDBMS_INSIGHTS);
		File insightFile = new File(insightLoc);
		File engineFolder = new File(insightFile.getParent());
		String folderName = engineFolder.getName();
		try {
			logger.debug("checking folder " + folderName + " against db " + this.engineName);//this check is to ensure we are deleting the right folder.
			if(folderName.equals(this.engineName))
			{
				logger.debug("folder getting deleted is " + engineFolder.getAbsolutePath());
				FileUtils.deleteDirectory(engineFolder);
			}
			else{
				logger.error("Cannot delete database folder as folder name does not line up with engine name");
				//try deleting each file individually
				logger.debug("Deleting insight file " + insightLoc);
				insightFile.delete();

				String ontoLoc = baseFolder + "/" + this.getProperty(Constants.ONTOLOGY);
				if(ontoLoc != null){
					logger.debug("Deleting onto file " + ontoLoc);
					File ontoFile = new File(ontoLoc);
					ontoFile.delete();
				}

				String owlLoc = baseFolder + "/" + this.getProperty(Constants.OWL);
				if(owlLoc != null){
					logger.debug("Deleting owl file " + owlLoc);
					File owlFile = new File(owlLoc);
					owlFile.delete();
				}
			}
			logger.debug("Deleting smss " + this.propFile);
			File smssFile = new File(this.propFile);
			smssFile.delete();

			//remove from DIHelper
			String engineNames = (String)DIHelper.getInstance().getLocalProp(Constants.ENGINES);
			engineNames = engineNames.replace(";" + engineName, "");
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public AbstractQueryParser getQueryParser() {
		return new SPARQLQueryParser();
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

	@Override
	public IEngine getInsightDatabase() {
		return this.insightRDBMS;
	}

	@Override
	public Vector<String> getPerspectives() {
		return Utility.getVectorOfReturn(GET_ALL_PERSPECTIVES_QUERY, insightRDBMS, false);
	}

	@Override
	public Vector<String> getInsights(String perspective) {
		String insightsInPerspective = GET_ALL_INSIGHTS_IN_PERSPECTIVE.replace(PERSPECTIVE_PARAM_KEY, perspective);
		return Utility.getVectorOfReturn(insightsInPerspective, insightRDBMS, false);
	}

	@Override
	public Vector<String> getInsights() {
		return Utility.getVectorOfReturn(GET_ALL_INSIGHTS_QUERY, insightRDBMS, false);
	}
	
	@Override
	public List<Map<String, Object>> getAllInsightsMetaData() {
		List<Map<String, Object>> retList = new Vector<Map<String, Object>>();
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightRDBMS, GET_ALL_INSIGHTS_WITH_METADATA_QUERY);
		String[] names = wrap.getVariables();
		while(wrap.hasNext()) {
			ISelectStatement ss = wrap.next();
			String insight = ss.getVar(names[0]) + "";
			String insightLabel = ss.getVar(names[1]) + "";
			String perspective = ss.getVar(names[2]) + "";
			String layout = ss.getVar(names[3]) + "";

			// TODO: need to clean this up
			// seems unnecessary to pass this for each insight since we know its from this engine
			HashMap<String, String> engineMetaData = new HashMap<String, String>();
			engineMetaData.put("name", this.engineName);
			engineMetaData.put("type", this.getEngineType().toString());
			
			Map<String, Object> insightMetadata = new Hashtable<String, Object>();
			insightMetadata.put("insight", insight);
			insightMetadata.put("label", insightLabel);
			insightMetadata.put("engine", engineMetaData);
			insightMetadata.put("perspective", perspective);
			insightMetadata.put("layout", layout);
			insightMetadata.put("visibility", "me");
			insightMetadata.put("count", 0);
			
			retList.add(insightMetadata);
		}
		return retList;
	}

	@Override
	public Vector<SEMOSSParam> getParams(String... paramIds) {
		String pIdString = "";
		int numIDs = paramIds.length;
		for(int i = 0; i < numIDs; i++) {
			String id = paramIds[i];
			pIdString = pIdString + "'" + id + "'";
			if(i != numIDs - 1) {
				pIdString = pIdString + ", ";
			}
		}
		String query = GET_INFO_FOR_PARAMS.replace(PARAMETER_ID_PARAM_KEY, pIdString);
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightRDBMS, query);
		String[] names = wrap.getVariables();

		Vector<SEMOSSParam> retParams = new Vector<SEMOSSParam>();
		while(wrap.hasNext()) {
			ISelectStatement ss = wrap.next();
			String label = ss.getVar(names[0]) + "";
			SEMOSSParam param = new SEMOSSParam();
			param.setName(label);
			if(ss.getVar(names[1]) != null)
				param.setType(ss.getVar(names[1]) +"");
			if(ss.getVar(names[2]) != null)
				param.setOptions(ss.getVar(names[2]) + "");
			if(ss.getVar(names[3]) != null)
				param.setQuery(ss.getVar(names[3]) + "");
			if(ss.getRawVar(names[4]) != null)
				param.addDependVar(ss.getRawVar(names[4]) +"");
			if(ss.getVar(names[5]) != null && ss.getVar(names[5]).toString().isEmpty())
				param.setDbQuery((boolean) ss.getVar(names[5]));
			if(!ss.getVar(names[6]).toString().isEmpty())
				param.setMultiSelect((boolean) ss.getVar(names[6]));
			if(!ss.getVar(names[7]).toString().isEmpty())
				param.setComponentFilterId(ss.getVar(names[7]) + "");
			if(ss.getVar(names[0]) != null)
				param.setParamID(ss.getVar(names[0]) +"");
			
			retParams.addElement(param);
		}		
		
		return retParams;
	}

	
	@Override
	public Vector<SEMOSSParam> getParams(String questionID) {
		String query = GET_ALL_PARAMS_FOR_QUESTION_ID.replace(QUESTION_ID_FK_PARAM_KEY, questionID);
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightRDBMS, query);
		String[] names = wrap.getVariables();

		Vector<SEMOSSParam> retParam = new Vector<SEMOSSParam>();
		while(wrap.hasNext()) {
			ISelectStatement ss = wrap.next();
			String label = ss.getVar(names[0]) + "";
			SEMOSSParam param = new SEMOSSParam();
			param.setName(label);
			if(!ss.getVar(names[1]).toString().isEmpty())
				param.setType(ss.getVar(names[1]) +"");
			if(!ss.getVar(names[2]).toString().isEmpty())
				param.setOptions(ss.getVar(names[2]) + "");
			if(!ss.getVar(names[3]).toString().isEmpty())
				param.setQuery(ss.getVar(names[3]) + "");
			if(!ss.getVar(names[4]).toString().isEmpty()) {
				String[] vars = (ss.getVar(names[4]) +"").split(";");
				for(String var : vars){
					param.addDependVar(var);
				}
			}
			if(!ss.getVar(names[5]).toString().isEmpty()) {
				param.setDbQuery((boolean) ss.getVar(names[5]));
			}
			if(!ss.getVar(names[6]).toString().isEmpty()) {
				param.setMultiSelect((boolean) ss.getVar(names[6]));
			}
			if(!ss.getVar(names[7]).toString().isEmpty())
				param.setComponentFilterId(ss.getVar(names[7]) + "");
			if(!ss.getRawVar(names[8]).toString().isEmpty())
				param.setParamID(ss.getVar(names[8]) +"");
			retParam.addElement(param);
		}

		return retParam;
	}

	@Override
	public Vector<Object> getParamOptions(String parameterID) {
		String query = GET_INFO_FOR_PARAM.replace(PARAMETER_ID_PARAM_KEY, parameterID);
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightRDBMS, query);
		String[] names = wrap.getVariables();

		Vector<SEMOSSParam> retParam = new Vector<SEMOSSParam>();
		while(wrap.hasNext()) {
			ISelectStatement ss = wrap.next();
			String label = ss.getVar(names[0]) + "";
			SEMOSSParam param = new SEMOSSParam();
			param.setName(label);
			if(ss.getVar(names[1]) != null)
				param.setType(ss.getVar(names[1]) +"");
			if(ss.getVar(names[2]) != null)
				param.setOptions(ss.getVar(names[2]) + "");
			if(ss.getVar(names[3]) != null)
				param.setQuery(ss.getVar(names[3]) + "");
			if(ss.getRawVar(names[4]) != null)
				param.addDependVar(ss.getRawVar(names[4]) +"");
			if(ss.getVar(names[5]) != null && ss.getVar(names[5]).toString().isEmpty())
				param.setDbQuery((boolean) ss.getVar(names[5]));
			if(!ss.getVar(names[6]).toString().isEmpty())
				param.setMultiSelect((boolean) ss.getVar(names[6]));
			if(!ss.getVar(names[7]).toString().isEmpty())
				param.setComponentFilterId(ss.getVar(names[7]) + "");
			if(ss.getVar(names[0]) != null)
				param.setParamID(ss.getVar(names[0]) +"");
			retParam.addElement(param);
		}

		Vector<Object> uris = new Vector<Object>();
		if(!retParam.isEmpty()){
			SEMOSSParam ourParam = retParam.get(0); // there should only be one as we are getting the param from a specific param URI
			//if the param has options defined, we are all set
			//grab the options and we are good to go
			Vector<String> options = ourParam.getOptions();
			if (options != null && !options.isEmpty()) {
				uris.addAll(options);
			}
			else{
				// if options are not defined, need to get uris either from custom sparql or type
				// need to use custom query if it has been specified in the xml
				// otherwise use generic fill query
				String paramQuery = ourParam.getQuery();
				String type = ourParam.getType();
				boolean isDbQuery = ourParam.isDbQuery();
				// RDBMS right now does type:type... need to get just the second type. This will be fixed once insights don't store generic query
				// TODO: fix this logic. need to decide how to store param type for rdbms
				if(paramQuery != null && !paramQuery.isEmpty()) {
					//TODO: rdbms has type as null... this is confusing given the other comments here....
					if(type != null && !type.isEmpty()) {
						if (this.getEngineType().equals(IEngine.ENGINE_TYPE.RDBMS)) {
							if (type.contains(":")) {
								String[] typeArray = type.split(":");
								String table = typeArray[0];
								type = typeArray[1];
								//if (type != null && table != null && !type.equalsIgnoreCase(table)) // Parameter structure: '@ParamName-Table:Column@'
								//paramQuery = paramQuery.substring(0, paramQuery.lastIndexOf("@entity@")) + table;
							}
						}
						Map<String, List<Object>> paramTable = new Hashtable<String, List<Object>>();
						List<Object> typeList = new Vector<Object>();
						typeList.add(type);
						paramTable.put(Constants.ENTITY, typeList);
						paramQuery = Utility.fillParam(paramQuery, paramTable);
					}
					if(isDbQuery) {
						uris = this.getCleanSelect(paramQuery);
					} else {
						uris = this.baseDataEngine.getCleanSelect(paramQuery);
					}
				}else { 
					// anything that is get Entity of Type must be on db
					uris = this.getEntityOfType(type);
				}
			}
		}
		return uris;
	}

	@Override
	public Vector<Insight> getInsight(String... questionIDs) {
		Vector<Insight> insightV = new Vector<Insight>();
		
		String idString = "";
		int numIDs = questionIDs.length;
		for(int i = 0; i < numIDs; i++) {
			String id = questionIDs[i];
			idString = idString + "'" + id + "'";
			if(i != numIDs - 1) {
				idString = idString + ", ";
			}
		}
		String query = GET_INSIGHT_INFO_QUERY.replace(QUESTION_PARAM_KEY, idString);
		logger.info("Running insights query " + query);
		
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightRDBMS, query);
		wrap.execute();
		String[] names = wrap.getVariables();
		while (wrap.hasNext()) {
			ISelectStatement ss = wrap.next();
			String insightID = ss.getVar(names[0]) + "";
			String insightName = ss.getVar(names[1]) + "";
			
			JdbcClob obj = (JdbcClob) ss.getRawVar(names[2]);
			
			InputStream insightDefinition = null;
			try {
				insightDefinition = obj.getAsciiStream();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String perspective = ss.getVar(names[3]) + "";
			String layout = ss.getVar(names[4]) + "";
			String order = ss.getVar(names[5]) + "";
			String dataTableAlign = ss.getVar(names[6]) + "";
			String dataMakerName = ss.getVar(names[7]) + "";

			Insight in = new Insight(this, dataMakerName, layout);
			in.setInsightID(insightID);
			in.setRdbmsId(insightID);
			in.setInsightName(insightName);
			in.setMakeup(insightDefinition);
			in.setPerspective(perspective);
			in.setOrder(order);
			in.setDataTableAlign(dataTableAlign);
			// adding semoss parameters to insight
			in.setInsightParameters(getParams(insightID));
			insightV.add(in);
			logger.debug(in.toString());
		}
		if(insightV.isEmpty()) {
			System.err.println("FAILED TO GET ANY INSIGHT FOR ARRAY :::::: "+ questionIDs[0]);
			// in = labelIdHash.get(label);
			Insight in = new Insight(this, "Unknown", "Unknown");
			in.setInsightID("DNE");
			in.setRdbmsId("DNE");
			in.setInsightName("DNE");
//			in.setMakeup("This will not work");
			insightV.add(in);
			logger.debug("Using Label ID Hash ");
		}
		return insightV;
	}

	//TODO: currently these are never actually used in the application....
	@Override
	public Vector<String> getInsight4Type(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	//TODO: currently these are never actually used in the application....
	@Override
	public Vector<String> getInsight4Tag(String tag) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getInsightDefinition()
	{
		StringBuilder stringBuilder = new StringBuilder();
		// call script command to get everything necessary to recreate rdbms engine on the other side//
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightRDBMS, "SCRIPT");
		String[] names = wrap.getVariables();

		while(wrap.hasNext()) {
			ISelectStatement ss = wrap.next();
			System.out.println(ss.getRPropHash().toString());//
			stringBuilder.append(ss.getRawVar(names[0]) + "").append("%!%");
		}
//		this.insightRDBMS.execQuery("SCRIPT TO 'C:\\Users\\bisutton\\workspace\\script.txt'");
		return stringBuilder.toString();
	}
}
