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
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.jdbc.JdbcClob;
import org.openrdf.model.vocabulary.RDFS;

import prerna.ds.QueryStruct;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.Insight;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSParam;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryBuilder;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.rdf.query.builder.SPARQLInterpreter;
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

	//THIS IS IN CASE YOU NEED TO RECREATE YOUR INSIGHTS FROM THE XML/PROP QUESTIONS FILE
	//USE THIS INSTEAD OF GOING THROUGH EACH DB FOLDER TO DELETE THE INSIGHT_DATABASE AND SMSS RDMBS_INSIGHT LINE
	//PLEASE REMEMBER TO TURN THIS TO FALSE AFTERWARDS!
	private static final boolean RECREATE_INSIGHTS = false;


	//THIS IS IN CASE YOU ARE MANUALLY MANIPULATING THE DB FOLDER AND WANT TO RE-ADD
	//INSIGHTS INTO THE SOLR INSTANCE ON YOUR LOCAL MACHINE
	//PLEASE REMEMBER TO TURN THIS TO FALSE AFTERWARDS!
	public static final boolean RECREATE_SOLR = false;
	
	private static final Logger logger = LogManager.getLogger(AbstractEngine.class.getName());

	protected String engineName = null;
	private String propFile = null;

	protected Properties prop = null;
	private Properties generalEngineProp = null;
	private Properties ontoProp = null;

	private MetaHelper owlHelper = null;
	
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

	private transient Map<String, String> tableUriCache = new HashMap<String, String>();

	private Hashtable<String, String> baseDataHash;

	private static final String SEMOSS_URI = "http://semoss.org/ontologies/";
	private String baseUri;

	private static final String CONTAINS_BASE_URI = SEMOSS_URI + Constants.DEFAULT_RELATION_CLASS + "/Contains";
	private static final String GET_ALL_INSIGHTS_QUERY = "SELECT DISTINCT ID, QUESTION_ORDER FROM QUESTION_ID ORDER BY ID";
	private static final String GET_ALL_INSIGHTS_WITH_METADATA_QUERY = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_PERSPECTIVE, QUESTION_LAYOUT, ID FROM QUESTION_ID ORDER BY ID";

	private static final String GET_ALL_PERSPECTIVES_QUERY = "SELECT DISTINCT QUESTION_PERSPECTIVE FROM QUESTION_ID ORDER BY QUESTION_PERSPECTIVE";

	private static final String QUESTION_PARAM_KEY = "@QUESTION_VALUE@";
	private static final String GET_INSIGHT_INFO_QUERY = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_MAKEUP, QUESTION_PERSPECTIVE, QUESTION_LAYOUT, QUESTION_ORDER, DATA_TABLE_ALIGN, QUESTION_DATA_MAKER FROM QUESTION_ID WHERE ID IN (" + QUESTION_PARAM_KEY + ") ORDER BY QUESTION_ORDER";

	private static final String PERSPECTIVE_PARAM_KEY = "@PERSPECTIVE_VALUE@";
	private static final String GET_ALL_INSIGHTS_IN_PERSPECTIVE = "SELECT DISTINCT ID, QUESTION_ORDER FROM QUESTION_ID WHERE QUESTION_PERSPECTIVE = '" + PERSPECTIVE_PARAM_KEY + "' ORDER BY QUESTION_ORDER";

	private static final String QUESTION_ID_FK_PARAM_KEY = "@QUESTION_ID_FK_VALUES@";
	private static final String GET_ALL_PARAMS_FOR_QUESTION_ID = "SELECT DISTINCT PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_OPTIONS, PARAMETER_QUERY, PARAMETER_DEPENDENCY, PARAMETER_IS_DB_QUERY, PARAMETER_MULTI_SELECT, PARAMETER_COMPONENT_FILTER_ID, PARAMETER_ID FROM PARAMETER_ID WHERE QUESTION_ID_FK = " + QUESTION_ID_FK_PARAM_KEY;

	private static final String PARAMETER_ID_PARAM_KEY = "@PARAMETER_ID";
	private static final String GET_INFO_FOR_PARAM = "SELECT DISTINCT PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_OPTIONS, PARAMETER_QUERY, PARAMETER_DEPENDENCY, PARAMETER_IS_DB_QUERY, PARAMETER_MULTI_SELECT, PARAMETER_COMPONENT_FILTER_ID FROM PARAMETER_ID WHERE PARAMETER_ID = '" + PARAMETER_ID_PARAM_KEY + "'";
	private static final String GET_INFO_FOR_PARAMS = "SELECT DISTINCT PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_OPTIONS, PARAMETER_QUERY, PARAMETER_DEPENDENCY, PARAMETER_IS_DB_QUERY, PARAMETER_MULTI_SELECT, PARAMETER_COMPONENT_FILTER_ID FROM PARAMETER_ID WHERE PARAMETER_ID IN (" + PARAMETER_ID_PARAM_KEY + ")";
	
	
	private static final String GET_BASE_URI_FROM_OWL = "SELECT DISTINCT ?entity WHERE {"
			+ "{ <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1";

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
				// creating logic to delete file if not there
				if(insightDatabaseLoc != null && RECREATE_INSIGHTS) {
					String location = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + insightDatabaseLoc + ".mv.db";
					FileUtils.forceDelete(new File(location));
				} 
				if(insightDatabaseLoc != null && !RECREATE_INSIGHTS) {
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
//					insightRDBMS.insertData("UPDATE QUESTION_ID p SET QUESTION_LAYOUT = 'Graph' WHERE p.QUESTION_DATA_MAKER = 'GraphDataModel'");
					insightRDBMS.insertData("UPDATE QUESTION_ID p SET QUESTION_DATA_MAKER = REPLACE(QUESTION_DATA_MAKER, 'BTreeDataFrame', 'TinkerFrame')");
//					insightRDBMS.insertData("UPDATE QUESTION_ID SET QUESTION_MAKEUP=REGEXP_REPLACE ( QUESTION_MAKEUP , '\\\\\\\"' , '''' )");
					insightRDBMS.insertData("UPDATE QUESTION_ID p SET QUESTION_MAKEUP = REPLACE(QUESTION_MAKEUP, 'SELECT @Concept-Concept:Concept@, ''http://www.w3.org/1999/02/22-rdf-syntax-ns#type'', ''http://semoss.org/ontologies/Concept''', 'SELECT @Concept-Concept:Concept@') WHERE p.QUESTION_DATA_MAKER = 'TinkerFrame'");
					insightRDBMS.insertData("UPDATE QUESTION_ID SET QUESTION_DATA_MAKER='TinkerFrame' WHERE QUESTION_NAME='Explore a concept from the database' OR QUESTION_NAME='Explore an instance of a selected node type'"); 
					// Update existing dbs to not include QUESTION_ID column if it is there. Can remove this once everyone has updated their dbs
//					ITableDataFrame f = WrapperManager.getInstance().getSWrapper(insightRDBMS, "select count(*) from information_schema.columns where table_name = 'QUESTION_ID' and column_name = 'QUESTION_ID'").getTableDataFrame();
//					if((Double)f.getData().get(0)[0] != 0 ){
//						insightRDBMS.insertData("UPDATE PARAMETER_ID p SET QUESTION_ID_FK = (SELECT ID FROM QUESTION_ID q WHERE q.QUESTION_ID = p.QUESTION_ID_FK)");
//						insightRDBMS.insertData("ALTER TABLE QUESTION_ID DROP COLUMN IF EXISTS QUESTION_ID");
//					}
					// Update existing dbs that do not have the UI table and have Question_ID_FK as a varchar
//					ITableDataFrame f = WrapperManager.getInstance().getSWrapper(insightRDBMS, "select count(*) from information_schema.tables where table_name = 'UI'").getTableDataFrame(); 
//					if((Double)f.getData().get(0)[0] == 0 ) {
//						//this is so we do not modify the form builder engine db -- make sure that it has the parameter id table
//						ITableDataFrame f2 = WrapperManager.getInstance().getSWrapper(insightRDBMS, "select count(*) from information_schema.tables where table_name = 'PARAMETER_ID'").getTableDataFrame(); 
//						if((Double)f2.getData().get(0)[0] != 0 ) {
//							insightRDBMS.insertData("CREATE TABLE UI (QUESTION_ID_FK INT, UI_DATA CLOB)");
//							insightRDBMS.insertData("ALTER TABLE PARAMETER_ID ALTER COLUMN QUESTION_ID_FK INT");
//						}
//					}
				} 
				else { // RDBMS Question engine has not been made. Must do conversion
					// set the necessary fun stuff
					createInsights(baseFolder);
				}
				// load the rdf owl db
				String owlFile = prop.getProperty(Constants.OWL);
				if (owlFile != null) {
					logger.info("Loading OWL: " + owlFile);
					setOWL(baseFolder + "/" + owlFile);
				}
//				String ontoFile = prop.getProperty(Constants.ONTOLOGY);
//				if (ontoFile != null) {
//					logger.info("Loading Ontology: " + ontoFile);
//					setOntology(baseFolder + "/" + ontoFile);
//				}
				// load properties object for db
				String genEngPropFile = prop.getProperty(Constants.ENGINE_PROPERTIES);
				if (genEngPropFile != null) {
					generalEngineProp = loadProp(baseFolder + "/" + genEngPropFile);
				}
				
				// since this is new, we need to add it to the smss file if missing
				String fillDataTypes = prop.getProperty(Constants.FILL_EMPTY_DATATYPES);
				if(fillDataTypes == null) {
					Utility.updateSMSSFile(propFile, Constants.FILL_EMPTY_DATATYPES, "true");
				}
				
			}
			this.owlHelper = new MetaHelper(baseDataEngine, getEngineType(), this.engineName);
			this.owlHelper.loadTransformedNodeNames();
			//this.loadTransformedNodeNames();
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void createInsights(String baseFolder) throws FileNotFoundException, IOException {
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
		if(!RECREATE_INSIGHTS) {
			converter.updateSMSSFile();
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
	 */
	public Properties loadProp(String fileName) {
		Properties retProp = new Properties();
		FileInputStream fis = null;
		if(fileName != null) {
			try {
				fis = new FileInputStream(fileName);
				retProp.load(fis);
			} catch (IOException e) {
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
		
		if(this.baseDataEngine != null) {
			if(this.baseDataEngine.getEngineName() == null) {
				this.baseDataEngine.setEngineName(this.engineName + "_OWL");
			}
		}
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
//	public void setOntology(String ontology) {
//		logger.debug("Ontology file is " + ontology);
//		this.ontology = ontology;
//
//		if (ontoProp == null) {
//			ontoProp = new Properties();
//			try {
//				ontoProp = loadProp(ontology);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//	}

	public void setOWL(String owl) {
		this.owl = owl;
		createBaseRelationEngine();
		this.owlHelper = new MetaHelper(baseDataEngine, getEngineType(), this.engineName);
		this.owlHelper.loadTransformedNodeNames();
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
		if(owlHelper == null)
			return null;
		return owlHelper.getFromNeighbors(nodeType, neighborHood);
	}

	// gets the to nodes
	public Vector<String> getToNeighbors(String nodeType, int neighborHood) {
		if(owlHelper == null)
			return null;
		return owlHelper.getToNeighbors(nodeType, neighborHood);
	}

	// gets the from and to nodes
	public Vector<String> getNeighbors(String nodeType, int neighborHood) {
		if(owlHelper == null)
			return null;
		return owlHelper.getNeighbors(nodeType, neighborHood);
	}

	public String getOWL() {
		return this.owl;
	}
	
	public String getPropFile() {
		return propFile;
	}

	@Override
	public void setPropFile(String propFile) {
		this.propFile = propFile;
		this.prop = loadProp(propFile);
	}

	public String getOWLDefinition()
	{
		if(owlHelper == null)
			return null;
		return owlHelper.getOWLDefinition();
	}

	public IQueryBuilder getQueryBuilder(){
		return new SPARQLQueryTableBuilder(this);
	}
	public IQueryInterpreter getQueryInterpreter(){
		return new SPARQLInterpreter(this);
	}

	/**
	 * Commits the base data engine
	 */
	public void commitOWL() {
		logger.debug("Committing base data engine of " + this.engineName);
		this.baseDataEngine.commit();
	}

	public abstract Vector<Object> getCleanSelect(String query);

	public Vector<String> getConcepts() {
		if(owlHelper == null)
			return null;
		return owlHelper.getConcepts();
}
	
	/**
	 * Get the list of the concepts within the database
	 * @param conceptualNames		Return the conceptualNames if present within the database
	 */
	public Vector<String> getConcepts2(boolean conceptualNames) {
		return owlHelper.getConcepts2(conceptualNames);
	}
	
	/**
	 * Goes to the owl using a regex sparql query to get the physical uri
	 * @param physicalName e.g. Studio
	 * @return e.g. http://semoss.org/ontologies/Concept/Studio
	 */
	public String getConceptUri4PhysicalName(String physicalName){
		if(tableUriCache.containsKey(physicalName)){
			return tableUriCache.get(physicalName);
		}
		Vector<String> cons = this.getConcepts2(false);
		for(String checkUri : cons){
			if(Utility.getInstanceName(checkUri).equals(physicalName)){
				tableUriCache.put(physicalName, checkUri);
				return checkUri;
			}
		}

		return "unable to get table uri for " + physicalName;
	}

	public List<String> getProperties4Concept(String concept, Boolean logicalNames) {
		String uri = concept;
		if(!uri.contains("http://")){
			uri = Constants.DISPLAY_URI + uri;
		}
		String query = "SELECT ?property WHERE { {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?concept  <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property}"
				+ "{?property a <" + CONTAINS_BASE_URI + ">}}"
				+ "BINDINGS ?concept {(<"+ this.getTransformedNodeName(uri, false) +">)}";
		List<String> uriProps = Utility.getVectorOfReturn(query, baseDataEngine, true);
		if(!logicalNames){
			return uriProps;
		}
		else{
			// need to go through each one and translate
			List<String> propNames = new Vector<String>();
			for(String uriProp : uriProps){
				String logicalName = this.getTransformedNodeName(uriProp, true);
				propNames.add(logicalName);
			}
			return propNames;
		}
	}
	
	/**
	 * Returns the set of properties for a given concept
	 * @param concept					The concept URI
	 * 									Assumes the concept URI is the conceptual URI
	 * @param conceptualNames			Boolean to determine if the return should be the properties
	 * 									conceptual names or physical names
	 * @return							List containing the property URIs for the given concept
	 */
	public List<String> getProperties4Concept2(String concept, Boolean conceptualNames) {
		return owlHelper.getProperties4Concept2(concept, conceptualNames);
	}
	
	/**
	 * Get the physical URI from the conceptual URI
	 * @param conceptualURI			The conceptual URI
	 * 								If it is not a valid URI, we will assume it is the instance_name and create the URI
	 * @return						Return the physical URI 					
	 */
	public String getPhysicalUriFromConceptualUri(String conceptualURI) {
		return owlHelper.getPhysicalUriFromConceptualUri(conceptualURI);
	}
	
	public String getConceptualUriFromPhysicalUri(String physicalURI) {
		return owlHelper.getConceptualUriFromPhysicalUri(physicalURI);
	}

	
	/**
	 * query the owl to get the display name or the physical name
	 */
	public String getTransformedNodeName(String nodeURI, boolean getDisplayName){
		//String returnNodeURI = nodeURI;
		
		//these validation peices are seperated out intentionally for readability.
		if(owlHelper == null)
			return nodeURI;
		return owlHelper.getTransformedNodeName(nodeURI, getDisplayName);
	}

	public void setTransformedNodeNames(Hashtable transformedNodeNames){
		owlHelper.setTransformedNodeNames(transformedNodeNames);
	}
	
	@Override
	public void loadTransformedNodeNames(){
		owlHelper.loadTransformedNodeNames();
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
		case BULK_INSERT : {
			retString = "bulkInsertPreparedStatement";
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

//				String ontoLoc = baseFolder + "/" + this.getProperty(Constants.ONTOLOGY);
//				if(ontoLoc != null){
//					logger.debug("Deleting onto file " + ontoLoc);
//					File ontoFile = new File(ontoLoc);
//					ontoFile.delete();
//				}

				String owlLoc = baseFolder + "/" + this.getProperty(Constants.OWL);
				if(owlLoc != null){
					logger.debug("Deleting owl file " + owlLoc);
					File owlFile = new File(owlLoc);
					owlFile.delete();
				}
			}
			logger.debug("Deleting smss " + this.propFile);
			File smssFile = new File(this.propFile);
			FileUtils.forceDelete(smssFile);

			//remove from DIHelper
			String engineNames = (String)DIHelper.getInstance().getLocalProp(Constants.ENGINES);
			engineNames = engineNames.replace(";" + engineName, "");
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
			DIHelper.getInstance().removeLocalProperty(engineName);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public AbstractQueryParser getQueryParser() {
		return new SPARQLQueryParser();
	}

	public static void main(String [] args) throws Exception
	{
		DIHelper.getInstance().loadCoreProp("C:\\workspace\\SEMOSSDev\\RDF_Map.prop");
		FileInputStream fileIn = null;
		try{
			Properties prop = new Properties();
			String fileName = "C:\\workspace\\SEMOSSDev\\db\\Movie_Test.smss";
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
		List<String> props = eng.getProperties4Concept("http://semoss.org/ontologies/Concept/Title", false);
		while(!props.isEmpty()){
			System.out.println(props.remove(0));
		}
	}

	@Override
	public IEngine getInsightDatabase() {
		return this.insightRDBMS;
	}
	
	@Override
	public Vector<String> executeInsightQuery(String sparqlQuery, boolean isDbQuery) {
		IEngine engine = this;
		if(!isDbQuery){
			engine = this.baseDataEngine;
		} 
			
		return Utility.getVectorOfReturn(sparqlQuery, engine, true);
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
			if(ss.getVar(names[5]) != null && !ss.getVar(names[5]).toString().isEmpty())
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
			if(ss.getVar(names[5]) != null && !ss.getVar(names[5]).toString().isEmpty())
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
						Vector<Object> baseUris = this.baseDataEngine.getCleanSelect(paramQuery);
						if(baseUris != null) {
							for(Object baseUri : baseUris) {
								uris.add(this.getTransformedNodeName(baseUri + "", true));
							}
						}
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
		String idString = "";
		int numIDs = questionIDs.length;
		Vector<Insight> insightV = new Vector<Insight>(numIDs);
		List<Integer> counts = new Vector<Integer>(numIDs);
		for(int i = 0; i < numIDs; i++) {
			String id = questionIDs[i];
			try {
				Integer.parseInt(id);
				idString = idString + "'" + id + "'";
				if(i != numIDs - 1) {
					idString = idString + ", ";
				}
				counts.add(i);
			} catch(NumberFormatException e) {
				/*
				 * UPDATE!!!! THIS LOGIC SHOULD NOT BE ENTERED ANYMORE.  THIS USED TO BE THE CASE FOR WHEN
				 * CSV INSIGHTS WERE CACHED AND NOT SAVED AS PKQL, BUT THAT IS NO LONGER THE CASE.
				 * 
				 * This logic was used to get an insight that didn't exist in the engines rdbms insights
				 * but it was saved with the engine as its "core_engine" in the solr schema.  
				 * 
				 * To get the appropriate information regarding the insight:
				 * 1) create an empty insight object
				 * 2) set the id within the insight
				 * 3) use method loadDataFromSolr and it will load the insight metadata from solr
				 * 
				 */
				System.err.println("FAILED TO GET ANY INSIGHT FOR ARRAY :::::: "+ questionIDs[0]);
				// 1) create an empty insight object
				Insight in = new Insight(this, "Unknown", "Unknown");
				// 2) set the insight id
				in.setInsightID(id);
				in.setDatabaseID(id);
				// this is a boolean that is used to determine if the insight is a "drag and drop" insight
				in.setIsDbInsight(false);
				// 3) loads insight metadata from the solr core into the insight class
				in.loadDataFromSolr();
				insightV.insertElementAt(in, i);
				logger.debug("Using Label ID Hash ");
			}
		}
		
		if(!idString.isEmpty()) {
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
				
				insightV.insertElementAt(in, counts.remove(0));
				logger.debug(in.toString());
			}
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
	
	@Override
	public String getNodeBaseUri(){
		if(baseUri == null) {
			ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(this.baseDataEngine, GET_BASE_URI_FROM_OWL);
			if(wrap.hasNext()) {
				ISelectStatement ss = wrap.next();
				baseUri = ss.getRawVar("entity") + "";
				logger.info("Got base uri from owl " + baseUri + " for engine " + getEngineName());
			}
			if(baseUri == null){
				baseUri = Constants.CONCEPT_URI;
				logger.info("couldn't get base uri from owl... defaulting to " + baseUri + " for engine " + getEngineName());
				
			}
		}
		
		return baseUri;
	}
	
	@Override
	public String getDataTypes(String uri) {
		String cleanUri = getTransformedNodeName(uri, false);
		String query = "SELECT DISTINCT ?TYPE WHERE { {<" + cleanUri + "> <" + RDFS.CLASS.toString() + "> ?TYPE} }";
			
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(baseDataEngine, query);
		String[] names = wrapper.getPhysicalVariables();
		String type = null;
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			type = ss.getVar(names[0]).toString();
		}
		
		return type;
	}
	
	@Override
	public Map<String, String> getDataTypes(String... uris) {
		Map<String, String> retMap = new Hashtable<String, String>();
		String bindings = "";
		for(String uri : uris) {
			String cleanUri = getTransformedNodeName(uri, false);
			bindings += "(<" + cleanUri + ">)";	
		}
		String query = null;
		if(!bindings.isEmpty()) {
			query = "SELECT DISTINCT ?NODE ?TYPE WHERE { {?NODE <" + RDFS.CLASS.toString() + "> ?TYPE} } BINDINGS ?NODE {" + bindings + "}";
			
		} else {
			// if no bindings, return everything
			query = "SELECT DISTINCT ?NODE ?TYPE WHERE { {?NODE <" + RDFS.CLASS.toString() + "> ?TYPE} }";
		}
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(baseDataEngine, query);
		String[] names = wrapper.getPhysicalVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String node = ss.getRawVar(names[0]).toString();
			String type = ss.getVar(names[1]).toString();
			
			retMap.put(getTransformedNodeName(node, true), type);
		}
		
		return retMap;
	}
	
	public String getParentOfProperty(String prop) {
		if(!prop.startsWith("http://")) {
			prop = "http://semoss.org/ontologies/Relation/Contains/" + prop;
		}
		
		String query = "SELECT DISTINCT ?concept WHERE { ?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> <" + prop + "> }";
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(baseDataEngine, query);
		String[] names = wrapper.getPhysicalVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String node = ss.getRawVar(names[0]).toString();
			return node;
		}
		
		// in new schema, try the conceptual
		query = "SELECT DISTINCT ?concept WHERE { "
				+ "{?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?phyProp }"
				+ "{?phyProp <http://semoss.org/ontologies/Relation/Conceptual> <" + prop + ">}"
				+ "}";
		
		wrapper = WrapperManager.getInstance().getSWrapper(baseDataEngine, query);
		names = wrapper.getPhysicalVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String node = ss.getRawVar(names[0]).toString();
			return node;
		}
		return null;
	}
	
	public List<String> getParentOfProperty2(String prop) {
		List<String> retList = new Vector<String>();
		
		if(!prop.startsWith("http://")) {
			prop = "http://semoss.org/ontologies/Relation/Contains/" + prop;
		}

		// in new schema, try the conceptual
		String query = "SELECT DISTINCT ?concept WHERE { "
				+ "{?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?phyProp }"
				+ "{?phyProp <http://semoss.org/ontologies/Relation/Conceptual> <" + prop + ">}"
				+ "}";
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(baseDataEngine, query);
		String[] names = wrapper.getPhysicalVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String node = ss.getRawVar(names[0]).toString();
			retList.add(node);
		}
		
		return retList;
	}
	
	
	/**
	 * This method will return a query struct which when interpreted would produce a query to 
	 * get all the data within the engine.  Will currently assume all joins to be inner.join
	 * @return
	 */
	public QueryStruct getDatabaseQueryStruct() {
		QueryStruct qs = new QueryStruct();
		
		boolean oldOwl = true;
		// TODO: need to get rid of this bifurcation in queries between having an OLD OWL vs.
		// having a new OWL
		String testOwlVersion = "SELECT DISTINCT ?conceptual WHERE {"
				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?concept <http://semoss.org/ontologies/Relation/Conceptual> ?conceptual }"
			+ "}"; // end where
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(baseDataEngine, testOwlVersion);
		if(wrapper.hasNext()) {
			oldOwl = false;
		}
		
		// query to get all the concepts and properties for selectors
		String getSelectorsInformation = "";
		//TODO: bifurcation because of different OWL versions
		if(oldOwl) {
			getSelectorsInformation = "SELECT DISTINCT ?concept ?property WHERE { "
					+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
					+ "OPTIONAL {"
						+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + CONTAINS_BASE_URI + "> } "
						+ "{?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property } "
					+ "}" // END OPTIONAL
					+ "}"; // END WHERE
		} else {
			getSelectorsInformation = "SELECT DISTINCT ?conceptualConcept ?conceptualProperty WHERE { "
					+ "{?concept2 <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
					+ "{?concept2 <http://semoss.org/ontologies/Relation/Conceptual> ?conceptualConcept }"
					+ "OPTIONAL {"
						+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + CONTAINS_BASE_URI + "> } "
						+ "{?concept2 <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property } "
						+ "{?property <http://semoss.org/ontologies/Relation/Conceptual> ?conceptualProperty }"
					+ "}" // END OPTIONAL
					+ "}"; // END WHERE
		}
	
		// execute the query and loop through and add it into the QS
		wrapper = WrapperManager.getInstance().getSWrapper(baseDataEngine, getSelectorsInformation);
		String[] names = wrapper.getPhysicalVariables();
		// we will keep a set of the concepts such that we know when we need to append a PRIM_KEY_PLACEHOLDER
		Set<String> conceptSet = new HashSet<String>();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String conceptURI = ss.getRawVar(names[0]) + "";
			if(conceptURI.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			
			Object property = ss.getVar(names[1]);
			String concept = conceptURI.replaceAll(".*/Concept/", "");
			if(concept.contains("/")) {
				concept = concept.substring(0, concept.indexOf("/"));
			}
			if(!conceptSet.contains(concept)) {			
				qs.addSelector(concept, null);
				conceptSet.add(concept);
			}
			
			if(property != null && !property.toString().isEmpty()) {
				qs.addSelector(concept, property.toString());
			}
		}
		// no need to keep this anymore
		conceptSet = null;
		
		// query to get all the relationships 
		String getRelationshipsInformation = "";
		//TODO: bifurcation because of different OWL versions
		if(oldOwl) {
			getRelationshipsInformation = "SELECT DISTINCT ?fromConcept ?toConcept WHERE { "
				+ "{?fromConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
				+ "{?toConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
				+ "{?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
				+ "{?fromConcept ?rel ?toConcept} "
				+ "}"; // END WHERE
		} else {
			getRelationshipsInformation = "SELECT DISTINCT ?fromConceptualConcept ?toConceptualConcept WHERE { "
					+ "{?fromConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
					+ "{?toConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
					+ "{?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
					+ "{?fromConcept ?rel ?toConcept} "
					+ "{?fromConcept <http://semoss.org/ontologies/Relation/Conceptual> ?fromConceptualConcept }"
					+ "{?toConcept <http://semoss.org/ontologies/Relation/Conceptual> ?toConceptualConcept }"
					+ "}"; // END WHERE
		}
		
		wrapper = WrapperManager.getInstance().getSWrapper(baseDataEngine, getRelationshipsInformation);
		names = wrapper.getPhysicalVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String fromConcept = ss.getVar(names[0]) + "";
			String toConcept = ss.getVar(names[1]) + "";
			
			qs.addRelation(fromConcept, toConcept, "inner.join");
		}
		
		return qs;
	}
	
	
	/**
	 * This will return the metamodel object used to view on dagger for an engine
	 * @return
	 */
	public Map<String, Object> getMetamodel() {
		// create the return object map and put all values inside
		// when objects are modified, they get modified in the retObj as well
		// yay modifying references
		Map<String, Object> retObj = new Hashtable<String, Object>();
		List<SEMOSSVertex> nodes = new Vector<SEMOSSVertex>();
		List<SEMOSSEdge> edges = new Vector<SEMOSSEdge>();
		retObj.put("nodes", nodes);
		retObj.put("edges", edges);

		// create this from the query struct
		QueryStruct qs = getDatabaseQueryStruct();

		// need to store the edges in a way that we can easily get them
		Map<String, SEMOSSVertex> vertStore = new Hashtable<String, SEMOSSVertex>();

		// first get all the nodes
		Hashtable<String, Vector<String>> vertices = qs.getSelectors();
		for(String concept : vertices.keySet()) {
			// grab each vert
			SEMOSSVertex vert = new SEMOSSVertex("http://semoss.org/ontologies/Concept/" + concept);
			vert.putProperty("PhysicalName", concept);
			// grab the properties and add it to the vert
			Vector<String> props = vertices.get(concept);
			for(String prop : props) {
				// ignore the placeholder as it is only used by interpreters for query construction
				if(prop.equals(QueryStruct.PRIM_KEY_PLACEHOLDER)) {
					continue;
				}
				
				// add the prop
				vert.setProperty("http://semoss.org/ontologies/Relation/Contains/" + prop, prop);
			}
			
			vertStore.put(concept, vert);
			// add it to the nodes list
			nodes.add(vert);
		}
		
		// now go through all the relations
		// remember, the map is the {fromConcept -> { joinType -> [toConcept1, toConcept2] } }
		// need to iterate through to get fromConcept -> toConcept and make a unique edge for each
		// remember the edge names are not every actually used
		Hashtable<String, Hashtable<String, Vector>> relations = qs.getRelations();
		for(String fromConcept : relations.keySet()) {
			// get the from-vertex
			SEMOSSVertex fromVert = vertStore.get(fromConcept);

			// need to iterate through the join types to get to the toConcpets
			Hashtable<String, Vector> joinsMap = relations.get(fromConcept);
			for(String joinType : joinsMap.keySet()) {
				Vector<String> toConcepts = joinsMap.get(joinType);
				
				for(String toConcept : toConcepts) {
					// get the to-vertex
					SEMOSSVertex toVert = vertStore.get(toConcept);
					
					// create the edge
					// edge name doesn't actually matter
					SEMOSSEdge edge = new SEMOSSEdge(fromVert, toVert, "http://semoss.org/ontologies/Relation/" + fromConcept + ":" + toConcept);
					
					// add it to the edge list
					edges.add(edge);
				}
			}
		}
		
		
		return retObj;
	}
	
}
