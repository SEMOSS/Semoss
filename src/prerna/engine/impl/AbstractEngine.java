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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.Date;
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
import org.apache.solr.client.solrj.SolrServerException;
import org.openrdf.model.vocabulary.RDFS;

import prerna.engine.api.IDatasourceIterator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.SparqlInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.reactor.legacy.playsheets.LegacyInsightDatabaseUtility;
import prerna.solr.SolrIndexEngine;
import prerna.solr.SolrUtility;
import prerna.ui.components.RDFEngineHelper;
import prerna.util.CSVToOwlMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * An Abstract Engine that sets up the base constructs needed to create an
 * engine.
 */
public abstract class AbstractEngine implements IEngine {

	private static final Logger lOGGER = LogManager.getLogger(AbstractEngine.class.getName());
	private static final String SEMOSS_URI = "http://semoss.org/ontologies/";
	private static final String CONTAINS_BASE_URI = SEMOSS_URI + Constants.DEFAULT_RELATION_CLASS + "/Contains";
	private static final String GET_ALL_INSIGHTS_QUERY = "SELECT DISTINCT ID, QUESTION_ORDER FROM QUESTION_ID ORDER BY ID";
	private static final String GET_ALL_PERSPECTIVES_QUERY = "SELECT DISTINCT QUESTION_PERSPECTIVE FROM QUESTION_ID ORDER BY QUESTION_PERSPECTIVE";
	private static final String QUESTION_PARAM_KEY = "@QUESTION_VALUE@";
	private static final String GET_INSIGHT_INFO_QUERY = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_MAKEUP, QUESTION_PERSPECTIVE, QUESTION_LAYOUT, QUESTION_ORDER, DATA_TABLE_ALIGN, QUESTION_DATA_MAKER, QUESTION_PKQL FROM QUESTION_ID WHERE ID IN (" + QUESTION_PARAM_KEY + ") ORDER BY QUESTION_ORDER";
	private static final String GET_BASE_URI_FROM_OWL = "SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1";

	protected String baseFolder = null;
	protected String propFile = null;
	protected Properties prop = null;
	
	protected String engineId = null;
	protected String engineName = null;
	
	protected Properties generalEngineProp = null;
	protected Properties ontoProp = null;

	private MetaHelper owlHelper = null;
	protected RDFFileSesameEngine baseDataEngine;
	
	public static final String USE_FILE = "USE_FILE";
	public static final String DATA_FILE = "DATA_FILE";
	
	protected RDBMSNativeEngine insightRDBMS;
	protected String insightDriver = "org.h2.Driver";
	protected String insightRDBMSType = "H2_DB";
	protected String connectionURLStart = "jdbc:h2:";
	protected String connectionURLEnd = ";query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
	protected String insightUsername = "sa";

	private boolean isBasic = false;
	private String owl;
	private String insightDatabaseLoc;

	private transient Map<String, String> tableUriCache = new HashMap<String, String>();

	private Hashtable<String, String> baseDataHash;

	private String baseUri;
	
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
			baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			if(propFile != null) {
				this.propFile = propFile;
				lOGGER.info("Opening DB - " + engineName);
				this.prop = Utility.loadProperties(propFile);
			}
			if(prop != null) {
				// grab the main properties
				this.engineId = prop.getProperty(Constants.ENGINE);
				this.engineName = prop.getProperty(Constants.ENGINE_ALIAS);
	
				// load the rdbms insights db
				this.insightDatabaseLoc = prop.getProperty(Constants.RDBMS_INSIGHTS);
				this.insightDatabaseLoc = SmssUtilities.getInsightsRdbmsFile(this.prop).getAbsolutePath();
				
				if(insightDatabaseLoc != null) {
					lOGGER.info("Loading insight rdbms database...");
					this.insightRDBMS = new RDBMSNativeEngine();
					Properties prop = new Properties();
					prop.put(Constants.DRIVER, insightDriver);
					prop.put(Constants.RDBMS_TYPE, insightRDBMSType);
					String connURL = connectionURLStart + insightDatabaseLoc.replace(".mv.db", "") + connectionURLEnd;
					prop.put(Constants.CONNECTION_URL, connURL);
					prop.put(Constants.USERNAME, insightUsername);
					this.insightRDBMS.setProperties(prop);
					this.insightRDBMS.openDB(null);
					
					boolean tableExists = false;
					ResultSet rs = null;
					try {
						rs = this.insightRDBMS.getConnectionMetadata().getTables(null, null, "QUESTION_ID", null);
						if (rs.next()) {
							  tableExists = true;
						}
					} catch (SQLException e) {
						e.printStackTrace();
					} finally {
						try {
							if(rs != null) {
								rs.close();
							}
						} catch(SQLException e) {
							e.printStackTrace();
						}
					}
					
					if(tableExists) {
						String q = "SELECT TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='QUESTION_ID' and COLUMN_NAME='ID'";
						IRawSelectWrapper wrap = WrapperManager.getInstance().getRawWrapper(this.insightRDBMS, q);
						while(wrap.hasNext()) {
							String val = wrap.next().getValues()[0] + "";
							if(!val.equals("VARCHAR")) {
								String update = "ALTER TABLE QUESTION_ID ALTER COLUMN ID VARCHAR(50);";
								this.insightRDBMS.insertData(update);
								this.insightRDBMS.commit();
							}
						}
						wrap.cleanUp();
						
						q = "SELECT TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='QUESTION_ID' and COLUMN_NAME='HIDDEN_INSIGHT'";
						wrap = WrapperManager.getInstance().getRawWrapper(insightRDBMS, q);
						if(!wrap.hasNext()) {
							String update = "ALTER TABLE QUESTION_ID ADD HIDDEN_INSIGHT BOOLEAN DEFAULT FALSE;";
							this.insightRDBMS.insertData(update);
							this.insightRDBMS.commit();
						}
						wrap.cleanUp();
					}
				}
				
				// TODO: this is new code to convert
				// TODO: this is new code to convert
				// TODO: this is new code to convert
				// TODO: this is new code to convert
				String updatedInsights = prop.getProperty(Constants.PIXEL_UPDATE);
				if(updatedInsights == null) {
					updateToPixelInsights();
					Utility.updateSMSSFile(propFile, Constants.PIXEL_UPDATE, "true");
				} else if(!Boolean.parseBoolean(updatedInsights)){
					updateToPixelInsights();
					Utility.changePropMapFileValue(propFile, Constants.PIXEL_UPDATE, "true");
				}
				// update explore an instance query!!!
				updateExploreInstanceQuery(this.insightRDBMS);
				
				// load the rdf owl db
				String owlFile = prop.getProperty(Constants.OWL);
				if(owlFile != null) {
					// need a check here to say if I am asking this to be remade or keep what it is
					if(owlFile.equalsIgnoreCase("REMAKE")) {
						// the process of remake will start here
						// see if the usefile is there
						if(this.prop.containsKey(USE_FILE)) {
							String csvFile = SmssUtilities.getDataFile(this.prop).getAbsolutePath();
							owlFile = csvFile.replace("data/", "") + ".OWL";
							//Map <String, String> paramHash = new Hashtable<String, String>();
							
							String fileName = Utility.getOriginalFileName(csvFile);
							// make the table name based on the fileName
							String cleanTableName = RDBMSEngineCreationHelper.cleanTableName(fileName).toUpperCase();
							owlFile = baseFolder + "/db/" + getEngineId() + "/" + cleanTableName + ".OWL";
							
							CSVToOwlMaker maker = new CSVToOwlMaker();
							maker.makeOwl(csvFile, owlFile, getEngineType());
							owlFile = "/db/" + SmssUtilities.getUniqueName(this.prop) + "/" + cleanTableName + ".OWL";
							
							if(this.prop.containsKey("REPLACE_OWL"))
								Utility.updateSMSSFile(propFile, Constants.OWL, owlFile);
						} else {
							owlFile = null;
						}
					}
					// set the owl file
					if(owlFile != null) {
						owlFile = SmssUtilities.getOwlFile(this.prop).getAbsolutePath();
						lOGGER.info("Loading OWL: " + owlFile);
						setOWL(owlFile);
					}
				}
				// load properties object for db
				File engineProps = SmssUtilities.getEngineProperties(this.prop);
				if (engineProps != null) {
					this.generalEngineProp = Utility.loadProperties(engineProps.getAbsolutePath());
				}
			}
			this.owlHelper = new MetaHelper(this.baseDataEngine, getEngineType(), this.engineId);
		} catch (RuntimeException e) {
			e.printStackTrace();
		} 
	}
	
	@Override
	public void closeDB() {
		if(this.baseDataEngine != null) {
			lOGGER.debug("closing its owl engine ");
			this.baseDataEngine.closeDB();
		}
		if(this.insightRDBMS != null) {
			lOGGER.debug("closing its insight engine ");
			this.insightRDBMS.closeDB();
		}
	}
	
	@Override
	public IDatasourceIterator query(SelectQueryStruct qs) {
		IQueryInterpreter interp = getQueryInterpreter();
		interp.setQueryStruct(qs);
		String query = interp.composeQuery();
		return query(query);
	}
	
	@Override
	public String getProperty(String key) {
		String retProp = null;

		lOGGER.debug("Property is " + key + "]");
		if (generalEngineProp != null && generalEngineProp.containsKey(key))
			retProp = generalEngineProp.getProperty(key);
		if (retProp == null && ontoProp != null && ontoProp.containsKey(key))
			retProp = ontoProp.getProperty(key);
		if (retProp == null && prop != null && prop.containsKey(key))
			retProp = prop.getProperty(key);
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
	 * Sets the unique id for the engine 
	 * @param engineId - id to set the engine 
	 */
	@Override
	public void setEngineId(String engineId) {
		this.engineId = engineId;
	}

	/**
	 * Gets the engine name for this engine	
	 * @return Name of the engine
	 */
	@Override
	public String getEngineId() {
		return this.engineId;
	}
	
	@Override
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	@Override
	public String getEngineName() {
		return this.engineName;
	}
	
	/**
	 * Writes the database back with updated properties if necessary
	 */
	public void saveConfiguration() {
		FileOutputStream fileOut = null;
		try {
			lOGGER.debug("Writing to file " + propFile);
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
			if(this.baseDataEngine.getEngineId() == null) {
				this.baseDataEngine.setEngineId(this.engineId + "_OWL");
			}
		}
	}

	/**
	 * Gets the base data engine.
	 * 
	 * @return RDFFileSesameEngine - the base data engine
	 */
	@Override
	public RDFFileSesameEngine getBaseDataEngine() {
		return this.baseDataEngine;
	}

	/**
	 * Sets the base data hash
	 * @param h		Hashtable - The base data hash that this is being set to
	 */
	public void setBaseHash(Hashtable h) {
		lOGGER.debug(this.engineId + " Set the Base Data Hash ");
		this.baseDataHash = h;
	}

	/**
	 * Gets the base data hash
	 * @return Hashtable - The base data hash.
	 */
	public Hashtable getBaseHash() {
		return this.baseDataHash;
	}

	public void setOWL(String owl) {
		this.owl = owl;
		createBaseRelationEngine();
		this.owlHelper = new MetaHelper(baseDataEngine, getEngineType(), this.engineId);
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
			owl = baseFolder + "/db/" + getEngineId() + "/" + getEngineId()	+ "_OWL.OWL";
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
		this.prop = Utility.loadProperties(propFile);
	}

	public String getOWLDefinition()
	{
		if(owlHelper == null)
			return null;
		return owlHelper.getOWLDefinition();
	}

	@Override
	public IQueryInterpreter getQueryInterpreter(){
		return new SparqlInterpreter(this);
	}
	
	/**
	 * Commits the base data engine
	 */
	public void commitOWL() {
		lOGGER.debug("Committing base data engine of " + this.engineId);
		this.baseDataEngine.commit();
	}

	public Vector<String> getConcepts() {
		if(owlHelper == null)
			return null;
		return owlHelper.getConcepts();
	}
	
	/**
	 * Get the list of the concepts within the database
	 * @param conceptualNames		Return the conceptualNames if present within the database
	 */
	public Vector<String> getConcepts(boolean conceptualNames) {
		return owlHelper.getConcepts(conceptualNames);
	}
	
	public Vector<String[]> getRelationships(boolean conceptualNames) {
		return owlHelper.getRelationships(conceptualNames);
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
		Vector<String> cons = this.getConcepts(false);
		for(String checkUri : cons){
			if(Utility.getInstanceName(checkUri).equals(physicalName)){
				tableUriCache.put(physicalName, checkUri);
				return checkUri;
			}
		}

		return "unable to get table uri for " + physicalName;
	}

	/**
	 * Returns the set of properties for a given concept
	 * @param concept					The concept URI
	 * 									Assumes the concept URI is the conceptual URI
	 * @param conceptualNames			Boolean to determine if the return should be the properties
	 * 									conceptual names or physical names
	 * @return							List containing the property URIs for the given concept
	 */
	public List<String> getProperties4Concept(String concept, Boolean conceptualNames) {
		return owlHelper.getProperties4Concept(concept, conceptualNames);
	}
	
	/**
	 * Get the physical URI from the conceptual URI
	 * @param conceptualURI			The conceptual URI
	 * 								If it is not a valid URI, we will assume it is the instance_name and create the URI
	 * @return						Return the physical URI 					
	 */
	@Override
	public String getPhysicalUriFromConceptualUri(String conceptualURI) {
		return owlHelper.getPhysicalUriFromConceptualUri(conceptualURI);
	}
	
	@Override
	public String getPhysicalUriFromConceptualUri(String propertyName, String paretName) {
		return owlHelper.getPhysicalUriFromConceptualUri(propertyName, paretName);
	}
	
	@Override
	public String getConceptualUriFromPhysicalUri(String physicalURI) {
		return owlHelper.getConceptualUriFromPhysicalUri(physicalURI);
	}
	
	/**
	 * Runs a select query on the base data engine of this engine
	 */
	public Object execOntoSelectQuery(String query) {
		lOGGER.debug("Running select query on base data engine of " + this.engineId);
		lOGGER.debug("Query is " + query);
		return this.baseDataEngine.execQuery(query);
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
		case VERTEX_UPSERT: {
			retString = "upsertVertex";
			break;
		}
		case EDGE_UPSERT: {
			retString = "upsertEdge";
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
			lOGGER.error(e);
		} catch (NoSuchMethodException e) {
			lOGGER.error(e);
		} catch (IllegalArgumentException e) {
			lOGGER.error(e);
		} catch (IllegalAccessException e) {
			lOGGER.error(e);
		} catch (InvocationTargetException e) {
			lOGGER.error(e);
		}
		return ret;
	}

	public void deleteDB() {
		lOGGER.debug("closing " + this.engineName);
		this.closeDB();
		
		File insightFile = SmssUtilities.getInsightsRdbmsFile(this.prop);
		File owlFile = SmssUtilities.getOwlFile(this.prop);
		File engineFolder = insightFile.getParentFile();
		String folderName = engineFolder.getName();
		try {
			if(owlFile != null && owlFile.exists()) {
				System.out.println("Deleting owl file " + owlFile.getAbsolutePath());
				FileUtils.forceDelete(owlFile);
			}
			if(insightFile != null && insightFile.exists()) {
				System.out.println("Deleting insight file " + insightFile.getAbsolutePath());
				FileUtils.forceDelete(insightFile);
			}
			
			//this check is to ensure we are deleting the right folder.
			lOGGER.debug("checking folder name is matching up : " + folderName + " against " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
			if(folderName.equals(SmssUtilities.getUniqueName(this.engineName, this.engineId))) {
				lOGGER.debug("folder getting deleted is " + engineFolder.getAbsolutePath());
				try {
					FileUtils.deleteDirectory(engineFolder);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			lOGGER.debug("Deleting smss " + this.propFile);
			File smssFile = new File(this.propFile);
			FileUtils.forceDelete(smssFile);

			//remove from DIHelper
			String engineNames = (String)DIHelper.getInstance().getLocalProp(Constants.ENGINES);
			engineNames = engineNames.replace(";" + this.engineId, "");
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
			DIHelper.getInstance().removeLocalProperty(this.engineId);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public IEngine getInsightDatabase() {
		return this.insightRDBMS;
	}
	
	@Override
	public void setInsightDatabase(IEngine insightDatabase) {
		this.insightRDBMS = (RDBMSNativeEngine) insightDatabase;
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
		Vector<String> perspectives = Utility.getVectorOfReturn(GET_ALL_PERSPECTIVES_QUERY, insightRDBMS, false);
		if(perspectives.contains("")){
			int index = perspectives.indexOf("");
			perspectives.set(index, "Semoss-Base-Perspective");
		}
		return perspectives;
	}

	@Override
	public Vector<String> getInsights(String perspective) {
		String insightsInPerspective = null;
		if(perspective.equals("Semoss-Base-Perspective")) {
			perspective = null;
		}
		if(perspective != null && !perspective.isEmpty()) {
			insightsInPerspective = "SELECT DISTINCT ID, QUESTION_ORDER FROM QUESTION_ID WHERE QUESTION_PERSPECTIVE = '" + perspective + "' ORDER BY QUESTION_ORDER";
		} else {
			insightsInPerspective = "SELECT DISTINCT ID, QUESTION_ORDER FROM QUESTION_ID WHERE QUESTION_PERSPECTIVE IS NULL ORDER BY QUESTION_ORDER";
		}
		return Utility.getVectorOfReturn(insightsInPerspective, insightRDBMS, false);
	}

	@Override
	public Vector<String> getInsights() {
		return Utility.getVectorOfReturn(GET_ALL_INSIGHTS_QUERY, insightRDBMS, false);
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
				idString = idString + "'" + id + "'";
				if(i != numIDs - 1) {
					idString = idString + ", ";
				}
				counts.add(i);
			} catch(NumberFormatException e) {
				System.err.println(">>>>>>>> FAILED TO GET ANY INSIGHT FOR ARRAY :::::: "+ questionIDs[i]);
			}
		}
		
		if(!idString.isEmpty()) {
			String query = GET_INSIGHT_INFO_QUERY.replace(QUESTION_PARAM_KEY, idString);
			lOGGER.info("Running insights query " + query);
			
			IRawSelectWrapper wrap = WrapperManager.getInstance().getRawWrapper(insightRDBMS, query);
			while (wrap.hasNext()) {
				IHeadersDataRow dataRow = wrap.next();
				Object[] values = dataRow.getValues();
//				Object[] rawValues = dataRow.getRawValues();

				String rdbmsId = values[0] + "";
				String insightName = values[1] + "";
				
				Clob obj = (Clob) values[2];
				InputStream insightDefinition = null;
				if(obj != null) {
					try {
						insightDefinition = obj.getAsciiStream();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				String layout = values[4] + "";
				String dataTableAlign = values[6] + "";
				String dataMakerName = values[7] + "";
				Object[] pixel = (Object[]) values[8];
				
				String perspective = values[3] + "";
				String order = values[5] + "";
				
				Insight in = null;
				if(pixel == null || pixel.length == 0) {
					in = new OldInsight(this, dataMakerName, layout);
					in.setRdbmsId(rdbmsId);
					in.setInsightName(insightName);
					((OldInsight) in).setOutput(layout);
					((OldInsight) in).setMakeup(insightDefinition);
//					in.setPerspective(perspective);
//					in.setOrder(order);
					((OldInsight) in).setDataTableAlign(dataTableAlign);
					// adding semoss parameters to insight
					((OldInsight) in).setInsightParameters(LegacyInsightDatabaseUtility.getParamsFromInsightId(this.insightRDBMS, rdbmsId));
					in.setIsOldInsight(true);
				} else {
					in = new Insight(this.engineId, rdbmsId);
					in.setEngineName(this.engineName);
					in.setInsightName(insightName);
					List<String> pixelList = new Vector<String>();
					for(int i = 0; i < pixel.length; i++) {
						pixelList.add(pixel[i].toString().trim());
					}
					in.setPixelRecipe(pixelList);
					// I need this for dashboard
				}
				insightV.insertElementAt(in, counts.remove(0));
			}
		}
		return insightV;
	}

	@Override
	public boolean isBasic() {
		return this.isBasic;
	}
	
	@Override
	public void setBasic(boolean isBasic) {
		this.isBasic = isBasic;
	}
	
	@Override
	public String getInsightDefinition() {
		StringBuilder stringBuilder = new StringBuilder();
		// call script command to get everything necessary to recreate rdbms engine on the other side//
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightRDBMS, "SCRIPT");
		String[] names = wrap.getVariables();

		while(wrap.hasNext()) {
			ISelectStatement ss = wrap.next();
			System.out.println(ss.getRPropHash().toString());//
			stringBuilder.append(ss.getVar(names[0]) + "").append("%!%");
		}
		return stringBuilder.toString();
	}
	
	@Override
	public String getNodeBaseUri(){
		if(baseUri == null) {
			ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(this.baseDataEngine, GET_BASE_URI_FROM_OWL);
			if(wrap.hasNext()) {
				ISelectStatement ss = wrap.next();
				baseUri = ss.getRawVar("entity") + "";
				lOGGER.info("Got base uri from owl " + baseUri + " for engine " + getEngineId());
			}
			if(baseUri == null){
				baseUri = Constants.CONCEPT_URI;
				lOGGER.info("couldn't get base uri from owl... defaulting to " + baseUri + " for engine " + getEngineId());
				
			}
		}
		
		return baseUri;
	}
	
	@Override
	public String getDataTypes(String uri) {
		return this.owlHelper.getDataTypes(uri);
	}
	
	@Override
	public Map<String, String> getDataTypes(String... uris) {
		return this.owlHelper.getDataTypes(uris);
	}
	
	@Override
	public String getAdtlDataTypes(String uri) {
		return this.owlHelper.getAdtlDataTypes(uri);
	}
	
	@Override
	public Map<String, String> getAdtlDataTypes(String... uris) {
		return this.owlHelper.getAdtlDataTypes(uris);
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
	public SelectQueryStruct getDatabaseQueryStruct() {
		SelectQueryStruct qs = new SelectQueryStruct();

		// query to get all the concepts and properties for selectors
		String getSelectorsInformation = "SELECT DISTINCT ?conceptualConcept ?conceptualProperty WHERE { "
				+ "{?concept2 <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?concept2 <http://semoss.org/ontologies/Relation/Conceptual> ?conceptualConcept }"
				+ "OPTIONAL {"
				+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + CONTAINS_BASE_URI + "> } "
				+ "{?concept2 <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property } "
				+ "{?property <http://semoss.org/ontologies/Relation/Conceptual> ?conceptualProperty }"
				+ "}" // END OPTIONAL
				+ "}"; // END WHERE

		// execute the query and loop through and add it into the QS
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(baseDataEngine, getSelectorsInformation);
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
		String getRelationshipsInformation = "SELECT DISTINCT ?fromConceptualConcept ?toConceptualConcept WHERE { "
				+ "{?fromConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
				+ "{?toConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
				+ "{?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} "
				+ "{?fromConcept ?rel ?toConcept} "
				+ "{?fromConcept <http://semoss.org/ontologies/Relation/Conceptual> ?fromConceptualConcept }"
				+ "{?toConcept <http://semoss.org/ontologies/Relation/Conceptual> ?toConceptualConcept }"
				+ "}"; // END WHERE

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
		SelectQueryStruct qs = getDatabaseQueryStruct();

		// need to store the edges in a way that we can easily get them
		Map<String, SEMOSSVertex> vertStore = new Hashtable<String, SEMOSSVertex>();

		// first get all the nodes
		List<IQuerySelector> vertices = qs.getSelectors();
		for(IQuerySelector selector : vertices) {
			// database query struct always returns columns
			QueryColumnSelector cSelector = (QueryColumnSelector) selector;
			String concept = cSelector.getTable();
			String prop = cSelector.getColumn();
			
			SEMOSSVertex vert = null;
			if(vertStore.containsKey(concept)) {
				// we got the vertex before
				vert = vertStore.get(concept);
			} else {
				// new vertex
				vert = new SEMOSSVertex("http://semoss.org/ontologies/Concept/" + concept);
				vert.putProperty("PhysicalName", concept);
				
				// add to nodes map
				vertStore.put(concept, vert);
				// add to nodes list
				nodes.add(vert);
			}
			
			// see if we need to add the property
			if(!prop.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				vert.setProperty("http://semoss.org/ontologies/Relation/Contains/" + prop, prop);
			}
		}
		
		// now go through all the relations
		// remember, the map is the {fromConcept -> { joinType -> [toConcept1, toConcept2] } }
		// need to iterate through to get fromConcept -> toConcept and make a unique edge for each
		// remember the edge names are not every actually used
		Map<String, Map<String, List>> relations = qs.getRelations();
		for(String fromConcept : relations.keySet()) {
			// get the from-vertex
			SEMOSSVertex fromVert = vertStore.get(fromConcept);

			// need to iterate through the join types to get to the toConcpets
			Map<String, List> joinsMap = relations.get(fromConcept);
			for(String joinType : joinsMap.keySet()) {
				List<Object> toConcepts = joinsMap.get(joinType);
				
				for(Object toConcept : toConcepts) {
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
	
	// load the prop file
	public void setProp(Properties prop) {
		this.prop = prop;
	}
	
	
	////////////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Methods that exist only to automate changes to databases
	 */
	
	@Deprecated
	private void updateToPixelInsights() {
		InsightsConverter2 converter = new InsightsConverter2(this);
		try {
			converter.modifyInsightsDatabase();
		} catch (IOException e) {
			lOGGER.error(e.getMessage());
		}
	}
	
	@Deprecated
	private void updateExploreInstanceQuery(RDBMSNativeEngine insightRDBMS) {
		try {
			// if solr doesn't have this engine
			// do not add anything yet
			// let it get added later
			if(!SolrIndexEngine.getInstance().containsEngine(this.engineId)) {
				return;
			}
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e3) {
			e3.printStackTrace();
		}
		boolean tableExists = false;
		ResultSet rs = null;
		try {
			rs = insightRDBMS.getConnectionMetadata().getTables(null, null, "QUESTION_ID", null);
			if (rs.next()) {
				  tableExists = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
			} catch(SQLException e) {
				e.printStackTrace();
			}
		}
		
		if(tableExists) {
			String exploreLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\ExploreInstanceDefaultWidget.json";
			File exploreF = new File(exploreLoc);
			if(!exploreF.exists()) {
				// ughhh... cant do anything for ya buddy
				return;
			}
			String newPixel = "AddPanel(0); Panel ( 0 ) | SetPanelView ( \"param\" , \"<encode> {\"json\":";
			try {
				newPixel += new String(Files.readAllBytes(exploreF.toPath())).replaceAll("\n|\r|\t", "").replaceAll("\\s\\s+", "").replace("<<ENGINE>>", this.engineId);
			} catch (IOException e2) {
				// can't help ya
				return;
			}
			newPixel += "} </encode>\" ) ;";
			
			// for debugging... delete from question_id where question_name = 'New Explore an Instance'
			InsightAdministrator admin = new InsightAdministrator(insightRDBMS);
			IRawSelectWrapper it1 = null;
			String oldId = null;
			try {
				it1 = WrapperManager.getInstance().getRawWrapper(insightRDBMS, "select id from question_id where "
						+ "question_name = 'Explore an instance of a selected node type' OR question_name = 'Explore an Instance(s) of a Selected Node'" );
				while(it1.hasNext()) {
					// drop the old insight
					oldId = it1.next().getValues()[0].toString();
					admin.dropInsight(oldId);
					try {
						List<String> rList = new Vector<String>();
						rList.add(this.engineId + "_" + oldId);
						SolrIndexEngine.getInstance().removeInsight(rList);
					} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
							| IOException e1) {
						e1.printStackTrace();
					}
				}
			} catch(Exception e) {
				// if we have a db that doesn't actually have this table (forms, local master, etc.)
			} finally {
				if(it1 != null) {
					it1.cleanUp();
				}
			}
			
			IRawSelectWrapper it2 = null;
			try {
				it2 = WrapperManager.getInstance().getRawWrapper(insightRDBMS, "select id from question_id where question_name = 'Explore an Instance(s) of a Selected Node'");
				if(!it2.hasNext()) {
					// add the new insight
					String insightIdToSave = admin.addInsight("Explore an instance of a selected node type", "Graph", new String[]{newPixel});
		
					if(oldId != null) {
						insightRDBMS.insertData("UPDATE QUESTION_ID SET ID='" + oldId + "' WHERE ID='" + insightIdToSave + "'");
						insightIdToSave = oldId;
					}
					
					Map<String, Object> solrInsights = new HashMap<>();
					DateFormat dateFormat = SolrIndexEngine.getDateFormat();
					Date date = new Date();
					String currDate = dateFormat.format(date);
					solrInsights.put(SolrIndexEngine.APP_ID, this.engineId);
					solrInsights.put(SolrIndexEngine.APP_NAME, this.engineName);
					solrInsights.put(SolrIndexEngine.APP_INSIGHT_ID, insightIdToSave);
					solrInsights.put(SolrIndexEngine.STORAGE_NAME, "Explore an instance of a selected node type");
					solrInsights.put(SolrIndexEngine.TAGS, "Explore");
					solrInsights.put(SolrIndexEngine.LAYOUT, "Graph");
					solrInsights.put(SolrIndexEngine.CREATED_ON, currDate);
					solrInsights.put(SolrIndexEngine.MODIFIED_ON, currDate);
					solrInsights.put(SolrIndexEngine.LAST_VIEWED_ON, currDate);
					solrInsights.put(SolrIndexEngine.DESCRIPTION, "");
					solrInsights.put(SolrIndexEngine.USER_ID, "Default");

					try {
						SolrIndexEngine.getInstance().addInsight(this.engineId + "_" + insightIdToSave, solrInsights);
						SolrUtility.addAppToSolr(this.engineId);
					} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
							| IOException e1) {
						e1.printStackTrace();
					}
				} else {
					// right now, delete and re add it
					// only need to to do this on the recipe
					// no need to modify solr
					oldId = it2.next().getValues()[0].toString();
					admin.dropInsight(oldId);
					
					// add the new insight
					// and modify the id
					String insightIdToSave = admin.addInsight("Explore an instance of a selected node type", "Graph", new String[]{newPixel});
					insightRDBMS.insertData("UPDATE QUESTION_ID SET ID='" + oldId + "' WHERE ID='" + insightIdToSave + "'");
				}
			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				if(it2 != null) {
					it2.cleanUp();
				}
			}
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Testing
	 */
	
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
	
}
