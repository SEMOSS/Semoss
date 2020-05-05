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
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDFS;

import com.google.gson.Gson;

import prerna.auth.utils.SecurityUpdateUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.AuditDatabase;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.SparqlInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.sablecc2.reactor.legacy.playsheets.LegacyInsightDatabaseUtility;
import prerna.security.SnowApi;
import prerna.ui.components.RDFEngineHelper;
import prerna.util.CSVToOwlMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SemossClassloader;
import prerna.util.Utility;

/**
 * An Abstract Engine that sets up the base constructs needed to create an
 * engine.
 */
public abstract class AbstractEngine implements IEngine {

	/**
	 * Static members
	 */
	
	public static final String USE_FILE = "USE_FILE";
	public static final String DATA_FILE = "DATA_FILE";
	
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	private static final Logger LOGGER = LogManager.getLogger(AbstractEngine.class.getName());
	private static final String SEMOSS_URI = "http://semoss.org/ontologies/";
	private static final String CONTAINS_BASE_URI = SEMOSS_URI + Constants.DEFAULT_RELATION_CLASS + "/Contains";
	private static final String GET_ALL_INSIGHTS_QUERY = "SELECT DISTINCT ID, QUESTION_ORDER FROM QUESTION_ID ORDER BY ID";
	private static final String GET_ALL_PERSPECTIVES_QUERY = "SELECT DISTINCT QUESTION_PERSPECTIVE FROM QUESTION_ID ORDER BY QUESTION_PERSPECTIVE";
	private static final String QUESTION_PARAM_KEY = "@QUESTION_VALUE@";
	private static final String GET_INSIGHT_INFO_QUERY = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_MAKEUP, QUESTION_PERSPECTIVE, QUESTION_LAYOUT, QUESTION_ORDER, DATA_TABLE_ALIGN, QUESTION_DATA_MAKER, CACHEABLE, QUESTION_PKQL FROM QUESTION_ID WHERE ID IN (" + QUESTION_PARAM_KEY + ") ORDER BY QUESTION_ORDER";
	private static final String GET_BASE_URI_FROM_OWL = "SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1";

	/**
	 * Class members
	 */
	
	protected String baseFolder = null;
	protected String propFile = null;
	protected Properties prop = null;
	
	protected String engineId = null;
	protected String engineName = null;
	
	protected Properties generalEngineProp = null;
	protected Properties ontoProp = null;

	/**
	 * OWL database
	 */
	private MetaHelper owlHelper = null;
	protected RDFFileSesameEngine baseDataEngine;
	private String owlFileLocation;
	private String baseUri;
	
	// this is optional owl schemas
//	List<RDFFileSesameEngine> additionalOwlEngine;
//	List<String> additionalOwlFiles;
//	List<String> additionalJsPlumbFiles;
	
	/**
	 * Insight rdbms database
	 */
	protected RDBMSNativeEngine insightRdbms;
	private String insightDatabaseLoc;

	private Hashtable<String, String> baseDataHash;

	/**
	 * This is used for tracking audit modifications 
	 */
	private AuditDatabase auditDatabase = null;

	/**
	 * This is if we have a connection but no OWL or INSIGHTS DB
	 */
	private boolean isBasic = false;

	/**
	 * Hash for the specific engine reactors
	 */
	private Map <String, Class> dbSpecificHash = null;
	
	/**
	 * Custom class loader
	 */
	private SemossClassloader engineClassLoader = new SemossClassloader(this.getClass().getClassLoader());

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
				LOGGER.info("Opening DB - " + engineName);
				this.prop = Utility.loadProperties(propFile);
			}
			if(this.prop != null) {
				
				// do the piece of encrypting here
				boolean encryptFile = false;
				if(DIHelper.getInstance().getProperty("ENCRYPT") != null)
					encryptFile = DIHelper.getInstance().getProperty("ENCRYPT").equalsIgnoreCase("true")?true:false;
				
				if(encryptFile)
				{
					if(this.prop.containsKey("PASSWORD") && !((String)this.prop.get("PASSWORD")).equalsIgnoreCase("encrypted password"))
						prop = encryptPropFile(propFile);
				}
				
				
				// grab the main properties
				this.engineId = prop.getProperty(Constants.ENGINE);
				this.engineName = prop.getProperty(Constants.ENGINE_ALIAS);
	
				// 
				// load the rdbms insights db
				loadInsightsRdbms();
				
				// load the rdf owl db
				String owlFile = SmssUtilities.getOwlFile(this.prop).getAbsolutePath();
				if(owlFile != null) {
					File owlF = new File(owlFile);
					// need a check here to say if I am asking this to be remade or keep what it is
					if(!owlF.exists() || owlFile.equalsIgnoreCase("REMAKE")) {
						// the process of remake will start here
						// see if the usefile is there
						if(this.prop.containsKey(DATA_FILE)) {
							String owlFileName = null;
							String dataFile = SmssUtilities.getDataFile(this.prop).getAbsolutePath();
							if(owlFile.equals("REMAKE")) {
								// we will make the name
								File dF = new File(dataFile);
								owlFileName = this.engineName + "_OWL.OWL";
								owlFile = dF.getParentFile() + DIR_SEPARATOR + owlFileName;
							} else {
								owlFileName = FilenameUtils.getName(owlFile);
							}
							
							owlFile = generateOwlFromFlatFile(dataFile, owlFile, owlFileName);
						} else {
							owlFile = null;
						}
					}
					// set the owl file
					if(owlFile != null) {
						owlFile = SmssUtilities.getOwlFile(this.prop).getAbsolutePath();
						LOGGER.info("Loading OWL: " + Utility.cleanLogString(owlFile));
						setOWL(owlFile);
					}
				}
				// load properties object for db
				File engineProps = SmssUtilities.getEngineProperties(this.prop);
				if (engineProps != null) {
					this.generalEngineProp = Utility.loadProperties(engineProps.getAbsolutePath());
				}
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		} 
	}
	
	/**
	 * Generate the OWL based on a flat file
	 * @param dataFile
	 * @param owlFile
	 * @param owlFileName
	 * @return
	 */
	protected String generateOwlFromFlatFile(String dataFile, String owlFile, String owlFileName) {
		CSVToOwlMaker maker = new CSVToOwlMaker();
		maker.makeFlatOwl(dataFile, owlFile, getEngineType(), true);
		if(owlFile.equals("REMAKE")) {
			Utility.changePropMapFileValue(this.propFile, Constants.OWL, owlFileName);
		}
		return owlFile;
	}

	@Override
	public void closeDB() {
		if(this.baseDataEngine != null) {
			LOGGER.debug("closing its owl engine ");
			this.baseDataEngine.closeDB();
		}
		if(this.insightRdbms != null) {
			LOGGER.debug("closing its insight engine ");
			this.insightRdbms.closeDB();
		}
		if (auditDatabase != null) {
			auditDatabase.close();
		}
	}
	
	@Override
	public String getProperty(String key) {
		String retProp = null;

		LOGGER.debug("Property is " + key + "]");
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
			LOGGER.debug("Writing to file " + propFile);
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
	 * Load the insights database
	 */
	protected void loadInsightsRdbms() {
		// load the rdbms insights db
		this.insightDatabaseLoc = SmssUtilities.getInsightsRdbmsFile(this.prop).getAbsolutePath();
		
		// if it is not defined directly in the smss
		// we will not create an insights database
		if(insightDatabaseLoc != null) {
			this.insightRdbms = EngineInsightsHelper.loadInsightsEngine(this.prop, LOGGER);
		}
		
		// yay! even more updates
		if(this.insightRdbms != null) {
			// TODO: this is new code to convert
			// TODO: this is new code to convert
			// TODO: this is new code to convert
			// TODO: this is new code to convert
//			String updatedInsights = prop.getProperty(Constants.PIXEL_UPDATE);
//			if(updatedInsights == null) {
//				updateToPixelInsights();
//				Utility.updateSMSSFile(propFile, Constants.PIXEL_UPDATE, "true");
//			} else if(!Boolean.parseBoolean(updatedInsights)){
//				updateToPixelInsights();
//				Utility.changePropMapFileValue(propFile, Constants.PIXEL_UPDATE, "true");
//			}
			
			// update explore an instance query!!!
			updateExploreInstanceQuery(this.insightRdbms);
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
	
	@Override
	public void setBaseDataEngine(RDFFileSesameEngine baseDataEngine) {
		this.baseDataEngine = baseDataEngine;
		this.owlHelper = new MetaHelper(this.baseDataEngine, getEngineType(), this.engineId);
	}

	/**
	 * Sets the base data hash
	 * @param h		Hashtable - The base data hash that this is being set to
	 */
	public void setBaseHash(Hashtable h) {
		LOGGER.debug(this.engineId + " Set the Base Data Hash ");
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
		this.owlFileLocation = owl;
		createBaseRelationEngine();
		this.owlHelper = new MetaHelper(baseDataEngine, getEngineType(), this.engineId);
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
		if (owlFileLocation == null) {
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			owlFileLocation = baseFolder + "/db/" + getEngineId() + "/" + getEngineId()	+ "_OWL.OWL";
		}
		baseRelEngine.setFileName(owlFileLocation);
		baseRelEngine.openDB(null);
		if(prop != null) {
			addProperty(Constants.OWL, owlFileLocation);
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
		return this.owlFileLocation;
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
		LOGGER.debug("Committing base data engine of " + this.engineId);
		this.baseDataEngine.commit();
	}

	public Vector<String> getConcepts() {
		if(owlHelper == null) {
			return null;
		}
		return owlHelper.getConcepts();
	}
	
	/**
	 * Runs a select query on the base data engine of this engine
	 */
	public Object execOntoSelectQuery(String query) {
		LOGGER.debug("Running select query on base data engine of " + this.engineId);
		LOGGER.debug("Query is " + query);
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
			LOGGER.error(e);
		} catch (NoSuchMethodException e) {
			LOGGER.error(e);
		} catch (IllegalArgumentException e) {
			LOGGER.error(e);
		} catch (IllegalAccessException e) {
			LOGGER.error(e);
		} catch (InvocationTargetException e) {
			LOGGER.error(e);
		}
		return ret;
	}

	public void deleteDB() {
		LOGGER.debug("closing " + this.engineName);
		this.closeDB();

		File insightFile = SmssUtilities.getInsightsRdbmsFile(this.prop);
		File owlFile = SmssUtilities.getOwlFile(this.prop);
		File engineFolder = insightFile.getParentFile();
		String folderName = engineFolder.getName();
		if(owlFile != null && owlFile.exists()) {
			System.out.println("Deleting owl file " + owlFile.getAbsolutePath());
			try {
				FileUtils.forceDelete(owlFile);
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
		if(insightFile != null && insightFile.exists()) {
			System.out.println("Deleting insight file " + insightFile.getAbsolutePath());
			try {
				FileUtils.forceDelete(insightFile);
			} catch(IOException e) {
				e.printStackTrace();
			}
		}

		//this check is to ensure we are deleting the right folder.
		LOGGER.debug("checking folder name is matching up : " + folderName + " against " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		if(folderName.equals(SmssUtilities.getUniqueName(this.engineName, this.engineId))) {
			LOGGER.debug("folder getting deleted is " + engineFolder.getAbsolutePath());
			try {
				FileUtils.deleteDirectory(engineFolder);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		LOGGER.debug("Deleting smss " + this.propFile);
		File smssFile = new File(this.propFile);
		try {
			FileUtils.forceDelete(smssFile);
		} catch(IOException e) {
			e.printStackTrace();
		}

		//remove from DIHelper
		String engineNames = (String)DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		engineNames = engineNames.replace(";" + this.engineId, "");
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
		DIHelper.getInstance().removeLocalProperty(this.engineId);
	}
	
	@Override
	public RDBMSNativeEngine getInsightDatabase() {
		return this.insightRdbms;
	}
	
	@Override
	public void setInsightDatabase(RDBMSNativeEngine insightDatabase) {
		this.insightRdbms = insightDatabase;
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
		Vector<String> perspectives = Utility.getVectorOfReturn(GET_ALL_PERSPECTIVES_QUERY, insightRdbms, false);
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
		return Utility.getVectorOfReturn(insightsInPerspective, insightRdbms, false);
	}

	@Override
	public Vector<String> getInsights() {
		return Utility.getVectorOfReturn(GET_ALL_INSIGHTS_QUERY, insightRdbms, false);
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
			LOGGER.info("Running insights query " + Utility.cleanLogString(query));
			
			IRawSelectWrapper wrap = null;
			try {
				wrap = WrapperManager.getInstance().getRawWrapper(insightRdbms, query);
				while (wrap.hasNext()) {
					IHeadersDataRow dataRow = wrap.next();
					Object[] values = dataRow.getValues();
//					Object[] rawValues = dataRow.getRawValues();

					String rdbmsId = values[0] + "";
					String insightName = values[1] + "";
					
					Clob insightMakeup = (Clob) values[2];
					InputStream insightMakeupIs = null;
					if(insightMakeup != null) {
						try {
							insightMakeupIs = insightMakeup.getAsciiStream();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					String layout = values[4] + "";
					String dataTableAlign = values[6] + "";
					String dataMakerName = values[7] + "";
					boolean cacheable = (boolean) values[8];
					Object[] pixel = null;
					// need to know if we have an array
					// or a clob
					if(insightRdbms.getQueryUtil().allowArrayDatatype()) {
						pixel = (Object[]) values[9];
					} else {
						Clob pixelArray = (Clob) values[9];
						InputStream pixelArrayIs = null;
						if(pixelArray != null) {
							try {
								pixelArrayIs = pixelArray.getAsciiStream();
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}
						// flush input stream to string
						Gson gson = new Gson();
						InputStreamReader reader = new InputStreamReader(pixelArrayIs);
						pixel = gson.fromJson(reader, String[].class);
					}
					
					String perspective = values[3] + "";
					String order = values[5] + "";
					
					Insight in = null;
					if(pixel == null || pixel.length == 0) {
						in = new OldInsight(this, dataMakerName, layout);
						in.setRdbmsId(rdbmsId);
						in.setInsightName(insightName);
						((OldInsight) in).setOutput(layout);
						((OldInsight) in).setMakeup(insightMakeupIs);
//						in.setPerspective(perspective);
//						in.setOrder(order);
						((OldInsight) in).setDataTableAlign(dataTableAlign);
						// adding semoss parameters to insight
						((OldInsight) in).setInsightParameters(LegacyInsightDatabaseUtility.getParamsFromInsightId(this.insightRdbms, rdbmsId));
						in.setIsOldInsight(true);
					} else {
						in = new Insight(this.engineId, this.engineName, rdbmsId, cacheable);
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
			} catch (Exception e1) {
				e1.printStackTrace();
			} finally {
				if(wrap != null) {
					wrap.cleanUp();
				}
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
		ISelectWrapper wrap = null;
		try {
			wrap = WrapperManager.getInstance().getSWrapper(insightRdbms, "SCRIPT");
			String[] names = wrap.getVariables();
			while(wrap.hasNext()) {
				ISelectStatement ss = wrap.next();
				System.out.println(ss.getRPropHash().toString());//
				stringBuilder.append(ss.getVar(names[0]) + "").append("%!%");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrap != null) {
				wrap.cleanUp();
			}
		}
		return stringBuilder.toString();
	}
	
	@Override
	public String getNodeBaseUri(){
		if(baseUri == null) {
			IRawSelectWrapper wrap = null;
			try {
				wrap = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, GET_BASE_URI_FROM_OWL);
				if(wrap.hasNext()) {
					IHeadersDataRow data = wrap.next();
					baseUri = data.getRawValues()[0] + "";
					LOGGER.info("Got base uri from owl " + this.baseUri + " for engine " + getEngineId() + " : " + getEngineName());
				}
				if(baseUri == null){
					baseUri = Constants.CONCEPT_URI;
					LOGGER.info("couldn't get base uri from owl... defaulting to " + baseUri + " for engine " + getEngineId() + " : " + getEngineName());
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if(wrap != null) {
					wrap.cleanUp();
				}
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
	
	/**
	 * This method will return a query struct which when interpreted would produce a query to 
	 * get all the data within the engine.  Will currently assume all joins to be inner.join
	 * @return
	 */
	public SelectQueryStruct getDatabaseQueryStruct() {
		SelectQueryStruct qs = new SelectQueryStruct();

		// query to get all the concepts and properties for selectors
		String getSelectorsInformation = "SELECT DISTINCT ?conceptualConcept ?property WHERE { "
				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?concept <http://semoss.org/ontologies/Relation/Conceptual> ?conceptualConcept }"
				+ "OPTIONAL {"
					+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + CONTAINS_BASE_URI + "> } "
					+ "{?concept <" + OWL.DATATYPEPROPERTY.toString() + "> ?property } "
					+ "{?property <http://semoss.org/ontologies/Relation/Conceptual> ?conceptualProperty }"
				+ "}" // END OPTIONAL
				+ "}"; // END WHERE

		// execute the query and loop through and add it into the QS
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, getSelectorsInformation);
			// we will keep a set of the concepts such that we know when we need to append a PRIM_KEY_PLACEHOLDER
			Set<String> conceptSet = new HashSet<String>();
			while(wrapper.hasNext()) {
				IHeadersDataRow hrow = wrapper.next();
				Object[] row = hrow.getValues();
				Object[] raw = hrow.getRawValues();
				if(raw[0].toString().equals("http://semoss.org/ontologies/Concept")) {
					continue;
				}
				
				String concept = row[0].toString();
				if(!conceptSet.contains(concept)) {
					qs.addSelector(new QueryColumnSelector(concept));
				}
				
				Object property = raw[1];
				if(property != null && !property.toString().isEmpty()) {
					qs.addSelector(new QueryColumnSelector(concept + "__" + Utility.getClassName(property.toString())));
				}
			}
			// no need to keep this anymore
			conceptSet.clear();
			conceptSet = null;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}

		// query to get all the relationships 
		String getRelationshipsInformation = "SELECT DISTINCT ?fromConceptualConcept ?toConceptualConcept WHERE { "
				+ "{?fromConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
				+ "{?toConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
				+ "{?rel <" + RDFS.SUBPROPERTYOF.toString() + "> <http://semoss.org/ontologies/Relation>} "
				+ "{?fromConcept ?rel ?toConcept} "
				+ "{?fromConcept <http://semoss.org/ontologies/Relation/Conceptual> ?fromConceptualConcept }"
				+ "{?toConcept <http://semoss.org/ontologies/Relation/Conceptual> ?toConceptualConcept }"
				+ "}"; // END WHERE

		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, getRelationshipsInformation);
			while(wrapper.hasNext()) {
				IHeadersDataRow hrow = wrapper.next();
				Object[] row = hrow.getValues();
				String fromConcept = row[0].toString();
				String toConcept = row[1].toString();
				qs.addRelation(fromConcept, toConcept, "inner.join");
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return qs;
	}
	
	
	/**
	 * This will return the metamodel object used to view on dagger for an engine
	 * @return
	 */
	public Map<String, Object[]> getMetamodel() {
		return owlHelper.getMetamodel();
	}
	
	// load the prop file
	@Override
	public void setProp(Properties prop) {
		this.prop = prop;
	}
	
	@Override
	public Properties getProp() {
		return this.prop;
	}
	
	/**
	 * Get an audit database for making modifications in a database
	 */
	@Override
	public synchronized AuditDatabase generateAudit() {
		if(this.auditDatabase == null) {
			this.auditDatabase = new AuditDatabase();
			this.auditDatabase.init(this, this.engineId, this.engineName);
		}
		return this.auditDatabase;
	}
	
	/*
	 * NEW PIXEL TO REPLACE CONCEPTUAL NAMES
	 */
	
	@Override
	public List<String> getPixelConcepts() {
		return owlHelper.getPixelConcepts();
	}
	
	@Override
	public List<String> getPixelSelectors(String conceptPixelName) {
		return owlHelper.getPixelSelectors(conceptPixelName);
	}
	
	@Override
	public List<String> getPropertyPixelSelectors(String conceptPixelName) {
		return owlHelper.getPropertyPixelSelectors(conceptPixelName);
	}
	
	@Override
	public List<String> getPhysicalConcepts() {
		return owlHelper.getPhysicalConcepts();
	}
	
	@Override
	public List<String[]> getPhysicalRelationships() {
		return owlHelper.getPhysicalRelationships();
	}
	
	public List<String> getPropertyUris4PhysicalUri(String physicalUri) {
		return owlHelper.getPropertyUris4PhysicalUri(physicalUri);
	}
	
	@Override
	public String getPhysicalUriFromPixelSelector(String pixelSelector) {
		return owlHelper.getPhysicalUriFromPixelSelector(pixelSelector);
	}
	
	@Override
	@Deprecated
	public String getPixelUriFromPhysicalUri(String physicalUri) {
		return owlHelper.getPixelUriFromPhysicalUri(physicalUri);
	}

	@Override
	public String getConceptPixelUriFromPhysicalUri(String conceptPhysicalUri) {
		return owlHelper.getConceptPixelUriFromPhysicalUri(conceptPhysicalUri);
	}
	
	@Override
	public String getPropertyPixelUriFromPhysicalUri(String conceptPhysicalUri, String propertyPhysicalUri) {
		return owlHelper.getPropertyPixelUriFromPhysicalUri(conceptPhysicalUri, propertyPhysicalUri);
	}
	
	@Override
	public String getPixelSelectorFromPhysicalUri(String physicalUri) {
		return owlHelper.getPixelSelectorFromPhysicalUri(physicalUri);
	}
	
	@Override
	public String getConceptualName(String physicalUri) {
		return this.owlHelper.getConceptualName(physicalUri);
	}
	
	@Override
	public Set<String> getLogicalNames(String physicalUri) {
		return this.owlHelper.getLogicalNames(physicalUri);
	}
	
	@Override
	public String getDescription(String physicalUri) {
		return this.owlHelper.getDescription(physicalUri);
	}
	
	@Override
	@Deprecated
	public String getLegacyPrimKey4Table(String physicalUri) {
		return this.owlHelper.getLegacyPrimKey4Table(physicalUri);
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Methods that exist only to automate changes to databases
	 */
	
	@Deprecated
	private void updateExploreInstanceQuery(RDBMSNativeEngine insightRDBMS) {
		// if solr doesn't have this engine
		// do not add anything yet
		// let it get added later
		if(!SecurityUpdateUtils.containsEngineId(this.engineId) 
				|| this.engineId.equals(Constants.LOCAL_MASTER_DB_NAME)
				|| this.engineId.equals(Constants.SECURITY_DB)) {
			return;
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
			String exploreLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "ExploreInstanceDefaultWidget.json";
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
				it1 = WrapperManager.getInstance().getRawWrapper(insightRDBMS, "select id from question_id where question_name='Explore an instance of a selected node type'");
				while(it1.hasNext()) {
					// drop the old insight
					oldId = it1.next().getValues()[0].toString();
				}
			} catch(Exception e) {
				// if we have a db that doesn't actually have this table (forms, local master, etc.)
			} finally {
				if(it1 != null) {
					it1.cleanUp();
				}
			}
			
			if(oldId != null) {
				// update with the latest explore an instance
				admin.updateInsight(oldId, "Explore an instance of a selected node type", "Graph", new String[]{newPixel});
			}
		}
	}
	
	
	////////////////////////////////////////////////////////////////////////////////////
	///////////////////// Load engine specific reactors ///////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	
	public IReactor getReactor(String className) 
	{	
		// try to get to see if this class already exists
		// no need to recreate if it does
		
		// get the prop file and find the parent

		File dbDirectory = new File(propFile);
		System.err.println(".");

		String dbFolder = engineName + "_" + dbDirectory.getParent()+ "/" + engineId;

		dbFolder = propFile.replaceAll(".smss", "");
		
		IReactor retReac = null;
		//String key = db + "." + insightId ;
		String key = engineId ;
		if(dbSpecificHash == null)
			dbSpecificHash = new HashMap<String, Class>();
		
		int randomNum = 0;
		//ReactorFactory.compileCache.remove(engineId);
		if(!ReactorFactory.compileCache.containsKey(engineId))
		{
			String classesFolder = dbFolder + "/version/classes";
			File classesDir = new File(classesFolder);
			if(classesDir.exists() && classesDir.isDirectory())
			{
				try {
					//FileUtils.cleanDirectory(classesDir);
					//classesDir.mkdir();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			int status = Utility.compileJava(dbFolder +"/version", getCP());
			if(status == 0)
			{
				ReactorFactory.compileCache.put(engineId, Boolean.TRUE);
				
				if(ReactorFactory.randomNumberAdder.containsKey(engineId))
					randomNum = ReactorFactory.randomNumberAdder.get(engineId);				
				randomNum++;
				ReactorFactory.randomNumberAdder.put(engineId, randomNum);
				
				// add it to the key so we can reload
				key = engineId + randomNum;
	
				dbSpecificHash.clear();
			}
			// avoid loading everytime since it is an error
		}

		
		if(dbSpecificHash.size() == 0)
		{
			//compileJava(insightDirector.getParentFile().getAbsolutePath());
			// delete the classes directory first
			
			dbSpecificHash = Utility.loadReactors(dbFolder + "/version", key);
			dbSpecificHash.put("loaded", "TRUE".getClass());
		}
		try
		{
			if(dbSpecificHash.containsKey(className.toUpperCase())) {
				Class thisReactorClass = dbSpecificHash.get(className.toUpperCase());
				retReac = (IReactor) thisReactorClass.newInstance();
				return retReac;
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
			
		return retReac;
	}


	public IReactor getReactor(String className, SemossClassloader customLoader) 
	{	
		// try to get to see if this class already exists
		// no need to recreate if it does
		SemossClassloader cl = engineClassLoader;
		if(customLoader != null)
			cl = customLoader;
				
		// get the prop file and find the parent
		File dbDirectory = new File(propFile);
		//System.err.println("..");

		String dbFolder = engineName + "_" + dbDirectory.getParent()+ "/" + engineId;

		dbFolder = propFile.replaceAll(".smss", "");
		
		cl.setFolder(dbFolder + "/version/classes");
		
		IReactor retReac = null;
		//String key = db + "." + insightId ;
		String key = engineId ;
		
		int randomNum = 0;
		//ReactorFactory.compileCache.remove(engineId);
		if(!ReactorFactory.compileCache.containsKey(engineId))
		{
			engineClassLoader = new SemossClassloader(this.getClass().getClassLoader());
			cl = engineClassLoader;
			cl.uncommitEngine(engineId);
			// if it 
			
			String classesFolder = dbFolder + "/version/classes";
			
			File classesDir = new File(classesFolder);
			if(classesDir.exists() && classesDir.isDirectory())
			{
				try {
					//FileUtils.cleanDirectory(classesDir);
					//classesDir.mkdir();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			int status = Utility.compileJava(dbFolder +"/version", getCP());
			//if(status == 0) // error or not I going to mark it. If we want to recompile. tell the system to recompile
			{
				ReactorFactory.compileCache.put(engineId, Boolean.TRUE);
				
				if(ReactorFactory.randomNumberAdder.containsKey(engineId))
					randomNum = ReactorFactory.randomNumberAdder.get(engineId);				
				randomNum++;
				ReactorFactory.randomNumberAdder.put(engineId, randomNum);
				
				// add it to the key so we can reload
				key = engineId + randomNum;	
			}
			// avoid loading everytime since it is an error
		}

		
		if(!cl.isCommitted(engineId))
		{
			//compileJava(insightDirector.getParentFile().getAbsolutePath());
			// delete the classes directory first
			dbSpecificHash = Utility.loadReactors(dbFolder + "/version", key, cl);
			cl.commitEngine(engineId);
		}
		try
		{
			if(dbSpecificHash.containsKey(className.toUpperCase())) {
				Class thisReactorClass = dbSpecificHash.get(className.toUpperCase());
				retReac = (IReactor) thisReactorClass.newInstance();
				return retReac;
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return retReac;
	}

	private String getCP()
	{
		String envClassPath = null;
		
		StringBuilder retClassPath = new StringBuilder("");
		ClassLoader cl = getClass().getClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
        	String thisURL = URLDecoder.decode((url.getFile().replaceFirst("/", "")));
        	if(thisURL.endsWith("/"))
        		thisURL = thisURL.substring(0, thisURL.length()-1);

        	retClassPath
        		//.append("\"")
        		.append(thisURL)
        		//.append("\"")
        		.append(";");
        	
        }
        envClassPath = "\"" + retClassPath.toString() + "\"";
        
        return envClassPath;
	}

	public String decryptPass(String propFile, boolean insight)
	{
		String retString = null;
		try {
			Properties prop = new Properties();
			File propF = new File(propFile);
			prop.load(new FileInputStream(propF));

			String engineAlias = prop.getProperty("ENGINE_ALIAS");
			if(engineAlias != null)
				engineAlias = engineAlias + "__";
			else
				engineAlias = "";

			String dir = propF.getParent() + java.nio.file.FileSystems.getDefault().getSeparator() + engineAlias + prop.getProperty("ENGINE");
			String passwordFileName = dir + java.nio.file.FileSystems.getDefault().getSeparator() + ".pass";
			if(insight)
				passwordFileName = dir + java.nio.file.FileSystems.getDefault().getSeparator() + ".insight";
			
			String creationTime = Files.getAttribute(Paths.get(propFile), "creationTime") + "";		

			
			
			File inputFile = new File(passwordFileName);
			if(inputFile.exists()) // if nothing is there return null
			{
				SnowApi snow = new SnowApi();			
				retString = snow.decryptMessage(creationTime, passwordFileName);
			}			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return retString;
		
	}


	public Properties encryptPropFile(String propFile)
	{
		try {
			Properties prop = new Properties();
			File propF = new File(propFile);
			prop.load(new FileInputStream(propF));

			Enumeration keys = prop.keys();
			
			String passToEncrypt = null;
			String insightPassToEncrypt = null;
			
			while(keys.hasMoreElements())
			{
				String thisKey = (String)keys.nextElement();
				if(thisKey.equalsIgnoreCase("password"))
				{
					passToEncrypt = prop.getProperty(thisKey);
					if(!passToEncrypt.equalsIgnoreCase("encrypted password"))
						prop.put(thisKey, "encrypted password");
				}
				if(thisKey.equalsIgnoreCase("insight_password"))
				{
					insightPassToEncrypt = prop.getProperty(thisKey);
					if(!insightPassToEncrypt.equalsIgnoreCase("encrypted password"))
						prop.put(thisKey, "encrypted password");
				}
			}	
			
			if(insightPassToEncrypt == null)
			{
				prop.put("insight_password", "encrypted password");
				insightPassToEncrypt = "";
			}
			
			if(passToEncrypt != null && !passToEncrypt.equalsIgnoreCase("encrypted password") || (insightPassToEncrypt != null && insightPassToEncrypt.equalsIgnoreCase("encrypted password")))
			{	
				// add the insight_password
				
				prop.store(new FileOutputStream(new File(propFile)), "Encrypted the password");
				propF = new File(propFile);
				
				
				// find the password to be used
				// use the property file as a input
				// I will use creation time as the password so if you move the file
				// it wont work and you need reset the password
				String creationTime = Files.getAttribute(Paths.get(propFile), "creationTime") + "";		
				String engineAlias = prop.getProperty("ENGINE_ALIAS");
				if(engineAlias != null)
					engineAlias = engineAlias + "__";
				else
					engineAlias = "";
				
				String dir = propF.getParent() + java.nio.file.FileSystems.getDefault().getSeparator() + engineAlias + prop.getProperty("ENGINE");

				if(passToEncrypt != null)
				{
					String passwordFileName = dir + java.nio.file.FileSystems.getDefault().getSeparator() + ".pass";
					
					File passFile = new File(passwordFileName);
					if(passFile.exists())
						passFile.delete();
					
					SnowApi snow = new SnowApi();		
					//System.out.println("Using cretion time.. " + creationTime);
					snow.encryptMessage(passToEncrypt, creationTime, propFile, passwordFileName);
				}
				if(insightPassToEncrypt != null)
				{
					String passwordFileName = dir + java.nio.file.FileSystems.getDefault().getSeparator() + ".insight";
					
					File passFile = new File(passwordFileName);
					if(passFile.exists())
						passFile.delete();
					
					SnowApi snow = new SnowApi();		
					//System.out.println("Using cretion time.. " + creationTime);
					snow.encryptMessage(insightPassToEncrypt, creationTime, propFile, passwordFileName);
					
				}
			}
			
			
			return prop;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
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
		List<String> props = eng.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/Title");
		while(!props.isEmpty()){
			System.out.println(props.remove(0));
		}
	}
	
	
	
}
