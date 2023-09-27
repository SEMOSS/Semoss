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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDFS;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.AuditDatabase;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.SparqlInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.security.SnowApi;
import prerna.ui.components.RDFEngineHelper;
import prerna.util.CSVToOwlMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.upload.UploadUtilities;

/**
 * An Abstract Engine that sets up the base constructs needed to create an
 * engine.
 */
public abstract class AbstractDatabaseEngine implements IDatabaseEngine {

	/**
	 * Static members
	 */
	
	public static final String USE_FILE = "USE_FILE";
	public static final String DATA_FILE = "DATA_FILE";
	public static final String OWL_POSITION_FILENAME = "positions.json";

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static final Logger classLogger = LogManager.getLogger(AbstractDatabaseEngine.class);
	
	private static final String SEMOSS_URI = "http://semoss.org/ontologies/";
	private static final String CONTAINS_BASE_URI = SEMOSS_URI + Constants.DEFAULT_RELATION_CLASS + "/Contains";
	private static final String GET_BASE_URI_FROM_OWL = "SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1";

	/**
	 * Class members
	 */
	
	protected String smssFilePath = null;
	protected CaseInsensitiveProperties origSmssProp = null;
	protected CaseInsensitiveProperties smssProp = null;
	
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
	
	private Hashtable<String, String> baseDataHash;

	/**
	 * This is used for tracking audit modifications 
	 */
	private AuditDatabase auditDatabase = null;

	/**
	 * This is if we have a connection but no OWL
	 */
	private boolean isBasic = false;

	/**
	 * Opens a database as defined by its properties file. What is included in
	 * the properties file is dependent on the type of engine that is being
	 * initiated. This is the function that first initializes an engine with the
	 * property file at the very least defining the data store.
	 * 
	 * @param smssFilePath
	 *            contains all information regarding the data store and how the
	 *            engine should be instantiated. Dependent on what type of
	 *            engine is being instantiated.
	 */
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}
	
	@Override
	public void open(Properties smssProp) throws Exception {
		// if smss prop is empty
		// then no metadata is associated with this database
		if(smssProp.isEmpty()) {
			return;
		}
		// this sets this.smssProp and this.origSmssProp
		setSmssProp(smssProp);
		
		// grab the main properties
		this.engineId = this.smssProp.getProperty(Constants.ENGINE);
		this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);
		
		if(this.isBasic) {
			// if this is a basic database, we dont care about the OWL or any other SMSS values
			return;
		}
		
		ISecrets secretStore = SecretsFactory.getSecretConnector();
		if(secretStore != null) {
			Map<String, String> engineSecrets = secretStore.getDatabaseSecrets(this.engineName, this.engineId);
			if(engineSecrets != null && !engineSecrets.isEmpty()) {
				this.smssProp.putAll(engineSecrets);
			}
		}
		
		// do the piece of encrypting here
		boolean encryptFile = false;
		if(DIHelper.getInstance().getProperty(Constants.ENCRYPT_SMSS) != null) {
			encryptFile = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.ENCRYPT_SMSS) + "");
		}
		// if not at application level, are we doing at app level
		if(!encryptFile && this.smssProp.containsKey(Constants.ENCRYPT_SMSS)) {
			encryptFile = Boolean.parseBoolean(smssProp.getProperty(Constants.ENCRYPT_SMSS));
		}
		
		if(this.smssFilePath != null && encryptFile && this.smssProp.containsKey(Constants.PASSWORD) && 
			!((String)this.smssProp.get(Constants.PASSWORD)).equalsIgnoreCase("encrypted password")) {
			this.smssProp = encryptPropFile(this.smssFilePath);
		}
		
		// load the rdf owl db
		String owlFile = SmssUtilities.getOwlFile(this.smssProp).getAbsolutePath();
		if(owlFile != null) {
			File owlF = new File(owlFile);
			// need a check here to say if I am asking this to be remade or keep what it is
			if(!owlF.exists() || owlFile.equalsIgnoreCase("REMAKE")) {
				// the process of remake will start here
				// see if the usefile is there
				if(this.smssProp.containsKey(DATA_FILE)) {
					String owlFileName = null;
					String dataFile = SmssUtilities.getDataFile(this.smssProp).getAbsolutePath();
					if(owlFile.equals("REMAKE")) {
						// we will make the name
						File dF = new File(dataFile);
						owlFileName = this.engineName + "_OWL.OWL";
						owlFile = dF.getParentFile() + DIR_SEPARATOR + owlFileName;
					} else {
						owlFileName = FilenameUtils.getName(owlFile);
					}
					
					owlFile = generateOwlFromFlatFile(this.engineId, dataFile, owlFile, owlFileName);
				} 
			}
			// set the owl file
			if(owlFile != null) {
				owlFile = SmssUtilities.getOwlFile(this.smssProp).getAbsolutePath();
				classLogger.info("Loading OWL: " + Utility.cleanLogString(owlFile));
				setOWL(owlFile);
			}
		}
		
		// load properties object for db
		File engineProps = SmssUtilities.getEngineProperties(this.smssProp);
		if (engineProps != null) {
			this.generalEngineProp = Utility.loadProperties(engineProps.getAbsolutePath());
		}
	}
	
	/**
	 * Generate the OWL based on a flat file
	 * @param dataFile
	 * @param owlFile
	 * @param owlFileName
	 * @return
	 * @throws Exception 
	 */
	protected String generateOwlFromFlatFile(String engineId, String dataFile, String owlFile, String owlFileName) throws Exception {
		CSVToOwlMaker maker = new CSVToOwlMaker();
		maker.makeFlatOwl(engineId, dataFile, owlFile, getDatabaseType(), true);
		if(owlFile.equals("REMAKE")) {
			try {
				Utility.changePropertiesFileValue(this.smssFilePath, Constants.OWL, owlFileName);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return owlFile;
	}

	@Override
	public void close() throws IOException {
		if(this.baseDataEngine != null) {
			classLogger.debug("Closing the owl engine");
			this.baseDataEngine.close();
		}
		if (auditDatabase != null) {
			classLogger.debug("Closing the audit database engine");
			auditDatabase.close();
		}
	}
	
	@Override
	public String getProperty(String key) {
		String retProp = null;

		classLogger.debug("Property is " + Utility.cleanLogString(key) + "]");
		if (generalEngineProp != null && generalEngineProp.containsKey(key))
			retProp = generalEngineProp.getProperty(key);
		if (retProp == null && ontoProp != null && ontoProp.containsKey(key))
			retProp = ontoProp.getProperty(key);
		if (retProp == null && smssProp != null && smssProp.containsKey(key))
			retProp = smssProp.getProperty(key);
		return retProp;
	}

	/**
	 * Returns whether or not an engine is currently connected to the data
	 * store. The connection becomes true when {@link #open(String)} is called
	 * and the connection becomes false when {@link #close()} is called.
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
			classLogger.debug("Writing to file " + smssFilePath);
			fileOut = new FileOutputStream(smssFilePath);
			smssProp.store(fileOut, null);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			try {
				if(fileOut!=null)
					fileOut.close();
			}catch(IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}

	/**
	 * Adds a new property to the properties list.
	 * @param name		String - The name of the property.
	 * @param value		String - The value of the property.
	 */
	public void addProperty(String name, String value) {
		smssProp.put(name, value);
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
	public MetaHelper getMetaHelper() {
		return this.owlHelper;
	}
	
	@Override
	public void setBaseDataEngine(RDFFileSesameEngine baseDataEngine) {
		this.baseDataEngine = baseDataEngine;
		if(this.baseDataEngine.getEngineId() == null) {
			this.baseDataEngine.setEngineId(this.engineId + "_" + Constants.OWL_ENGINE_SUFFIX);
		}
		this.owlHelper = new MetaHelper(this.baseDataEngine, getDatabaseType(), this.engineId);
	}

	/**
	 * Sets the base data hash
	 * @param h		Hashtable - The base data hash that this is being set to
	 */
	public void setBaseHash(Hashtable h) {
		classLogger.debug(this.engineId + " Set the Base Data Hash ");
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
		this.owlHelper = new MetaHelper(baseDataEngine, getDatabaseType(), this.engineId);
	}

	/**
	 * Checks for an OWL and adds it to the engine. Sets the base data hash from
	 * the engine properties, commits the database, and creates the base
	 * relation engine.
	 */
	public void createBaseRelationEngine() {
		RDFFileSesameEngine baseRelEngine = new RDFFileSesameEngine();
		Hashtable baseHash = new Hashtable<>();
		// If OWL file doesn't exist, go the old way and create the base
		// relation engine
		// String owlFileName =
		// (String)DIHelper.getInstance().getCoreProp().get(engine.getEngineName()
		// + "_" + Constants.OWL);
		if (this.owlFileLocation == null) {
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			owlFileLocation = baseFolder + DIR_SEPARATOR + 
					Constants.DATABASE_FOLDER + DIR_SEPARATOR + 
					SmssUtilities.getUniqueName(getEngineName(), getEngineId()) + DIR_SEPARATOR +
					getEngineName()	+ "_OWL.OWL"; 
		}
		baseRelEngine.setFileName(this.owlFileLocation);
		try {
			baseRelEngine.open(new Properties());
			if(this.smssProp != null) {
				addProperty(Constants.OWL, owlFileLocation);
			}
			try {
				baseHash.putAll(RDFEngineHelper.createBaseFilterHash(baseRelEngine.getRc()));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				classLogger.error(Constants.STACKTRACE, e);
			}
			setBaseHash(baseHash);
			baseRelEngine.commit();
			setBaseDataEngine(baseRelEngine);
		} catch (Exception e) {
			classLogger.warn("Error occurred loading the OWL file for the database");
			classLogger.error(Constants.STACKTRACE, e);
		}
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
	
	@Override
	public void setSmssFilePath(String smssFilePath) {
		this.smssFilePath = smssFilePath;
	}
	
	@Override
	public String getSmssFilePath() {
		return this.smssFilePath;
	}
	
	public String getOWLDefinition() {
		if(owlHelper == null) {
			return null;
		}
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
		classLogger.debug("Committing base data engine of " + this.engineId);
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
		classLogger.debug("Running select query on base data engine of " + this.engineId);
		classLogger.debug("Query is " + query);
		return this.baseDataEngine.execQuery(query);
	}

	public String getMethodName(IDatabaseEngine.ACTION_TYPE actionType){
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


	public Object doAction(IDatabaseEngine.ACTION_TYPE actionType, Object[] args){
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
			classLogger.error(Constants.STACKTRACE, e);
		} catch (NoSuchMethodException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IllegalArgumentException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IllegalAccessException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InvocationTargetException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return ret;
	}

	@Override
	public void delete() throws IOException {
		classLogger.debug("Delete database engine " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		try {
			this.close();
		} catch(IOException e) {
			classLogger.warn("Error occurred trying to close the connection");
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		File engineFolder = null;
		File owlFile = SmssUtilities.getOwlFile(this.smssProp);
		String folderName = null;
		if(owlFile != null) {
			engineFolder = owlFile.getParentFile();
		} else {
			engineFolder = new File(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
					+ "/" + Constants.DATABASE_FOLDER + "/" + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		}
		folderName = engineFolder.getName();

		if(owlFile != null && owlFile.exists()) {
			classLogger.info("Deleting owl file " + owlFile.getAbsolutePath());
			try {
				FileUtils.forceDelete(owlFile);
			} catch(IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		//this check is to ensure we are deleting the right folder.
		classLogger.info("Checking folder name is matching up : " + folderName + " against " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		if(folderName.equals(SmssUtilities.getUniqueName(this.engineName, this.engineId))) {
			classLogger.info("folder getting deleted is " + engineFolder.getAbsolutePath());
			try {
				FileUtils.deleteDirectory(engineFolder);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		classLogger.debug("Deleting smss " + this.smssFilePath);
		File smssFile = new File(this.smssFilePath);
		try {
			FileUtils.forceDelete(smssFile);
		} catch(IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// remove from DIHelper
		UploadUtilities.removeEngineFromDIHelper(this.engineId);
	}
	
	@Override
	public Vector<String> executeInsightQuery(String sparqlQuery, boolean isDbQuery) {
		IDatabaseEngine engine = this;
		if(!isDbQuery){
			engine = this.baseDataEngine;
		} 
			
		return Utility.getVectorOfReturn(sparqlQuery, engine, true);
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
	public String getNodeBaseUri(){
		if(baseUri == null) {
			IRawSelectWrapper wrap = null;
			try {
				wrap = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, GET_BASE_URI_FROM_OWL);
				if(wrap.hasNext()) {
					IHeadersDataRow data = wrap.next();
					baseUri = data.getRawValues()[0] + "";
					classLogger.info("Got base uri from owl " + Utility.cleanLogString(this.baseUri) + " for engine " + getEngineId() + " : " + getEngineName());
				}
				if(baseUri == null){
					baseUri = Constants.CONCEPT_URI;
					classLogger.info("couldn't get base uri from owl... defaulting to " + baseUri + " for engine " + getEngineId() + " : " + getEngineName());
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrap != null) {
					try {
						wrap.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
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
	
	/**
	 * Get the OWL position map file location
	 * @return
	 */
	public File getOwlPositionFile() {
		String owlFileLocation = getOWL();
		// put in same location
		File owlF = new File(owlFileLocation);
		String baseFolder = owlF.getParent();
		String positionJson = baseFolder + DIR_SEPARATOR + AbstractDatabaseEngine.OWL_POSITION_FILENAME;
		File positionFile = new File(positionJson);
		return positionFile;
	}
	
	@Override
	public void setSmssProp(Properties smssProp) {
		if(smssProp instanceof CaseInsensitiveProperties) {
			this.origSmssProp = (CaseInsensitiveProperties) smssProp;
			this.smssProp = new CaseInsensitiveProperties(smssProp);
		} else {
			this.origSmssProp = new CaseInsensitiveProperties(smssProp);
			this.smssProp = new CaseInsensitiveProperties(smssProp);
		}
	}
	
	@Override
	public CaseInsensitiveProperties getSmssProp() {
		return this.smssProp;
	}
	
	@Override
	public CaseInsensitiveProperties getOrigSmssProp() {
		return this.origSmssProp;
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
	
	public String decryptPass(String propFile, boolean insight) {
		propFile = Utility.normalizePath(propFile);
		String retString = null;
		try {
			File propF = new File(propFile);
			Properties prop = Utility.loadProperties(propFile);
			String dir = propF.getParent() + DIR_SEPARATOR + SmssUtilities.getUniqueName(prop);
			String passwordFileName = dir + DIR_SEPARATOR + ".pass";
			if(insight) {
				passwordFileName = dir + DIR_SEPARATOR + ".insight";
			}

			String creationTime = Files.getAttribute(Paths.get(propFile), "creationTime") + "";		
			File inputFile = new File(Utility.normalizePath(passwordFileName));
			if(inputFile.exists()) {
				// if nothing is there return null
				SnowApi snow = new SnowApi();			
				retString = snow.decryptMessage(creationTime, passwordFileName);
			}			
		} catch (FileNotFoundException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return retString;
	}


	public CaseInsensitiveProperties encryptPropFile(String propFile) {
		propFile = Utility.normalizePath(propFile);
		OutputStream os = null;
		try {
			File propF = new File(propFile);
			CaseInsensitiveProperties prop = new CaseInsensitiveProperties(Utility.loadProperties(propFile));

			String passToEncrypt = null;
			String insightPassToEncrypt = null;

			Iterator<Object> keys = prop.keySet().iterator();
			while(keys.hasNext()) {
				String thisKey = (String) keys.next();
				if(thisKey.equalsIgnoreCase("password")) {
					passToEncrypt = prop.getProperty(thisKey);
					if(!passToEncrypt.equalsIgnoreCase("encrypted password")) {
						prop.put(thisKey, "encrypted password");
					}
				} else if(thisKey.equalsIgnoreCase("insight_password")) {
					insightPassToEncrypt = prop.getProperty(thisKey);
					if(!insightPassToEncrypt.equalsIgnoreCase("encrypted password")) {
						prop.put(thisKey, "encrypted password");
					}
				}
			}	
			
			if(insightPassToEncrypt == null) {
				prop.put("insight_password", "encrypted password");
				insightPassToEncrypt = "";
			}
			
			if(passToEncrypt != null && !passToEncrypt.equalsIgnoreCase("encrypted password") || 
					(insightPassToEncrypt != null && insightPassToEncrypt.equalsIgnoreCase("encrypted password"))) {
				// add the insight_password
				os = new FileOutputStream(propF);
				prop.store(os, "Encrypted the password");
				
				// find the password to be used
				// use the property file as a input
				// I will use creation time as the password so if you move the file
				// it wont work and you need reset the password
				String creationTime = Files.getAttribute(Paths.get(propFile), "creationTime") + "";		
				String dir = propF.getParent() + DIR_SEPARATOR + SmssUtilities.getUniqueName(prop);
				if(passToEncrypt != null) {
					String passwordFileName = dir + DIR_SEPARATOR + ".pass";
					
					File passFile = new File(Utility.normalizePath(passwordFileName));
					if(passFile.exists()) {
						passFile.delete();
					}
					
					SnowApi snow = new SnowApi();		
					//logger.info("Using creation time.. " + creationTime);
					snow.encryptMessage(passToEncrypt, creationTime, propFile, passwordFileName);
				}
				
				if(insightPassToEncrypt != null) {
					String passwordFileName = dir + DIR_SEPARATOR + ".insight";
					
					File passFile = new File(Utility.normalizePath(passwordFileName));
					if(passFile.exists()) {
						passFile.delete();
					}
					SnowApi snow = new SnowApi();		
					//logger.info("Using creation time.. " + creationTime);
					snow.encryptMessage(insightPassToEncrypt, creationTime, propFile, passwordFileName);
				}
			}
			
			return prop;
		} catch (FileNotFoundException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(os != null) {
				try {
					os.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return null;
	}
	
	public String [] getUDF() {
		if(smssProp.containsKey("UDF")) {
			return smssProp.get("UDF").toString().split(";");
		}
		return null;
	}
	
	public String getOwl()
	{
		String retOwl = null;
		if(owlFileLocation != null)
		{
			try {
				retOwl = FileUtils.readFileToString(new File(owlFileLocation));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return retOwl;
	}
	
	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.DATABASE;
	}
	
	@Override
	public String getCatalogSubType(Properties smssProp) {
		return getDatabaseType().toString();
	}
	
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Testing
	 */
	
//	public static void main(String [] args) throws Exception
//	{
//		DIHelper.getInstance().loadCoreProp("C:\\workspace\\SEMOSSDev\\RDF_Map.prop");
//		FileInputStream fileIn = null;
//		try{
//			Properties prop = new Properties();
//			String fileName = "C:\\workspace\\SEMOSSDev\\db\\Movie_Test.smss";
//			fileIn = new FileInputStream(fileName);
//			prop.load(fileIn);
//			System.err.println("Loading DB " + fileName);
//			Utility.loadEngine(fileName, prop);
//		}catch(IOException e){
//			e.printStackTrace();
//		}finally{
//			try{
//				if(fileIn!=null) {
//					fileIn.close();
//				}
//			} catch(IOException e) {
//				e.printStackTrace();
//			}
//		}
//		IDatabase eng = (IDatabase) DIHelper.getInstance().getLocalProp("Movie_Test");
//		List<String> props = eng.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/Title");
//		while(!props.isEmpty()){
//			System.out.println(props.remove(0));
//		}
//	}
	
}
