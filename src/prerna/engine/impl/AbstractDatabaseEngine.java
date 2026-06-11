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
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDFS;

import prerna.auth.external.ExternalDatabaseMetadataHelper;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.owl.OWLEngineFactory;
import prerna.engine.impl.rdbms.AuditDatabase;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.SparqlInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.RDFEngineHelper;
import prerna.util.CSVToOwlMaker;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.SystemDefaultDatabases;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * An Abstract Engine that sets up the base constructs needed to create an
 * engine.
 */
public abstract class AbstractDatabaseEngine extends AbstractEngine implements IDatabaseEngine {

	/**
	 * Static members
	 */

	public static final String USE_FILE = "USE_FILE";
	public static final String DATA_FILE = "DATA_FILE";
	public static final String OWL_POSITION_FILENAME = "positions.json";

	private static final Logger classLogger = LogManager.getLogger(AbstractDatabaseEngine.class);

	private static final String SEMOSS_URI = "http://semoss.org/ontologies/";
	private static final String CONTAINS_BASE_URI = SEMOSS_URI + Constants.DEFAULT_RELATION_CLASS + "/Contains";
	private static final String GET_BASE_URI_FROM_OWL = "SELECT DISTINCT ?entity WHERE { { <SEMOSS:ENGINE_METADATA> <CONTAINS:BASE_URI> ?entity } } LIMIT 1";

	/**
	 * Class members
	 */

	protected Properties generalEngineProp = null;
	protected Properties ontoProp = null;

	/**
	 * OWL database
	 */
	private OWLEngineFactory owlEnginefactory = null;
	protected RDFFileSesameEngine baseDataEngine;
	private String owlFileLocation;
	private String baseUri;

	private Hashtable<String, String> baseDataHash;

	/**
	 * This is used for tracking audit modifications
	 */
	private AuditDatabase auditDatabase = null;

	protected ZoneId databaseZoneId;

	/**
	 * Opens a database as defined by its properties file. What is included in the
	 * properties file is dependent on the type of engine that is being initiated.
	 * This is the function that first initializes an engine with the property file
	 * at the very least defining the data store.
	 * 
	 * @param smssFilePath contains all information regarding the data store and how
	 *                     the engine should be instantiated. Dependent on what type
	 *                     of engine is being instantiated.
	 */
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		// basic would be an insights database for example
		if (this.isBasic) {
			// still try to set the db zone id...
			setDatabaseZoneId();
			// if this is a basic database, we dont care about the OWL or any other SMSS
			// values
			return;
		}

		// try to set the db zone id if defined
		setDatabaseZoneId();

		// load the rdf owl db
		String owlFile = null;
		String owlPropStr = this.smssProp.getProperty(Constants.OWL);
		if (owlPropStr == null || (owlPropStr = owlPropStr.trim()).isEmpty()) {
			// make a new empty owl
			owlFile = UploadUtilities.generateOwlFile(getCatalogType(), this.engineId, this.engineName)
					.getAbsolutePath();
			setOwlFilePath(owlFile);
		} else if (owlPropStr.equalsIgnoreCase("REMAKE")) {
			classLogger.info("Attempting to create new OWL file");
			// the process of remake will start here
			// see if the usefile is there
			File dataF = SmssUtilities.getDataFile(this.smssProp);
			if (dataF != null && dataF.exists()) {
				owlFile = UploadUtilities.generateOwlFile(getCatalogType(), this.engineId, this.engineName)
						.getAbsolutePath();
				setOwlFilePath(owlFile);
				owlFile = generateOwlFromFlatFile(dataF.getAbsolutePath(), owlFile, FilenameUtils.getName(owlFile));
			}
		} else {
			// if its not one of these special values
			// then lets grab the owlFile
			File owlF = SmssUtilities.getOwlFile(this.smssFilePath, this.smssProp);
			if (owlF == null) {
				// make a new empty owl
				owlFile = UploadUtilities.generateOwlFile(getCatalogType(), this.engineId, this.engineName)
						.getAbsolutePath();
				setOwlFilePath(owlFile);
			} else if (!owlF.exists() || !owlF.isFile()) {
				// load default OWL based on file location
				owlFile = UploadUtilities.generateEmptyRDFXMLFile(owlF.getAbsolutePath()).getAbsolutePath();
				setOwlFilePath(owlFile);
			} else {
				owlFile = owlF.getAbsolutePath();
				// it exists, just set it
				classLogger.info("Loading OWL: " + Utility.cleanLogString(owlFile));
				setOwlFilePath(owlFile);
			}
		}

		// this section is if we are getting the metadata on load from an external
		// service
		boolean externalDatabaseMetadata = Boolean
				.parseBoolean(Utility.getDIHelperProperty(Constants.EXTERNAL_DATABASE_MANAGEMENT_ENABLED) + "");
		if (externalDatabaseMetadata
				&& !SystemDefaultDatabases.getDatabasesWithGeneratedOwl().contains(this.engineId)) {
			try {
				// store in the OWL
				// so that the local master can pick up the OWL
				if (owlPropStr == null || (owlPropStr = owlPropStr.trim()).isEmpty()) {
					Map<String, String> mods = new HashMap<>();
					mods.put(Constants.OWL, new File(owlFile).getName());
					Utility.addKeysAtLocationIntoPropertiesFile(this.smssFilePath, null, mods);
				}
				ExternalDatabaseMetadataHelper.parseJsonToOwl(this);
			} catch (Exception e) {
				classLogger.warn("Could not load metadata externally for "
						+ SmssUtilities.getUniqueName(this.engineName, this.engineId));
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		// load properties object for db
		File engineProps = SmssUtilities.getEngineProperties(this.smssProp);
		if (engineProps != null) {
			this.generalEngineProp = Utility.loadProperties(engineProps.getAbsolutePath());
		}
	}

	/**
	 * 
	 */
	protected void setDatabaseZoneId() {
		String dbZoneIdStr = this.smssProp.getProperty(Constants.DATABASE_ZONEID);
		if (dbZoneIdStr != null && !(dbZoneIdStr = dbZoneIdStr.trim()).isEmpty()) {
			try {
				this.databaseZoneId = ZoneId.of(dbZoneIdStr);
			} catch (Exception e) {
				classLogger.warn("Could not determine the database zone id from string input = " + dbZoneIdStr
						+ " for engine " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else {
			classLogger.warn("Please consider adding a default database zone id for engine "
					+ SmssUtilities.getUniqueName(this.engineName, this.engineId));
		}
	}

	/**
	 * Generate the OWL based on a flat file
	 * 
	 * @param dataFile
	 * @param owlFile
	 * @param owlFileName
	 * @return
	 * @throws Exception
	 */
	protected String generateOwlFromFlatFile(String dataFile, String owlFile, String owlFileName) throws Exception {
		CSVToOwlMaker maker = new CSVToOwlMaker();
		maker.makeFlatOwl(getOWLEngineFactory().getWriteOWL(), dataFile, owlFile, getDatabaseType(), true);
		if (owlFile.equals("REMAKE")) {
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
		if (this.baseDataEngine != null) {
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

//		classLogger.debug("Property is " + Utility.cleanLogString(key) + "]");
		if (generalEngineProp != null && generalEngineProp.containsKey(key)) {
			retProp = generalEngineProp.getProperty(key);
		}
		if (retProp == null && ontoProp != null && ontoProp.containsKey(key)) {
			retProp = ontoProp.getProperty(key);
		}
		if (retProp == null && smssProp != null && smssProp.containsKey(key)) {
			retProp = smssProp.getProperty(key);
		}
		return retProp;
	}

	/**
	 * Returns whether or not an engine is currently connected to the data store.
	 * The connection becomes true when {@link #open(String)} is called and the
	 * connection becomes false when {@link #close()} is called.
	 * 
	 * @return true if the engine is connected to its data store and false if it is
	 *         not
	 */
	@Override
	public boolean isConnected() {
		return false;
	}

	/**
	 * Adds a new property to the properties list.
	 * 
	 * @param name  String - The name of the property.
	 * @param value String - The value of the property.
	 */
	@Override
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
	public OWLEngineFactory getOWLEngineFactory() {
		return this.owlEnginefactory;
	}

	@Override
	public void setBaseDataEngine(RDFFileSesameEngine baseDataEngine) {
		this.baseDataEngine = baseDataEngine;
		if (this.baseDataEngine.getEngineId() == null) {
			this.baseDataEngine.setEngineId(this.engineId + "_" + Constants.OWL_ENGINE_SUFFIX);
		}
		this.owlEnginefactory = new OWLEngineFactory(this.baseDataEngine, getDatabaseType(), this.engineId,
				this.engineName);
	}

	/**
	 * Sets the base data hash
	 * 
	 * @param h Hashtable - The base data hash that this is being set to
	 */
	public void setBaseHash(Hashtable h) {
		classLogger.debug(this.engineId + " Set the Base Data Hash ");
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
	 * Checks for an OWL and adds it to the engine. Sets the base data hash from the
	 * engine properties, commits the database, and creates the base relation
	 * engine.
	 */
	public void createBaseRelationEngine() {
		// if we have an existing one, close it
		if (this.baseDataEngine != null) {
			try {
				this.baseDataEngine.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		// new base data engine being made
		RDFFileSesameEngine baseRelEngine = new RDFFileSesameEngine();
		baseRelEngine.setBasic(true);
		baseRelEngine.setEngineId(this.engineId + "_" + Constants.OWL_ENGINE_SUFFIX);
		Hashtable<String, String> baseHash = new Hashtable<>();
		// If OWL file doesn't exist, go the old way and create the base relation engine
		if (this.owlFileLocation == null) {
			this.owlFileLocation = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.DATABASE,
					getEngineId(), getEngineName()) + FILE_SEPARATOR + getEngineName() + "_OWL.OWL";
		}
		baseRelEngine.setFilePath(this.owlFileLocation);
		try {
			baseRelEngine.open(new Properties());
			if (this.smssProp != null) {
				addProperty(Constants.OWL, owlFileLocation);
			}
			try {
				baseHash.putAll(RDFEngineHelper.createBaseFilterHash(baseRelEngine.getRc()));
			} catch (Exception e) {
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
	@Override
	public Vector<String> getFromNeighbors(String nodeType, int neighborHood) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getFromNeighbors(nodeType, neighborHood);
	}

	// gets the to nodes
	@Override
	public Vector<String> getToNeighbors(String nodeType, int neighborHood) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getToNeighbors(nodeType, neighborHood);
	}

	// gets the from and to nodes
	@Override
	public Vector<String> getNeighbors(String nodeType, int neighborHood) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getNeighbors(nodeType, neighborHood);
	}

	@Override
	public void setOwlFilePath(String owl) {
		this.owlFileLocation = owl;
		createBaseRelationEngine();
		this.owlEnginefactory = new OWLEngineFactory(this.baseDataEngine, getDatabaseType(), this.engineId,
				this.engineName);
	}

	@Override
	public String getOwlFilePath() {
		return this.owlFileLocation;
	}

	@Override
	public String getOWLDefinition() {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getOWLDefinition();
	}

	@Override
	public IQueryInterpreter getQueryInterpreter() {
		return new SparqlInterpreter(this);
	}

	/**
	 * Commits the base data engine
	 */
	@Override
	public void commitOWL() {
		classLogger.debug("Committing base data engine of " + this.engineId);
		this.baseDataEngine.commit();
	}

	public Vector<String> getConcepts() {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getConcepts();
	}

	/**
	 * Runs a select query on the base data engine of this engine
	 */
	@Override
	public Object execOntoSelectQuery(String query) {
		classLogger.debug("Running select query on base data engine of " + this.engineId);
		classLogger.debug("Query is " + query);
		return this.baseDataEngine.execQuery(query);
	}

	public String getMethodName(IDatabaseEngine.ACTION_TYPE actionType) {
		String retString = "";
		switch (actionType) {
		case ADD_STATEMENT: {
			retString = "addStatement";
			break;
		}
		case REMOVE_STATEMENT: {
			retString = "removeStatement";
			break;
		}
		case BULK_INSERT: {
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

	/**
	 * 
	 */
	@Override
	public Object doAction(IDatabaseEngine.ACTION_TYPE actionType, Object[] args) {
		// Iterate through methods on the engine -- do this on startup
		// Find the method on the engine that matches the action type passed in
		// pass the arguments and let it run

		// if the method does not exist on the engine
		// look at the smss for the method (?)
		String methodName = this.getMethodName(actionType);

		Object[] params = { args };
		java.lang.reflect.Method method = null;
		Object ret = null;
		try {
			method = this.getClass().getMethod(methodName, args.getClass());
			ret = method.invoke(this, params);
		} catch (SecurityException | NoSuchMethodException | IllegalArgumentException | IllegalAccessException
				| InvocationTargetException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return ret;
	}

	@Override
	public Vector<String> executeInsightQuery(String sparqlQuery, boolean isDbQuery) {
		IDatabaseEngine engine = this;
		if (!isDbQuery) {
			engine = this.baseDataEngine;
		}

		return Utility.getVectorOfReturn(sparqlQuery, engine, true);
	}

	@Override
	public String getNodeBaseUri() {
		if (baseUri == null) {
			IRawSelectWrapper wrap = null;
			try {
				wrap = WrapperManager.getInstance().getRawWrapper(this.baseDataEngine, GET_BASE_URI_FROM_OWL);
				if (wrap.hasNext()) {
					IHeadersDataRow data = wrap.next();
					baseUri = data.getRawValues()[0] + "";
					classLogger.info("Got base uri from owl " + Utility.cleanLogString(this.baseUri) + " for engine "
							+ getEngineId() + " : " + getEngineName());
				}
				if (baseUri == null) {
					baseUri = Constants.CONCEPT_URI;
					classLogger.info("couldn't get base uri from owl... defaulting to " + baseUri + " for engine "
							+ getEngineId() + " : " + getEngineName());
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if (wrap != null) {
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
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getDataTypes(uri);
	}

	@Override
	public Map<String, String> getDataTypes(String... uris) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getDataTypes(uris);
	}

	@Override
	public String getAdtlDataTypes(String uri) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getAdtlDataTypes(uri);
	}

	@Override
	public Map<String, String> getAdtlDataTypes(String... uris) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getAdtlDataTypes(uris);
	}

	/**
	 * This method will return a query struct which when interpreted would produce a
	 * query to get all the data within the engine. Will currently assume all joins
	 * to be inner.join
	 * 
	 * @return
	 */
	public SelectQueryStruct getDatabaseQueryStruct() {
		SelectQueryStruct qs = new SelectQueryStruct();

		// query to get all the concepts and properties for selectors
		String getSelectorsInformation = "SELECT DISTINCT ?conceptualConcept ?property WHERE { "
				+ "{?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?concept <http://semoss.org/ontologies/Relation/Conceptual> ?conceptualConcept }" + "OPTIONAL {"
				+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + CONTAINS_BASE_URI + "> } "
				+ "{?concept <" + OWL.DATATYPEPROPERTY.toString() + "> ?property } "
				+ "{?property <http://semoss.org/ontologies/Relation/Conceptual> ?conceptualProperty }" + "}" // END
																												// OPTIONAL
				+ "}"; // END WHERE

		// execute the query and loop through and add it into the QS
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, getSelectorsInformation);
			// we will keep a set of the concepts such that we know when we need to append a
			// PRIM_KEY_PLACEHOLDER
			Set<String> conceptSet = new HashSet<String>();
			while (wrapper.hasNext()) {
				IHeadersDataRow hrow = wrapper.next();
				Object[] row = hrow.getValues();
				Object[] raw = hrow.getRawValues();
				if (raw[0].toString().equals("http://semoss.org/ontologies/Concept")) {
					continue;
				}

				String concept = row[0].toString();
				if (!conceptSet.contains(concept)) {
					qs.addSelector(new QueryColumnSelector(concept));
				}

				Object property = raw[1];
				if (property != null && !property.toString().isEmpty()) {
					qs.addSelector(new QueryColumnSelector(concept + "__" + Utility.getClassName(property.toString())));
				}
			}
			// no need to keep this anymore
			conceptSet.clear();
			conceptSet = null;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
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
				+ "{?toConcept <http://semoss.org/ontologies/Relation/Conceptual> ?toConceptualConcept }" + "}"; // END
																													// WHERE

		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(baseDataEngine, getRelationshipsInformation);
			while (wrapper.hasNext()) {
				IHeadersDataRow hrow = wrapper.next();
				Object[] row = hrow.getValues();
				String fromConcept = row[0].toString();
				String toConcept = row[1].toString();
				qs.addRelation(fromConcept, toConcept, "inner.join");
			}

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
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
	 * 
	 * @return
	 */
	@Override
	public Map<String, Object[]> getMetamodel() {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getMetamodel();
	}

	/**
	 * Get the OWL position map file location
	 * 
	 * @return
	 */
	@Override
	public File getOwlPositionFile() {
		String owlFileLocation = getOwlFilePath();
		// put in same location
		File owlF = new File(owlFileLocation);
		String baseFolder = owlF.getParent();
		String positionJson = baseFolder + FILE_SEPARATOR + AbstractDatabaseEngine.OWL_POSITION_FILENAME;
		File positionFile = new File(positionJson);
		return positionFile;
	}

	@Override
	public void setSmssProp(Properties smssProp) {
		if (smssProp instanceof CaseInsensitiveProperties) {
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
		if (this.auditDatabase == null) {
			this.auditDatabase = new AuditDatabase();
			try {
				this.auditDatabase.init(this, this.engineId, this.engineName);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return this.auditDatabase;
	}

	/*
	 * NEW PIXEL TO REPLACE CONCEPTUAL NAMES
	 */

	@Override
	public List<String> getPixelConcepts() {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getPixelConcepts();
	}

	@Override
	public List<String> getPixelSelectors(String conceptPixelName) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getPixelSelectors(conceptPixelName);
	}

	@Override
	public List<String> getPropertyPixelSelectors(String conceptPixelName) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getPropertyPixelSelectors(conceptPixelName);
	}

	@Override
	public List<String> getPhysicalConcepts() {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getPhysicalConcepts();
	}

	@Override
	public List<String[]> getPhysicalRelationships() {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getPhysicalRelationships();
	}

	@Override
	public List<String> getPropertyUris4PhysicalUri(String physicalUri) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getPropertyUris4PhysicalUri(physicalUri);
	}

	@Override
	public String getPhysicalUriFromPixelSelector(String pixelSelector) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getPhysicalUriFromPixelSelector(pixelSelector);
	}

	@Override
	@Deprecated
	public String getPixelUriFromPhysicalUri(String physicalUri) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getPixelUriFromPhysicalUri(physicalUri);
	}

	@Override
	public String getConceptPixelUriFromPhysicalUri(String conceptPhysicalUri) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getConceptPixelUriFromPhysicalUri(conceptPhysicalUri);
	}

	@Override
	public String getPropertyPixelUriFromPhysicalUri(String conceptPhysicalUri, String propertyPhysicalUri) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getPropertyPixelUriFromPhysicalUri(conceptPhysicalUri,
				propertyPhysicalUri);
	}

	@Override
	public String getPixelSelectorFromPhysicalUri(String physicalUri) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getPixelSelectorFromPhysicalUri(physicalUri);
	}

	@Override
	public String getConceptualName(String physicalUri) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getConceptualName(physicalUri);
	}

	@Override
	public Set<String> getLogicalNames(String physicalUri) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getLogicalNames(physicalUri);
	}

	@Override
	public String getDescription(String physicalUri) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getDescription(physicalUri);
	}

	@Override
	@Deprecated
	public String getLegacyPrimKey4Table(String physicalUri) {
		if (owlEnginefactory == null) {
			return null;
		}
		return owlEnginefactory.getReadOWL().getLegacyPrimKey4Table(physicalUri);
	}

	/**
	 * 
	 */
	@Override
	public String[] getUDF() {
		if (smssProp.containsKey("UDF")) {
			return smssProp.get("UDF").toString().split(";");
		}
		return null;
	}

	@Override
	public ZoneId getDatabaseZoneId() {
		return this.databaseZoneId;
	}

	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.DATABASE;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return getDatabaseType().toString();
	}

}
