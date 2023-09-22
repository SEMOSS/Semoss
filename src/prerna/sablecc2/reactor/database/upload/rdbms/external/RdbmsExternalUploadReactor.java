package prerna.sablecc2.reactor.database.upload.rdbms.external;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.RDFS;

import com.google.common.io.Files;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.ACTION_TYPE;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public class RdbmsExternalUploadReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RdbmsExternalUploadReactor.class);

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String CLASS_NAME = RdbmsExternalUploadReactor.class.getName();

	private static final String[] JDBC_CONSTANTS = {Constants.USE_CONNECTION_POOLING,
			Constants.POOL_MIN_SIZE,
			Constants.POOL_MAX_SIZE,
			Constants.CONNECTION_QUERY_TIMEOUT,
			Constants.FETCH_SIZE};
	
	// we need to define some variables that are stored at the class level
	// so that we can properly account for cleanup if errors occur
	protected transient Logger logger;
	protected transient String databaseId;
	protected transient String databaseName;
	protected transient IDatabaseEngine database;
	protected transient File databaseFolder;
	protected transient File tempSmss;
	protected transient File smssFile;

	protected transient boolean error = false;

	public RdbmsExternalUploadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CONNECTION_DETAILS.getKey(), UploadInputUtility.DATABASE, 
				UploadInputUtility.METAMODEL, ReactorKeysEnum.EXISTING.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.logger = getLogger(this.getClass().getName());
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create a database", PixelDataType.CONST_STRING,
					PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		// throw error is user doesn't have rights to publish new databases
		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
			throwUserNotPublisherError();
		}
		
		if (AbstractSecurityUtils.adminOnlyEngineAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[1]);
		String userPassedExisting = this.keyValue.get(this.keysToGet[3]);
		boolean existingDatabase = false;
		IRDBMSEngine nativeDatabase = null;

		// make sure both fields exist
		if (databaseId != null && userPassedExisting != null) {
			existingDatabase = Boolean.parseBoolean(userPassedExisting);
			
			IDatabaseEngine database = Utility.getDatabase(databaseId);
			if(database instanceof IRDBMSEngine) {
				nativeDatabase = (IRDBMSEngine) database;
			} else {
				throw new IllegalArgumentException("Database must be a valid JDBC database");
			}
		}

		// if user enters existing=true and the database doesn't exist
		if (existingDatabase && (databaseId == null || nativeDatabase == null)) {
			throw new IllegalArgumentException("Database " + databaseId + " does not exist");
		}
		this.databaseName = UploadInputUtility.getDatabaseNameOrId(this.store);

		if (existingDatabase) {
			// check if input is alias since we are adding to existing
			databaseId = SecurityQueryUtils.testUserEngineIdForAlias(user, databaseId);
			if (!SecurityEngineUtils.userCanEditEngine(user, databaseId)) {
				NounMetadata noun = new NounMetadata(
						"User does not have sufficient priviledges to create or update a database",
						PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}

			this.databaseId = databaseId;
			this.database = Utility.getDatabase(databaseId);
			try {
				this.logger.info("Updating existing database");
				updateExistingDatabase();
				this.logger.info("Done updating existing database");
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				this.error = true;
				if (e instanceof SemossPixelException) {
					throw (SemossPixelException) e;
				} else {
					NounMetadata noun = new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING,
							PixelOperationType.ERROR);
					SemossPixelException err = new SemossPixelException(noun);
					err.setContinueThreadOfExecution(false);
					throw err;
				}
			}
		} else { // if database doesn't exist create new
			try {
				// make a new id
				this.databaseId = UUID.randomUUID().toString();
				// validate database
				this.logger.info("Start validating database");
				UploadUtilities.validateDatabase(user, this.databaseName, this.databaseId);
				this.logger.info("Done validating database");
				// create database folder
				this.logger.info("Start generating database folder");
				this.databaseFolder = UploadUtilities.generateDatabaseFolder(this.databaseId, this.databaseName);
				this.logger.info("Complete");
				generateNewDatabase(user);
				// and rename .temp to .smss
				this.smssFile = new File(this.tempSmss.getAbsolutePath().replace(".temp", ".smss"));
				FileUtils.copyFile(this.tempSmss, this.smssFile);
				this.tempSmss.delete();
				this.database.setSmssFilePath(this.smssFile.getAbsolutePath());
				UploadUtilities.updateDIHelper(this.databaseId, this.databaseName, this.database, this.smssFile);
				// sync metadata
				this.logger.info("Process database metadata to allow for traversing across databases");
				UploadUtilities.updateMetadata(this.databaseId, user);

				// adding all the git here
				// make a version folder if one doesn't exist
				/*
					String versionFolder = 	AssetUtility.getAppAssetVersionFolder(databaseName, databaseId);;
					File file = new File(versionFolder);
					if (!file.exists()) {
						file.mkdir();
					}
					// I will assume the directory is there now
					GitRepoUtils.init(versionFolder);
				*/
				this.logger.info("Complete");
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				this.error = true;
				if (e instanceof SemossPixelException) {
					throw (SemossPixelException) e;
				} else {
					NounMetadata noun = new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING,
							PixelOperationType.ERROR);
					SemossPixelException err = new SemossPixelException(noun);
					err.setContinueThreadOfExecution(false);
					throw err;
				}
			} finally {
				if (this.error) {
					// need to delete everything...
					cleanUpCreateNewError();
				}
			}
		}

		// even if no security, just add user as database owner
		if (user != null) {
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityEngineUtils.addEngineOwner(this.databaseId, user.getAccessToken(ap).getId());
			}
		}

		ClusterUtil.pushEngine(this.databaseId);

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), this.databaseId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	private void generateNewDatabase(User user) throws Exception {
		Logger logger = getLogger(CLASS_NAME);
		
		File originalFileLocation = null;
		Map<String, Object> connectionDetails = getConDetails();
		if(connectionDetails != null) {
			String host = (String) connectionDetails.get(AbstractSqlQueryUtil.HOSTNAME);
			if(host != null && !(host=host.trim()).isEmpty()) {
				String testUpdatedHost = this.insight.getAbsoluteInsightFolderPath(host);
				originalFileLocation = new File(testUpdatedHost);
				if (originalFileLocation.exists()) {
					// move the file
					// and then update the host value
					String newLocation = this.databaseFolder.getAbsolutePath() + DIR_SEPARATOR
							+ FilenameUtils.getName(originalFileLocation.getAbsolutePath());
					try {
						Files.copy(originalFileLocation, new File(newLocation));
					} catch (IOException e) {
						throw new IOException("Unable to relocate database to correct database folder");
					}
					host = newLocation;
					connectionDetails.put(AbstractSqlQueryUtil.HOSTNAME, host);
				}
			}
		}
		
		String driver = (String) connectionDetails.get(AbstractSqlQueryUtil.DRIVER_NAME);
		RdbmsTypeEnum driverEnum = RdbmsTypeEnum.getEnumFromString(driver);
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(driverEnum);
		
		String connectionUrl = null;
		try {
			connectionUrl = queryUtil.setConnectionDetailsfromMap(connectionDetails);
		} catch (RuntimeException e) {
			throw new SemossPixelException(new NounMetadata("Unable to generation connection url with message " + e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		int stepCounter = 1;
		logger.info(stepCounter + ". Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(this.databaseId, this.databaseName);
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		// the logical metamodel for the upload
		Map<String, Object> newMetamodel = UploadInputUtility.getMetamodel(this.store);
		if (newMetamodel == null) {
			throw new IllegalArgumentException("Must define the metamodel portions we are uploading");
		}
		Map<String, List<String>> nodesAndProps = (Map<String, List<String>>) newMetamodel.get(ExternalJdbcSchemaReactor.TABLES_KEY);
		List<Map<String, Object>> relationships = (List<Map<String, Object>>) newMetamodel.get(ExternalJdbcSchemaReactor.RELATIONS_KEY);
		logger.info(stepCounter + ". Create properties file for database...");
		// Create default RDBMS database or Impala
		String databaseClassName = RDBMSNativeEngine.class.getName();
		this.database = new RDBMSNativeEngine();
//		if (driverEnum == RdbmsTypeEnum.IMPALA) {
//			databaseClassName = ImpalaEngine.class.getName();
//			database = new ImpalaEngine();
//		}
		
		Map<String, Object> jdbcPropertiesMap = validateJDBCProperties(connectionDetails);	

		this.tempSmss = UploadUtilities.createTemporaryExternalRdbmsSmss(this.databaseId, this.databaseName, owlFile,
				databaseClassName, driverEnum, connectionUrl, connectionDetails, jdbcPropertiesMap);
		DIHelper.getInstance().setEngineProperty(this.databaseId + "_" + Constants.STORE, this.tempSmss.getAbsolutePath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Create database store...");
		database.setEngineId(this.databaseId);
		database.setEngineName(this.databaseName);
		database.setOWL(owlFile.getAbsolutePath());
		Properties smssProps = Utility.loadProperties(tempSmss.getAbsolutePath());
		smssProps.put("TEMP", "TRUE");
		database.open(smssProps);
		if (!database.isConnected()) {
			throw new IllegalArgumentException("Unable to connect to external database");
		}
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Start generating database metadata...");
		Owler owler = new Owler(owlFile.getAbsolutePath(), database.getDatabaseType());
		// get the existing datatypes
		// table names -> column name, column type
		Set<String> cleanTables = new HashSet<String>();
		for (String t : nodesAndProps.keySet()) {
			cleanTables.add(t.split("\\.")[0]);
		}
		Map<String, Map<String, String>> existingRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(database, cleanTables);
		// parse the nodes and get the prime keys and write to OWL
		Map<String, String> nodesAndPrimKeys = parseNodesAndProps(owler, nodesAndProps, existingRDBMSStructure);
		// parse the relationships and write to OWL
		parseRelationships(owler, relationships, existingRDBMSStructure, nodesAndPrimKeys);
		// commit and save the owl
		owler.commit();
		owler.export();
		database.setOWL(owler.getOwlPath());
		logger.info(stepCounter + ". Complete");
		stepCounter++;

		logger.info(stepCounter + ". Process database metadata to allow for traversing across databases	");
		UploadUtilities.updateMetadata(this.databaseId, user);
		logger.info(stepCounter + ". Complete");
		stepCounter++;
		
		if(originalFileLocation != null && originalFileLocation.exists()) {
			try {
				FileUtils.forceDelete(originalFileLocation);
			} catch (IOException e) {
				// ignore but log
				classLogger.error(Constants.STACKTRACE);
				classLogger.warn("After successful upload, unable to delete sql database file uploaded into temp location");
			}
		}
	}
	
	/**
	 * Update the existing database 
	 * @throws Exception
	 */
	private void updateExistingDatabase() throws Exception {
		this.logger.info("Bringing in metamodel");
		Owler owler = new Owler(this.database);
		Map<String, Map<String, SemossDataType>> existingMetamodel = UploadUtilities.getExistingMetamodel(owler);
		Map<String, Object> newMetamodel = UploadInputUtility.getMetamodel(this.store);
		if (newMetamodel == null) {
			throw new IllegalArgumentException("Must define the metamodel portions to change");
		}
		
		Map<String, List<String>> nodesAndProps = (Map<String, List<String>>) newMetamodel.get(ExternalJdbcSchemaReactor.TABLES_KEY);
		List<Map<String, Object>> relationships = (List<Map<String, Object>>) newMetamodel.get(ExternalJdbcSchemaReactor.RELATIONS_KEY);

		// separate table names from primary keys
		Set<String> cleanTables = new HashSet<String>();
		Map<String, String> nodesAndPrimKeys = new HashMap<String, String>();
		for (String t : nodesAndProps.keySet()) {
			cleanTables.add(t.split("\\.")[0]);
			nodesAndPrimKeys.put(t.split("\\.")[0], t.split("\\.")[1]);
		}

		Map<String, Map<String, String>> newRDBMSStructure = RDBMSEngineCreationHelper.getExistingRDBMSStructure(this.database, cleanTables);
		boolean metamodelsEqual = existingMetamodel.equals(newRDBMSStructure);

		// clean up/remove spaces and dashes in new metamodel
		newRDBMSStructure.forEach((tName, columnNames) -> {
			Map<String, String> cleanedColumns = new HashMap<>();
			columnNames.forEach((newColumnName, newDataType) -> {
				String cleanedName = RDBMSEngineCreationHelper.cleanTableName(newColumnName);
				cleanedColumns.put(cleanedName, newDataType);
			});
			newRDBMSStructure.replace(tName, cleanedColumns);
		});

		if (!metamodelsEqual) {
			this.logger.info("Checking differences in metamodel to remove");
			Map<String, String> removedProperties = new HashMap<>();
			RDFFileSesameEngine owlEngine = this.database.getBaseDataEngine();

			// loop through old tables and column names and remove them from existing metamodel
			existingMetamodel.forEach((existingTableName, columnsFromOld) -> {
				boolean tableRemoved = false;
				if (!newRDBMSStructure.containsKey(existingTableName)) {
					owler.removeConcept(this.databaseId, existingTableName, null);
					tableRemoved = true;
				}
				
				if (!tableRemoved) {
					Map<String, String> newColumnNames = newRDBMSStructure.get(existingTableName);
					columnsFromOld.forEach((existingColumnName, existingDataType) -> {
						if (!newColumnNames.containsKey(existingColumnName) || 
								SemossDataType.convertStringToDataType(newColumnNames.get(existingColumnName)) != existingDataType) {
							// track removed properties
							removedProperties.put(existingTableName, existingColumnName);
							this.logger.info("removing relationships from owl");
							removeRelationships(removedProperties, owlEngine);
							this.logger.info("removing properties from owl");
							owler.removeProp(existingTableName, existingColumnName, existingDataType + "", null, null);
						}
					});
				}
			});
			
			this.logger.info("Checking differences in metamodel to add");
			// loop through new tables and column names and add them in to existing metamodel
			newRDBMSStructure.forEach((newTableName, columnsFromNew) -> {
				this.logger.info("Adding table to OWL: " + Utility.cleanLogString(newTableName));
				if (!existingMetamodel.containsKey(newTableName)) {
					owler.addConcept(newTableName, null, null);
				}

				this.logger.info("Adding columns to OWL");
				columnsFromNew.forEach((newColumnName, newDataType) -> {
					owler.addProp(newTableName, newColumnName, newDataType, null, null);
				});

				this.logger.info("Parsing relationships and writing to OWL");
				parseRelationships(owler, relationships, newRDBMSStructure, nodesAndPrimKeys);
			});

			this.logger.info("committing and saving OWL");
			owler.commit();
			this.logger.info("writing changes to OWL");
			owler.export();
			this.logger.info("deleting OWL position map");
			File owlF = this.database.getOwlPositionFile();
			if(owlF.exists()) {
				owlF.delete();
			}
			
			// also clear caching that is stored for the database
			EngineSyncUtility.clearEngineCache(databaseId);
		}
	}

	private void removeRelationships(Map<String, String> removedProperties, RDFFileSesameEngine owlEngine) {
		List<String[]> fkRelationships = getPhysicalRelationships(owlEngine);

		for (String[] relations: fkRelationships) {
			String instanceName = Utility.getInstanceName(relations[2]);
			String[] tablesAndPrimaryKeys = instanceName.split("\\.");

			for (int i=0; i < tablesAndPrimaryKeys.length; i+=2) {
				String key = tablesAndPrimaryKeys[i], value = tablesAndPrimaryKeys[i+1], removedValue = removedProperties.get(key);

				if (removedValue != null && removedValue.equalsIgnoreCase(value)) {
					owlEngine.doAction(ACTION_TYPE.REMOVE_STATEMENT, new Object[] { relations[0], relations[2], relations[1], true });
					owlEngine.doAction(ACTION_TYPE.REMOVE_STATEMENT, new Object[] { relations[2], RDFS.SUBPROPERTYOF.toString(), "http://semoss.org/ontologies/Relation", true });
				}
			}
		}
	}

	private List<String[]> getPhysicalRelationships(IDatabaseEngine database) {
		String query = "SELECT DISTINCT ?start ?end ?rel WHERE { "
				+ "{?start <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?end <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> }"
				+ "{?rel <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>} " + "{?start ?rel ?end}"
				+ "Filter(?rel != <" + RDFS.SUBPROPERTYOF + ">)"
				+ "Filter(?rel != <http://semoss.org/ontologies/Relation>)" + "}";
		return Utility.getVectorArrayOfReturn(query, database, true);
	}
	
	/**
	 * Delete all the corresponding files that are generated from the upload the
	 * failed
	 */
	private void cleanUpCreateNewError() {
		// TODO:clean up DIHelper!
		try {
			// close the DB so we can delete it
			if (this.database != null) {
				database.close();
			}

			// delete the .temp file
			if (this.tempSmss != null && this.tempSmss.exists()) {
				FileUtils.forceDelete(this.tempSmss);
			}
			// delete the .smss file
			if (this.smssFile != null && this.smssFile.exists()) {
				FileUtils.forceDelete(this.smssFile);
			}
			// delete the database folder and all its contents
			if (this.databaseFolder != null && this.databaseFolder.exists()) {
				File[] files = this.databaseFolder.listFiles();
				if (files != null) { // some JVMs return null for empty dirs
					for (File f : files) {
						FileUtils.forceDelete(f);
					}
				}
				FileUtils.forceDelete(this.databaseFolder);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * Add the concepts and properties into the OWL
	 * 
	 * @param owler
	 * @param nodesAndProps
	 * @param dataTypes
	 * @return
	 */
	private Map<String, String> parseNodesAndProps(Owler owler, Map<String, List<String>> nodesAndProps, Map<String, Map<String, String>> dataTypes) {
		Map<String, String> nodesAndPrimKeys = new HashMap<String, String>(nodesAndProps.size());
		for (String node : nodesAndProps.keySet()) {
			String[] tableAndPrimaryKey = node.split("\\.");
			String nodeName = tableAndPrimaryKey[0];
			String primaryKey = tableAndPrimaryKey[1];
			nodesAndPrimKeys.put(nodeName, primaryKey);
			String cleanConceptTableName = RDBMSEngineCreationHelper.cleanTableName(nodeName);
			// add concepts
			owler.addConcept(cleanConceptTableName, null, null);
			owler.addProp(cleanConceptTableName, primaryKey, dataTypes.get(nodeName).get(primaryKey));
			// add concept properties
			for (String prop : nodesAndProps.get(node)) {
				if (!prop.equals(primaryKey)) {
					String cleanProp = RDBMSEngineCreationHelper.cleanTableName(prop);
					owler.addProp(cleanConceptTableName, cleanProp, dataTypes.get(nodeName).get(prop));
				}
			}
		}
		return nodesAndPrimKeys;
	}

	/**
	 * Add the relationships into the OWL
	 * 
	 * @param owler
	 * @param relationships
	 * @param dataTypes
	 * @param nodesAndPrimKeys
	 */
	private void parseRelationships(Owler owler, List<Map<String, Object>> relationships,
			Map<String, Map<String, String>> dataTypes, Map<String, String> nodesAndPrimKeys) {
		for (Map relation : relationships) {
			String subject = RDBMSEngineCreationHelper.cleanTableName(relation.get(Constants.FROM_TABLE).toString());
			String object = RDBMSEngineCreationHelper.cleanTableName(relation.get(Constants.TO_TABLE).toString());
			// TODO: check if this needs to be cleaned
			String[] joinColumns = relation.get(Constants.REL_NAME).toString().split("\\.");
			// predicate is: "fromTable.fromJoinCol.toTable.toJoinCol"
			String predicate = subject + "." + joinColumns[0] + "." + object + "." + joinColumns[1];
			owler.addRelation(subject, object, predicate);
		}
	}

	private Map<String, Object> getConDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.CONNECTION_DETAILS.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
			if(mapInput != null && !mapInput.isEmpty()) {
				return (Map<String, Object>) mapInput.get(0);
			}
		}
		
		List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
		if(mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
		}
		
		return null;
	}

	/**
	 * Validates JDBC properties and returns a LinkedHash of the properties while removing said
	 * properties from connection details. 
	 * 
	 * @param connectionDetails
	 * @return jdbcProperties
	 */
	private Map<String, Object> validateJDBCProperties(Map<String, Object> connectionDetails) {
		// keep an ordered map for the jdbc properties
		Map<String, Object> jdbcProperties = new LinkedHashMap<String, Object>();
		int minPool = -1;
		int maxPool = -1;
		for (String key : JDBC_CONSTANTS) {
			if(connectionDetails.containsKey(key)) {
				Object jdbcVal = connectionDetails.remove(key);
				// ignore empty string inputs
				if(jdbcVal.toString().isEmpty()) {
					continue;
				}
				jdbcProperties.put(key, jdbcVal);
				if (key.equals(Constants.USE_CONNECTION_POOLING)) {
					// boolean check
					String strBool = jdbcVal.toString();
					if(!(strBool.equalsIgnoreCase("false") || strBool.equalsIgnoreCase("true"))) {
						throw new IllegalArgumentException("Parameter " + key + " is not a valid boolean value");
					}
				} else {
					// currently all other parameter inputs are integer values
					// make sure it is a valid integer or turn it to an integer
					int integerInput = -1;
					if(jdbcVal instanceof Number) {
						integerInput = ((Number) jdbcVal).intValue();
					} else {
						try {
							integerInput = Integer.parseInt(jdbcVal + "");
						} catch(NumberFormatException e) {
							throw new IllegalArgumentException("Parameter " + key + " is not a valid number");
						}
					}
					
					// perform the integer check
					if(integerInput < 0) {
						throw new IllegalArgumentException("Paramter " + key + " must be a numeric value greater than 0");
					}
					
					// assign so we can do a final check for min/max pool size
					if(key.equals(Constants.POOL_MIN_SIZE)) {
						minPool = integerInput;
					} else if(key.equals(Constants.POOL_MAX_SIZE)) {
						maxPool = integerInput;
					}
				}
			}
		}
		// after check pool min/max size
		if(minPool > 0 && maxPool >0) {
			if (minPool > maxPool) {
				throw new IllegalArgumentException("Max pool size must be greater than min pool size");
			}
		}
		
		return jdbcProperties;
	}
}
	

