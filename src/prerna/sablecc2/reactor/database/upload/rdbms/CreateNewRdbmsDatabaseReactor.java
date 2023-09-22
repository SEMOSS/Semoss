package prerna.sablecc2.reactor.database.upload.rdbms;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.io.Files;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.DatabaseUpdateMetadata;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public abstract class CreateNewRdbmsDatabaseReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateNewRdbmsDatabaseReactor.class);

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String CLASS_NAME = CreateNewRdbmsDatabaseReactor.class.getName();

	private static final String[] JDBC_CONSTANTS = { Constants.USE_CONNECTION_POOLING, Constants.POOL_MIN_SIZE,
			Constants.POOL_MAX_SIZE, Constants.CONNECTION_QUERY_TIMEOUT, Constants.FETCH_SIZE };

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
	protected transient boolean internal = false;

	protected NounMetadata doExecute() {
		this.logger = getLogger(CLASS_NAME);
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
		if (AbstractSecurityUtils.adminSetPublisher()
				&& !SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
			throwUserNotPublisherError();
		}

		if (AbstractSecurityUtils.adminOnlyEngineAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();
		this.databaseName = UploadInputUtility.getDatabaseNameOrId(this.store);
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
			generateNewDatabase();
			// and rename .temp to .smss
			this.smssFile = new File(this.tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(this.tempSmss, this.smssFile);
			this.tempSmss.delete();
			this.database.setSmssFilePath(this.smssFile.getAbsolutePath());
			UploadUtilities.updateDIHelper(this.databaseId, this.databaseName, this.database, this.smssFile);
			// sync metadata
			this.logger.info("Process database metadata to allow for traversing across databases");
			UploadUtilities.updateMetadata(this.databaseId, user);
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

	private void generateNewDatabase() throws Exception {
		Map<String, Object> connectionDetails = getConDetails();
		if(connectionDetails != null) {
			String host = (String) connectionDetails.get(AbstractSqlQueryUtil.HOSTNAME);
			if(host != null) {
				String testUpdatedHost = this.insight.getAbsoluteInsightFolderPath(host);
				File f = new File(testUpdatedHost);
				if (f.exists()) {
					// move the file
					// and then update the host value
					String newLocation = this.databaseFolder.getAbsolutePath() + DIR_SEPARATOR
							+ FilenameUtils.getName(f.getAbsolutePath());
					try {
						Files.move(f, new File(newLocation));
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
		
		// handle internal vs external connection details
		connectionDetails = editConnectionDetails(connectionDetails, driverEnum);
		
		String connectionUrl = null;
		try {
			connectionUrl = queryUtil.setConnectionDetailsfromMap(connectionDetails);
		} catch (RuntimeException e) {
			throw new SemossPixelException(new NounMetadata("Unable to generation connection url with message " + e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		int stepCounter = 1;
		this.logger.info(stepCounter + ". Create metadata for database...");
		File owlFile = UploadUtilities.generateOwlFile(this.databaseId, this.databaseName);
		this.logger.info(stepCounter + ". Complete");
		stepCounter++;

		// the metamodel to build
		Map<String, Map<String, String>> newMetamodel = UploadInputUtility.getMetamodelAdditions(this.store);
		if (newMetamodel == null) {
			throw new IllegalArgumentException("Must define the metamodel portions we are uploading");
		}
		this.logger.info(stepCounter + ". Create properties file for database...");
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
		this.logger.info(stepCounter + ". Complete");
		stepCounter++;

		this.logger.info(stepCounter + ". Create database store...");
		database.setEngineId(this.databaseId);
		database.setEngineName(this.databaseName);
		database.setOWL(owlFile.getAbsolutePath());
		Properties prop = Utility.loadProperties(tempSmss.getAbsolutePath());
		prop.put("TEMP", "TRUE");
		database.open(prop);
		if(!database.isConnected()) {
			throw new IllegalArgumentException("Unable to connect to external database");
		}
		this.logger.info(stepCounter + ". Complete");
		stepCounter++;

		this.logger.info(stepCounter + ". Start generating database tables...");

		DatabaseUpdateMetadata dbUpdateMeta = AbstractSqlQueryUtil.performDatabaseAdditions((IRDBMSEngine) database, newMetamodel, logger);
		Owler owler = dbUpdateMeta.getOwler();
		String errorMessages = dbUpdateMeta.getCombinedErrors();
		if(!errorMessages.isEmpty()) {
			throw new IllegalArgumentException(errorMessages);
		}
		
		// now push the OWL and sync
		try {
			owler.export();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred savig the metadata file with the executed changes");
		}
		
		this.logger.info(stepCounter + ". Complete");
		stepCounter++;
	}

	protected abstract Map<String, Object> editConnectionDetails(Map<String, Object> connectionDetails, RdbmsTypeEnum driverEnum);

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

	private Map<String, Object> getConDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.CONNECTION_DETAILS.getKey());
		if (grs != null && !grs.isEmpty()) {
			List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
			if (mapInput != null && !mapInput.isEmpty()) {
				return (Map<String, Object>) mapInput.get(0);
			}
		}

		List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
		if (mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
		}

		return null;
	}

	/**
	 * Validates JDBC properties and returns a LinkedHash of the properties while
	 * removing said properties from connection details.
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
			if (connectionDetails.containsKey(key)) {
				Object jdbcVal = connectionDetails.remove(key);
				// ignore empty string inputs
				if (jdbcVal.toString().isEmpty()) {
					continue;
				}
				jdbcProperties.put(key, jdbcVal);
				if (key.equals(Constants.USE_CONNECTION_POOLING)) {
					// boolean check
					String strBool = jdbcVal.toString();
					if (!(strBool.equalsIgnoreCase("false") || strBool.equalsIgnoreCase("true"))) {
						throw new IllegalArgumentException("Parameter " + key + " is not a valid boolean value");
					}
				} else {
					// currently all other parameter inputs are integer values
					// make sure it is a valid integer or turn it to an integer
					int integerInput = -1;
					if (jdbcVal instanceof Number) {
						integerInput = ((Number) jdbcVal).intValue();
					} else {
						try {
							integerInput = Integer.parseInt(jdbcVal + "");
						} catch (NumberFormatException e) {
							throw new IllegalArgumentException("Parameter " + key + " is not a valid number");
						}
					}

					// perform the integer check
					if (integerInput < 0) {
						throw new IllegalArgumentException(
								"Paramter " + key + " must be a numeric value greater than 0");
					}

					// assign so we can do a final check for min/max pool size
					if (key.equals(Constants.POOL_MIN_SIZE)) {
						minPool = integerInput;
					} else if (key.equals(Constants.POOL_MAX_SIZE)) {
						maxPool = integerInput;
					}
				}
			}
		}
		// after check pool min/max size
		if (minPool > 0 && maxPool > 0) {
			if (minPool > maxPool) {
				throw new IllegalArgumentException("Max pool size must be greater than min pool size");
			}
		}

		return jdbcProperties;
	}

}
