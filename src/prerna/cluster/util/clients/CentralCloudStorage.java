package prerna.cluster.util.clients;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.LegacyToProjectRestructurerHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.storage.AbstractRCloneStorageEngine;
import prerna.engine.impl.storage.AzureBlobStorageEngine;
import prerna.engine.impl.storage.GoogleCloudStorageEngine;
import prerna.engine.impl.storage.MinioStorageEngine;
import prerna.engine.impl.storage.S3StorageEngine;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.test.TestUtilityMethods;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.ProjectSyncUtility;
import prerna.util.SMSSModelWatcher;
import prerna.util.SMSSStorageWatcher;
import prerna.util.SMSSWebWatcher;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class CentralCloudStorage implements ICloudClient {

	private static final Logger classLogger = LogManager.getLogger(CentralCloudStorage.class);

	public static final String DB_BLOB = "semoss-db";
	public static final String STORAGE_BLOB = "semoss-storage";
	public static final String MODEL_BLOB = "semoss-model";
	public static final String PROJECT_BLOB = "semoss-project";
	public static final String USER_BLOB = "semoss-user";
	public static final String DB_IMAGES_BLOB = "semoss-dbimagecontainer";
	public static final String STORAGE_IMAGES_BLOB = "semoss-storageimagecontainer";
	public static final String MODEL_IMAGES_BLOB = "semoss-modelimagecontainer";
	public static final String PROJECT_IMAGES_BLOB = "semoss-projectimagecontainer";

	private static ICloudClient instance = null;
	private static AbstractRCloneStorageEngine centralStorageEngine = null;
	
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String SMSS_POSTFIX = "-smss";

	private static String DATABASE_FOLDER = null;
	private static String STORAGE_FOLDER = null;
	private static String MODEL_FOLDER = null;
	private static String PROJECT_FOLDER = null;
	private static String USER_FOLDER = null;
	
	// these can change based on the cloud client type
	private static String DB_CONTAINER_PREFIX = "/" + DB_BLOB + "/";
	private static String STORAGE_CONTAINER_PREFIX = "/" + STORAGE_BLOB + "/";
	private static String MODEL_CONTAINER_PREFIX = "/" + MODEL_BLOB + "/";
	private static String PROJECT_CONTAINER_PREFIX = "/" + PROJECT_BLOB + "/";
	private static String USER_CONTAINER_PREFIX = "/" + USER_BLOB + "/";
	
	// these are all the legacy keys before we had engine
	// need to do some mapping between these ones and the ones used 
	// in the storage engine classes
	// will keep all of these indendted to identify that
//		public static String RCLONE_PATH = "RCLONE_PATH";
//	
//		// s3 prefix is used for minio as well
//		public static final String S3_REGION_KEY = "S3_REGION";
//		public static final String S3_BUCKET_KEY = "S3_BUCKET";
//		public static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
//		public static final String S3_SECRET_KEY = "S3_SECRET_KEY";
//		public static final String S3_ENDPOINT_KEY = "S3_ENDPOINT";
//		
//		// gcp keys
//		public static final String GCP_SERVICE_ACCOUNT_FILE_KEY = "GCP_SERVICE_ACCOUNT_FILE";
//		public static final String GCP_REGION_KEY = "GCP_REGION";
//		public static final String GCP_BUCKET_KEY = "GCP_BUCKET";
//		
//		// az keys
//		public static final String AZ_CONN_STRING = "AZ_CONN_STRING";
//		public static final String AZ_NAME = "AZ_NAME";
//		public static final String AZ_KEY = "AZ_KEY";
//		public static final String SAS_URL = "SAS_URL";
//		public static final String AZ_URI = "AZ_URI";
//		public static final String STORAGE = "STORAGE"; // says if this is local / cluster
//		public static final String KEY_HOME = "KEY_HOME"; // this is where the various keys are cycled
//	
//		protected String rclonePath = "rclone";
//		protected String rcloneConfigF = null;
	
	private CentralCloudStorage() throws Exception {
		buildStorageEngine();
	}
	
	public static ICloudClient getInstance() throws Exception {
		if(instance != null) {
			return instance;
		}
		
		if(instance == null) {
			synchronized (CentralCloudStorage.class) {
				if(instance != null) {
					return instance;
				}
				
				instance = new CentralCloudStorage();
				CentralCloudStorage.DATABASE_FOLDER = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.DB_FOLDER;
				CentralCloudStorage.STORAGE_FOLDER = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.STORAGE_FOLDER;
				CentralCloudStorage.MODEL_FOLDER = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.MODEL_FOLDER;
				CentralCloudStorage.PROJECT_FOLDER = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.PROJECT_FOLDER;
				CentralCloudStorage.USER_FOLDER = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.USER_FOLDER;
			}
		}
		
		return instance;
	}
	
	
	private static synchronized void buildStorageEngine() throws Exception {
		Properties props = new Properties();
		AppCloudClientProperties clientProps = new AppCloudClientProperties();
		if(ClusterUtil.STORAGE_PROVIDER == null || ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("AZURE")){
			
			centralStorageEngine = new AzureBlobStorageEngine();
			propertiesMigratePut(props, AzureBlobStorageEngine.AZ_ACCOUNT_NAME, clientProps, AbstractClientBuilder.AZ_NAME);
			propertiesMigratePut(props, AzureBlobStorageEngine.AZ_PRIMARY_KEY, clientProps, AbstractClientBuilder.AZ_KEY);
			propertiesMigratePut(props, AzureBlobStorageEngine.AZ_CONN_STRING, clientProps, AbstractClientBuilder.AZ_CONN_STRING);
			
			// we have a different structure for AZ storage since it doesn't represent the blobs as folders
			CentralCloudStorage.DB_CONTAINER_PREFIX = "db-";
			CentralCloudStorage.STORAGE_CONTAINER_PREFIX = "semoss-storage";
			CentralCloudStorage.MODEL_CONTAINER_PREFIX = "semoss-model";
			CentralCloudStorage.PROJECT_CONTAINER_PREFIX = "project-";
			CentralCloudStorage.USER_CONTAINER_PREFIX = "user-";
			
		}
		else if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("AWS") ||
				ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("S3")){
			
			centralStorageEngine = new S3StorageEngine();
			propertiesMigratePut(props, S3StorageEngine.S3_REGION_KEY, clientProps, AbstractClientBuilder.S3_REGION_KEY);
			propertiesMigratePut(props, S3StorageEngine.S3_BUCKET_KEY, clientProps, AbstractClientBuilder.S3_BUCKET_KEY);
			propertiesMigratePut(props, S3StorageEngine.S3_ACCESS_KEY, clientProps, AbstractClientBuilder.S3_ACCESS_KEY);
			propertiesMigratePut(props, S3StorageEngine.S3_SECRET_KEY, clientProps, AbstractClientBuilder.S3_SECRET_KEY);

		} 
		else if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("MINIO")){
			
			centralStorageEngine = new MinioStorageEngine();
			propertiesMigratePut(props, MinioStorageEngine.MINIO_REGION_KEY, clientProps, AbstractClientBuilder.S3_REGION_KEY);
			propertiesMigratePut(props, MinioStorageEngine.MINIO_BUCKET_KEY, clientProps, AbstractClientBuilder.S3_BUCKET_KEY);
			propertiesMigratePut(props, MinioStorageEngine.MINIO_ACCESS_KEY, clientProps, AbstractClientBuilder.S3_ACCESS_KEY);
			propertiesMigratePut(props, MinioStorageEngine.MINIO_SECRET_KEY, clientProps, AbstractClientBuilder.S3_SECRET_KEY);
			propertiesMigratePut(props, MinioStorageEngine.MINIO_ENDPOINT_KEY, clientProps, AbstractClientBuilder.S3_ENDPOINT_KEY);

			if(!props.containsKey(MinioStorageEngine.MINIO_REGION_KEY)) {
				propertiesMigratePut(props, MinioStorageEngine.MINIO_REGION_KEY, clientProps, MinioStorageEngine.MINIO_REGION_KEY);
			}
			if(!props.containsKey(MinioStorageEngine.MINIO_BUCKET_KEY)) {
				propertiesMigratePut(props, MinioStorageEngine.MINIO_BUCKET_KEY, clientProps, MinioStorageEngine.MINIO_BUCKET_KEY);
			}
			if(!props.containsKey(MinioStorageEngine.MINIO_ACCESS_KEY)) {
				propertiesMigratePut(props, MinioStorageEngine.MINIO_ACCESS_KEY, clientProps, MinioStorageEngine.MINIO_ACCESS_KEY);
			}
			if(!props.containsKey(MinioStorageEngine.MINIO_SECRET_KEY)) {
				propertiesMigratePut(props, MinioStorageEngine.MINIO_SECRET_KEY, clientProps, MinioStorageEngine.MINIO_SECRET_KEY);
			}
			if(!props.containsKey(MinioStorageEngine.MINIO_ENDPOINT_KEY)) {
				propertiesMigratePut(props, MinioStorageEngine.MINIO_ENDPOINT_KEY, clientProps, MinioStorageEngine.MINIO_ENDPOINT_KEY);
			}
		} 
		else if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("GCS") ||
				ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("GCP") ||
				ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("GOOGLE")){
			
			centralStorageEngine = new GoogleCloudStorageEngine();
			propertiesMigratePut(props, GoogleCloudStorageEngine.GCS_REGION, clientProps, AbstractClientBuilder.GCP_REGION_KEY);
			propertiesMigratePut(props, GoogleCloudStorageEngine.GCS_SERVICE_ACCOUNT_FILE_KEY, clientProps, AbstractClientBuilder.GCP_SERVICE_ACCOUNT_FILE_KEY);
			propertiesMigratePut(props, GoogleCloudStorageEngine.GCS_BUCKET_KEY, clientProps, AbstractClientBuilder.GCP_BUCKET_KEY);
			
		}
		else {
			throw new IllegalArgumentException("You have specified an incorrect storage provider");
		}
		
		props.put(Constants.ENGINE, "CENTRAL_CLOUD_STORAGE");
		centralStorageEngine.open(props);
	}
	
	/**
	 * 
	 * @param prop
	 * @param propKey
	 * @param clientProps
	 * @param oldKey
	 */
	private static void propertiesMigratePut(Properties prop, String propKey, AppCloudClientProperties clientProps, String oldKey) {
		if(clientProps.get(oldKey) != null) {
			prop.put(propKey, clientProps.get(oldKey));
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////

	/**
	 * 
	 * @param type
	 * @return
	 */
	public String getStoragePrefixForEngine(IEngine.CATALOG_TYPE type) {
		if(IEngine.CATALOG_TYPE.DATABASE == type) {
			return DB_CONTAINER_PREFIX;
		} else if(IEngine.CATALOG_TYPE.STORAGE == type) {
			return STORAGE_CONTAINER_PREFIX;
		} else if(IEngine.CATALOG_TYPE.MODEL == type) {
			return MODEL_CONTAINER_PREFIX;
		} else if(IEngine.CATALOG_TYPE.PROJECT == type) {
			return PROJECT_CONTAINER_PREFIX;
		}
		
		throw new IllegalArgumentException("Unhandled engine type = " + type);
	}
	
	/**
	 * 
	 * @param type
	 * @return
	 */
	public String getEngineBaseDirectory(IEngine.CATALOG_TYPE type) {
		if(IEngine.CATALOG_TYPE.DATABASE == type) {
			return DATABASE_FOLDER;
		} else if(IEngine.CATALOG_TYPE.STORAGE == type) {
			return STORAGE_FOLDER;
		} else if(IEngine.CATALOG_TYPE.MODEL == type) {
			return MODEL_FOLDER;
		} else if(IEngine.CATALOG_TYPE.PROJECT == type) {
			return PROJECT_FOLDER;
		}
		
		throw new IllegalArgumentException("Unhandled engine type = " + type);
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	
	
	@Override
	public void pushDatabase(String databaseId) throws IOException, InterruptedException {
		pushEngine(databaseId);
	}
	
	@Override
	public void pushStorage(String storageId) throws IOException, InterruptedException {
		pushEngine(storageId);
	}
	
	@Override
	public void pushModel(String modelId) throws IOException, InterruptedException {
		pushEngine(modelId);
	}
	
	public void pushEngine(String engineId) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(engineId, false);
		if (engine == null) {
			throw new IllegalArgumentException("Engine not found...");
		}

		IEngine.CATALOG_TYPE engineType = engine.getCatalogType();

		String engineName = SecurityEngineUtils.getEngineAliasForId(engineId);
		String aliasAndEngineId = SmssUtilities.getUniqueName(engineName, engineId);
		
		String localEngineBaseFolder = getEngineBaseDirectory(engineType);
		String localEngineFolder = localEngineBaseFolder + FILE_SEPARATOR + aliasAndEngineId;
		{
			// lets make sure this exists
			File localEngineF = new File(localEngineFolder);
			if(!localEngineF.exists() || !localEngineF.isDirectory()) {
				localEngineF.mkdirs();
			}
			ClusterUtil.validateFolder(localEngineFolder);
		}
		String localSmssFileName = aliasAndEngineId + ".smss";
		String localSmssFilePath = localEngineBaseFolder + FILE_SEPARATOR + localSmssFileName;

		String sharedRCloneConfig = null;

		String storageContainerPrefix = getStoragePrefixForEngine(engineType);
		String storageEngineFolder = storageContainerPrefix + engineId;
		String storageSmssFolder = storageContainerPrefix + engineId + SMSS_POSTFIX;

		// synchronize on the engine id
		classLogger.info("Applying lock for " + aliasAndEngineId + " to push engine " + aliasAndEngineId);
		ReentrantLock lock = EngineSyncUtility.getEngineLock(engineId);
		lock.lock();
		classLogger.info("Engine "+ aliasAndEngineId + " is locked");
		try {
			if(engine.holdsFileLocks()) {
				DIHelper.getInstance().removeEngineProperty(engineId);
				engine.close();
			}
			
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			centralStorageEngine.syncLocalToStorage(localEngineFolder, storageEngineFolder, sharedRCloneConfig);
			centralStorageEngine.copyToStorage(localSmssFilePath, storageSmssFolder, sharedRCloneConfig);
		} finally {
			try {
				// Re-open the engine
				if(engine.holdsFileLocks()) {
					Utility.getEngine(engineId, false);
				}
				if(sharedRCloneConfig != null) {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				}
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Engine "+ aliasAndEngineId + " is unlocked");
		}
	}
	
	/*
	 * Database
	 */
	
	
	@Override
	public void pushLocalDatabaseFile(String databaseId, RdbmsTypeEnum dbType) throws IOException, InterruptedException {
		if (dbType != RdbmsTypeEnum.SQLITE
				&& dbType != RdbmsTypeEnum.H2_DB) {
			throw new IllegalArgumentException("Unallowed database type. Must be either SQLITE or H2");
		}
		
		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
		if (database == null) {
			throw new IllegalArgumentException("Database not found...");
		}
		
		// We need to push the folder alias__databaseId and the file alias__databaseId.smss
		String databaseName = SecurityEngineUtils.getEngineAliasForId(databaseId);
		String aliasAndDatabaseId = SmssUtilities.getUniqueName(databaseName, databaseId);
		String localDatabaseFolder = DATABASE_FOLDER + FILE_SEPARATOR + aliasAndDatabaseId;
		
		String storageDatabaseFolder = DB_CONTAINER_PREFIX + databaseId;
		
		// synchronize on the engine id
		classLogger.info("Applying lock for " + aliasAndDatabaseId + " to push db file");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
		lock.lock();
		classLogger.info("Database "+ aliasAndDatabaseId + " is locked");
		try {
			DIHelper.getInstance().removeEngineProperty(databaseId);
			database.close();

			classLogger.info("Pushing local database file from " + localDatabaseFolder + " to remote " + storageDatabaseFolder);
			List<String> dbFiles = null;
			if (dbType == RdbmsTypeEnum.SQLITE) {
				dbFiles = getSqlLiteFile(localDatabaseFolder);
			} else if (dbType == RdbmsTypeEnum.H2_DB) {
				dbFiles = getH2File(localDatabaseFolder);
			}
			for (String dbFileName : dbFiles) {
				centralStorageEngine.copyToStorage(localDatabaseFolder+"/"+dbFileName, storageDatabaseFolder);
			}
		} finally {
			try {
				// Re-open the database
				Utility.getDatabase(databaseId, false);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Database "+ aliasAndDatabaseId + " is unlocked");
		}
	}

	@Override
	public void pullDatabase(String databaseId) throws IOException, InterruptedException {
		pullDatabase(databaseId, false);
	}

	@Override
	public void pullDatabase(String databaseId, boolean databaseAlreadyLoaded) throws IOException, InterruptedException {
		IDatabaseEngine database = null;
		if (databaseAlreadyLoaded) {
			database = Utility.getDatabase(databaseId, false);
			if (database == null) {
				throw new IllegalArgumentException("Database not found...");
			}
		}

		// We need to pull the folder alias__databaseId and the file alias__databaseId.smss
		String databaseName = SecurityEngineUtils.getEngineAliasForId(databaseId);
		String aliasAndDatabaseId = SmssUtilities.getUniqueName(databaseName, databaseId);
		String localDatabaseFolder = DATABASE_FOLDER + FILE_SEPARATOR + aliasAndDatabaseId;

		String storageDatabaseFolder = DB_CONTAINER_PREFIX + databaseId;
		String storageSmssFolder = DB_CONTAINER_PREFIX + databaseId + SMSS_POSTFIX;

		String sharedRCloneConfig = null;

		// synchronize on the engine id
		classLogger.info("Applying lock for " + aliasAndDatabaseId + " to push database");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
		lock.lock();
		classLogger.info("Database "+ aliasAndDatabaseId + " is locked");
		
		try {
			if(database != null) {
				DIHelper.getInstance().removeEngineProperty(databaseId);
				database.close();
			}
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			
			// List the smss directory to get the alias + database id
			List<String> results = centralStorageEngine.list(storageSmssFolder, sharedRCloneConfig);
			boolean foundSmss = false;
			for (String result : results) {
				if (result.endsWith(".smss")) {
					foundSmss = true;
					break;
				}
			}
			if (!foundSmss) {
				classLogger.warn("Could not find smss file for database " + aliasAndDatabaseId + " in remote="+storageSmssFolder);
				try {
					classLogger.warn("Attemping to fix legacy database structure for database " + aliasAndDatabaseId);
					classLogger.warn("Attemping to fix legacy database structure for database " + aliasAndDatabaseId);
					classLogger.warn("Attemping to fix legacy database structure for database " + aliasAndDatabaseId);
					classLogger.warn("Attemping to fix legacy database structure for database " + aliasAndDatabaseId);
					fixLegacyDbStructure(databaseId);
				} catch(IOException | InterruptedException e) {
					classLogger.info(Constants.STACKTRACE, e);
					throw new IOException("Failed to pull database " + aliasAndDatabaseId);
				}
				
				// try again
				results = centralStorageEngine.list(storageSmssFolder, sharedRCloneConfig);
				for (String result : results) {
					if (result.endsWith(".smss")) {
						foundSmss = true;
						break;
					}
				}
				
				if (!foundSmss) {
					classLogger.warn("Could not find smss file for database " + aliasAndDatabaseId);
					throw new IOException("Failed to pull database " + aliasAndDatabaseId);
				} else {
					// we just fixed the structure and this was pulled and synched up
					// can just return from here
					return;
				}
			}
			
			// Close the database so that we can pull without file lock issues
			if (databaseAlreadyLoaded) {
				DIHelper.getInstance().removeEngineProperty(databaseId);
				database.close();
			}

			// Make the database directory (if it doesn't already exist)
			File localDatabaseF = new File(Utility.normalizePath(localDatabaseFolder));
			if(!localDatabaseF.exists() || !localDatabaseF.isDirectory()) {
				localDatabaseF.mkdirs(); 
			}

			// Pull the contents of the database folder before the smss
			classLogger.info("Pulling database from remote=" + Utility.cleanLogString(aliasAndDatabaseId) + " to target=" + Utility.cleanLogString(localDatabaseFolder));
			centralStorageEngine.syncStorageToLocal(storageDatabaseFolder, localDatabaseFolder, sharedRCloneConfig);
			classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(aliasAndDatabaseId) + " to target=" + Utility.cleanLogString(localDatabaseFolder));

			// Now pull the smss
			classLogger.info("Pulling smss from remote=" + Utility.cleanLogString(storageSmssFolder) + " to target=" + DATABASE_FOLDER);
			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
			centralStorageEngine.copyToLocal(storageSmssFolder, DATABASE_FOLDER, sharedRCloneConfig);
			classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(storageSmssFolder) + " to target=" + DATABASE_FOLDER);

			// Catalog the db if it is new
			if (!databaseAlreadyLoaded) {
				classLogger.info("Synchronizing the database metadata for " + aliasAndDatabaseId);
				SMSSWebWatcher.catalogEngine(results.get(0), DATABASE_FOLDER);
			}
		} finally {
			try {
				// Re-open the database
				Utility.getDatabase(databaseId, false);
				if(sharedRCloneConfig != null) {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				}
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Database "+ aliasAndDatabaseId + " is unlocked");
		}
	}
	
	@Override
	public void pullLocalDatabaseFile(String databaseId, RdbmsTypeEnum rdbmsType) throws IOException, InterruptedException {
		if (rdbmsType != RdbmsTypeEnum.SQLITE
				&& rdbmsType != RdbmsTypeEnum.H2_DB) {
			throw new IllegalArgumentException("Unallowed database type. Must be either SQLITE or H2");
		}
		
		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
		if (database == null) {
			throw new IllegalArgumentException("Database not found...");
		}
		String databaseName = SecurityEngineUtils.getEngineAliasForId(databaseId);
		String aliasAndDatabaseId = SmssUtilities.getUniqueName(databaseName, databaseId);
		String localDatabaseFolder = DATABASE_FOLDER + FILE_SEPARATOR + aliasAndDatabaseId;

		String storageDatabaseFolder = DB_CONTAINER_PREFIX + databaseId;

		String sharedRCloneConfig = null;

		// synchronize on the engine id
		classLogger.info("Applying lock for " + aliasAndDatabaseId + " to pull database file");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
		lock.lock();
		classLogger.info("Database "+ databaseId + " is locked");
		try {
			DIHelper.getInstance().removeEngineProperty(databaseId);
			database.close();
			
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			
			classLogger.info("Pulling database files for " + aliasAndDatabaseId + " from remote=" + databaseId);
			List<String> filesToPull = new ArrayList<>();
			List<String> cloudFiles = centralStorageEngine.list(storageDatabaseFolder, sharedRCloneConfig);
			for(String cloudF : cloudFiles) {
				if(rdbmsType == RdbmsTypeEnum.SQLITE && cloudF.endsWith(".sqlite")) {
					filesToPull.add(cloudF);	
				} else if(rdbmsType == RdbmsTypeEnum.H2_DB && cloudF.endsWith(".mv.db")) {
					filesToPull.add(cloudF);	
				}
			}
			
			for(String fileToPull : filesToPull) {
				centralStorageEngine.copyToLocal(storageDatabaseFolder+"/"+fileToPull, localDatabaseFolder, sharedRCloneConfig);
			}
		} finally {
			try {
				// Re-open the database
				Utility.getDatabase(databaseId, false);
				if(sharedRCloneConfig != null) {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				}
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Database "+ aliasAndDatabaseId + " is unlocked");
		}
	}

	@Override
	public void pushDatabaseSmss(String databaseId) throws IOException, InterruptedException {
		// We need to push the file alias__databaseId.smss
		String databaseName = SecurityEngineUtils.getEngineAliasForId(databaseId);
		String aliasAndDatabaseId = SmssUtilities.getUniqueName(databaseName, databaseId);
		String localSmssFileName = SmssUtilities.getUniqueName(databaseName, databaseId) + ".smss";
		String localSmssFilePath = Utility.normalizePath(DATABASE_FOLDER + FILE_SEPARATOR + localSmssFileName);
		
		String storageSmssFolder = DB_CONTAINER_PREFIX + databaseId + SMSS_POSTFIX;

		// synchronize on the engine id
		classLogger.info("Applying lock for " + aliasAndDatabaseId + " to push database");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
		lock.lock();
		classLogger.info("Database " + aliasAndDatabaseId + " is locked");
		try {
			centralStorageEngine.copyToStorage(localSmssFilePath, storageSmssFolder);
		} finally {
			lock.unlock();
			classLogger.info("Database " + aliasAndDatabaseId + " is unlocked");
		}
	}
	
	@Override
	public void pullDatabaseSmss(String databaseId) throws IOException, InterruptedException {
		// We need to push the file alias__databaseId.smss
		String databaseName = SecurityEngineUtils.getEngineAliasForId(databaseId);
		String aliasAndDatabaseId = SmssUtilities.getUniqueName(databaseName, databaseId);
		
		String storageSmssFolder = DB_CONTAINER_PREFIX + databaseId + SMSS_POSTFIX;

		// synchronize on the engine id
		classLogger.info("Applying lock for " + aliasAndDatabaseId + " to push database smss");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
		lock.lock();
		classLogger.info("Database " + aliasAndDatabaseId + " is locked");
		try {
			centralStorageEngine.copyToLocal(storageSmssFolder, DATABASE_FOLDER);
		} finally {
			lock.unlock();
			classLogger.info("Database " + aliasAndDatabaseId + " is unlocked");
		}
	}

	@Override
	public void pushOwl(String databaseId) throws IOException, InterruptedException {
		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
		if (database == null) {
			throw new IllegalArgumentException("Database not found...");
		}
		
		String databaseName = SecurityEngineUtils.getEngineAliasForId(databaseId);
		String aliasAndDatabaseId = SmssUtilities.getUniqueName(databaseName, databaseId);
		File localOwlF = SmssUtilities.getOwlFile(database.getSmssProp());
		String localOwlFile = localOwlF.getAbsolutePath();
		String localOwlPositionFile = localOwlF.getParent() + "/" + AbstractDatabaseEngine.OWL_POSITION_FILENAME;
		boolean hasPositionFile = new File(localOwlPositionFile).exists();
		
		String storageDatabaseFolder = DB_CONTAINER_PREFIX + databaseId;

		String sharedRCloneConfig = null;

		// synchronize on the engine id
		classLogger.info("Applying lock for " + aliasAndDatabaseId + " to push database owl and postions.json");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
		lock.lock();
		classLogger.info("Database " + aliasAndDatabaseId + " is locked");
		try {
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			//close the owl
			database.getBaseDataEngine().close();
			centralStorageEngine.copyToStorage(localOwlFile, storageDatabaseFolder, sharedRCloneConfig);
			if(hasPositionFile) {
				centralStorageEngine.copyToStorage(localOwlPositionFile, storageDatabaseFolder, sharedRCloneConfig);
			}
		} finally {
			try {
				database.setOWL(localOwlFile);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			if(sharedRCloneConfig != null) {
				try {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Database "+ aliasAndDatabaseId + " is unlocked");
		}
	}

	@Override
	public void pullOwl(String databaseId) throws IOException, InterruptedException {
		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
		if (database == null) {
			throw new IllegalArgumentException("Database not found...");
		}
		
		String databaseName = SecurityEngineUtils.getEngineAliasForId(databaseId);
		String aliasAndDatabaseId = SmssUtilities.getUniqueName(databaseName, databaseId);
		String localDatabaseFolder = DATABASE_FOLDER + FILE_SEPARATOR + aliasAndDatabaseId;

		File localOwlF = SmssUtilities.getOwlFile(database.getSmssProp());
		String localOwlFile = localOwlF.getAbsolutePath();
		String owlFileName = localOwlF.getName();
		
		String storageDatabaseFolder = DB_CONTAINER_PREFIX + databaseId;
		String storageDatabaseOwl = storageDatabaseFolder + "/" + owlFileName;
		String storageDatabaseOwlPosition = storageDatabaseFolder + "/" + AbstractDatabaseEngine.OWL_POSITION_FILENAME;
		
		String sharedRCloneConfig = null;

		// synchronize on the engine id
		classLogger.info("Applying lock for " + aliasAndDatabaseId + " to pull database owl and postions.json");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
		lock.lock();
		classLogger.info("Database " + aliasAndDatabaseId + " is locked");
		try {
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			//close the owl
			database.getBaseDataEngine().close();
			centralStorageEngine.copyToLocal(storageDatabaseOwl, localDatabaseFolder);
			centralStorageEngine.copyToLocal(storageDatabaseOwlPosition, localDatabaseFolder);
		} finally {
			try {
				database.setOWL(localOwlFile);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			if(sharedRCloneConfig != null) {
				try {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Database "+ aliasAndDatabaseId + " is unlocked");
		}
	}

	@Override
	public void pullDatabaseImageFolder() throws IOException, InterruptedException {
		String sharedRCloneConfig = null;
		try {
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			List<String> results = centralStorageEngine.list(CentralCloudStorage.DB_IMAGES_BLOB, sharedRCloneConfig);
			if(results.isEmpty()) {
				fixLegacyImageStructure();
				return;
			}
			
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String localImagesFolderPath = baseFolder + "/images/databases";
			File localImageF = new File(localImagesFolderPath);
			if(!localImageF.exists() || !localImageF.isDirectory()) {
				localImageF.mkdirs();
			}
			centralStorageEngine.copyToLocal(CentralCloudStorage.DB_IMAGES_BLOB, localImagesFolderPath);
		} finally {
			if(sharedRCloneConfig != null) {
				try {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	@Override
	public void pushDatabaseImageFolder() throws IOException, InterruptedException {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String localImagesFolderPath = baseFolder + "/images/databases";
		File localImageF = new File(localImagesFolderPath);
		if(!localImageF.exists() || !localImageF.isDirectory()) {
			localImageF.mkdirs();
		}
		centralStorageEngine.syncLocalToStorage(localImagesFolderPath, CentralCloudStorage.DB_IMAGES_BLOB);
	}
	
	@Override
	public void deleteDatabase(String databaseId) throws IOException, InterruptedException {
		String sharedRCloneConfig = null;
		if(centralStorageEngine.canReuseRcloneConfig()) {
			sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
		}
		String storageDatabaseFolder = DB_CONTAINER_PREFIX + databaseId;
		String storageSmssFolder = DB_CONTAINER_PREFIX + databaseId + SMSS_POSTFIX;

		centralStorageEngine.deleteFolderFromStorage(storageDatabaseFolder, sharedRCloneConfig);
		centralStorageEngine.deleteFolderFromStorage(storageSmssFolder, sharedRCloneConfig);
	}

	@Override
	public void pushDatabaseFolder(String databaseId, String localAbsoluteFilePath, String storageRelativePath) throws IOException, InterruptedException {
		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
		if (database == null) {
			throw new IllegalArgumentException("Database not found...");
		}
		
		if(storageRelativePath != null) {
			storageRelativePath = storageRelativePath.replace("\\", "/");
		}
		if(storageRelativePath.startsWith("/")) {
			storageRelativePath = storageRelativePath.substring(1);
		}
		
		String databaseName = SecurityEngineUtils.getEngineAliasForId(databaseId);
		String aliasAndDatabaseId = SmssUtilities.getUniqueName(databaseName, databaseId);

		File absoluteFolder = new File(localAbsoluteFilePath);
		if(absoluteFolder.isDirectory()) {
			//this is adding a hidden file into every sub folder to make sure there is no empty directory
			ClusterUtil.validateFolder(absoluteFolder.getAbsolutePath());
		}
		
		String storageDatabaseFolderPath = DB_CONTAINER_PREFIX + databaseId;
		if(storageRelativePath != null) {
			storageDatabaseFolderPath = storageDatabaseFolderPath + "/" + storageRelativePath;
		}
		
		// adding a lock for now, but there may be times we don't need one and other times we do
		// reaching h2 db from version folder vs static assets in asset database
		// might need to also close if embedded engine?
		
		classLogger.info("Applying lock for database " + aliasAndDatabaseId + " to push database relative folder " + storageRelativePath);
		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
		lock.lock();
		classLogger.info("Database " + aliasAndDatabaseId + " is locked");
		try {
			classLogger.info("Pushing folder local=" + localAbsoluteFilePath + " to from remote=" + storageDatabaseFolderPath);
			centralStorageEngine.syncLocalToStorage(localAbsoluteFilePath, storageDatabaseFolderPath);
		} finally {
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Database " + aliasAndDatabaseId + " is unlocked");
		}
	}

	@Override
	public void pullDatabaseFolder(String databaseId, String localAbsoluteFilePath, String storageRelativePath) throws IOException, InterruptedException {
		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
		if (database == null) {
			throw new IllegalArgumentException("Database not found...");
		}
		if(storageRelativePath != null) {
			storageRelativePath = storageRelativePath.replace("\\", "/");
		}
		if(storageRelativePath.startsWith("/")) {
			storageRelativePath = storageRelativePath.substring(1);
		}
		
		String databaseName = SecurityEngineUtils.getEngineAliasForId(databaseId);
		String aliasAndDatabaseId = SmssUtilities.getUniqueName(databaseName, databaseId);
		
		String storageDatabaseFolderPath = DB_CONTAINER_PREFIX + databaseId;
		if(storageRelativePath != null) {
			storageDatabaseFolderPath = storageDatabaseFolderPath + "/" + storageRelativePath;
		}
		classLogger.info("Applying lock for database " + aliasAndDatabaseId + " to pull database relative folder " + storageRelativePath);
		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
		lock.lock();
		classLogger.info("Database " + aliasAndDatabaseId + " is locked");
		try {
			classLogger.info("Pulling folder from remote=" + storageDatabaseFolderPath + " to local=" + localAbsoluteFilePath);
			centralStorageEngine.syncStorageToLocal(storageDatabaseFolderPath, localAbsoluteFilePath);
		} finally {
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Database " + aliasAndDatabaseId + " is unlocked");
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Project
	 */
	
	
	@Override
	public void pushProject(String projectId) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}

		// We need to push the folder alias__projectId and the file alias__projectId.smss
		String alias = project.getProjectName();
		if(alias == null) {
			alias = SecurityProjectUtils.getProjectAliasForId(projectId);
		}

		String aliasAndProjectId = alias + "__" + projectId;
		String localProjectFolder = PROJECT_FOLDER + FILE_SEPARATOR + aliasAndProjectId;
		String localSmssFileName = aliasAndProjectId + ".smss";
		String localSmssFilePath = PROJECT_FOLDER + FILE_SEPARATOR + localSmssFileName;

		String sharedRCloneConfig = null;

		String storageProjectFolder = PROJECT_CONTAINER_PREFIX + projectId;
		String storageSmssFolder = PROJECT_CONTAINER_PREFIX + projectId + SMSS_POSTFIX;

		// synchronize on the project id
		classLogger.info("Applying lock for " + aliasAndProjectId + " to push project");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		classLogger.info("Project "+ aliasAndProjectId + " is locked");
		try {
			DIHelper.getInstance().removeProjectProperty(projectId);
			project.close();
			
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			centralStorageEngine.syncLocalToStorage(localProjectFolder, storageProjectFolder, sharedRCloneConfig);
			centralStorageEngine.copyToStorage(localSmssFilePath, storageSmssFolder, sharedRCloneConfig);
		} finally {
			try {
				// Re-open the project
				Utility.getProject(projectId, false);
				if(sharedRCloneConfig != null) {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				}
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Project "+ aliasAndProjectId + " is unlocked");
		}
	}
	
	@Override
	public void pullProject(String projectId) throws IOException, InterruptedException {
		pullProject(projectId, false);
	}

	@Override
	public void pullProject(String projectId, boolean projectAlreadyLoaded) throws IOException, InterruptedException {
		IProject project = null;
		if (projectAlreadyLoaded) {
			project = Utility.getProject(projectId, false);
			if (project == null) {
				throw new IllegalArgumentException("Project not found...");
			}
		}

		// We need to pull the folder alias__projectId and the file alias__projectId.smss
		String alias = SecurityProjectUtils.getProjectAliasForId(projectId);
		String aliasAndProjectId = alias + "__" + projectId;
		String localProjectFolder = PROJECT_FOLDER + FILE_SEPARATOR + aliasAndProjectId;
		String localSmssFileName = aliasAndProjectId + ".smss";
		String localSmssFilePath = PROJECT_FOLDER + FILE_SEPARATOR + localSmssFileName;

		String sharedRCloneConfig = null;

		String storageProjectFolder = PROJECT_CONTAINER_PREFIX + projectId;
		String storageSmssFolder = PROJECT_CONTAINER_PREFIX + projectId + SMSS_POSTFIX;

		// synchronize on the project id
		classLogger.info("Applying lock for " + aliasAndProjectId + " to push project");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		classLogger.info("Project "+ aliasAndProjectId + " is locked");
		try {
			if (projectAlreadyLoaded) {
				DIHelper.getInstance().removeProjectProperty(projectId);
				project.close();
			}
			
			// Make the project directory (if it doesn't already exist)
			File localProjectF = new File(Utility.normalizePath(localProjectFolder));
			if(!localProjectF.exists() || !localProjectF.isDirectory()) {
				localProjectF.mkdirs(); 
			}
			
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			centralStorageEngine.syncStorageToLocal(storageProjectFolder, localProjectFolder, sharedRCloneConfig);
			centralStorageEngine.copyToLocal(storageSmssFolder, PROJECT_FOLDER, sharedRCloneConfig);
		} finally {
			try {
				// Re-open the project - if already loaded
				if (projectAlreadyLoaded) {
					Utility.getProject(projectId, false);
				}
				if(sharedRCloneConfig != null) {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				}
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Project "+ aliasAndProjectId + " is unlocked");
		}
	}
	
	@Override
	public void pushProjectSmss(String projectId) throws IOException, InterruptedException {
		// We need to push the file alias__projectId.smss
		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		String aliasAndProjectId = SmssUtilities.getUniqueName(projectName, projectId);
		String localSmssFileName = SmssUtilities.getUniqueName(projectName, projectId) + ".smss";
		String localSmssFilePath = Utility.normalizePath(PROJECT_FOLDER + FILE_SEPARATOR + localSmssFileName);
		
		String storageSmssFolder = PROJECT_CONTAINER_PREFIX + projectId + SMSS_POSTFIX;

		// synchronize on the project id
		classLogger.info("Applying lock for " + aliasAndProjectId + " to push project smss");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		classLogger.info("Project " + aliasAndProjectId + " is locked");
		try {
			centralStorageEngine.copyToStorage(localSmssFilePath, storageSmssFolder);
		} finally {
			lock.unlock();
			classLogger.info("Project " + aliasAndProjectId + " is unlocked");
		}
	}
	
	@Override
	public void pullProjectSmss(String projectId) throws IOException, InterruptedException {
		// We need to push the file alias__projectId.smss
		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		String aliasAndProjectId = SmssUtilities.getUniqueName(projectName, projectId);
		
		String storageSmssFolder = PROJECT_CONTAINER_PREFIX + projectId + SMSS_POSTFIX;

		// synchronize on the project id
		classLogger.info("Applying lock for " + aliasAndProjectId + " to push project smss");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		classLogger.info("Project " + aliasAndProjectId + " is locked");
		try {
			centralStorageEngine.copyToLocal(storageSmssFolder, PROJECT_FOLDER);
		} finally {
			lock.unlock();
			classLogger.info("Project " + aliasAndProjectId + " is unlocked");
		}
	}
	
	@Override
	public void pullProjectImageFolder() throws IOException, InterruptedException {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String localImagesFolderPath = baseFolder + "/images/projects";
		File localImageF = new File(localImagesFolderPath);
		if(!localImageF.exists() || !localImageF.isDirectory()) {
			localImageF.mkdirs();
		}
		centralStorageEngine.copyToLocal(CentralCloudStorage.PROJECT_IMAGES_BLOB, localImagesFolderPath);
	}

	@Override
	public void pushProjectImageFolder() throws IOException, InterruptedException {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String localImagesFolderPath = baseFolder + "/images/projects";
		File localImageF = new File(localImagesFolderPath);
		if(!localImageF.exists() || !localImageF.isDirectory()) {
			localImageF.mkdirs();
		}
		centralStorageEngine.syncLocalToStorage(localImagesFolderPath, CentralCloudStorage.PROJECT_IMAGES_BLOB);
	}
	
	@Override
	public void deleteProject(String projectId) throws IOException, InterruptedException {
		String sharedRCloneConfig = null;
		if(centralStorageEngine.canReuseRcloneConfig()) {
			sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
		}
		String storageProjectFolder = PROJECT_CONTAINER_PREFIX + projectId;
		String storageSmssFolder = PROJECT_CONTAINER_PREFIX + projectId + SMSS_POSTFIX;

		centralStorageEngine.deleteFolderFromStorage(storageProjectFolder, sharedRCloneConfig);
		centralStorageEngine.deleteFolderFromStorage(storageSmssFolder, sharedRCloneConfig);
	}

	@Override
	public void pullInsightsDB(String projectId) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		
		String projectName = project.getProjectName();
		String aliasAndProjectId = SmssUtilities.getUniqueName(projectName, projectId);
		String localProjectFolder = PROJECT_FOLDER + FILE_SEPARATOR + aliasAndProjectId;
		String insightDbFileName = getInsightDB(project, localProjectFolder);

		String storageProjectInsightFilePath = PROJECT_CONTAINER_PREFIX + projectId + "/" + insightDbFileName;
		// synchronize on the project id
		classLogger.info("Applying lock for " + aliasAndProjectId + " to pull insights db");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		classLogger.info("Project " + aliasAndProjectId + " is locked");
		try {
			project.getInsightDatabase().close();
			centralStorageEngine.copyToLocal(storageProjectInsightFilePath, localProjectFolder);
		}  finally {
			try {
				//open the insight db
				String insightDbLoc = SmssUtilities.getInsightsRdbmsFile(project.getSmssProp()).getAbsolutePath();
				if(insightDbLoc != null) {
					try {
						project.setInsightDatabase( ProjectHelper.loadInsightsEngine(project.getSmssProp(), LogManager.getLogger(AbstractDatabaseEngine.class)));
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("Error in loading new insights database for project " + aliasAndProjectId);
					}
				} else {
					throw new IllegalArgumentException("Insight database was not able to be found");
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				classLogger.info("Project " + aliasAndProjectId + " is unlocked");
			}
		}
	}

	@Override
	public void pushInsightDB(String projectId) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		
		String projectName = project.getProjectName();
		String aliasAndProjectId = SmssUtilities.getUniqueName(projectName, projectId);
		String localProjectFolder = PROJECT_FOLDER + FILE_SEPARATOR + aliasAndProjectId;
		String insightDbFileName = getInsightDB(project, localProjectFolder);
		String localProjectInsightDb = localProjectFolder + "/" + insightDbFileName;
		
		String storageProjectFolder = PROJECT_CONTAINER_PREFIX + projectId;
		
		// synchronize on the project id
		classLogger.info("Applying lock for " + aliasAndProjectId + " to pull insights db");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		classLogger.info("Project " + aliasAndProjectId + " is locked");
		try {
			project.getInsightDatabase().close();
			centralStorageEngine.copyToStorage(localProjectInsightDb, storageProjectFolder);
		}  finally {
			try {
				//open the insight db
				String insightDbLoc = SmssUtilities.getInsightsRdbmsFile(project.getSmssProp()).getAbsolutePath();
				if(insightDbLoc != null) {
					try {
						project.setInsightDatabase( ProjectHelper.loadInsightsEngine(project.getSmssProp(), LogManager.getLogger(AbstractDatabaseEngine.class)));
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("Error in loading new insights database for project " + aliasAndProjectId);
					}
				} else {
					throw new IllegalArgumentException("Insight database was not able to be found");
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				classLogger.info("Project " + aliasAndProjectId + " is unlocked");
			}
		}
	}

	@Override
	public void pushProjectFolder(String projectId, String localAbsoluteFilePath, String storageRelativePath) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		if(storageRelativePath != null) {
			storageRelativePath = storageRelativePath.replace("\\", "/");
		}
		if(storageRelativePath.startsWith("/")) {
			storageRelativePath = storageRelativePath.substring(1);
		}
		
		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		String aliasAndProjectId = SmssUtilities.getUniqueName(projectName, projectId);
		
		File absoluteFolder = new File(localAbsoluteFilePath);
		if(absoluteFolder.isDirectory()) {
			//this is adding a hidden file into every sub folder to make sure there is no empty directory
			ClusterUtil.validateFolder(absoluteFolder.getAbsolutePath());
		}
		
		String storageProjectFolderPath = PROJECT_CONTAINER_PREFIX + projectId;
		if(storageRelativePath != null) {
			storageProjectFolderPath = storageProjectFolderPath + "/" + storageRelativePath;
		}
		
		// adding a lock for now, but there may be times we don't need one and other times we do
		// reaching h2 db from version folder vs static assets in project...
		
		classLogger.info("Applying lock for project " + aliasAndProjectId + " to push project relative folder " + storageRelativePath);
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		classLogger.info("Project " + aliasAndProjectId + " is locked");
		try {
			classLogger.info("Pushing folder from local=" + localAbsoluteFilePath + " to remote=" + storageProjectFolderPath);
			centralStorageEngine.syncLocalToStorage(localAbsoluteFilePath, storageProjectFolderPath);
		} finally {
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Project " + aliasAndProjectId + " is unlocked");
		}
	}

	@Override
	public void pullProjectFolder(String projectId, String localAbsoluteFilePath, String storageRelativePath) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		if(storageRelativePath != null) {
			storageRelativePath = storageRelativePath.replace("\\", "/");
		}
		if(storageRelativePath.startsWith("/")) {
			storageRelativePath = storageRelativePath.substring(1);
		}
		
		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		String aliasAndProjectId = SmssUtilities.getUniqueName(projectName, projectId);
		
		String storageProjectFolderPath = PROJECT_CONTAINER_PREFIX + projectId;
		if(storageRelativePath != null) {
			storageProjectFolderPath = storageProjectFolderPath + "/" + storageRelativePath;
		}
		
		// adding a lock for now, but there may be times we don't need one and other times we do
		// reaching h2 db from version folder vs static assets in project...
		
		classLogger.info("Applying lock for project " + aliasAndProjectId + " to pull project relative folder " + storageRelativePath);
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		classLogger.info("Project " + aliasAndProjectId + " is locked");
		try {
			classLogger.info("Pulling folder from remote=" + storageProjectFolderPath + " to local=" + localAbsoluteFilePath);
			centralStorageEngine.syncStorageToLocal(storageProjectFolderPath, localAbsoluteFilePath);
		} finally {
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Project " + aliasAndProjectId + " is unlocked");
		}
	}

	
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Insight
	 */
	

	@Override
	public void pushInsight(String projectId, String insightId) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}

		// only need to pull the insight folder - 99% the project is always already loaded to get to this point
		String localInsightFolderPath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId);
		File localInsightF = new File(localInsightFolderPath);
		if(!localInsightF.exists() || !localInsightF.exists()) {
			localInsightF.mkdirs();
		}
		String storageInsightFolder = PROJECT_CONTAINER_PREFIX+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;
		
		classLogger.info("Pushing insight from local=" + Utility.cleanLogString(localInsightFolderPath) + " to remote=" + Utility.cleanLogString(storageInsightFolder));
		centralStorageEngine.syncLocalToStorage(localInsightFolderPath, storageInsightFolder);
		classLogger.debug("Done pushing insight from local=" + Utility.cleanLogString(localInsightFolderPath) + " to remote=" + Utility.cleanLogString(storageInsightFolder));
	}

	@Override
	public void pullInsight(String projectId, String insightId) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}

		// only need to pull the insight folder - 99% the project is always already loaded to get to this point
		String localInsightFolderPath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId);
		File localInsightF = new File(localInsightFolderPath);
		if(!localInsightF.exists() || !localInsightF.exists()) {
			localInsightF.mkdirs();
		}
		String storageInsightFolder = PROJECT_CONTAINER_PREFIX+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;
		
		classLogger.info("Pulling insight from remote=" + Utility.cleanLogString(storageInsightFolder) + " to target=" + Utility.cleanLogString(localInsightFolderPath));
		centralStorageEngine.syncStorageToLocal(storageInsightFolder, localInsightFolderPath);
		classLogger.debug("Done pulling insight from remote=" + Utility.cleanLogString(storageInsightFolder) + " to target=" + Utility.cleanLogString(localInsightFolderPath));
	}

	@Override
	public void pushInsightImage(String projectId, String insightId, String oldImageFileName, String newImageFileName) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		} 

		String sharedRCloneConfig = null;
		try {
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			
			String storageInsightFolder = PROJECT_CONTAINER_PREFIX+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;
			// since extensions might be different, need to actually delete the old file by name
			if(oldImageFileName != null) {
				String storageOldFileToDelete = storageInsightFolder+"/"+oldImageFileName;
				classLogger.info("Deleting old insight image from remote=" + Utility.cleanLogString(storageOldFileToDelete));
				centralStorageEngine.deleteFromStorage(storageOldFileToDelete, sharedRCloneConfig);
				classLogger.debug("Done deleting old insight image from remote=" + Utility.cleanLogString(storageOldFileToDelete));
			} else {
				classLogger.info("No old insight image on remote to delete");
			}

			if(newImageFileName != null) {
				String localInsightImageFilePath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId + "/" + newImageFileName);
				classLogger.info("Pushing insight image from local=" + Utility.cleanLogString(localInsightImageFilePath) + " to remote=" + Utility.cleanLogString(storageInsightFolder));
				centralStorageEngine.copyToStorage(localInsightImageFilePath, storageInsightFolder, sharedRCloneConfig);
				classLogger.debug("Done pushing insight image from local=" + Utility.cleanLogString(localInsightImageFilePath) + " to remote=" + Utility.cleanLogString(storageInsightFolder));
			} else {
				classLogger.info("No new insight image to add to remote");
			}
		} finally {
			if(sharedRCloneConfig != null) {
				try {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	
	
	/*
	 * Storage
	 */
	
	@Override
	public void pullStorage(String storageId) throws IOException, InterruptedException {
		pullStorage(storageId, false);
	}

	@Override
	public void pullStorage(String storageId, boolean storageAlreadyLoaded) throws IOException, InterruptedException {
		IStorageEngine storage = null;
		if (storageAlreadyLoaded) {
			storage = Utility.getStorage(storageId, false);
			if (storage == null) {
				throw new IllegalArgumentException("Storage not found...");
			}
		}

		// We need to pull the file alias__storageId.smss
		String storageName = SecurityEngineUtils.getEngineAliasForId(storageId);
		String aliasAndStorageId = SmssUtilities.getUniqueName(storageName, storageId);
		String localSmssFileName = SmssUtilities.getUniqueName(storageName, storageId) + ".smss";
		String localSmssFilePath = Utility.normalizePath(STORAGE_FOLDER + FILE_SEPARATOR + localSmssFileName);
		
		String storageSmssFolder = STORAGE_CONTAINER_PREFIX + storageId + SMSS_POSTFIX;

		// synchronize on the engine id
		classLogger.info("Applying lock for " + aliasAndStorageId + " to push database");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(storageId);
		lock.lock();
		classLogger.info("Storage "+ aliasAndStorageId + " is locked");
		
		try {
			if(storage != null) {
				DIHelper.getInstance().removeEngineProperty(storageId);
				storage.close();
			}
			centralStorageEngine.copyToLocal(storageSmssFolder, STORAGE_FOLDER);
			// Catalog the storage if it is new
			if (!storageAlreadyLoaded) {
				classLogger.info("Synchronizing the storage metadata for " + aliasAndStorageId);
				SMSSStorageWatcher.catalogEngine(localSmssFileName, STORAGE_FOLDER);
			}
		} finally {
			try {
				// Re-open the storage
				Utility.getStorage(storageId, false);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Storage "+ aliasAndStorageId + " is unlocked");
		}
	}

	@Override
	public void pushStorageSmss(String storageId) throws IOException, InterruptedException {
		// We need to push the file alias__storageId.smss
		String storageName = SecurityEngineUtils.getEngineAliasForId(storageId);
		String aliasAndStorageId = SmssUtilities.getUniqueName(storageName, storageId);
		String localSmssFileName = SmssUtilities.getUniqueName(storageName, storageId) + ".smss";
		String localSmssFilePath = Utility.normalizePath(STORAGE_FOLDER + FILE_SEPARATOR + localSmssFileName);
		
		String storageSmssFolder = STORAGE_CONTAINER_PREFIX + storageId + SMSS_POSTFIX;

		// synchronize on the storage id
		classLogger.info("Applying lock for " + aliasAndStorageId + " to push storage");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(storageId);
		lock.lock();
		classLogger.info("Storage " + aliasAndStorageId + " is locked");
		try {
			centralStorageEngine.copyToStorage(localSmssFilePath, storageSmssFolder);
		} finally {
			lock.unlock();
			classLogger.info("Storage " + aliasAndStorageId + " is unlocked");
		}
	}

	@Override
	public void pullStorageSmss(String storageId) throws IOException, InterruptedException {
		// We need to push the file alias__storageId.smss
		String storageName = SecurityEngineUtils.getEngineAliasForId(storageId);
		String aliasAndStorageId = SmssUtilities.getUniqueName(storageName, storageId);
		
		String storageSmssFolder = STORAGE_CONTAINER_PREFIX + storageId + SMSS_POSTFIX;

		// synchronize on the storage id
		classLogger.info("Applying lock for " + aliasAndStorageId + " to push storage smss");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(storageId);
		lock.lock();
		classLogger.info("Storage " + aliasAndStorageId + " is locked");
		try {
			centralStorageEngine.copyToLocal(storageSmssFolder, STORAGE_FOLDER);
		} finally {
			lock.unlock();
			classLogger.info("Storage " + aliasAndStorageId + " is unlocked");
		}
	}
	
	@Override
	public void deleteStorage(String storageId) throws IOException, InterruptedException {
		String sharedRCloneConfig = null;
		if(centralStorageEngine.canReuseRcloneConfig()) {
			sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
		}
		String storageDatabaseFolder = STORAGE_CONTAINER_PREFIX + storageId;
		String storageSmssFolder = STORAGE_CONTAINER_PREFIX + storageId + SMSS_POSTFIX;

		centralStorageEngine.deleteFolderFromStorage(storageDatabaseFolder, sharedRCloneConfig);
		centralStorageEngine.deleteFolderFromStorage(storageSmssFolder, sharedRCloneConfig);
	}
	
	@Override
	public void pullStorageImageFolder() throws IOException, InterruptedException {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String localImagesFolderPath = baseFolder + "/images/storages";
		File localImageF = new File(localImagesFolderPath);
		if(!localImageF.exists() || !localImageF.isDirectory()) {
			localImageF.mkdirs();
		}
		centralStorageEngine.copyToLocal(CentralCloudStorage.STORAGE_IMAGES_BLOB, localImagesFolderPath);
	}

	@Override
	public void pushStorageImageFolder() throws IOException, InterruptedException {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String localImagesFolderPath = baseFolder + "/images/storages";
		File localImageF = new File(localImagesFolderPath);
		if(!localImageF.exists() || !localImageF.isDirectory()) {
			localImageF.mkdirs();
		}
		centralStorageEngine.syncLocalToStorage(localImagesFolderPath, CentralCloudStorage.STORAGE_IMAGES_BLOB);
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	
	
	/*
	 * Model
	 */

	@Override
	public void pullModel(String modelId) throws IOException, InterruptedException {
		pullModel(modelId, false);
	}

	@Override
	public void pullModel(String modelId, boolean modelAlreadyLoaded) throws IOException, InterruptedException {
		IModelEngine model = null;
		if (modelAlreadyLoaded) {
			model = Utility.getModel(modelId, false);
			if (model == null) {
				throw new IllegalArgumentException("Model not found...");
			}
		}

		// We need to pull the file alias__storageId.smss
		String modelName = SecurityEngineUtils.getEngineAliasForId(modelId);
		String aliasAndModelId = SmssUtilities.getUniqueName(modelName, modelId);
		String localSmssFileName = SmssUtilities.getUniqueName(modelName, modelId) + ".smss";
		String localSmssFilePath = Utility.normalizePath(MODEL_FOLDER + FILE_SEPARATOR + localSmssFileName);
		
		String modelSmssFolder = MODEL_CONTAINER_PREFIX + modelId + SMSS_POSTFIX;

		// synchronize on the model id
		classLogger.info("Applying lock for " + aliasAndModelId + " to push database");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(modelId);
		lock.lock();
		classLogger.info("Storage "+ aliasAndModelId + " is locked");
		
		try {
			if(model != null) {
				DIHelper.getInstance().removeEngineProperty(modelId);
				model.close();
			}
			centralStorageEngine.copyToLocal(modelSmssFolder, MODEL_FOLDER);
			// Catalog the model if it is new
			if (!modelAlreadyLoaded) {
				classLogger.info("Synchronizing the model metadata for " + aliasAndModelId);
				SMSSModelWatcher.catalogEngine(localSmssFileName, MODEL_FOLDER);
			}
		} finally {
			try {

				// Re-open the model
				Utility.getModel(modelId, false);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("Model "+ aliasAndModelId + " is unlocked");
		}
	}

	@Override
	public void pushModelSmss(String modelId) throws IOException, InterruptedException {
		// We need to push the file alias__appId.smss
		String modelName = SecurityEngineUtils.getEngineAliasForId(modelId);
		String aliasAndModelId = SmssUtilities.getUniqueName(modelName, modelId);
		String localSmssFileName = SmssUtilities.getUniqueName(modelName, modelId) + ".smss";
		String localSmssFilePath = Utility.normalizePath(MODEL_FOLDER + FILE_SEPARATOR + localSmssFileName);
		
		String storageSmssFolder = MODEL_CONTAINER_PREFIX + modelId + SMSS_POSTFIX;

		// synchronize on the model id
		classLogger.info("Applying lock for " + aliasAndModelId + " to push model");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(modelId);
		lock.lock();
		classLogger.info("Model " + aliasAndModelId + " is locked");
		try {
			centralStorageEngine.copyToStorage(localSmssFilePath, storageSmssFolder);
		} finally {
			lock.unlock();
			classLogger.info("Model " + aliasAndModelId + " is unlocked");
		}
	}

	@Override
	public void pullModelSmss(String modelId) throws IOException, InterruptedException {
		// We need to push the file alias__modelId.smss
		String modelName = SecurityEngineUtils.getEngineAliasForId(modelId);
		String aliasAndModelId = SmssUtilities.getUniqueName(modelName, modelId);
		
		String storageSmssFolder = STORAGE_CONTAINER_PREFIX + modelId + SMSS_POSTFIX;

		// synchronize on the engine id
		classLogger.info("Applying lock for " + aliasAndModelId + " to push model smss");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(modelId);
		lock.lock();
		classLogger.info("Model " + aliasAndModelId + " is locked");
		try {
			centralStorageEngine.copyToLocal(storageSmssFolder, MODEL_FOLDER);
		} finally {
			lock.unlock();
			classLogger.info("Model " + aliasAndModelId + " is unlocked");
		}
	}
	
	@Override
	public void deleteModel(String modelId) throws IOException, InterruptedException {
		String sharedRCloneConfig = null;
		if(centralStorageEngine.canReuseRcloneConfig()) {
			sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
		}
		String storageDatabaseFolder = MODEL_CONTAINER_PREFIX + modelId;
		String storageSmssFolder = MODEL_CONTAINER_PREFIX + modelId + SMSS_POSTFIX;

		centralStorageEngine.deleteFolderFromStorage(storageDatabaseFolder, sharedRCloneConfig);
		centralStorageEngine.deleteFolderFromStorage(storageSmssFolder, sharedRCloneConfig);
	}
	
	@Override
	public void pullModelImageFolder() throws IOException, InterruptedException {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String localImagesFolderPath = baseFolder + "/images/models";
		File localImageF = new File(localImagesFolderPath);
		if(!localImageF.exists() || !localImageF.isDirectory()) {
			localImageF.mkdirs();
		}
		centralStorageEngine.copyToLocal(CentralCloudStorage.MODEL_IMAGES_BLOB, localImagesFolderPath);
	}

	@Override
	public void pushModelImageFolder() throws IOException, InterruptedException {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String localImagesFolderPath = baseFolder + "/images/models";
		File localImageF = new File(localImagesFolderPath);
		if(!localImageF.exists() || !localImageF.isDirectory()) {
			localImageF.mkdirs();
		}
		centralStorageEngine.syncLocalToStorage(localImagesFolderPath, CentralCloudStorage.MODEL_IMAGES_BLOB);
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * User
	 */

	@Override
	public void pullUserAssetOrWorkspace(String projectId, boolean isAsset, boolean projectAlreadyLoaded) throws IOException, InterruptedException {
		IProject project = null;
		String alias = null;
		if (projectAlreadyLoaded) {
			project = Utility.getUserAssetWorkspaceProject(projectId, isAsset);
			if (project == null) {
				throw new IllegalArgumentException("User asset/workspace project not found...");
			}
			alias = project.getProjectName();
		} else {
			if(isAsset) {
				alias = WorkspaceAssetUtils.ASSET_APP_NAME;
			} else {
				alias = WorkspaceAssetUtils.WORKSPACE_APP_NAME;
			}
		}

		// We need to pull the folder alias__projectId and the file alias__projectId.smss
		String aliasAndUserAssetWorkspaceId = SmssUtilities.getUniqueName(alias, projectId);
		String localUserAndAssetFolder = USER_FOLDER + FILE_SEPARATOR + aliasAndUserAssetWorkspaceId;
		String storageUserAssetWorkspaceFolder = USER_CONTAINER_PREFIX + projectId;
		String storageSmssFolder = USER_CONTAINER_PREFIX + projectId + SMSS_POSTFIX;
		
		String sharedRCloneConfig = null;
		try {
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			
			List<String> results = centralStorageEngine.list(storageSmssFolder, sharedRCloneConfig);
			String smss = null;
			for (String result : results) {
				if (result.endsWith(".smss")) {
					smss = result;
					break;
				}
			}
			if (smss == null) {
				// assume this is for pulling a legacy asset/workspace
				try {
					fixLegacyUserAssetStructure(projectId, isAsset);
				} catch(IOException | InterruptedException e) {
					classLogger.info(Constants.STACKTRACE, e);
					throw new IOException("Failed to pull legacy user asset/workspace with id=" + projectId);
				}
				
				// try again
				results = centralStorageEngine.list(storageSmssFolder, sharedRCloneConfig);
				for (String result : results) {
					if (result.endsWith(".smss")) {
						smss = result;
						break;
					}
				}
				
				if (smss == null) {
					throw new IOException("Failed to pull legacy user asset/workspace with id=" + projectId);
				} else {
					// we just fixed the structure and this was pulled and synched up
					// can just return from here
					return;
				}
			}

			// Close the user asset/workspace project so that we can pull without file locks
			try {
				if (projectAlreadyLoaded) {
					DIHelper.getInstance().removeProjectProperty(projectId);
					project.close();
				}

				// Make the user asset/workspace directory (if it doesn't already exist)
				File localUserAndAssetF = new File(localUserAndAssetFolder);
				if(!localUserAndAssetF.exists() || !localUserAndAssetF.isDirectory()) {
					localUserAndAssetF.mkdir(); 
				}

				// Pull the contents of the project folder before the smss
				classLogger.info("Pulling user asset/workspace from remote=" + storageUserAssetWorkspaceFolder + " to target=" + localUserAndAssetFolder);
				centralStorageEngine.syncStorageToLocal(storageUserAssetWorkspaceFolder, localUserAndAssetFolder, sharedRCloneConfig);
				classLogger.debug("Done pulling from remote=" + storageUserAssetWorkspaceFolder + " to target=" + localUserAndAssetFolder);

				// Now pull the smss
				classLogger.info("Pulling smss from remote=" + storageSmssFolder + " to target=" + USER_FOLDER);
				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE USER FOLDER
				centralStorageEngine.copyToLocal(storageSmssFolder, USER_FOLDER, sharedRCloneConfig);
				classLogger.debug("Done pulling from remote=" + storageSmssFolder + " to target=" + USER_FOLDER);
			} finally {
				// Re-open the project
				if (projectAlreadyLoaded) {
					Utility.getUserAssetWorkspaceProject(projectId, isAsset);
				}
			}
		} finally {
			if(sharedRCloneConfig != null) {
				try {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	@Override
	public void pushUserAssetOrWorkspace(String projectId, boolean isAsset) throws IOException, InterruptedException {
		IProject project = Utility.getUserAssetWorkspaceProject(projectId, isAsset);
		if (project == null) {
			throw new IllegalArgumentException("User asset/workspace project not found...");
		}

		// We need to push the folder alias__projectId and the file alias__projectId.smss
		String alias = project.getProjectName();
		String aliasAndUserAssetWorkspaceId = alias + "__" + projectId;
		String localUserAssetWorkspaceFolder = USER_FOLDER + FILE_SEPARATOR + aliasAndUserAssetWorkspaceId;
		String localSmssFileName = aliasAndUserAssetWorkspaceId + ".smss";
		String localSmssFilePath = USER_FOLDER + FILE_SEPARATOR + localSmssFileName;

		String sharedRCloneConfig = null;

		String storageUserAssetWorkspaceFolder = USER_CONTAINER_PREFIX + projectId;
		String storageSmssFolder = USER_CONTAINER_PREFIX + projectId + SMSS_POSTFIX;

		try {
			DIHelper.getInstance().removeProjectProperty(projectId);
			project.close();
			
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			centralStorageEngine.syncLocalToStorage(localUserAssetWorkspaceFolder, storageUserAssetWorkspaceFolder, sharedRCloneConfig);
			centralStorageEngine.copyToStorage(localSmssFilePath, storageSmssFolder, sharedRCloneConfig);
		} finally {
			try {
				// Re-open the project
				Utility.getUserAssetWorkspaceProject(projectId, isAsset);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			if(sharedRCloneConfig != null) {
				try {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////

	// utility methods
	
	/**
	 * 
	 * @param directory
	 * @return
	 */
	protected List<String> getSqlLiteFile(String directory) {
		File dir = new File(directory);
		List<String> sqlFiles = new ArrayList<>();
		//search dir for .sqlite files 
		for(File file : dir.listFiles()) {
			if (file.getName().endsWith((".sqlite"))) {
				sqlFiles.add(file.getName());
			}
		}
		if(sqlFiles.size() > 1){
			if(sqlFiles.contains("insights_database.sqlite")) {
				sqlFiles.remove("insights_database.sqlite");
			}
		}
		if(sqlFiles.size() > 1) {
			classLogger.warn("Found multiple sqlite files. Only expecting 1 for database");
			classLogger.warn("Found multiple sqlite files. Only expecting 1 for database");
			classLogger.warn("Found multiple sqlite files. Only expecting 1 for database");
			classLogger.warn("Found multiple sqlite files. Only expecting 1 for database");
		}
		return sqlFiles;
	}
	
	/**
	 * 
	 * @param directory
	 * @return
	 */
	protected List<String> getH2File(String directory) {
		File dir = new File(directory);
		List<String> sqlFiles = new ArrayList<>();
		//search dir for .sqlite files 
		for(File file : dir.listFiles()) {
			if (file.getName().endsWith((".mv.db"))) {
				sqlFiles.add(file.getName());
			}
		}
		if(sqlFiles.size() > 1){
			if(sqlFiles.contains("insights_database.mv.db")) {
				sqlFiles.remove("insights_database.mv.db");
			}
		}
		if(sqlFiles.size() > 1) {
			classLogger.warn("Found multiple h2 files. Only expecting 1 for database");
			classLogger.warn("Found multiple h2 files. Only expecting 1 for database");
			classLogger.warn("Found multiple h2 files. Only expecting 1 for database");
			classLogger.warn("Found multiple h2 files. Only expecting 1 for database");
		}
		return sqlFiles;
	}

	/**
	 * 
	 * @param project
	 * @param specificProjectFolder
	 * @return
	 */
	protected String getInsightDB(IProject project, String specificProjectFolder) {
		RdbmsTypeEnum insightDbType = project.getInsightDatabase().getDbType();
		String insightDbName = null;
		if (insightDbType == RdbmsTypeEnum.H2_DB) {
			insightDbName = "insights_database.mv.db";
		} else {
			insightDbName = "insights_database.sqlite";
		}
		File dir = new File(specificProjectFolder);
		for (File file : dir.listFiles()) {
			if (file.getName().equalsIgnoreCase(insightDbName)){
				return file.getName();
			}
		}
		throw new IllegalArgumentException("There is no insight database for project: " + project.getProjectName());
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Legacy
	 */

	@Override
	@Deprecated
	public void fixLegacyDbStructure(String legacyAppId) throws IOException, InterruptedException {
		String sharedRCloneConfig = null;
		String legacyStorageDatabaseFolder = legacyAppId;
		String legacyStorageSmssFolder = legacyAppId + SMSS_POSTFIX;
		
		// We need to push the folder alias__databaseId and the file alias__databaseId.smss
		String potentialDatabaseName = SecurityEngineUtils.getEngineAliasForId(legacyAppId);

		classLogger.info("Applying lock to pull legacy app (database+insights) " + legacyAppId);
		ReentrantLock lock = EngineSyncUtility.getEngineLock(legacyAppId);
		lock.lock();
		classLogger.info("App "+ legacyAppId + " is locked");
		try {
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			
			// List the smss directory to get the alias + app id
			List<String> results = centralStorageEngine.list(legacyStorageSmssFolder, sharedRCloneConfig);
			String storageSmssFileName = null;
			boolean foundSmss = false;
			for (String result : results) {
				if (result.endsWith(".smss")) {
					storageSmssFileName = result;
					foundSmss = true;
					break;
				}
			}
		
			if (!foundSmss) {
				// IF STILL NOT FOUND... CANT HELP YOU
				throw new IOException("Failed to pull legacy app (database+insights) with id " + legacyAppId + " and potential alias " + potentialDatabaseName);
			}
			String aliasAndAppId = storageSmssFileName.replaceAll(".smss", "");
			String localDatabaseFolder = DATABASE_FOLDER + FILE_SEPARATOR + aliasAndAppId;
			File localDatabaseF = new File(localDatabaseFolder);
			if(!localDatabaseF.exists() || !localDatabaseF.isDirectory()) {
				localDatabaseF.mkdirs();
			}
			classLogger.info("Pulling legacy app (database+insights) from remote=" + Utility.cleanLogString(legacyStorageDatabaseFolder) + " to target=" + Utility.cleanLogString(localDatabaseFolder));
			centralStorageEngine.syncStorageToLocal(legacyStorageDatabaseFolder, localDatabaseFolder);
			classLogger.debug("Done pulling legacy app (database+insights) from remote=" + Utility.cleanLogString(legacyStorageDatabaseFolder) + " to target=" + Utility.cleanLogString(localDatabaseFolder));

			// Now pull the smss
			classLogger.info("Pulling smss from remote=" + legacyStorageSmssFolder + " to target=" + DATABASE_FOLDER);
			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
			centralStorageEngine.copyToLocal(legacyStorageSmssFolder, DATABASE_FOLDER);
			classLogger.debug("Done pulling from remote=" + legacyStorageSmssFolder + " to target=" + DATABASE_FOLDER);

			LegacyToProjectRestructurerHelper fixer = new LegacyToProjectRestructurerHelper();
			fixer.init();
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String dbDir = baseFolder + LegacyToProjectRestructurerHelper.ENGINE_DIRECTORY;
			String projectDir = baseFolder + LegacyToProjectRestructurerHelper.PROJECT_DIRECTORY;
			fixer.copyDataToNewFolderStructure(aliasAndAppId, projectDir, dbDir);
			
			Utility.loadDatabase(dbDir + "/" + storageSmssFileName, Utility.loadProperties(dbDir + "/" + storageSmssFileName));
			Utility.loadProject(projectDir + "/" + storageSmssFileName, Utility.loadProperties(projectDir + "/" + storageSmssFileName));

			// now push the new db and app into the right locations
			pushDatabase(legacyAppId);
			pushProject(legacyAppId);
		} finally {
			if(sharedRCloneConfig != null) {
				try {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			// always unlock regardless of errors
			lock.unlock();
			classLogger.info("App "+ legacyAppId + " is unlocked");
		}
	}

	@Override
	@Deprecated
	public void fixLegacyImageStructure() throws IOException, InterruptedException {
		String sharedRCloneConfig = null;
		try {
			if(centralStorageEngine.canReuseRcloneConfig()) {
				sharedRCloneConfig = centralStorageEngine.createRCloneConfig();
			}
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String localImagesFolderPath = baseFolder + "/images/databases";
			File localImageF = new File(localImagesFolderPath);
			if(!localImageF.exists() || !localImageF.isDirectory()) {
				localImageF.mkdirs();
			}
			
			// copy the images
			// we will push these images to the new location
			centralStorageEngine.copyToLocal("semoss-imagecontainer", localImagesFolderPath, sharedRCloneConfig);
			centralStorageEngine.syncLocalToStorage(localImagesFolderPath, CentralCloudStorage.DB_IMAGES_BLOB, sharedRCloneConfig);
		} finally {
			if(sharedRCloneConfig != null) {
				try {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	@Override
	@Deprecated
	public void fixLegacyUserAssetStructure(String legacyAppId, boolean isAsset) throws IOException, InterruptedException {
		String sharedRCloneConfig = null;
		String legacyStorageUserAssetWorkspaceFolder = legacyAppId;
		String legacyStorageSmssFolder = legacyAppId + SMSS_POSTFIX;
		
		try {
			// List the smss directory to get the alias + app id
			List<String> results = centralStorageEngine.list(legacyStorageSmssFolder, sharedRCloneConfig);
			String storageSmssFileName = null;
			boolean foundSmss = false;
			for (String result : results) {
				if (result.endsWith(".smss")) {
					storageSmssFileName = result;
					foundSmss = true;
					break;
				}
			}
		
			if (!foundSmss) {
				// IF STILL NOT FOUND... CANT HELP YOU
				throw new IOException("Failed to pull legacy user asset/workspace with with id " + legacyAppId);
			}
			String aliasAndAppId = storageSmssFileName.replaceAll(".smss", "");
			String localUserAssetWorkspaceFolder = USER_FOLDER + FILE_SEPARATOR + aliasAndAppId;
			File localUserAssetWorkspaceF = new File(localUserAssetWorkspaceFolder);
			if(!localUserAssetWorkspaceF.exists() || !localUserAssetWorkspaceF.isDirectory()) {
				localUserAssetWorkspaceF.mkdirs();
			}
			
			classLogger.info("Pulling legacy user asset/workspace from remote=" + Utility.cleanLogString(legacyStorageUserAssetWorkspaceFolder) + " to target=" + Utility.cleanLogString(localUserAssetWorkspaceFolder));
			centralStorageEngine.syncStorageToLocal(legacyStorageUserAssetWorkspaceFolder, localUserAssetWorkspaceFolder);
			classLogger.debug("Done pulling legacy user asset/workspace from remote=" + Utility.cleanLogString(legacyStorageUserAssetWorkspaceFolder) + " to target=" + Utility.cleanLogString(localUserAssetWorkspaceFolder));

			// Now pull the smss
			classLogger.info("Pulling smss from remote=" + legacyStorageSmssFolder + " to target=" + USER_FOLDER);
			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
			centralStorageEngine.copyToLocal(legacyStorageSmssFolder, USER_FOLDER);
			classLogger.debug("Done pulling from remote=" + legacyStorageSmssFolder + " to target=" + USER_FOLDER);
			

			LegacyToProjectRestructurerHelper fixer = new LegacyToProjectRestructurerHelper();
			fixer.init();
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String dbDir = baseFolder + LegacyToProjectRestructurerHelper.ENGINE_DIRECTORY;
			String userDir = baseFolder + LegacyToProjectRestructurerHelper.USER_DIRECTORY;
			fixer.userCopyDataToNewFolderStructure(aliasAndAppId, userDir, dbDir, WorkspaceAssetUtils.isAssetProject(legacyAppId));
			
			// only load the project
			Utility.loadProject(userDir + "/" + storageSmssFileName, Utility.loadProperties(userDir + "/" + storageSmssFileName));

			// now push the project into the right locations
			pushUserAssetOrWorkspace(legacyAppId, isAsset);
		} finally {
			if(sharedRCloneConfig != null) {
				try {
					centralStorageEngine.deleteRcloneConfig(sharedRCloneConfig);
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	@Override
	public List<String> listAllBlobContainers() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteContainer(String containerId) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////

	
	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadAll("C:/workspace/Semoss_Dev/RDF_Map.prop");
		Properties coreProp = DIHelper.getInstance().getCoreProp();
		coreProp.put("SEMOSS_STORAGE_PROVIDER", "MINIO");
		coreProp.put(MinioStorageEngine.MINIO_REGION_KEY, "us-east-1");
		coreProp.put(MinioStorageEngine.MINIO_ACCESS_KEY, "aq7eJfuoYwDrtOVeWoGb");
		coreProp.put(MinioStorageEngine.MINIO_SECRET_KEY, "14A9uh4eXwJYvBxBzcY32p4aYufzv6Sn7Ogkq7p2");
		coreProp.put(MinioStorageEngine.MINIO_ENDPOINT_KEY, "http://localhost:9000");
		coreProp.put(Constants.ENGINE, "CENTRAL_STORAGE");

		{
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String engineProp = baseFolder + "\\db\\diabetes sanjay and sarji__56af9395-64fd-40a2-b68c-bbd6961336a5.smss";
			IDatabaseEngine sampleDb = new RDBMSNativeEngine();
			sampleDb.open(engineProp);
			DIHelper.getInstance().setEngineProperty("56af9395-64fd-40a2-b68c-bbd6961336a5", sampleDb);
		}
		
		ICloudClient centralStorage = CentralCloudStorage.getInstance();
		centralStorage.pushDatabase("56af9395-64fd-40a2-b68c-bbd6961336a5");
		centralStorage.pullDatabase("56af9395-64fd-40a2-b68c-bbd6961336a5", true);
		centralStorage.pullLocalDatabaseFile("56af9395-64fd-40a2-b68c-bbd6961336a5", RdbmsTypeEnum.H2_DB);
	}

}
