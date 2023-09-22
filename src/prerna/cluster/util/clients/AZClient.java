//package prerna.cluster.util.clients;
//
//import java.io.File;
//import java.io.IOException;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.security.InvalidKeyException;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.EnumSet;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.locks.ReentrantLock;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.apache.zookeeper.Watcher.Event.EventType;
//
//import com.google.common.io.Files;
//import com.microsoft.azure.storage.CloudStorageAccount;
//import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
//import com.microsoft.azure.storage.StorageException;
//import com.microsoft.azure.storage.StorageUri;
//import com.microsoft.azure.storage.blob.CloudBlobClient;
//import com.microsoft.azure.storage.blob.CloudBlobContainer;
//import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
//import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
//
//import prerna.auth.utils.SecurityEngineUtils;
//import prerna.auth.utils.SecurityProjectUtils;
//import prerna.auth.utils.WorkspaceAssetUtils;
//import prerna.cluster.util.AZStorageListener;
//import prerna.cluster.util.ClusterUtil;
//import prerna.cluster.util.ZKClient;
//import prerna.engine.api.IDatabaseEngine;
//import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
//import prerna.engine.impl.AbstractDatabaseEngine;
//import prerna.engine.impl.LegacyToProjectRestructurerHelper;
//import prerna.engine.impl.SmssUtilities;
//import prerna.project.api.IProject;
//import prerna.project.impl.ProjectHelper;
//import prerna.util.AssetUtility;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.EngineSyncUtility;
//import prerna.util.ProjectSyncUtility;
//import prerna.util.ProjectWatcher;
//import prerna.util.SMSSWebWatcher;
//import prerna.util.Utility;
//import prerna.util.sql.RdbmsTypeEnum;
//
//public class AZClient extends AbstractCloudClient {
//	
//	private static final Logger classLogger = LogManager.getLogger(AZClient.class);
//
//	private static String DB_CONTAINER_PREFIX = "db-";
//	private static String PROJECT_CONTAINER_PREFIX = "project-";
//	private static String USER_CONTAINER_PREFIX = "user-";
//	
//	// does some basic ops
//	// get the SAS URL for a given container - boolean create or not
//	// Delete the container
//	{
//		this.PROVIDER = "azureblob";
//	}
//
//	public String azKeyRoot = "/khome";
//	public static final String KEY_HOME = "KEY_HOME"; // this is where the various keys are cycled
//
//	private CloudBlobClient serviceClient = null;
//	private String connectionString = null;
//	private String name = null;
//	private String key = null;
//	private String blobURI = null;
//	private String sasURL = null;
//	
//	public AZClient(AZClientBuilder builder) {
//		super(builder);
//		
//		// if the zookeeper is defined.. find from zookeeper what the key is
//		// and register for the key change
//		// if not.. the storage key is sitting some place pick it up and get it
//		String storage = builder.storage;
//		if(storage == null || storage.equalsIgnoreCase("LOCAL")) {
//			this.connectionString = builder.connectionString;
//			this.blobURI = builder.blobURI;
//			this.name = builder.name;
//			this.key = builder.key;
//			this.sasURL = builder.sasURL;
//		} else {
//			Map <String, String> env = System.getenv();
//			if(env.containsKey(KEY_HOME)) {
//				this.azKeyRoot = env.get(KEY_HOME);
//			}
//
//			if(env.containsKey(KEY_HOME.toUpperCase())) {
//				this.azKeyRoot = env.get(KEY_HOME.toUpperCase());
//			}
//			
//			// need the zk piece here
//			ZKClient client = ZKClient.getInstance();
//			this.connectionString = client.getNodeData(azKeyRoot, client.zk);
//
//			// if SAS_URL it should starts with SAS_URL=			
//			if(connectionString.startsWith("SAS_URL=")) {
//				this.sasURL = connectionString.replace("SAS_URL=", "");
//			}
//
//			AZStorageListener azList = new AZStorageListener();
//			client.watchEvent(azKeyRoot, EventType.NodeDataChanged, azList);
//		}
//
//		createServiceClient();
//	}	
//	
//	public void createServiceClient() {
//		try {
//			if(sasURL != null) {
//				this.serviceClient = new CloudBlobClient(new StorageUri(new URI(blobURI)),
//						new StorageCredentialsSharedAccessSignature(connectionString));
//			} else {
//				CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
//				this.serviceClient = account.createCloudBlobClient();
//			}
//		} catch (URISyntaxException use) {
//			classLogger.error(Constants.STACKTRACE, use);
//		} catch (InvalidKeyException ike) {
//			classLogger.error(Constants.STACKTRACE, ike);
//		}
//	}
//
//	// get SAS URL for a container
//	public String getSAS(String containerName) {
//		String retString = null;
//		try {
//			//createServiceClient();
//			CloudBlobContainer container = serviceClient.getContainerReference(containerName);
//			container.createIfNotExists();
//			retString = container.getUri() + "?" + container.generateSharedAccessSignature(getSASConstraints(), null); 
//
//		} catch (URISyntaxException use) {
//			classLogger.error(Constants.STACKTRACE, use);
//		} catch (StorageException se) {
//			classLogger.error(Constants.STACKTRACE, se);
//		} catch (InvalidKeyException ike) {
//			classLogger.error(Constants.STACKTRACE, ike);
//		}
//
//		return retString;
//	}
//
//	// swaps the key
//	public void swapKey(String key) {
//		// if sasURL is null then it is account
//		if(sasURL != null) {
//			sasURL = key;
//		} else {
//			connectionString = key;
//		}
//		createServiceClient();
//	}
//
//	public void quarantineContainer(String containerName)
//	{
//		// take this out in terms of listing
//
//	}
//
//	public SharedAccessBlobPolicy getSASConstraints() {
//		SharedAccessBlobPolicy sasConstraints = null;
//		sasConstraints = new SharedAccessBlobPolicy();
//
//		// get the current time + 24 hours or some
//		Calendar calendar = Calendar.getInstance();
//		calendar.add(Calendar.MINUTE, +5);
//		Date date = calendar.getTime();
//
//		sasConstraints.setSharedAccessExpiryTime(date);
//
//		EnumSet <SharedAccessBlobPermissions> permSet = EnumSet.noneOf(SharedAccessBlobPermissions.class);
//		// I need to read the database to find if this guy is allowed etc. but for now
//		permSet.add(SharedAccessBlobPermissions.LIST);
//		permSet.add(SharedAccessBlobPermissions.WRITE);
//		permSet.add(SharedAccessBlobPermissions.CREATE);
//		permSet.add(SharedAccessBlobPermissions.READ);
//		permSet.add(SharedAccessBlobPermissions.DELETE);
//		permSet.add(SharedAccessBlobPermissions.ADD);
//
//		sasConstraints.setPermissions(permSet);
//		return sasConstraints;
//	}
//	
//	@Override
//	public void pullOwl(String databaseId) throws IOException, InterruptedException{
//		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
//		if (database == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//		String appRcloneConfig = null;
//		File owlFile = null;
//		String alias = SecurityEngineUtils.getEngineAliasForId(databaseId);
//		String aliasAppId = alias + "__" + databaseId;
//		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to pull owl");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ databaseId + " is locked");
//
//		try {
//			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + databaseId);
//			//close the owl
//			database.getBaseDataEngine().close();
//			owlFile = new File(database.getProperty(Constants.OWL));
//
//			classLogger.info("Pulling owl and postions.json for " + appFolder + " from remote=" + databaseId);
//
//			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
//			//sync will delete files that are in the destination if they aren't being synced
//			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", appRcloneConfig + ":"+ DB_CONTAINER_PREFIX +databaseId+"/"+ owlFile.getName(), appFolder);
//			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", appRcloneConfig + ":"+ DB_CONTAINER_PREFIX + databaseId+"/"+ AbstractDatabaseEngine.OWL_POSITION_FILENAME, appFolder);
//
//		}  finally {
//			try {
//				if (appRcloneConfig != null) {
//					deleteRcloneConfig(appRcloneConfig);
//				}
//				//open the owl
//				if(owlFile!=null && owlFile.exists()) {
//					database.setOWL(owlFile.getAbsolutePath());
//				} else {
//					throw new IllegalArgumentException("Pull failed. OWL for engine " + databaseId + " was not found");
//				}
//			} finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("App "+ databaseId + " is unlocked");
//			}
//		}
//	}
//
//	@Override
//	public void pushOwl(String databaseId) throws IOException, InterruptedException{
//		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
//		if (database == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//		String appRcloneConfig = null;
//		File owlFile = null;
//		String alias = SecurityEngineUtils.getEngineAliasForId(databaseId);
//		String aliasAppId = alias + "__" + databaseId;
//		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to push owl");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ databaseId + " is locked");
//		try {
//			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + databaseId);
//			//close the owl
//			database.getBaseDataEngine().close();
//			owlFile = new File(database.getProperty(Constants.OWL));
//
//			classLogger.info("Pushing owl and postions.json for " + appFolder + " from remote=" + databaseId);
//
//
//			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
//			//sync will delete files that are in the destination if they aren't being synced
//
//			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", appFolder+"/" + owlFile.getName(), appRcloneConfig + ":" + DB_CONTAINER_PREFIX + databaseId);			 
//			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", appFolder+"/" + AbstractDatabaseEngine.OWL_POSITION_FILENAME, appRcloneConfig + ":" + DB_CONTAINER_PREFIX + databaseId);			 
//
//
//		}  finally {
//			try {
//				if (appRcloneConfig != null) {
//					deleteRcloneConfig(appRcloneConfig);
//				}
//				//open the owl
//				if(owlFile!=null && owlFile.exists()) {
//					database.setOWL(owlFile.getAbsolutePath());
//				} else {
//					throw new IllegalArgumentException("Push failed. OWL for engine " + databaseId + " was not found");
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("App "+ databaseId + " is unlocked");
//			}
//		}
//	}
//
//
//	@Override
//	public void pullInsightsDB(String projectId) throws IOException, InterruptedException{
//		IProject project = Utility.getProject(projectId, false);
//		if (project == null) {
//			throw new IllegalArgumentException("Project not found...");
//		}
//		
//		String appRcloneConfig = null;
//		String alias = project.getProjectName();
//		String aliasProjectId = SmssUtilities.getUniqueName(alias, projectId);
//		String thisProjectFolder = this.projectFolder + FILE_SEPARATOR + aliasProjectId;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + projectId + " to pull insights db");
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ projectId + " is locked");
//		try {
//			appRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
//			classLogger.info("Pulling insights database for " + alias + " from remote=" + projectId);
//			String insightDB = getInsightDB(project, thisProjectFolder);
//
//			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
//			//sync will delete files that are in the destination if they aren't being synced
//			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", appRcloneConfig  + ":" + PROJECT_CONTAINER_PREFIX + projectId + "/" + insightDB, thisProjectFolder);
//
//		}  finally {
//			try {
//				if (appRcloneConfig != null) {
//					deleteRcloneConfig(appRcloneConfig);
//				}
//				//open the insight db
//				String insightDbLoc = SmssUtilities.getInsightsRdbmsFile(project.getSmssProp()).getAbsolutePath();
//				if(insightDbLoc != null) {
//					try {
//						project.setInsightDatabase( ProjectHelper.loadInsightsEngine(project.getSmssProp(), LogManager.getLogger(AbstractDatabaseEngine.class)));
//					} catch (Exception e) {
//						classLogger.error(Constants.STACKTRACE, e);
//						throw new IllegalArgumentException("Error in loading new insights database for project " + aliasProjectId);
//					}
//				} else {
//					throw new IllegalArgumentException("Insight database was not able to be found");
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("Project "+ projectId + " is unlocked");
//			}
//		}
//	}
//
//	@Override
//	public void pushInsightDB(String projectId) throws IOException, InterruptedException {
//		IProject project = Utility.getProject(projectId, false);
//		if (project == null) {
//			throw new IllegalArgumentException("Project not found...");
//		}
//		String appRcloneConfig = null;
//		String alias = project.getProjectName();
//		String aliasProjectId = SmssUtilities.getUniqueName(alias, projectId);
//		String thisProjectFolder = this.projectFolder + FILE_SEPARATOR + aliasProjectId;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + projectId + " to push insights db");
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ projectId + " is locked");
//		try {
//			appRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
//			project.getInsightDatabase().close();
//			classLogger.info("Pushing insights database for " + alias + " from remote=" + projectId);
//			String insightDB = getInsightDB(project, thisProjectFolder);
//
//			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
//			//sync will delete files that are in the destination if they aren't being synced
//
//			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", thisProjectFolder+"/"+ insightDB, appRcloneConfig + ":" + PROJECT_CONTAINER_PREFIX + projectId);			 
//		}
//		finally {
//			try {
//				if (appRcloneConfig != null) {
//					deleteRcloneConfig(appRcloneConfig);
//				}
//				//open the insight db
//				String insightDbLoc = SmssUtilities.getInsightsRdbmsFile(project.getSmssProp()).getAbsolutePath();
//				if(insightDbLoc != null) {
//					try {
//						project.setInsightDatabase( ProjectHelper.loadInsightsEngine(project.getSmssProp(), LogManager.getLogger(AbstractDatabaseEngine.class)));
//					} catch (Exception e) {
//						classLogger.error(Constants.STACKTRACE, e);
//						throw new IllegalArgumentException("Error in loading new insights database for project " + aliasProjectId);
//					}				
//				} else {
//					throw new IllegalArgumentException("Insight database was not able to be found");
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("Project "+ projectId + " is unlocked");
//			}
//
//		}
//	}
//
//
//	@Override
//	public void pushLocalDatabaseFile(String databaseId, RdbmsTypeEnum dbType) throws IOException, InterruptedException {
//		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
//		if (database == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//		String appRcloneConfig = null;
//		String alias = SecurityEngineUtils.getEngineAliasForId(databaseId);
//		String aliasAppId = alias + "__" + databaseId;
//		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to push db file");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ databaseId + " is locked");
//		try {
//			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + databaseId);
//			DIHelper.getInstance().removeEngineProperty(databaseId);
//			database.close();
//
//			classLogger.info("Pushing database for " + alias + " from remote=" + databaseId);
//			if(dbType == RdbmsTypeEnum.SQLITE){
//				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
//				
//				for(String sqliteFile : sqliteFileNames){
//					runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appFolder + "/" + sqliteFile, appRcloneConfig + ":"+ DB_CONTAINER_PREFIX+ databaseId);
//				}
//			} else if(dbType == RdbmsTypeEnum.H2_DB){
//				runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appFolder + "/database.mv.db", appRcloneConfig + ":"+DB_CONTAINER_PREFIX+databaseId);
//			} else{
//				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
//			}
//
//			//open the engine again
//			Utility.getDatabase(databaseId, false);
//		} finally {
//			try {
//				if (appRcloneConfig != null) {
//					deleteRcloneConfig(appRcloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("App "+ databaseId + " is unlocked");
//			}
//		}		
//	}
//
//	@Override
//	public void pushDatabaseSmss(String databaseId) throws IOException, InterruptedException {
//		// We need to push the file alias__appId.smss
//		String alias = SecurityEngineUtils.getEngineAliasForId(databaseId);
//		String smss = alias + "__" + databaseId + ".smss";
//		String smssFile = Utility.normalizePath(dbFolder + FILE_SEPARATOR + smss);
//		
//		// Start with the sas token
//		String smssRCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to push app");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ databaseId + " is locked");
//		try {
//			String smssContainer = databaseId + SMSS_POSTFIX;
//			smssRCloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + smssContainer);
//
//			// Some temp files needed for the transfer
//			File temp = null;
//			File copy = null;
//			try {
//				// Move the smss to an empty temp directory (otherwise will push all items in the db folder)
//				String tempFolder = Utility.getRandomString(10);
//				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
//				temp.mkdir();
//				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
//				Files.copy(new File(Utility.normalizePath(smssFile)), copy);
//
//				// Push the smss
//				classLogger.info("Pushing smss from source=" + smssFile + " to remote=" + smssContainer);
//				runRcloneTransferProcess(smssRCloneConfig, "rclone", "sync", temp.getPath(), smssRCloneConfig + ":" + DB_CONTAINER_PREFIX + smssContainer);
//				classLogger.debug("Done pushing from source=" + smssFile + " to remote=" + smssContainer);
//			} finally {
//				if (copy != null) {
//					copy.delete();
//				}
//				if (temp != null) {
//					temp.delete();
//				}
//
//				// Re-open the database
//				Utility.getDatabase(databaseId, false);
//			}
//		} finally {
//			try {
//				if (smssRCloneConfig != null) {
//					deleteRcloneConfig(smssRCloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("App "+ databaseId + " is unlocked");
//			}
//		}
//	}
//	
//	@Override
//	public void pullDatabaseSmss(String databaseId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//	
//	@Override
//	public void pullLocalDatabaseFile(String databaseId, RdbmsTypeEnum rdbmsType) throws IOException, InterruptedException {
//		IDatabaseEngine engine = Utility.getDatabase(databaseId, false);
//		if (engine == null) {
//			throw new IllegalArgumentException("Database not found...");
//		}
//		String appRcloneConfig = null;
//		String alias = SecurityEngineUtils.getEngineAliasForId(databaseId);
//		String aliasAppId = alias + "__" + databaseId;
//		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to pull database file");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("Database "+ databaseId + " is locked");
//		try {
//			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + databaseId);
//			engine.close();
//			classLogger.info("Pulling database for " + alias + " from remote=" + databaseId);
//			if(rdbmsType == RdbmsTypeEnum.SQLITE){
//				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
//				//TODO kunal: below calls will break
//				for(String sqliteFile : sqliteFileNames){			
//					runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appRcloneConfig + ":"+DB_CONTAINER_PREFIX+databaseId+"/"+sqliteFile, appFolder);
//				}
//			} else if(rdbmsType == RdbmsTypeEnum.H2_DB){
//				runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appRcloneConfig + ":" + DB_CONTAINER_PREFIX+databaseId+"/database.mv.db", appFolder);
//			} else{
//				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
//			}
//		} finally {
//			try {
//				if (appRcloneConfig != null) {
//					deleteRcloneConfig(appRcloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("Database "+ databaseId + " is unlocked");
//			}
//		}
//	}
//
//
//	@Override
//	public void pullDatabaseFolder(String appId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException {
//		IDatabaseEngine engine = Utility.getDatabase(appId, false);
//		if (engine == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//		String appRcloneConfig = null;
//		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
//		//String aliasAppId = alias + "__" + appId;
//		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		// adding a lock for now, but there may be times we don't need one and other times we do
//		// reaching h2 db from version folder vs static assets in asset app
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + appId + " to pull folder " + remoteRelativePath);
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
//		lock.lock();
//		classLogger.info("App "+ appId + " is locked");
//		try {
//			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + appId);
//			classLogger.info("Pulling folder for " + remoteRelativePath + " from remote=" + appId);
//
//			runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appRcloneConfig + ":"+ DB_CONTAINER_PREFIX +appId+  "/" + remoteRelativePath, absolutePath);
//		} finally {
//			try {
//				if (appRcloneConfig != null) {
//					deleteRcloneConfig(appRcloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("App "+ appId + " is unlocked");
//
//			}
//		}
//	}
//
//	@Override
//	public void pushDatabaseFolder(String databaseId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException {
//		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
//		if (database == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//		String appRcloneConfig = null;
//		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
//		//String aliasAppId = alias + "__" + appId;
//		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		File absoluteFolder = new File(absolutePath);
//		if(absoluteFolder.isDirectory()) {
//			//this is adding a hidden file into every sub folder to make sure there is no empty directory
//			ClusterUtil.validateFolder(absoluteFolder.getAbsolutePath());
//		}
//		// adding a lock for now, but there may be times we don't need one and other times we do
//		// reaching h2 db from version folder vs static assets in asset app
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to push folder " + remoteRelativePath);
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ databaseId + " is locked");
//
//		try {
//			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + databaseId);
//			classLogger.info("Pushing folder for " + remoteRelativePath + " to remote=" + databaseId);
//
//			runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", absolutePath, appRcloneConfig + ":"+DB_CONTAINER_PREFIX+databaseId+  "/" + remoteRelativePath);
//		} finally {
//			try {
//				if (appRcloneConfig != null) {
//					deleteRcloneConfig(appRcloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("App "+ databaseId + " is unlocked");
//			}
//		}
//	}
//
//	@Override
//	public void pullProjectFolder(String projectId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException {
//		IProject project = Utility.getProject(projectId, false);
//		if (project == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//		String rcloneConfig = null;
//
//		classLogger.info("Applying lock for " + Utility.cleanLogString(projectId) + " to pull folder " + remoteRelativePath);
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ Utility.cleanLogString(projectId) + " is locked");
//		try {
//			rcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
//			classLogger.info("Pulling folder for " + remoteRelativePath + " from remote=" + Utility.cleanLogString(projectId));
//
//			runRcloneTransferProcess(rcloneConfig, "rclone", "sync", rcloneConfig + ":"+PROJECT_CONTAINER_PREFIX+projectId+  "/" + remoteRelativePath, absolutePath);
//		} finally {
//			try {
//				if (rcloneConfig != null) {
//					deleteRcloneConfig(rcloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("Project "+ Utility.cleanLogString(projectId) + " is unlocked");
//
//			}
//		}
//	}
//	
//	@Override
//	public void pushProjectFolder(String projectId, String absolutePath, String remoteRelativePath)
//			throws IOException, InterruptedException {
//		IProject project = Utility.getProject(projectId, false);
//		if (project == null) {
//			throw new IllegalArgumentException("Project not found...");
//		}
//		String appRcloneConfig = null;
//		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
//		//String aliasAppId = alias + "__" + appId;
//		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		File absoluteFolder = new File(Utility.normalizePath(absolutePath));
//		if(absoluteFolder.isDirectory()) {
//			//this is adding a hidden file into every sub folder to make sure there is no empty directory
//			ClusterUtil.validateFolder(absoluteFolder.getAbsolutePath());
//		}
//		// adding a lock for now, but there may be times we don't need one and other times we do
//		// reaching h2 db from version folder vs static assets in asset app
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + Utility.cleanLogString(projectId) + " to push folder " + remoteRelativePath);
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ projectId + " is locked");
//
//		try {
//			appRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
//			classLogger.info("Pushing folder for " + remoteRelativePath + " to remote=" + Utility.cleanLogString(projectId));
//
//			runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", absolutePath, appRcloneConfig + ":" + PROJECT_CONTAINER_PREFIX+projectId+  "/" + remoteRelativePath);
//		} finally {
//			try {
//				if (appRcloneConfig != null) {
//					deleteRcloneConfig(appRcloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("Project "+ Utility.cleanLogString(projectId) + " is unlocked");
//			}
//		}
//	}
//	
//	@Override
//	@Deprecated
//	public void fixLegacyDbStructure(String appId) throws IOException, InterruptedException {
//		String smssContainer = appId + SMSS_POSTFIX;
//		
//		String smssCloneConfig = null;
//		String rCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + appId + " to pull app");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
//		lock.lock();
//		classLogger.info("App "+ appId + " is locked");
//		try {
//			smssCloneConfig = createRcloneConfig(smssContainer);
//			rCloneConfig = createRcloneConfig(appId);
//			// WE HAVE TO PULL FROM THE OLD LOCATION WITHOUT THERE BEING A /db/ IN THE PATH
//			List<String> results = runRcloneProcess(smssCloneConfig, "rclone", "lsf", smssCloneConfig+":" + smssContainer);
//			String smss = null;
//			for (String result : results) {
//				if (result.endsWith(".smss")) {
//					smss = result;
//					break;
//				}
//			}
//			if (smss == null) {
//				// IF STILL NOT FOUND... CANT HELP YOU
//				throw new IOException("Failed to pull app for appid=" + appId);
//			}
//			
//			String aliasAppId = smss.replaceAll(".smss", "");
//			File appFolder = new File(dbFolder + FILE_SEPARATOR + Utility.normalizePath(aliasAppId));
//			appFolder.mkdir();
//			// Pull the contents of the app folder before the smss
//			classLogger.info("Pulling app from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
//			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+":"+appId, appFolder.getPath());
//			classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
//
//			// Now pull the smss
//			classLogger.info("Pulling smss from remote=" + smssContainer + " to target=" + dbFolder);
//			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
//			runRcloneTransferProcess(smssCloneConfig, "rclone", "copy", smssCloneConfig+":"+smssContainer, dbFolder);
//			classLogger.debug("Done pulling from remote=" + smssContainer + " to target=" + dbFolder);
//
//			LegacyToProjectRestructurerHelper fixer = new LegacyToProjectRestructurerHelper();
//			fixer.init();
//			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
//			String dbDir = baseFolder + LegacyToProjectRestructurerHelper.ENGINE_DIRECTORY;
//			String projectDir = baseFolder + LegacyToProjectRestructurerHelper.PROJECT_DIRECTORY;
//			fixer.copyDataToNewFolderStructure(aliasAppId, projectDir, dbDir);
//			
//			Utility.loadDatabase(dbDir + "/" + smss, Utility.loadProperties(dbDir + "/" + smss));
//			Utility.loadProject(projectDir + "/" + smss, Utility.loadProperties(projectDir + "/" + smss));
//
//			// now push the new db and app into the right locations
//			pushDatabase(appId);
//			pushProject(appId);
//		} finally {
//			try {
//				if (smssCloneConfig != null) {
//					deleteRcloneConfig(smssCloneConfig);
//				}
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("App "+ appId + " is unlocked");
//			}
//		}
//	}
//	
//	@Override
//	@Deprecated
//	public void fixLegacyUserAssetStructure(String appId, boolean isAsset) throws IOException, InterruptedException {
//		String smssContainer = appId + SMSS_POSTFIX;
//		String smssCloneConfig = null;
//		String rCloneConfig = null;
//
//		try {
//			smssCloneConfig = createRcloneConfig(smssContainer);
//			rCloneConfig = createRcloneConfig(appId);			
//			// WE HAVE TO PULL FROM THE OLD LOCATION WITHOUT THERE BEING A /db/ IN THE PATH
//			List<String> results = runRcloneProcess(smssCloneConfig, "rclone", "lsf", smssCloneConfig+":"+smssContainer);
//			String smss = null;
//			for (String result : results) {
//				if (result.endsWith(".smss")) {
//					smss = result;
//					break;
//				}
//			}
//			if (smss == null) {
//				// IF STILL NOT FOUND... CANT HELP YOU
//				throw new IOException("Failed to pull app for appid=" + appId);
//			}
//
//			String aliasAppId = smss.replaceAll(".smss", "");
//			File appFolder = new File(dbFolder + FILE_SEPARATOR + Utility.normalizePath(aliasAppId));
//			appFolder.mkdir();
//			// Pull the contents of the app folder before the smss
//			classLogger.info("Pulling app from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
//			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+":"+appId, appFolder.getPath());
//			classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
//
//			// Now pull the smss
//			classLogger.info("Pulling smss from remote=" + smssContainer + " to target=" + dbFolder);
//			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
//			runRcloneTransferProcess(smssCloneConfig, "rclone", "copy", smssCloneConfig+":"+smssContainer, dbFolder);
//			classLogger.debug("Done pulling from remote=" + smssContainer + " to target=" + dbFolder);
//
//			LegacyToProjectRestructurerHelper fixer = new LegacyToProjectRestructurerHelper();
//			fixer.init();
//			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
//			String dbDir = baseFolder + LegacyToProjectRestructurerHelper.ENGINE_DIRECTORY;
//			String userDir = baseFolder + LegacyToProjectRestructurerHelper.USER_DIRECTORY;
//			fixer.userCopyDataToNewFolderStructure(aliasAppId, userDir, dbDir, WorkspaceAssetUtils.isAssetProject(appId));
//			
//			// only load the project
//			Utility.loadProject(userDir + "/" + smss, Utility.loadProperties(userDir + "/" + smss));
//
//			// now push the project into the right locations
//			pushUserAssetOrWorkspace(appId, isAsset);
//		} finally {
//			if (smssCloneConfig != null) {
//				deleteRcloneConfig(smssCloneConfig);
//			}
//		}
//	}
//	
//	@Override
//	@Deprecated
//	public void fixLegacyImageStructure() throws IOException, InterruptedException {
//		String rCloneConfig = null;
//		String fixedCloneConfig = null;
//		try {
//			rCloneConfig = createRcloneConfig("semoss-imagecontainer");
//			fixedCloneConfig = createRcloneConfig(CentralCloudStorage.DB_IMAGES_BLOB);
//			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
//			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "databases";
//			File imageFolder = new File(imagesFolderPath);
//			imageFolder.mkdir();
//			// first pull
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":"+"semoss-imagecontainer", imagesFolderPath);
//			// now push into the correct folder
//			runRcloneProcess(fixedCloneConfig, "rclone", "sync", imagesFolderPath, fixedCloneConfig+":"+CentralCloudStorage.DB_IMAGES_BLOB);
//		} finally {
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
//			}
//			if (fixedCloneConfig != null) {
//				deleteRcloneConfig(fixedCloneConfig);
//			}
//		}
//	}
//	
//	@Override
//	public void pushProject(String projectId) throws IOException, InterruptedException {
//		IProject project = Utility.getProject(projectId, false);
//		if (project == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//
//		// We need to push the folder alias__appId and the file alias__appId.smss
//		String alias = project.getProjectName();
//		if(alias == null) {
//			alias = SecurityProjectUtils.getProjectAliasForId(projectId);
//		}
//
//		String aliasProjectId = alias + "__" + projectId;
//		String thisProjectFolder = projectFolder + FILE_SEPARATOR + aliasProjectId;
//		String smss = aliasProjectId + ".smss";
//		String smssFile = projectFolder + FILE_SEPARATOR + smss;
//
//		// Start with the sas token
//		String projectRcloneConfig = null;
//		String smssRCloneConfig = null;
//
//		// synchronize on the project id
//		classLogger.info("Applying lock for " + projectId + " to push app");
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ projectId + " is locked");
//		try {
//			projectRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
//			String smssContainer = projectId + SMSS_POSTFIX;
//			smssRCloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + smssContainer);
//
//			// Some temp files needed for the transfer
//			File temp = null;
//			File copy = null;
//
//			// Close the project, so that we can push without file locks (also ensures that the project doesn't change mid push)
//			try {
//				DIHelper.getInstance().removeProjectProperty(projectId);
//				project.close();
//
//				// Push the app folder
//				classLogger.info("Pushing app from source=" + thisProjectFolder + " to remote=" + projectId);
//				runRcloneTransferProcess(projectRcloneConfig, "rclone", "sync", thisProjectFolder, projectRcloneConfig + ":"+PROJECT_CONTAINER_PREFIX + projectId);
//				classLogger.debug("Done pushing from source=" + thisProjectFolder + " to remote=" + projectId);
//
//				// Move the smss to an empty temp directory (otherwise will push all items in the project folder)
//				String tempFolder = Utility.getRandomString(10);
//				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
//				temp.mkdir();
//				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
//				Files.copy(new File(Utility.normalizePath(smssFile)), copy);
//
//				// Push the smss
//				classLogger.info("Pushing smss from source=" + smssFile + " to remote=" + smssContainer);
//				runRcloneTransferProcess(smssRCloneConfig, "rclone", "sync", temp.getPath(), smssRCloneConfig + ":"+PROJECT_CONTAINER_PREFIX + smssContainer);
//				classLogger.debug("Done pushing from source=" + smssFile + " to remote=" + smssContainer);
//			} finally {
//				if (copy != null) {
//					copy.delete();
//				}
//				if (temp != null) {
//					temp.delete();
//				}
//
//				// Re-open the project
//				Utility.getProject(projectId, false);
//			}
//		} finally {
//			try {
//				if (projectRcloneConfig != null) {
//					deleteRcloneConfig(projectRcloneConfig);
//				}
//				if (smssRCloneConfig != null) {
//					deleteRcloneConfig(smssRCloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("Project "+ projectId + " is unlocked");
//			}
//		}
//	}
//	
//	@Override
//	public void pushProjectSmss(String projectId) throws IOException, InterruptedException {
//		// We need to push the file alias__appId.smss
//		String alias = SecurityProjectUtils.getProjectAliasForId(projectId);
//		String smss = alias + "__" + projectId + ".smss";
//		String smssFile = Utility.normalizePath(projectFolder + FILE_SEPARATOR + smss);
//		
//		// Start with the sas token
//		String smssRCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + projectId + " to push app");
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ projectId + " is locked");
//		try {
//			String smssContainer = projectId + SMSS_POSTFIX;
//			smssRCloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + smssContainer);
//
//			// Some temp files needed for the transfer
//			File temp = null;
//			File copy = null;
//			try {
//				// Move the smss to an empty temp directory (otherwise will push all items in the project folder)
//				String tempFolder = Utility.getRandomString(10);
//				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
//				temp.mkdir();
//				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
//				Files.copy(new File(Utility.normalizePath(smssFile)), copy);
//
//				// Push the smss
//				classLogger.info("Pushing smss from source=" + smssFile + " to remote=" + smssContainer);
//				runRcloneTransferProcess(smssRCloneConfig, "rclone", "sync", temp.getPath(), smssRCloneConfig + ":"+PROJECT_CONTAINER_PREFIX + smssContainer);
//				classLogger.debug("Done pushing from source=" + smssFile + " to remote=" + smssContainer);
//			} finally {
//				if (copy != null) {
//					copy.delete();
//				}
//				if (temp != null) {
//					temp.delete();
//				}
//			}
//		} finally {
//			try {
//				if (smssRCloneConfig != null) {
//					deleteRcloneConfig(smssRCloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("Project "+ projectId + " is unlocked");
//			}
//		}
//	}
//	
//	@Override
//	public void pullProjectSmss(String projectId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void pullProject(String projectId) throws IOException, InterruptedException {
//		pullProject(projectId, false);
//	}
//
//	@Override
//	public void pullProject(String projectId, boolean projectAlreadyLoaded) throws IOException, InterruptedException {
//		IProject project = null;
//		if (projectAlreadyLoaded) {
//			project = Utility.getProject(projectId, false);
//			if (project == null) {
//				throw new IllegalArgumentException("Project not found...");
//			}
//		}
//
//		String smssContainer = projectId + SMSS_POSTFIX;
//
//		// Start with the sas token
//		String projectRcloneConfig = null;
//		String smssRcloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + projectId + " to pull project");
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ projectId + " is locked");
//		try {
//			projectRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
//			smssRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + smssContainer);
//
//			// List the smss directory to get the alias + app id
//			List<String> results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig + ":"+PROJECT_CONTAINER_PREFIX + smssContainer);
//			String smss = null;
//			for (String result : results) {
//				if (result.endsWith(".smss")) {
//					smss = result;
//					break;
//				}
//			}
//			if (smss == null) {
//				try {
//					fixLegacyDbStructure(projectId);
//				} catch(IOException | InterruptedException e) {
//					classLogger.info(Constants.STACKTRACE, e);
//					throw new IOException("Failed to pull app for projectId=" + projectId);
//				}
//				
//				// try again
//				results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig+":"+PROJECT_CONTAINER_PREFIX + smssContainer);
//				for (String result : results) {
//					if (result.endsWith(".smss")) {
//						smss = result;
//						break;
//					}
//				}
//				
//				if (smss == null) {
//					throw new IOException("Failed to pull app for projectId=" + projectId);
//				} else {
//					// we just fixed the structure and this was pulled and synched up
//					// can just return from here
//					return;
//				}
//			}
//
//			// We need to pull the folder alias__appId and the file alias__appId.smss
//			String aliasProjectId = smss.replaceAll(".smss", "");
//
//			// Close the database (if an existing app), so that we can pull without file locks
//			try {
//				if (projectAlreadyLoaded) {
//					DIHelper.getInstance().removeProjectProperty(projectId);
//					project.close();
//				}
//
//				// Make the app directory (if it doesn't already exist)
//				File thisProjectFolder = new File(Utility.normalizePath(projectFolder + FILE_SEPARATOR + aliasProjectId));
//				thisProjectFolder.mkdir(); 
//
//				// Pull the contents of the project folder before the smss
//				classLogger.info("Pulling app from remote=" + projectId + " to target=" + thisProjectFolder.getPath());
//				runRcloneTransferProcess(projectRcloneConfig, "rclone", "sync", projectRcloneConfig + ":" + PROJECT_CONTAINER_PREFIX + projectId, thisProjectFolder.getPath());
//				classLogger.debug("Done pulling from remote=" + projectId + " to target=" + thisProjectFolder.getPath());
//
//				// Now pull the smss
//				classLogger.info("Pulling smss from remote=" + smssContainer + " to target=" + projectFolder);
//
//				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE PROJECT FOLDER
//				runRcloneTransferProcess(smssRcloneConfig, "rclone", "copy", smssRcloneConfig + ":"+PROJECT_CONTAINER_PREFIX + smssContainer, projectFolder);
//				classLogger.debug("Done pulling from remote=" + smssContainer + " to target=" + projectFolder);
//
//				// Catalog the project if it is new
//				if (!projectAlreadyLoaded) {
//					ProjectWatcher.catalogProject(smss, projectFolder);
//				}
//			} finally {
//				// Re-open the database (if an existing app)
//				if (projectAlreadyLoaded) {
//					Utility.getProject(projectId, false);
//				}
//			}
//		} finally {
//			try {
//				if (projectRcloneConfig != null) {
//					deleteRcloneConfig(projectRcloneConfig);
//				}
//				if (smssRcloneConfig != null) {
//					deleteRcloneConfig(smssRcloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("Project "+ projectId + " is unlocked");
//			}
//		}
//	}
//	
//	@Override
//	public void pushInsight(String projectId, String insightId) throws IOException, InterruptedException {
//		IProject project = Utility.getProject(projectId);
//		if (project == null) {
//			throw new IllegalArgumentException("Project not found...");
//		}
//		String rcloneConfig = null;
//
//		try {
//			rcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
//
//			// only need to pull the insight folder - 99% the project is always already loaded to get to this point
//			String insightFolderPath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId);
//			File insightFolder = new File(insightFolderPath);
//			insightFolder.mkdir();
//			
//			String remoteInsightLoc = PROJECT_CONTAINER_PREFIX+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;
//			
//			classLogger.info("Pushing insight from local=" + Utility.cleanLogString(insightFolder.getPath()) + " to remote=" + Utility.cleanLogString(remoteInsightLoc));
//			runRcloneTransferProcess(rcloneConfig, "rclone", "sync", 
//					insightFolder.getPath(),
//					rcloneConfig+":"+remoteInsightLoc);
//			classLogger.debug("Done pushing insight from local=" + Utility.cleanLogString(insightFolder.getPath()) + " to remote=" + Utility.cleanLogString(remoteInsightLoc));
//		} finally {
//			if (rcloneConfig != null) {
//				deleteRcloneConfig(rcloneConfig);
//			}
//		}
//	}
//
//	@Override
//	public void pullInsight(String projectId, String insightId) throws IOException, InterruptedException {
//		IProject project = Utility.getProject(projectId);
//		if (project == null) {
//			throw new IllegalArgumentException("Project not found...");
//		}
//		String rcloneConfig = null;
//
//		try {
//			rcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
//
//			// only need to pull the insight folder - 99% the project is always already loaded to get to this point
//			String insightFolderPath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId);
//			File insightFolder = new File(insightFolderPath);
//			insightFolder.mkdir();
//			
//			String remoteInsightLoc = PROJECT_CONTAINER_PREFIX+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;
//			
//			classLogger.info("Pulling insight from remote=" + Utility.cleanLogString(remoteInsightLoc) + " to target=" + Utility.cleanLogString(insightFolder.getPath()));
//			runRcloneTransferProcess(rcloneConfig, "rclone", "sync", 
//					rcloneConfig+":"+remoteInsightLoc,
//					insightFolder.getPath());
//			classLogger.debug("Done pulling insight from remote=" + Utility.cleanLogString(remoteInsightLoc) + " to target=" + Utility.cleanLogString(insightFolder.getPath()));
//		} finally {
//			if (rcloneConfig != null) {
//				deleteRcloneConfig(rcloneConfig);
//			}
//		}
//	}
//	
//	@Override
//	public void pushInsightImage(String projectId, String insightId, String oldImageFileName, String newImageFileName) throws IOException, InterruptedException {
//		IProject project = Utility.getProject(projectId, false);
//		if (project == null) {
//			throw new IllegalArgumentException("Project not found...");
//		}
//		String rcloneConfig = null;
//
//		try {
//			rcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
//
//			String remoteInsightImageFilePath = PROJECT_CONTAINER_PREFIX+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;
//			
//			// since extensions might be different, need to actually delete the old file by name
//			if(oldImageFileName != null) {
//				String oldFileToDelete = remoteInsightImageFilePath+"/"+oldImageFileName;
//				
//				classLogger.info("Deleting old insight image from remote=" + Utility.cleanLogString(oldFileToDelete));
//				runRcloneDeleteFileProcess(rcloneConfig, "rclone", "deletefile", rcloneConfig+":"+oldFileToDelete);
//				classLogger.debug("Done deleting old insight image from remote=" + Utility.cleanLogString(oldFileToDelete));
//			} else {
//				classLogger.info("No old insight image on remote to delete");
//			}
//
//			if(newImageFileName != null) {
//				String insightImageFilePath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId + "/" + newImageFileName);
//	
//				classLogger.info("Pushing insight image from local=" + Utility.cleanLogString(insightImageFilePath) + " to remote=" + Utility.cleanLogString(remoteInsightImageFilePath));
//				runRcloneTransferProcess(rcloneConfig, "rclone", "sync", 
//						insightImageFilePath,
//						rcloneConfig+":"+remoteInsightImageFilePath);
//				classLogger.debug("Done pushing insight image from local=" + Utility.cleanLogString(insightImageFilePath) + " to remote=" + Utility.cleanLogString(remoteInsightImageFilePath));
//			} else {
//				classLogger.info("No new insight image to add to remote");
//			}
//		} finally {
//			if (rcloneConfig != null) {
//				deleteRcloneConfig(rcloneConfig);
//			}
//		}
//	}
//
//	@Override
//	public void pullUserAssetOrWorkspace(String projectId, boolean isAsset, boolean projectAlreadyLoaded) throws IOException, InterruptedException {
//		IProject project = null;
//		if (projectAlreadyLoaded) {
//			project = Utility.getUserAssetWorkspaceProject(projectId, isAsset);
//			if (project == null) {
//				throw new IllegalArgumentException("User asset/workspace project not found...");
//			}
//		}
//
//		String smssContainer = projectId + SMSS_POSTFIX;
//
//		// Start with the sas token
//		String userRcloneConfig = null;
//		String smssRcloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + projectId + " to pull project");
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ projectId + " is locked");
//		try {
//			userRcloneConfig = createRcloneConfig(USER_CONTAINER_PREFIX + projectId);
//			smssRcloneConfig = createRcloneConfig(USER_CONTAINER_PREFIX + smssContainer);
//
//			// List the smss directory to get the alias + app id
//			List<String> results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig + ":" + USER_CONTAINER_PREFIX + smssContainer);
//			String smss = null;
//			for (String result : results) {
//				if (result.endsWith(".smss")) {
//					smss = result;
//					break;
//				}
//			}
//			if (smss == null) {
//				// assume this is for pulling an unprocessed project from an app
//				try {
//					fixLegacyUserAssetStructure(projectId, isAsset);
//				} catch(IOException | InterruptedException e) {
//					classLogger.info(Constants.STACKTRACE, e);
//					throw new IOException("Failed to pull project for projectId=" + projectId);
//				}
//				
//				// try again
//				results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig+":" + USER_CONTAINER_PREFIX + smssContainer);
//				for (String result : results) {
//					if (result.endsWith(".smss")) {
//						smss = result;
//						break;
//					}
//				}
//				
//				if (smss == null) {
//					throw new IOException("Failed to pull project for projectId=" + projectId);
//				} else {
//					// we just fixed the structure and this was pulled and synched up
//					// can just return from here
//					return;
//				}
//			}
//
//			// We need to pull the folder alias__appId and the file alias__appId.smss
//			String aliasProjectId = smss.replaceAll(".smss", "");
//
//			// Close the database (if an existing app), so that we can pull without file locks
//			try {
//				if (projectAlreadyLoaded) {
//					DIHelper.getInstance().removeProjectProperty(projectId);
//					project.close();
//				}
//
//				// Make the app directory (if it doesn't already exist)
//				File thisUserFolder = new File(Utility.normalizePath(userFolder + FILE_SEPARATOR + aliasProjectId));
//				thisUserFolder.mkdir(); 
//
//				// Pull the contents of the project folder before the smss
//				classLogger.info("Pulling app from remote=" + projectId + " to target=" + thisUserFolder.getPath());
//				runRcloneTransferProcess(userRcloneConfig, "rclone", "sync", userRcloneConfig + ":" + USER_CONTAINER_PREFIX + projectId, thisUserFolder.getPath());
//				classLogger.debug("Done pulling from remote=" + projectId + " to target=" + thisUserFolder.getPath());
//
//				// Now pull the smss
//				classLogger.info("Pulling smss from remote=" + smssContainer + " to target=" + userFolder);
//				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE USER FOLDER
//				runRcloneTransferProcess(smssRcloneConfig, "rclone", "copy", smssRcloneConfig + ":" + USER_CONTAINER_PREFIX + smssContainer, userFolder);
//				classLogger.debug("Done pulling from remote=" + smssContainer + " to target=" + userFolder);
//			} finally {
//				// Re-open the project
//				if (projectAlreadyLoaded) {
//					Utility.getUserAssetWorkspaceProject(projectId, isAsset);
//				}
//			}
//		} finally {
//			try {
//				if (userRcloneConfig != null) {
//					deleteRcloneConfig(userRcloneConfig);
//				}
//				if (smssRcloneConfig != null) {
//					deleteRcloneConfig(smssRcloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("Project "+ projectId + " is unlocked");
//			}
//		}
//	}
//
//	@Override
//	public void pushUserAssetOrWorkspace(String projectId, boolean isAsset) throws IOException, InterruptedException {
//		IProject project = Utility.getUserAssetWorkspaceProject(projectId, isAsset);
//		if (project == null) {
//			throw new IllegalArgumentException("User asset/workspace project not found...");
//		}
//
//		// We need to push the folder alias__appId and the file alias__appId.smss
//		String alias = project.getProjectName();
//
//		String aliasProjectId = alias + "__" + projectId;
//		String thisUserFolder = userFolder + FILE_SEPARATOR + aliasProjectId;
//		String smss = aliasProjectId + ".smss";
//		String smssFile = userFolder + FILE_SEPARATOR + smss;
//
//		// Start with the sas token
//		String userRcloneConfig = null;
//		String smssRCloneConfig = null;
//
//		try {
//			userRcloneConfig = createRcloneConfig(USER_CONTAINER_PREFIX + projectId);
//			String smssContainer = projectId + SMSS_POSTFIX;
//			smssRCloneConfig = createRcloneConfig(USER_CONTAINER_PREFIX + smssContainer);
//
//			// Some temp files needed for the transfer
//			File temp = null;
//			File copy = null;
//
//			// Close the database, so that we can push without file locks (also ensures that the db doesn't change mid push)
//			try {
//				DIHelper.getInstance().removeProjectProperty(projectId);
//				project.close();
//
//				// Push the app folder
//				classLogger.info("Pushing app from source=" + thisUserFolder + " to remote=" + projectId);
//				runRcloneTransferProcess(userRcloneConfig, "rclone", "sync", thisUserFolder, userRcloneConfig + ":" + USER_CONTAINER_PREFIX + projectId);
//				classLogger.debug("Done pushing from source=" + thisUserFolder + " to remote=" + projectId);
//
//				// Move the smss to an empty temp directory (otherwise will push all items in the db folder)
//				String tempFolder = Utility.getRandomString(10);
//				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
//				temp.mkdir();
//				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
//				Files.copy(new File(Utility.normalizePath(smssFile)), copy);
//
//				// Push the smss
//				classLogger.info("Pushing smss from source=" + smssFile + " to remote=" + smssContainer);
//				runRcloneTransferProcess(smssRCloneConfig, "rclone", "sync", temp.getPath(), smssRCloneConfig + ":" + USER_CONTAINER_PREFIX + smssContainer);
//				classLogger.debug("Done pushing from source=" + smssFile + " to remote=" + smssContainer);
//			} finally {
//				if (copy != null) {
//					copy.delete();
//				}
//				if (temp != null) {
//					temp.delete();
//				}
//
//				// Re-open the database
//				Utility.getUserAssetWorkspaceProject(projectId, isAsset);
//			}
//		} finally {
//			if (userRcloneConfig != null) {
//				deleteRcloneConfig(userRcloneConfig);
//			}
//			if (smssRCloneConfig != null) {
//				deleteRcloneConfig(smssRCloneConfig);
//			}
//		}
//	}
//	
//
//	// This is the sync the whole app. It shouldn't be used yet. Only the insights DB should be sync actively 
//
//	//	public  Boolean syncApp(String appId) throws IOException, InterruptedException{
//	//		Boolean sync = false;
//	//		IDatabase engine = Utility.getDatabase(appId, false);
//	//		if (engine == null) {
//	//			throw new IllegalArgumentException("App not found...");
//	//		}
//	//		String appRcloneConfig = null;
//	//		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
//	//		String aliasAppId = alias + "__" + appId;
//	//		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//	//		try {
//	//			appRcloneConfig = createRcloneConfig(appId);
//	//				engine.close();
//	//				classLogger.debug("Checking from app path" + appFolder + " to remote=" + appId);
//	//				List<String> results = runRcloneProcess(appRcloneConfig, "rclone", "check", appFolder+FILE_SEPARATOR + "insights_database.mv.db", appRcloneConfig + ":"+appId);
//	//				for(String s:results){
//	//				classLogger.debug("Result String: " + s);
//	//				}
//	//				if(results.get(0).contains("ERROR")){
//	//					sync=true;
//	//				}
//	//		}  finally {
//	//			if (appRcloneConfig != null) {
//	//				deleteRcloneConfig(appRcloneConfig);
//	//			}
//	//		}
//	//
//	//		return sync;
//	//	}
//
//	//////////////////////////////////////////////////////////////////////////////////////////
//	//////////////////////////////////////// Push ////////////////////////////////////////////
//
//	public void pushDatabase(String databaseId) throws IOException, InterruptedException {
//		IDatabaseEngine engine = Utility.getDatabase(databaseId, false);
//		if (engine == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//
//		DATABASE_TYPE engineType = engine.getDatabaseType();
//
//		// We need to push the folder alias__appId and the file alias__appId.smss
//		String alias = null;
//		if (engineType == DATABASE_TYPE.APP){
//			alias = engine.getEngineName();
//		} else{
//			alias = SecurityEngineUtils.getEngineAliasForId(databaseId);
//		}
//
//		String aliasAppId = alias + "__" + databaseId;
//		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//		String smss = aliasAppId + ".smss";
//		String smssFile = dbFolder + FILE_SEPARATOR + smss;
//
//		// Start with the sas token
//		String appRcloneConfig = null;
//		String smssRCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to push app");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ databaseId + " is locked");
//		try {
//			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + databaseId);
//			String smssContainer = databaseId + SMSS_POSTFIX;
//			smssRCloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + smssContainer);
//
//			// Some temp files needed for the transfer
//			File temp = null;
//			File copy = null;
//
//			// Close the database, so that we can push without file locks (also ensures that the db doesn't change mid push)
//			try {
//				DIHelper.getInstance().removeEngineProperty(databaseId);
//				engine.close();
//
//				// Push the app folder
//				classLogger.info("Pushing app from source=" + appFolder + " to remote=" + databaseId);
//				runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appFolder, appRcloneConfig + ":"+DB_CONTAINER_PREFIX + databaseId);
//				classLogger.debug("Done pushing from source=" + appFolder + " to remote=" + databaseId);
//
//				// Move the smss to an empty temp directory (otherwise will push all items in the db folder)
//				String tempFolder = Utility.getRandomString(10);
//				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
//				temp.mkdir();
//				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
//				Files.copy(new File(Utility.normalizePath(smssFile)), copy);
//
//				// Push the smss
//				classLogger.info("Pushing smss from source=" + smssFile + " to remote=" + smssContainer);
//				runRcloneTransferProcess(smssRCloneConfig, "rclone", "sync", temp.getPath(), smssRCloneConfig + ":" + DB_CONTAINER_PREFIX + smssContainer);
//				classLogger.debug("Done pushing from source=" + smssFile + " to remote=" + smssContainer);
//			} finally {
//				if (copy != null) {
//					copy.delete();
//				}
//				if (temp != null) {
//					temp.delete();
//				}
//
//				// Re-open the database
//				Utility.getDatabase(databaseId, false);
//			}
//		} finally {
//			try {
//				if (appRcloneConfig != null) {
//					deleteRcloneConfig(appRcloneConfig);
//				}
//				if (smssRCloneConfig != null) {
//					deleteRcloneConfig(smssRCloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("App "+ databaseId + " is unlocked");
//			}
//		}
//	}
//
//	//////////////////////////////////////////////////////////////////////////////////////////
//	//////////////////////////////////////// Pull ////////////////////////////////////////////
//
//	public void pullDatabase(String databaseId) throws IOException, InterruptedException {
//		pullDatabase(databaseId, false);
//	}
//
//	public void pullDatabase(String databaseId, boolean databaseAlreadyLoaded) throws IOException, InterruptedException {
//		IDatabaseEngine database = null;
//		if (databaseAlreadyLoaded) {
//			database = Utility.getDatabase(databaseId, false);
//			if (database == null) {
//				throw new IllegalArgumentException("Database not found...");
//			}
//		}
//
//		String smssContainer = databaseId + SMSS_POSTFIX;
//
//		// Start with the sas token
//		String appRcloneConfig = null;
//		String smssRcloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + Utility.cleanLogString(databaseId) + " to pull app");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ Utility.cleanLogString(databaseId) + " is locked");
//		try {
//			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + databaseId);
//			smssRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + smssContainer);
//
//			// List the smss directory to get the alias + app id
//			List<String> results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig + ":" +DB_CONTAINER_PREFIX + smssContainer);
//			String smss = null;
//			for (String result : results) {
//				if (result.endsWith(".smss")) {
//					smss = result;
//					break;
//				}
//			}
//			if (smss == null) {
//				try {
//					fixLegacyDbStructure(databaseId);
//				} catch(IOException | InterruptedException e) {
//					classLogger.info(Constants.STACKTRACE, e);
//					throw new IOException("Failed to pull app for appid=" + databaseId);
//				}
//				
//				// try again
//				results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig+":"+DB_CONTAINER_PREFIX + smssContainer);
//				for (String result : results) {
//					if (result.endsWith(".smss")) {
//						smss = result;
//						break;
//					}
//				}
//				
//				if (smss == null) {
//					throw new IOException("Failed to pull app for appid=" + databaseId);
//				} else {
//					// we just fixed the structure and this was pulled and synched up
//					// can just return from here
//					return;
//				}
//			}
//
//			// We need to pull the folder alias__appId and the file alias__appId.smss
//			String aliasAppId = smss.replaceAll(".smss", "");
//
//			// Close the database (if an existing app), so that we can pull without file locks
//			try {
//				if (databaseAlreadyLoaded) {
//					DIHelper.getInstance().removeEngineProperty(databaseId);
//					database.close();
//				}
//
//				// Make the app directory (if it doesn't already exist)
//				File appFolder = new File(Utility.normalizePath(dbFolder + FILE_SEPARATOR + aliasAppId));
//				appFolder.mkdir(); 
//
//				// Pull the contents of the app folder before the smss
//				classLogger.info("Pulling app from remote=" + Utility.cleanLogString(databaseId) + " to target=" + appFolder.getPath());
//				runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appRcloneConfig + ":" + DB_CONTAINER_PREFIX + databaseId, appFolder.getPath());
//				classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(databaseId) + " to target=" + appFolder.getPath());
//
//				// Now pull the smss
//				classLogger.info("Pulling smss from remote=" + Utility.cleanLogString(smssContainer) + " to target=" + dbFolder);
//
//				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
//				runRcloneTransferProcess(smssRcloneConfig, "rclone", "copy", smssRcloneConfig + ":" + DB_CONTAINER_PREFIX + smssContainer, dbFolder);
//				classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(smssContainer) + " to target=" + dbFolder);
//
//				// Catalog the db if it is new
//				if (!databaseAlreadyLoaded) {
//					SMSSWebWatcher.catalogEngine(smss, dbFolder);
//				}
//			} finally {
//				// Re-open the database (if an existing app)
//				if (databaseAlreadyLoaded) {
//					Utility.getDatabase(databaseId, false);
//				}
//			}
//		} finally {
//			try {
//				if (appRcloneConfig != null) {
//					deleteRcloneConfig(appRcloneConfig);
//				}
//				if (smssRcloneConfig != null) {
//					deleteRcloneConfig(smssRcloneConfig);
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("App "+ Utility.cleanLogString(databaseId) + " is unlocked");
//			}
//		}
//	}
//
//	@Override
//	public void pullDatabaseImageFolder() throws IOException, InterruptedException {
//		String rCloneConfig = null;
//		try {
//			rCloneConfig = createRcloneConfig(CentralCloudStorage.DB_IMAGES_BLOB);
//			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+":"+CentralCloudStorage.DB_IMAGES_BLOB);
//			if(results.isEmpty()) {
//				fixLegacyImageStructure();
//				return;
//			}
//			
//			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
//			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "databases";
//			File imageFolder = new File(imagesFolderPath);
//			imageFolder.mkdir();
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig + ":"+CentralCloudStorage.DB_IMAGES_BLOB, imagesFolderPath);
//		}finally {
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
//			}
//		}
//	}
//
//	@Override
//	public void pushDatabaseImageFolder() throws IOException, InterruptedException {
//		String appRcloneConfig = null;
//		try {
//			appRcloneConfig = createRcloneConfig(CentralCloudStorage.DB_IMAGES_BLOB);
//			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
//			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "databases";
//			File imageFolder = new File(imagesFolderPath);
//			imageFolder.mkdir();
//			runRcloneTransferProcess(appRcloneConfig, "rclone", "sync",imagesFolderPath,  appRcloneConfig + ":"+CentralCloudStorage.DB_IMAGES_BLOB);
//		}finally {
//			if (appRcloneConfig != null) {
//				deleteRcloneConfig(appRcloneConfig);
//			}
//		}
//	}
//
//	@Override
//	public void pullProjectImageFolder() throws IOException, InterruptedException {
//		String rCloneConfig = null;
//		try {
//			rCloneConfig = createRcloneConfig(CentralCloudStorage.PROJECT_IMAGES_BLOB);
//			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
//			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "projects";
//			File imageFolder = new File(imagesFolderPath);
//			imageFolder.mkdir();
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig + ":"+CentralCloudStorage.PROJECT_IMAGES_BLOB, imagesFolderPath);
//		} finally {
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
//			}
//		}
//	}
//
//	@Override
//	public void pushProjectImageFolder() throws IOException, InterruptedException {
//		String rCloneConfig = null;
//		try {
//			rCloneConfig = createRcloneConfig(CentralCloudStorage.PROJECT_IMAGES_BLOB);
//			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
//			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "projects";
//			File imageFolder = new File(imagesFolderPath);
//			imageFolder.mkdir();
//			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", imagesFolderPath, rCloneConfig + ":"+CentralCloudStorage.PROJECT_IMAGES_BLOB);
//		} finally {
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
//			}
//		}
//	}
//
//
//	//////////////////////////////////////////////////////////////////////////////////////////
//	//////////////////////////////////////// Update///////////////////////////////////////////
//
//	// TODO >>>timb: pixel to update app so that neel can add refresh button or something
//	// TODO >>>timb: still need to test this method
//	public void updateApp(String appId) throws IOException, InterruptedException {
//		if (Utility.getDatabase(appId, true) == null) {
//			throw new IllegalArgumentException("App needs to be defined in order to update...");
//		}
//		pullDatabase(appId, false);
//	}
//
//	//////////////////////////////////////////////////////////////////////////////////////////
//	//////////////////////////////////////// Delete //////////////////////////////////////////
//
//	// TODO >>>timb: test out delete functionality
//	@Override
//	public void deleteDatabase(String appId) throws IOException, InterruptedException {
//		String rcloneConfig = Utility.getRandomString(10);
//		try {
//			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "account", name, "key", key);
//			classLogger.debug("Deleting container=" + appId + ", " + appId + SMSS_POSTFIX);
//			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + DB_CONTAINER_PREFIX + appId);
//			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + DB_CONTAINER_PREFIX +appId + SMSS_POSTFIX);
//			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + DB_CONTAINER_PREFIX +appId);
//			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + DB_CONTAINER_PREFIX +appId + SMSS_POSTFIX);
//			classLogger.debug("Done deleting container=" + appId + ", " + appId + SMSS_POSTFIX);
//		} finally {
//			deleteRcloneConfig(rcloneConfig);
//		}
//	}
//	
//	//TODO: make this different compared to app
//	//TODO: make this different compared to app
//	//TODO: make this different compared to app
//	@Override
//	public void deleteProject(String projectId) throws IOException, InterruptedException {
//		String rcloneConfig = Utility.getRandomString(10);
//		try {
//			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "account", name, "key", key);
//			classLogger.debug("Deleting container=" + projectId + ", " + projectId + SMSS_POSTFIX);
//			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + PROJECT_CONTAINER_PREFIX + projectId);
//			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + PROJECT_CONTAINER_PREFIX + projectId + SMSS_POSTFIX);
//			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + PROJECT_CONTAINER_PREFIX + projectId);
//			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + PROJECT_CONTAINER_PREFIX + projectId + SMSS_POSTFIX);
//			classLogger.debug("Done deleting container=" + projectId + ", " + projectId + SMSS_POSTFIX);
//		} finally {
//			deleteRcloneConfig(rcloneConfig);
//		}
//	}
//
//	//////////////////////////////////////////////////////////////////////////////////////////
//	//////////////////////////////////////// Cleanup//////////////////////////////////////////	
//	
//	public List<String> listAllBlobContainers() throws IOException, InterruptedException {
//		String rcloneConfig = Utility.getRandomString(10);
//		List<String> allContainers = new ArrayList<>();
//		try {
//			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "account", name, "key", key);
//			List<String> results = runRcloneProcess(rcloneConfig, "rclone", "lsf", rcloneConfig + ":");
//			for (String result : results) {
//				allContainers.add(result);
//			}
//		} finally {
//			deleteRcloneConfig(rcloneConfig);
//		}
//		return allContainers;
//	}
//
//	@Override
//	public void deleteContainer(String containerId) throws IOException, InterruptedException {
//		String rcloneConfig = Utility.getRandomString(10);
//		try {
//			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "account", name, "key", key);
//			classLogger.debug("Deleting container=" + containerId);
//			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + containerId);
//			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + containerId);
//			classLogger.debug("Done deleting container=" + containerId);
//		} finally {
//			deleteRcloneConfig(rcloneConfig);
//		}
//	}
//	
//	///////////////////////////////////////////////////////////////////////////////////
//	
//	
//	/*
//	 * Storage
//	 */
//	
//	@Override
//	public void pushStorage(String storageId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void pullStorage(String storageId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void pullStorage(String storageId, boolean storageAlreadyLoaded) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void pushStorageSmss(String storageId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void pullStorageSmss(String storageId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//	
//	@Override
//	public void deleteStorage(String storageId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//	
//	@Override
//	public void pullStorageImageFolder() throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void pushStorageImageFolder() throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	///////////////////////////////////////////////////////////////////////////////////
//	
//	
//	/*
//	 * Model
//	 */
//
//	@Override
//	public void pushModel(String modelId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void pullModel(String modelId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void pullModel(String modelId, boolean modelAlreadyLoaded) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void pushModelSmss(String modelId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void pullModelSmss(String modelId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//	
//	@Override
//	public void deleteModel(String modelId) throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//	
//	@Override
//	public void pullModelImageFolder() throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void pushModelImageFolder() throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		
//	}
//	
//	///////////////////////////////////////////////////////////////////////////////////
//
//	//////////////////////////////////////////////////////////////////////////////////////////
//	///////////////////////////////// Static Util Methods ////////////////////////////////////
//
//	/**
//	 * Create the SAS for the container
//	 * REMEMBER TO PASS IN THE PREFIX project-, db-, user- through the constants DB_CONTAINER_PREFIX, PROJECT_CONTAINER_PREFIX, USER_CONTAINER_PREFIX
//	 * @param container
//	 * @return
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	private String createRcloneConfig(String container) throws IOException, InterruptedException {
//		if(!(container.startsWith(DB_CONTAINER_PREFIX) || container.startsWith(PROJECT_CONTAINER_PREFIX) || container.startsWith(USER_CONTAINER_PREFIX)
//				|| container.startsWith(CentralCloudStorage.DB_IMAGES_BLOB) || container.startsWith(CentralCloudStorage.PROJECT_IMAGES_BLOB))) {
//			classLogger.warn("Requesting SAS but haven't defined the container prefix - likely an error");
//			classLogger.warn("Requesting SAS but haven't defined the container prefix - likely an error");
//			classLogger.warn("Requesting SAS but haven't defined the container prefix - likely an error");
//		}
//		classLogger.debug("Generating SAS for container=" + container);
//		String sasUrl = getSAS(container);
//		String rcloneConfig = Utility.getRandomString(10);
//		runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "sas_url", sasUrl);
//		return rcloneConfig;
//	}
//	
//	@Override
//	public String createRcloneConfig() throws IOException, InterruptedException {
//		// not implementing - only create config at the container level
//		return null;
//	}
//
//	public static void main(String[] args) {
//		String[] sarr = new String[] {"sakdf.smss","/opt/semosshome/db/kunal__3241231242", "/opt/semosshome/db/kunal__3241231242.smss", "/opt/semosshome/db/kunal__3241231242//../sakdf"};
//		String s = "/opt/semosshome/db/kunal__3241231242";
//		for(int i = 0; i<sarr.length; i++) {
//			//System.out.println(sarr[i] + " -> " + Utility.normalizePath(sarr[i]));
//		}
//		System.out.println(Utility.normalizePath("/opt/semosshome/db/../../../../root/myvirus.virus"));
//		//System.out.println("/opt/semosshome/db" + FILE_SEPARATOR + Utility.normalizePath("../root/myvirus.virus"));
//
//
//
//
//	}
//
//	/*
//
//	private static void deleteRcloneConfig(String rcloneConfig) throws IOException, InterruptedException {
//		String configPath = getConfigPath(rcloneConfig);
//		try {
//			runRcloneProcess(rcloneConfig, "rclone", "config", "delete", rcloneConfig);
//		} finally {
//			new File(configPath).delete();
//		}
//	}
//
//	private static List<String> runRcloneProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
//		String configPath = getConfigPath(rcloneConfig);
//		List<String> commandList = new ArrayList<>();
//		commandList.addAll(Arrays.asList(command));
//		commandList.add("--config");
//		commandList.add(configPath);
//		String[] newCommand = commandList.toArray(new String[] {});
//		return runAnyProcess(newCommand);	
//	}
//
//	private static String getConfigPath(String rcloneConfig) {
//		return rcloneConfigFolder + FILE_SEPARATOR + rcloneConfig + ".conf";
//	}
//
//	private static List<String> runAnyProcess(String... command) throws IOException, InterruptedException {
//		Process p = null;
//		try {
//			ProcessBuilder pb = new ProcessBuilder(command);
//			pb.directory(new File(System.getProperty("user.home")));
//		    pb.redirectOutput(Redirect.PIPE);
//		    pb.redirectError(Redirect.PIPE);
//			p = pb.start();
//			p.waitFor();
//			List<String> results = streamOutput(p.getInputStream());
//			streamError(p.getErrorStream());
//			return results;
//		} finally {
//			if (p != null) {
//				p.destroyForcibly();
//			}
//		}
//	}
//
//	private static List<String> streamOutput(InputStream stream) throws IOException {
//		return stream(stream, false);
//	}
//
//	private static List<String> streamError(InputStream stream) throws IOException {
//		return stream(stream, true);
//	}
//
//	private static List<String> stream(InputStream stream, boolean error) throws IOException {
//		try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
//			List<String> lines = reader.lines().collect(Collectors.toList());
//			for(String line : lines) {
//				if (error) {
//					System.err.println(line);
//				} else {
//					classLogger.debug(line);
//				}
//			}
//			return lines;
//		}
//	}
//	 */
//
//
//
//	/*
//
//	public static void main(String[] args) throws IOException, InterruptedException {
//
//
//		try {
//
//			List<String> appWithImages= new ArrayList();
//
//			// List the smss directory to get the alias + app id
//			String[] cmdArray = {"rclone", "lsf", "kunalp:"};
//			List<String> containers = runAnyProcess("rclone", "lsf", "kunalp:");
//			for (String container: containers){
//				List<String> images = runAnyProcess("rclone", "ls" , "kunalp:"+container+"/version/image.png");
//				if(!(images.isEmpty()) && images != null){
//					appWithImages.add(container);
//				}
//			}
//			for(String s: appWithImages){
//
//				classLogger.debug(s);
//				List<String> copied = runAnyProcess("rclone", "copy" , "kunalp:"+s+"version/image.png", "kunalp:aaa-imagecontainer/");
//				List<String> moved = runAnyProcess("rclone", "moveto" , "kunalp:aaa-imagecontainer/image.png", "kunalp:aaa-imagecontainer/"+s.substring(0, s.length()-1)+".png");
//
//				//List<String> copied = runAnyProcess("rclone", "copy" , "kunalp:"+s+"version/image.png", "/Users/semoss/Documents/workspace/Semoss/images/download/");
//				//List<String> copied = runAnyProcess("rclone", "copy" , "kunalp:"+s+"version/image.png", "/Users/semoss/Documents/workspace/Semoss/images/download/"+s.substring(0, s.length()-1)+".png");
//
//			}
//
////			List<String> containers = runAnyProcess("rclone", "lsf", "kunalp:");
////
////			for(String appid : results1){
////				if( appid.contains("smss")){
////					continue;
////				}
////				
////				
////				
////				List<String> results2= runAnyProcess("rclone", "lsf", "kunalp:"+appid);
////				for(String content : results2){
////					if( content.contains("version")){
////						
////						classLogger.debug(appid + " has a version folder");
////						appWithVersion.add(appid);
////					}
////				}
////			}
//
//		} catch(Exception e){
//			classLogger.error(STACKTRACE, e);
//		}
//
//		//DIHelper.getInstance().loadCoreProp("C:\\Users\\tbanach\\Documents\\Workspace\\Semoss\\RDF_Map.prop");
//		//		String appId = "a295698a-1f1c-4639-aba6-74b226cd2dfc";
//		//		classLogger.debug(AZClient.getInstance().getSAS("timb"));
//		//		AZClient.getInstance().deleteApp("1bab355d-a2ea-4fde-9d2c-088287d46978");
//		//		AZClient.getInstance().pushApp(appId);
//		//		AZClient.getInstance().pullApp(appId);
//		//		List<String> containers = AZClient.getInstance().listAllBlobContainers();
//		//		for(String container : containers) {
//		//			classLogger.debug(container);
//		//		}
//	}
//	 */
//	
//}
