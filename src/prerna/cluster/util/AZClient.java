package prerna.cluster.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.google.common.io.Files;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.LegacyToProjectRestructurerHelper;
import prerna.engine.impl.ProjectHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.ProjectSyncUtility;
import prerna.util.ProjectWatcher;
import prerna.util.SMSSWebWatcher;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class AZClient extends CloudClient {
	
	private static final Logger logger = LogManager.getLogger(AZClient.class);

	// this is a singleton

	// does some basic ops
	// get the SAS URL for a given container - boolean create or not
	// Delete the container

	protected static final String PROVIDER = "azureblob";
	protected static final String SMSS_POSTFIX = "-smss";

	public static final String AZ_CONN_STRING = "AZ_CONN_STRING";
	public static final String AZ_NAME = "AZ_NAME";
	public static final String AZ_KEY = "AZ_KEY";
	public static final String SAS_URL = "SAS_URL";
	public static final String AZ_URI = "AZ_URI";
	public static final String STORAGE = "STORAGE"; // says if this is local / cluster
	public static final String KEY_HOME = "KEY_HOME"; // this is where the various keys are cycled

	public String azKeyRoot = "/khome";

	static AZClient client = null;
	static String rcloneConfigFolder = null;

	CloudBlobClient serviceClient = null;
	String connectionString = null;
	String name = null;
	String key = null;
	String blobURI = null;
	String sasURL = null;
	
	String dbFolder = null;
	String projectFolder = null;
	String userFolder = null;

	static String DB_CONTAINER_PREFIX = "db-";
	static String PROJECT_CONTAINER_PREFIX = "project-";
	static String USER_CONTAINER_PREFIX = "user-";
	
	// create an instance
	// Also needs to be synchronized so multiple calls don't try to init at the same time
	public static synchronized AZClient getInstance() {
		if(client == null) {
			client = new AZClient();
			client.init();
		}
		return client;
	}

	// initialize
	public void init()
	{
		rcloneConfigFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + "rcloneConfig";		
		new File(rcloneConfigFolder).mkdir();

		// if the zookeeper is defined.. find from zookeeper what the key is
		// and register for the key change
		// if not.. the storage key is sitting some place pick it up and get it
		String storage = DIHelper.getInstance().getProperty(STORAGE);
		
		this.dbFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.DB_FOLDER;
		this.projectFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.PROJECT_FOLDER;
		this.userFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.USER_FOLDER;

		Map <String, String> env = System.getenv();
		if(env.containsKey(KEY_HOME)) {
			this.azKeyRoot = env.get(KEY_HOME);
		}

		if(env.containsKey(KEY_HOME.toUpperCase())) {
			this.azKeyRoot = env.get(KEY_HOME.toUpperCase());
		}

		if(storage == null || storage.equalsIgnoreCase("LOCAL")) {
			// dont bother with anything
			// TODO >>>timb: these should all be centralized somewhere so we know what is needed for cluster
			this.connectionString = DIHelper.getInstance().getProperty(AZ_CONN_STRING);
			this.name = DIHelper.getInstance().getProperty(AZ_NAME);
			this.key = DIHelper.getInstance().getProperty(AZ_KEY);
			this.blobURI = DIHelper.getInstance().getProperty(AZ_URI);
			this.sasURL = DIHelper.getInstance().getProperty(SAS_URL);
		} else {
			// need the zk piece here
			ZKClient client = ZKClient.getInstance();
			this.connectionString = client.getNodeData(azKeyRoot, client.zk);

			// if SAS_URL it should starts with SAS_URL=			
			if(connectionString.startsWith("SAS_URL=")) {
				this.sasURL = connectionString.replace("SAS_URL=", "");
			}

			AZStorageListener azList = new AZStorageListener();
			client.watchEvent(azKeyRoot, EventType.NodeDataChanged, azList);
		}

		createServiceClient();
	}

	public void createServiceClient() {
		try {
			if(sasURL != null) {
				this.serviceClient = new CloudBlobClient(new StorageUri(new URI(blobURI)),
						new StorageCredentialsSharedAccessSignature(connectionString));
			} else {
				CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
				this.serviceClient = account.createCloudBlobClient();
			}
		} catch (URISyntaxException use) {
			logger.error(Constants.STACKTRACE, use);
		} catch (InvalidKeyException ike) {
			logger.error(Constants.STACKTRACE, ike);
		}
	}

	// get SAS URL for a container
	public String getSAS(String containerName) {
		String retString = null;
		try {
			//createServiceClient();
			CloudBlobContainer container = serviceClient.getContainerReference(containerName);
			container.createIfNotExists();
			retString = container.getUri() + "?" + container.generateSharedAccessSignature(getSASConstraints(), null); 

		} catch (URISyntaxException use) {
			logger.error(Constants.STACKTRACE, use);
		} catch (StorageException se) {
			logger.error(Constants.STACKTRACE, se);
		} catch (InvalidKeyException ike) {
			logger.error(Constants.STACKTRACE, ike);
		}

		return retString;
	}

	// swaps the key
	public void swapKey(String key) {
		// if sasURL is null then it is account
		if(sasURL != null) {
			sasURL = key;
		} else {
			connectionString = key;
		}
		createServiceClient();
	}

	public void quarantineContainer(String containerName)
	{
		// take this out in terms of listing

	}

	public SharedAccessBlobPolicy getSASConstraints() {
		SharedAccessBlobPolicy sasConstraints = null;
		sasConstraints = new SharedAccessBlobPolicy();

		// get the current time + 24 hours or some
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MINUTE, +5);
		Date date = calendar.getTime();

		sasConstraints.setSharedAccessExpiryTime(date);

		EnumSet <SharedAccessBlobPermissions> permSet = EnumSet.noneOf(SharedAccessBlobPermissions.class);
		// I need to read the database to find if this guy is allowed etc. but for now
		permSet.add(SharedAccessBlobPermissions.LIST);
		permSet.add(SharedAccessBlobPermissions.WRITE);
		permSet.add(SharedAccessBlobPermissions.CREATE);
		permSet.add(SharedAccessBlobPermissions.READ);
		permSet.add(SharedAccessBlobPermissions.DELETE);
		permSet.add(SharedAccessBlobPermissions.ADD);

		sasConstraints.setPermissions(permSet);
		return sasConstraints;
	}
	
	@Override
	public void pullOwl(String appId) throws IOException, InterruptedException{
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String appRcloneConfig = null;
		File owlFile = null;
		String alias = SecurityQueryUtils.getDatabaseAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to pull owl");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");

		try {
			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + appId);
			//close the owl
			engine.getBaseDataEngine().closeDB();
			owlFile = new File(engine.getProperty(Constants.OWL));

			logger.info("Pulling owl and postions.json for " + appFolder + " from remote=" + appId);

			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
			//sync will delete files that are in the destination if they aren't being synced
			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", appRcloneConfig + ":"+ DB_CONTAINER_PREFIX +appId+"/"+ owlFile.getName(), appFolder);
			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", appRcloneConfig + ":"+ DB_CONTAINER_PREFIX + appId+"/"+ AbstractEngine.OWL_POSITION_FILENAME, appFolder);

		}  finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
				//open the owl
				if(owlFile!=null && owlFile.exists()) {
					engine.setOWL(owlFile.getAbsolutePath());
				} else {
					throw new IllegalArgumentException("Pull failed. OWL for engine " + appId + " was not found");
				}
			} finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("App "+ appId + " is unlocked");
			}
		}
	}

	@Override
	public void pushOwl(String appId) throws IOException, InterruptedException{
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String appRcloneConfig = null;
		File owlFile = null;
		String alias = SecurityQueryUtils.getDatabaseAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to push owl");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + appId);
			//close the owl
			engine.getBaseDataEngine().closeDB();
			owlFile = new File(engine.getProperty(Constants.OWL));

			logger.info("Pushing owl and postions.json for " + appFolder + " from remote=" + appId);


			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
			//sync will delete files that are in the destination if they aren't being synced

			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", appFolder+"/" + owlFile.getName(), appRcloneConfig + ":");			 
			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", appFolder+"/" + AbstractEngine.OWL_POSITION_FILENAME, appRcloneConfig + ":");			 


		}  finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
				//open the owl
				if(owlFile!=null && owlFile.exists()) {
					engine.setOWL(owlFile.getAbsolutePath());
				} else {
					throw new IllegalArgumentException("Push failed. OWL for engine " + appId + " was not found");
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("App "+ appId + " is unlocked");
			}
		}
	}


	@Override
	public void pullInsightsDB(String projectId) throws IOException, InterruptedException{
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		
		String appRcloneConfig = null;
		String alias = project.getProjectName();
		String aliasProjectId = SmssUtilities.getUniqueName(alias, projectId);
		String thisProjectFolder = this.projectFolder + FILE_SEPARATOR + aliasProjectId;

		// synchronize on the app id
		logger.info("Applying lock for " + projectId + " to pull insights db");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			appRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
			logger.info("Pulling insights database for " + alias + " from remote=" + projectId);
			String insightDB = getInsightDB(project, thisProjectFolder);

			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
			//sync will delete files that are in the destination if they aren't being synced
			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", appRcloneConfig  + ":" + PROJECT_CONTAINER_PREFIX + projectId + "/" + insightDB, thisProjectFolder);

		}  finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
				//open the insight db
				String insightDbLoc = SmssUtilities.getInsightsRdbmsFile(project.getProp()).getAbsolutePath();
				if(insightDbLoc != null) {
					project.setInsightDatabase( ProjectHelper.loadInsightsEngine(project.getProp(), LogManager.getLogger(AbstractEngine.class)));
				} else {
					throw new IllegalArgumentException("Insight database was not able to be found");
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("Project "+ projectId + " is unlocked");
			}
		}
	}

	@Override
	public void pushInsightDB(String projectId) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		String appRcloneConfig = null;
		String alias = project.getProjectName();
		String aliasProjectId = SmssUtilities.getUniqueName(alias, projectId);
		String thisProjectFolder = this.projectFolder + FILE_SEPARATOR + aliasProjectId;

		// synchronize on the app id
		logger.info("Applying lock for " + projectId + " to push insights db");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			appRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
			project.getInsightDatabase().closeDB();
			logger.info("Pushing insights database for " + alias + " from remote=" + projectId);
			String insightDB = getInsightDB(project, thisProjectFolder);

			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
			//sync will delete files that are in the destination if they aren't being synced

			runRcloneTransferProcess(appRcloneConfig, "rclone", "copy", thisProjectFolder+"/"+ insightDB, appRcloneConfig + ":" + PROJECT_CONTAINER_PREFIX + projectId);			 
		}
		finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
				//open the insight db
				String insightDbLoc = SmssUtilities.getInsightsRdbmsFile(project.getProp()).getAbsolutePath();
				if(insightDbLoc != null) {
					project.setInsightDatabase(ProjectHelper.loadInsightsEngine(project.getProp(), LogManager.getLogger(AbstractEngine.class)));
				} else {
					throw new IllegalArgumentException("Insight database was not able to be found");
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("Project "+ projectId + " is unlocked");
			}

		}
	}




	@Override
	public void pushDB(String appId, RdbmsTypeEnum e) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String appRcloneConfig = null;
		String alias = SecurityQueryUtils.getDatabaseAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to push db file");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + appId);
			DIHelper.getInstance().removeDbProperty(appId);
			engine.closeDB();

			logger.info("Pushing database for " + alias + " from remote=" + appId);
			if(e == RdbmsTypeEnum.SQLITE){
				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
				
				for(String sqliteFile : sqliteFileNames){
					runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appFolder + "/" + sqliteFile, appRcloneConfig + ":"+ DB_CONTAINER_PREFIX+ appId);
				}
			} else if(e == RdbmsTypeEnum.H2_DB){
				runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appFolder + "/database.mv.db", appRcloneConfig + ":"+DB_CONTAINER_PREFIX+appId);
			} else{
				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
			}

			//open the engine again
			Utility.getEngine(appId, false);
		} finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("App "+ appId + " is unlocked");
			}
		}		
	}

	@Override
	public void pullDB(String appId, RdbmsTypeEnum e) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String appRcloneConfig = null;
		String alias = SecurityQueryUtils.getDatabaseAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to pull db file");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + appId);
			engine.closeDB();
			logger.info("Pulling database for " + alias + " from remote=" + appId);
			if(e == RdbmsTypeEnum.SQLITE){
				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
				//TODO kunal: below calls will break
				for(String sqliteFile : sqliteFileNames){			
					runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appRcloneConfig + ":"+DB_CONTAINER_PREFIX+appId+"/"+sqliteFile, appFolder);
				}
			} else if(e == RdbmsTypeEnum.H2_DB){
				runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appRcloneConfig + ":" + DB_CONTAINER_PREFIX+appId+"/database.mv.db", appFolder);
			} else{
				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
			}
		} finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("App "+ appId + " is unlocked");
			}
		}
	}


	@Override
	public void pullEngineFolder(String appId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String appRcloneConfig = null;
		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		//String aliasAppId = alias + "__" + appId;
		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		// adding a lock for now, but there may be times we don't need one and other times we do
		// reaching h2 db from version folder vs static assets in asset app
		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to pull folder " + remoteRelativePath);
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + appId);
			logger.info("Pulling folder for " + remoteRelativePath + " from remote=" + appId);

			runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appRcloneConfig + ":"+ DB_CONTAINER_PREFIX +appId+  "/" + remoteRelativePath, absolutePath);
		} finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("App "+ appId + " is unlocked");

			}
		}
	}

	@Override
	public void pushEngineFolder(String appId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String appRcloneConfig = null;
		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		//String aliasAppId = alias + "__" + appId;
		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		File absoluteFolder = new File(absolutePath);
		if(absoluteFolder.isDirectory()) {
			//this is adding a hidden file into every sub folder to make sure there is no empty directory
			ClusterUtil.validateFolder(absoluteFolder.getAbsolutePath());
		}
		// adding a lock for now, but there may be times we don't need one and other times we do
		// reaching h2 db from version folder vs static assets in asset app
		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to push folder " + remoteRelativePath);
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");

		try {
			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + appId);
			logger.info("Pushing folder for " + remoteRelativePath + " to remote=" + appId);

			runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", absolutePath, appRcloneConfig + ":"+DB_CONTAINER_PREFIX+appId+  "/" + remoteRelativePath);
		} finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("App "+ appId + " is unlocked");
			}
		}
	}

	@Override
	public void pullProjectFolder(String projectId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String appRcloneConfig = null;
		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		//String aliasAppId = alias + "__" + appId;
		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		// adding a lock for now, but there may be times we don't need one and other times we do
		// reaching h2 db from version folder vs static assets in asset app
		// synchronize on the app id
		logger.info("Applying lock for " + projectId + " to pull folder " + remoteRelativePath);
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			appRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
			logger.info("Pulling folder for " + remoteRelativePath + " from remote=" + projectId);

			runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appRcloneConfig + ":"+PROJECT_CONTAINER_PREFIX+projectId+  "/" + remoteRelativePath, absolutePath);
		} finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("Project "+ projectId + " is unlocked");

			}
		}
	}
	
	@Override
	public void pushProjectFolder(String projectId, String absolutePath, String remoteRelativePath)
			throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		String appRcloneConfig = null;
		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		//String aliasAppId = alias + "__" + appId;
		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		File absoluteFolder = new File(absolutePath);
		if(absoluteFolder.isDirectory()) {
			//this is adding a hidden file into every sub folder to make sure there is no empty directory
			ClusterUtil.validateFolder(absoluteFolder.getAbsolutePath());
		}
		// adding a lock for now, but there may be times we don't need one and other times we do
		// reaching h2 db from version folder vs static assets in asset app
		// synchronize on the app id
		logger.info("Applying lock for " + projectId + " to push folder " + remoteRelativePath);
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");

		try {
			appRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
			logger.info("Pushing folder for " + remoteRelativePath + " to remote=" + projectId);

			runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", absolutePath, appRcloneConfig + ":" + PROJECT_CONTAINER_PREFIX+projectId+  "/" + remoteRelativePath);
		} finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("Project "+ projectId + " is unlocked");
			}
		}
	}
	
	@Override
	@Deprecated
	public void fixLegacyDbStructure(String appId) throws IOException, InterruptedException {
		String smssContainer = appId + SMSS_POSTFIX;
		
		String smssCloneConfig = null;
		String rCloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to pull app");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			smssCloneConfig = createRcloneConfig(smssContainer);
			rCloneConfig = createRcloneConfig(appId);
			// WE HAVE TO PULL FROM THE OLD LOCATION WITHOUT THERE BEING A /db/ IN THE PATH
			List<String> results = runRcloneProcess(smssCloneConfig, "rclone", "lsf", smssCloneConfig+":");
			String smss = null;
			for (String result : results) {
				if (result.endsWith(".smss")) {
					smss = result;
					break;
				}
			}
			if (smss == null) {
				// IF STILL NOT FOUND... CANT HELP YOU
				throw new IOException("Failed to pull app for appid=" + appId);
			}
			
			String aliasAppId = smss.replaceAll(".smss", "");
			File appFolder = new File(dbFolder + FILE_SEPARATOR + aliasAppId);
			appFolder.mkdir();
			// Pull the contents of the app folder before the smss
			logger.info("Pulling app from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+":", appFolder.getPath());
			logger.debug("Done pulling from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));

			// Now pull the smss
			logger.info("Pulling smss from remote=" + smssContainer + " to target=" + dbFolder);
			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
			runRcloneTransferProcess(smssCloneConfig, "rclone", "copy", smssCloneConfig+":", dbFolder);
			logger.debug("Done pulling from remote=" + smssContainer + " to target=" + dbFolder);

			LegacyToProjectRestructurerHelper fixer = new LegacyToProjectRestructurerHelper();
			fixer.init();
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String dbDir = baseFolder + LegacyToProjectRestructurerHelper.ENGINE_DIRECTORY;
			String projectDir = baseFolder + LegacyToProjectRestructurerHelper.PROJECT_DIRECTORY;
			fixer.copyDataToNewFolderStructure(aliasAppId, projectDir, dbDir);
			
			Utility.loadEngine(dbDir + "/" + smss, Utility.loadProperties(dbDir + "/" + smss));
			Utility.loadProject(projectDir + "/" + smss, Utility.loadProperties(projectDir + "/" + smss));

			// now push the new db and app into the right locations
			pushApp(appId);
			pushProject(appId);
		} finally {
			try {
				if (smssCloneConfig != null) {
					deleteRcloneConfig(smssCloneConfig);
				}
				if (rCloneConfig != null) {
					deleteRcloneConfig(rCloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("App "+ appId + " is unlocked");
			}
		}
	}
	
	@Override
	@Deprecated
	public void fixLegacyUserAssetStructure(String appId, boolean isAsset) throws IOException, InterruptedException {
		String smssContainer = appId + SMSS_POSTFIX;
		String smssCloneConfig = null;
		String rCloneConfig = null;

		try {
			smssCloneConfig = createRcloneConfig(smssContainer);
			rCloneConfig = createRcloneConfig(appId);			
			// WE HAVE TO PULL FROM THE OLD LOCATION WITHOUT THERE BEING A /db/ IN THE PATH
			List<String> results = runRcloneProcess(smssCloneConfig, "rclone", "lsf", smssCloneConfig+":");
			String smss = null;
			for (String result : results) {
				if (result.endsWith(".smss")) {
					smss = result;
					break;
				}
			}
			if (smss == null) {
				// IF STILL NOT FOUND... CANT HELP YOU
				throw new IOException("Failed to pull app for appid=" + appId);
			}

			String aliasAppId = smss.replaceAll(".smss", "");
			File appFolder = new File(dbFolder + FILE_SEPARATOR + aliasAppId);
			appFolder.mkdir();
			// Pull the contents of the app folder before the smss
			logger.info("Pulling app from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+":", appFolder.getPath());
			logger.debug("Done pulling from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));

			// Now pull the smss
			logger.info("Pulling smss from remote=" + smssContainer + " to target=" + dbFolder);
			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
			runRcloneTransferProcess(smssCloneConfig, "rclone", "copy", smssCloneConfig+":", dbFolder);
			logger.debug("Done pulling from remote=" + smssContainer + " to target=" + dbFolder);

			LegacyToProjectRestructurerHelper fixer = new LegacyToProjectRestructurerHelper();
			fixer.init();
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String dbDir = baseFolder + LegacyToProjectRestructurerHelper.ENGINE_DIRECTORY;
			String userDir = baseFolder + LegacyToProjectRestructurerHelper.USER_DIRECTORY;
			fixer.userCopyDataToNewFolderStructure(aliasAppId, userDir, dbDir, WorkspaceAssetUtils.isAssetProject(appId));
			
			// only load the project
			Utility.loadProject(userDir + "/" + smss, Utility.loadProperties(userDir + "/" + smss));

			// now push the project into the right locations
			pushUserAssetOrWorkspace(appId, isAsset);
		} finally {
			if (smssCloneConfig != null) {
				deleteRcloneConfig(smssCloneConfig);
			}
		}
	}
	
	@Override
	@Deprecated
	public void fixLegacyImageStructure() throws IOException, InterruptedException {
		String rCloneConfig = null;
		String fixedCloneConfig = null;
		try {
			rCloneConfig = createRcloneConfig("semoss-imagecontainer");
			fixedCloneConfig = createRcloneConfig(ClusterUtil.DB_IMAGES_BLOB);
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "databases";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			// first pull
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":", imagesFolderPath);
			// now push into the correct folder
			runRcloneProcess(fixedCloneConfig, "rclone", "sync", imagesFolderPath, fixedCloneConfig+":");
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
			if (fixedCloneConfig != null) {
				deleteRcloneConfig(fixedCloneConfig);
			}
		}
	}
	
	@Override
	public void pushProject(String projectId) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("App not found...");
		}

		// We need to push the folder alias__appId and the file alias__appId.smss
		String alias = project.getProjectName();
		if(alias == null) {
			alias = SecurityProjectUtils.getProjectAliasForId(projectId);
		}

		String aliasProjectId = alias + "__" + projectId;
		String thisProjectFolder = projectFolder + FILE_SEPARATOR + aliasProjectId;
		String smss = aliasProjectId + ".smss";
		String smssFile = projectFolder + FILE_SEPARATOR + smss;

		// Start with the sas token
		String projectRcloneConfig = null;
		String smssRCloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + projectId + " to push app");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			projectRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
			String smssContainer = projectId + SMSS_POSTFIX;
			smssRCloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + smssContainer);

			// Some temp files needed for the transfer
			File temp = null;
			File copy = null;

			// Close the database, so that we can push without file locks (also ensures that the db doesn't change mid push)
			try {
				DIHelper.getInstance().removeProjectProperty(projectId);
				project.closeProject();

				// Push the app folder
				logger.info("Pushing app from source=" + thisProjectFolder + " to remote=" + projectId);
				runRcloneTransferProcess(projectRcloneConfig, "rclone", "sync", thisProjectFolder, projectRcloneConfig + ":");
				logger.debug("Done pushing from source=" + thisProjectFolder + " to remote=" + projectId);

				// Move the smss to an empty temp directory (otherwise will push all items in the db folder)
				String tempFolder = Utility.getRandomString(10);
				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
				temp.mkdir();
				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
				Files.copy(new File(Utility.normalizePath(smssFile)), copy);

				// Push the smss
				logger.info("Pushing smss from source=" + smssFile + " to remote=" + smssContainer);
				runRcloneTransferProcess(smssRCloneConfig, "rclone", "sync", temp.getPath(), smssRCloneConfig + ":");
				logger.debug("Done pushing from source=" + smssFile + " to remote=" + smssContainer);
			} finally {
				if (copy != null) {
					copy.delete();
				}
				if (temp != null) {
					temp.delete();
				}

				// Re-open the database
				Utility.getProject(projectId, false);
			}
		} finally {
			try {
				if (projectRcloneConfig != null) {
					deleteRcloneConfig(projectRcloneConfig);
				}
				if (smssRCloneConfig != null) {
					deleteRcloneConfig(smssRCloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("Project "+ projectId + " is unlocked");
			}
		}
	}

	@Override
	public void pullProject(String projectId) throws IOException, InterruptedException {
		pullProject(projectId, false);
	}

	@Override
	protected void pullProject(String projectId, boolean projectAlreadyLoaded) throws IOException, InterruptedException {
		IProject project = null;
		if (projectAlreadyLoaded) {
			project = Utility.getProject(projectId, false);
			if (project == null) {
				throw new IllegalArgumentException("Project not found...");
			}
		}

		String smssContainer = projectId + SMSS_POSTFIX;

		// Start with the sas token
		String projectRcloneConfig = null;
		String smssRcloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + projectId + " to pull project");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			projectRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + projectId);
			smssRcloneConfig = createRcloneConfig(PROJECT_CONTAINER_PREFIX + smssContainer);

			// List the smss directory to get the alias + app id
			List<String> results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig + ":");
			String smss = null;
			for (String result : results) {
				if (result.endsWith(".smss")) {
					smss = result;
					break;
				}
			}
			if (smss == null) {
				try {
					fixLegacyDbStructure(projectId);
				} catch(IOException | InterruptedException e) {
					throw new IOException("Failed to pull app for projectId=" + projectId);
				}
				
				// try again
				results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig+":");
				for (String result : results) {
					if (result.endsWith(".smss")) {
						smss = result;
						break;
					}
				}
				
				if (smss == null) {
					throw new IOException("Failed to pull app for projectId=" + projectId);
				} else {
					// we just fixed the structure and this was pulled and synched up
					// can just return from here
					return;
				}
			}

			// We need to pull the folder alias__appId and the file alias__appId.smss
			String aliasProjectId = smss.replaceAll(".smss", "");

			// Close the database (if an existing app), so that we can pull without file locks
			try {
				if (projectAlreadyLoaded) {
					DIHelper.getInstance().removeProjectProperty(projectId);
					project.closeProject();
				}

				// Make the app directory (if it doesn't already exist)
				File thisProjectFolder = new File(Utility.normalizePath(projectFolder + FILE_SEPARATOR + aliasProjectId));
				thisProjectFolder.mkdir(); 

				// Pull the contents of the project folder before the smss
				logger.info("Pulling app from remote=" + projectId + " to target=" + thisProjectFolder.getPath());
				runRcloneTransferProcess(projectRcloneConfig, "rclone", "sync", projectRcloneConfig + ":", thisProjectFolder.getPath());
				logger.debug("Done pulling from remote=" + projectId + " to target=" + thisProjectFolder.getPath());

				// Now pull the smss
				logger.info("Pulling smss from remote=" + smssContainer + " to target=" + projectFolder);

				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE PROJECT FOLDER
				runRcloneTransferProcess(smssRcloneConfig, "rclone", "copy", smssRcloneConfig + ":", projectFolder);
				logger.debug("Done pulling from remote=" + smssContainer + " to target=" + projectFolder);

				// Catalog the project if it is new
				if (!projectAlreadyLoaded) {
					ProjectWatcher.catalogProject(smss, projectFolder);
				}
			} finally {
				// Re-open the database (if an existing app)
				if (projectAlreadyLoaded) {
					Utility.getProject(projectId, false);
				}
			}
		} finally {
			try {
				if (projectRcloneConfig != null) {
					deleteRcloneConfig(projectRcloneConfig);
				}
				if (smssRcloneConfig != null) {
					deleteRcloneConfig(smssRcloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("Project "+ projectId + " is unlocked");
			}
		}
	}
	
	@Override
	public void pullUserAssetOrWorkspace(String projectId, boolean isAsset, boolean projectAlreadyLoaded) throws IOException, InterruptedException {
		IProject project = null;
		if (projectAlreadyLoaded) {
			project = Utility.getProject(projectId, false);
			if (project == null) {
				throw new IllegalArgumentException("Project not found...");
			}
		}

		String smssContainer = projectId + SMSS_POSTFIX;

		// Start with the sas token
		String userRcloneConfig = null;
		String smssRcloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + projectId + " to pull project");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			userRcloneConfig = createRcloneConfig(USER_CONTAINER_PREFIX + projectId);
			smssRcloneConfig = createRcloneConfig(USER_CONTAINER_PREFIX + smssContainer);

			// List the smss directory to get the alias + app id
			List<String> results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig + ":");
			String smss = null;
			for (String result : results) {
				if (result.endsWith(".smss")) {
					smss = result;
					break;
				}
			}
			if (smss == null) {
				// assume this is for pulling an unprocessed project from an app
				try {
					fixLegacyUserAssetStructure(projectId, isAsset);
				} catch(IOException | InterruptedException e) {
					e.printStackTrace();
					throw new IOException("Failed to pull project for projectId=" + projectId);
				}
				
				// try again
				results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig+":");
				for (String result : results) {
					if (result.endsWith(".smss")) {
						smss = result;
						break;
					}
				}
				
				if (smss == null) {
					throw new IOException("Failed to pull project for projectId=" + projectId);
				} else {
					// we just fixed the structure and this was pulled and synched up
					// can just return from here
					return;
				}
			}

			// We need to pull the folder alias__appId and the file alias__appId.smss
			String aliasProjectId = smss.replaceAll(".smss", "");

			// Close the database (if an existing app), so that we can pull without file locks
			try {
				if (projectAlreadyLoaded) {
					DIHelper.getInstance().removeProjectProperty(projectId);
					project.closeProject();
				}

				// Make the app directory (if it doesn't already exist)
				File thisUserFolder = new File(Utility.normalizePath(userFolder + FILE_SEPARATOR + aliasProjectId));
				thisUserFolder.mkdir(); 

				// Pull the contents of the project folder before the smss
				logger.info("Pulling app from remote=" + projectId + " to target=" + thisUserFolder.getPath());
				runRcloneTransferProcess(userRcloneConfig, "rclone", "sync", userRcloneConfig + ":", thisUserFolder.getPath());
				logger.debug("Done pulling from remote=" + projectId + " to target=" + thisUserFolder.getPath());

				// Now pull the smss
				logger.info("Pulling smss from remote=" + smssContainer + " to target=" + userFolder);
				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE USER FOLDER
				runRcloneTransferProcess(smssRcloneConfig, "rclone", "copy", smssRcloneConfig + ":", userFolder);
				logger.debug("Done pulling from remote=" + smssContainer + " to target=" + userFolder);
			} finally {
				// Re-open the project
				if (projectAlreadyLoaded) {
					Utility.getUserAssetWorkspaceProject(projectId, isAsset);
				}
			}
		} finally {
			try {
				if (userRcloneConfig != null) {
					deleteRcloneConfig(userRcloneConfig);
				}
				if (smssRcloneConfig != null) {
					deleteRcloneConfig(smssRcloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("Project "+ projectId + " is unlocked");
			}
		}
	}

	@Override
	public void pushUserAssetOrWorkspace(String projectId, boolean isAsset) throws IOException, InterruptedException {
		IProject project = Utility.getUserAssetWorkspaceProject(projectId, isAsset);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}

		// We need to push the folder alias__appId and the file alias__appId.smss
		String alias = project.getProjectName();

		String aliasProjectId = alias + "__" + projectId;
		String thisUserFolder = userFolder + FILE_SEPARATOR + aliasProjectId;
		String smss = aliasProjectId + ".smss";
		String smssFile = userFolder + FILE_SEPARATOR + smss;

		// Start with the sas token
		String userRcloneConfig = null;
		String smssRCloneConfig = null;

		try {
			userRcloneConfig = createRcloneConfig(USER_CONTAINER_PREFIX + projectId);
			String smssContainer = projectId + SMSS_POSTFIX;
			smssRCloneConfig = createRcloneConfig(USER_CONTAINER_PREFIX + smssContainer);

			// Some temp files needed for the transfer
			File temp = null;
			File copy = null;

			// Close the database, so that we can push without file locks (also ensures that the db doesn't change mid push)
			try {
				DIHelper.getInstance().removeProjectProperty(projectId);
				project.closeProject();

				// Push the app folder
				logger.info("Pushing app from source=" + thisUserFolder + " to remote=" + projectId);
				runRcloneTransferProcess(userRcloneConfig, "rclone", "sync", thisUserFolder, userRcloneConfig + ":");
				logger.debug("Done pushing from source=" + thisUserFolder + " to remote=" + projectId);

				// Move the smss to an empty temp directory (otherwise will push all items in the db folder)
				String tempFolder = Utility.getRandomString(10);
				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
				temp.mkdir();
				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
				Files.copy(new File(Utility.normalizePath(smssFile)), copy);

				// Push the smss
				logger.info("Pushing smss from source=" + smssFile + " to remote=" + smssContainer);
				runRcloneTransferProcess(smssRCloneConfig, "rclone", "sync", temp.getPath(), smssRCloneConfig + ":");
				logger.debug("Done pushing from source=" + smssFile + " to remote=" + smssContainer);
			} finally {
				if (copy != null) {
					copy.delete();
				}
				if (temp != null) {
					temp.delete();
				}

				// Re-open the database
				Utility.getUserAssetWorkspaceProject(projectId, isAsset);
			}
		} finally {
			if (userRcloneConfig != null) {
				deleteRcloneConfig(userRcloneConfig);
			}
			if (smssRCloneConfig != null) {
				deleteRcloneConfig(smssRCloneConfig);
			}
		}
	}
	

	// This is the sync the whole app. It shouldn't be used yet. Only the insights DB should be sync actively 

	//	public  Boolean syncApp(String appId) throws IOException, InterruptedException{
	//		Boolean sync = false;
	//		IEngine engine = Utility.getEngine(appId, false);
	//		if (engine == null) {
	//			throw new IllegalArgumentException("App not found...");
	//		}
	//		String appRcloneConfig = null;
	//		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
	//		String aliasAppId = alias + "__" + appId;
	//		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
	//		try {
	//			appRcloneConfig = createRcloneConfig(appId);
	//				engine.closeDB();
	//				logger.debug("Checking from app path" + appFolder + " to remote=" + appId);
	//				List<String> results = runRcloneProcess(appRcloneConfig, "rclone", "check", appFolder+FILE_SEPARATOR + "insights_database.mv.db", appRcloneConfig + ":"+appId);
	//				for(String s:results){
	//				logger.debug("Result String: " + s);
	//				}
	//				if(results.get(0).contains("ERROR")){
	//					sync=true;
	//				}
	//		}  finally {
	//			if (appRcloneConfig != null) {
	//				deleteRcloneConfig(appRcloneConfig);
	//			}
	//		}
	//
	//		return sync;
	//	}

	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// Push ////////////////////////////////////////////

	public void pushApp(String appId) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}

		ENGINE_TYPE engineType = engine.getEngineType();

		// We need to push the folder alias__appId and the file alias__appId.smss
		String alias = null;
		if (engineType == ENGINE_TYPE.APP){
			alias = engine.getEngineName();
		} else{
			alias = SecurityQueryUtils.getDatabaseAliasForId(appId);
		}

		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		String smss = aliasAppId + ".smss";
		String smssFile = dbFolder + FILE_SEPARATOR + smss;

		// Start with the sas token
		String appRcloneConfig = null;
		String smssRCloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to push app");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + appId);
			String smssContainer = appId + SMSS_POSTFIX;
			smssRCloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + smssContainer);

			// Some temp files needed for the transfer
			File temp = null;
			File copy = null;

			// Close the database, so that we can push without file locks (also ensures that the db doesn't change mid push)
			try {
				DIHelper.getInstance().removeDbProperty(appId);
				engine.closeDB();

				// Push the app folder
				logger.info("Pushing app from source=" + appFolder + " to remote=" + appId);
				runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appFolder, appRcloneConfig + ":");
				logger.debug("Done pushing from source=" + appFolder + " to remote=" + appId);

				// Move the smss to an empty temp directory (otherwise will push all items in the db folder)
				String tempFolder = Utility.getRandomString(10);
				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
				temp.mkdir();
				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
				Files.copy(new File(Utility.normalizePath(smssFile)), copy);

				// Push the smss
				logger.info("Pushing smss from source=" + smssFile + " to remote=" + smssContainer);
				runRcloneTransferProcess(smssRCloneConfig, "rclone", "sync", temp.getPath(), smssRCloneConfig + ":");
				logger.debug("Done pushing from source=" + smssFile + " to remote=" + smssContainer);
			} finally {
				if (copy != null) {
					copy.delete();
				}
				if (temp != null) {
					temp.delete();
				}

				// Re-open the database
				Utility.getEngine(appId, false);
			}
		} finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
				if (smssRCloneConfig != null) {
					deleteRcloneConfig(smssRCloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("App "+ appId + " is unlocked");
			}
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// Pull ////////////////////////////////////////////

	public void pullApp(String appId) throws IOException, InterruptedException {
		pullApp(appId, false);
	}

	protected void pullApp(String appId, boolean appAlreadyLoaded) throws IOException, InterruptedException {
		IEngine engine = null;
		if (appAlreadyLoaded) {
			engine = Utility.getEngine(appId, false);
			if (engine == null) {
				throw new IllegalArgumentException("App not found...");
			}
		}

		String smssContainer = appId + SMSS_POSTFIX;

		// Start with the sas token
		String appRcloneConfig = null;
		String smssRcloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to pull app");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			appRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + appId);
			smssRcloneConfig = createRcloneConfig(DB_CONTAINER_PREFIX + smssContainer);

			// List the smss directory to get the alias + app id
			List<String> results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig + ":");
			String smss = null;
			for (String result : results) {
				if (result.endsWith(".smss")) {
					smss = result;
					break;
				}
			}
			if (smss == null) {
				try {
					fixLegacyDbStructure(appId);
				} catch(IOException | InterruptedException e) {
					throw new IOException("Failed to pull app for appid=" + appId);
				}
				
				// try again
				results = runRcloneProcess(smssRcloneConfig, "rclone", "lsf", smssRcloneConfig+":");
				for (String result : results) {
					if (result.endsWith(".smss")) {
						smss = result;
						break;
					}
				}
				
				if (smss == null) {
					throw new IOException("Failed to pull app for appid=" + appId);
				} else {
					// we just fixed the structure and this was pulled and synched up
					// can just return from here
					return;
				}
			}

			// We need to pull the folder alias__appId and the file alias__appId.smss
			String aliasAppId = smss.replaceAll(".smss", "");

			// Close the database (if an existing app), so that we can pull without file locks
			try {
				if (appAlreadyLoaded) {
					DIHelper.getInstance().removeDbProperty(appId);
					engine.closeDB();
				}

				// Make the app directory (if it doesn't already exist)
				File appFolder = new File(Utility.normalizePath(dbFolder + FILE_SEPARATOR + aliasAppId));
				appFolder.mkdir(); 

				// Pull the contents of the app folder before the smss
				logger.info("Pulling app from remote=" + appId + " to target=" + appFolder.getPath());
				runRcloneTransferProcess(appRcloneConfig, "rclone", "sync", appRcloneConfig + ":", appFolder.getPath());
				logger.debug("Done pulling from remote=" + appId + " to target=" + appFolder.getPath());

				// Now pull the smss
				logger.info("Pulling smss from remote=" + smssContainer + " to target=" + dbFolder);

				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
				runRcloneTransferProcess(smssRcloneConfig, "rclone", "copy", smssRcloneConfig + ":", dbFolder);
				logger.debug("Done pulling from remote=" + smssContainer + " to target=" + dbFolder);

				// Catalog the db if it is new
				if (!appAlreadyLoaded) {
					SMSSWebWatcher.catalogDB(smss, dbFolder);
				}
			} finally {
				// Re-open the database (if an existing app)
				if (appAlreadyLoaded) {
					Utility.getEngine(appId, false);
				}
			}
		} finally {
			try {
				if (appRcloneConfig != null) {
					deleteRcloneConfig(appRcloneConfig);
				}
				if (smssRcloneConfig != null) {
					deleteRcloneConfig(smssRcloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("App "+ appId + " is unlocked");
			}
		}
	}

	@Override
	public void pullAppImageFolder() throws IOException, InterruptedException {
		String rCloneConfig = null;
		try {
			rCloneConfig = createRcloneConfig(ClusterUtil.DB_IMAGES_BLOB);
			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+":");
			if(results.isEmpty()) {
				fixLegacyImageStructure();
				return;
			}
			
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "databases";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig + ":", imagesFolderPath);
		}finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void pushAppImageFolder() throws IOException, InterruptedException {
		String appRcloneConfig = null;
		try {
			appRcloneConfig = createRcloneConfig(ClusterUtil.DB_IMAGES_BLOB);
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "databases";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneTransferProcess(appRcloneConfig, "rclone", "sync",imagesFolderPath,  appRcloneConfig + ":");
		}finally {
			if (appRcloneConfig != null) {
				deleteRcloneConfig(appRcloneConfig);
			}
		}
	}

	@Override
	public void pullProjectImageFolder() throws IOException, InterruptedException {
		String rCloneConfig = null;
		try {
			rCloneConfig = createRcloneConfig(ClusterUtil.PROJECT_IMAGES_BLOB);
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "projects";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig + ":", imagesFolderPath);
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void pushProjectImageFolder() throws IOException, InterruptedException {
		String rCloneConfig = null;
		try {
			rCloneConfig = createRcloneConfig(ClusterUtil.PROJECT_IMAGES_BLOB);
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "projects";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", imagesFolderPath, rCloneConfig + ":");
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}


	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// Update///////////////////////////////////////////

	// TODO >>>timb: pixel to update app so that neel can add refresh button or something
	// TODO >>>timb: still need to test this method
	public void updateApp(String appId) throws IOException, InterruptedException {
		if (Utility.getEngine(appId, true) == null) {
			throw new IllegalArgumentException("App needs to be defined in order to update...");
		}
		pullApp(appId, false);
	}

	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// Delete //////////////////////////////////////////

	// TODO >>>timb: test out delete functionality
	@Override
	public void deleteApp(String appId) throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "account", name, "key", key);
			logger.debug("Deleting container=" + appId + ", " + appId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + DB_CONTAINER_PREFIX + appId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + DB_CONTAINER_PREFIX +appId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + DB_CONTAINER_PREFIX +appId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + DB_CONTAINER_PREFIX +appId + SMSS_POSTFIX);
			logger.debug("Done deleting container=" + appId + ", " + appId + SMSS_POSTFIX);
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
	}
	
	//TODO: make this different compared to app
	//TODO: make this different compared to app
	//TODO: make this different compared to app
	@Override
	public void deleteProject(String projectId) throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "account", name, "key", key);
			logger.debug("Deleting container=" + projectId + ", " + projectId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + PROJECT_CONTAINER_PREFIX + projectId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + PROJECT_CONTAINER_PREFIX + projectId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + PROJECT_CONTAINER_PREFIX + projectId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + PROJECT_CONTAINER_PREFIX + projectId + SMSS_POSTFIX);
			logger.debug("Done deleting container=" + projectId + ", " + projectId + SMSS_POSTFIX);
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// Cleanup//////////////////////////////////////////	
	
	public List<String> listAllBlobContainers() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		List<String> allContainers = new ArrayList<>();
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "account", name, "key", key);
			List<String> results = runRcloneProcess(rcloneConfig, "rclone", "lsf", rcloneConfig + ":");
			for (String result : results) {
				allContainers.add(result);
			}
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
		return allContainers;
	}

	@Override
	public void deleteContainer(String containerId) throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "account", name, "key", key);
			logger.debug("Deleting container=" + containerId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + containerId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + containerId);
			logger.debug("Done deleting container=" + containerId);
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////// Static Util Methods ////////////////////////////////////

	private static String createRcloneConfig(String container) throws IOException, InterruptedException {
		logger.debug("Generating SAS for container=" + container);
		String sasUrl = client.getSAS(container);
		String rcloneConfig = Utility.getRandomString(10);
		runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "sas_url", sasUrl);
		return rcloneConfig;
	}
	
	@Override
	public String createRcloneConfig() throws IOException, InterruptedException {
		// not implementing - only create config at the container level
		return null;
	}

	public static void main(String[] args) {
		String[] sarr = new String[] {"sakdf.smss","/opt/semosshome/db/kunal__3241231242", "/opt/semosshome/db/kunal__3241231242.smss", "/opt/semosshome/db/kunal__3241231242//../sakdf"};
		String s = "/opt/semosshome/db/kunal__3241231242";
		for(int i = 0; i<sarr.length; i++) {
			//System.out.println(sarr[i] + " -> " + Utility.normalizePath(sarr[i]));
		}
		System.out.println(Utility.normalizePath("/opt/semosshome/db/../../../../root/myvirus.virus"));
		//System.out.println("/opt/semosshome/db" + FILE_SEPARATOR + Utility.normalizePath("../root/myvirus.virus"));




	}

	/*

	private static void deleteRcloneConfig(String rcloneConfig) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "delete", rcloneConfig);
		} finally {
			new File(configPath).delete();
		}
	}

	private static List<String> runRcloneProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--config");
		commandList.add(configPath);
		String[] newCommand = commandList.toArray(new String[] {});
		return runAnyProcess(newCommand);	
	}

	private static String getConfigPath(String rcloneConfig) {
		return rcloneConfigFolder + FILE_SEPARATOR + rcloneConfig + ".conf";
	}

	private static List<String> runAnyProcess(String... command) throws IOException, InterruptedException {
		Process p = null;
		try {
			ProcessBuilder pb = new ProcessBuilder(command);
			pb.directory(new File(System.getProperty("user.home")));
		    pb.redirectOutput(Redirect.PIPE);
		    pb.redirectError(Redirect.PIPE);
			p = pb.start();
			p.waitFor();
			List<String> results = streamOutput(p.getInputStream());
			streamError(p.getErrorStream());
			return results;
		} finally {
			if (p != null) {
				p.destroyForcibly();
			}
		}
	}

	private static List<String> streamOutput(InputStream stream) throws IOException {
		return stream(stream, false);
	}

	private static List<String> streamError(InputStream stream) throws IOException {
		return stream(stream, true);
	}

	private static List<String> stream(InputStream stream, boolean error) throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
			List<String> lines = reader.lines().collect(Collectors.toList());
			for(String line : lines) {
				if (error) {
					System.err.println(line);
				} else {
					logger.debug(line);
				}
			}
			return lines;
		}
	}
	 */



	/*

	public static void main(String[] args) throws IOException, InterruptedException {


		try {

			List<String> appWithImages= new ArrayList();

			// List the smss directory to get the alias + app id
			String[] cmdArray = {"rclone", "lsf", "kunalp:"};
			List<String> containers = runAnyProcess("rclone", "lsf", "kunalp:");
			for (String container: containers){
				List<String> images = runAnyProcess("rclone", "ls" , "kunalp:"+container+"/version/image.png");
				if(!(images.isEmpty()) && images != null){
					appWithImages.add(container);
				}
			}
			for(String s: appWithImages){

				logger.debug(s);
				List<String> copied = runAnyProcess("rclone", "copy" , "kunalp:"+s+"version/image.png", "kunalp:aaa-imagecontainer/");
				List<String> moved = runAnyProcess("rclone", "moveto" , "kunalp:aaa-imagecontainer/image.png", "kunalp:aaa-imagecontainer/"+s.substring(0, s.length()-1)+".png");

				//List<String> copied = runAnyProcess("rclone", "copy" , "kunalp:"+s+"version/image.png", "/Users/semoss/Documents/workspace/Semoss/images/download/");
				//List<String> copied = runAnyProcess("rclone", "copy" , "kunalp:"+s+"version/image.png", "/Users/semoss/Documents/workspace/Semoss/images/download/"+s.substring(0, s.length()-1)+".png");

			}

//			List<String> containers = runAnyProcess("rclone", "lsf", "kunalp:");
//
//			for(String appid : results1){
//				if( appid.contains("smss")){
//					continue;
//				}
//				
//				
//				
//				List<String> results2= runAnyProcess("rclone", "lsf", "kunalp:"+appid);
//				for(String content : results2){
//					if( content.contains("version")){
//						
//						logger.debug(appid + " has a version folder");
//						appWithVersion.add(appid);
//					}
//				}
//			}

		} catch(Exception e){
			logger.error(STACKTRACE, e);
		}

		//DIHelper.getInstance().loadCoreProp("C:\\Users\\tbanach\\Documents\\Workspace\\Semoss\\RDF_Map.prop");
		//		String appId = "a295698a-1f1c-4639-aba6-74b226cd2dfc";
		//		logger.debug(AZClient.getInstance().getSAS("timb"));
		//		AZClient.getInstance().deleteApp("1bab355d-a2ea-4fde-9d2c-088287d46978");
		//		AZClient.getInstance().pushApp(appId);
		//		AZClient.getInstance().pullApp(appId);
		//		List<String> containers = AZClient.getInstance().listAllBlobContainers();
		//		for(String container : containers) {
		//			logger.debug(container);
		//		}
	}
	 */
	
}
