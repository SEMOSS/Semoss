package prerna.cluster.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.io.Files;

import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.LegacyToProjectRestructurerHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.ProjectSyncUtility;
import prerna.util.ProjectWatcher;
import prerna.util.SMSSWebWatcher;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class S3Client extends CloudClient {

	private static Logger logger = LogManager.getLogger(S3Client.class);

	private static final String PROVIDER = "s3";
	private static final String SMSS_POSTFIX = "-smss";

	static String BUCKET = null;
	private static String REGION = null;
	private static String ENDPOINT = null;
	private static String ACCESS_KEY = null;
	private static String SECRET_KEY = null;
	private static String RCLONE = "rclone";
	private static String RCLONE_PATH = "RCLONE_PATH";

	protected String dbFolder = null;
	protected String projectFolder = null;
	protected String userFolder = null;

	public static final String S3_REGION_KEY = "S3_REGION";
	public static final String S3_BUCKET_KEY = "S3_BUCKET";
	public static final String STORAGE = "STORAGE"; // says if this is local / cluster
	public static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
	public static final String S3_SECRET_KEY = "S3_SECRET_KEY";
	public static final String S3_ENDPOINT_KEY = "S3_ENDPOINT";

	public boolean useMinio = false;
	public boolean s3KeysProvided = false;

	static S3Client client = null;
	static String rcloneConfigFolder = null;

	static String RCLONE_DB_PATH = null;
	static String RCLONE_PROJECT_PATH = null;
	static String RCLONE_USER_PATH = null;

	// create an instance
	// Also needs to be synchronized so multiple calls don't try to init at the same
	// time
	public static synchronized S3Client getInstance() {
		if (client == null) {
			client = new S3Client();
			client.init();
		}
		return client;
	}

	@Override
	public void init() {
		rcloneConfigFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + "rcloneConfig";
		new File(rcloneConfigFolder).mkdir();

		Map<String, String> env = System.getenv();
		if (env.containsKey(S3_REGION_KEY)) {
			REGION = env.get(S3_REGION_KEY);
		} else if (DIHelper.getInstance().getProperty(S3_REGION_KEY) != null
				&& !(DIHelper.getInstance().getProperty(S3_REGION_KEY).isEmpty())) {
			REGION = DIHelper.getInstance().getProperty(S3_REGION_KEY);
		} else {
			throw new IllegalArgumentException("There is no region specified.");
		}

		if (env.containsKey(S3_BUCKET_KEY)) {
			BUCKET = env.get(S3_BUCKET_KEY);
		} else if (DIHelper.getInstance().getProperty(S3_BUCKET_KEY) != null
				&& !(DIHelper.getInstance().getProperty(S3_BUCKET_KEY).isEmpty())) {
			BUCKET = DIHelper.getInstance().getProperty(S3_BUCKET_KEY);
		} else {
			throw new IllegalArgumentException("There is no bucket key specified.");
		}

		if (env.containsKey(S3_ACCESS_KEY)) {
			ACCESS_KEY = env.get(S3_ACCESS_KEY);
		} else if (DIHelper.getInstance().getProperty(S3_ACCESS_KEY) != null
				&& !(DIHelper.getInstance().getProperty(S3_ACCESS_KEY).isEmpty())) {
			ACCESS_KEY = DIHelper.getInstance().getProperty(S3_ACCESS_KEY);
		}

		if (env.containsKey(S3_SECRET_KEY)) {
			SECRET_KEY = env.get(S3_SECRET_KEY);
		} else if (DIHelper.getInstance().getProperty(S3_SECRET_KEY) != null
				&& !(DIHelper.getInstance().getProperty(S3_SECRET_KEY).isEmpty())) {
			SECRET_KEY = DIHelper.getInstance().getProperty(S3_SECRET_KEY);
		}

		if (env.containsKey(S3_ENDPOINT_KEY)) {
			ENDPOINT = env.get(S3_ENDPOINT_KEY);
		} else if (DIHelper.getInstance().getProperty(S3_ENDPOINT_KEY) != null
				&& !(DIHelper.getInstance().getProperty(S3_ENDPOINT_KEY).isEmpty())) {
			ENDPOINT = DIHelper.getInstance().getProperty(S3_ENDPOINT_KEY);
		}

		if (ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("MINIO")) {
			if (ACCESS_KEY == null || ACCESS_KEY.isEmpty() || SECRET_KEY == null || SECRET_KEY.isEmpty()
					|| ENDPOINT == null || ENDPOINT.isEmpty()) {
				throw new IllegalArgumentException(
						"Minio needs to have an access key, secret key, and endpoint defined.");
			}
			useMinio = true;
		} else if (ACCESS_KEY == null || ACCESS_KEY.isEmpty() || SECRET_KEY == null || SECRET_KEY.isEmpty()) {
			s3KeysProvided = false;
		} else {
			s3KeysProvided = true;
		}

		if (env.containsKey(RCLONE_PATH)) {
			RCLONE = env.get(RCLONE_PATH);
		} else if (DIHelper.getInstance().getProperty(RCLONE_PATH) != null
				&& !(DIHelper.getInstance().getProperty(RCLONE_PATH).isEmpty())) {
			RCLONE = DIHelper.getInstance().getProperty(RCLONE_PATH);
		} else {
			RCLONE = "rclone";
		}

		// useful variables
		this.dbFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.DB_FOLDER;
		this.projectFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.PROJECT_FOLDER;
		this.userFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.USER_FOLDER;
		
		S3Client.RCLONE_DB_PATH = ":" + BUCKET + "/" + ClusterUtil.DB_BLOB + "/";
		S3Client.RCLONE_PROJECT_PATH = ":" + BUCKET + "/" + ClusterUtil.PROJECT_BLOB + "/";
		S3Client.RCLONE_USER_PATH = ":" + BUCKET + "/" + ClusterUtil.USER_BLOB + "/";
	}

	@Override
	public void pullOwl(String appId) throws IOException, InterruptedException{
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		File owlFile = null;
		String alias = SecurityDatabaseUtils.getDatabaseAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		String rCloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to pull owl");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(appId);
			//close the owl
			engine.getBaseDataEngine().closeDB();
			owlFile = new File(engine.getProperty(Constants.OWL));

			logger.info("Pulling owl and postions.json for " + appFolder + " from remote=" + appId);


			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
			//sync will delete files that are in the destination if they aren't being synced

			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_DB_PATH+appId+"/"+owlFile.getName(), appFolder);
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_DB_PATH+appId+"/"+AbstractEngine.OWL_POSITION_FILENAME, appFolder);

		}  finally {
			try {
				if (rCloneConfig != null) {
					deleteRcloneConfig(rCloneConfig);
				}
				//open the owl
				if(owlFile != null && owlFile.exists()) {
					engine.setOWL(owlFile.getAbsolutePath());
				} else {
					throw new IllegalArgumentException("Pull failed. OWL for engine " + appId + " was not found");
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
	public void pushOwl(String appId) throws IOException, InterruptedException{
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		File owlFile = null;
		String alias = SecurityDatabaseUtils.getDatabaseAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		String rCloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to push owl");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(appId);
			//close the owl
			engine.getBaseDataEngine().closeDB();
			owlFile = new File(engine.getProperty(Constants.OWL));

			logger.info("Pushing owl and postions.json for " + appFolder + " from remote=" + appId);


			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
			//sync will delete files that are in the destination if they aren't being synced

			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", appFolder+"/"+owlFile.getName(), rCloneConfig+RCLONE_DB_PATH+appId);			 
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", appFolder+"/"+AbstractEngine.OWL_POSITION_FILENAME, rCloneConfig+RCLONE_DB_PATH+appId);			 
		}  finally {
			try {
				if (rCloneConfig != null) {
					deleteRcloneConfig(rCloneConfig);
				}
				//open the owl
				if(owlFile != null && owlFile.exists()) {
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
	public void pullInsightsDB(String projectId) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}

		String rCloneConfig = null;
		String alias = project.getProjectName();
		String aliasProjectId = SmssUtilities.getUniqueName(alias, projectId);
		String thisProjectFolder = this.projectFolder + FILE_SEPARATOR + aliasProjectId;
		
		// synchronize on the app id
		logger.info("Applying lock for " + projectId + " to pull insights db");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(projectId);
			project.getInsightDatabase().closeDB();
			logger.info("Pulling insights database for " + thisProjectFolder + " from remote=" + projectId);
			String insightDB = getInsightDB(project, thisProjectFolder);
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_PROJECT_PATH+projectId+"/"+insightDB, thisProjectFolder);
		} finally {
			try {
				if (rCloneConfig != null) {
					deleteRcloneConfig(rCloneConfig);
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
		
		String rCloneConfig = null;
		String alias = project.getProjectName();
		String aliasProjectId = SmssUtilities.getUniqueName(alias, projectId);
		String thisProjectFolder = this.projectFolder + FILE_SEPARATOR + aliasProjectId;

		// synchronize on the project id
		logger.info("Applying lock for " + projectId + " to push insights db");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(projectId);
			project.getInsightDatabase().closeDB();
			logger.info("Pushing insights database for " + thisProjectFolder + " from remote=" + projectId);
			String insightDB = getInsightDB(project, thisProjectFolder);
			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
			//sync will delete files that are in the destination if they aren't being synced
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", thisProjectFolder+"/"+insightDB, rCloneConfig+RCLONE_PROJECT_PATH+projectId);			 
		}
		finally {
			try {
				if (rCloneConfig != null) {
					deleteRcloneConfig(rCloneConfig);
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
	public void pullEngineFolder(String appId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String rCloneConfig = null;
		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		//String aliasAppId = alias + "__" + appId;
		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to pull folder " + remoteRelativePath);
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(appId);
			logger.info("Pulling folder for " + remoteRelativePath + " from remote=" + appId);

			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_DB_PATH+appId+"/"+remoteRelativePath, absolutePath);
		} finally {
			try {
				if (rCloneConfig != null) {
					deleteRcloneConfig(rCloneConfig);
				}
			}finally {
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
		String rCloneConfig = null;
		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		//String aliasAppId = alias + "__" + appId;
		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		File absoluteFolder = new File(absolutePath);
		if(absoluteFolder.isDirectory()) {
			//this is adding a hidden file into every sub folder to make sure there is no empty directory
			ClusterUtil.validateFolder(absoluteFolder.getAbsolutePath());
		}
		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to push folder " + remoteRelativePath);
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(appId);
			logger.info("Pushing folder for " + remoteRelativePath + " from remote=" + appId);

			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", absolutePath, rCloneConfig+RCLONE_DB_PATH+appId+"/"+remoteRelativePath);
		} finally {
			try {
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
	public void pullProjectFolder(String projectId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		String rCloneConfig = null;
		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		//String aliasAppId = alias + "__" + appId;
		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		// synchronize on the app id
		logger.info("Applying lock for " + projectId + " to pull folder " + remoteRelativePath);
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(projectId);
			logger.info("Pulling folder for " + remoteRelativePath + " from remote=" + projectId);

			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_PROJECT_PATH+projectId+"/"+remoteRelativePath, absolutePath);
		} finally {
			try {
				if (rCloneConfig != null) {
					deleteRcloneConfig(rCloneConfig);
				}
			}finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("Project "+ projectId + " is unlocked");
			}
		}
	}

	@Override
	public void pushProjectFolder(String projectId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		String rCloneConfig = null;
		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		//String aliasAppId = alias + "__" + appId;
		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		File absoluteFolder = new File(Utility.normalizePath(absolutePath));
		if(absoluteFolder.isDirectory()) {
			// this is adding a hidden file into every sub folder to make sure there is no empty directory
			ClusterUtil.validateFolder(absoluteFolder.getAbsolutePath());
		}
		// synchronize on the app id
		logger.info("Applying lock for project " + projectId + " to push folder " + remoteRelativePath);
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(projectId);
			logger.info("Pushing folder for " + remoteRelativePath + " from remote=" + projectId);

			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", absolutePath, rCloneConfig+RCLONE_PROJECT_PATH+projectId+"/"+remoteRelativePath);
		} finally {
			try {
				if (rCloneConfig != null) {
					deleteRcloneConfig(rCloneConfig);
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
	public void pushApp(String appId) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}

		ENGINE_TYPE engineType = engine.getEngineType();

		// We need to push the folder alias__appId and the file alias__appId.smss
		String alias = null;
		if (engineType == ENGINE_TYPE.APP) {
			alias = engine.getEngineName();
		} else {
			alias = SecurityDatabaseUtils.getDatabaseAliasForId(appId);
		}

		String normalizedAlias = Utility.normalizePath(alias);
		String aliasAppId = normalizedAlias + "__" + appId;
		String thisDbFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		String smss = aliasAppId + ".smss";
		String smssFile = dbFolder + FILE_SEPARATOR + smss;

		String rCloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to push app");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(appId);
			String smssContainer = appId + SMSS_POSTFIX;

			// Some temp files needed for the transfer
			File temp = null;
			File copy = null;

			// Close the database, so that we can push without file locks (also ensures that
			// the db doesn't change mid push)
			try {
				DIHelper.getInstance().removeDbProperty(appId);
				engine.closeDB();

				// Push the app folder
				logger.info("Pushing app from source=" + thisDbFolder + " to remote=" + Utility.cleanLogString(appId));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", thisDbFolder, rCloneConfig+RCLONE_DB_PATH+appId);
				logger.debug("Done pushing from source=" + thisDbFolder + " to remote=" + Utility.cleanLogString(appId));

				// Move the smss to an empty temp directory (otherwise will push all items in
				// the db folder)
				String tempFolder = Utility.getRandomString(10);
				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
				temp.mkdir();
				copy = new File(temp.getPath() + FILE_SEPARATOR + smss);
				Files.copy(new File(smssFile), copy);

				// Push the smss
				logger.info("Pushing smss from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", temp.getPath(), rCloneConfig+RCLONE_DB_PATH+smssContainer);
				logger.debug("Done pushing from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
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
	public void pushDatabaseSmss(String databaseId) throws IOException, InterruptedException {
		// We need to push the file alias__appId.smss
		String alias = SecurityDatabaseUtils.getDatabaseAliasForId(databaseId);
		String smss = alias + "__" + databaseId + ".smss";
		String smssFile = Utility.normalizePath(dbFolder + FILE_SEPARATOR + smss);

		String rCloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + databaseId + " to push app");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
		lock.lock();
		logger.info("App "+ databaseId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(databaseId);
			String smssContainer = databaseId + SMSS_POSTFIX;

			// Some temp files needed for the transfer
			File temp = null;
			File copy = null;

			// Close the database, so that we can push without file locks (also ensures that
			// the db doesn't change mid push)
			try {
				// Move the smss to an empty temp directory (otherwise will push all items in
				// the db folder)
				String tempFolder = Utility.getRandomString(10);
				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
				temp.mkdir();
				copy = new File(temp.getPath() + FILE_SEPARATOR + smss);
				Files.copy(new File(smssFile), copy);

				// Push the smss
				logger.info("Pushing smss from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", temp.getPath(), rCloneConfig+RCLONE_DB_PATH+smssContainer);
				logger.debug("Done pushing from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
			} finally {
				if (copy != null) {
					copy.delete();
				}
				if (temp != null) {
					temp.delete();
				}
			}
		} finally {
			try {
				if (rCloneConfig != null) {
					deleteRcloneConfig(rCloneConfig);
				}
			}
			finally {
				// always unlock regardless of errors
				lock.unlock();
				logger.info("App "+ databaseId + " is unlocked");
			}
		}
	}

	@Override
	public void pullApp(String appId) throws IOException, InterruptedException {
		pullApp(appId, false);
	}

	@Override
	protected void pullApp(String appId, boolean appAlreadyLoaded) throws IOException, InterruptedException {
		IEngine engine = null;
		if (appAlreadyLoaded) {
			engine = Utility.getEngine(appId, false);
			if (engine == null) {
				throw new IllegalArgumentException("App not found...");
			}
		}
		String smssContainer = appId + SMSS_POSTFIX;
		String rCloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to pull app");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(appId);
			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_DB_PATH+smssContainer);
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
				results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_DB_PATH+smssContainer);
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

			// Close the database (if an existing app), so that we can pull without file
			// locks
			try {
				if (appAlreadyLoaded) {
					DIHelper.getInstance().removeDbProperty(appId);
					engine.closeDB();
				}

				// Make the app directory (if it doesn't already exist)
				File appFolder = new File(dbFolder + FILE_SEPARATOR + aliasAppId);
				appFolder.mkdir();
				// Pull the contents of the app folder before the smss
				logger.info("Pulling app from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_DB_PATH+appId, appFolder.getPath());
				logger.debug("Done pulling from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));

				// Now pull the smss
				logger.info("Pulling smss from remote=" + smssContainer + " to target=" + dbFolder);
				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
				runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_DB_PATH+smssContainer, dbFolder);
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
	public void updateApp(String appId) throws IOException, InterruptedException {
		if (Utility.getEngine(appId, true) == null) {
			throw new IllegalArgumentException("App needs to be defined in order to update...");
		}
		pullApp(appId, false);
	}

	@Override
	public void deleteApp(String appId) throws IOException, InterruptedException {
		String rcloneConfig = null;
		String cleanedAppId = Utility.cleanLogString(appId);
		try {
			rcloneConfig = createRcloneConfig(appId);
			logger.info("Deleting container=" + cleanedAppId + ", " + cleanedAppId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig+RCLONE_DB_PATH+appId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig+RCLONE_DB_PATH+appId+SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig+RCLONE_DB_PATH+appId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig+RCLONE_DB_PATH+appId+SMSS_POSTFIX);
			logger.info("Done deleting container=" + cleanedAppId + ", " + cleanedAppId + SMSS_POSTFIX);
		} finally {
			deleteRcloneConfig(rcloneConfig);
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
		String rCloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + projectId + " to pull project");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(projectId);
			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_PROJECT_PATH+smssContainer);
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
					fixLegacyDbStructure(projectId);
				} catch(IOException | InterruptedException e) {
					throw new IOException("Failed to pull project for projectId=" + projectId);
				}
				
				// try again
				results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_PROJECT_PATH+smssContainer);
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

			// Close the project (if an existing app), so that we can pull without file locks
			try {
				if (projectAlreadyLoaded) {
					DIHelper.getInstance().removeProjectProperty(projectId);
					project.closeProject();
				}

				// Make the project directory (if it doesn't already exist)
				File thisProjectFolder = new File(projectFolder + FILE_SEPARATOR + Utility.normalizePath(aliasProjectId));
				thisProjectFolder.mkdir();
				// Pull the contents of the app folder before the smss
				logger.info("Pulling project from remote=" + Utility.cleanLogString(projectId) + " to target=" + Utility.cleanLogString(thisProjectFolder.getPath()));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_PROJECT_PATH+projectId, thisProjectFolder.getPath());
				logger.debug("Done pulling from remote=" + Utility.cleanLogString(projectId) + " to target=" + Utility.cleanLogString(thisProjectFolder.getPath()));

				// Now pull the smss
				logger.info("Pulling smss from remote=" + smssContainer + " to target=" + projectFolder);
				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE PROJECT FOLDER
				runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_PROJECT_PATH+smssContainer, projectFolder);
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
				if (rCloneConfig != null) {
					deleteRcloneConfig(rCloneConfig);
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
	public void pushProject(String projectId) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}

		String alias = project.getProjectName();
		if(alias == null) {
			alias = SecurityProjectUtils.getProjectAliasForId(projectId);
		}

		String normalizedAlias = Utility.normalizePath(alias);
		String aliasProjectId = normalizedAlias + "__" + projectId;
		String thisProjectFolder = projectFolder + FILE_SEPARATOR + aliasProjectId;
		String smss = aliasProjectId + ".smss";
		String smssFile = projectFolder + FILE_SEPARATOR + smss;

		String rCloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + projectId + " to push project");
		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
		lock.lock();
		logger.info("Project "+ projectId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(projectId);
			String smssContainer = projectId + SMSS_POSTFIX;

			// Some temp files needed for the transfer
			File temp = null;
			File copy = null;

			// Close the database, so that we can push without file locks (also ensures that
			// the db doesn't change mid push)
			try {
				DIHelper.getInstance().removeProjectProperty(projectId);
				project.closeProject();

				// Push the project folder
				logger.info("Pushing project from source=" + thisProjectFolder + " to remote=" + Utility.cleanLogString(projectId));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", thisProjectFolder, rCloneConfig+RCLONE_PROJECT_PATH+projectId);
				logger.debug("Done pushing from source=" + thisProjectFolder + " to remote=" + Utility.cleanLogString(projectId));

				// Move the smss to an empty temp directory (otherwise will push all items in the project folder)
				String tempFolder = Utility.getRandomString(10);
				temp = new File(projectFolder + FILE_SEPARATOR + tempFolder);
				temp.mkdir();
				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
				Files.copy(new File(Utility.normalizePath(smssFile)), copy);

				// Push the smss
				logger.info("Pushing smss from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", temp.getPath(), rCloneConfig+RCLONE_PROJECT_PATH+smssContainer);
				logger.debug("Done pushing from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
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
				if (rCloneConfig != null) {
					deleteRcloneConfig(rCloneConfig);
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
	public void deleteProject(String projectId) throws IOException, InterruptedException {
		String rcloneConfig = null;
		String cleanedProjectId = Utility.cleanLogString(projectId);
		try {
			rcloneConfig = createRcloneConfig(projectId);
			logger.info("Deleting container=" + cleanedProjectId + ", " + cleanedProjectId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig+RCLONE_PROJECT_PATH+projectId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig+RCLONE_PROJECT_PATH+projectId+SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig+RCLONE_PROJECT_PATH+projectId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig+RCLONE_PROJECT_PATH+projectId+SMSS_POSTFIX);
			logger.info("Done deleting container=" + cleanedProjectId + ", " + cleanedProjectId + SMSS_POSTFIX);
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
	}

	@Override
	public List<String> listAllBlobContainers() throws IOException, InterruptedException {
		String rcloneConfig = null;
		List<String> allContainers = new ArrayList<>();
		try {
			rcloneConfig = createRcloneConfig();
			List<String> results = runRcloneProcess(rcloneConfig, "rclone", "lsf", rcloneConfig + ":" + BUCKET);
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
		String rcloneConfig = null;
		String cleanedContainerId = Utility.cleanLogString(containerId);
		try {
			rcloneConfig = createRcloneConfig();
			logger.info("Deleting container = " + cleanedContainerId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + BUCKET + "/" + containerId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + BUCKET + "/" + containerId);
			logger.info("Done deleting container = " + cleanedContainerId);
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////// Static Util Methods
	////////////////////////////////////////////////////////////////////////////////////////// ////////////////////////////////////

	private String createRcloneConfig(String id) {
		logger.info("Generating config for app/project/image " + Utility.cleanLogString(id));
		String rcloneConfig = null;
		try {
			rcloneConfig = createRcloneConfig();
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (InterruptedException ie) {
			logger.error(Constants.STACKTRACE, ie);
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return rcloneConfig;
	}

	@Override
	public String createRcloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);

		if (useMinio) {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id",
					ACCESS_KEY, "secret_access_key", SECRET_KEY, "region", REGION, "endpoint", ENDPOINT);
		} else if (s3KeysProvided) {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id",
					ACCESS_KEY, "secret_access_key", SECRET_KEY, "region", REGION);
		} else {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "env_auth", "true",
					"region", REGION);
		}
		return rcloneConfig;
	}

	@Override
	public void pullDatabaseImageFolder() throws IOException, InterruptedException {
		String rCloneConfig = null;
		try {
			rCloneConfig = createRcloneConfig(ClusterUtil.DB_IMAGES_BLOB);
			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+":"+BUCKET+"/"+ClusterUtil.DB_IMAGES_BLOB);
			if(results.isEmpty()) {
				fixLegacyImageStructure();
				return;
			}
			
			String imagesFolderPath = ClusterUtil.IMAGES_FOLDER_PATH + FILE_SEPARATOR + "databases";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":"+BUCKET+"/"+ClusterUtil.DB_IMAGES_BLOB, imagesFolderPath);
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void pushDatabaseImageFolder() throws IOException, InterruptedException {
		String rCloneConfig = null;
		try {
			rCloneConfig = createRcloneConfig(ClusterUtil.DB_IMAGES_BLOB);
			String imagesFolderPath = ClusterUtil.IMAGES_FOLDER_PATH + FILE_SEPARATOR + "databases";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneProcess(rCloneConfig, "rclone", "sync", imagesFolderPath, rCloneConfig+":"+BUCKET+"/"+ClusterUtil.DB_IMAGES_BLOB);
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	public void pullProjectImageFolder() throws IOException, InterruptedException {
		String rCloneConfig = null;
		try {
			rCloneConfig = createRcloneConfig(ClusterUtil.PROJECT_IMAGES_BLOB);
			String imagesFolderPath = ClusterUtil.IMAGES_FOLDER_PATH + FILE_SEPARATOR + "projects";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":"+BUCKET+"/"+ClusterUtil.PROJECT_IMAGES_BLOB, imagesFolderPath);
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
			String imagesFolderPath = ClusterUtil.IMAGES_FOLDER_PATH + FILE_SEPARATOR + "projects";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneProcess(rCloneConfig, "rclone", "sync", imagesFolderPath, rCloneConfig+":"+BUCKET+"/"+ClusterUtil.PROJECT_IMAGES_BLOB);
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void pushDB(String appId, RdbmsTypeEnum e) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String rCloneConfig = null;
		String alias = SecurityDatabaseUtils.getDatabaseAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to push db file");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(appId);

			DIHelper.getInstance().removeDbProperty(appId);
			engine.closeDB();

			logger.info("Pulling database for" + appFolder + " from remote=" + appId);
			if (e == RdbmsTypeEnum.SQLITE) {
				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
				for (String sqliteFile : sqliteFileNames) {
					runRcloneProcess(rCloneConfig, "rclone", "sync", appFolder + "/" + sqliteFile, rCloneConfig+RCLONE_DB_PATH+appId);
				}
			} else if (e == RdbmsTypeEnum.H2_DB) {
				runRcloneProcess(rCloneConfig, "rclone", "sync", appFolder + "/database.mv.db", rCloneConfig+RCLONE_DB_PATH+appId);
			} else {
				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
			}

			// open the engine again
			Utility.getEngine(appId, false);
		} finally {
			try {
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
	public void pullDB(String appId, RdbmsTypeEnum e) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String rCloneConfig = null;
		String alias = SecurityDatabaseUtils.getDatabaseAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to pull db file");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");

		try {
			rCloneConfig = createRcloneConfig(appId);
			engine.closeDB();
			logger.info("Pulling database for" + appFolder + " from remote=" + appId);
			if (e == RdbmsTypeEnum.SQLITE) {
				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
				for (String sqliteFile : sqliteFileNames) {
					runRcloneProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_DB_PATH+appId+"/"+sqliteFile, appFolder);
				}
			} else if (e == RdbmsTypeEnum.H2_DB) {
				runRcloneProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_DB_PATH+appId+"/database.mv.db", appFolder);
			} else {
				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
			}
		} finally {
			try {
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

	protected static List<String> listBucketFiles(String rcloneConfig, String subpath) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		try {
			if(subpath != null && !subpath.isEmpty()) {
				return runRcloneProcess(rcloneConfig, "rclone", "lsf", rcloneConfig + ":" + BUCKET + subpath);
			} else {
				return runRcloneProcess(rcloneConfig, "rclone", "lsf", rcloneConfig + ":" + BUCKET);
			}
		} finally {
			new File(configPath).delete();
		}
	}

	@Override
	@Deprecated
	public void fixLegacyDbStructure(String appId) throws IOException, InterruptedException {
		String smssContainer = appId + SMSS_POSTFIX;
		String rCloneConfig = null;

		// synchronize on the app id
		logger.info("Applying lock for " + appId + " to pull app");
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		logger.info("App "+ appId + " is locked");
		try {
			rCloneConfig = createRcloneConfig(appId);
			// WE HAVE TO PULL FROM THE OLD LOCATION WITHOUT THERE BEING A /db/ IN THE PATH
			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+":"+BUCKET+"/"+smssContainer);
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
			File appFolder = new File(dbFolder + FILE_SEPARATOR + Utility.normalizePath(aliasAppId));
			appFolder.mkdir();
			// Pull the contents of the app folder before the smss
			logger.info("Pulling app from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+":"+BUCKET+"/"+appId, appFolder.getPath());
			logger.debug("Done pulling from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));

			// Now pull the smss
			logger.info("Pulling smss from remote=" + smssContainer + " to target=" + dbFolder);
			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":"+BUCKET+"/"+smssContainer, dbFolder);
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
		String rCloneConfig = null;

		try {
			rCloneConfig = createRcloneConfig(appId);
			// WE HAVE TO PULL FROM THE OLD LOCATION WITHOUT THERE BEING A /db/ IN THE PATH
			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+":"+BUCKET+"/"+smssContainer);
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
			File appFolder = new File(dbFolder + FILE_SEPARATOR + Utility.normalizePath(aliasAppId));
			appFolder.mkdir();
			// Pull the contents of the app folder before the smss
			logger.info("Pulling app from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+":"+BUCKET+"/"+appId, appFolder.getPath());
			logger.debug("Done pulling from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));

			// Now pull the smss
			logger.info("Pulling smss from remote=" + smssContainer + " to target=" + dbFolder);
			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":"+BUCKET+"/"+smssContainer, dbFolder);
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
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	@Deprecated
	public void fixLegacyImageStructure() throws IOException, InterruptedException {
		String rCloneConfig = null;
		try {
			rCloneConfig = createRcloneConfig(ClusterUtil.DB_IMAGES_BLOB);
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "databases";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			// first pull
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":"+BUCKET+"/semoss-imagecontainer", imagesFolderPath);
			// now push into the correct folder
			runRcloneProcess(rCloneConfig, "rclone", "sync", imagesFolderPath, rCloneConfig+":"+BUCKET+"/"+ClusterUtil.DB_IMAGES_BLOB);
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	public void pushInsight(String projectId, String insightId) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		String rCloneConfig = null;

		try {
			rCloneConfig = createRcloneConfig(projectId);

			// only need to pull the insight folder - 99% the project is always already loaded to get to this point
			String insightFolderPath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId);
			File insightFolder = new File(insightFolderPath);
			insightFolder.mkdir();
			
			String remoteInsightFolder = RCLONE_PROJECT_PATH+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;
			
			logger.info("Pushing insight insight from local=" + Utility.cleanLogString(insightFolder.getPath()) + " to remote=" + Utility.cleanLogString(remoteInsightFolder));
			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", 
					insightFolder.getPath(),
					rCloneConfig+remoteInsightFolder);
			logger.debug("Done pushing insight from remote=" + Utility.cleanLogString(insightFolder.getPath()) + " to remote=" + Utility.cleanLogString(remoteInsightFolder));
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void pullInsight(String projectId, String insightId) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		String rCloneConfig = null;

		try {
			rCloneConfig = createRcloneConfig(projectId);

			// only need to pull the insight folder - 99% the project is always already loaded to get to this point
			String insightFolderPath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId);
			File insightFolder = new File(insightFolderPath);
			insightFolder.mkdir();
			
			String remoteInsightFolder = RCLONE_PROJECT_PATH+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;
			
			logger.info("Pulling insight from remote=" + Utility.cleanLogString(remoteInsightFolder) + " to local=" + Utility.cleanLogString(insightFolder.getPath()));
			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", 
					rCloneConfig+remoteInsightFolder, 
					insightFolder.getPath());
			logger.debug("Done pulling insight from remote=" + Utility.cleanLogString(remoteInsightFolder) + " to local=" + Utility.cleanLogString(insightFolder.getPath()));
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	public void pushInsightImage(String projectId, String insightId, String oldImageFileName, String newImageFileName) throws IOException, InterruptedException {
		IProject project = Utility.getProject(projectId, false);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}
		String rcloneConfig = null;

		try {
			rcloneConfig = createRcloneConfig(projectId);

			String remoteInsightImageFilePath = RCLONE_PROJECT_PATH+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;

			// since extensions might be different, need to actually delete the old file by name
			if(oldImageFileName != null) {
				String oldFileToDelete = remoteInsightImageFilePath+"/"+oldImageFileName;
				
				logger.info("Deleting old insight image from remote=" + Utility.cleanLogString(oldFileToDelete));
				runRcloneDeleteFileProcess(rcloneConfig, "rclone", "deletefile", rcloneConfig+oldFileToDelete);
				logger.debug("Done deleting old insight image from remote=" + Utility.cleanLogString(oldFileToDelete));
			} else {
				logger.info("No old insight image on remote to delete");
			}

			if(newImageFileName != null) {
				String insightImageFilePath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId + "/" + newImageFileName);
	
				logger.info("Pushing new insight image from local=" + Utility.cleanLogString(insightImageFilePath) + " to remote=" + Utility.cleanLogString(remoteInsightImageFilePath));
				runRcloneTransferProcess(rcloneConfig, "rclone", "sync", 
						insightImageFilePath,
						rcloneConfig+remoteInsightImageFilePath);
				logger.debug("Done pushing new insight image from local=" + Utility.cleanLogString(insightImageFilePath) + " to remote=" + Utility.cleanLogString(remoteInsightImageFilePath));
			} else {
				logger.info("No new insight image to add to remote");
			}
		} finally {
			if (rcloneConfig != null) {
				deleteRcloneConfig(rcloneConfig);
			}
		}
	}

	@Override
	public void pullUserAssetOrWorkspace(String projectId, boolean isAsset, boolean projectAlreadyLoaded) throws IOException, InterruptedException {
		IProject project = null;
		if (projectAlreadyLoaded) {
			project = Utility.getUserAssetWorkspaceProject(projectId, isAsset);
			if (project == null) {
				throw new IllegalArgumentException("Project not found...");
			}
		}
		String smssContainer = projectId + SMSS_POSTFIX;
		String rCloneConfig = null;

		try {
			rCloneConfig = createRcloneConfig(projectId);
			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_USER_PATH+smssContainer);
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
				results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_USER_PATH+smssContainer);
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

			// Close the project (if an existing app), so that we can pull without file locks
			try {
				if (projectAlreadyLoaded) {
					DIHelper.getInstance().removeProjectProperty(projectId);
					project.closeProject();
				}

				// Make the project directory (if it doesn't already exist)
				// THIS IS THE SAME AS PUSH PROJECT BUT USES THE userFolder
				File thisUserFolder = new File(userFolder + FILE_SEPARATOR + Utility.normalizePath(aliasProjectId));
				thisUserFolder.mkdir();
				// Pull the contents of the app folder before the smss
				logger.info("Pulling app from remote=" + Utility.cleanLogString(projectId) + " to target=" + Utility.cleanLogString(thisUserFolder.getPath()));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_USER_PATH+projectId, thisUserFolder.getPath());
				logger.debug("Done pulling from remote=" + Utility.cleanLogString(projectId) + " to target=" + Utility.cleanLogString(thisUserFolder.getPath()));

				// Now pull the smss
				logger.info("Pulling smss from remote=" + smssContainer + " to target=" + userFolder);
				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE USER FOLDER
				runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_USER_PATH+smssContainer, userFolder);
				logger.debug("Done pulling from remote=" + smssContainer + " to target=" + userFolder);
			} finally {
				// Re-open the database (if an existing app)
				if (projectAlreadyLoaded) {
					Utility.getUserAssetWorkspaceProject(projectId, isAsset);
				}
			}
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void pushUserAssetOrWorkspace(String projectId, boolean isAsset) throws IOException, InterruptedException {
		IProject project = Utility.getUserAssetWorkspaceProject(projectId, isAsset);
		if (project == null) {
			throw new IllegalArgumentException("Project not found...");
		}

		String alias = project.getProjectName();

		String normalizedAlias = Utility.normalizePath(alias);
		String aliasProjectId = normalizedAlias + "__" + projectId;
		// THIS IS THE SAME AS PUSH PROJECT BUT USES THE userFolder
		String thisProjectFolder = userFolder + FILE_SEPARATOR + aliasProjectId;
		String smss = aliasProjectId + ".smss";
		String smssFile = userFolder + FILE_SEPARATOR + smss;

		String rCloneConfig = null;
		try {
			rCloneConfig = createRcloneConfig(projectId);
			String smssContainer = projectId + SMSS_POSTFIX;

			// Some temp files needed for the transfer
			File temp = null;
			File copy = null;

			// Close the database, so that we can push without file locks (also ensures that
			// the db doesn't change mid push)
			try {
				DIHelper.getInstance().removeProjectProperty(projectId);
				project.closeProject();

				// Push the app folder
				logger.info("Pushing project from source=" + thisProjectFolder + " to remote=" + Utility.cleanLogString(projectId));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", thisProjectFolder, rCloneConfig+RCLONE_USER_PATH+projectId);
				logger.debug("Done pushing from source=" + thisProjectFolder + " to remote=" + Utility.cleanLogString(projectId));

				// Move the smss to an empty temp directory (otherwise will push all items in the project folder)
				String tempFolder = Utility.getRandomString(10);
				temp = new File(userFolder + FILE_SEPARATOR + tempFolder);
				temp.mkdir();
				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
				Files.copy(new File(smssFile), copy);

				// Push the smss
				logger.info("Pushing smss from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", temp.getPath(), rCloneConfig+RCLONE_USER_PATH+smssContainer);
				logger.debug("Done pushing from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
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
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

}
