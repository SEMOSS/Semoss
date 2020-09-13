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

import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.EngineInsightsHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.SMSSWebWatcher;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class S3Client extends CloudClient {
	private static Logger logger = LogManager.getLogger(S3Client.class);

	private static final String PROVIDER = "s3";
	private static final String STACKTRACE = "StackTrace: ";
	private static final String SMSS_POSTFIX = "-smss";

	private static String BUCKET = null;
	private static String REGION = null;
	private static String ENDPOINT = null;
	private static String ACCESS_KEY = null;
	private static String SECRET_KEY = null;
	private static String RCLONE = "rclone";
	private static String RCLONE_PATH = "RCLONE_PATH";

	private String dbFolder = null;

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

	protected S3Client() {

	}

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

		rcloneConfigFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR
				+ "rcloneConfig";
		new File(rcloneConfigFolder).mkdir();
		dbFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + "db";

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

	}
	
	@Override
	public void pullOwl(String appId) throws IOException, InterruptedException{
		IEngine engine = Utility.getEngine(appId, false, true);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		File owlFile = null;
		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		String rCloneConfig = null;

		// synchronize on the app id
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		try {
			rCloneConfig = createRcloneConfig(appId);
			//close the owl
			engine.getBaseDataEngine().closeDB();
			owlFile = new File(engine.getProperty(Constants.OWL));
								
			logger.info("Pulling owl and postions.json for " + appFolder + " from remote=" + appId);
			
	
			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
			//sync will delete files that are in the destination if they aren't being synced
			
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig + ":" + BUCKET + "/" + appId+"/"+ owlFile.getName(), appFolder);
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig + ":" + BUCKET + "/" + appId+"/"+ AbstractEngine.OWL_POSITION_FILENAME, appFolder);

		}  finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
			//open the owl
			if(owlFile.exists()) {
				engine.setOWL(owlFile.getAbsolutePath());
			} else {
				throw new IllegalArgumentException("Pull failed. OWL for engine " + appId + " was not found");
			}
			// always unlock regardless of errors
			lock.unlock();

		}
	}
	
	@Override
	public void pushOwl(String appId) throws IOException, InterruptedException{
		IEngine engine = Utility.getEngine(appId, false, true);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		File owlFile = null;
		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		String rCloneConfig = null;

		// synchronize on the app id
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		try {
			rCloneConfig = createRcloneConfig(appId);
			//close the owl
			engine.getBaseDataEngine().closeDB();
			owlFile = new File(engine.getProperty(Constants.OWL));
								
			logger.info("Pushing owl and postions.json for " + appFolder + " from remote=" + appId);
			
	
			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
			//sync will delete files that are in the destination if they aren't being synced

			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", appFolder+"/" + owlFile.getName(), rCloneConfig + ":" + BUCKET + "/" + appId);			 
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", appFolder+"/" + AbstractEngine.OWL_POSITION_FILENAME, rCloneConfig + ":" + BUCKET + "/" + appId);			 


		}  finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
			//open the owl
			if(owlFile.exists()) {
				engine.setOWL(owlFile.getAbsolutePath());
			} else {
				throw new IllegalArgumentException("Push failed. OWL for engine " + appId + " was not found");
			}
			// always unlock regardless of errors
			lock.unlock();
		}
	}

	@Override
	public void pullInsightsDB(String appId) throws IOException, InterruptedException {
		
		IEngine engine = Utility.getEngine(appId, false, true);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}

		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		String rCloneConfig = null;

		// synchronize on the app id
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		try {
			rCloneConfig = createRcloneConfig(appId);
			engine.getInsightDatabase().closeDB();
			logger.info("Pulling insights database for " + appFolder + " from remote=" + appId);
			String insightDB = getInsightDB(engine, appFolder);
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig + ":" + BUCKET + "/" + appId + "/" + insightDB, appFolder);
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
			//open the insight db
			String insightDbLoc = SmssUtilities.getInsightsRdbmsFile(engine.getProp()).getAbsolutePath();
			if(insightDbLoc != null) {
				engine.setInsightDatabase( EngineInsightsHelper.loadInsightsEngine(engine.getProp(), LogManager.getLogger(AbstractEngine.class)));
			} else {
				throw new IllegalArgumentException("Insight database was not able to be found");
			}
			// always unlock regardless of errors
			lock.unlock();
		}
	}
	
	@Override
	public void pushInsightDB(String appId) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false, true);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String rCloneConfig = null;
		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		
		// synchronize on the app id
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		try {
			rCloneConfig = createRcloneConfig(appId);
			engine.getInsightDatabase().closeDB();
			logger.info("Pushing insights database for " + appFolder + " from remote=" + appId);
			String insightDB = getInsightDB(engine, appFolder);
			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
			//sync will delete files that are in the destination if they aren't being synced
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", appFolder+"/"+ insightDB, rCloneConfig + ":" + BUCKET + "/" + appId);			 
		}
		finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
			//open the insight db
			String insightDbLoc = SmssUtilities.getInsightsRdbmsFile(engine.getProp()).getAbsolutePath();
			if(insightDbLoc != null) {
				engine.setInsightDatabase( EngineInsightsHelper.loadInsightsEngine(engine.getProp(), LogManager.getLogger(AbstractEngine.class)));
			} else {
				throw new IllegalArgumentException("Insight database was not able to be found");
			}
			// always unlock regardless of errors
			lock.unlock();

		}
	}
	
	@Override
	public void pullFolder(String appId, String absolutePath, String remoteRelativePath)
			throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false, true);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String rCloneConfig = null;
		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		//String aliasAppId = alias + "__" + appId;
		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		
		// synchronize on the app id
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		try {
			rCloneConfig = createRcloneConfig(appId);
			logger.info("Pulling folder for " + remoteRelativePath + " from remote=" + appId);

			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig + ":" + BUCKET + "/" + appId + "/"  + remoteRelativePath, absolutePath);
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
			// always unlock regardless of errors
			lock.unlock();
		}
	}
	
	@Override
	public void pushFolder(String appId, String absolutePath, String remoteRelativePath)
			throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false, true);
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
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		try {
			rCloneConfig = createRcloneConfig(appId);
			logger.info("Pushing folder for " + remoteRelativePath + " from remote=" + appId);

			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", absolutePath, rCloneConfig + ":" + BUCKET + "/" + appId + "/"  + remoteRelativePath);
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
			// always unlock regardless of errors
			lock.unlock();
		}
	}

	@Override
	public void pushApp(String appId) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false, true);

		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}

		ENGINE_TYPE engineType = engine.getEngineType();

		// We need to push the folder alias__appId and the file alias__appId.smss
		String alias = null;
		if (engineType == ENGINE_TYPE.APP) {
			alias = engine.getEngineName();
		} else {
			alias = SecurityQueryUtils.getEngineAliasForId(appId);
		}

		String normalizedAlias = Utility.normalizePath(alias);
		String aliasAppId = normalizedAlias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		String smss = aliasAppId + ".smss";
		String smssFile = dbFolder + FILE_SEPARATOR + smss;

		String rCloneConfig = null;
		
		// synchronize on the app id
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		try {
			rCloneConfig = createRcloneConfig(appId);
			String smssContainer = appId + SMSS_POSTFIX;

			// Some temp files needed for the transfer
			File temp = null;
			File copy = null;

			// Close the database, so that we can push without file locks (also ensures that
			// the db doesn't change mid push)
			try {
				DIHelper.getInstance().removeLocalProperty(appId);
				engine.closeDB();

				// Push the app folder
				logger.info("Pushing app from source=" + appFolder + " to remote=" + Utility.cleanLogString(appId));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", appFolder, rCloneConfig + ":" + BUCKET + "/" + appId);
				logger.debug("Done pushing from source=" + appFolder + " to remote=" + Utility.cleanLogString(appId));

				// Move the smss to an empty temp directory (otherwise will push all items in
				// the db folder)
				String tempFolder = Utility.getRandomString(10);
				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
				temp.mkdir();
				copy = new File(temp.getPath() + FILE_SEPARATOR + smss);
				Files.copy(new File(smssFile), copy);

				// Push the smss
				logger.info("Pushing smss from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", temp.getPath(),
						rCloneConfig + ":" + BUCKET + "/" + smssContainer);
				logger.debug("Done pushing from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
			} finally {
				if (copy != null) {
					copy.delete();
				}
				if (temp != null) {
					temp.delete();
				}

				// Re-open the database
				Utility.getEngine(appId, false, true);
			}
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
			
			// always unlock regardless of errors
			lock.unlock();
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
			engine = Utility.getEngine(appId, false, true);
			if (engine == null) {
				throw new IllegalArgumentException("App not found...");
			}
		}
		String smssContainer = appId + SMSS_POSTFIX;
		String rCloneConfig = null;
		
		// synchronize on the app id
		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
		lock.lock();
		try {
			rCloneConfig = createRcloneConfig(appId);
			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf",
					rCloneConfig + ":" + BUCKET + "/" + smssContainer);
			String smss = null;
			for (String result : results) {
				if (result.endsWith(".smss")) {
					smss = result;
					break;
				}
			}
			if (smss == null) {
				throw new IOException("Failed to pull app for appid=" + appId);
			}

			// We need to pull the folder alias__appId and the file alias__appId.smss
			String aliasAppId = smss.replaceAll(".smss", "");

			// Close the database (if an existing app), so that we can pull without file
			// locks
			try {
				if (appAlreadyLoaded) {
					DIHelper.getInstance().removeLocalProperty(appId);
					engine.closeDB();
				}

				// Make the app directory (if it doesn't already exist)
				File appFolder = new File(dbFolder + FILE_SEPARATOR + aliasAppId);
				appFolder.mkdir();
				// Pull the contents of the app folder before the smss
				logger.info("Pulling app from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig + ":" + BUCKET + "/" + appId,
						appFolder.getPath());
				logger.debug("Done pulling from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));

				// Now pull the smss
				logger.info("Pulling smss from remote=" + smssContainer + " to target=" + dbFolder);
				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
				runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig + ":" + BUCKET + "/" + smssContainer,
						dbFolder);
				logger.debug("Done pulling from remote=" + smssContainer + " to target=" + dbFolder);

				// Catalog the db if it is new
				if (!appAlreadyLoaded) {
					SMSSWebWatcher.catalogDB(smss, dbFolder);
				}
			} finally {
				// Re-open the database (if an existing app)
				if (appAlreadyLoaded) {
					Utility.getEngine(appId, false, true);
				}
			}
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
			
			// always unlock regardless of errors
			lock.unlock();
		}
	}

	@Override
	public void updateApp(String appId) throws IOException, InterruptedException {
		if (Utility.getEngine(appId, true, true) == null) {
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
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + BUCKET + "/" + appId);
			runRcloneProcess(rcloneConfig, "rclone", "delete",
					rcloneConfig + ":" + BUCKET + "/" + appId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + BUCKET + "/" + appId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + BUCKET + "/" + appId + SMSS_POSTFIX);
			logger.info("Done deleting container=" + cleanedAppId + ", " + cleanedAppId + SMSS_POSTFIX);
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

	private String createRcloneConfig(String appId) {
		logger.info("Generating config for app " + Utility.cleanLogString(appId));
		String rcloneConfig = null;
		try {
			rcloneConfig = createRcloneConfig();
		} catch (IOException ioe) {
			logger.error(STACKTRACE, ioe);
		} catch (InterruptedException ie) {
			logger.error(STACKTRACE, ie);
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			logger.error(STACKTRACE, e);
		}
		return rcloneConfig;
	}

	private String createRcloneConfig() throws IOException, InterruptedException {
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

	protected static void deleteRcloneConfig(String rcloneConfig) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		try {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "delete", rcloneConfig);
		} finally {
			new File(configPath).delete();
		}
	}

	@Override

	public void pullImageFolder() throws IOException, InterruptedException {
		String rCloneConfig = null;
		try {
			rCloneConfig = createRcloneConfig(ClusterUtil.IMAGES_BLOB);
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "apps";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy",
					rCloneConfig + ":" + BUCKET + "/" + ClusterUtil.IMAGES_BLOB, imagesFolderPath);
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void pushImageFolder() throws IOException, InterruptedException {
		String rCloneConfig = null;
		try {
			rCloneConfig = createRcloneConfig(ClusterUtil.IMAGES_BLOB);
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "apps";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneProcess(rCloneConfig, "rclone", "sync", imagesFolderPath,
					rCloneConfig + ":" + BUCKET + "/" + ClusterUtil.IMAGES_BLOB);
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void pushDB(String appId, RdbmsTypeEnum e) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false, true);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String rCloneConfig = null;
		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		try {
			rCloneConfig = createRcloneConfig(appId);

			DIHelper.getInstance().removeLocalProperty(appId);
			engine.closeDB();
			
			logger.info("Pulling database for" + appFolder + " from remote=" + appId);
			if (e == RdbmsTypeEnum.SQLITE) {
				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
				for (String sqliteFile : sqliteFileNames) {
					runRcloneProcess(rCloneConfig, "rclone", "sync", appFolder + "/" + sqliteFile,
							rCloneConfig + ":" + BUCKET + "/" + appId);
				}
			} else if (e == RdbmsTypeEnum.H2_DB) {
				runRcloneProcess(rCloneConfig, "rclone", "sync", appFolder + "/database.mv.db",
						rCloneConfig + ":" + BUCKET + "/" + appId);
			} else {
				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
			}

			// open the engine again
			Utility.getEngine(appId, false, true);
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void pullDB(String appId, RdbmsTypeEnum e) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false, true);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String rCloneConfig = null;
		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		try {
			rCloneConfig = createRcloneConfig(appId);
			engine.closeDB();
			logger.info("Pulling database for" + appFolder + " from remote=" + appId);
			if (e == RdbmsTypeEnum.SQLITE) {
				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
				for (String sqliteFile : sqliteFileNames) {
					runRcloneProcess(rCloneConfig, "rclone", "sync",
							rCloneConfig + ":" + BUCKET + "/" + appId + "/" + sqliteFile, appFolder);
				}
			} else if (e == RdbmsTypeEnum.H2_DB) {
				runRcloneProcess(rCloneConfig, "rclone", "sync",
						rCloneConfig + ":" + BUCKET + "/" + appId + "/database.mv.db", appFolder);
			} else {
				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
			}
		} finally {
			if (rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}



}
