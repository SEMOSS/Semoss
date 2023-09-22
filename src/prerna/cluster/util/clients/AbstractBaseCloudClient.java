//package prerna.cluster.util.clients;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.locks.ReentrantLock;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import com.google.common.io.Files;
//
//import prerna.auth.utils.SecurityEngineUtils;
//import prerna.auth.utils.SecurityProjectUtils;
//import prerna.auth.utils.WorkspaceAssetUtils;
//import prerna.cluster.util.ClusterUtil;
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
//public abstract class AbstractBaseCloudClient extends AbstractCloudClient {
//
//	private static Logger classLogger = LogManager.getLogger(AbstractBaseCloudClient.class);
//
//	{
//		this.PROVIDER = "s3";
//	}
//
//	protected String BUCKET = null;
//	protected String REGION = null;
//	protected String ACCESS_KEY = null;
//	protected String SECRET_KEY = null;
//
//	protected String RCLONE_DB_PATH = null;
//	protected String RCLONE_PROJECT_PATH = null;
//	protected String RCLONE_USER_PATH = null;
//
//	public AbstractBaseCloudClient(ICloudClientBuilder builder) {
//		// used to enforce builder for creation of cloud clients
//		super(builder);
//	}
//	
//	@Override
//	public void pullOwl(String databaseId) throws IOException, InterruptedException{
//		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
//		if (database == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//		File owlFile = null;
//		String alias = SecurityEngineUtils.getEngineAliasForId(databaseId);
//		String aliasAppId = alias + "__" + databaseId;
//		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//		String rCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to pull owl");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ databaseId + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(databaseId);
//			//close the owl
//			database.getBaseDataEngine().close();
//			owlFile = new File(database.getProperty(Constants.OWL));
//
//			classLogger.info("Pulling owl and postions.json for " + appFolder + " from remote=" + databaseId);
//
//
//			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
//			//sync will delete files that are in the destination if they aren't being synced
//
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_DB_PATH+databaseId+"/"+owlFile.getName(), appFolder);
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_DB_PATH+databaseId+"/"+AbstractDatabaseEngine.OWL_POSITION_FILENAME, appFolder);
//
//		}  finally {
//			try {
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
//				}
//				//open the owl
//				if(owlFile != null && owlFile.exists()) {
//					database.setOWL(owlFile.getAbsolutePath());
//				} else {
//					throw new IllegalArgumentException("Pull failed. OWL for engine " + databaseId + " was not found");
//				}
//			}
//			finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("App "+ databaseId + " is unlocked");
//			}
//
//		}
//	}
//
//	@Override
//	public void pushOwl(String databaseId) throws IOException, InterruptedException{
//		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
//		if (database == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//		File owlFile = null;
//		String alias = SecurityEngineUtils.getEngineAliasForId(databaseId);
//		String aliasAppId = alias + "__" + databaseId;
//		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//		String rCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to push owl");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ databaseId + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(databaseId);
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
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", appFolder+"/"+owlFile.getName(), rCloneConfig+RCLONE_DB_PATH+databaseId);			 
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", appFolder+"/"+AbstractDatabaseEngine.OWL_POSITION_FILENAME, rCloneConfig+RCLONE_DB_PATH+databaseId);			 
//		}  finally {
//			try {
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
//				}
//				//open the owl
//				if(owlFile != null && owlFile.exists()) {
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
//	@Override
//	public void pullInsightsDB(String projectId) throws IOException, InterruptedException {
//		IProject project = Utility.getProject(projectId, false);
//		if (project == null) {
//			throw new IllegalArgumentException("Project not found...");
//		}
//
//		String rCloneConfig = null;
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
//			rCloneConfig = createRcloneConfig(projectId);
//			project.getInsightDatabase().close();
//			classLogger.info("Pulling insights database for " + thisProjectFolder + " from remote=" + projectId);
//			String insightDB = getInsightDB(project, thisProjectFolder);
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_PROJECT_PATH+projectId+"/"+insightDB, thisProjectFolder);
//		} finally {
//			try {
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//		
//		String rCloneConfig = null;
//		String alias = project.getProjectName();
//		String aliasProjectId = SmssUtilities.getUniqueName(alias, projectId);
//		String thisProjectFolder = this.projectFolder + FILE_SEPARATOR + aliasProjectId;
//
//		// synchronize on the project id
//		classLogger.info("Applying lock for " + projectId + " to push insights db");
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ projectId + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(projectId);
//			project.getInsightDatabase().close();
//			classLogger.info("Pushing insights database for " + thisProjectFolder + " from remote=" + projectId);
//			String insightDB = getInsightDB(project, thisProjectFolder);
//			//use copy. copy moves the 1 file from local to remote so we don't override all of the remote with sync.
//			//sync will delete files that are in the destination if they aren't being synced
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", thisProjectFolder+"/"+insightDB, rCloneConfig+RCLONE_PROJECT_PATH+projectId);			 
//		}
//		finally {
//			try {
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//	@Override
//	public void pullDatabaseFolder(String appId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException {
//		IDatabaseEngine engine = Utility.getDatabase(appId, false);
//		if (engine == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//		String rCloneConfig = null;
//		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
//		//String aliasAppId = alias + "__" + appId;
//		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + appId + " to pull folder " + remoteRelativePath);
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
//		lock.lock();
//		classLogger.info("App "+ appId + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(appId);
//			classLogger.info("Pulling folder for " + remoteRelativePath + " from remote=" + appId);
//
//			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_DB_PATH+appId+"/"+remoteRelativePath, absolutePath);
//		} finally {
//			try {
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
//				}
//			}finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("App "+ appId + " is unlocked");
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
//		String rCloneConfig = null;
//		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
//		//String aliasAppId = alias + "__" + appId;
//		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		File absoluteFolder = new File(absolutePath);
//		if(absoluteFolder.isDirectory()) {
//			//this is adding a hidden file into every sub folder to make sure there is no empty directory
//			ClusterUtil.validateFolder(absoluteFolder.getAbsolutePath());
//		}
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to push folder " + remoteRelativePath);
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ databaseId + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(databaseId);
//			classLogger.info("Pushing folder for " + remoteRelativePath + " from remote=" + databaseId);
//
//			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", absolutePath, rCloneConfig+RCLONE_DB_PATH+databaseId+"/"+remoteRelativePath);
//		} finally {
//			try {
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//			throw new IllegalArgumentException("Project not found...");
//		}
//		String rCloneConfig = null;
//		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
//		//String aliasAppId = alias + "__" + appId;
//		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + Utility.cleanLogString(projectId) + " to pull folder " + Utility.normalizePath(remoteRelativePath));
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ Utility.cleanLogString(projectId) + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(projectId);
//			classLogger.info("Pulling folder for " + Utility.normalizePath(remoteRelativePath) + " from remote=" + Utility.cleanLogString(projectId));
//
//			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_PROJECT_PATH+projectId+"/"+remoteRelativePath, absolutePath);
//		} finally {
//			try {
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
//				}
//			}finally {
//				// always unlock regardless of errors
//				lock.unlock();
//				classLogger.info("Project "+ Utility.cleanLogString(projectId) + " is unlocked");
//			}
//		}
//	}
//
//	@Override
//	public void pushProjectFolder(String projectId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException {
//		IProject project = Utility.getProject(projectId, false);
//		if (project == null) {
//			throw new IllegalArgumentException("Project not found...");
//		}
//		String rCloneConfig = null;
//		//String alias = SecurityQueryUtils.getEngineAliasForId(appId);
//		//String aliasAppId = alias + "__" + appId;
//		//String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		File absoluteFolder = new File(Utility.normalizePath(absolutePath));
//		if(absoluteFolder.isDirectory()) {
//			// this is adding a hidden file into every sub folder to make sure there is no empty directory
//			ClusterUtil.validateFolder(absoluteFolder.getAbsolutePath());
//		}
//		// synchronize on the app id
//		classLogger.info("Applying lock for project " + Utility.cleanLogString(projectId) + " to push folder " + Utility.normalizePath(remoteRelativePath));
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ Utility.cleanLogString(projectId) + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(projectId);
//			classLogger.info("Pushing folder for " + Utility.normalizePath(remoteRelativePath) + " from remote=" + Utility.cleanLogString(projectId));
//
//			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", absolutePath, rCloneConfig+RCLONE_PROJECT_PATH+projectId+"/"+remoteRelativePath);
//		} finally {
//			try {
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//
//	@Override
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
//		if (engineType == DATABASE_TYPE.APP) {
//			alias = engine.getEngineName();
//		} else {
//			alias = SecurityEngineUtils.getEngineAliasForId(databaseId);
//		}
//
//		String normalizedAlias = Utility.normalizePath(alias);
//		String aliasAppId = normalizedAlias + "__" + databaseId;
//		String thisDbFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//		String smss = aliasAppId + ".smss";
//		String smssFile = dbFolder + FILE_SEPARATOR + smss;
//
//		String rCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to push app");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ databaseId + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(databaseId);
//			String smssContainer = databaseId + SMSS_POSTFIX;
//
//			// Some temp files needed for the transfer
//			File temp = null;
//			File copy = null;
//
//			// Close the database, so that we can push without file locks (also ensures that
//			// the db doesn't change mid push)
//			try {
//				DIHelper.getInstance().removeEngineProperty(databaseId);
//				engine.close();
//
//				// Push the app folder
//				classLogger.info("Pushing app from source=" + thisDbFolder + " to remote=" + Utility.cleanLogString(databaseId));
//				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", thisDbFolder, rCloneConfig+RCLONE_DB_PATH+databaseId);
//				classLogger.debug("Done pushing from source=" + thisDbFolder + " to remote=" + Utility.cleanLogString(databaseId));
//
//				// Move the smss to an empty temp directory (otherwise will push all items in
//				// the db folder)
//				String tempFolder = Utility.getRandomString(10);
//				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
//				temp.mkdir();
//				copy = new File(temp.getPath() + FILE_SEPARATOR + smss);
//				Files.copy(new File(smssFile), copy);
//
//				// Push the smss
//				classLogger.info("Pushing smss from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
//				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", temp.getPath(), rCloneConfig+RCLONE_DB_PATH+smssContainer);
//				classLogger.debug("Done pushing from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
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
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//		String rCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to push app");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ databaseId + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(databaseId);
//			String smssContainer = databaseId + SMSS_POSTFIX;
//
//			// Some temp files needed for the transfer
//			File temp = null;
//			File copy = null;
//
//			try {
//				// Move the smss to an empty temp directory (otherwise will push all items in the db folder)
//				String tempFolder = Utility.getRandomString(10);
//				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
//				temp.mkdir();
//				copy = new File(temp.getPath() + FILE_SEPARATOR + smss);
//				Files.copy(new File(smssFile), copy);
//
//				// Push the smss
//				classLogger.info("Pushing smss from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
//				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", temp.getPath(), rCloneConfig+RCLONE_DB_PATH+smssContainer);
//				classLogger.debug("Done pushing from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
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
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//	public void pullDatabase(String databaseId) throws IOException, InterruptedException {
//		pullDatabase(databaseId, false);
//	}
//
//	@Override
//	public void pullDatabase(String databaseId, boolean databaseAlreadyLoaded) throws IOException, InterruptedException {
//		IDatabaseEngine database = null;
//		if (databaseAlreadyLoaded) {
//			database = Utility.getDatabase(databaseId, false);
//			if (database == null) {
//				throw new IllegalArgumentException("App not found...");
//			}
//		}
//		String smssContainer = databaseId + SMSS_POSTFIX;
//		String rCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + Utility.cleanLogString(databaseId) + " to pull app");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("App "+ Utility.cleanLogString(databaseId) + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(databaseId);
//			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_DB_PATH+smssContainer);
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
//				results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_DB_PATH+smssContainer);
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
//			// Close the database (if an existing app), so that we can pull without file
//			// locks
//			try {
//				if (databaseAlreadyLoaded) {
//					DIHelper.getInstance().removeEngineProperty(databaseId);
//					database.close();
//				}
//
//				// Make the app directory (if it doesn't already exist)
//				File appFolder = new File(dbFolder + FILE_SEPARATOR + aliasAppId);
//				appFolder.mkdir();
//				// Pull the contents of the app folder before the smss
//				classLogger.info("Pulling app from remote=" + Utility.cleanLogString(databaseId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
//				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_DB_PATH+databaseId, appFolder.getPath());
//				classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(databaseId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
//
//				// Now pull the smss
//				classLogger.info("Pulling smss from remote=" + Utility.cleanLogString(smssContainer) + " to target=" + Utility.normalizePath(dbFolder));
//				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
//				runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_DB_PATH+smssContainer, dbFolder);
//				classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(smssContainer) + " to target=" + Utility.normalizePath(dbFolder));
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
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//	public void deleteDatabase(String appId) throws IOException, InterruptedException {
//		String rcloneConfig = null;
//		String cleanedAppId = Utility.cleanLogString(appId);
//		try {
//			rcloneConfig = createRcloneConfig(appId);
//			classLogger.info("Deleting container=" + cleanedAppId + ", " + cleanedAppId + SMSS_POSTFIX);
//			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig+RCLONE_DB_PATH+appId);
//			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig+RCLONE_DB_PATH+appId+SMSS_POSTFIX);
//			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig+RCLONE_DB_PATH+appId);
//			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig+RCLONE_DB_PATH+appId+SMSS_POSTFIX);
//			classLogger.info("Done deleting container=" + cleanedAppId + ", " + cleanedAppId + SMSS_POSTFIX);
//		} finally {
//			deleteRcloneConfig(rcloneConfig);
//		}
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
//		String smssContainer = projectId + SMSS_POSTFIX;
//		String rCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + projectId + " to pull project");
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ projectId + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(projectId);
//			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_PROJECT_PATH+smssContainer);
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
//					fixLegacyDbStructure(projectId);
//				} catch(IOException | InterruptedException e) {
//					classLogger.info(Constants.STACKTRACE, e);
//					throw new IOException("Failed to pull project for projectId=" + projectId);
//				}
//				
//				// try again
//				results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_PROJECT_PATH+smssContainer);
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
//			// Close the project (if an existing app), so that we can pull without file locks
//			try {
//				if (projectAlreadyLoaded) {
//					DIHelper.getInstance().removeProjectProperty(projectId);
//					project.close();
//				}
//
//				// Make the project directory (if it doesn't already exist)
//				File thisProjectFolder = new File(projectFolder + FILE_SEPARATOR + Utility.normalizePath(aliasProjectId));
//				thisProjectFolder.mkdir();
//				// Pull the contents of the app folder before the smss
//				classLogger.info("Pulling project from remote=" + Utility.cleanLogString(projectId) + " to target=" + Utility.cleanLogString(thisProjectFolder.getPath()));
//				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_PROJECT_PATH+projectId, thisProjectFolder.getPath());
//				classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(projectId) + " to target=" + Utility.cleanLogString(thisProjectFolder.getPath()));
//
//				// Now pull the smss
//				classLogger.info("Pulling smss from remote=" + smssContainer + " to target=" + projectFolder);
//				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE PROJECT FOLDER
//				runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_PROJECT_PATH+smssContainer, projectFolder);
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
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//	public void pushProject(String projectId) throws IOException, InterruptedException {
//		IProject project = Utility.getProject(projectId, false);
//		if (project == null) {
//			throw new IllegalArgumentException("Project not found...");
//		}
//
//		String alias = project.getProjectName();
//		if(alias == null) {
//			alias = SecurityProjectUtils.getProjectAliasForId(projectId);
//		}
//
//		String normalizedAlias = Utility.normalizePath(alias);
//		String aliasProjectId = normalizedAlias + "__" + projectId;
//		String thisProjectFolder = projectFolder + FILE_SEPARATOR + aliasProjectId;
//		String smss = aliasProjectId + ".smss";
//		String smssFile = projectFolder + FILE_SEPARATOR + smss;
//
//		String rCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + projectId + " to push project");
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ projectId + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(projectId);
//			String smssContainer = projectId + SMSS_POSTFIX;
//
//			// Some temp files needed for the transfer
//			File temp = null;
//			File copy = null;
//
//			// Close the database, so that we can push without file locks (also ensures that
//			// the db doesn't change mid push)
//			try {
//				DIHelper.getInstance().removeProjectProperty(projectId);
//				project.close();
//
//				// Push the project folder
//				classLogger.info("Pushing project from source=" + thisProjectFolder + " to remote=" + Utility.cleanLogString(projectId));
//				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", thisProjectFolder, rCloneConfig+RCLONE_PROJECT_PATH+projectId);
//				classLogger.debug("Done pushing from source=" + thisProjectFolder + " to remote=" + Utility.cleanLogString(projectId));
//
//				// Move the smss to an empty temp directory (otherwise will push all items in the project folder)
//				String tempFolder = Utility.getRandomString(10);
//				temp = new File(projectFolder + FILE_SEPARATOR + tempFolder);
//				temp.mkdir();
//				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
//				Files.copy(new File(Utility.normalizePath(smssFile)), copy);
//
//				// Push the smss
//				classLogger.info("Pushing smss from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
//				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", temp.getPath(), rCloneConfig+RCLONE_PROJECT_PATH+smssContainer);
//				classLogger.debug("Done pushing from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
//			} finally {
//				if (copy != null) {
//					copy.delete();
//				}
//				if (temp != null) {
//					temp.delete();
//				}
//
//				// Re-open the database
//				Utility.getProject(projectId, false);
//			}
//		} finally {
//			try {
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//		String rCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + projectId + " to push project");
//		ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
//		lock.lock();
//		classLogger.info("Project "+ projectId + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(projectId);
//			String smssContainer = projectId + SMSS_POSTFIX;
//
//			// Some temp files needed for the transfer
//			File temp = null;
//			File copy = null;
//
//			try {
//				// Move the smss to an empty temp directory (otherwise will push all items in the project folder)
//				String tempFolder = Utility.getRandomString(10);
//				temp = new File(projectFolder + FILE_SEPARATOR + tempFolder);
//				temp.mkdir();
//				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
//				Files.copy(new File(Utility.normalizePath(smssFile)), copy);
//
//				// Push the smss
//				classLogger.info("Pushing smss from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
//				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", temp.getPath(), rCloneConfig+RCLONE_PROJECT_PATH+smssContainer);
//				classLogger.debug("Done pushing from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
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
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//	public void deleteProject(String projectId) throws IOException, InterruptedException {
//		String rcloneConfig = null;
//		String cleanedProjectId = Utility.cleanLogString(projectId);
//		try {
//			rcloneConfig = createRcloneConfig(projectId);
//			classLogger.info("Deleting container=" + cleanedProjectId + ", " + cleanedProjectId + SMSS_POSTFIX);
//			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig+RCLONE_PROJECT_PATH+projectId);
//			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig+RCLONE_PROJECT_PATH+projectId+SMSS_POSTFIX);
//			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig+RCLONE_PROJECT_PATH+projectId);
//			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig+RCLONE_PROJECT_PATH+projectId+SMSS_POSTFIX);
//			classLogger.info("Done deleting container=" + cleanedProjectId + ", " + cleanedProjectId + SMSS_POSTFIX);
//		} finally {
//			deleteRcloneConfig(rcloneConfig);
//		}
//	}
//
//	@Override
//	public List<String> listAllBlobContainers() throws IOException, InterruptedException {
//		String rcloneConfig = null;
//		List<String> allContainers = new ArrayList<>();
//		try {
//			rcloneConfig = createRcloneConfig();
//			List<String> results = runRcloneProcess(rcloneConfig, "rclone", "lsf", rcloneConfig + ":" + BUCKET);
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
//		String rcloneConfig = null;
//		String cleanedContainerId = Utility.cleanLogString(containerId);
//		try {
//			rcloneConfig = createRcloneConfig();
//			classLogger.info("Deleting container = " + cleanedContainerId);
//			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + BUCKET + "/" + containerId);
//			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + BUCKET + "/" + containerId);
//			classLogger.info("Done deleting container = " + cleanedContainerId);
//		} finally {
//			deleteRcloneConfig(rcloneConfig);
//		}
//	}
//
//	//////////////////////////////////////////////////////////////////////////////////////////
//	//////////////////////////////////////////////////////////////////////////////////////////
//
//	protected String createRcloneConfig(String id) {
//		classLogger.info("Generating config for db/project/asset/image: " + Utility.cleanLogString(id));
//		String rcloneConfig = null;
//		try {
//			rcloneConfig = createRcloneConfig();
//		} catch (IOException ioe) {
//			classLogger.error(Constants.STACKTRACE, ioe);
//		} catch (InterruptedException ie) {
//			classLogger.error(Constants.STACKTRACE, ie);
//			Thread.currentThread().interrupt();
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		return rcloneConfig;
//	}
//
//	@Override
//	public void pullDatabaseImageFolder() throws IOException, InterruptedException {
//		String rCloneConfig = null;
//		try {
//			rCloneConfig = createRcloneConfig(CentralCloudStorage.DB_IMAGES_BLOB);
//			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+":"+BUCKET+"/"+CentralCloudStorage.DB_IMAGES_BLOB);
//			if(results.isEmpty()) {
//				fixLegacyImageStructure();
//				return;
//			}
//			
//			String imagesFolderPath = ClusterUtil.IMAGES_FOLDER_PATH + FILE_SEPARATOR + "databases";
//			File imageFolder = new File(imagesFolderPath);
//			imageFolder.mkdir();
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":"+BUCKET+"/"+CentralCloudStorage.DB_IMAGES_BLOB, imagesFolderPath);
//		} finally {
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
//			}
//		}
//	}
//
//	@Override
//	public void pushDatabaseImageFolder() throws IOException, InterruptedException {
//		String rCloneConfig = null;
//		try {
//			rCloneConfig = createRcloneConfig(CentralCloudStorage.DB_IMAGES_BLOB);
//			String imagesFolderPath = ClusterUtil.IMAGES_FOLDER_PATH + FILE_SEPARATOR + "databases";
//			File imageFolder = new File(imagesFolderPath);
//			imageFolder.mkdir();
//			runRcloneProcess(rCloneConfig, "rclone", "sync", imagesFolderPath, rCloneConfig+":"+BUCKET+"/"+CentralCloudStorage.DB_IMAGES_BLOB);
//		} finally {
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
//			}
//		}
//	}
//	
//	@Override
//	public void pullProjectImageFolder() throws IOException, InterruptedException {
//		String rCloneConfig = null;
//		try {
//			rCloneConfig = createRcloneConfig(CentralCloudStorage.PROJECT_IMAGES_BLOB);
//			String imagesFolderPath = ClusterUtil.IMAGES_FOLDER_PATH + FILE_SEPARATOR + "projects";
//			File imageFolder = new File(imagesFolderPath);
//			imageFolder.mkdir();
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":"+BUCKET+"/"+CentralCloudStorage.PROJECT_IMAGES_BLOB, imagesFolderPath);
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
//			String imagesFolderPath = ClusterUtil.IMAGES_FOLDER_PATH + FILE_SEPARATOR + "projects";
//			File imageFolder = new File(imagesFolderPath);
//			imageFolder.mkdir();
//			runRcloneProcess(rCloneConfig, "rclone", "sync", imagesFolderPath, rCloneConfig+":"+BUCKET+"/"+CentralCloudStorage.PROJECT_IMAGES_BLOB);
//		} finally {
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
//			}
//		}
//	}
//
//	@Override
//	public void pushLocalDatabaseFile(String databaseId, RdbmsTypeEnum dbType) throws IOException, InterruptedException {
//		IDatabaseEngine database = Utility.getDatabase(databaseId, false);
//		if (database == null) {
//			throw new IllegalArgumentException("App not found...");
//		}
//		String rCloneConfig = null;
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
//			rCloneConfig = createRcloneConfig(databaseId);
//
//			DIHelper.getInstance().removeEngineProperty(databaseId);
//			database.close();
//
//			classLogger.info("Pulling database for" + appFolder + " from remote=" + databaseId);
//			if (dbType == RdbmsTypeEnum.SQLITE) {
//				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
//				for (String sqliteFile : sqliteFileNames) {
//					runRcloneProcess(rCloneConfig, "rclone", "sync", appFolder + "/" + sqliteFile, rCloneConfig+RCLONE_DB_PATH+databaseId);
//				}
//			} else if (dbType == RdbmsTypeEnum.H2_DB) {
//				runRcloneProcess(rCloneConfig, "rclone", "sync", appFolder + "/database.mv.db", rCloneConfig+RCLONE_DB_PATH+databaseId);
//			} else {
//				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
//			}
//
//			// open the engine again
//			Utility.getDatabase(databaseId, false);
//		} finally {
//			try {
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//	public void pullLocalDatabaseFile(String databaseId, RdbmsTypeEnum rdbmsType) throws IOException, InterruptedException {
//		IDatabaseEngine engine = Utility.getDatabase(databaseId, false);
//		if (engine == null) {
//			throw new IllegalArgumentException("Database not found...");
//		}
//		String rCloneConfig = null;
//		String alias = SecurityEngineUtils.getEngineAliasForId(databaseId);
//		String aliasAppId = alias + "__" + databaseId;
//		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + databaseId + " to pull database file");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(databaseId);
//		lock.lock();
//		classLogger.info("Database "+ databaseId + " is locked");
//
//		try {
//			rCloneConfig = createRcloneConfig(databaseId);
//			engine.close();
//			classLogger.info("Pulling database for" + appFolder + " from remote=" + databaseId);
//			if (rdbmsType == RdbmsTypeEnum.SQLITE) {
//				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
//				for (String sqliteFile : sqliteFileNames) {
//					runRcloneProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_DB_PATH+databaseId+"/"+sqliteFile, appFolder);
//				}
//			} else if (rdbmsType == RdbmsTypeEnum.H2_DB) {
//				runRcloneProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_DB_PATH+databaseId+"/database.mv.db", appFolder);
//			} else {
//				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
//			}
//		} finally {
//			try {
//				if (rCloneConfig != null) {
//					deleteRcloneConfig(rCloneConfig);
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
//	protected List<String> listBucketFiles(String rcloneConfig, String subpath) throws IOException, InterruptedException {
//		String configPath = getConfigPath(rcloneConfig);
//		try {
//			if(subpath != null && !subpath.isEmpty()) {
//				return runRcloneProcess(rcloneConfig, "rclone", "lsf", rcloneConfig + ":" + BUCKET + subpath);
//			} else {
//				return runRcloneProcess(rcloneConfig, "rclone", "lsf", rcloneConfig + ":" + BUCKET);
//			}
//		} finally {
//			new File(configPath).delete();
//		}
//	}
//
//	@Override
//	@Deprecated
//	public void fixLegacyDbStructure(String appId) throws IOException, InterruptedException {
//		String smssContainer = appId + SMSS_POSTFIX;
//		String rCloneConfig = null;
//
//		// synchronize on the app id
//		classLogger.info("Applying lock for " + appId + " to pull app");
//		ReentrantLock lock = EngineSyncUtility.getEngineLock(appId);
//		lock.lock();
//		classLogger.info("App "+ appId + " is locked");
//		try {
//			rCloneConfig = createRcloneConfig(appId);
//			// WE HAVE TO PULL FROM THE OLD LOCATION WITHOUT THERE BEING A /db/ IN THE PATH
//			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+":"+BUCKET+"/"+smssContainer);
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
//			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+":"+BUCKET+"/"+appId, appFolder.getPath());
//			classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
//
//			// Now pull the smss
//			classLogger.info("Pulling smss from remote=" + smssContainer + " to target=" + dbFolder);
//			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":"+BUCKET+"/"+smssContainer, dbFolder);
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
//		String rCloneConfig = null;
//
//		try {
//			rCloneConfig = createRcloneConfig(appId);
//			// WE HAVE TO PULL FROM THE OLD LOCATION WITHOUT THERE BEING A /db/ IN THE PATH
//			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+":"+BUCKET+"/"+smssContainer);
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
//			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+":"+BUCKET+"/"+appId, appFolder.getPath());
//			classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(appId) + " to target=" + Utility.cleanLogString(appFolder.getPath()));
//
//			// Now pull the smss
//			classLogger.info("Pulling smss from remote=" + smssContainer + " to target=" + dbFolder);
//			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":"+BUCKET+"/"+smssContainer, dbFolder);
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
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
//			}
//		}
//	}
//	
//	@Override
//	@Deprecated
//	public void fixLegacyImageStructure() throws IOException, InterruptedException {
//		String rCloneConfig = null;
//		try {
//			rCloneConfig = createRcloneConfig(CentralCloudStorage.DB_IMAGES_BLOB);
//			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
//			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "databases";
//			File imageFolder = new File(imagesFolderPath);
//			imageFolder.mkdir();
//			// first pull
//			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+":"+BUCKET+"/semoss-imagecontainer", imagesFolderPath);
//			// now push into the correct folder
//			runRcloneProcess(rCloneConfig, "rclone", "sync", imagesFolderPath, rCloneConfig+":"+BUCKET+"/"+CentralCloudStorage.DB_IMAGES_BLOB);
//		} finally {
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
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
//		String rCloneConfig = null;
//
//		try {
//			rCloneConfig = createRcloneConfig(projectId);
//
//			// only need to pull the insight folder - 99% the project is always already loaded to get to this point
//			String insightFolderPath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId);
//			File insightFolder = new File(insightFolderPath);
//			insightFolder.mkdir();
//			
//			String remoteInsightFolder = RCLONE_PROJECT_PATH+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;
//			
//			classLogger.info("Pushing insight insight from local=" + Utility.cleanLogString(insightFolder.getPath()) + " to remote=" + Utility.cleanLogString(remoteInsightFolder));
//			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", 
//					insightFolder.getPath(),
//					rCloneConfig+remoteInsightFolder);
//			classLogger.debug("Done pushing insight from remote=" + Utility.cleanLogString(insightFolder.getPath()) + " to remote=" + Utility.cleanLogString(remoteInsightFolder));
//		} finally {
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
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
//		String rCloneConfig = null;
//
//		try {
//			rCloneConfig = createRcloneConfig(projectId);
//
//			// only need to pull the insight folder - 99% the project is always already loaded to get to this point
//			String insightFolderPath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId);
//			File insightFolder = new File(insightFolderPath);
//			insightFolder.mkdir();
//			
//			String remoteInsightFolder = RCLONE_PROJECT_PATH+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;
//			
//			classLogger.info("Pulling insight from remote=" + Utility.cleanLogString(remoteInsightFolder) + " to local=" + Utility.cleanLogString(insightFolder.getPath()));
//			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", 
//					rCloneConfig+remoteInsightFolder, 
//					insightFolder.getPath());
//			classLogger.debug("Done pulling insight from remote=" + Utility.cleanLogString(remoteInsightFolder) + " to local=" + Utility.cleanLogString(insightFolder.getPath()));
//		} finally {
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
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
//			rcloneConfig = createRcloneConfig(projectId);
//
//			String remoteInsightImageFilePath = RCLONE_PROJECT_PATH+projectId+"/"+Constants.APP_ROOT_FOLDER+"/"+Constants.VERSION_FOLDER+"/"+insightId;
//
//			// since extensions might be different, need to actually delete the old file by name
//			if(oldImageFileName != null) {
//				String oldFileToDelete = remoteInsightImageFilePath+"/"+oldImageFileName;
//				
//				classLogger.info("Deleting old insight image from remote=" + Utility.cleanLogString(oldFileToDelete));
//				runRcloneDeleteFileProcess(rcloneConfig, "rclone", "deletefile", rcloneConfig+oldFileToDelete);
//				classLogger.debug("Done deleting old insight image from remote=" + Utility.cleanLogString(oldFileToDelete));
//			} else {
//				classLogger.info("No old insight image on remote to delete");
//			}
//
//			if(newImageFileName != null) {
//				String insightImageFilePath = Utility.normalizePath(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId) + "/" + insightId + "/" + newImageFileName);
//	
//				classLogger.info("Pushing new insight image from local=" + Utility.cleanLogString(insightImageFilePath) + " to remote=" + Utility.cleanLogString(remoteInsightImageFilePath));
//				runRcloneTransferProcess(rcloneConfig, "rclone", "sync", 
//						insightImageFilePath,
//						rcloneConfig+remoteInsightImageFilePath);
//				classLogger.debug("Done pushing new insight image from local=" + Utility.cleanLogString(insightImageFilePath) + " to remote=" + Utility.cleanLogString(remoteInsightImageFilePath));
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
//		String smssContainer = projectId + SMSS_POSTFIX;
//		String rCloneConfig = null;
//
//		try {
//			rCloneConfig = createRcloneConfig(projectId);
//			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_USER_PATH+smssContainer);
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
//				results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rCloneConfig+RCLONE_USER_PATH+smssContainer);
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
//			// Close the project (if an existing app), so that we can pull without file locks
//			try {
//				if (projectAlreadyLoaded) {
//					DIHelper.getInstance().removeProjectProperty(projectId);
//					project.close();
//				}
//
//				// Make the project directory (if it doesn't already exist)
//				// THIS IS THE SAME AS PUSH PROJECT BUT USES THE userFolder
//				File thisUserFolder = new File(userFolder + FILE_SEPARATOR + Utility.normalizePath(aliasProjectId));
//				thisUserFolder.mkdir();
//				// Pull the contents of the app folder before the smss
//				classLogger.info("Pulling app from remote=" + Utility.cleanLogString(projectId) + " to target=" + Utility.cleanLogString(thisUserFolder.getPath()));
//				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rCloneConfig+RCLONE_USER_PATH+projectId, thisUserFolder.getPath());
//				classLogger.debug("Done pulling from remote=" + Utility.cleanLogString(projectId) + " to target=" + Utility.cleanLogString(thisUserFolder.getPath()));
//
//				// Now pull the smss
//				classLogger.info("Pulling smss from remote=" + smssContainer + " to target=" + userFolder);
//				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE USER FOLDER
//				runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rCloneConfig+RCLONE_USER_PATH+smssContainer, userFolder);
//				classLogger.debug("Done pulling from remote=" + smssContainer + " to target=" + userFolder);
//			} finally {
//				// Re-open the database (if an existing app)
//				if (projectAlreadyLoaded) {
//					Utility.getUserAssetWorkspaceProject(projectId, isAsset);
//				}
//			}
//		} finally {
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
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
//		String alias = project.getProjectName();
//
//		String normalizedAlias = Utility.normalizePath(alias);
//		String aliasProjectId = normalizedAlias + "__" + projectId;
//		// THIS IS THE SAME AS PUSH PROJECT BUT USES THE userFolder
//		String thisProjectFolder = userFolder + FILE_SEPARATOR + aliasProjectId;
//		String smss = aliasProjectId + ".smss";
//		String smssFile = userFolder + FILE_SEPARATOR + smss;
//
//		String rCloneConfig = null;
//		try {
//			rCloneConfig = createRcloneConfig(projectId);
//			String smssContainer = projectId + SMSS_POSTFIX;
//
//			// Some temp files needed for the transfer
//			File temp = null;
//			File copy = null;
//
//			// Close the database, so that we can push without file locks (also ensures that
//			// the db doesn't change mid push)
//			try {
//				DIHelper.getInstance().removeProjectProperty(projectId);
//				project.close();
//
//				// Push the app folder
//				classLogger.info("Pushing project from source=" + thisProjectFolder + " to remote=" + Utility.cleanLogString(projectId));
//				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", thisProjectFolder, rCloneConfig+RCLONE_USER_PATH+projectId);
//				classLogger.debug("Done pushing from source=" + thisProjectFolder + " to remote=" + Utility.cleanLogString(projectId));
//
//				// Move the smss to an empty temp directory (otherwise will push all items in the project folder)
//				String tempFolder = Utility.getRandomString(10);
//				temp = new File(userFolder + FILE_SEPARATOR + tempFolder);
//				temp.mkdir();
//				copy = new File(temp.getPath() + FILE_SEPARATOR + Utility.normalizePath(smss));
//				Files.copy(new File(smssFile), copy);
//
//				// Push the smss
//				classLogger.info("Pushing smss from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
//				runRcloneTransferProcess(rCloneConfig, "rclone", "sync", temp.getPath(), rCloneConfig+RCLONE_USER_PATH+smssContainer);
//				classLogger.debug("Done pushing from source=" + smssFile + " to remote=" + Utility.cleanLogString(smssContainer));
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
//			if (rCloneConfig != null) {
//				deleteRcloneConfig(rCloneConfig);
//			}
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
//}
