package prerna.cluster.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.clients.AbstractCloudClient;
import prerna.engine.api.IDatabase;
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.insight.TextToGraphic;

public class ClusterUtil {

	// Env vars used in clustered deployments
	// TODO >>>timb: make sure that everything cluster related starts with this,
	// also introduces tracibility
	
	private static final Logger logger = LogManager.getLogger(ClusterUtil.class);

	public static final String DB_BLOB = "semoss-db";
	public static final String PROJECT_BLOB = "semoss-project";
	public static final String USER_BLOB = "semoss-user";
	public static final String DB_IMAGES_BLOB = "semoss-dbimagecontainer";
	public static final String PROJECT_IMAGES_BLOB = "semoss-projectimagecontainer";
	
	private static final String IS_CLUSTER_KEY = "SEMOSS_IS_CLUSTER";
	public static final boolean IS_CLUSTER = (DIHelper.getInstance().getProperty(IS_CLUSTER_KEY) != null && !(DIHelper.getInstance().getProperty(IS_CLUSTER_KEY).isEmpty())) 
			? Boolean.parseBoolean(DIHelper.getInstance().getProperty(IS_CLUSTER_KEY)) : (
					(System.getenv().containsKey(IS_CLUSTER_KEY)) 
					? Boolean.parseBoolean(System.getenv(IS_CLUSTER_KEY)) : false);
			


	private static final String STORAGE_PROVIDER_KEY = "SEMOSS_STORAGE_PROVIDER";
	public static final String STORAGE_PROVIDER = (DIHelper.getInstance().getProperty(STORAGE_PROVIDER_KEY) != null && !(DIHelper.getInstance().getProperty(STORAGE_PROVIDER_KEY).isEmpty())) 
			? DIHelper.getInstance().getProperty(STORAGE_PROVIDER_KEY) : System.getenv(STORAGE_PROVIDER_KEY);

	private static final String REMOTE_RSERVE_KEY = "REMOTE_RSERVE";
	public static final boolean REMOTE_RSERVE = (DIHelper.getInstance().getProperty(REMOTE_RSERVE_KEY) != null && !(DIHelper.getInstance().getProperty(IS_CLUSTER_KEY).isEmpty())) 
			? Boolean.parseBoolean(DIHelper.getInstance().getProperty(REMOTE_RSERVE_KEY)) : (
					(System.getenv().containsKey(REMOTE_RSERVE_KEY)) 
					? Boolean.parseBoolean(System.getenv(REMOTE_RSERVE_KEY)) : false);


	private static final String LOAD_ENGINES_LOCALLY_KEY = "SEMOSS_LOAD_ENGINES_LOCALLY";
	public static final boolean LOAD_ENGINES_LOCALLY = (DIHelper.getInstance().getProperty(LOAD_ENGINES_LOCALLY_KEY) != null && !(DIHelper.getInstance().getProperty(IS_CLUSTER_KEY).isEmpty())) 
			? Boolean.parseBoolean(DIHelper.getInstance().getProperty(LOAD_ENGINES_LOCALLY_KEY)) : (
					(System.getenv().containsKey(LOAD_ENGINES_LOCALLY_KEY)) 
					? Boolean.parseBoolean(System.getenv(LOAD_ENGINES_LOCALLY_KEY)) : false);

	public static final List<String> CONFIGURATION_BLOBS = new ArrayList<String>(Arrays.asList(DB_IMAGES_BLOB));
	
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	public static String IMAGES_FOLDER_PATH = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "images";
	private static final String SCHEDULER_EXECUTOR_KEY = "SCHEDULER_EXECUTOR";

	private static final String IS_CLUSTERED_SCHEDULER_KEY = "SEMOSS_SCHEDULER_IS_CLUSTER";
	
	public static final boolean IS_CLUSTERED_SCHEDULER = (DIHelper.getInstance().getProperty(IS_CLUSTERED_SCHEDULER_KEY) != null && !(DIHelper.getInstance().getProperty(IS_CLUSTERED_SCHEDULER_KEY).isEmpty())) 
			? Boolean.parseBoolean(DIHelper.getInstance().getProperty(IS_CLUSTERED_SCHEDULER_KEY)) : (
					(System.getenv().containsKey(IS_CLUSTERED_SCHEDULER_KEY)) 
					? Boolean.parseBoolean(System.getenv(IS_CLUSTERED_SCHEDULER_KEY)) : IS_CLUSTER);
			

	/*
	 * private static final String MULTIPLE_STORAGE_ACCOUNTS_KEY =
	 * "MULTIPLE_STORAGE_ACCOUNTS"; public static final boolean
	 * MULTIPLE_STORAGE_ACCOUNTS =
	 * System.getenv().containsKey(MULTIPLE_STORAGE_ACCOUNTS_KEY) ?
	 * Boolean.parseBoolean(System.getenv(MULTIPLE_STORAGE_ACCOUNTS_KEY)) : false;
	 * 
	 * private static final String MAIN_STORAGE_ACCOUNT_KEY =
	 * "MAIN_STORAGE_ACCOUNT"; public static final String MAIN_STORAGE_ACOUNT =
	 * System.getenv(MAIN_STORAGE_ACCOUNT_KEY);
	 * 
	 * 
	 * //redis table info public static final String REDIS_STORAGE_ACCOUNT =
	 * "storageAccount"; public static final String REDIS_TIMESTAMP = "timestamp";
	 */

	public static void reactorPushApp(Collection<String> appIds) {
		if (ClusterUtil.IS_CLUSTER) {
			for (String appId : appIds) {
				reactorPushDatabase(appId);
			}
		}

	}

	public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
		InputStream is = new URL(url).openStream();
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
			return json;
		} finally {
			is.close();
		}
	}

	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	public static boolean isSchedulerExecutor() {
		if (ClusterUtil.IS_CLUSTER) {
			logger.info("Checking if pod is leader");
			//check rdf
			if(DIHelper.getInstance().getProperty(SCHEDULER_EXECUTOR_KEY) != null && !(DIHelper.getInstance().getProperty(SCHEDULER_EXECUTOR_KEY).isEmpty())) {
				return Boolean.parseBoolean(DIHelper.getInstance().getProperty(SCHEDULER_EXECUTOR_KEY));
			}

			//then check env var
			String leader = System.getenv(SCHEDULER_EXECUTOR_KEY);
			if(leader != null && !leader.isEmpty()) {
				return Boolean.parseBoolean(leader);
			}

			//zk
			return SchedulerListener.getListener().isZKLeader();

			//finally dynamic

//			String hostName = System.getenv("HOSTNAME");
//			logger.info("pod host name is " + hostName);
//
//			if(hostName == null || hostName.isEmpty()) {
//				throw new IllegalArgumentException("Hostname is null or empty along with no reference to scheduler execution in RDF_Map or env vars");
//			}
//		    try {
//		    	// TODO make this dynamic url instead of hard coded
//				JSONObject json = readJsonFromUrl("http://localhost:4040/");
//			    String electedLeader = json.get("name").toString();
//				logger.info("elected leader is " + electedLeader);
//
//				return hostName.equals(electedLeader);
//			} catch (JSONException e) {
//				logger.error(STACKTRACE, e);
//			} catch (IOException e) {
//				logger.error(STACKTRACE, e);
//			}
//		    return false;
		} else {
			//if its not clustered, return true to say its a executor
			return true;
		}
	}


	public static void reactorPullApp(String appId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pullDatabase(appId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to pull app to cloud storage", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	public static void reactorPullApp(String appId, boolean appAlreadyLoaded) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pullDatabase(appId, appAlreadyLoaded);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to pull app to cloud storage", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	public static void reactorPushDatabase(String appId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pushDatabase(appId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push app to cloud storage", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	public static void reactorPushDatabaseSmss(String appId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pushDatabaseSmss(appId);
			} catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push app smss to cloud storage", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	public static void reactorPullInsightsDB(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pullInsightsDB(projectId);
			} catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to check if project has been modified", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return;
	}

	public static void reactorPushInsightDB(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pushInsightDB(projectId);
			} catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to check if project has been modified", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return;
	}

	public static void reactorPullOwl(String appId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pullOwl(appId);
			} catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to pull owl for engine: " + appId, PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return;
	}

	public static void reactorPushOwl(String appId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pushOwl(appId);
			} catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push owl for engine: " + appId, PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return;
	}



	public static void reactorUpdateApp(String appId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().updateApp(appId);
			} catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to update app from cloud storage",
						PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	public static void reactorImagePull(String appId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().updateApp(appId);
			}  catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to fetch app image", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	public static void reactorPushDatabaseFolder(IDatabase engine, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {

			String appHome = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
					+ DIR_SEPARATOR + Constants.DB_FOLDER
					+ DIR_SEPARATOR + SmssUtilities.getUniqueName(engine.getEngineName(), engine.getEngineId());
			Path appHomePath = Paths.get(appHome);
			Path relative = appHomePath.relativize( Paths.get(absolutePath));
			ClusterUtil.reactorPushDatabaseFolder(engine.getEngineId(),absolutePath, relative.toString());
		}		
	}

	private static void reactorPushDatabaseFolder(String appId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pushEngineFolder(appId, absolutePath, remoteRelativePath);
			}  catch (IOException | InterruptedException e) {
				NounMetadata noun = new NounMetadata("Failed to push files", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}		
	}

	//This is only for items that fall under the an app directory. it won't work for abstract folders etc. 
	public static void  reactorPullDatabaseFolder(IDatabase engine, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {

			String appHome = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
					+ DIR_SEPARATOR + Constants.DB_FOLDER
					+ DIR_SEPARATOR + SmssUtilities.getUniqueName(engine.getEngineName(), engine.getEngineId());
			Path appHomePath = Paths.get(appHome);
			Path relative = appHomePath.relativize( Paths.get(absolutePath));
			ClusterUtil.reactorPullDatabaseFolder(engine.getEngineId(),absolutePath, relative.toString());

		}		
	}

	//create a pull folder
	public static void reactorPullDatabaseFolder(String appId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pullEngineFolder(appId, absolutePath, remoteRelativePath);
			}  catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push files", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}		
	}
	
	public static void reactorPushProject(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pushProject(projectId);
			} catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push project to cloud storage", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	public static void reactorPushProjectSmss(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pushProjectSmss(projectId);
			} catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push project smss to cloud storage", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	public static void  reactorPushProjectFolder(IProject project, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {
			reactorPushProjectFolder(project, absolutePath, null);
		}		
	}
	
	public static void  reactorPushProjectFolder(IProject project, String absolutePath, String relativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			if(relativePath != null && !(relativePath=relativePath.trim()).isEmpty()) {
				if(absolutePath.endsWith(DIR_SEPARATOR)) {
					absolutePath += relativePath;
				} else {
					absolutePath += DIR_SEPARATOR + relativePath;
				}
			}
			
			String projectHome = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
					+ DIR_SEPARATOR + Constants.PROJECT_FOLDER
					+ DIR_SEPARATOR + SmssUtilities.getUniqueName(project.getProjectName(), project.getProjectId());
			Path projectHomePath = Paths.get(projectHome);
			Path relative = projectHomePath.relativize( Paths.get(absolutePath));
			ClusterUtil.reactorPushProjectFolder(project.getProjectId(), absolutePath, relative.toString());

		}		
	}

	public static void reactorPushProjectFolder(String projectId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pushProjectFolder(projectId, absolutePath, remoteRelativePath);
			}  catch (IOException | InterruptedException e) {
				NounMetadata noun = new NounMetadata("Failed to push files", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	//This is only for items that fall under the an app directory. it won't work for abstract folders etc. 
	public static void  reactorPullProjectFolder(IProject project, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {
			reactorPullProjectFolder(project, absolutePath, null);
		}		
	}

	public static void  reactorPullProjectFolder(IProject project, String absolutePath, String relativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			if(relativePath != null && !(relativePath=relativePath.trim()).isEmpty()) {
				if(absolutePath.endsWith(DIR_SEPARATOR)) {
					absolutePath += relativePath;
				} else {
					absolutePath += DIR_SEPARATOR + relativePath;
				}
			}
			
			String projectHome = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
					+ DIR_SEPARATOR + Constants.PROJECT_FOLDER
					+ DIR_SEPARATOR + SmssUtilities.getUniqueName(project.getProjectName(), project.getProjectId());
			Path projectHomePath = Paths.get(projectHome);
			Path relative = projectHomePath.relativize( Paths.get(absolutePath));
			ClusterUtil.reactorPullProjectFolder(project.getProjectId(), absolutePath, relative.toString());
		}		
	}
	
	//create a pull folder
	public static void reactorPullProjectFolder(String projectId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pullProjectFolder(projectId, absolutePath, remoteRelativePath);
			}  catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push files", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	public static void reactorPushInsight(String projectId, String rdbmsId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pushInsight(projectId, rdbmsId);
			}  catch (IOException | InterruptedException e) {
				NounMetadata noun = new NounMetadata("Failed to push files", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	public static void reactorPullInsight(String projectId, String rdbmsId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pullInsight(projectId, rdbmsId);
			}  catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push files", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}		
	}
	
	public static void reactorPushUserWorkspace(IProject project, boolean isAsset) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pushUserAssetOrWorkspace(project.getProjectId(), isAsset);
			}  catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push user/workplace project", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	public static void reactorPullUserWorkspace(IProject project, boolean isAsset) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				AbstractCloudClient.getClient().pullUserAssetOrWorkspace(project.getProjectId(), isAsset, false);
			}  catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push user/workplace project", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	public static File getDatabaseImage(String databaseId) {
		File imageFile = null;
		File imageFolder= new File (IMAGES_FOLDER_PATH + DIR_SEPARATOR + "databases");
		String imageFilePath; 
		imageFolder.mkdirs();

		//so i dont always know the extension, but every image should be named by the appid which means i need to search the folder for something like the file
		File[] images = imageFolder.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.contains(databaseId);
			}
		});
		if(images!= null && images.length > 0){
			//we got a file. hopefully there is only 1 file if there is more, return [0] for now
			return images[0];
		}	
		else {
			try {
				//first try to pull the images folder, Return it after the pull, or else we make the file
				AbstractCloudClient.getClient().pullDatabaseImageFolder();
				//so i dont always know the extension, but every image should be named by the appid which means i need to search the folder for something like the file
				images = imageFolder.listFiles(new FilenameFilter() {
					@Override
					public boolean accept(File dir, String name) {
						return name.contains(databaseId);
					}
				});
				if(images.length > 0){
					//we got a file. hopefully there is only 1 file if there is more, return [0] for now
					return images[0];
				} else {
					String alias = SecurityEngineUtils.getEngineAliasForId(databaseId);
					imageFilePath = IMAGES_FOLDER_PATH + DIR_SEPARATOR + "databases" + DIR_SEPARATOR + databaseId + ".png";

					if(alias != null) {
						TextToGraphic.makeImage(alias, imageFilePath);
					} else{
						TextToGraphic.makeImage(databaseId, imageFilePath);
					}
					AbstractCloudClient.getClient().pushDatabaseImageFolder();
				}
				//finally we will return it if it exists, and if it doesn't we return back the stock. 
				imageFile = new File(imageFilePath);

				if(imageFile.exists()){
					return imageFile;
				} else{
					String stockImageDir = IMAGES_FOLDER_PATH + DIR_SEPARATOR + "stock" + DIR_SEPARATOR + "color-logo.png";
					imageFile = new File (stockImageDir);
				}

			} catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to fetch app image", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return imageFile;
	}
	
	public static File getProjectImage(String projectId) {
		File imageFile = null;
		File imageFolder= new File (IMAGES_FOLDER_PATH + DIR_SEPARATOR + "projects");
		String imageFilePath; 
		imageFolder.mkdirs();

		// so i dont always know the extension, 
		// but every image should be named by the projectid 
		// which means i need to search the folder for something like the file
		File[] images = imageFolder.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.contains(projectId);
			}
		});
		if(images!= null && images.length > 0){
			// we got a file. hopefully there is only 1 file if there is more, return [0] for now
			return images[0];
		}	
		else {
			try {
				// first try to pull the images folder, Return it after the pull, or else we make the file
				AbstractCloudClient.getClient().pullProjectImageFolder();
				// so i dont always know the extension, but every image should be named by the 
				// projectId which means i need to search the folder for something like the file
				images = imageFolder.listFiles(new FilenameFilter() {
					@Override
					public boolean accept(File dir, String name) {
						return name.contains(projectId);
					}
				});
				if(images.length > 0){
					//we got a file. hopefully there is only 1 file if there is more, return [0] for now
					return images[0];
				} else {
					String alias = SecurityProjectUtils.getProjectAliasForId(projectId);
					imageFilePath = IMAGES_FOLDER_PATH + DIR_SEPARATOR + "projects" + DIR_SEPARATOR + projectId + ".png";

					if(alias != null) {
						TextToGraphic.makeImage(alias, imageFilePath);
					} else{
						TextToGraphic.makeImage(projectId, imageFilePath);
					}
					AbstractCloudClient.getClient().pushProjectImageFolder();
				}
				//finally we will return it if it exists, and if it doesn't we return back the stock. 
				imageFile = new File(imageFilePath);

				if(imageFile.exists()){
					return imageFile;
				} else{
					String stockImageDir = IMAGES_FOLDER_PATH + DIR_SEPARATOR + "stock" + DIR_SEPARATOR + "color-logo.png";
					imageFile = new File (stockImageDir);
				}

			} catch (IOException | InterruptedException e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to fetch project image", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return imageFile;
	}


	public static List<File> getSubdirs(String path) {

		File file = new File(path);
		if (!file.isDirectory()){
			throw new IllegalArgumentException("File path must be a directory");
		}
		List<File> subdirs = Arrays.asList(file.listFiles(new FileFilter() {
			public boolean accept(File f) {
				return f.isDirectory();
			}
		}));
		subdirs = new ArrayList<File>(subdirs);

		List<File> deepSubdirs = new ArrayList<File>();
		for(File subdir : subdirs) {
			deepSubdirs.addAll(getSubdirs(subdir.getAbsolutePath())); 
		}
		subdirs.addAll(deepSubdirs);
		return subdirs;
	}

	public static void addHiddenFileToDir(File folder){
		File hiddenFile = new File(folder.getAbsolutePath() + DIR_SEPARATOR + Constants.HIDDEN_FILE_EXTENSION);
		try {
			hiddenFile.createNewFile();
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	public static void validateFolder(String folderPath){
		List<File> subdirs = getSubdirs(folderPath);
		for (File f : subdirs){
			if(!(f.list().length>0)){
				//System.out.println(f.getAbsolutePath());
				addHiddenFileToDir(f);
			}
		}
	}

	public static String getAppHome(IDatabase engine) {
		return  DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
				+ DIR_SEPARATOR + "db"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(engine.getEngineName(), engine.getEngineId());
	}

	public static String getAppHome(String engineId, String engineName) {
		return DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
				+ DIR_SEPARATOR + "db"
						+ DIR_SEPARATOR + SmssUtilities.getUniqueName(engineId, engineName);
			}

}
