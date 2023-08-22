package prerna.cluster.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.clients.CentralCloudStorage;
import prerna.cluster.util.clients.ICloudClient;
import prerna.engine.api.IDatabaseEngine;
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

	public static final List<String> CONFIGURATION_BLOBS = new ArrayList<String>(Arrays.asList(CentralCloudStorage.DB_IMAGES_BLOB));
	
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

	/**
	 * 
	 * @return
	 * @throws Exception 
	 */
	public static ICloudClient getCentralStorageClient() throws Exception {
		return CentralCloudStorage.getInstance();
	}
	
	/**
	 * 
	 * @param databaseId
	 */
	public static void pullDatabase(String databaseId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullDatabase(databaseId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull database '"+databaseId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * 
	 * @param databaseId
	 * @param databaseAlreadyLoaded
	 */
	public static void pullDatabase(String databaseId, boolean databaseAlreadyLoaded) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullDatabase(databaseId, databaseAlreadyLoaded);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull database '"+databaseId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * 
	 * @param databaseId
	 */
	public static void pushDatabase(String databaseId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushDatabase(databaseId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push database '"+databaseId+"' to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param databaseId
	 */
	public static void pushDatabaseSmss(String databaseId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushDatabaseSmss(databaseId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push database '"+databaseId+"'smss to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param databaseId
	 */
	public static void deleteDatabase(String databaseId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().deleteDatabase(databaseId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to delete database '"+databaseId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param storageId
	 */
	public static void pullStorage(String storageId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullStorage(storageId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull storage '"+storageId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param storageId
	 * @param storageAlreadyLoaded
	 */
	public static void pullStorage(String storageId, boolean storageAlreadyLoaded) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullStorage(storageId, storageAlreadyLoaded);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull storage '"+storageId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param storageId
	 */
	public static void pushStorage(String storageId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushStorage(storageId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push storage '"+storageId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param databaseId
	 */
	public static void pushStorageSmss(String storageId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushStorageSmss(storageId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push storage '"+storageId+"'smss to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param storageId
	 */
	public static void deleteStorage(String storageId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().deleteStorage(storageId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull storage '"+storageId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param modelId
	 */
	public static void pullModel(String modelId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullModel(modelId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull model '"+modelId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param modelId
	 * @param modelAlreadyLoaded
	 */
	public static void pullModel(String modelId, boolean modelAlreadyLoaded) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullModel(modelId, modelAlreadyLoaded);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull model '"+modelId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param modelId
	 */
	public static void pushModel(String modelId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushModel(modelId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push model '"+modelId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param databaseId
	 */
	public static void pushModelSmss(String modelId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushModelSmss(modelId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push model '"+modelId+"'smss to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param modelId
	 */
	public static void deleteModel(String modelId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().deleteModel(modelId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to delete model '"+modelId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * 
	 * @param projectId
	 */
	public static void pullProject(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullProject(projectId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull project '"+projectId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param projectId
	 * @param projectAlreadyLoaded
	 */
	public static void pullProject(String projectId, boolean projectAlreadyLoaded) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullProject(projectId, projectAlreadyLoaded);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull project '"+projectId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param projectId
	 */
	public static void deleteProject(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().deleteProject(projectId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to delete project '"+projectId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param projectId
	 */
	public static void pullInsightsDB(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullInsightsDB(projectId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull project '"+projectId+"' insight database from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return;
	}

	/**
	 * 
	 * @param projectId
	 */
	public static void pushInsightDB(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushInsightDB(projectId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push project '"+projectId+"' insight database to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return;
	}

	/**
	 * 
	 * @param databaseId
	 */
	public static void pullOwl(String databaseId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullOwl(databaseId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull owl for database '"+databaseId+"' from cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return;
	}

	/**
	 * 
	 * @param databaseId
	 */
	public static void pushOwl(String databaseId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushOwl(databaseId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push owl for database '"+databaseId+"' to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return;
	}

	/**
	 * 
	 * @param database
	 * @param absolutePath
	 */
	public static void pushDatabaseFolder(IDatabaseEngine database, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {
			String dbHome = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
					+ DIR_SEPARATOR + Constants.DB_FOLDER
					+ DIR_SEPARATOR + SmssUtilities.getUniqueName(database.getEngineName(), database.getEngineId());
			Path dbHomePath = Paths.get(dbHome);
			Path relative = dbHomePath.relativize( Paths.get(absolutePath));
			ClusterUtil.pushDatabaseFolder(database.getEngineId(),absolutePath, relative.toString());
		}		
	}

	/**
	 * 
	 * @param databaseId
	 * @param absolutePath
	 * @param remoteRelativePath
	 */
	private static void pushDatabaseFolder(String databaseId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushDatabaseFolder(databaseId, absolutePath, remoteRelativePath);
			}  catch (Exception e) {
				SemossPixelException err = new SemossPixelException("Failed to push database '"+databaseId+"' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}		
	}

	/**
	 * 
	 * @param database
	 * @param absolutePath
	 */
	public static void  pullDatabaseFolder(IDatabaseEngine database, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {
			String dbHome = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
					+ DIR_SEPARATOR + Constants.DB_FOLDER
					+ DIR_SEPARATOR + SmssUtilities.getUniqueName(database.getEngineName(), database.getEngineId());
			Path dbHomePath = Paths.get(dbHome);
			Path relative = dbHomePath.relativize( Paths.get(absolutePath));
			ClusterUtil.pullDatabaseFolder(database.getEngineId(),absolutePath, relative.toString());
		}		
	}

	/**
	 * 
	 * @param databaseId
	 * @param absolutePath
	 * @param remoteRelativePath
	 */
	public static void pullDatabaseFolder(String databaseId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullDatabaseFolder(databaseId, absolutePath, remoteRelativePath);
			}  catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull database '"+databaseId+"' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param projectId
	 */
	public static void pushProject(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushProject(projectId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push project '"+projectId+"' to cloud storage", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param projectId
	 */
	public static void pushProjectSmss(String projectId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushProjectSmss(projectId);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata("Failed to push project '"+projectId+"' smss to cloud storage", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param project
	 * @param absolutePath
	 */
	public static void  pushProjectFolder(IProject project, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {
			pushProjectFolder(project, absolutePath, null);
		}		
	}
	
	/**
	 * 
	 * @param project
	 * @param absolutePath
	 * @param relativePath
	 */
	public static void  pushProjectFolder(IProject project, String absolutePath, String relativePath) {
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
			ClusterUtil.pushProjectFolder(project.getProjectId(), absolutePath, relative.toString());

		}		
	}

	/**
	 * 
	 * @param projectId
	 * @param absolutePath
	 * @param remoteRelativePath
	 */
	public static void pushProjectFolder(String projectId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushProjectFolder(projectId, absolutePath, remoteRelativePath);
			}  catch (Exception e) {
				SemossPixelException err = new SemossPixelException("Failed to push project '"+projectId+"' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * 
	 * @param project
	 * @param absolutePath
	 */
	public static void  pullProjectFolder(IProject project, String absolutePath) {
		if (ClusterUtil.IS_CLUSTER) {
			pullProjectFolder(project, absolutePath, null);
		}		
	}

	/**
	 * 
	 * @param project
	 * @param absolutePath
	 * @param relativePath
	 */
	public static void  pullProjectFolder(IProject project, String absolutePath, String relativePath) {
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
			ClusterUtil.pullProjectFolder(project.getProjectId(), absolutePath, relative.toString());
		}		
	}
	
	/**
	 * 
	 * @param projectId
	 * @param absolutePath
	 * @param remoteRelativePath
	 */
	public static void pullProjectFolder(String projectId, String absolutePath, String remoteRelativePath) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullProjectFolder(projectId, absolutePath, remoteRelativePath);
			}  catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull project '"+projectId+"' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param projectId
	 * @param rdbmsId
	 */
	public static void pushInsight(String projectId, String rdbmsId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushInsight(projectId, rdbmsId);
			}  catch (Exception e) {
				SemossPixelException err = new SemossPixelException("Failed to push project '"+projectId+"' insight '"+rdbmsId+"' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param projectId
	 * @param rdbmsId
	 */
	public static void pullInsight(String projectId, String rdbmsId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullInsight(projectId, rdbmsId);
			}  catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull project '"+projectId+"' insight '"+rdbmsId+"' folder");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}		
	}
	
	/**
	 * 
	 * @param project
	 * @param isAsset
	 */
	public static void pushUserWorkspace(String projectId, boolean isAsset) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushUserAssetOrWorkspace(projectId, isAsset);
			}  catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push user/workplace project");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param project
	 * @param isAsset
	 */
	public static void pullUserWorkspace(String projectId, boolean isAsset) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullUserAssetOrWorkspace(projectId, isAsset, false);
			}  catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull user/workplace project");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 * @param project
	 * @param isAsset
	 */
	public static void pullUserWorkspace(String projectId, boolean isAsset, boolean alreadyLoaded) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullUserAssetOrWorkspace(projectId, isAsset, alreadyLoaded);
			}  catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull user/workplace project");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 */
	public static void pushDatabaseImageFolder() {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushDatabaseImageFolder();
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push database image folder to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 */
	public static void pullDatabaseImageFolder() {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullDatabaseImageFolder();
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull database image folder to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 */
	public static void pushProjectImageFolder() {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushProjectImageFolder();
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push project image folder to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	/**
	 * 
	 */
	public static void pullProjectImageFolder() {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pullProjectImageFolder();
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to pull project image folder to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	public static void pushInsightImage(String projectId, String insightId, String oldImageName, String imageFileName) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				getCentralStorageClient().pushInsightImage(projectId, insightId, oldImageName, imageFileName);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to push insight image to cloud storage");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	/**
	 * 
	 * @param databaseId
	 * @return
	 */
	public static File getDatabaseImage(String databaseId) {
		File imageFile = null;
		File imageFolder= new File (IMAGES_FOLDER_PATH + DIR_SEPARATOR + "databases");
		String imageFilePath; 
		imageFolder.mkdirs();

		//so i dont always know the extension, but every image should be named by the engineid which means i need to search the folder for something like the file
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
				getCentralStorageClient().pullDatabaseImageFolder();
				//so i dont always know the extension, but every image should be named by the engineid which means i need to search the folder for something like the file
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
					getCentralStorageClient().pushDatabaseImageFolder();
				}
				//finally we will return it if it exists, and if it doesn't we return back the stock. 
				imageFile = new File(imageFilePath);

				if(imageFile.exists()){
					return imageFile;
				} else{
					String stockImageDir = IMAGES_FOLDER_PATH + DIR_SEPARATOR + "stock" + DIR_SEPARATOR + "color-logo.png";
					imageFile = new File (stockImageDir);
				}

			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to fetch database image");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return imageFile;
	}
	
	/**
	 * 
	 * @param storageId
	 * @return
	 */
	public static File getStorageImage(String storageId) {
		File imageFile = null;
		File imageFolder= new File (IMAGES_FOLDER_PATH + DIR_SEPARATOR + "storages");
		String imageFilePath; 
		imageFolder.mkdirs();

		//so i dont always know the extension, but every image should be named by the engineid which means i need to search the folder for something like the file
		File[] images = imageFolder.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.contains(storageId);
			}
		});
		if(images!= null && images.length > 0){
			//we got a file. hopefully there is only 1 file if there is more, return [0] for now
			return images[0];
		}	
		else {
			try {
				//first try to pull the images folder, Return it after the pull, or else we make the file
				getCentralStorageClient().pullStorageImageFolder();
				//so i dont always know the extension, but every image should be named by the engineid which means i need to search the folder for something like the file
				images = imageFolder.listFiles(new FilenameFilter() {
					@Override
					public boolean accept(File dir, String name) {
						return name.contains(storageId);
					}
				});
				if(images.length > 0){
					//we got a file. hopefully there is only 1 file if there is more, return [0] for now
					return images[0];
				} else {
					String alias = SecurityEngineUtils.getEngineAliasForId(storageId);
					imageFilePath = IMAGES_FOLDER_PATH + DIR_SEPARATOR + "storages" + DIR_SEPARATOR + storageId + ".png";

					if(alias != null) {
						TextToGraphic.makeImage(alias, imageFilePath);
					} else{
						TextToGraphic.makeImage(storageId, imageFilePath);
					}
					getCentralStorageClient().pushStorageImageFolder();
				}
				//finally we will return it if it exists, and if it doesn't we return back the stock. 
				imageFile = new File(imageFilePath);

				if(imageFile.exists()){
					return imageFile;
				} else{
					String stockImageDir = IMAGES_FOLDER_PATH + DIR_SEPARATOR + "stock" + DIR_SEPARATOR + "color-logo.png";
					imageFile = new File (stockImageDir);
				}

			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to fetch storage image");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return imageFile;
	}
	
	/**
	 * 
	 * @param modelId
	 * @return
	 */
	public static File getModelImage(String modelId) {
		File imageFile = null;
		File imageFolder= new File (IMAGES_FOLDER_PATH + DIR_SEPARATOR + "models");
		String imageFilePath; 
		imageFolder.mkdirs();

		//so i dont always know the extension, but every image should be named by the engineid which means i need to search the folder for something like the file
		File[] images = imageFolder.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.contains(modelId);
			}
		});
		if(images!= null && images.length > 0){
			//we got a file. hopefully there is only 1 file if there is more, return [0] for now
			return images[0];
		}	
		else {
			try {
				//first try to pull the images folder, Return it after the pull, or else we make the file
				getCentralStorageClient().pullModelImageFolder();
				//so i dont always know the extension, but every image should be named by the engineid which means i need to search the folder for something like the file
				images = imageFolder.listFiles(new FilenameFilter() {
					@Override
					public boolean accept(File dir, String name) {
						return name.contains(modelId);
					}
				});
				if(images.length > 0){
					//we got a file. hopefully there is only 1 file if there is more, return [0] for now
					return images[0];
				} else {
					String alias = SecurityEngineUtils.getEngineAliasForId(modelId);
					imageFilePath = IMAGES_FOLDER_PATH + DIR_SEPARATOR + "models" + DIR_SEPARATOR + modelId + ".png";

					if(alias != null) {
						TextToGraphic.makeImage(alias, imageFilePath);
					} else{
						TextToGraphic.makeImage(modelId, imageFilePath);
					}
					getCentralStorageClient().pushModelImageFolder();
				}
				//finally we will return it if it exists, and if it doesn't we return back the stock. 
				imageFile = new File(imageFilePath);

				if(imageFile.exists()){
					return imageFile;
				} else{
					String stockImageDir = IMAGES_FOLDER_PATH + DIR_SEPARATOR + "stock" + DIR_SEPARATOR + "color-logo.png";
					imageFile = new File (stockImageDir);
				}

			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to fetch model image");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return imageFile;
	}
	
	/**
	 * 
	 * @param projectId
	 * @return
	 */
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
				getCentralStorageClient().pullProjectImageFolder();
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
					getCentralStorageClient().pushProjectImageFolder();
				}
				//finally we will return it if it exists, and if it doesn't we return back the stock. 
				imageFile = new File(imageFilePath);

				if(imageFile.exists()){
					return imageFile;
				} else{
					String stockImageDir = IMAGES_FOLDER_PATH + DIR_SEPARATOR + "stock" + DIR_SEPARATOR + "color-logo.png";
					imageFile = new File (stockImageDir);
				}

			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to fetch project image");
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		return imageFile;
	}

	/**
	 * 
	 * @param folderPath
	 */
	public static void validateFolder(String folderPath){
		List<File> subdirs = getSubdirs(folderPath);
		for (File f : subdirs){
			if(!(f.list().length>0)){
				addHiddenFileToDir(f);
			}
		}
	}

	/**
	 * 
	 * @param path
	 * @return
	 */
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

	/**
	 * 
	 * @param folder
	 */
	public static void addHiddenFileToDir(File folder){
		File hiddenFile = new File(folder.getAbsolutePath() + DIR_SEPARATOR + Constants.HIDDEN_FILE_EXTENSION);
		try {
			hiddenFile.createNewFile();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

}
