package prerna.cluster.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collection;

import prerna.auth.utils.SecurityQueryUtils;
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
	
	private static final String IS_CLUSTER_KEY = "SEMOSS_IS_CLUSTER";
	public static final boolean IS_CLUSTER = System.getenv().containsKey(IS_CLUSTER_KEY)
			? Boolean.parseBoolean(System.getenv(IS_CLUSTER_KEY))
			: false;

	private static final String STORAGE_PROVIDER_KEY = "SEMOSS_STORAGE_PROVIDER";
	public static final String STORAGE_PROVIDER = System.getenv(STORAGE_PROVIDER_KEY);

	private static final String REMOTE_RSERVE_KEY = "REMOTE_RSERVE";
	public static final boolean REMOTE_RSERVE = System.getenv().containsKey(REMOTE_RSERVE_KEY)
			? Boolean.parseBoolean(System.getenv(REMOTE_RSERVE_KEY))
			: false;

	private static final String LOAD_ENGINES_LOCALLY_KEY = "SEMOSS_LOAD_ENGINES_LOCALLY";
	public static final boolean LOAD_ENGINES_LOCALLY = System.getenv().containsKey(LOAD_ENGINES_LOCALLY_KEY)
			? Boolean.parseBoolean(System.getenv(LOAD_ENGINES_LOCALLY_KEY))
			: true;

	public static final String IMAGES_BLOB = "aaa-imagecontainer";


	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	public static String IMAGES_FOLDER_PATH = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR+"images";;
;

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
				reactorPushApp(appId);
			}
		}

	}
	
	public static void reactorPullApp(String appId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				CloudClient.getClient().pullApp(appId);
			} catch (IOException | InterruptedException e) {
				NounMetadata noun = new NounMetadata("Failed to pull app to cloud storage", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	public static void reactorPushApp(String appId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
				CloudClient.getClient().pushApp(appId);
			} catch (IOException | InterruptedException e) {
				NounMetadata noun = new NounMetadata("Failed to push app to cloud storage", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}
	
	public static void reactorSyncInsightsDB(String appId) {
		if (ClusterUtil.IS_CLUSTER) {
			try {
			 CloudClient.getClient().syncInsightsDB(appId);
			} catch (IOException | InterruptedException e) {
				NounMetadata noun = new NounMetadata("Failed to check if app has been modified", PixelDataType.CONST_STRING,
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
				CloudClient.getClient().updateApp(appId);
			} catch (IOException | InterruptedException e) {
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
				CloudClient.getClient().updateApp(appId);
			}  catch (IOException | InterruptedException e) {
				NounMetadata noun = new NounMetadata("Failed to fetch app image", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
	}

	public static File getImage(String appID) {
		
			File imageFile = null;
			File imageFolder= new File (IMAGES_FOLDER_PATH + DIR_SEPARATOR + "apps");
			String imageFilePath; 
			imageFolder.mkdirs();
			
			//so i dont always know the extension, but every image should be named by the appid which means i need to search the folder for something like the file
			  File[] images = imageFolder.listFiles(new FilenameFilter() {
			        @Override
			        public boolean accept(File dir, String name) {
			            return name.contains(appID);
			        }
			    });
			if(images!= null && images.length > 0){
				//we got a file. hopefully there is only 1 file if there is more, return [0] for now
				return images[0];
			}	
		else
			try {
				//first try to pull the images folder, Return it after the pull, or else we make the file
				CloudClient.getClient().pullImageFolder();
				//so i dont always know the extension, but every image should be named by the appid which means i need to search the folder for something like the file
				 images = imageFolder.listFiles(new FilenameFilter() {
				        @Override
				        public boolean accept(File dir, String name) {
				            return name.contains(appID);
				        }
				    });
				if(images.length > 0){
					//we got a file. hopefully there is only 1 file if there is more, return [0] for now
					return images[0];
				} else {
					String alias = SecurityQueryUtils.getEngineAliasForId(appID);
					 imageFilePath = IMAGES_FOLDER_PATH + DIR_SEPARATOR + appID + ".png";

					if(alias != null) {
						TextToGraphic.makeImage(alias, imageFilePath);
					} else{
						TextToGraphic.makeImage(appID, imageFilePath);
					}
					CloudClient.getClient().pushImageFolder();
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
				NounMetadata noun = new NounMetadata("Failed to fetch app image", PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			} 
			return imageFile;
	}
}
