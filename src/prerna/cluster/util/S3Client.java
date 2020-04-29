package prerna.cluster.util;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import com.google.common.io.Files;

import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SMSSWebWatcher;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class S3Client extends CloudClient{

	private static final String PROVIDER = "s3";
	private static String REGION = null;
	public static final String S3_REGION_KEY = "S3_REGION"; 
	private static String BUCKET = null;
	public static final String S3_BUCKET_KEY = "S3_BUCKET"; 

	private static final String SMSS_POSTFIX = "-smss";

	public static final String STORAGE = "STORAGE"; // says if this is local / cluster


	static S3Client client = null;
	static String rcloneConfigFolder = null;
	
	

	private static String ENDPOINT = null;
	private static String ACCESS_KEY = null;
	private static String SECRET_KEY = null;
	private String dbFolder = null;


	private static String RCLONE = "rclone";
	private static String RCLONE_PATH = "RCLONE_PATH";

	public static final String S3_ACCESS_KEY = "S3_ACCESS_KEY"; 
	public static final String S3_SECRET_KEY = "S3_SECRET_KEY"; 


	public static final String S3_ENDPOINT_KEY = "S3_ENDPOINT"; 
	
	public Boolean useMinio = false;
	
	public Boolean s3KeysProvided = false;
	
	
	protected S3Client()
	{

	}

	// create an instance
	// Also needs to be synchronized so multiple calls don't try to init at the same time
	public static synchronized S3Client getInstance()
	{
		if(client == null)
		{
			client = new S3Client();
			client.init();
		}
		return client;
	}


	@Override
	public void init() {

		rcloneConfigFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + "rcloneConfig";		
		new File(rcloneConfigFolder).mkdir();	
		dbFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + "db";

		Map <String, String> env = System.getenv();
		if(env.containsKey(S3_REGION_KEY)){
			REGION = env.get(S3_REGION_KEY);
		} else if (DIHelper.getInstance().getProperty(S3_REGION_KEY) != null &&  !(DIHelper.getInstance().getProperty(S3_REGION_KEY).isEmpty())) {
			REGION = DIHelper.getInstance().getProperty(S3_REGION_KEY);
		} else{
			throw new IllegalArgumentException("There is no region specified.");
		}
		
		
		if(env.containsKey(S3_BUCKET_KEY)){
			BUCKET = env.get(S3_BUCKET_KEY);
		} else if (DIHelper.getInstance().getProperty(S3_BUCKET_KEY) != null &&  !(DIHelper.getInstance().getProperty(S3_BUCKET_KEY).isEmpty())) {
			BUCKET = DIHelper.getInstance().getProperty(S3_BUCKET_KEY);
		}
		else{
			throw new IllegalArgumentException("There is no bucket key specified.");
		}
		
		if(env.containsKey(S3_ACCESS_KEY)){
			ACCESS_KEY = env.get(S3_ACCESS_KEY);
		} else if (DIHelper.getInstance().getProperty(S3_ACCESS_KEY) != null &&  !(DIHelper.getInstance().getProperty(S3_ACCESS_KEY).isEmpty())) {
			ACCESS_KEY = DIHelper.getInstance().getProperty(S3_ACCESS_KEY);
		}

		
		if(env.containsKey(S3_SECRET_KEY)){
			SECRET_KEY = env.get(S3_SECRET_KEY);
		} else if (DIHelper.getInstance().getProperty(S3_SECRET_KEY) != null &&  !(DIHelper.getInstance().getProperty(S3_SECRET_KEY).isEmpty())) {
			SECRET_KEY = DIHelper.getInstance().getProperty(S3_SECRET_KEY);
		}
		
		if(env.containsKey(S3_ENDPOINT_KEY)){
			ENDPOINT = env.get(S3_ENDPOINT_KEY);
		} else if (DIHelper.getInstance().getProperty(S3_ENDPOINT_KEY) != null &&  !(DIHelper.getInstance().getProperty(S3_ENDPOINT_KEY).isEmpty())) {
			ENDPOINT = DIHelper.getInstance().getProperty(S3_ENDPOINT_KEY);
		}
			
		if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("MINIO")){
			if (ACCESS_KEY == null || ACCESS_KEY.isEmpty() || SECRET_KEY == null || SECRET_KEY.isEmpty()  || ENDPOINT == null || ENDPOINT.isEmpty()){
				throw new IllegalArgumentException("Minio needs to have an access key, secret key, and endpoint defined.");
			}
			useMinio=true;
		} else if (ACCESS_KEY == null || ACCESS_KEY.isEmpty() || SECRET_KEY == null || SECRET_KEY.isEmpty()){
			s3KeysProvided = false;
		} else {
			s3KeysProvided = true;
		}

		if(env.containsKey(RCLONE_PATH)){
			RCLONE = env.get(RCLONE_PATH);
		} else if (DIHelper.getInstance().getProperty(RCLONE_PATH) != null &&  !(DIHelper.getInstance().getProperty(RCLONE_PATH).isEmpty())) {
			RCLONE = DIHelper.getInstance().getProperty(RCLONE_PATH);
		} else{
			RCLONE="rclone";
		}
		
	}

	public void syncInsightsDB(String appId) throws IOException, InterruptedException{
		IEngine engine = Utility.getEngine(appId, false, true);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}

		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		String RCloneConfig = null;

		try {
			RCloneConfig = createRcloneConfig(appId);
			engine.closeDB();
			System.out.println("Pulling insights database for" + appFolder + " from remote=" + appId);
			String insightDB = getInsightDB(appFolder);
			runRcloneProcess(RCloneConfig, "rclone", "sync", RCloneConfig + ":" + BUCKET+"/"+appId+"/"+insightDB, appFolder);

		}  finally {
			if (RCloneConfig != null) {
				deleteRcloneConfig(RCloneConfig);
			}
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
		if (engineType == ENGINE_TYPE.APP){
			 alias = engine.getEngineName();
		} else{
		     alias = SecurityQueryUtils.getEngineAliasForId(appId);
		}
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		String smss = aliasAppId + ".smss";
		String smssFile = dbFolder + FILE_SEPARATOR + smss;


		String RCloneConfig = null;

		try {
			RCloneConfig = createRcloneConfig(appId);
			String smssContainer = appId + SMSS_POSTFIX;

			// Some temp files needed for the transfer
			File temp = null;
			File copy = null;

			// Close the database, so that we can push without file locks (also ensures that the db doesn't change mid push)
			try {
				engine.closeDB();

				// Push the app folder
				System.out.println("Pushing from source=" + appFolder + " to remote=" + appId);
				runRcloneProcess(RCloneConfig, "rclone", "sync", appFolder, RCloneConfig + ":"+ BUCKET +"/"+ appId);
				System.out.println("Done pushing from source=" + appFolder + " to remote=" + appId);

				// Move the smss to an empty temp directory (otherwise will push all items in the db folder)
				String tempFolder = Utility.getRandomString(10);
				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
				temp.mkdir();
				copy = new File(temp.getPath() + FILE_SEPARATOR + smss);
				Files.copy(new File(smssFile), copy);

				// Push the smss
				System.out.println("Pushing from source=" + smssFile + " to remote=" + smssContainer);
				runRcloneProcess(RCloneConfig, "rclone", "sync", temp.getPath(), RCloneConfig + ":"+BUCKET+"/"+smssContainer);
				System.out.println("Done pushing from source=" + smssFile + " to remote=" + smssContainer);
			} finally {
				if (copy != null) {
					copy.delete();
				}
				if (temp != null) {
					temp.delete();
				}

				// Re-open the database

				if (engineType != ENGINE_TYPE.APP){
					DIHelper.getInstance().removeLocalProperty(appId);
					Utility.getEngine(appId, false, true);
				}
			}
		} finally {
			if (RCloneConfig != null) {
				deleteRcloneConfig(RCloneConfig);
			}
		}
	}

	@Override
	public void pullApp(String appId) throws IOException, InterruptedException {
		pullApp(appId, true);

	}

	@Override
	protected void pullApp(String appId, boolean newApp) throws IOException, InterruptedException {
		IEngine engine = null;
		if (!newApp) {
			engine = Utility.getEngine(appId, false, true);
			if (engine == null) {
				throw new IllegalArgumentException("App not found...");
			}
		}
		String smssContainer = appId + SMSS_POSTFIX;
		String RCloneConfig = null;
		try {
			RCloneConfig = createRcloneConfig(appId);
			List<String> results = runRcloneProcess(RCloneConfig, "rclone", "lsf", RCloneConfig + ":" + BUCKET + "/" + smssContainer);
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


			// Close the database (if an existing app), so that we can pull without file locks
			try {
				if (!newApp) {
					engine.closeDB();
				}

				// Make the app directory (if it doesn't already exist)
				File appFolder = new File(dbFolder + FILE_SEPARATOR + aliasAppId);
				appFolder.mkdir(); 
				// Pull the contents of the app folder before the smss
				System.out.println("Pulling from remote=" + appId + " to target=" + appFolder.getPath());
				runRcloneProcess(RCloneConfig, "rclone", "sync", RCloneConfig + ":" + BUCKET+"/"+appId, appFolder.getPath());
				System.out.println("Done pulling from remote=" + appId + " to target=" + appFolder.getPath());

				// Now pull the smss
				System.out.println("Pulling from remote=" + smssContainer + " to target=" + dbFolder);
				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
				runRcloneProcess(RCloneConfig, "rclone", "copy", RCloneConfig + ":"+ BUCKET + "/" + smssContainer, dbFolder);
				System.out.println("Done pulling from remote=" + smssContainer + " to target=" + dbFolder);

				// Catalog the db if it is new
				if (newApp) {
					SMSSWebWatcher.catalogDB(smss, dbFolder);
				}
			} finally {

				// Re-open the database (if an existing app)
				if (!newApp) {
					DIHelper.getInstance().removeLocalProperty(appId);
					Utility.getEngine(appId, false, true);
				}
			}
		} finally {
			if (RCloneConfig != null) {
				deleteRcloneConfig(RCloneConfig);
			}
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
		try {
			rcloneConfig = createRcloneConfig(appId);
			System.out.println("Deleting container=" + appId + ", " + appId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + BUCKET+"/"+appId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + BUCKET+"/" + appId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + BUCKET+"/" + appId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + BUCKET+"/" +appId + SMSS_POSTFIX);
			System.out.println("Done deleting container=" + appId + ", " + appId + SMSS_POSTFIX);
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
			List<String> results = runRcloneProcess(rcloneConfig, "rclone", "lsf", rcloneConfig + ":"+BUCKET);
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
		try {
			rcloneConfig = createRcloneConfig();
			System.out.println("Deleting container=" + containerId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + BUCKET +"/" + containerId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + BUCKET +"/" + containerId);
			System.out.println("Done deleting container=" + containerId);
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
	}



	//////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////// Static Util Methods ////////////////////////////////////

	private String createRcloneConfig(String appId){
		System.out.println("Generating config for app" + appId);
		String rcloneConfig = null;
		try {
			rcloneConfig = createRcloneConfig();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rcloneConfig;
	}
	
	private String createRcloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		
		if(useMinio){
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id", ACCESS_KEY, "secret_access_key", SECRET_KEY,"region", REGION, "endpoint", ENDPOINT);
		} else if(s3KeysProvided) {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "access_key_id", ACCESS_KEY, "secret_access_key", SECRET_KEY,"region", REGION);
		} else{
		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "env_auth","true","region",REGION);
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
		String RCloneConfig = null;
		try {
			RCloneConfig = createRcloneConfig(ClusterUtil.IMAGES_BLOB);
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "apps";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneProcess(RCloneConfig, "rclone", "copy", RCloneConfig + ":" + BUCKET+"/"+ClusterUtil.IMAGES_BLOB, imagesFolderPath);
		}finally {
			if (RCloneConfig != null) {
				deleteRcloneConfig(RCloneConfig);
			}
		}
	}

	@Override
	public void pushImageFolder() throws IOException, InterruptedException {
		String RCloneConfig = null;
		try {
			RCloneConfig = createRcloneConfig(ClusterUtil.IMAGES_BLOB);
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String imagesFolderPath = baseFolder + FILE_SEPARATOR + "images" + FILE_SEPARATOR + "apps";
			File imageFolder = new File(imagesFolderPath);
			imageFolder.mkdir();
			runRcloneProcess(RCloneConfig, "rclone", "sync",imagesFolderPath,  RCloneConfig + ":" + BUCKET+"/"+ClusterUtil.IMAGES_BLOB);
		}finally {
			if (RCloneConfig != null) {
				deleteRcloneConfig(RCloneConfig);
			}
		}
	}		
	

	@Override
	public void pushDB(String appId, RdbmsTypeEnum e) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false, true);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String RCloneConfig = null;
		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		try {
			RCloneConfig = createRcloneConfig(appId);
			engine.closeDB();
			System.out.println("Pulling database for" + appFolder + " from remote=" + appId);
			if(e == RdbmsTypeEnum.SQLITE){
				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
				for(String sqliteFile : sqliteFileNames){
				runRcloneProcess(RCloneConfig, "rclone", "sync", appFolder + "/" + sqliteFile, RCloneConfig + ":" + BUCKET + "/" + appId);
				}
			} else if(e == RdbmsTypeEnum.H2_DB){
				runRcloneProcess(RCloneConfig, "rclone", "sync", appFolder + "/database.mv.db", RCloneConfig + ":" + BUCKET + "/" +appId);
			} else{
				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
			}
			
			//open the engine again
			DIHelper.getInstance().removeLocalProperty(appId);
			Utility.getEngine(appId, false, true);
		} finally {
			if (RCloneConfig != null) {
				deleteRcloneConfig(RCloneConfig);
			}
		}		
	}

	@Override
	public void pullDB(String appId, RdbmsTypeEnum e) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false, true);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		String RCloneConfig = null;
		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		try {
			RCloneConfig = createRcloneConfig(appId);
			engine.closeDB();
			System.out.println("Pulling database for" + appFolder + " from remote=" + appId);
			if(e == RdbmsTypeEnum.SQLITE){
				List<String> sqliteFileNames = getSqlLiteFile(appFolder);
				for(String sqliteFile : sqliteFileNames){			
					runRcloneProcess(RCloneConfig, "rclone", "sync", RCloneConfig + ":" + BUCKET + "/" + appId+"/"+sqliteFile, appFolder);
				}
			} else if(e == RdbmsTypeEnum.H2_DB){
				runRcloneProcess(RCloneConfig, "rclone", "sync", RCloneConfig + ":"+ BUCKET + "/" + appId+"/database.mv.db", appFolder);
			} else{
				throw new IllegalArgumentException("Incorrect database type. Must be either sqlite or H2");
			}
		} finally {
			if (RCloneConfig != null) {
				deleteRcloneConfig(RCloneConfig);
			}
		}			
	}





}
