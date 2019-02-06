package prerna.cluster.util;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import com.google.common.io.Files;
import com.microsoft.azure.storage.analytics.CloudAnalyticsClient;

import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SMSSWebWatcher;
import prerna.util.Utility;

public class AWSClient extends CloudClient{

	private static final String PROVIDER = "s3";
	private static String aws_region = null;
	public static final String AWS_REGION_KEY = "AWS_REGION"; 
	private static String aws_bucket = null;
	public static final String AWS_BUCKET_KEY = "AWS_BUCKET"; 

	private static final String SMSS_POSTFIX = "-smss";
	private static final String FILE_SEPARATOR = System.getProperty("file.separator");

	public static final String STORAGE = "STORAGE"; // says if this is local / cluster


	static AWSClient client = null;
	static String rcloneConfigFolder = null;

	String dbFolder = null;
	protected AWSClient()
	{

	}

	// create an instance
	// Also needs to be synchronized so multiple calls don't try to init at the same time
	public static synchronized AWSClient getInstance()
	{
		if(client == null)
		{
			client = new AWSClient();
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
		if(env.containsKey(AWS_REGION_KEY)){
			aws_region = env.get(AWS_REGION_KEY);
		}
		else{
			throw new IllegalArgumentException("Region has not beem set");
		}
		if(env.containsKey(AWS_BUCKET_KEY)){
			aws_bucket = env.get(AWS_BUCKET_KEY);
		}
		else{
			throw new IllegalArgumentException("Bucket has not beem set");
		}
	}


	@Override
	public void pushApp(String appId) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		// We need to push the folder alias__appId and the file alias__appId.smss
		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		String aliasAppId = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + aliasAppId;
		String smss = aliasAppId + ".smss";
		String smssFile = dbFolder + FILE_SEPARATOR + smss;


		String awsRCloneConfig = null;

		try {
			awsRCloneConfig = createRcloneConfig(appId);
			String smssContainer = appId + SMSS_POSTFIX;

			// Some temp files needed for the transfer
			File temp = null;
			File copy = null;

			// Close the database, so that we can push without file locks (also ensures that the db doesn't change mid push)
			try {
				engine.closeDB();

				// Push the app folder
				System.out.println("Pushing from source=" + appFolder + " to remote=" + appId);
				runRcloneProcess(awsRCloneConfig, "rclone", "sync", appFolder, awsRCloneConfig + ":"+ aws_bucket +"/"+ appId);
				System.out.println("Done pushing from source=" + appFolder + " to remote=" + appId);

				// Move the smss to an empty temp directory (otherwise will push all items in the db folder)
				String tempFolder = Utility.getRandomString(10);
				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
				temp.mkdir();
				copy = new File(temp.getPath() + FILE_SEPARATOR + smss);
				Files.copy(new File(smssFile), copy);

				// Push the smss
				System.out.println("Pushing from source=" + smssFile + " to remote=" + smssContainer);
				runRcloneProcess(awsRCloneConfig, "rclone", "sync", temp.getPath(), awsRCloneConfig + ":"+aws_bucket+"/"+smssContainer);
				System.out.println("Done pushing from source=" + smssFile + " to remote=" + smssContainer);
			} finally {
				if (copy != null) {
					copy.delete();
				}
				if (temp != null) {
					temp.delete();
				}

				// Re-open the database
				DIHelper.getInstance().removeLocalProperty(appId);
				Utility.getEngine(appId, false);
			}
		} finally {
			if (awsRCloneConfig != null) {
				deleteRcloneConfig(awsRCloneConfig);
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
			engine = Utility.getEngine(appId, false);
			if (engine == null) {
				throw new IllegalArgumentException("App not found...");
			}
		}
		String smssContainer = appId + SMSS_POSTFIX;
		String awsRcloneConfig = null;
		try {
			awsRcloneConfig = createRcloneConfig(appId);
			List<String> results = runRcloneProcess(awsRcloneConfig, "rclone", "lsf", awsRcloneConfig + ":" + aws_bucket + "/" + smssContainer);
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
				runRcloneProcess(awsRcloneConfig, "rclone", "sync", awsRcloneConfig + ":" + aws_bucket+"/"+appId, appFolder.getPath());
				System.out.println("Done pulling from remote=" + appId + " to target=" + appFolder.getPath());

				// Now pull the smss
				System.out.println("Pulling from remote=" + smssContainer + " to target=" + dbFolder);
				// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
				runRcloneProcess(awsRcloneConfig, "rclone", "copy", awsRcloneConfig + ":"+ aws_bucket + "/" + smssContainer, dbFolder);
				System.out.println("Done pulling from remote=" + smssContainer + " to target=" + dbFolder);

				// Catalog the db if it is new
				if (newApp) {
					SMSSWebWatcher.catalogDB(smss, dbFolder);
				}
			} finally {

				// Re-open the database (if an existing app)
				if (!newApp) {
					DIHelper.getInstance().removeLocalProperty(appId);
					Utility.getEngine(appId, false);
				}
			}
		} finally {
			if (awsRcloneConfig != null) {
				deleteRcloneConfig(awsRcloneConfig);
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
		String rcloneConfig = Utility.getRandomString(10);
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "env_auth","true","region", aws_region);
			System.out.println("Deleting container=" + appId + ", " + appId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + aws_bucket+"/"+appId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + aws_bucket+"/" + appId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + aws_bucket+"/" + appId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + aws_bucket+"/" +appId + SMSS_POSTFIX);
			System.out.println("Done deleting container=" + appId + ", " + appId + SMSS_POSTFIX);
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
	}

	@Override
	public List<String> listAllBlobContainers() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		List<String> allContainers = new ArrayList<>();
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "env_auth","true","region", aws_region);
			List<String> results = runRcloneProcess(rcloneConfig, "rclone", "lsf", rcloneConfig + ":"+aws_bucket);
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
			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "env_auth","true","region", aws_region);
			System.out.println("Deleting container=" + containerId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + aws_bucket +"/" + containerId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + aws_bucket +"/" + containerId);
			System.out.println("Done deleting container=" + containerId);
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
	}

	

	//////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////// Static Util Methods ////////////////////////////////////

	private static String createRcloneConfig(String appId) throws IOException, InterruptedException {
		System.out.println("Generating config for app" + appId);
		String rcloneConfig = Utility.getRandomString(10);
		runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "env_auth","true","region",aws_region);
		return rcloneConfig;
	}


}
