package prerna.cluster.util;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GCPClient extends S3Client{

	private static final String PROVIDER = "google cloud storage";

	private static String REGION = null;
	private static String RCLONE = "rclone";
	private static String RCLONE_PATH = "RCLONE_PATH";
	private static String GCP_SERVICE_ACCOUNT_FILE = null;

	public static final String GCP_SERVICE_ACCOUNT_FILE_KEY = "GCP_SERVICE_ACCOUNT_FILE";
	public static final String GCP_REGION_KEY = "GCP_REGION";
	public static final String GCP_BUCKET_KEY = "GCP_BUCKET";
	public static final String STORAGE = "STORAGE"; // says if this is local / cluster

	static GCPClient client = null;
	static String rcloneConfigFolder = null;

	// create an instance
	// Also needs to be synchronized so multiple calls don't try to init at the same
	// time
	public static synchronized GCPClient getInstance() {
		if (client == null) {
			client = new GCPClient();
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
		
		
		//right now GCP will only be done via the json key which provides everything needed
		Map<String, String> env = System.getenv();
		if (env.containsKey(GCP_SERVICE_ACCOUNT_FILE_KEY)) {
			GCP_SERVICE_ACCOUNT_FILE = env.get(GCP_SERVICE_ACCOUNT_FILE_KEY);
		} else if (DIHelper.getInstance().getProperty(GCP_SERVICE_ACCOUNT_FILE_KEY) != null
				&& !(DIHelper.getInstance().getProperty(GCP_SERVICE_ACCOUNT_FILE_KEY).isEmpty())) {
			GCP_SERVICE_ACCOUNT_FILE = DIHelper.getInstance().getProperty(GCP_SERVICE_ACCOUNT_FILE_KEY);
		} else {
			throw new IllegalArgumentException("There is no service account file specified.");
		}
		
		if (env.containsKey(GCP_BUCKET_KEY)) {
			BUCKET = env.get(GCP_BUCKET_KEY);
		} else if (DIHelper.getInstance().getProperty(GCP_BUCKET_KEY) != null
				&& !(DIHelper.getInstance().getProperty(GCP_BUCKET_KEY).isEmpty())) {
			BUCKET = DIHelper.getInstance().getProperty(GCP_BUCKET_KEY);
		} else {
			throw new IllegalArgumentException("There is no bucket key specified.");
		}
		

		if (env.containsKey(GCP_REGION_KEY)) {
			REGION = env.get(GCP_REGION_KEY);
		} else if (DIHelper.getInstance().getProperty(GCP_REGION_KEY) != null
				&& !(DIHelper.getInstance().getProperty(GCP_REGION_KEY).isEmpty())) {
			REGION = DIHelper.getInstance().getProperty(GCP_REGION_KEY);
		} else {
			throw new IllegalArgumentException("There is no region specified.");
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
	public String createRcloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);

		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "service_account_file",
				GCP_SERVICE_ACCOUNT_FILE, "location", REGION , "bucket_policy_only", "true");
	
		return rcloneConfig;
	}
}
